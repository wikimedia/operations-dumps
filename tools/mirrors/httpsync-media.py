import os, sys, re, hashlib, httplib, getopt, gzip
from httplib import HTTPConnection

class HttpSync(object):
    """http-sync media from a remote site to local directory"""
    def __init__(self, host, uploadDir, localDir, newMediaFile, oldMediaFile, remoteSubdirsFile, updatedMediaFile, sha1, fieldNum, dryrun, verbose):
        self.host = host
        self.uploadDir = uploadDir
        self.localDir = localDir
        self.newMediaFile = newMediaFile
        self.oldMediaFile = oldMediaFile
        self.remoteSubdirsFile = remoteSubdirsFile
        self.updatedMediaFile = updatedMediaFile
        self.updfd = None
        self.sha1Check = sha1Check
        self.fieldNum = fieldNum - 1 # because we drop the filename off the list of fields before using the list
        self.dryrun = dryrun
        self.verbose = verbose
        self.oldMediaDict = {}       # media filenames from old list with metadata (size or sha1)
        self.newMediaToCheck = {}    # media filenames from new list with metadata (size or sha1)
        self.mediaToDelete = None    # will contain lists of media to delete per directory, 1 = delete, 0 = keep
        self.httpConn = None
        self.alphabet="0123456789abcdefghijklmnopqrstuvwxyz"
        self.remoteDirDict = {}
        self.dirMaxDepth = 0

    def getLineFromMediaFile(self, fd):
        """return line read from media file list
        args:
        fd: open file descriptor from which to read
        returns:
          None on eof
          False on blank line or comment or badly formatted line
          stripped line otherwise"""

        line = fd.readline()
        if not line:
            return None # eof
        line = line.strip()
        # skip blank lines and comments
        if not line or line[0] == '#':
            return False
        if not '\t' in line:
            whine()
            return False
        return line
        
    def loadOldMediaList(self):
        """set up dict of old media info if there is any
        returns: nothing"""
        if not oldMediaFile:
            return

        omfd = open(oldMediaFile, "r")
        while (True):
            line = self.getLineFromMediaFile(omfd)
            if line == None:
                break
            elif not line:
                continue

            # FIXME This should be done on a per-dir basis, anything else 
            # will hurt a lot for commons. how do we get that done?
            # add a processing step that sorts by the hashdir
            # of the lists?  this can be run separately. if the sort someday
            # is too big to fit in mem then we do it on disk.

            # format of lines in list:
            # filename<tab>stuff<tab>...<tab>size or sha1<tab>...<tab>subdir\n
            filename, metaDataList = line.split('\t')
            dirname = metaDataList[-1]
            if dirname not in self.oldMediaDict:
                self.oldMediaDict[dirname] = {}
            self.oldMediaDict[dirname][filename] = metaDataList[self.fieldNum]
        omfd.close()

    def scanDirForPossibleDeletes(self, subdir):
        """add all files in directory on local filesystem to the list of
        possible media to delete
        args:
        subdir: directory, relative to self.localDir, to scan
        returns: nothing"""
        dirToScan = os.path.join(self.localDir, subdir)
        if subdir not in self.mediaToDelete:
            self.mediaToDelete[subdir] = {}
        if os.path.isdir(dirToScan):
            files = os.listdir(dirToScan)
            for f in files:
                self.mediaToDelete[subdir][f] = 1  # default: all files are up for deletion

    def loadNewMediaList(self):
        """set up dict from new media list; this will contain all media to
        retrieve either because they don't exist on local filesystem or they
        have been changed
        also clear entries for files in media to delete if we in fact want
        to keep them
        returns: nothing"""
        self.mediaToDelete = self.oldMediaDict
        if self.newMediaFile == '-':
            wfd = sys.stdin
        elif newMediaFile.endswith("gz"):
            wfd = gzip.open(newMediaFile, "r")
        else:
            wfd = open(newMediaFile, "r")
        while (True):
            line = self.getLineFromMediaFile(wfd)
            if line == None:
                break
            elif not line:
                continue
            # format of lines in list:
            # filename<tab>stuff<tab>...<tab>size or sha1<tab>...<tab>subdir\n
            filename, metaData = line.split('\t', 1)
            if '\t' in metaData:
                metaDataList = metaData.split('\t')
            else:
                metaDataList = [ metaData ]
            dirname = metaDataList[-1]
            if ( dirname not in self.oldMediaDict or 
            	filename not in self.oldMediaDict[dirname] or
                self.oldMediaDict[dirname][filename] != metaDataList[self.fieldNum] ):
                # new file since old media list was created or file has changed
                if dirname not in self.newMediaToCheck:
                    self.newMediaToCheck[dirname] = {}
                self.newMediaToCheck[dirname][filename] = metaDataList[self.fieldNum]
                if not dirname in self.mediaToDelete:
                    # for every subdir with media on remote server, record what files we have locally
                    # we have locally, if we haven't already collected the info; we may need to delete 
                    # these files later
                    self.scanDirForPossibleDeletes(dirname)
                self.mediaToDelete[dirname][filename] = 0 # not to be deleted
            # file won't be changed, so write it to 'current status' list
            elif self.updfd:
                self.updfd.write( "%s\t%s\t%s\n" % (filename,  metaDataList[self.fieldNum], dirname))
        if wfd != sys.stdin:
            wfd.close()

    def openUpdatesFile(self):
        """if we are to write the current status of media files as we
        update them, open the file where those updates will be written
        returns: nothing"""
        if self.updatedMediaFile:
            if self.updatedMediaFile == '-':
                self.updfd = sys.stdout
            elif self.updatedMediaFile.endswith(".gz"):
                self.updfd = gzip.open(self.updatedMediaFile, "w")
            else:
                self.updfd = open(self.updatedMediaFile, "w")

    def loadMediaLists(self):
        """read new media list and (if set) old media list, creating
        list of media to check (media changed from old list to new,
        or the entire new list of there is no old list)
        files ending in .gz will be opened with gzip
        returns: nothing"""
        self.openUpdatesFile()
        self.loadOldMediaList()
        self.loadNewMediaList()

    def doSync(self):
        """delete, update and create media files on local filesystem so that
        it corresponds to the state in the new media file list
        returns: nothing"""
        for d in self.mediaToDelete:
            # delete local files no longer on remote server (according to new media list)
            self.doDeletes(d)
        for d in self.newMediaToCheck:
            # retrieve all media that have changed on remote server or which do not
            # exist on local filesystem but the remote server has them (according to 
            # new media list)
            self.doUpdatesAndCreates(d)
        self.deleteContentsOfExtraDirs()
        self.closeUpdatesFile()

    def closeUpdatesFile(self):
        """if we are writing the current status of the local copy of media files
        someplace, close that file"""
        if self.updfd:
            if self.updfd != sys.stdout:
                self.updfd.close()

    def cleanupDirAndSubdirs(self, dirName, depth):
        """check this directory and all subdirs against list
        of remote subdirs, to see if it exists remotely or
        if we should delete all files in it or any subdirs.
        if we fail to remove a file we will log it to the 
        status file for the local filesystem (if one is being kept).
        args:
        dirName: name of directory, relative to self.localDir
        depth: blah"""

        if not os.path.isdir(os.path.join(self.localDir,dirName)):
            return

#        if self.dryrun or self.verbose:
#            sys.stderr.write("checking dir %s with depth %s and maxdepth %s\n" % (dirName, depth, self.dirMaxDepth))

        if not self.appendOsSepToDirName(dirName) in self.remoteDirDict: # not on remote server? delete the contents then
            if self.dryrun:
                sys.stderr.write("would recursively delete contents of %s\n" % dirName)
            elif self.verbose:
                sys.stderr.write("going to recursively delete contents of %s\n" % dirName)
            self.doRecursiveFileDeletes(dirName)
            return

        depth +=1
        if depth > self.dirMaxDepth:
            return

        for item in os.listdir(os.path.join(self.localDir, dirName)):
            self.cleanupDirAndSubdirs(os.path.join(dirName, item), depth)

    def appendOsSepToDirName(self, dirName):
        """If the dirname doesn't end in os.sep, tack it on
        args: name of the dir
        returns: name of dir ending in os.sep"""
        if len(dirName) == 0:
            dirName = os.sep
        elif not dirName.endswith(os.sep):
            dirName = dirName + os.sep
        return dirName
        
    def doRecursiveFileDeletes(self, dirName):
        """delete all files in dir or any subdir in tree
        symlinks or other items are left alone, *only* files are removed
        args:
        dirName: name of dir, relative to self.localDir
        returns: nothing"""
        # dirName must be rel to self.localDir
        for item in os.listdir(os.path.join(self.localDir, dirName)):
            fullPath = os.path.join(self.localDir,dirName,item)
            if os.path.isdir(fullPath):
                self.doRecursiveFileDeletes(os.path.join(dirName, item))
            elif os.path.isfile(fullPath):
                if self.dryrun:
                    sys.stderr.write("would remove %s from %s\n" % (item, os.path.join(self.localDir, dirName)))
                else:
                    if self.verbose:
                        sys.stderr.write("removing %s from %s\n" % (item, os.path.join(self.localDir, dirName)))
                    try:
                        os.unlink(fullPath)
                    except:
                        # don't bail, just complain
                        sys.stderr.write("failed to remove %s from %s\n" % (item, os.path.join(self.localDir, dirName)))
                        # since the delete failed we still have the file, write to 'current status' list
                        if self.updfd:
                            metaData = self.getFileMetaData(os.path.join(self.localDir, dirName, item))
                            self.updfd.write("%s\t%s\t%s\n" % (item,  metaData, self.appendOsSepToDirName(dirName)))
            # if it's not a file or a dir, leave it alone

    def setupRemoteDirDict(self):
        """read list of the remote server's media subdirectories
        into a dict, adding the parent directories to the dict as well;
        directory names are expected to be relative to self.uploadDir.
        args: none
        returns: nothing"""
        if not self.remoteSubdirsFile:
            return
        if self.remoteSubdirsFile.endswith(".gz"):
            fd = gzip.open(self.remoteSubdirsFile,"rb")
        else:
            fd = open(self.remoteSubdirsFile, "rb")
        self.remoteDirDict = {}
        while True:
            depth = 0
            # expect lines to look like dir/subd1/subd2/.../filename or
            # dir/subd1/.../subdn/
            line = fd.readline()
            if not line:
                break
            dirName = line.rstrip()
            if not dirName or dirName[0] == '#':
                continue
            self.remoteDirDict[dirName] = 1
            # add all the parent dirs of the remote dir to the list
            count = dirName.count(os.sep) - 1
            if count > self.dirMaxDepth:
                self.dirMaxDepth = count
            path = dirName.rstrip(os.sep)
            while path:
                ind = path.rfind(os.sep)
                if ind ==-1:
                    break
                path = path[:ind+1] # include the path sep at the end
                if path in self.remoteDirDict: # already added this and its parents
                    break
                self.remoteDirDict[path] = 1
                path = path[:-1] # drop off the path sep from the end for the next round

    def deleteContentsOfExtraDirs(self):
        """starting with the top level media directory (self.localDir), 
        check subdirectories to a level of self.maxDepth (determined
        by the depth of subdirectories in the remote subdirs file list)
        removing files from or below any directory which is not listed 
        in remoteSubdirsFile.
        example: if directories of the form a/bc are listed in remoteSubdirsFile
        but not a/bc/def, then local directories of the form a, a/bc will be
        checked and contents removed if they aren't in the list, however a
        local directory of the form a/bc/de wil not be checked (assuming that
        a/bc exists remotely).
        we do ths instead of of scanning the whole disk; we don't want
        to scan millions of files for commons when we know there's only two 
        levels of subdirs in the wmf media directory layout per wiki and we could stop
        as soon as we read and recursively delete as needed those two levels
        of subdirs
        FIXME that explanation was atrocious. make it suck less.
        args: none
        returns: none"""
        # FIXME someplace we must make clear that top level stuff in top level dir
        # doesnt' get deleted, only in subdirs if the subdirs are not meant to be kept
        self.setupRemoteDirDict()
        depth = 0
        for item in os.listdir(self.localDir):
            self.cleanupDirAndSubdirs(item, 0)

    def doDeletes(self, dirname):
        """delete media in the dict of media to delete
        args:
        dirname: subdir on which to operate
        returns: nothing"""
        for f in os.listdir(os.path.join(self.localDir,dirname)):
            if f in self.mediaToDelete[dirname]:
                if not self.mediaToDelete[dirname][f]:
                    continue
                if self.dryrun:
                    sys.stderr.write("would remove %s from %s\n" % ( f, dirname ))
                else:
                    try:
                        os.unlink(os.path.join(self.localDir, dirname, f))
                    except:
                        # don't bail, just complain
                        sys.stderr.write("failed to remove %s from %s\n" % (dirname, f))
                        # since the delete failed we still have the file, write to 'current status' list
                        if self.updfd:
                            self.updfd.write( "%s\t%s\t%s\n" % (f,  self.mediaToDelete[dirname][f], dirname))

    def doUpdatesAndCreates(self, dirname):
        """retrieve all media set for update in the given directory, checking
        first to see if we have a copy of each file on the local filesystem that is current
        and if not, retrieving and storing it
        args:
        dirname: subdir containing media
        returns: nothing"""
        for f in self.newMediaToCheck[dirname]:
            metaData = None
            try:
                metaData = self.getFileMetaData(os.path.join(self.localDir, dirname, f))
            except:
                # local file may not exist, we don't care, we'll just retrieve it
                pass
            if not metaData or metaData != self.newMediaToCheck[dirname][f]:
                if self.dryrun:
                    sys.stderr.write("would retrieve ")
                elif self.verbose:
                    sys.stderr.write("retrieving ")
                if self.verbose or self.dryrun:
                    if not metaData:
                        sys.stderr.write("%s, no metadata available\n" % f)
                    else:
                        sys.stderr.write("%s metadata:%s vs %s\n" % (f, metaData, self.newMediaToCheck[dirname][f]))
                if not self.dryrun:
                    self.retrieveAndStoreFile(dirname, f)
            # file won't be changed, so write it to 'current status' list
            elif self.updfd:
                self.updfd.write( "%s\t%s\t%s\n" % (f, metaData, dirname))

    def getFileMetaData(self, filename):
        """get sha1 as a string of hex digits, or the size
        of the specified file, depending on whether we are comparing
        sha1 or filesize to determine if files need to be updated
        args:
        filename: full name of the file for which to check size or sha1
        returns: either the sha1 in hex or the size of the file, as a string """
        if self.sha1Check:
            fd = None
            try:
                sha = hashlib.sha1()
                fd = open(filename)
                while True:
                    data = fd.read(128000000)
                    if not data:
                        break
                    sha.update(data)
                    if len(data) < 128000000:
                        break
                if fd:
                    fd.close()
            except:
                sys.stderr.write("failed to get sha1 of file %s\n" % filename)
                return "0"
            return self.base36encode(int(sha.hexdigest(),16)).zfill(31)
        else:
            return str(os.path.getsize(filename))

    # this code direct from wikipedia :-P
    def base36encode(self, number):
        """Converts an integer to a base36 string.
        args: 
        number: integer to be converted
        returns: base36 string"""
        if number >= 0 and number <= 9:
            return self.alphabet[number]
        base36 = ''
        while number != 0:
            number, i = divmod(number, len(self.alphabet))
            base36 = self.alphabet[i] + base36
        return base36

    def getFileViaHTTP(self, url, path):
        """retrieve media file from remote server and store it locally
        arguments:
        url: web path on remote server to media file to retrieve
        path: full path on local filesystem where media file will be stored
        returns: True on success, False on error
        raises exception on failure to establish http conn to server"""
        if not self.httpConn:
            try:
                self.httpConn = httplib.HTTPConnection(self.host, timeout = 20)
            except:
                sys.stderr.write("failed to establish http connection to remote server %s\n" % self.host)
                self.closeUpdatesFile()
                raise
        self.httpConn.request("GET", url, headers={ "User-Agent" : "httpsync-media.py/0.0 (WMF media syncer)" })
        # if it's not 200, whine and move on

        response = self.httpConn.getresponse()
        if response.status != 200:
            sys.stderr.write("failed to retrieve file %s with response code %s (%s)\n" %( path, response.status, response.reason ))
            return False

        outfd = open( path, "wb")
        while True:
            data = response.read([1048576])
            if not data:
                break
            outfd.write(data)
        outfd.close()
        return True

    def retrieveAndStoreFile(self, subdir, filename):
        """given subdir and media filename, retrieve and store the file
        arguments:
        subdir:  subdir in which media file is located
        filename: media filename
        returns: nothing"""
        url = '/'.join([ self.uploadDir.rstrip('/'),  subdir.strip('/') , filename.strip('/')])
        fullDir = os.path.join(self.localDir, subdir)
        if not os.path.isdir(fullDir):
            os.makedirs(fullDir)
        path = os.path.join(fullDir, filename)
        result = self.getFileViaHTTP(url, path)
        if result and self.updfd:
            # successful retrieval of file, so write it to 'current status' list
            self.updfd.write( "%s\t%s\t%s\n" % (filename,  self.newMediaToCheck[subdir][filename], subdir))
        
def usage(message = None):
    """write specified error message, if any, to stderr,
    display usage message, and exit"""
    if message:
        sys.stderr.write("%s\n" % message)
        sys.stderr.write("Usage: python httpsync-media.py --host hostname --uploadurl url --localdir dirname\n")
        sys.stderr.write("                                --newmedialist filename [--fieldnum num]\n")
        sys.stderr.write("                                [--oldmedialist filename] [--updatedmediafile filename]\n")
        sys.stderr.write("                                [--subdirlist filename] [--sha1] [--dryrun] [--verbose]\n")
        sys.stderr.write("\n")
        sys.stderr.write("This script reads two list of remote media files with timestamps of upload, compares\n")
        sys.stderr.write("the newer one against the older one for changes, and applies these changes to the media\n")
        sys.stderr.write("on the local filesystem.\n")
        sys.stderr.write("\n")
        sys.stderr.write("The lists of media should contain the filename and either the size or the sha1 of the\n")
        sys.stderr.write("file, which is used to determine whether the local copy, if it exists, is current.\n")
        sys.stderr.write("By default the size is expected and used.\n")
        sys.stderr.write("One can think of this as a weak rsync over http, without preserving permissions and times.\n")
        sys.stderr.write("\n")
        sys.stderr.write("Note that deletions are done on a per subdirectory basis, with the assumption that all\n")
        sys.stderr.write("files that should be kept are in the new media list.  If you plan to httpsync a large\n")
        sys.stderr.write("number of files, large enough that you need to split the list into multiple parts and\n")
        sys.stderr.write("run each separately, make sure that all files belonging to the same subdirectory are\n")
        sys.stderr.write("included in the same part, otherwise files that should be kept may be deleted.\n")
        sys.stderr.write("Directories are not deleted, only files.\n")
        sys.stderr.write("\n")
        sys.stderr.write("--host:             name of host from which to retrieve files\n")
        sys.stderr.write("--uploadurl:        url (starting with /) to the directory where media for the given\n")
        sys.stderr.write("                    wiki can be retrieved\n")
        sys.stderr.write("--localdir:         path to the local directory where media files for the wiki are stored\n")
        sys.stderr.write("--newmedialist:     file containing a list of all media for the given wiki with media\n")
        sys.stderr.write("                    filename, sha1 and/or size, and relative dirname separated by tabs\n")
        sys.stderr.write("--oldmedialist:     file containing older list of all media for the given wiki with media\n")
        sys.stderr.write("                    filename, sha1 and/or size, and relative dirname separated by tabs\n")
        sys.stderr.write("                    the local media dir should be known to be as recent as this list\n")
        sys.stderr.write("                    because only differences between the old and new list will be processed\n")
        sys.stderr.write("                    If no old list is provided, all files in the newmedialist and the local\n")
        sys.stderr.write("                    filesystem will be checked.\n")
        sys.stderr.write("--updatedmediafile: write out a media list that reflects the current state of the local\n")
        sys.stderr.write("                    filesystem after retrievals and updates, taking any errors into account\n")
        sys.stderr.write("                    if value is '-' then list will be written to stdout; if value ends in '.gz'\n")
        sys.stderr.write("                    then contents will be gzipped\n")
        sys.stderr.write("                    each line in this list will have te format: filename<tab>size or sha1<tab>subdir")
        sys.stderr.write("--subdirlist:       list of all subdirs relative to the uploadurl; contents of any subdir on the local\n")
        sys.stderr.write("                    filesystem not in this list will be deleted; if this option is not specified,\n")
        sys.stderr.write("                    content in local dirs which don't exist remotely will be kept\n")
        sys.stderr.write("--sha1:             use sha1 to check media files; if this option is omitted, size will be used\n")
        sys.stderr.write("                    the sha1 string in the media file list should be in base36\n")
        sys.stderr.write("--fieldnum:         number of the tab-separated field in the media list that has either the\n")
        sys.stderr.write("                    size or the sha1, where the 0th field is the filename; default 1\n")
        sys.stderr.write("--dryrun:           don't delete or retrieve media files, print information about each\n")
        sys.stderr.write("                    file that would be deleted or retrieved\n")
        sys.stderr.write("--verbose:          print information about each file retrieved\n")
        sys.exit(1)

if __name__ == "__main__":
    host = None              # hostname of the server from which we will retrieve media files
    uploadUrl = None         # url, starting with '/', to the directory on the server which contains media files
    localDir = None          # path to local directory where media files will be written
    newMediaFile = None      # current list of media files, which will all be retrieved if not on the local filesystem 
    oldMediaFile = None      # old list of media files, which have all been retrieved and stored locally
    updatedMediaFile = None  # where to write an updated list of media and sizes/sha1, if any; '-' means stdout
    remoteSubdirsFile = None # full list of subdirectories relative to uploadUrl (in case someone splis a remote file list
    #                          across multiple httpsyncs, we won't know which local dirs we can delete without this list)
    sha1Check = False        # whether or not to compare sha1 of media file on disk and on server to decide if file is different
    fieldNum = 1             # which field in the tab-separated list of media has the size or sha1 of the file
    dryrun = False
    verbose = False
    
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "host=", "uploadurl=", "localdir=", "newmedialist=", 
                                                                     "oldmedialist=", "fieldnum=", "updatedmediafile=", "subdirlist=", "sha1", "dryrun", "verbose" ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--host":
            host = val
        elif opt == "--uploadurl":
            uploadUrl = val
        elif opt == "--localdir":
            localDir = val
        elif opt == "--newmedialist":
            newMediaFile = val
        elif opt == "--oldmedialist":
            oldMediaFile = val
        elif opt == "--updatedmediafile":
            updatedMediaFile = val
        elif opt == "--subdirlist":
            remoteSubdirsFile = val
        elif opt == "--sha1":
            sha1Check = True
        elif opt == "--fieldnum":
            if not val.isdigit():
                usage("fieldnum must be a potisive number")
            fieldNum = int(val)
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not ( host and uploadUrl and localDir and newMediaFile ):
        usage("One or more mandatory options missing")

    if dryrun and updatedMediaFile:
        sys.stderr.write("can't write updated media list with dryrun, disabling\n")
        updatedMediaFile = None

    hs = HttpSync(host, uploadUrl, localDir, newMediaFile, oldMediaFile, remoteSubdirsFile, updatedMediaFile, sha1Check, fieldNum, dryrun, verbose)
    hs.loadMediaLists()
    hs.doSync()
