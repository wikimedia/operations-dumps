# Worker process, does the actual dumping

import hashlib, os, re, sys
import time
import signal
import traceback

from os.path import exists
from dumps.WikiDump import MiscUtils
from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError


class DumpFilename(object):
    """
    filename without directory name, and the methods that go with it,
    primarily for filenames that follow the standard naming convention, i.e.
    projectname-date-dumpName.sql/xml.gz/bz2/7z (possibly with a chunk
    number, possibly with start/end page id information embedded in the name).

    Constructor:
    DumpFilename(dumpName, date = None, filetype, ext, chunk = None, checkpoint = None, temp = False) -- pass in dumpName and
                                  filetype/extension at least. filetype is one of xml sql, extension is one of
                                      bz2/gz/7z.  Or you can pass in the entire string without project name and date,
                      e.g. pages-meta-history5.xml.bz2
                      If dumpName is not passed, no member variables will be initialized, and
                      the caller is expected to invoke newFromFilename as an alternate
                      constructor before doing anything else with the object.

    newFromFilename(filename)  -- pass in full filename. This is called by the regular constructor and is
                                      what sets all attributes

    attributes:

    isCheckpointFile  filename of form dbname-date-dumpName-pxxxxpxxxx.xml.bz2
    isChunkFile       filename of form dbname-date-dumpNamex.xml.gz/bz2/7z
    isTempFile        filename of form dbname-date-dumpName.xml.gz/bz2/7z-tmp
    firstPageID       for checkpoint files, taken from value in filename
    lastPageID        for checkpoint files, value taken from filename
    filename          full filename
    basename          part of the filename after the project name and date (for
                          "enwiki-20110722-pages-meta-history12.xml.bz2" this would be
                          "pages-meta-history12.xml.bz2")
    fileExt           extension (everything after the last ".") of the file
    date              date embedded in filename
    dumpName          dump name embedded in filename (eg "pages-meta-history"), if any
    chunk             chunk number of file as string (for "pages-meta-history5.xml.bz2" this would be "5")
    chinkInt          chunk number as int
    """

    def __init__(self, wiki, date = None, dumpName = None, filetype = None, ext = None, chunk = None, checkpoint = None, temp = False):
        """Constructor.  Arguments: the dump name as it should appear in the filename,
        the date if different than the date of the dump run, the chunk number
        if there is one, and temp which is true if this is a temp file (ending in "-tmp")
        Alternatively, one can leave off all other other stuff and just pass the entire
        filename minus the dbname and the date. Returns true on success, false otherwise.."""
        self.wiki = wiki
        # if dumpName is not set, the caller can call newFromFilename to initialize various values instead
        if dumpName:
            filename =  self.newFilename(dumpName, filetype, ext, date, chunk, checkpoint, temp)
            self.newFromFilename(filename)

    def isExt(self,ext):
        if ext == "gz" or ext == "bz2" or ext == "7z" or ext == "html" or ext == "txt":
            return True
        else:
            return False

    # returns True if successful, False otherwise (filename is not in the canonical form that we manage)
    def newFromFilename(self, filename):
        """Constructor.  Arguments: the full file name including the chunk, the extension, etc BUT NOT the dir name. """
        self.filename = filename

        self.dbName = None
        self.date = None
        self.dumpName = None

        self.basename = None
        self.fileExt = None
        self.fileType = None

        self.filePrefix = ""
        self.filePrefixLength = 0

        self.isChunkFile = False
        self.chunk = None
        self.chunkInt = 0

        self.isCheckpointFile = False
        self.checkpoint = None
        self.firstPageID = None
        self.lastPageID = None

        self.isTempFile = False
        self.temp = None

        # example filenames:
        # elwikidb-20110729-all-titles-in-ns0.gz
        # elwikidb-20110729-abstract.xml
        # elwikidb-20110727-pages-meta-history2.xml-p000048534p000051561.bz2

        # we need to handle cases without the projectname-date stuff in them too, as this gets used
        # for all files now
        if self.filename.endswith("-tmp"):
            self.isTempFile = True
            self.temp = "-tmp"

        if ('.' in self.filename):
            (fileBase, self.fileExt) = self.filename.rsplit('.',1)
            if (self.temp):
                self.fileExt = self.fileExt[:-4];
        else:
            return False

        if not self.isExt(self.fileExt):
            self.fileType = self.fileExt
#            self.fileExt = None
            self.fileExt = ""
        else:
            if '.' in fileBase:
                (fileBase, self.fileType) = fileBase.split('.',1)

        # some files are not of this form, we skip them
        if not '-' in fileBase:
            return False

        (self.dbName, self.date, self.dumpName) = fileBase.split('-',2)
        if not self.date or not self.dumpName:
            self.dumpName = fileBase
        else:
            self.filePrefix = "%s-%s-" % (self.dbName, self.date)
            self.filePrefixLength = len(self.filePrefix)

        if self.filename.startswith(self.filePrefix):
            self.basename = self.filename[self.filePrefixLength:]

        self.checkpointPattern = "-p(?P<first>[0-9]+)p(?P<last>[0-9]+)\." + self.fileExt + "$"
        self.compiledCheckpointPattern = re.compile(self.checkpointPattern)
        result = self.compiledCheckpointPattern.search(self.filename)

        if result:
            self.isCheckpointFile = True
            self.firstPageID = result.group('first')
            self.lastPageID = result.group('last')
            self.checkpoint = "p" + self.firstPageID + "p" + self.lastPageID
            if self.fileType and self.fileType.endswith("-" + self.checkpoint):
                self.fileType = self.fileType[:-1 * (len(self.checkpoint) + 1)]

        self.chunkPattern = "(?P<chunk>[0-9]+)$"
        self.compiledChunkPattern = re.compile(self.chunkPattern)
        result = self.compiledChunkPattern.search(self.dumpName)
        if result:
            self.isChunkFile = True
            self.chunk = result.group('chunk')
            self.chunkInt = int(self.chunk)
            # the dumpName has the chunk in it so lose it
            self.dumpName = self.dumpName.rstrip('0123456789')

        return True

    def newFilename(self, dumpName, filetype, ext, date = None, chunk = None, checkpoint = None, temp = None):
        if not chunk:
            chunk = ""
        if not date:
            date = self.wiki.date
        # fixme do the right thing in case no filetype or no ext
        parts = []
        parts.append(self.wiki.dbName + "-" + date + "-" + dumpName + "%s" % chunk)
        if checkpoint:
            filetype = filetype + "-" + checkpoint
        if filetype:
            parts.append(filetype)
        if ext:
            parts.append(ext)
        filename = ".".join(parts)
        if temp:
            filename = filename + "-tmp"
        return filename

class DumpFile(file):
    """File containing output created by any job of a jump run.  This includes
    any file that follows the standard naming convention, i.e.
    projectname-date-dumpName.sql/xml.gz/bz2/7z (possibly with a chunk
    number, possibly with start/end page id information embedded in the name).

    Methods:

    md5Sum(): return md5sum of the file contents.
    checkIfTruncated(): for compressed files, check if the file is truncated (stops
       abruptly before the end of the compressed data) or not, and set and return
         self.isTruncated accordingly.  This is fast for bzip2 files
       and slow for gz and 7z fles, since for the latter two types it must serially
       read through the file to determine if it is truncated or not.
    getSize(): returns the current size of the file in bytes
    rename(newname): rename the file. Arguments: the new name of the file without
       the directory.
    findFirstPageIDInFile(): set self.firstPageID by examining the file contents,
       returning the value, or None if there is no pageID.  We uncompress the file
       if needed and look through the first 500 lines.

    plus the usual file methods (read, write, open, close)

    useful variables:

    firstPageID       Determined by examining the first few hundred lines of the contents,
                          looking for page and id tags, wihout other tags in between. (hmm)
    filename          full filename with directory
    """
    def __init__(self, wiki, filename, fileObj = None, verbose = False):
        """takes full filename including path"""
        self._wiki = wiki
        self.filename = filename
        self.firstLines = None
        self.isTruncated = None
        self.firstPageID = None
        self.dirname = os.path.dirname(filename)
        if fileObj:
            self.fileObj = fileObj
        else:
            self.fileObj = DumpFilename(wiki)
            self.fileObj.newFromFilename(os.path.basename(filename))

    def md5Sum(self):
        if not self.filename:
            return None
        summer = hashlib.md5()
        infile = file(self.filename, "rb")
        bufsize = 4192 * 32
        buffer = infile.read(bufsize)
        while buffer:
            summer.update(buffer)
            buffer = infile.read(bufsize)
        infile.close()
        return summer.hexdigest()

    def getFirst500Lines(self):
        if self.firstLines:
            return(self.firstLines)

        if not self.filename or not exists(self.filename):
            return None

        pipeline = self.setupUncompressionCommand()

        if (not exists(self._wiki.config.head)):
            raise BackupError("head command %s not found" % self._wiki.config.head)
        head = self._wiki.config.head
        headEsc = MiscUtils.shellEscape(head)
        pipeline.append([head, "-500"])
        # without shell
        p = CommandPipeline(pipeline, quiet=True)
        p.runPipelineAndGetOutput()
        if p.exitedSuccessfully() or p.getFailedCommandsWithExitValue() == [[-signal.SIGPIPE, pipeline[0]]] or p.getFailedCommandsWithExitValue() == [[signal.SIGPIPE + 128, pipeline[0]]]:
            self.firstLines = p.output()
        return(self.firstLines)

    # unused
    # xml, sql, text
    def determineFileContentsType(self):
        output = self.getFirst500Lines()
        if (output):
            pageData = output
            if (pageData.startswith('<mediawiki')):
                return('xml')
            if (pageData.startswith('-- MySQL dump')):
                return('sql')
            return('txt')
        return(None)

    def setupUncompressionCommand(self):
        if not self.filename or not exists(self.filename):
            return None
        pipeline = []
        if self.fileObj.fileExt == 'bz2':
            command = [self._wiki.config.bzip2, '-dc']
        elif self.fileObj.fileExt == 'gz':
            command = [self._wiki.config.gzip, '-dc']
        elif self.fileObj.fileExt == '7z':
            command = [self._wiki.config.sevenzip, "e", "-so"]
        else:
            command = [self._wiki.config.cat]

        if (not exists(command[0])):
            raise BackupError("command %s to uncompress/read file not found" % command[0])
        command.append(self.filename)
        pipeline.append(command)
        return(pipeline)

    # unused
    # return its first and last page ids from name or from contents, depending
    # return its date

    # fixme what happens if this is not an xml dump? errr. must detect and bail immediately?
    # maybe instead of all that we should just open the file ourselves, read a few lines... oh.
    # right. stupid compressed files. um.... do we have stream wrappers? no. this is python
    # what's the easy was to read *some* compressed data into a buffer?
    def findFirstPageIDInFile(self):
        if (self.firstPageID):
            return(self.firstPageID)
        output = self.getFirst500Lines()
        if (output):
            pageData = output
            titleAndIDPattern = re.compile('<title>(?P<title>.+?)</title>\s*' + '(<ns>[0-9]+</ns>\s*)?' + '<id>(?P<pageid>\d+?)</id>')
            result = titleAndIDPattern.search(pageData)
            if (result):
                self.firstPageID = result.group('pageid')
        return(self.firstPageID)

    def checkIfTruncated(self):
        if self.isTruncated:
            return self.isTruncated

        # Setting up the pipeline depending on the file extension
        if self.fileObj.fileExt == "bz2":
            if (not exists(self._wiki.config.checkforbz2footer)):
                raise BackupError("checkforbz2footer command %s not found" % self._wiki.config.checkforbz2footer)
            checkforbz2footer = self._wiki.config.checkforbz2footer
            pipeline = []
            pipeline.append([checkforbz2footer, self.filename])
        else:
            if self.fileObj.fileExt == 'gz':
                pipeline = [[self._wiki.config.gzip, "-dc", self.filename, ">", "/dev/null"]]
            elif self.fileObj.fileExt == '7z':
                # Note that 7z does return 0, if archive contains
                # garbage /after/ the archive end
                pipeline = [[self._wiki.config.sevenzip, "e", "-so", self.filename, ">", "/dev/null"]]
            else:
                # we do't know how to handle this type of file.
                return self.isTruncated

        # Run the perpared pipeline
        p = CommandPipeline(pipeline, quiet=True)
        p.runPipelineAndGetOutput()
        self.isTruncated = not p.exitedSuccessfully()

        return self.isTruncated

    def getSize(self):
        if (exists(self.filename)):
            return os.path.getsize(self.filename)
        else:
            return None

    def rename(self, newname):
        try:
            os.rename(self.filename, os.path.join(self.dirname,newname))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise BackupError("failed to rename file %s" % self.filename)

        self.filename = os.path.join(self.dirname,newname)


class DumpDir(object):
    def __init__(self, wiki, dbName):
        self._wiki = wiki
        self._dbName = dbName
        self._dirCache = {}
        self._dirCacheTime = {}
        self._chunkFileCache = {}
        self._checkpointFileCache = {}

    def filenamePrivatePath(self, dumpFile, dateString = None):
        """Given a DumpFilename object, produce the full path to the filename in the date subdir
        of the the private dump dir for the selected database.
        If a different date is specified, use that instead"""
        if (not dateString):
            dateString = self._wiki.date
        return os.path.join(self._wiki.privateDir(), dateString, dumpFile.filename)

    def filenamePublicPath(self, dumpFile, dateString = None):
        """Given a DumpFilename object produce the full path to the filename in the date subdir
        of the public dump dir for the selected database.
        If this database is marked as private, use the private dir instead.
        If a different date is specified, use that instead"""
        if (not dateString):
            dateString = self._wiki.date
        return os.path.join(self._wiki.publicDir(), dateString, dumpFile.filename)

    def latestDir(self):
        """Return 'latest' directory for the current project being dumped, e.g.
        if the current project is enwiki, this would return something like
        /mnt/data/xmldatadumps/public/enwiki/latest (if the directory /mnt/data/xmldatadumps/public
        is the path to the directory for public dumps)."""
        return os.path.join(self._wiki.publicDir(), "latest")

    def webPath(self, dumpFile, dateString = None):
        """Given a DumpFilename object produce the full url to the filename for the date of
        the dump for the selected database."""
        if (not dateString):
            dateString = self._wiki.date
        return os.path.join(self._wiki.webDir(), dateString, dumpFile.filename)


    def webPathRelative(self, dumpFile, dateString = None):
        """Given a DumpFilename object produce the url relative to the docroot for the filename for the date of
        the dump for the selected database."""
        if (not dateString):
            dateString = self._wiki.date
        return os.path.join(self._wiki.webDirRelative(), dateString, dumpFile.filename)

    def dirCacheOutdated(self, date):
        if not date:
            date = self._wiki.date
        directory = os.path.join(self._wiki.publicDir(), date)
        if exists(directory):
            dirTimeStamp = os.stat(directory).st_mtime
            if (not date in self._dirCache or dirTimeStamp > self._dirCacheTime[date]):
                return True
            else:
                return False
        else:
            return True

    # warning: date can also be "latest"
    def getFilesInDir(self, date = None):
        if not date:
            date = self._wiki.date
        if (self.dirCacheOutdated(date)):
            directory = os.path.join(self._wiki.publicDir(),date)
            if exists(directory):
                dirTimeStamp = os.stat(directory).st_mtime
                files = os.listdir(directory)
                fileObjs = []
                for f in files:
                    fileObj = DumpFilename(self._wiki)
                    fileObj.newFromFilename(f)
                    fileObjs.append(fileObj)
                self._dirCache[date] = fileObjs
                # The directory listing should get cached. However, some tyical file
                # system's (eg. ext2, ext3) mtime's resolution is 1s. If we would
                # unconditionally cache, it might happen that we cache at x.1 seconds
                # (with mtime x). If a new file is added to the filesystem at x.2,
                # the directory's mtime would still be set to x. Hence we would not
                # detect that the cache needs to be purged. Therefore, we cache only,
                # if adding a file now would yield a /different/ mtime.
                if time.time() >= dirTimeStamp + 1:
                    self._dirCacheTime[date] = dirTimeStamp
                else:
                    # By setting _dirCacheTime to 0, we provoke an outdated cache
                    # on the next check. Hence, we effectively do not cache.
                    self._dirCacheTime[date] = 0
            else:
                self._dirCache[date] = []
        return(self._dirCache[date])

    # list all files that exist, filtering by the given args.
    # if we get None for an arg then we accept all values for that arg in the filename, including missing
    # if we get False for an arg (chunk, temp, checkpoint), we reject any filename which contains a value for that arg
    # if we get True for an arg (chunk, temp, checkpoint), we include only filenames which contain a value for that arg
    # chunks should be a list of value(s) or True / False / None
    #
    # note that we ignore files with ".truncated". these are known to be bad.
    def _getFilesFiltered(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = None, temp = None, checkpoint = None):
        if not date:
            date = self._wiki.date
        fileObjs = self.getFilesInDir(date)
        filesMatched = []
        for f in fileObjs:
            # fixme this is a bit hackish
            if f.filename.endswith("truncated"):
                continue

            if dumpName and f.dumpName != dumpName:
                continue
            if fileType != None and f.fileType != fileType:
                continue
            if fileExt != None and f.fileExt != fileExt:
                continue
            if (chunks == False and f.isChunkFile):
                continue
            if (chunks == True and not f.isChunkFile):
                continue
            # chunks is a list...
            if (chunks and chunks != True and not f.chunkInt in chunks):
                continue
            if (temp == False and f.isTempFile) or (temp and not f.isTempFile):
                continue
            if (checkpoint == False and f.isCheckpointFile) or (checkpoint and not f.isCheckpointFile):
                continue
            filesMatched.append(f)
            self.sort_fileobjs(filesMatched)
        return filesMatched

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_fileobjs(self, l):
        """ Sort the given list in the way that humans expect.
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key.filename)]
        l.sort(key=alphanum_key)

    # list all checkpoint files that exist, filtering by the given args.
    # if we get None for an arg then we accept all values for that arg in the filename
    # if we get False for an arg (chunks, temp), we reject any filename which contains a value for that arg
    # if we get True for an arg (chunk, temp), we accept only filenames which contain a value for the arg
    # chunks should be a list of value(s), or True / False / None
    def getCheckpointFilesExisting(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = False, temp = False):
        return self._getFilesFiltered(date, dumpName, fileType, fileExt, chunks, temp, checkpoint = True)

    # list all non-checkpoint files that exist, filtering by the given args.
    # if we get None for an arg then we accept all values for that arg in the filename
    # if we get False for an arg (chunk, temp), we reject any filename which contains a value for that arg
    # if we get True for an arg (chunk, temp), we accept only filenames which contain a value for the arg
    # chunks should be a list of value(s), or True / False / None
    def getRegularFilesExisting(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = False, temp = False):
        return self._getFilesFiltered(date, dumpName, fileType, fileExt, chunks, temp, checkpoint = False)

