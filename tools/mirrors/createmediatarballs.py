import os, re, sys, time, hashlib, urllib, httplib, getopt, gzip, subprocess
from subprocess import Popen, PIPE

class MediaPath(object):
    def __init__(self, inputDir, hashLevels):
        # we assume that the media files are kept in the usual 
        # MediaWiki hash dir structure; a two level hash
        # would have, for example,
        # ...images/a/a4/My_Cool_File....jpg
        self.inputDir = inputDir # relative path to where media is uploaded for a given wiki, e.g. wiktionary/fr
        self.hashLevels = hashLevels # number of levels of hash directories derived from media filename, for wmf this is 2

    # convert media filenames (with _ instead of spaces) to 
    # full path with hash
    def getHashPathForLevel(self, mediaFileName):
        if self.hashLevels == 0:
            return ''
        else:
            summer = hashlib.md5()
            summer.update( mediaFileName )
            md5Hash = summer.hexdigest()        
            path = ''
        for i in range( 1,self.hashLevels+1 ):
            path = path + md5Hash[0:i] + os.sep
        return path

    def getMediaFilePath(self, mediaFileName):
        return os.path.join(self.inputDir, self.getHashPathForLevel(mediaFileName), mediaFileName)

class Tarball(object):
    def __init__(self, baseDir, listInputDir, uploadDir, outputDir, listType, listFileNameFormat, tarballNameFormat, wiki, date, hashLevel, numFilesPerTarball, tarName, overwrite, verbose):
        self.baseDir = baseDir # path to dir with uploaded media for all wikis, e.g. /export/uploads
        self.listInputDir = listInputDir # path to dir with lists of local/remote media
        self.uploadDir = uploadDir # rel path to wiki's upload dir, eg. wikipedia/en
        self.outputDir = outputDir # path to dir where we will write the tarballs
        self.listType = listType # "local" (write locally uploaded media) or "remote" (write media uploaded to remote repo)
        self.listFileNameFormat = listFileNameFormat # e.g. '{w}-{d}-{t}-lists.gz'
        self.tarballNameFormat = outputFileNameFormat # e.g. '{w}-{d}-{t}-media.tar'
        self.wiki = wiki
        self.date = date # date as it appears in the filenames with lists of local/remote media, YYYYMMDD format
        self.hashLevel = hashLevel # basically we expect it to be 2 but it would work for other values
        self.numFilesPerTarball = numFilesPerTarball
        self.tarName = tarName
        self.overwrite = overwrite
        self.verbose = verbose

    def writeTarballs(self):

        # don't overwrite tarballs if we are told not to
        if not self.overwrite:
            firstTarballName = os.path.join(self.outputDir, self.tarballNameFormat.format(w = wiki, d = self.date, t = self.listType, n = 1))
            if os.path.exists(firstTarballName):
                # there are already (some) output files for this wiki and date. don't regenerate them.
                if self.verbose:
                    print "Skipping %s since tarballs for this wiki and date already exist" % self.wiki
                return

        # media file path is relative to basedir, we will cd to basedir
        # for the tar so the tarball filenames are reasonable
        listFileName = os.path.join(self.listInputDir, self.listFileNameFormat.format(w = self.wiki, d = self.date, t = self.listType))
        if not os.path.exists(listFileName):
            # could be a closed wiki, could be the remote file for commons (which 
            # we should not have generated), etc. warn and continue
            if self.verbose:
                print "Skipping %s since it does not exist" % listFileName
            return

        listfd = gzip.open(listFileName, "rb")

        mp = MediaPath(self.uploadDir, 2)

        filesToTar = []
        serial = 1
        fileCount = 0
        for line in listfd:
            fileCount += 1
            # format of these lines: mediafilename<tab>metadata
            filesToTar.append(mp.getMediaFilePath(line.split('\t',1)[0])+'\n')

            if fileCount >= self.numFilesPerTarball:
                outFileName = os.path.join(self.outputDir, self.tarballNameFormat.format(w = self.wiki, d = self.date, t = self.listType, n = serial))
                self.writeTarball('\n'.join(filesToTar) + '\n', outFileName)
                fileCount = 0
                serial += 1
                filesToTar = []
        if fileCount:
            # do the last batch
                outFileName = os.path.join(self.outputDir, self.tarballNameFormat.format(w = wiki, d = date, t = self.listType, n = serial))
                self.writeTarball('\n'.join(filesToTar)+'\n', outFileName)
        listfd.close()

    def writeTarball(self, inputToTar, tarballFileName):
        # if there are files that have been deleted in the meantime, tar will whine but continue
        # seriously? tar is option-order sensitive for -C?? bleep bleepers!
        command = [ self.tarName, "-C", self.baseDir, "-cpf", tarballFileName,  "-T", "-", "--no-unquote", "--ignore-failed-read" ]
        commandString = " ".join([ "'" + c + "'" for c in command ])
        if verbose:
            print "For wiki", wiki, "command:", commandString
        try:
            proc = Popen(command, stderr = PIPE, stdin = PIPE)
            output, error = proc.communicate(inputToTar) # no output, ignore it
            if proc.returncode:
                sys.stderr.write("command '%s failed with return code %s and error %s\n" % ( command, proc.returncode,  error ))
                sys.exit(1)
        except:
            sys.stderr.write("command %s failed\n" % command)
            raise
        if error:
            # log any file read perm or file missing errors we might have encountered
            sys.stderr.write("error from command %s: %s\n" % (command, error))

def usage(message = None):
    if message:
        sys.stderr.write("%s\n" % message)
        sys.stderr.write("Usage: python createmediatarballs.py --mediadir dirname --listsinputdir dirname\n")
	sys.stderr.write("                     --remoterepo reponame [--outputdir dirname] [--date YYYYMMDD]\n")
        sys.stderr.write("                     [--wikilist filename] [--inputnameformat format]\n")
        sys.stderr.write("                     [--outputnameformat format] [--filespertarball num]\n")
        sys.stderr.write("                     [--tar tarcmd] [--nooverwrite] [--verbose]\n")
        sys.stderr.write("\n")
        sys.stderr.write("This script reads lists of media files local to a project and hosted remotely, and\n")
	sys.stderr.write("produces tarballs of each in the specified output directory.\n")
        sys.stderr.write("\n")
	sys.stderr.write("--mediadir:        where to find the upload directories for each wiki\n")
	sys.stderr.write("--listsinputdir:   where to find the lists of local and remote files per project\n")
	sys.stderr.write("--remoterepo:      name of the remote repo as it appears in the wikilist file\n")
        sys.stderr.write("--outputdir:       where to put the tarballs\n")
        sys.stderr.write("                   default: current working directory\n")
	sys.stderr.write("--date:            date string that appears in the names of the lists; if not specifed,\n")
        sys.stderr.write("                   the date appearing in the local media list filename for each wiki\n")
        sys.stderr.write("                   will be used\n")
	sys.stderr.write("--wikilist:        file with names of wikis and corresponding upload dirs to process; if\n")
        sys.stderr.write("                   '-' is given, the list will be read from stdin\n")
	sys.stderr.write("                   default: 'uploaddirs.txt'\n")
	sys.stderr.write("--inputnameformat: format string from which, given the wiki name, the type (local\n")
	sys.stderr.write("                   or remote) and the date, the filenames of the lists of media files\n")
        sys.stderr.write("                   can be contructed by replacing {w} with the wiki name, {d} with\n")
        sys.stderr.write("                   the date string, and {t} by the type.\n")
	sys.stderr.write("                   default: {w}-{d}-{t}-wikiqueries.gz\n")
	sys.stderr.write("--outputnameformat:  format string from which, given the wiki name, the type (local\n")
	sys.stderr.write("                   or remote) and the date, the filename can be contructed\n")
	sys.stderr.write("                   by replacing {w} with the wiki name, {d} with the date string\n")
	sys.stderr.write("                   {t} by the type, and {n} for the sequence number, see\n")
        sys.stderr.write("                   filespertarball below\n")
	sys.stderr.write("                   default: {w}-{d}-{t}-wikiqueries-{n}.tar\n")
        sys.stderr.write("--filespertarball: each tarball name for a given project will be tagged with a\n")
        sys.stderr.write("                   sequence number starting with 1, with no more than this number\n")
        sys.stderr.write("                   of files in each\n")
        sys.stderr.write("                   default: 100,000\n")
        sys.stderr.write("--tar:             name of gnu tar command, default: 'tar'\n")
        sys.stderr.write("--nooverwrite:     do not overwrite existingtarballs for a given project and date; by default")
        sys.stderr.write("                   a new tarball will be created every time\n")
        sys.stderr.write("--verbose:         print lots of status messages\n")
        sys.exit(1)

def findMostRecentDate(listsInputDir, inputFileNameFormat, wiki):
    listFileName = "^" + inputFileNameFormat.format(w = wiki, d="(20[0-9]{6})", t = "local") + "$"
    fileNames = [ f for f in os.listdir(listsInputDir) if re.match(listFileName,f) ]
    if not fileNames:
        return None
    fileNames.sort()

    mostRecent = fileNames[-1]
    result = re.match(listFileName,mostRecent)
    mostRecentDate = result.group(1)
    return mostRecentDate

if __name__ == "__main__":
    mediaBaseDir = None
    listsInputDir = None
    outputDir = os.getcwd()
    date = None
    wikiListFile = "all.dblist"
    remoteRepoName = None
    inputFileNameFormat = "{w}-{d}-{t}-wikiqueries.gz"
    outputFileNameFormat = "{w}-{d}-{t}-wikiqueries-{n}.tar"
    filesPerTarball = 100000
    tar = "tar"
    overwrite = True
    verbose = False
    
#    dbListPath = os.path.join(os.getcwd(), dbList)

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "mediadir=", "listsinputdir=", "outputdir=", "date=", "wikilist=", "remoterepo=", "inputnameformat=", "outputnameformat=", "filespertarball=", "tar=", "nooverwrite", "verbose" ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--mediadir":
            mediaBaseDir = val
        elif opt == "--listsinputdir":
            listsInputDir = val
        elif opt == "--outputdir":
            outputDir = val
        elif opt == "--date":
            date = val
        elif opt == "--wikilist":
            wikiListFile = val
        elif opt == "--remoterepo":
            remoteRepoName = val
        elif opt == "--inputnameformat":
            inputFileNameFormat = val
        elif opt == "--outputnameformat":
            outputFileNameFormat = val
        elif opt == "--filespertarball":
            if not val.isdigit():
                usage("filespertarball requires a positive number")
            filesPerTarball = int(val)
        elif opt == "--tar":
            tar = val
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--verbose":
            verbose = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not ( mediaBaseDir and listsInputDir and outputDir and remoteRepoName ):
        usage("One or more mandatory options missing")
    if date and not re.match("^20[0-9]{6}$", date):
        usage("Date must be in the format YYYYMMDD (four digit year, two digit month, two digit date)")

    if wikiListFile == '-':
        wfd = sys.stdin
    else:
        wfd = open(wikiListFile, "r")

    wikiListTemp = [ l.strip() for l in wfd ]
    # toss blank lines, comments
    wikiList = [ l for l in wikiListTemp if l and l[0] != '#' ]

    if wfd != sys.stdin:
        wfd.close()

    # find the remote repo upload dir or bail
    #
    remoteUploadDir = None
    for l in wikiList:
        if l.startswith(remoteRepoName + "\t"):
            remoteWiki, remoteUploadDir = l.split('\t',1)
            break
    if not remoteUploadDir:
        sys.stderr.write("can't find remote repo %s in wiki list, can't determine remote upload dir\n" % remoteRepoName)
        sys.exit(1)

    for line in wikiList:

        # expect <wiki>\t<uploaddir>  (where uploaddir is a rel path to localMediaBaseDir)
        if not '\t' in line:
            sys.stderr.write("missing tab (field separator) in line from wikilist file: %s\n" % line)
            continue
        wiki, uploadDir = line.split('\t',1)
        if not wiki or not uploadDir:
            sys.stderr.write("missing field in line from wikilist file: %s\n" % line)
            continue
        if wiki == remoteRepoName:
            if verbose:
                print "Skipping remote wiki"
            continue

        if date:
            fileDate = date
        else:
            fileDate = findMostRecentDate(listsInputDir, inputFileNameFormat, wiki)
            if not fileDate:
                sys.stderr.write("No date option specified and no existing list files for wiki %s\n" % wiki)
                continue
        # FIXME if there is already an output file with the given name etc do we overwrite, or do we skip it??
        # maybe we want a parameter for that. mmm

        if verbose:
            print "Doing local media files tarball for wiki", wiki
        tb = Tarball(mediaBaseDir, listsInputDir, uploadDir, outputDir, "local", inputFileNameFormat, outputFileNameFormat, wiki, fileDate, 2, filesPerTarball, tar, overwrite, verbose)
        tb.writeTarballs()

        if verbose:
            print "Doing remote media files tarball for wiki", wiki

        tb = Tarball(mediaBaseDir, listsInputDir, remoteUploadDir, outputDir, "remote", inputFileNameFormat, outputFileNameFormat, wiki, fileDate, 2, filesPerTarball, tar, overwrite, verbose)
        tb.writeTarballs()
