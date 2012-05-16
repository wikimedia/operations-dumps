import os, re, sys, time, hashlib, urllib, httplib, getopt, gzip, subprocess, multiprocessing, mirror, jobs
from subprocess import Popen, PIPE
from Queue import Empty
from jobs import JobQueue, Job
from mirror import MirrorMsg

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
            return ""
        else:
            md5 = hashlib.md5()
            md5.update( mediaFileName)
            md5Hash = md5.hexdigest()
            path = ''
        for i in range( 1,self.hashLevels+1 ):
            path = path + md5Hash[0:i] + os.sep
        return path

    def getMediaFilePath(self, mediaFileName):
        return os.path.join(self.inputDir, self.getHashPathForLevel(mediaFileName), mediaFileName)

class Tarball(object):
    def __init__(self, baseDir, listInputDir, uploadDir, outputDir, listType, listFileNameFormat, tarballNameFormat, wiki, date, hashLevel, numFilesPerTarball, tarName, tempDir, overwrite, workerCount, verify, verbose):
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
        self.tempDir = tempDir
        self.overwrite = overwrite
        self.verify = verify
        self.verbose = verbose
        # we guess it will take at most 5 minutes to create a job and stuff it on the queue
        # and yes this should become a parameter later, FIXME
        if not self.verify:
            self.jQ = JobQueue(workerCount, self, 3600, self.verbose, False)
        self.jobs = {}

    def getTarballTOC(self, fileName):
        command = [ self.tarName, "-tf", fileName ]
        commandString = " ".join([ "'" + c + "'" for c in command ])
        try:
            proc = Popen(command, stderr = PIPE, stdout = PIPE)
            output, error = proc.communicate() 
            if proc.returncode:
                sys.stderr.write("command '%s failed with return code %s and error %s\n" % ( command, proc.returncode,  error ))
                return []
        except:
            sys.stderr.write("command %s failed\n" % command)
            return []
        return output.strip().split('\n')

    def verifyTarballs(self):
        # read all filenames from local tarballs
        # if difference whine
        listFileName = self.getListFileName()
        if not os.path.exists(listFileName):
            if self.verbose:
                print "Verification: skipping %s since it does not exist" % listFileName
            return
        listfd = gzip.open(listFileName, "rb")
        filesInList = {}
        listFilesCount = 0
        mp = MediaPath(self.uploadDir, 2)
        for line in listfd:
            # format of these lines: mediafilename<tab>metadata
            filesInList[mp.getMediaFilePath(line.split('\t',1)[0])] = False
            listFilesCount +=1

        # this approach assumes that we don't have some missing sequence number in the middle,
        # if we have tarballs out of sequence, verification should fail anyways
        serial = 0
        tarballFilesCount = 0
        extras = 0
        while True:
            serial += 1
            tarballFileName = self.getTarballFileName(serial)
            if not os.path.exists(tarballFileName):
                break
            filesFromTarball = self.getTarballTOC(tarballFileName)
            
            firstWhine = False
            for f in filesFromTarball:
                if not f in filesInList:
                    if firstWhine:
                        # we only print one of these.
                        MirrorMsg.display("wiki %s (%s): file %s in tarball %s not in file list\n" % (self.wiki, self.listType, f, tarballFileName))
                        firstWhine = False
                    extras += 1
                else:
                    del filesInList[f]
                    tarballFilesCount += 1 # number of files in the tarball(s) also in the input list

        if tarballFilesCount < listFilesCount:
            # just print the first one we grab, as an indication
            MirrorMsg.display("wiki %s (%s): file %s in list not in tarballs (total: %s missing)\n" % (self.wiki, self.listType, filesInList.keys()[0], listFilesCount - tarballFilesCount))
        if extras:
            MirrorMsg.display("wiki %s (%s): some files in tarballs not in list (total: %s missing)\n" % (self.wiki, self.listType, extras))
        if not extras and (tarballFilesCount == listFilesCount):
            MirrorMsg.display("wiki %s (%s): verify good\n" % (self.wiki, self.listType))

    def getTarballFileName(self, num):
        return os.path.join(self.outputDir, self.tarballNameFormat.format(w = self.wiki, d = self.date, t = self.listType, n = num))

    def getListFileName(self):
        return os.path.join(self.listInputDir, self.listFileNameFormat.format(w = self.wiki, d = self.date, t = self.listType))

    def getTempFileName(self, num):
        return os.path.join(self.temDir, self.tarballNameFormat.format(w = self.wiki, d = self.date, t = self.listType, n = serial))

    def putTarballJobsOnQueue(self):
        # don't overwrite tarballs if we are told not to
        if not self.overwrite:
            firstTarballName = self.getTarballName(self.wiki, 1)
            if os.path.exists(firstTarballName):
                # there are already (some) output files for this wiki and date. don't regenerate them.
                if self.verbose:
                    print "Skipping %s since tarballs for this wiki and date already exist" % self.wiki
                return

        # media file path is relative to basedir, we will cd to basedir
        # for the tar so the tarball filenames are reasonable

        listFileName = self.getListFileName()

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
                if tempDir:
                    tempFileName = self.getTempFileName(serial)
                else:
                    tempFileName = ""
                    outFileName = self.getTarballFileName(serial)

                job = self.makeJob(tempFileName, outFileName, filesToTar)
                if self.verbose:
                    MirrorMsg.display("adding job %s (filecount %d) to queue\n" % (job.jobId, fileCount))
                self.jQ.addToJobQueue(job)
                fileCount = 0
                serial += 1
                filesToTar = []

        if fileCount:
            # do the last batch
                if tempDir:
                    tempFileName = self.getTempFileName(serial)
                else:
                    tempFileName = ""
                outFileName = self.getTarballFileName(serial)
                job = self.makeJob(tempFileName, outFileName, filesToTar)
                if self.verbose:
                    MirrorMsg.display("adding job %s (filecount %d) to queue\n" % (job.jobId, fileCount))
                self.jQ.addToJobQueue(job)

        listfd.close()
        self.jQ.setEndOfJobs()

    def makeJob(self, tempFileName, outFileName, filesToTar):
        contents = [ tempFileName, outFileName ]
        contents.append('\n'.join(filesToTar) + '\n')
        job = Job(outFileName, contents)
        self.jobs[job.jobId] = job
        return job

    def doJob(self, jobContents):
        return self.writeTarball(jobContents)
            
    def writeTarball(self, jobContents):
        tempFileName = jobContents[0]
        tarballFileName = jobContents[1]
        filesToTar = jobContents[2]
        if tempFileName:
            outFileName = tempFileName
        else:
            outFileName = tarballFileName
        # if there are files that have been deleted in the meantime, tar will whine but continue
        # seriously? tar is option-order sensitive for -C?? bleep bleepers!
        command = [ self.tarName, "-C", self.baseDir, "-cpf", outFileName,  "-T", "-", "--no-unquote", "--ignore-failed-read" ]
        commandString = " ".join([ "'" + c + "'" for c in command ])
        if verbose:
            print "For wiki", self.wiki, "command:", commandString
        try:
            proc = Popen(command, stderr = PIPE, stdin = PIPE)
            output, error = proc.communicate(filesToTar) # no output, ignore it
            if proc.returncode:
                sys.stderr.write("command '%s failed with return code %s and error %s\n" % ( command, proc.returncode,  error ))
                return True # failure
        except:
            sys.stderr.write("command %s failed\n" % command)
            return True # failure

        if error:
            # log any file read perm or file missing errors we might have encountered
            sys.stderr.write("error from command %s: %s\n" % (command, error))
        if tempFileName:
            # do the rename. shutils.rename or something? 
            try:
                shutil.move(tempFileName, outFileName)
            except:
                sys.stderr.write("failed to rename % to %s\n" % tempFileName, outFileName)
                return True # failure albeit at thelast second, and the tarball itself is likely fine
            return  # success

    def renameBadTarballFile(self, job):
        if job.checkIfFailed():
            # rename the tarball so folks know it's broken
            tarballFile = job.jobId
            try:
                os.rename(tarballFile, tarballFile + ".bad")
            except:
                return False
        return True

    def watchJobQueue(self):
        while True:
            # any completed jobs?
            job = self.jQ.getJobFromNotifyQueue()
            # no more jobs and mo more workers.
            if not job:
                if not self.jQ.getActiveWorkerCount():
                    # check for and rename output files of jobs that died
                    # and therefore never completed
                    for jId in self.jobs:
                        self.renameBadTarballFile(self.jobs[jId])
                    if self.verbose:
                        MirrorMsg.display( "no jobs left and no active workers\n")
                    break
                else:
                    continue
            if self.verbose:
                MirrorMsg.display("jobId %s completed\n" % job.jobId)

            j = self.jobs[job.jobId]
            if job.checkIfDone():
                j.markDone()
            if job.checkIfFailed():
                j.markFailed()
                # rename the tarball so folks know it's broken
                if not self.renameBadTarballFile(j):
                    MirrorMsg.display( "job %s: failed to move tar file out of the way (bad)\n" % job.jobId)
                    
                
def usage(message = None):
    if message:
        sys.stderr.write("%s\n" % message)
        sys.stderr.write("Usage: python createmediatarballs.py --mediadir dirname --listsinputdir dirname\n")
	sys.stderr.write("                     --remoterepo reponame [--outputdir dirname] [--date YYYYMMDD]\n")
        sys.stderr.write("                     [--wikilist filename] [--inputnameformat format]\n")
        sys.stderr.write("                     [--outputnameformat format] [--filespertarball num]\n")
        sys.stderr.write("                     [--tar tarcmd] [--nooverwrite] [--tempdir dirname] [--workers]\n")
        sys.stderr.write("                     [--verbose]\n")
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
        sys.stderr.write("--verify:          verify existing tarballs, don't write new ones; this verifies that all\n")
        sys.stderr.write("                   filenames in the local and remote media lists are included in the tarball\n")
        sys.stderr.write("                   contents, it does not check the media files themselves\n")
        sys.stderr.write("--tar:             name of gnu tar command, default: 'tar'\n")
        sys.stderr.write("--nooverwrite:     do not overwrite existingtarballs for a given project and date; by default\n")
        sys.stderr.write("                   a new tarball will be created every time\n")
        sys.stderr.write("--tempdir:         write each tarball to the temp directory specified and move into place afterwards\n")
        sys.stderr.write("                   by default, files are written directly in place\n")
        sys.stderr.write("--workers:         how many workers are started up to write tarballs in parallel; default: 1\n")
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
    tempDir = None
    date = None
    wikiListFile = "all.dblist"
    remoteRepoName = None
    inputFileNameFormat = "{w}-{d}-{t}-wikiqueries.gz"
    outputFileNameFormat = "{w}-{d}-{t}-wikiqueries-{n}.tar"
    filesPerTarball = 100000
    tar = "tar"
    overwrite = True
    verify = False
    workerCount = 1
    verbose = False
    
#    dbListPath = os.path.join(os.getcwd(), dbList)

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "mediadir=", "listsinputdir=", "outputdir=", "date=", "wikilist=", "remoterepo=", "inputnameformat=", "outputnameformat=", "filespertarball=", "tar=", "workers=", "tempdir=", "nooverwrite", "verify", "verbose" ])
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
        elif opt == "--workers":
            if not val.isdigit():
                usage("workers must be a positive integer")
            workerCount = int(val)
        elif opt == "--tempdir":
            tempDir = False
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--verify":
            verify = True
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

        if verbose and not verify:
            print "Doing local media files tarball for wiki", wiki
        tb = Tarball(mediaBaseDir, listsInputDir, uploadDir, outputDir, "local", inputFileNameFormat, outputFileNameFormat, wiki, fileDate, 2, filesPerTarball, tar, tempDir, overwrite, workerCount, verify, verbose)
        if verify:
            tb.verifyTarballs()
        else:
            tb.putTarballJobsOnQueue()

        if verbose and not verify:
            print "Doing remote media files tarball for wiki", wiki

        tb = Tarball(mediaBaseDir, listsInputDir, remoteUploadDir, outputDir, "remote", inputFileNameFormat, outputFileNameFormat, wiki, fileDate, 2, filesPerTarball, tar, tempDir, overwrite, workerCount, verify, verbose)
        if verify:
            tb.verifyTarballs()
        else:
            tb.putTarballJobsOnQueue()
            tb.watchJobQueue()
