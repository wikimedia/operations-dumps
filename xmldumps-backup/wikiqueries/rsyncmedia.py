import os, sys, getopt, subprocess
from subprocess import Popen, PIPE

class Rsyncer(object):
    def __init__(self, rsyncHost, remoteBaseDir, outputDir, verbose, dryrun):
        self.rsyncHost = rsyncHost
        self.remoteBaseDir = remoteBaseDir
        self.outputDir = outputDir
        self.verbose = verbose
        self.dryrun = dryrun

    def makePath(self, dirList):
        dirs = filter(None, dirList)
        if len(dirs) == 0:
            return None
        elif len(dirs) == 1:
            # this is ok even with 'None'
            return dirs[0]
        else:
            return os.path.join(*dirs)

    def doRsync(self, filesToDo, localPath):
        localdir = self.makePath([ self.outputDir, localPath ])

        command = [ "rsync", "-rltDp" ]
        if filesToDo:
            command.extend([ "--files-from", "-" ])
        command.extend([self.rsyncHost + "::" + self.remoteBaseDir, self.outputDir ])
        # 23 = Partial transfer due to error
        # 24 = Partial transfer due to vanished source files
        # we can see these from rsync because 1) the source dir doesn't exist, for
        # small projects which now have media upload disabled, or 2) the file
        # about to be rsynced is deleted.  Since we will likely encounter
        # some of each type of error on every single run, log things
        # but don't bail

        self.doCommand(command, filesToDo, [23, 24])

    def doCommand(self, command, inputToCommand, returnCodesAllowed):
        commandString = " ".join(command)
        if self.dryrun:
            print "would run commmand:",
        elif self.verbose:
            print "about to run command:",
        if self.dryrun or self.verbose:
            print commandString, "with input: ", inputToCommand

        if self.dryrun:
            return

        try:
            proc = Popen(command, stdin = PIPE, stdout = PIPE, stderr = PIPE)
            output, error = proc.communicate(inputToCommand)
            if proc.returncode and proc.returncode not in returnCodesAllowed:
                print "command '%s failed with return code %s and error %s" % ( command, proc.returncode,  error ) 
                # we don't bail here, let the caller decide what to do about it"
        except:
            print "command %s failed" % command
            if error:
                print error
            # the problem is probably serious enough that we should refuse to do further processing
            raise
        if output:
            print output
        if error:
            print error
        return proc.returncode
    
class RsyncProject(object):
    def __init__(self, rsyncer, wiki, wtype, wikidir):
        self.rsyncer = rsyncer
        self.wiki = wiki
        self.wtype = wtype
        self.wikidir = wikidir

    def doRsync(self):

        if self.wtype == "huge":
            # do all 256 shards separately
            self.doHugeRsync()

        elif self.wtype == "big":
            # do the top 16 shards separately
            self.doBigRsync()
        else:
            # do the whole thing at once
            self.doNormalRsync()

    def getFilesFrom(self, hashdir = None, subdir = None):
        """get list of directories for rsync that will
        be fed to the "--files-from -" option"""
        return self.rsyncer.makePath([ self.wikidir, hashdir, subdir ])

    def getLocalPath(self, hashdir = None, subdir = None):
        """get the local output path for.."""
        return self.rsyncer.makePath([ self.wikidir, hashdir, subdir ])

    def doHugeRsync(self):
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            print "doing 256 separate shards for wiki", self.wiki

        dirs = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"]
        subdirs = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"]
        for d in dirs:
            for s in subdirs:
                filesFrom = self.getFilesFrom(d, d+s)
                localPath = self.getLocalPath(d, d+s)
                self.rsyncer.doRsync(filesFrom, localPath)
        # now get the archive dir
        for d in dirs:
            filesFrom = self.getFilesFrom("archive", d)
            localPath = self.getLocalPath("archive", d)
            self.rsyncer.doRsync(filesFrom, localPath)

    def doBigRsync(self):
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            print "doing 16 separate shards for wiki", self.wiki

        dirs = [ "0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f","archive"]
        for d in dirs:
            filesFrom = self.getFilesFrom(d)
            localPath = self.getLocalPath(d)
            self.rsyncer.doRsync(filesFrom, localPath)
        
    def doNormalRsync(self):
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            print "doing 1 shard for wiki", self.wiki

        # explicitly list the 17 dirs we want
        filesFrom = '\n'.join([ self.rsyncer.makePath([self.wikidir, d]) for d in ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f","archive"]])
        localPath = self.getLocalPath()
        self.rsyncer.doRsync(filesFrom, localPath)

def usage(message = None):
    if message:
        print message
        sys.stderr.write("Usage: python rsyncmedia.py --remotehost hostname --remotedir dirname\n")
        sys.stderr.write("                      --localdir dirname --wikilist filename\n")
        sys.stderr.write("                      [--big wiki1,wiki2,...] [--huge wiki3,wiki4,...]\n")
        sys.stderr.write("                      [--verbose] [--dryrun]\n")
        sys.stderr.write("\n")
        sys.stderr.write("This script rsyncs media from a primary media host. getting only media\n")
        sys.stderr.write("publically available (no deleted images, no data from private wikis)\n")
        sys.stderr.write("and skipping thumbs, math, timeline, temp, old and misc other directories\n")
        sys.stderr.write("that may have been created over time.\n")
        sys.stderr.write("\n")
        sys.stderr.write("--remotehost:    hostname of the remote host form which we are rsyncing.\n")
        sys.stderr.write("--remotedir:     path to point in remote directory in which media for the\n")
        sys.stderr.write("                 wiki(s) are stored; this path is relative to the rsync root.\n")
        sys.stderr.write("--localdir:      path to root of local directory tree in which media for\n")
        sys.stderr.write("                 the wiki(s) will be copied.\n")
        sys.stderr.write("--wikilist       filename which contains names of the wiki databases and their\n")
        sys.stderr.write("                 corresponding media upload directories,  one wiki per line,\n")
        sys.stderr.write("                 line, to be rsynced. The wikiname and the directory should be\n")
        sys.stderr.write("                 separated by a tab character.  If '-' is given as the name\n")
        sys.stderr.write("                 wiki db names and directories will be read from stdin.\n")
        sys.stderr.write("--big            comma-separated list of wiki db names which have enough media\n")
        sys.stderr.write("                 that we should rsync them in 16 batches, one per subdir\n")
        sys.stderr.write("                 instead of all at once.\n")
        sys.stderr.write("--huge           comma-separated list of wiki db names which have enough media\n")
        sys.stderr.write("                 that we should rsync them in 256 batches, one per 2nd level\n")
        sys.stderr.write("                 subdir instead of all at once.\n")
        sys.stderr.write("--verbose:       print lots of status messages.\n")
        sys.stderr.write("--dryrun:        don't do the rsync, print what would be done.\n")
        sys.stderr.write("wiki             name of wikidb for rsync; if specified, this will override\n")
        sys.stderr.write("                 any file given for 'wikilist'.\n")
        sys.exit(1)
                         
def getCommaSepList(text):
    if text:
        if ',' in text:
            result = text.split(',')
        else:
            result = [ text ]
    else:
        result = []
    return result

if __name__ == "__main__":
    remoteDir = None
    rsyncHost = None
    localDir = None
    big = None
    huge = None
    wikiListFile = None
    verbose = False
    dryrun = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "big=", "huge=", "localdir=", "remotedir=", "remotehost=", "localdir=", "wikilist=", "verbose", "dryrun" ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--remotedir":
            remoteDir = val
        elif opt == "--remotehost":
            rsyncHost = val
        elif opt == "--localdir":
            localDir = val
        elif opt == "--big":
            big = val
        elif opt == "--huge":
            huge = val
        elif opt == "--wikilist":
            wikiListFile = val
        elif opt == "--verbose":
            verbose = True
        elif opt == "--dryrun":
            dryrun = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not remoteDir or not rsyncHost or not localDir or not wikiListFile:
        usage("One or more mandatory options missing")

    if wikiListFile == "-":
        fd = sys.stdin
    else:
        fd = open(wikiListFile ,"r")
    wikiList = [ line.strip() for line in fd ]

    if fd != sys.stdin:
        fd.close()

    # eg enwiki
    bigWikis = getCommaSepList(big)
    # eg commonswiki
    hugeWikis = getCommaSepList(huge)

    rsyncer = Rsyncer(rsyncHost, remoteDir, localDir, verbose, dryrun)

    for winfo in wikiList:
        # first skip blank lines and comments
        if not winfo or winfo[0] == '#':
            continue
        if not '\t' in winfo:
            sys.stderr.write("unexpected line with no tab in wikilist: %s\n") % winfo
            continue

        # expect <wikiname>\t<directory>
        w, wikidir = winfo.split('\t', 1)

        if w in hugeWikis:
            wtype = "huge"
        elif w in bigWikis:
            wtype = "big"
        else:
            wtype = "normal"

        rp = RsyncProject(rsyncer, w, wtype, wikidir)
        rp.doRsync()
