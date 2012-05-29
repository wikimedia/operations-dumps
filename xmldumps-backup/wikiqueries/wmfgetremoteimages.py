import os, sys, re, gzip, getopt, time, ConfigParser, subprocess, codecs
from subprocess import Popen, PIPE
from wikiqueries import Config

class MediaPerProject(object):
    def __init__(self, conf, outputDir, remoteRepoName, reuseRepoDate, verbose, wqConfigFile, wqPath):
        self.conf = conf
        self.outputDir = outputDir
        self.reuseRepoDate = reuseRepoDate
        self.remoteRepoName = remoteRepoName
        self.remoteRepoMediaDict = None
        self.verbose = verbose
        self.date = time.strftime("%Y%m%d", time.gmtime())
        self.fileNameFormat = "{w}-{d}-wikiqueries.gz"
        self.wqConfigFile = wqConfigFile
        self.wqPath = wqPath
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        self.wikisToDo = [ w for w in self.conf.allWikisList if w not in self.conf.privateWikisList and w not in self.conf.closedWikisList ]

    def getFileNameFormat(self, phase):
        return "{w}-{d}-" + phase + "-wikiqueries.gz"
        
    def generateSqlFiles(self):
        if self.verbose:
            print "Starting round one wikiqueries for imagelinks table"
        self.doWikiQueries('select distinct il_to from imagelinks', self.getFileNameFormat("links"))
        if self.verbose:
            print "Done round one!!"
            print "Starting round two wikiqueries for image table"
        self.doWikiQueries('select img_name, img_timestamp from image', self.getFileNameFormat("local"))
        if self.verbose:
            print "Done round two!!"
        if self.verbose:
            print "Starting round three wikiquries for remote redirs"
        self.doWikiQueries("select p.page_title, r.rd_title from redirect as r, page as p where rd_namespace = 6 and p.page_id = r.rd_from and page_namespace = 6", self.getFileNameFormat("redirs"), self.remoteRepoName)
        if self.verbose:
            print "Done round three!!"

    def doWikiQueries(self, query, fileNameFormat, wiki = None):
        if not os.path.exists(wqConfigFile):
            print "config file  %s does not exist" % wqConfigFile
            sys.exit(1)
        command = [ "python", self.wqPath, "--configfile", wqConfigFile, "--query", query, "--outdir", self.outputDir, "--filenameformat", fileNameFormat ]
        if verbose:
            command.append("--verbose")
        if wiki:
            command.append(wiki)
        commandString = " ".join([ "'" + c + "'" for c in command ])

        if verbose:
            print "About to run wikiqueries:", commandString
        try:
            proc = Popen(command, stderr = PIPE)
            output, error = proc.communicate() # no output, ignore it
            if proc.returncode:
                print "command '%s failed with return code %s and error %s" % ( command, proc.returncode,  error ) 
                sys.exit(1)
        except:
            print "command %s failed" % command
            raise

    def writeRemoteHostedMediaList(self, db, remoteRepoMediaDict):
        if db == self.remoteRepoName:
            if self.verbose:
                print "Skipping", db, "because it's the remote repo"
            return
        if self.verbose:
            print "Doing db", db

        # get all media used on a project and remotely stored (or 
        # they just don't exist; links to nonexistent media still 
        # go into the links table so we'll get those too)
        
        if not os.path.exists(self.getPath(self.getMediaLinksFileName(db))):
            if self.verbose:
                print "Skipping", db, "since sql files for it were not generated"
            return

        # links may have initial lowercase; media titles are all initial uppercase.

        # can't use codecs.getreader()gzip.open()) because it will find '\n' in multibyte chars
        mediaLinksFd = gzip.open(self.getPath(self.getMediaLinksFileName(db)), "rb")
        mediaLinks = filter(None, [ self.safeDecode(line) for line in mediaLinksFd ])
        mediaLinks = [ line[0].upper() + line.strip()[1:] for line in mediaLinks ]
        mediaLinksFd.close()

        localMediaFd = gzip.open(self.getPath(self.getLocalMediaFileName(db)), "rb")
        localMediaDict = self.getMediaDict(localMediaFd)
        localMediaFd.close()

        remoteHostedMedia = [ m for m in mediaLinks if m not in localMediaDict ]

        # replace all the remoteHosted entries that we find in the remote redir
        # list with the titles of the redirect targets.
        remoteMediaRedirsFd = gzip.open(self.getPath(self.getremoteMediaRedirsFileName(self.date)), "rb")
        remoteMediaRedirsDict = self.getMediaRedirsDict(remoteMediaRedirsFd)
        remoteMediaRedirsFd.close()

        remoteHostedMediaNoRedirs = set([ remoteMediaRedirsDict[f] if f in remoteMediaRedirsDict else f for f in remoteHostedMedia ])
        remoteHostedMediaExists = [ m for m in remoteHostedMediaNoRedirs if m in remoteRepoMediaDict ]

        outFd = codecs.getwriter("utf-8")(gzip.open(self.getPath(self.getRemoteMediaFileName(db)), "wb"))
        for f in remoteHostedMediaExists:
            outFd.write("%s\t%s\n" % (f, self.remoteRepoMediaDict[f]))
        outFd.close()
        if self.verbose:
            print "Done!"

    def safeDecode(self, line):
        try:
            line = line.strip().decode("utf-8")
        except UnicodeDecodeError:
            print "unicode decode failed, line is", line
            line = None
        return line

    def getMediaRedirsDict(self, RedirsFd):
        redirsDict = {}
        for line in RedirsFd:
            line = self.safeDecode(line)
            fromTitle, toTitle = self.getSqlFields(line, 2)
            if fromTitle and toTitle:
                redirsDict[fromTitle] = toTitle
        return redirsDict

    # sure this is functionally identical to getmediaredirsdict
    # but we may want more fields in here later so it's split out
    def getMediaDict(self, mediaFd):
        mediaDict = {}
        for line in mediaFd:
            line = self.safeDecode(line)
            title, timestamp = self.getSqlFields(line, 2)
            if title and timestamp:
                mediaDict[title] = timestamp
        return mediaDict
    
    def getSqlFields(self, line, numFields):
        if not line or not '\t' in line:
            return None, None
        return line.split('\t',1)

    def initializeRemoteRepoMediaDict(self):
        if verbose:
            print "setting up list of media from remote repo"
        if not self.remoteRepoMediaDict:
            if reuseRepoDate:
                if self.verbose:
                    print "attempting to reuse previously generated remote repo media list"
                try:
                    self.readRemoteRepoMediaDict(self.reuseRepoDate)
                except:
                    pass
        if not self.remoteRepoMediaDict:
            if self.verbose:
                print "reading current list of remote repo media"
                try:
                    self.readRemoteRepoMediaDict()
                except:
                    print "failed to read remote repo media list"
                    sys.exit(1)

    def readRemoteRepoMediaDict(self, date = None):
        remoteRepoMediaDictFd = gzip.open(self.getPath(self.getRemoteRepoFileName(date)), "rb")
        self.remoteRepoMediaDict = self.getMediaDict(remoteRepoMediaDictFd)
        remoteRepoMediaDictFd.close()
        
    def getPath(self, fileName):
        return(os.path.join(self.outputDir, fileName))

    def getFileName(self, phase, wiki, date):
        if not date:
            date = self.date
        return (self.getFileNameFormat(phase).format(w=wiki, d=date))
    
    def getremoteMediaRedirsFileName(self, date = None):
        return self.getFileName("redirs", self.remoteRepoName, date)

    def getRemoteRepoFileName(self, date = None):
        return self.getFileName("local", self.remoteRepoName, date)

    def getMediaLinksFileName(self, wiki, date = None):
        return self.getFileName("links", wiki, date)

    def getLocalMediaFileName(self, wiki, date = None):
        return self.getFileName("local", wiki, date)

    def getRemoteMediaFileName(self, wiki, date = None):
        return self.getFileName("remote", wiki, date)

    def doAllProjects(self):
        self.initializeRemoteRepoMediaDict()
        if wiki:
            dbList = [ wiki ]
        else:
            dbList = self.wikisToDo
        if self.verbose:
            print "Starting generation of remote media lists for all wikis"
        for db in dbList:
            self.writeRemoteHostedMediaList(db, self.remoteRepoMediaDict)
        if self.verbose:
            print "Done with generation of remote media lists for all wikis"

def usage(message = None):
    if message:
        print message
        print "Usage: python wmfgetremoteimages.py --outputdir dirname [--remoterepo reponame]"
        print "                      [--reuseremoterepolist] [--verbose] [--wqconfig filename]"
        print "                      [wqpath filename] [wiki]"
        print ""
        print "This script produces a list of images in use on the local wiki stored on remote"
        print "repo (e.g. commons)."
        print ""
        print "--outputdir:      where to put the list of remotely hosted media per project"
        print "--remotereponame: name of the remote repo that houses images for projects"
        print "                  default: 'commons'"
        print "--verbose:        print lots of status messages"
        print "--wqconfig:       relative or absolute path of wikiquery config file"
        print "                  default: wikiqueries.conf"
        print "--wqpath:         relative or absolute path of the wikiqieries python script"
        print "                  default: wikiqueries.py"
        print "--sqlonly:        only do the sql queries (first half of run)"
        print "--listsonly:      only generate the lists from the sql files (second half of run)"
        sys.exit(1)


if __name__ == "__main__":
    outputDir = None
    remoteRepoName = "commons"
    reuseRepoDate = None
    verbose = False
    wiki = None
    sqlOnly = False
    listsOnly = False
    wqPath = os.path.join(os.getcwd(), "wikiqueries.py")
    wqConfigFile = os.path.join(os.getcwd(), "wikiqueries.conf")

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "outputdir=", "remotereponame=", "wqconfig=", "wqpath=", "reuseremoterepolist=", "sqlonly", "listsonly", "verbose" ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--outputdir":
            outputDir = val
        elif opt == "--remotereponame":
            remoteRepoName = val
        elif opt == "--reuseremoterepolist":
            reuseRepoDate = val
        elif opt == "--sqlonly":
            sqlOnly = True
        elif opt == "--listsonly":
            listsOnly = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--wqconfig":
            wqConfigFile = val
            if not os.sep in val:
                wqConfigFile = os.path.join(os.getcwd(), wqConfigFile)
            # bummer but we can't really avoid ita
            config = Config(wqConfigFile)
        elif opt == "--wqpath":
            wqPath = val
            if not os.sep in val:
                wqPath = os.path.join(os.getcwd(), wqPath)

    if len(remainder) > 0:
        if not remainder.isalpha():
            usage("Unknown option specified")
        else:
            wiki = remainder

    if not outputDir:
        usage("One or more mandatory options missing")
    if listsOnly and sqlOnly:
        usage("Only one of 'listsonly' and 'sqlonly' may be specified at once.")

    mpp = MediaPerProject(config, outputDir, remoteRepoName, reuseRepoDate, verbose, wqConfigFile, wqPath)
    if not listsOnly:
        if verbose: 
            print "generating sql output from all projects"
        mpp.generateSqlFiles()
    # we'll need the list of existing remote repo images to compare against
    if not sqlOnly:
        if verbose:
            print "generating remote hosted media list for all projects"
        mpp.doAllProjects()
    if verbose:
        print "all projects completed."
