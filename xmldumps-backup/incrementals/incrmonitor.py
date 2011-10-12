# generate an index page covering the status of and links to 
# incremental files for the latest date for each project 

import ConfigParser
import getopt
import os
import re
import sys
import WikiDump
from WikiDump import FileUtils, TimeUtils, MiscUtils
import subprocess
import socket
import time
import IncrDumpLib
from IncrDumpLib import Lock, Config, RunSimpleCommand, MultiVersion, DBServer, IncrementDir, IncrementDumpsError, IndexFile, IncrDumpLockFile, IncDumpDirs, IncrDumpLock, MaxRevIDLock, StubFile, RevsFile, StatusFile
from subprocess import Popen, PIPE
from os.path import exists
import hashlib
import traceback

class Link(object):

    def makeLink(path, linkText):
        return('<a href = "' + path + '">' + linkText + "</a>")

    makeLink = staticmethod(makeLink)

class Index(object):
    def __init__(self, config, verbose):
        self._config = config
        self.indexFile = IndexFile(self._config)
        self.incrDir = IncrementDir(self._config)
        self.verbose = verbose

    def doAllWikis(self):
        text = ""
        for w in self._config.allWikisList:
            result = self.doOneWiki(w)
            if result:
                text = text + "<li>"+ result + "</li>\n"
        indexText = self._config.readTemplate("incrs-index.html") %  { "items" : text }
        FileUtils.writeFileInPlace(self.indexFile.getPath(), indexText, self._config.fileperms)

    def doOneWiki(self, w):
        if w not in self._config.privateWikisList and w not in self._config.closedWikisList:
            self.incrDumpsDirs = IncDumpDirs(self._config, w)
            if not exists(self.incrDir.getIncDirNoDate(w)):
                if (self.verbose):
                    print "No dump for wiki ", w
                    next

            incrDate = self.incrDumpsDirs.getLatestIncrDate()
            if not incrDate:
                if (self.verbose):
                    print "No dump for wiki ", w
                    next

            try:
                lock = IncrDumpLock(self._config, incrDate, w)
                lockDate = lock.getLockInfo()

                stub = StubFile(self._config, incrDate, w)
                (stubDate, stubSize) = stub.getFileInfo()
                revs = RevsFile(self._config, incrDate, w)
                (revsDate, revsSize) = revs.getFileInfo()
                stat = StatusFile(self._config, incrDate, w)
                statContents = FileUtils.readFile(stat.getPath())
                    
            except:
                if (self.verbose):
                    traceback.print_exc(file=sys.stdout)
                return "Error encountered, no information available for wiki", w

            try:
                wikinameText = "<strong>%s</strong>" % w
                if lockDate:
                    lockText = "run started on %s." % lockDate
                else:
                    lockText = None
                if stubDate:
                    stubText = "stubs: %s (size %s)" %  (Link.makeLink(os.path.join(w, incrDate, stub.getFileName()),stubDate), stubSize)
                else:
                    stubText = None
                if revsDate:
                    revsText = "revs: %s (size %s)" %  (Link.makeLink(os.path.join(w, incrDate, revs.getFileName()),revsDate), revsSize)
                else:
                    revsText = None
                if statContents:
                    statText = "(%s)" % (statContents)
                else:
                    statText = None

                wikiInfo = " ".join( filter( None, [ wikinameText, lockText, statText ] ) ) + "<br />"
                wikiInfo = wikiInfo + " &nbsp;&nbsp; " + " |  ".join( filter( None, [ stubText, revsText ] ))
            except:
                if (self.verbose):
                    traceback.print_exc(file=sys.stdout)
                return "Error encountered formatting information for wiki", w
                
            return wikiInfo

def usage(message = None):
    if message:
        print message
        print "Usage: python monitor.py [options] [wikidbname]"
        print "Options: --configfile, --verbose"
        print "--configfile:  Specify an alternate config file to read. Default file is 'dumpincr.conf' in the current directory."
        print "--verbose:     Print error messages and other informative messages (normally the"
        print "               script runs silently)."
        sys.exit(1)
            
if __name__ == "__main__":
    configFile = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 ['configfile=', 'verbose' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--configfile":
            configFile = val
        elif opt == '--verbose':
            verbose = True

    if (configFile):
        config = Config(configFile)
    else:
        config = Config()

    index = Index(config, verbose)
    index.doAllWikis()
