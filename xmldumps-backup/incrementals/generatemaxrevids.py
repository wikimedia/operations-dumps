# for every wiki, find and record the max rev_id in use.
# this is phase 1 of daily xml change/adds dumps.

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
from IncrDumpLib import Lock, Config, RunSimpleCommand, MultiVersion, DBServer, IncrementDir, IncrementDumpsError, MaxRevIDFile, MaxRevIDLockFile, IncrDumpLock, MaxRevIDLock
from subprocess import Popen, PIPE
from os.path import exists
import traceback

class MaxRevID(object):
    def __init__(self, config, wikiName, date):
        self._config = config
        self.wikiName = wikiName
        self.date = date
        self.maxID = 0
        self.maxRevIdFile = MaxRevIDFile(self._config, self.date, self.wikiName)

    def getMaxRevID(self):
        query = "select MAX(rev_id) from revision";
        db = DBServer(self._config, self.wikiName)
        # get the result
        self.maxID = RunSimpleCommand.runWithOutput(db.buildSqlCommand(query), shell = True)

    def recordMaxRevID(self):
        self.getMaxRevID()
        # write the max id in a file in the right place
        FileUtils.writeFileInPlace(self.maxRevIdFile.getPath(), self.maxID, self._config.fileperms)

    def exists(self):
        return exists(self.maxRevIdFile.getPath())

class MaxIDDump(object):
    def __init__(self,config, date, verbose):
        self._config = config
        self.date = date
        self.incrDir = IncrementDir(self._config, self.date)
        self.verbose = verbose

    def doOneWiki(self, w):
        success = True
        if w not in self._config.privateWikisList and w not in self._config.closedWikisList:
            if not exists(self.incrDir.getIncDir(w)):
                os.makedirs(self.incrDir.getIncDir(w))
            lock = MaxRevIDLock(self._config, self.date, w)
            if lock.getLock():
                try:
                    maxRevID = MaxRevID(self._config, w, self.date)
                    if not maxRevID.exists():
                        maxRevID.recordMaxRevID()
                except:
                    if (self.verbose):
                        print "Wiki ", w, "failed to get max revid."
                        traceback.print_exc(file=sys.stdout)
                    success = False
                lock.unlock()
            else:
                if (self.verbose):
                    print "Wiki ", w, "failed to get lock."
                    traceback.print_exc(file=sys.stdout)
        if success:
            if (self.verbose):
                print "Success!  Wiki", w, "adds/changes dump complete."
        return success

    def doRunOnAllWikis(self):
        failures = 0
        for w in self._config.allWikisList:
            if not self.doOneWiki(w):
                failures = failures + 1
        return failures

    def doAllWikisTilDone(self,numFails):
        fails = 0
        while 1:
            result = self.doRunOnAllWikis()
            if not result:
                break
            fails  = fails + 1
            if fails > numFails:
                raise("Too many consecutive failures, giving up")
            # wait 5 minutes and try another loop
            time.sleep(300)

def usage(message = None):
    if message:
        print message
        print "Usage: python generateincrementals.py [options] [wikidbname]"
        print "Options: --configfile, --date, --verbose"
        print "--configfile:  Specify an alternate config file to read. Default file is 'dumpincr.conf' in the current directory."
        print "--date:        (Re)run incremental of a given date (use with care)."
        print "--verbose:     Print error messages and other informative messages (normally the"
        print "               script runs silently)."
        print "wikiname:      Run the dumps only for the specific wiki."
        sys.exit(1)

if __name__ == "__main__":
    configFile = False
    result = False
    date = None
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 ['date=', 'configfile=', 'verbose' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--date":
            date = val
        elif opt == "--configfile":
            configFile = val
        elif opt == "--verbose":
            verbose = True

    if (configFile):
        config = Config(configFile)
    else:
        config = Config()

    if not date:
        date = TimeUtils.today()

    dump = MaxIDDump(config, date, verbose)
    if len(remainder) > 0:
        dump.doOneWiki(remainder[0])
    else:
        dump.doAllWikisTilDone(3)
