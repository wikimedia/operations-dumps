# for every wiki, read the maxid and the prev maxid
# recorded for incrementals, dump stubs and dump history file
# based on stubs.
# this is phase 2 of daily xml change/adds dumps.

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
from IncrDumpLib import Lock, Config, RunSimpleCommand, MultiVersion, DBServer, IncrementDir, IncrementDumpsError, MaxRevIDFile, StatusFile, IncrDumpLockFile, StubFile, RevsFile, MD5File, IncDumpDirs, IncrDumpLock, MaxRevIDLock, StatusInfo
from subprocess import Popen, PIPE
from os.path import exists
import hashlib
import traceback

class DumpResults(object):
    def __init__(self):
        self.TODO = 1
        self.FAILED = -1
        self.OK = 0

class IncrDump(object):
    def __init__(self,config, date, wikiName, doStubs, doRevs, dryrun, verbose):
        self._config = config
        self.date = date
        self.wikiName = wikiName
        self.incrDir = IncrementDir(self._config, self.date)
        self.doStubs = doStubs
        self.doRevs = doRevs
        self.dryrun = dryrun
        self.maxRevIDFile = MaxRevIDFile(self._config, self.date, self.wikiName)
        self.statusInfo = StatusInfo(self._config, self.date, self.wikiName)
        self.stubFile = StubFile(self._config, self.date, self.wikiName)
        self.revsFile = RevsFile(self._config, self.date, self.wikiName)
        self.incrDumpsDirs = IncDumpDirs(self._config, self.wikiName)
        self.verbose = verbose

    def getMaxRevIdFromFile(self, date = None):
        if date == None:
            date = self.date
        maxRevIDFile = MaxRevIDFile(self._config, date, self.wikiName)
        return FileUtils.readFile(maxRevIDFile.getPath().rstrip())

    def doOneWiki(self):
        retCodes = DumpResults()
        if self.wikiName not in self._config.privateWikisList and self.wikiName not in self._config.closedWikisList:
            if not exists(self.incrDir.getIncDir(self.wikiName)):
                os.makedirs(self.incrDir.getIncDir(self.wikiName))
            status = self.statusInfo.getStatus()
            if status == "done":
                if (self.verbose):
                    print "wiki",self.wikiName,"skipped, adds/changes dump already complete"
                return retCodes.OK
            if time.time() - os.path.getmtime(self.maxRevIDFile.getPath()) < self._config.delay:
                if (self.verbose):
                    print "wiki",self.wikiName,"skipped, must wait for configured delay interval"
                return retCodes.TODO
            if not dryrun:
                lock = IncrDumpLock(self._config, self.date, self.wikiName)
                if not lock.getLock():
                    if (self.verbose):
                        print "wiki",self.wikiName,"skipped, wiki is locked, another process should be doing the job"
                    return retCodes.TODO
            try:
                if not dryrun:
                    self.incrDumpsDirs.cleanupOldIncrDumps(self.date)
                maxRevID = self.getMaxRevIdFromFile()
                prevDate = self.incrDumpsDirs.getPrevIncrDate(self.date)
                prevRevID = None
                if prevDate:
                    prevRevID = self.getMaxRevIdFromFile(prevDate)
                if not prevRevID:
                    prevRevID = str(int(maxRevID) - 10)
                    if int(prevRevID) < 1:
                        prevRevID = str(1)
                else:
                    # this incr will cover every revision from the last incremental
                    # through the maxid we wrote out in phase one of this job.
                    prevRevID = str(int(prevRevID) + 1)
                if doStubs:
                    maxRevID = str(int(maxRevID) + 1) # end rev id is not included in dump
                    if not self.dumpStub(prevRevID, maxRevID):
                        return retCodes.FAILED
                if doRevs:
                    if not self.dumpRevs():
                        return retCodes.FAILED
                if not dryrun:
                    if not self.md5sums():
                        return retCodes.FAILED
                    self.statusInfo.setStatus("done")
                    lock.unlock()
            except:
                if (self.verbose):
                    traceback.print_exc(file=sys.stdout)
                if not dryrun:
                    lock.unlock()
                return retCodes.FAILED
        if (self.verbose):
            print "Success!  Wiki", self.wikiName, "incremental dump complete."
        return retCodes.OK

    def dumpStub(self, startRevID, endRevID):
        scriptCommand = MultiVersion.MWScriptAsArray(self._config, "dumpBackup.php")
        command = [ "%s" % self._config.php, "-q" ]
        command.extend(scriptCommand)
        command.extend(["--wiki=%s" % self.wikiName, "--stub", "--quiet",
                        "--force-normal", "--output=gzip:%s" % self.stubFile.getPath(),
                        "--revrange", "--revstart=%s" % startRevID, "--revend=%s" % endRevID ])
        if dryrun:
            print "would run command for stubs dump:", command
        else:
            error = RunSimpleCommand.runWithNoOutput(command, shell = False)
            if (error):
                if (self.verbose):
                    print ("error producing stub files for wiki" % self.wikiName)
                return False
        return True

    def dumpRevs(self):
        scriptCommand = MultiVersion.MWScriptAsArray(self._config, "dumpTextPass.php")
        command = [ "%s" % self._config.php, "-q" ]
        command.extend(scriptCommand)
        command.extend(["--wiki=%s" % self.wikiName, "--stub=gzip:%s" % self.stubFile.getPath(),
                            "--force-normal", "--quiet", "--spawn=%s" % self._config.php,
                            "--output=bzip2:%s" % self.revsFile.getPath()
                            ])
        if dryrun:
            print "would run command for revs dump:", command
        else:
            error = RunSimpleCommand.runWithNoOutput(command, shell = False)
            if (error):
                if (self.verbose):
                    print("error producing revision text files for wiki" % self.wikiName)
                return False
        return True

    def md5sumOneFile(self, filename):
        summer = hashlib.md5()
        infile = file(filename, "rb")
        bufsize = 4192 * 32
        buffer = infile.read(bufsize)
        while buffer:
            summer.update(buffer)
            buffer = infile.read(bufsize)
        infile.close()
        return summer.hexdigest()

    def md5sums(self):
       try:
           md5File = MD5File(self._config, self.date, self.wikiName)
           text = ""
           summer = hashlib.md5()
           files = []
           if self.doStubs:
               files.append(self.stubFile.getPath())
           if self.doRevs:
               files.append(self.revsFile.getPath())
           for f in files:
               text = text + "%s\n" % self.md5sumOneFile(f)
               FileUtils.writeFileInPlace(md5File.getPath(), text, self._config.fileperms)
           return True
       except:
           return False

class IncrDumpLoop(object):
    def __init__(self, config, date, doStubs, doRevs, dryrun, verbose):
        self._config = config
        self.date = date
        self.doStubs = doStubs
        self.doRevs = doRevs
        self.dryrun = dryrun
        self.verbose = verbose

    def doRunOnAllWikis(self):
        retCodes = DumpResults()
        failures = 0
        todos = 0
        for w in self._config.allWikisList:
            dump = IncrDump(config, date, w, doStubs, doRevs, dryrun, self.verbose)
            result = dump.doOneWiki()
            if result == retCodes.FAILED:
                failures = failures + 1
            elif result == retCodes.TODO:
                todos = todos + 1
        return (failures, todos)

    def doAllWikisTilDone(self,numFails):
        fails = 0
        while 1:
            (failures, todos) = self.doRunOnAllWikis()
            if not failures and not todos:
                break
            fails  = fails + 1
            if fails > numFails:
                raise IncrementDumpsError("Too many consecutive failures, giving up")
            # wait 5 minutes and try another loop
#            raise IncrementDumpsError("would sleep")
            time.sleep(300)

def usage(message = None):
    if message:
        print message
        print "Usage: python generateincrementals.py [options] [wikidbname]"
        print "Options: --configfile, --date, --dryrun, --revsonly, --stubsonly, --verbose"
        print "--configfile:  Specify an alternate config file to read. Default file is 'dumpincr.conf' in the current directory."
        print "--date:        (Re)run incremental of a given date (use with care)."
        print "--dryrun:      Don't actually dump anything but print the commands that would be run."
        print "--revsonly:    Do only the stubs part of the dumps."
        print "--stubsonly:   Do only the revision text part of the dumps."
        print "--verbose:     Print error messages and other informative messages (normally the"
        print "               script runs silently)."
        print "wikiname:      Run the dumps only for the specific wiki."
        sys.exit(1)

if __name__ == "__main__":
    configFile = False
    result = False
    date = None
    doStubs = True
    doRevs = True
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 ['date=', 'configfile=', 'stubsonly', 'revsonly', 'dryrun', 'verbose' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--date":
            date = val
        elif opt == "--configfile":
            configFile = val
        elif opt == "--stubsonly":
            doRevs = False
        elif opt == "--revsonly":
            doStubs = False
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        
    if not doRevs and not doStubs:
        usage("You may not specify stubsonly and revsonly options together.")

    if (configFile):
        config = Config(configFile)
    else:
        config = Config()

    if not date:
        date = TimeUtils.today()

    if len(remainder) > 0:
        dump = IncrDump(config, date, remainder[0], doStubs, doRevs, dryrun, verbose)
        dump.doOneWiki()
    else:
        dump = IncrDumpLoop(config, date, doStubs, doRevs, dryrun, verbose)
        dump.doAllWikisTilDone(3)
