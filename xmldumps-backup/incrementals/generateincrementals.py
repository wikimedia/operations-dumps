# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

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
from IncrDumpLib import Lock, Config, RunSimpleCommand, MultiVersion
from IncrDumpLib import DBServer, IncrementDir, IncrementDumpsError
from IncrDumpLib import MaxRevIDFile, StatusFile, IndexFile, IncrDumpLockFile
from IncrDumpLib import StubFile, RevsFile, MD5File, IncDumpDirs
from IncrDumpLib import IncrDumpLock, MaxRevIDLock, StatusInfo
from subprocess import Popen, PIPE
from os.path import exists
import hashlib
import traceback
import calendar


class MaxRevID(object):
    def __init__(self, config, date, cutoff):
        self._config = config
        self.date = date
        self.cutoff = cutoff
        self.maxID = 0

    def getMaxRevID(self, wikiName):
        query = ("select rev_id from revision where rev_timestamp < \"%s\" "
                 "order by rev_timestamp desc limit 1" % self.cutoff)
        db = DBServer(self._config, wikiName)
        # get the result
        c = db.buildSqlCommand(query)
        self.maxID = RunSimpleCommand.runWithOutput(db.buildSqlCommand(query),
                                                    shell=True)

    def recordMaxRevID(self, wikiName):
        self.getMaxRevID(wikiName)
        fileObj = MaxRevIDFile(self._config, self.date, wikiName)
        FileUtils.writeFileInPlace(fileObj.getPath(), self.maxID,
                                   self._config.fileperms)

    def readMaxRevIDFromFile(self, wikiName, date=None):
        if date is None:
            date = self.date
        try:
            fileObj = MaxRevIDFile(self._config, date, wikiName)
            return FileUtils.readFile(fileObj.getPath().rstrip())
        except:
            return None

    def exists(self, wikiName, date=None):
        if date is None:
            date = self.date
        return exists(MaxRevIDFile(self._config, date, wikiName).getPath())


class Link(object):

    def makeLink(path, linkText):
        return('<a href = "' + path + '">' + linkText + "</a>")

    makeLink = staticmethod(makeLink)


class Index(object):
    def __init__(self, config, date, verbose):
        self._config = config
        self.date = date
        self.indexFile = IndexFile(self._config)
        self.incrDir = IncrementDir(self._config)
        self.verbose = verbose

    def doAllWikis(self):
        text = ""
        for w in self._config.allWikisList:
            result = self.doOneWiki(w)
            if result:
                if (self.verbose):
                    print "result for wiki ", w, "is ", result
                text = text + "<li>" + result + "</li>\n"
        indexText = (self._config.readTemplate("incrs-index.html")
                     % {"items": text})
        FileUtils.writeFileInPlace(self.indexFile.getPath(),
                                   indexText, self._config.fileperms)

    def doOneWiki(self, w):
        if (w not in self._config.privateWikisList and
                w not in self._config.closedWikisList):
            self.incrDumpsDirs = IncDumpDirs(self._config, w)
            if not exists(self.incrDir.getIncDirNoDate(w)):
                if (self.verbose):
                    print "No dump for wiki ", w
                    next
            if date:
                incrDate = date
            else:
                incrDate = self.incrDumpsDirs.getLatestIncrDate(True)
            if not incrDate:
                if (self.verbose):
                    print "No dump for wiki ", w
                    next

            otherRunsText = "other runs: %s" % Link.makeLink(w, w)
            try:
                lock = IncrDumpLock(self._config, incrDate, w)
                lockDate = lock.getLockInfo()
            except:
                lockDate = None
            if lockDate is not None:
                lockText = "run started on %s." % lockDate
            else:
                lockText = None

            try:
                stub = StubFile(self._config, incrDate, w)
                (stubDate, stubSize) = stub.getFileInfo()
                if verbose:
                    print "stub for", w, stubDate, stubSize
                if stubDate:
                    stubText = ("stubs: %s (size %s)"
                                % (Link.makeLink(
                                    os.path.join(
                                        w, incrDate,
                                        stub.getFileName()),
                                    stubDate), stubSize))
                else:
                    stubText = None

                revs = RevsFile(self._config, incrDate, w)
                (revsDate, revsSize) = revs.getFileInfo()
                if verbose:
                    print "revs for", w, revsDate, revsSize
                if revsDate:
                    revsText = ("revs: %s (size %s)"
                                % (Link.makeLink(os.path.join(
                                    w, incrDate, revs.getFileName()),
                                    revsDate), revsSize))
                else:
                    revsText = None

                stat = StatusFile(self._config, incrDate, w)
                statContents = FileUtils.readFile(stat.getPath())
                if verbose:
                    print "status for", w, statContents
                if statContents:
                    statText = "(%s)" % (statContents)
                else:
                    statText = None

            except:
                if (self.verbose):
                    print ("Error encountered, no information available"
                           " for wiki %s" % w)
                return ("<strong>%s</strong> Error encountered,"
                        " no information available | %s" % (w, otherRunsText))

            try:
                wikinameText = "<strong>%s</strong>" % w

                wikiInfo = (" ".join(filter(None,
                                            [wikinameText,
                                             lockText, statText]))
                            + "<br />")
                wikiInfo = (wikiInfo + " &nbsp;&nbsp; " +
                            " |  ".join(filter(None,
                                               [stubText, revsText,
                                                otherRunsText])))
            except:
                if (self.verbose):
                    traceback.print_exc(file=sys.stdout)
                    print ("Error encountered formatting information"
                           " for wiki %s" % w)
                return ("Error encountered formatting information"
                        " for wiki %s" % w)

            return wikiInfo


class DumpResults(object):
    def __init__(self):
        self.TODO = 1
        self.FAILED = -1
        self.OK = 0


class IncrDump(object):
    def __init__(self, config, date, cutoff, wikiName, doStubs,
                 doRevs, doIndexUpdate, dryrun, verbose, forcerun):
        self._config = config
        self.date = date
        self.cutoff = cutoff
        self.wikiName = wikiName
        self.incrDir = IncrementDir(self._config, self.date)
        self.doStubs = doStubs
        self.doRevs = doRevs
        self.doIndexUpdate = doIndexUpdate
        self.dryrun = dryrun
        self.forcerun = forcerun
        self.maxRevIDObj = MaxRevID(self._config, self.date, cutoff)
        self.statusInfo = StatusInfo(self._config, self.date, self.wikiName)
        self.stubFile = StubFile(self._config, self.date, self.wikiName)
        self.revsFile = RevsFile(self._config, self.date, self.wikiName)
        self.incrDumpsDirs = IncDumpDirs(self._config, self.wikiName)
        self.verbose = verbose

    def doOneWiki(self):
        retCodes = DumpResults()
        if (self.wikiName not in self._config.privateWikisList and
                self.wikiName not in self._config.closedWikisList):
            if not exists(self.incrDir.getIncDir(self.wikiName)):
                os.makedirs(self.incrDir.getIncDir(self.wikiName))
            status = self.statusInfo.getStatus()
            if status == "done" and not forcerun:
                if (self.verbose):
                    print ("wiki %s skipped, adds/changes dump already"
                           " complete" % self.wikiName)
                return retCodes.OK
            if not dryrun:
                lock = IncrDumpLock(self._config, self.date, self.wikiName)
                if not lock.getLock():
                    if (self.verbose):
                        print ("wiki %s skipped, wiki is locked, another"
                               " process should be doing the job"
                               % self.wikiName)
                    return retCodes.TODO
                if not dryrun:
                    self.incrDumpsDirs.cleanupOldIncrDumps(self.date)
                    try:
                        if not self.maxRevIDObj.exists(self.wikiName):
                            if self.verbose:
                                print ("Wiki %s retrieving max revid from db."
                                       % self.wikiName)
                            self.maxRevIDObj.recordMaxRevID(self.wikiName)
                    except:
                        if (self.verbose):
                            print ("Wiki %s failed to get max revid."
                                   % self.wikiName)
                            traceback.print_exc(file=sys.stdout)

            try:
                maxRevID = self.maxRevIDObj.readMaxRevIDFromFile(self.wikiName)
                if (self.verbose):
                    print "Doing run for wiki: ", self.wikiName
                    if maxRevID:
                        print "maxRevID is ", maxRevID
                    else:
                        print "no maxRevID found"
                # get the previous run with a max rev id file in it
                prevDate = self.incrDumpsDirs.getPrevIncrDate(self.date,
                                                              revidok=True)
                if (self.verbose):
                    if prevDate:
                        print "prevDate is", prevDate
                    else:
                        print "no prevDate found"
                prevRevID = None
                if prevDate:
                    prevRevID = self.maxRevIDObj.readMaxRevIDFromFile(
                        self.wikiName, prevDate)
                    if (self.verbose):
                        if prevRevID:
                            print "prevRevId is ", prevRevID
                        else:
                            print "no prevRevID found"
                if not prevRevID:
                    prevRevID = str(int(maxRevID) - 10)
                    if int(prevRevID) < 1:
                        prevRevID = str(1)
                else:
                    # this incr will cover every revision from the last
                    # incremental through the maxid we wrote out already.
                    prevRevID = str(int(prevRevID) + 1)
                if doStubs:
                    # end rev id is not included in dump
                    maxRevID = str(int(maxRevID) + 1)
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
                if doIndexUpdate:
                    index = Index(config, date, verbose)
                    index.doAllWikis()
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
        scriptCommand = MultiVersion.MWScriptAsArray(self._config,
                                                     "dumpBackup.php")
        command = ["%s" % self._config.php, "-q"]
        command.extend(scriptCommand)
        command.extend(["--wiki=%s" % self.wikiName, "--stub", "--quiet",
                        "--force-normal",
                        "--output=gzip:%s" % self.stubFile.getPath(),
                        "--revrange", "--revstart=%s" % startRevID,
                        "--revend=%s" % endRevID])
        if dryrun:
            print "would run command for stubs dump:", command
        else:
            error = RunSimpleCommand.runWithNoOutput(command, shell=False)
            if (error):
                if (self.verbose):
                    print ("error producing stub files for wiki"
                           % self.wikiName)
                return False
        return True

    def dumpRevs(self):
        scriptCommand = MultiVersion.MWScriptAsArray(self._config,
                                                     "dumpTextPass.php")
        command = ["%s" % self._config.php, "-q"]
        command.extend(scriptCommand)
        command.extend(["--wiki=%s" % self.wikiName,
                        "--stub=gzip:%s" % self.stubFile.getPath(),
                        "--force-normal", "--quiet",
                        "--spawn=%s" % self._config.php,
                        "--output=bzip2:%s" % self.revsFile.getPath()])
        if dryrun:
            print "would run command for revs dump:", command
        else:
            error = RunSimpleCommand.runWithNoOutput(command, shell=False)
            if (error):
                if (self.verbose):
                    print("error producing revision text files for wiki"
                          % self.wikiName)
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
                FileUtils.writeFileInPlace(md5File.getPath(),
                                           text, self._config.fileperms)
            return True
        except:
            return False


class IncrDumpLoop(object):
    def __init__(self, config, date, cutoff, doStubs, doRevs,
                 doIndexUpdate, dryrun, verbose, forcerun):
        self._config = config
        self.date = date
        self.cutoff = cutoff
        self.doStubs = doStubs
        self.doRevs = doRevs
        self.doIndexUpdate = doIndexUpdate
        self.dryrun = dryrun
        self.verbose = verbose
        self.forcerun = forcerun

    def doRunOnAllWikis(self):
        retCodes = DumpResults()
        failures = 0
        todos = 0
        for w in self._config.allWikisList:
            dump = IncrDump(self._config, self.date, self.cutoff, w,
                            self.doStubs, self.doRevs, self.doIndexUpdate,
                            self.dryrun, self.verbose, self.forcerun)
            result = dump.doOneWiki()
            if result == retCodes.FAILED:
                failures = failures + 1
            elif result == retCodes.TODO:
                todos = todos + 1
        return (failures, todos)

    def doAllWikisTilDone(self, numFails):
        fails = 0
        while 1:
            (failures, todos) = self.doRunOnAllWikis()
            if not failures and not todos:
                break
            fails = fails + 1
            if fails > numFails:
                raise IncrementDumpsError("Too many consecutive failures,"
                                          "giving up")
            time.sleep(300)


def usage(message=None):
    if message:
        print message
    usage_message = (
        """Usage: python generateincrementals.py [options] [wikidbname]

Options: --configfile, --date, --dryrun, --revsonly, --stubsonly, --verbose"

 --configfile:  Specify an alternate config file to read. Default
                file is 'dumpincr.conf' in the current directory."
 --date:        (Re)run incremental of a given date (use with care)."
 --dryrun:      Don't dump anything but print the commands that would be run."
 --forcerun:    Do the run even if there is already a successful run in place."
 --revsonly:    Do only the stubs part of the dumps."
 --stubsonly:   Do only the revision text part of the dumps."
 --verbose:     Print error messages and other informative messages"
                (normally the script runs silently)."

 wikidbname:    Run the dumps only for the specific wiki.
""")
    sys.stderr.write(usage_message)
    sys.exit(1)

if __name__ == "__main__":
    configFile = False
    result = False
    date = None
    doStubs = True
    doRevs = True
    doIndexUpdate = True
    dryrun = False
    verbose = False
    forcerun = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ['date=', 'configfile=', 'stubsonly', 'revsonly',
             'indexonly', 'dryrun', 'verbose', 'forcerun'])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--date":
            date = val
        elif opt == "--configfile":
            configFile = val
        elif opt == "--stubsonly":
            doRevs = False
            doIndexUpdate = False
        elif opt == "--revsonly":
            doStubs = False
            doIndexUpdate = False
        elif opt == "--indexonly":
            doStubs = False
            doRevs = False
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--forcerun":
            forcerun = True

    if not doRevs and not doStubs and not doIndexUpdate:
        usage("You may not specify more than one of stubsonly,"
              "revsonly and indexonly together.")

    if (configFile):
        config = Config(configFile)
    else:
        config = Config()

    if not date:
        date = TimeUtils.today()
        cutoff = time.strftime("%Y%m%d%H%M%S",
                               time.gmtime(time.time() - config.delay))
    else:
        cutoff = time.strftime("%Y%m%d%H%M%S",
                               time.gmtime(calendar.timegm(time.strptime(
                                   date + "235900UTC", "%Y%m%d%H%M%S%Z"))
                                   - config.delay))

    if len(remainder) > 0:
        dump = IncrDump(config, date, cutoff, remainder[0], doStubs,
                        doRevs, doIndexUpdate, dryrun, verbose, forcerun)
        dump.doOneWiki()
    else:
        dump = IncrDumpLoop(config, date, cutoff, doStubs, doRevs,
                            doIndexUpdate, dryrun, verbose, forcerun)
        dump.doAllWikisTilDone(3)
