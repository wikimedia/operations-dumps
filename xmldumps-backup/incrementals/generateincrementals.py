# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

import getopt
import os
import sys
import time
from IncrDumpLib import Config, RunSimpleCommand, MultiVersion
from IncrDumpLib import DBServer, IncrementDir, IncrementDumpsError
from IncrDumpLib import MaxRevIDFile, StatusFile, IndexFile
from IncrDumpLib import StubFile, RevsFile, MD5File, IncDumpDirs
from IncrDumpLib import IncrDumpLock, StatusInfo
from WikiDump import FileUtils, TimeUtils
from os.path import exists
import hashlib
import traceback
import calendar


class MaxRevID(object):
    def __init__(self, config, date, cutoff, dryrun):
        self._config = config
        self.date = date
        self.cutoff = cutoff
        self.dryrun = dryrun
        self.maxID = None

    def getMaxRevID(self, wikiName):
        query = ("select rev_id from revision where rev_timestamp < \"%s\" "
                 "order by rev_timestamp desc limit 1" % self.cutoff)
        db = DBServer(self._config, wikiName)
        self.maxID = RunSimpleCommand.runWithOutput(db.buildSqlCommand(query),
                                                    shell=True)

    def recordMaxRevID(self, wikiName):
        self.getMaxRevID(wikiName)
        if not self.dryrun:
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
                log(self.verbose, "result for wiki %s is %s"
                    % (w, result))
                text = text + "<li>" + result + "</li>\n"
        indexText = (self._config.readTemplate("incrs-index.html")
                     % {"items": text})
        FileUtils.writeFileInPlace(self.indexFile.getPath(),
                                   indexText, self._config.fileperms)

    def doOneWiki(self, w, date=None):
        if (w not in self._config.privateWikisList and
                w not in self._config.closedWikisList):
            incrDumpsDirs = IncDumpDirs(self._config, w)
            if not exists(self.incrDir.getIncDirNoDate(w)):
                log(self.verbose, "No dump for wiki %s" % w)
                return
            if date is not None:
                incrDate = date
            else:
                incrDate = incrDumpsDirs.getLatestIncrDate(True)
            if not incrDate:
                log(self.verbose, "No dump for wiki %s" % w)
                return

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
                log(self.verbose, "stub for %s %s %s"
                    % (w, safe(stubDate), safe(stubSize)))
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
                log(verbose, "revs for %s %s %s"
                    % (w, safe(revsDate), safe(revsSize)))
                if revsDate:
                    revsText = ("revs: %s (size %s)"
                                % (Link.makeLink(os.path.join(
                                    w, incrDate, revs.getFileName()),
                                    revsDate), revsSize))
                else:
                    revsText = None

                stat = StatusFile(self._config, incrDate, w)
                statContents = FileUtils.readFile(stat.getPath())
                log(self.verbose, "status for %s %s" % (w, safe(statContents)))
                if statContents:
                    statText = "(%s)" % (statContents)
                else:
                    statText = None

            except:
                log(self.verbose, "Error encountered, no information available"
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
                log(self.verbose, "Error encountered formatting information"
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
        self.maxRevIDObj = MaxRevID(self._config, self.date, cutoff,
                                    self.dryrun)
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
                log(self.verbose, "wiki %s skipped, adds/changes dump already"
                    " complete" % self.wikiName)
                return retCodes.OK

            if not dryrun:
                lock = IncrDumpLock(self._config, self.date, self.wikiName)
                if not lock.getLock():
                    log(self.verbose, "wiki %s skipped, wiki is locked,"
                        " another process should be doing the job"
                        % self.wikiName)
                    return retCodes.TODO

                self.incrDumpsDirs.cleanupOldIncrDumps(self.date)

            log(self.verbose, "Doing run for wiki: %s" % self.wikiName)

            try:
                maxRevID = self.dumpMaxRevID()
                if not maxRevID:
                    return retCodes.FAILED

                prevRevID = self.getPrevRevID(maxRevID)
                if not prevRevID:
                    return retCodes.FAILED

                if doStubs:
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
        log(self.verbose, "Success!  Wiki %s incremental dump complete."
            % self.wikiName)
        return retCodes.OK

    def dumpMaxRevID(self):
        if not self.maxRevIDObj.exists(self.wikiName):
            log(self.verbose, "Wiki %s retrieving max revid from db."
                % self.wikiName)
            self.maxRevIDObj.recordMaxRevID(self.wikiName)
            maxRevID = self.maxRevIDObj.maxID
        else:
            maxRevID = self.maxRevIDObj.readMaxRevIDFromFile(
                self.wikiName)

        # end rev id is not included in dump
        if maxRevID is not None:
            maxRevID = str(int(maxRevID) + 1)

        log(self.verbose, "maxRevID is %s" % safe(maxRevID))
        return maxRevID

    def getPrevRevID(self, maxRevID):
        # get the previous rundate, with or without maxrevid file
        # we can populate that file if need be
        prevDate = self.incrDumpsDirs.getPrevIncrDate(self.date)
        log(self.verbose, "prevDate is %s" % safe(prevDate))

        prevRevID = None

        if prevDate:
            prevRevID = self.maxRevIDObj.readMaxRevIDFromFile(
                self.wikiName, prevDate)

            if prevRevID is None:
                log(self.verbose, "Wiki %s retrieving prevRevId from db."
                    % self.wikiName)
                prevRevIDObj = MaxRevID(self._config, prevDate,
                                        cutoffFromDate(prevDate),
                                        self.dryrun)
                prevRevIDObj.recordMaxRevID(self.wikiName)
                prevRevID = prevRevIDObj.maxID
        else:
            log(self.verbose, "Wiki %s no previous runs, using %s - 10 "
                % (self.wikiName, maxRevID))
            prevRevID = str(int(maxRevID) - 10)
            if int(prevRevID) < 1:
                prevRevID = str(1)

        # this incr will cover every revision from the last
        # incremental through the maxid we wrote out already.
        if prevRevID is not None:
            prevRevID = str(int(prevRevID) + 1)
        log(self.verbose, "prevRevID is %s" % safe(prevRevID))
        return prevRevID

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
                log(self.verbose, "error producing stub files for wiki"
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
                log(self.verbose, "error producing revision text files"
                    " for wiki" % self.wikiName)
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


def log(verbose, message):
    if verbose:
        print message


def safe(item):
    if item is not None:
        return item
    else:
        return "None"


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


def cutoffFromDate(date):
    return time.strftime("%Y%m%d%H%M%S",
                         time.gmtime(calendar.timegm(time.strptime(
                             date + "235900UTC", "%Y%m%d%H%M%S%Z"))
                             - config.delay))


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
        cutoff = cutoffFromDate(date)

    if len(remainder) > 0:
        dump = IncrDump(config, date, cutoff, remainder[0], doStubs,
                        doRevs, doIndexUpdate, dryrun, verbose, forcerun)
        dump.doOneWiki()
    else:
        dump = IncrDumpLoop(config, date, cutoff, doStubs, doRevs,
                            doIndexUpdate, dryrun, verbose, forcerun)
        dump.doAllWikisTilDone(3)
