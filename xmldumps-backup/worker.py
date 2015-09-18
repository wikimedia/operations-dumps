# Worker process, does the actual dumping

import getopt, hashlib, os, re, sys, errno, time
import subprocess, select
import shutil, stat, signal, glob
import Queue, thread, traceback, socket

from os.path import exists
from subprocess import Popen, PIPE
from dumps.WikiDump import FileUtils, MiscUtils, TimeUtils, Wiki, Config, cleanup
from dumps.CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel
from dumps.jobs import *
from dumps.runnerutils import *
from dumps.utils import DbServerInfo

class Logger(object):

    def __init__(self, logFileName=None):
        if logFileName:
            self.logFile = open(logFileName, "a")
        else:
            self.logFile = None
        self.queue = Queue.Queue()
        self.JobsDone = "JOBSDONE"

    def logWrite(self, line=None):
        if self.logFile:
            self.logFile.write(line)
            self.logFile.flush()

    def logClose(self):
        if self.logFile:
            self.logFile.close()

    # return 1 if logging terminated, 0 otherwise
    def doJobOnLogQueue(self):
        line = self.queue.get()
        if line == self.JobsDone:
            self.logClose()
            return 1
        else:
            self.logWrite(line)
            return 0

    def addToLogQueue(self, line=None):
        if line:
            self.queue.put_nowait(line)

    # set in order to have logging thread clean up and exit
    def indicateJobsDone(self):
        self.queue.put_nowait(self.JobsDone)

class DumpItemList(object):
    def __init__(self, wiki, prefetch, spawn, chunkToDo, checkpointFile, singleJob, skipJobs, chunkInfo, pageIDRange, runInfoFile, dumpDir):
        self.wiki = wiki
        self._hasFlaggedRevs = self.wiki.hasFlaggedRevs()
        self._hasWikidata = self.wiki.hasWikidata()
        self._isWikidataClient = self.wiki.isWikidataClient()
        self._prefetch = prefetch
        self._spawn = spawn
        self.chunkInfo = chunkInfo
        self.checkpointFile = checkpointFile
        self._chunkToDo = chunkToDo
        self._singleJob = singleJob
        self.skipJobs = skipJobs
        self._runInfoFile = runInfoFile
        self.dumpDir = dumpDir
        self.pageIDRange = pageIDRange

        if self.wiki.config.checkpointTime:
            checkpoints = True
        else:
            checkpoints = False

        if self._singleJob and self._chunkToDo:
            if (self._singleJob[-5:] == 'table' or
                self._singleJob[-9:] == 'recombine' or
                            self._singleJob == 'createdirs' or
                self._singleJob == 'noop' or
                self._singleJob == 'latestlinks' or
                self._singleJob == 'xmlpagelogsdump' or
                self._singleJob == 'pagetitlesdump' or
                self._singleJob == 'allpagetitlesdump' or
                self._singleJob.endswith('recombine')):
                raise BackupError("You cannot specify a chunk with the job %s, exiting.\n" % self._singleJob)

        if self._singleJob and self.checkpointFile:
            if (self._singleJob[-5:] == 'table' or
                self._singleJob[-9:] == 'recombine' or
                self._singleJob == 'noop' or
                self._singleJob == 'createdirs' or
                self._singleJob == 'latestlinks' or
                self._singleJob == 'xmlpagelogsdump' or
                self._singleJob == 'pagetitlesdump' or
                self._singleJob == 'allpagetitlesdump' or
                self._singleJob == 'abstractsdump' or
                self._singleJob == 'xmlstubsdump' or
                self._singleJob.endswith('recombine')):
                raise BackupError("You cannot specify a checkpoint file with the job %s, exiting.\n" % self._singleJob)

        self.dumpItems = [PrivateTable("user", "usertable", "User account data."),
            PrivateTable("watchlist", "watchlisttable", "Users' watchlist settings."),
            PrivateTable("ipblocks", "ipblockstable", "Data for blocks of IP addresses, ranges, and users."),
            PrivateTable("archive", "archivetable", "Deleted page and revision data."),
#            PrivateTable("updates", "updatestable", "Update dataset for OAI updater system."),
            PrivateTable("logging", "loggingtable", "Data for various events (deletions, uploads, etc)."),
            PrivateTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded images."),
            #PrivateTable("filearchive", "filearchivetable", "Deleted image data"),

            PublicTable("site_stats", "sitestatstable", "A few statistics such as the page count."),
            PublicTable("image", "imagetable", "Metadata on current versions of uploaded media/files."),
            #PublicTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded media/files."),
            PublicTable("pagelinks", "pagelinkstable", "Wiki page-to-page link records."),
            PublicTable("categorylinks", "categorylinkstable", "Wiki category membership link records."),
            PublicTable("imagelinks", "imagelinkstable", "Wiki media/files usage records."),
            PublicTable("templatelinks", "templatelinkstable", "Wiki template inclusion link records."),
            PublicTable("externallinks", "externallinkstable", "Wiki external URL link records."),
            PublicTable("langlinks", "langlinkstable", "Wiki interlanguage link records."),
            #PublicTable("interwiki", "interwikitable", "Set of defined interwiki prefixes and links for this wiki."),
            PublicTable("user_groups", "usergroupstable", "User group assignments."),
            PublicTable("category", "categorytable", "Category information."),

            PublicTable("page", "pagetable", "Base per-page data (id, title, old restrictions, etc)."),
            PublicTable("page_restrictions", "pagerestrictionstable", "Newer per-page restrictions table."),
            PublicTable("page_props", "pagepropstable", "Name/value pairs for pages."),
            PublicTable("protected_titles", "protectedtitlestable", "Nonexistent pages that have been protected."),
            #PublicTable("revision", #revisiontable", "Base per-revision data (does not include text)."), // safe?
            #PrivateTable("text", "texttable", "Text blob storage. May be compressed, etc."), // ?
            PublicTable("redirect", "redirecttable", "Redirect list"),
            PublicTable("iwlinks", "iwlinkstable", "Interwiki link tracking records"),
            PublicTable("geo_tags", "geotagstable", "List of pages' geographical coordinates"),

            TitleDump("pagetitlesdump", "List of page titles in main namespace"),
            AllTitleDump("allpagetitlesdump", "List of all page titles"),

            AbstractDump("abstractsdump", "Extracted page abstracts for Yahoo", self._getChunkToDo("abstractsdump"), self.wiki.dbName, self.chunkInfo.getPagesPerChunkAbstract())]

        if self.chunkInfo.chunksEnabled():
            self.dumpItems.append(RecombineAbstractDump("abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo", self.findItemByName('abstractsdump')))

        self.dumpItems.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps", self._getChunkToDo("xmlstubsdump"), self.chunkInfo.getPagesPerChunkHistory()))
        if self.chunkInfo.chunksEnabled():
            self.dumpItems.append(RecombineXmlStub("xmlstubsdumprecombine", "Recombine first-pass for page XML data dumps", self.findItemByName('xmlstubsdump')))

        # NOTE that the chunkInfo thing passed here is irrelevant, these get generated from the stubs which are all done in one pass
        self.dumpItems.append(
            XmlDump("articles",
                "articlesdump",
                "<big><b>Articles, templates, media/file descriptions, and primary meta-pages.</b></big>",
                "This contains current versions of article content, and is the archive most mirror sites will probably want.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("articlesdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))
        if self.chunkInfo.chunksEnabled():
            self.dumpItems.append(RecombineXmlDump("articlesdumprecombine", "<big><b>Recombine articles, templates, media/file descriptions, and primary meta-pages.</b></big>", "This contains current versions of article content, and is the archive most mirror sites will probably want.", self.findItemByName('articlesdump')))

        self.dumpItems.append(
            XmlDump("meta-current",
                "metacurrentdump",
                "All pages, current versions only.",
                "Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("metacurrentdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))

        if self.chunkInfo.chunksEnabled():
            self.dumpItems.append(RecombineXmlDump("metacurrentdumprecombine", "Recombine all pages, current versions only.", "Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.findItemByName('metacurrentdump')))

        self.dumpItems.append(
            XmlLogging("Log events to all pages and users."))

        if self._hasFlaggedRevs:
            self.dumpItems.append(
                PublicTable("flaggedpages", "flaggedpagestable", "This contains a row for each flagged article, containing the stable revision ID, if the lastest edit was flagged, and how long edits have been pending."))
            self.dumpItems.append(
                PublicTable("flaggedrevs", "flaggedrevstable", "This contains a row for each flagged revision, containing who flagged it, when it was flagged, reviewer comments, the flag values, and the quality tier those flags fall under."))

        if self._hasWikidata:
            self.dumpItems.append(
                PublicTable("wb_items_per_site", "wbitemspersitetable", "For each Wikidata item, this contains rows with the corresponding page name on a given wiki project."))
            self.dumpItems.append(
                PublicTable("wb_terms", "wbtermstable", "For each Wikidata item, this contains rows with a label, an alias and a description of the item in a given language."))
            self.dumpItems.append(
                PublicTable("wb_entity_per_page", "wbentityperpagetable", "Contains a mapping of page ids and entity ids, with an additional entity type column."))
            self.dumpItems.append(
                PublicTable("wb_property_info", "wbpropertyinfotable", "Contains a mapping of Wikidata property ids and data types."))
            self.dumpItems.append(
                PublicTable("wb_changes_subscription", "wbchangessubscriptiontable", "Tracks which Wikibase Client wikis are using which items."))
            self.dumpItems.append(
                PublicTable("sites", "sitestable", "This contains the SiteMatrix information from meta.wikimedia.org provided as a table."))

        if self._isWikidataClient:
            self.dumpItems.append(
                PublicTable("wbc_entity_usage", "wbcentityusagetable", "Tracks which pages use which Wikidata items or properties and what aspect (e.g. item label) is used."))

        self.dumpItems.append(
            BigXmlDump("meta-history",
                   "metahistorybz2dump",
                   "All pages with complete page edit history (.bz2)",
                   "These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
                   "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("metahistorybz2dump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))
        if self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory():
            self.dumpItems.append(
                RecombineXmlDump("metahistorybz2dumprecombine",
                         "Recombine all pages with complete edit history (.bz2)",
                         "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                         "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistorybz2dump')))
        self.dumpItems.append(
            XmlRecompressDump("meta-history",
                      "metahistory7zdump",
                      "All pages with complete edit history (.7z)",
                      "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                      "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistorybz2dump'), self.wiki, self._getChunkToDo("metahistory7zdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile))
        if self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory():
            self.dumpItems.append(
                RecombineXmlRecompressDump("metahistory7zdumprecombine",
                               "Recombine all pages with complete edit history (.7z)",
                               "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                               "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistory7zdump'), self.wiki))
        # doing this only for recombined/full articles dump
        if self.wiki.config.multistreamEnabled:
            if self.chunkInfo.chunksEnabled():
                inputForMultistream = "articlesdumprecombine"
            else:
                inputForMultistream = "articlesdump"
            self.dumpItems.append(
                XmlMultiStreamDump("articles",
                       "articlesmultistreamdump",
                       "Articles, templates, media/file descriptions, and primary meta-pages, in multiple bz2 streams, 100 pages per stream",
                       "This contains current versions of article content, in concatenated bz2 streams, 100 pages per stream, plus a separate" +
                       "index of page titles/ids and offsets into the file.  Useful for offline readers, or for parallel processing of pages.",
                       self.findItemByName(inputForMultistream), self.wiki, None))

        results = self._runInfoFile.getOldRunInfoFromFile()
        if results:
            for runInfoObj in results:
                self._setDumpItemRunInfo(runInfoObj)
            self.oldRunInfoRetrieved = True
        else:
            self.oldRunInfoRetrieved = False

        def appendJob(self, jobname, job):
                if jobname not in self.skipJobs:
                        self.dumpItems.append(job)

    def reportDumpRunInfo(self, done=False):
        """Put together a dump run info listing for this database, with all its component dumps."""
        runInfoLines = [self._reportDumpRunInfoLine(item) for item in self.dumpItems]
        runInfoLines.reverse()
        text = "\n".join(runInfoLines)
        text = text + "\n"
        return text

    def allPossibleJobsDone(self, skipJobs):
        for item in self.dumpItems:
            if (item.status() != "done" and item.status() != "failed"
                            and item.status() != "skipped"):
                return False
        return True

    # determine list of dumps to run ("table" expands to all table dumps,
    # the rest of the names expand to single items)
    # and mark the items in the list as such
    # return False if there is no such dump or set of dumps
    def markDumpsToRun(self, job, skipgood=False):
        if job == "tables":
            for item in self.dumpItems:
                if item.name()[-5:] == "table":
                    if item.name in self.skipJobs:
                        item.setSkipped()
                    elif not skipgood or item.status() != "done":
                        item.setToBeRun(True)
            return True
        else:
            for item in self.dumpItems:
                if item.name() == job:
                    if item.name in self.skipJobs:
                        item.setSkipped()
                    elif not skipgood or item.status() != "done":
                        item.setToBeRun(True)
                    return True
        if job == "noop" or job == "latestlinks" or job == "createdirs":
            return True
        sys.stderr.write("No job of the name specified exists. Choose one of the following:\n")
        sys.stderr.write("noop (runs no job but rewrites md5sums file and resets latest links)\n")
        sys.stderr.write("latestlinks (runs no job but resets latest links)\n")
        sys.stderr.write("createdirs (runs no job but creates dump dirs for the given date)\n")
        sys.stderr.write("tables (includes all items below that end in 'table')\n")
        for item in self.dumpItems:
            sys.stderr.write("%s\n" % item.name())
            return False

    def markFollowingJobsToRun(self, skipgood=False):
        # find the first one marked to run, mark the following ones
        i = 0;
        for item in self.dumpItems:
            i = i + 1;
            if item.toBeRun():
                for j in range(i, len(self.dumpItems)):
                                        if item.name in self.skipJobs:
                                                item.setSkipped()
                                        elif not skipgood or item.status() != "done":
                                                self.dumpItems[j].setToBeRun(True)
                break

    def markAllJobsToRun(self, skipgood=False):
        """Marks each and every job to be run"""
        for item in self.dumpItems:
                        if item.name() in self.skipJobs:
                                item.setSkipped()
                        elif not skipgood or item.status() != "done":
                                item.setToBeRun(True)

    def findItemByName(self, name):
        for item in self.dumpItems:
            if item.name() == name:
                return item
        return None

    def _getChunkToDo(self, jobName):
        if self._singleJob:
            if self._singleJob == jobName:
                return(self._chunkToDo)
        return(False)

    # read in contents from dump run info file and stuff into dumpItems for later reference
    def _setDumpItemRunInfo(self, runInfo):
        if not runInfo.name():
            return False
        for item in self.dumpItems:
            if item.name() == runInfo.name():
                item.setStatus(runInfo.status(), False)
                item.setUpdated(runInfo.updated())
                item.setToBeRun(runInfo.toBeRun())
                return True
        return False

    # write dump run info file
    # (this file is rewritten with updates after each dumpItem completes)
    def _reportDumpRunInfoLine(self, item):
        # even if the item has never been run we will at least have "waiting" in the status
        return "name:%s; status:%s; updated:%s" % (item.name(), item.status(), item.updated())


class Runner(object):
    def __init__(self, wiki, prefetch=True, spawn=True, job=None, skipJobs=None, restart=False, notice="", dryrun=False, loggingEnabled=False, chunkToDo=False, checkpointFile=None, pageIDRange=None, skipdone=False, verbose=False):
        self.wiki = wiki
        self.dbName = wiki.dbName
        self.prefetch = prefetch
        self.spawn = spawn
        self.chunkInfo = Chunk(wiki, self.dbName, self.logAndPrint)
        self.restart = restart
        self.htmlNoticeFile = None
        self.log = None
        self.dryrun = dryrun
        self._chunkToDo = chunkToDo
        self.checkpointFile = checkpointFile
        self.pageIDRange = pageIDRange
        self.skipdone = skipdone
        self.verbose = verbose

        if self.checkpointFile:
            f = DumpFilename(self.wiki)
            f.newFromFilename(checkpointFile)
            # we should get chunk if any
            if not self._chunkToDo and f.chunkInt:
                self._chunkToDo = f.chunkInt
            elif self._chunkToDo and f.chunkInt and self._chunkToDo != f.chunkInt:
                raise BackupError("specifed chunk to do does not match chunk of checkpoint file %s to redo", self.checkpointFile)
            self.checkpointFile = f

        self._loggingEnabled = loggingEnabled
        self._statusEnabled = True
        self._checksummerEnabled = True
        self._runInfoFileEnabled = True
        self._symLinksEnabled = True
        self._feedsEnabled = True
        self._noticeFileEnabled = True
        self._makeDirEnabled = True
        self._cleanOldDumpsEnabled = True
        self._cleanupOldFilesEnabled = True
        self._checkForTruncatedFilesEnabled = True

        if self.dryrun or self._chunkToDo:
            self._statusEnabled = False
            self._checksummerEnabled = False
            self._runInfoFileEnabled = False
            self._symLinksEnabled = False
            self._feedsEnabled = False
            self._noticeFileEnabled = False
            self._makeDirEnabled = False
            self._cleanOldDumpsEnabled = False

        if self.dryrun:
            self._loggingEnabled = False
            self._checkForTruncatedFilesEnabled = False
            self._cleanupOldFilesEnabled = False

        if self.checkpointFile:
            self._statusEnabled = False
            self._checksummerEnabled = False
            self._runInfoFileEnabled = False
            self._symLinksEnabled = False
            self._feedsEnabled = False
            self._noticeFileEnabled = False
            self._makeDirEnabled = False
            self._cleanOldDumpsEnabled = False

        if self.pageIDRange:
            self._statusEnabled = False
            self._checksummerEnabled = False
            self._runInfoFileEnabled = False
            self._symLinksEnabled = False
            self._feedsEnabled = False
            self._noticeFileEnabled = False
            self._makeDirEnabled = False
            self._cleanupOldFilesEnabled = True

        self.jobRequested = job

        self.skipJobs = skipJobs
        if skipJobs is None:
            self.skipJobs = []

        if self.jobRequested == "latestlinks":
            self._statusEnabled = False
            self._runInfoFileEnabled = False

        if self.jobRequested == "createdirs":
            self._symLinksEnabled = False
            self._feedsEnabled = False

        if self.jobRequested == "latestlinks" or self.jobRequested == "createdirs":
            self._checksummerEnabled = False
            self._noticeFileEnabled = False
            self._makeDirEnabled = False
            self._cleanOldDumpsEnabled = False
            self._cleanupOldFilesEnabled = False
            self._checkForTruncatedFilesEnabled = False

        if self.jobRequested == "noop":
            self._cleanOldDumpsEnabled = False
            self._cleanupOldFilesEnabled = False
            self._checkForTruncatedFilesEnabled = False

        self.dbServerInfo = DbServerInfo(self.wiki, self.dbName, self.logAndPrint)
        self.dumpDir = DumpDir(self.wiki, self.dbName)

        # these must come after the dumpdir setup so we know which directory we are in
        if self._loggingEnabled and self._makeDirEnabled:
            fileObj = DumpFilename(self.wiki)
            fileObj.newFromFilename(self.wiki.config.logFile)
            self.logFileName = self.dumpDir.filenamePrivatePath(fileObj)
            self.makeDir(os.path.join(self.wiki.privateDir(), self.wiki.date))
            self.log = Logger(self.logFileName)
            thread.start_new_thread(self.logQueueReader, (self.log,))
        self.runInfoFile = RunInfoFile(wiki, self._runInfoFileEnabled, self.verbose)
        self.symLinks = SymLinks(self.wiki, self.dumpDir, self.logAndPrint, self.debug, self._symLinksEnabled)
        self.feeds = Feeds(self.wiki, self.dumpDir, self.dbName, self.debug, self._feedsEnabled)
        self.htmlNoticeFile = NoticeFile(self.wiki, notice, self._noticeFileEnabled)
        self.checksums = Checksummer(self.wiki, self.dumpDir, self._checksummerEnabled, self.verbose)

        # some or all of these dumpItems will be marked to run
        self.dumpItemList = DumpItemList(self.wiki, self.prefetch, self.spawn, self._chunkToDo, self.checkpointFile, self.jobRequested, self.skipJobs, self.chunkInfo, self.pageIDRange, self.runInfoFile, self.dumpDir)
        # only send email failure notices for full runs
        if self.jobRequested:
            email = False
        else:
            email = True
        self.status = Status(self.wiki, self.dumpDir, self.dumpItemList.dumpItems, self.checksums, self._statusEnabled, email, self.htmlNoticeFile, self.logAndPrint, self.verbose)

    def logQueueReader(self, log):
        if not log:
            return
        done = False
        while not done:
            done = log.doJobOnLogQueue()

    def logAndPrint(self, message):
        if hasattr(self, 'log') and self.log and self._loggingEnabled:
            self.log.addToLogQueue("%s\n" % message)
        sys.stderr.write("%s\n" % message)

    # returns 0 on success, 1 on error
    def saveCommand(self, commands, outfile):
        """For one pipeline of commands, redirect output to a given file."""
        commands[-1].extend([">", outfile])
        series = [commands]
        if self.dryrun:
            self.prettyPrintCommands([series])
            return 0
        else:
            return self.runCommand([series], callbackTimed = self.status.updateStatusFiles)

    def prettyPrintCommands(self, commandSeriesList):
        for series in commandSeriesList:
            for pipeline in series:
                commandStrings = []
                for command in pipeline:
                    commandStrings.append(" ".join(command))
                pipelineString = " | ".join(commandStrings)
                print "Command to run: ", pipelineString

    # command series list: list of (commands plus args) is one pipeline. list of pipelines = 1 series.
    # this function wants a list of series.
    # be a list (the command name and the various args)
    # If the shell option is true, all pipelines will be run under the shell.
    # callbackinterval: how often we will call callbackTimed (in milliseconds), defaults to every 5 secs
    def runCommand(self, commandSeriesList, callbackStderr=None, callbackStderrArg=None, callbackTimed=None, callbackTimedArg=None, shell=False, callbackInterval=5000):
        """Nonzero return code from the shell from any command in any pipeline will cause this
        function to print an error message and return 1, indicating error.
        Returns 0 on success.
        If a callback function is passed, it will receive lines of
        output from the call.  If the callback function takes another argument (which will
        be passed before the line of output) must be specified by the arg paraemeter.
        If no callback is provided, and no output file is specified for a given
        pipe, the output will be written to stderr. (Do we want that?)
        This function spawns multiple series of pipelines  in parallel.

        """
        if self.dryrun:
            self.prettyPrintCommands(commandSeriesList)
            return 0

        else:
            commands = CommandsInParallel(commandSeriesList, callbackStderr=callbackStderr, callbackStderrArg=callbackStderrArg, callbackTimed=callbackTimed, callbackTimedArg=callbackTimedArg, shell=shell, callbackInterval=callbackInterval)
            commands.runCommands()
            if commands.exitedSuccessfully():
                return 0
            else:
                problemCommands = commands.commandsWithErrors()
                errorString = "Error from command(s): "
                for cmd in problemCommands:
                    errorString = errorString + "%s " % cmd
                self.logAndPrint(errorString)
                return 1

    def debug(self, stuff):
        self.logAndPrint("%s: %s %s" % (TimeUtils.prettyTime(), self.dbName, stuff))

    def runHandleFailure(self):
        if self.status.failCount < 1:
            # Email the site administrator just once per database
            self.status.reportFailure()
        self.status.failCount += 1

    def runUpdateItemFileInfo(self, item):
        # this will include checkpoint files if they are enabled.
        for fileObj in item.listOutputFilesToPublish(self.dumpDir):
            if exists(self.dumpDir.filenamePublicPath(fileObj)):
                # why would the file not exist? because we changed chunk numbers in the
                # middle of a run, and now we list more files for the next stage than there
                # were for earlier ones
                self.symLinks.saveSymlink(fileObj)
                self.feeds.saveFeed(fileObj)
                self.checksums.checksum(fileObj, self)
                self.symLinks.cleanupSymLinks()
                self.feeds.cleanupFeeds()

    def run(self):
        if self.jobRequested:
            if not self.dumpItemList.oldRunInfoRetrieved and self.wiki.existsPerDumpIndex():

                # There was a previous run of all or part of this date, but...
                # There was no old RunInfo to be had (or an error was encountered getting it)
                # so we can't rerun a step and keep all the status information about the old run around.
                # In this case ask the user if they reeeaaally want to go ahead
                print "No information about the previous run for this date could be retrieved."
                print "This means that the status information about the old run will be lost, and"
                print "only the information about the current (and future) runs will be kept."
                reply = raw_input("Continue anyways? [y/N]: ")
                if not reply in ["y", "Y"]:
                    raise RuntimeError("No run information available for previous dump, exiting")

            if not self.dumpItemList.markDumpsToRun(self.jobRequested, self.skipdone):
                # probably no such job
                sys.stderr.write( "No job marked to run, exiting" )
                return None
            if restart:
                # mark all the following jobs to run as well
                self.dumpItemList.markFollowingJobsToRun(self.skipdone)
        else:
            self.dumpItemList.markAllJobsToRun(self.skipdone);

        Maintenance.exitIfInMaintenanceMode("In maintenance mode, exiting dump of %s" % self.dbName)

        self.makeDir(os.path.join(self.wiki.publicDir(), self.wiki.date))
        self.makeDir(os.path.join(self.wiki.privateDir(), self.wiki.date))

        self.showRunnerState("Cleaning up old dumps for %s" % self.dbName)
        self.cleanOldDumps()
        self.cleanOldDumps(private=True)

        # Informing what kind backup work we are about to do
        if self.jobRequested:
            if self.restart:
                self.logAndPrint("Preparing for restart from job %s of %s" % (self.jobRequested, self.dbName))
            else:
                self.logAndPrint("Preparing for job %s of %s" % (self.jobRequested, self.dbName))
        else:
            self.showRunnerState("Starting backup of %s" % self.dbName)

        self.checksums.prepareChecksums()

        for item in self.dumpItemList.dumpItems:
            Maintenance.exitIfInMaintenanceMode("In maintenance mode, exiting dump of %s at step %s" % (self.dbName, item.name()))
            if item.toBeRun():
                item.start(self)
                self.status.updateStatusFiles()
                self.runInfoFile.saveDumpRunInfoFile(self.dumpItemList.reportDumpRunInfo())
                try:
                    item.dump(self)
                except Exception, ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    if self.verbose:
                        sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    else:
                        if exc_type.__name__ == 'BackupPrereqError':
                            self.debug(str(ex))
                        else:
                            self.debug("*** exception! " + str(ex))
                        if exc_type.__name__ != 'BackupPrereqError':
                            item.setStatus("failed")

            if item.status() == "done":
                self.checksums.cpMd5TmpFileToPermFile()
                self.runUpdateItemFileInfo(item)
            elif item.status() == "waiting" or item.status() == "skipped":
                # don't update the md5 file for this item.
                continue
            else:
                # Here for example status is "failed". But maybe also
                # "in-progress", if an item chooses to override dump(...) and
                # forgets to set the status. This is a failure as well.
                self.runHandleFailure()

                # special case
                if self.jobRequested == "createdirs":
                        if not os.path.exists(os.path.join(self.wiki.publicDir(), self.wiki.date)):
                                os.makedirs(os.path.join(self.wiki.publicDir(), self.wiki.date))
                        if not os.path.exists(os.path.join(self.wiki.privateDir(), self.wiki.date)):
                                os.makedirs(os.path.join(self.wiki.privateDir(), self.wiki.date))

        if self.dumpItemList.allPossibleJobsDone(self.skipJobs):
            # All jobs are either in status "done", "waiting", "failed", "skipped"
            self.status.updateStatusFiles("done")
        else:
            # This may happen if we start a dump now and abort before all items are
            # done. Then some are left for example in state "waiting". When
            # afterwards running a specific job, all (but one) of the jobs
            # previously in "waiting" are still in status "waiting"
            self.status.updateStatusFiles("partialdone")

        self.runInfoFile.saveDumpRunInfoFile(self.dumpItemList.reportDumpRunInfo())

        # if any job succeeds we might as well make the sym link
        if self.status.failCount < 1:
            self.completeDump()

        if self.jobRequested:
            # special case...
            if self.jobRequested == "latestlinks":
                if self.dumpItemList.allPossibleJobsDone(self.skipJobs):
                    self.symLinks.removeSymLinksFromOldRuns(self.wiki.date)
                    self.feeds.cleanupFeeds()

        # Informing about completion
        if self.jobRequested:
            if self.restart:
                self.showRunnerState("Completed run restarting from job %s for %s" % (self.jobRequested, self.dbName))
            else:
                self.showRunnerState("Completed job %s for %s" % (self.jobRequested, self.dbName))
        else:
            self.showRunnerStateComplete()

        # let caller know if this was a successful run
        if self.status.failCount > 0:
            return False
        else:
            return True

    def cleanOldDumps(self, private=False):
        """Removes all but the wiki.config.keep last dumps of this wiki.
        If there is already a directory for todays dump, this is omitted in counting and
        not removed."""
        if self._cleanOldDumpsEnabled:
            if private:
                old = self.wiki.dumpDirs(private=True)
                dumptype='private'
            else:
                old = self.wiki.dumpDirs()
                dumptype='public'
            if old:
                if old[-1] == self.wiki.date:
                    # If we're re-running today's (or jobs from a given day's) dump, don't count it as one
                    # of the old dumps to keep... or delete it halfway through!
                    old = old[:-1]
                if self.wiki.config.keep > 0:
                    # Keep the last few
                    old = old[:-(self.wiki.config.keep)]
            if old:
                for dump in old:
                    self.showRunnerState("Purging old %s dump %s for %s" % (dumptype, dump, self.dbName))
                    if private:
                        base = os.path.join(self.wiki.privateDir(), dump)
                    else:
                        base = os.path.join(self.wiki.publicDir(), dump)
                    shutil.rmtree("%s" % base)
            else:
                self.showRunnerState("No old %s dumps to purge." % dumptype)

    def showRunnerState(self, message):
        self.debug(message)

    def showRunnerStateComplete(self):
        self.debug("SUCCESS: done.")

    def completeDump(self):
        # note that it's possible for links in "latest" to point to
        # files from different runs, in which case the md5sums file
        # will have accurate checksums for the run for which it was
        # produced, but not the other files. FIXME
        self.checksums.moveMd5FileIntoPlace()
        dumpFile = DumpFilename(self.wiki, None, self.checksums.getChecksumFileNameBasename())
        self.symLinks.saveSymlink(dumpFile)
        self.symLinks.cleanupSymLinks()

        for item in self.dumpItemList.dumpItems:
            if item.toBeRun():
                dumpNames = item.listDumpNames()
                if type(dumpNames).__name__!='list':
                    dumpNames = [dumpNames]

                if item._chunksEnabled:
                    # if there is a specific chunk, we want to only clear out
                    # old files for that piece, because new files for the other
                    # pieces may not have been generated yet.
                    chunk = item._chunkToDo
                else:
                    chunk = None

                checkpoint = None
                if item._checkpointsEnabled:
                    if item.checkpointFile:
                        # if there's a specific checkpoint file we are
                        # rerunning, we would only clear out old copies
                        # of that very file. meh. how likely is it that we
                        # have one? these files are time based and the start/end pageids
                        # are going to fluctuate. whatever
                        checkpoint = item.checkpointFile.checkpoint

                for d in dumpNames:
                    self.symLinks.removeSymLinksFromOldRuns(self.wiki.date, d, chunk, checkpoint, onlychunks=item.onlychunks)

                self.feeds.cleanupFeeds()

    def makeDir(self, dir):
        if self._makeDirEnabled:
            if exists(dir):
                self.debug("Checkdir dir %s ..." % dir)
            else:
                self.debug("Creating %s ..." % dir)
                os.makedirs(dir)

def checkJobs(wiki, date, job, skipjobs, pageIDRange, chunkToDo, checkpointFile, prereqs=False):
        '''
        if prereqs is False:
        see if dump run on specific date completed specific job(s)
        or if no job was specified, ran to completion

        if prereqs is True:
        see if dump run on specific date completed prereqs for specific job(s)
        or if no job was specified, return True

        '''
        if not date:
                return False

        if date == 'last':
                dumps = sorted(wiki.dumpDirs())
                if dumps:
                        date = dumps[-1]
                else:
                        # never dumped so that's the same as 'job didn't run'
                        return False

        if not job and prereqs:
                return True

        wiki.setDate(date)

        runInfoFile = RunInfoFile(wiki, False)
        chunkInfo = Chunk(wiki, wiki.dbName)
        dumpDir = DumpDir(wiki, wiki.dbName)
        dumpItemList = DumpItemList(wiki, False, False, chunkToDo, checkpointFile, job, skipjobs, chunkInfo, pageIDRange, runInfoFile, dumpDir)
        if not dumpItemList.oldRunInfoRetrieved:
                # failed to get the run's info so let's call it 'didn't run'
                return False

        results = dumpItemList._runInfoFile.getOldRunInfoFromFile()
        if results:
                for runInfoObj in results:
                        dumpItemList._setDumpItemRunInfo(runInfoObj)

        # mark the jobs we would run
        if job:
                dumpItemList.markDumpsToRun(job, True)
                if restart:
                        dumpItemList.markFollowingJobsToRun(True)
        else:
                dumpItemList.markAllJobsToRun(True)

        if not prereqs:
                # see if there are any to run. no? then return True (all job(s) done)
                # otherwise return False (still some to do)
                for item in dumpItemList.dumpItems:
                        if item.toBeRun():
                                return False
                return True
        else:
                # get the list of prereqs, see if they are all status done, if so
                # return True, otherwise False (still some to do)
                prereqItems = []
                for item in dumpItemList.dumpItems:
                        if item.name() == job:
                                prereqItems = item._prerequisiteItems
                        break

                for item in prereqItems:
                        if item.status() != "done":
                                return False
                return True


def findAndLockNextWiki(config, locksEnabled, cutoff, bystatustime=False, check_job_status=False,
                        check_prereq_status=False, date=None, job=None, skipjobs=None, pageIDRange=None,
                        chunkToDo=None, checkpointFile=None):
    if config.halt:
        sys.stderr.write("Dump process halted by config.\n")
        return None

    next = config.dbListByAge(bystatustime)
    next.reverse()

    if verbose and not cutoff:
        sys.stderr.write("Finding oldest unlocked wiki...\n")

        # if we skip locked wikis which are missing the prereqs for this job,
        # there are still wikis where this job needs to run
        missingPrereqs = False
    for db in next:
        wiki = Wiki(config, db)
        if cutoff:
            lastUpdated = wiki.dateTouchedLatestDump()
            if lastUpdated >= cutoff:
                continue
        if check_job_status:
            if checkJobs(wiki, date, job, skipjobs, pageIDRange, chunkToDo, checkpointFile):
                continue
        try:
            if locksEnabled:
                wiki.lock()
            return wiki
        except:
            if check_prereq_status:
                # if we skip locked wikis which are missing the prereqs for this job,
                # there are still wikis where this job needs to run
                if not checkJobs(wiki, date, job, skipjobs, pageIDRange, chunkToDo,
                                 checkpointFile, prereqs=True):
                    missingPrereqs = True
            sys.stderr.write("Couldn't lock %s, someone else must have got it...\n" % db)
            continue
    if missingPrereqs:
        return False
    else:
        return None


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    sys.stderr.write("Usage: python worker.py [options] [wikidbname]\n")
    sys.stderr.write("Options: --aftercheckpoint, --checkpoint, --chunk, --configfile, --date, --job, --skipjobs, --addnotice, --delnotice, --force, --noprefetch, --nospawn, --restartfrom, --log, --cutoff\n")
    sys.stderr.write("--aftercheckpoint: Restart thie job from the after specified checkpoint file, doing the\n")
    sys.stderr.write("               rest of the job for the appropriate chunk if chunks are configured\n")
    sys.stderr.write("               or for the all the rest of the revisions if no chunks are configured;\n")
    sys.stderr.write("               only for jobs articlesdump, metacurrentdump, metahistorybz2dump.\n")
    sys.stderr.write("--checkpoint:  Specify the name of the checkpoint file to rerun (requires --job,\n")
    sys.stderr.write("               depending on the file this may imply --chunk)\n")
    sys.stderr.write("--chunk:       Specify the number of the chunk to rerun (use with a specific job\n")
    sys.stderr.write("               to rerun, only if parallel jobs (chunks) are enabled).\n")
    sys.stderr.write("--configfile:  Specify an alternative configuration file to read.\n")
    sys.stderr.write("               Default config file name: wikidump.conf\n")
    sys.stderr.write("--date:        Rerun dump of a given date (probably unwise)\n")
    sys.stderr.write("               If 'last' is given as the value, will rerun dump from last run date if any,\n")
    sys.stderr.write("               or today if there has never been a previous run\n")
    sys.stderr.write("--addnotice:   Text message that will be inserted in the per-dump-run index.html\n")
    sys.stderr.write("               file; use this when rerunning some job and you want to notify the\n")
    sys.stderr.write("               potential downloaders of problems, for example.  This option\n")
    sys.stderr.write("               remains in effective for the specified wiki and date until\n")
    sys.stderr.write("               the delnotice option is given.\n")
    sys.stderr.write("--delnotice:   Remove any notice that has been specified by addnotice, for\n")
    sys.stderr.write("               the given wiki and date.\n")
    sys.stderr.write("--job:         Run just the specified step or set of steps; for the list,\n")
    sys.stderr.write("               give the option --job help\n")
    sys.stderr.write("               This option requires specifiying a wikidbname on which to run.\n")
    sys.stderr.write("               This option cannot be specified with --force.\n")
    sys.stderr.write("--skipjobs:    Comma separated list of jobs not to run on the wiki(s)\n")
    sys.stderr.write("               give the option --job help\n")
    sys.stderr.write("--dryrun:      Don't really run the job, just print what would be done (must be used\n")
    sys.stderr.write("               with a specified wikidbname on which to run\n")
    sys.stderr.write("--force:       remove a lock file for the specified wiki (dangerous, if there is\n")
    sys.stderr.write("               another process running, useful if you want to start a second later\n")
    sys.stderr.write("               run while the first dump from a previous date is still going)\n")
    sys.stderr.write("               This option cannot be specified with --job.\n")
    sys.stderr.write("--exclusive    Even if rerunning just one job of a wiki, get a lock to make sure no other\n")
    sys.stderr.write("               runners try to work on that wiki. Default: for single jobs, don't lock\n")
    sys.stderr.write("--noprefetch:  Do not use a previous file's contents for speeding up the dumps\n")
    sys.stderr.write("               (helpful if the previous files may have corrupt contents)\n")
    sys.stderr.write("--nospawn:     Do not spawn a separate process in order to retrieve revision texts\n")
    sys.stderr.write("--restartfrom: Do all jobs after the one specified via --job, including that one\n")
    sys.stderr.write("--skipdone:    Do only jobs that are not already succefully completed\n")
    sys.stderr.write("--log:         Log progress messages and other output to logfile in addition to\n")
    sys.stderr.write("               the usual console output\n")
    sys.stderr.write("--cutoff:      Given a cutoff date in yyyymmdd format, display the next wiki for which\n")
    sys.stderr.write("               dumps should be run, if its last dump was older than the cutoff date,\n")
    sys.stderr.write("               and exit, or if there are no such wikis, just exit\n")
    sys.stderr.write("--verbose:     Print lots of stuff (includes printing full backtraces for any exception)\n")
    sys.stderr.write("               This is used primarily for debugging\n")

    sys.exit(1)

if __name__ == "__main__":
    try:
        date = None
        configFile = False
        forceLock = False
        prefetch = True
        spawn = True
        restart = False
        jobRequested = None
        skipJobs = None
        enableLogging = False
        log = None
        htmlNotice = ""
        dryrun = False
        chunkToDo = False
        afterCheckpoint = False
        checkpointFile = None
        pageIDRange = None
        cutoff = None
        exitcode = 1
        skipdone = False
        doLocking = False
        verbose = False

        try:
            (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                 ['date=', 'job=', 'skipjobs=', 'configfile=', 'addnotice=', 'delnotice', 'force', 'dryrun', 'noprefetch', 'nospawn', 'restartfrom', 'aftercheckpoint=', 'log', 'chunk=', 'checkpoint=', 'pageidrange=', 'cutoff=', "skipdone", "exclusive", 'verbose'])
        except:
            usage("Unknown option specified")

        for (opt, val) in options:
            if opt == "--date":
                date = val
            elif opt == "--configfile":
                configFile = val
            elif opt == '--checkpoint':
                checkpointFile = val
            elif opt == '--chunk':
                chunkToDo = int(val)
            elif opt == "--force":
                forceLock = True
            elif opt == '--aftercheckpoint':
                afterCheckpoint = True
                checkpointFile = val
            elif opt == "--noprefetch":
                prefetch = False
            elif opt == "--nospawn":
                spawn = False
            elif opt == "--dryrun":
                dryrun = True
            elif opt == "--job":
                jobRequested = val
            elif opt == "--skipjobs":
                skipJobs = val
            elif opt == "--restartfrom":
                restart = True
            elif opt == "--log":
                enableLogging = True
            elif opt == "--addnotice":
                htmlNotice = val
            elif opt == "--delnotice":
                htmlNotice = False
            elif opt == "--pageidrange":
                pageIDRange = val
            elif opt == "--cutoff":
                cutoff = val
                if not cutoff.isdigit() or not len(cutoff) == 8:
                    usage("--cutoff value must be in yyyymmdd format")
            elif opt == "--skipdone":
                skipdone = True
            elif opt == "--exclusive":
                doLocking = True
            elif opt == "--verbose":
                verbose = True

        if dryrun and (len(remainder) == 0):
            usage("--dryrun requires the name of a wikidb to be specified")
        if jobRequested and forceLock:
            usage("--force cannot be used with --job option")
        if restart and not jobRequested:
            usage("--restartfrom requires --job and the job from which to restart")
        if chunkToDo and not jobRequested:
            usage("--chunk option requires a specific job for which to rerun that chunk")
        if chunkToDo and restart:
            usage("--chunk option can be specified only for one specific job")
        if checkpointFile and (len(remainder) == 0):
            usage("--checkpoint option requires the name of a wikidb to be specified")
        if checkpointFile and not jobRequested:
            usage("--checkpoint option requires --job and the job from which to restart")
        if pageIDRange and not jobRequested:
            usage("--pageidrange option requires --job and the job from which to restart")
        if pageIDRange and checkpointFile:
            usage("--pageidrange option cannot be used with --checkpoint option")

        if skipJobs is None:
            skipJobs = []
        else:
            skipJobs = skipJobs.split(",")

        # allow alternate config file
        if configFile:
            config = Config(configFile)
        else:
            config = Config()
        externals = [
            'php', 'mysql', 'mysqldump', 'head', 'tail',
            'checkforbz2footer', 'grep', 'gzip', 'bzip2',
            'writeuptopageid', 'recompressxml', 'sevenzip', 'cat',]

        failed = False
        unknowns = []
        notfound = []
        for external in externals:
            try:
                ext = getattr(config, external)
            except AttributeError:
                unknowns.append(external)
                failed = True
            else:
                if not exists(ext):
                    notfound.append(ext)
                    failed = True
        if failed:
            if unknowns:
                sys.stderr.write("Unknown config param(s): %s\n" % ", ".join(unknowns))
            if notfound:
                sys.stderr.write("Command(s) not found: %s\n" % ", ".join(notfound))
            sys.stderr.write("Exiting.\n")
            sys.exit(1)

        if dryrun or chunkToDo or (jobRequested and not restart  and not doLocking):
            locksEnabled = False
        else:
            locksEnabled = True

        if dryrun:
            print "***"
            print "Dry run only, no files will be updated."
            print "***"

        if len(remainder) > 0:
            wiki = Wiki(config, remainder[0])
            if cutoff:
                # fixme if we asked for a specific job then check that job only
                # not the dir
                lastRan = wiki.latestDump()
                if lastRan >= cutoff:
                    wiki = None
            if wiki is not None and locksEnabled:
                if forceLock and wiki.isLocked():
                    wiki.unlock()
                if locksEnabled:
                    wiki.lock()

        else:
            # if the run is across all wikis and we are just doing one job,
            # we want the age of the wikis by the latest status update
            # and not the date the run started
            if jobRequested:
                check_status_time = True
            else:
                check_status_time = False
            if skipdone:
                check_job_status = True
            else:
                check_job_status = False
            if jobRequested and skipdone:
                check_prereq_status = True
            else:
                check_prereq_status = False
            wiki = findAndLockNextWiki(config, locksEnabled, cutoff, check_status_time,
                                                   check_job_status, check_prereq_status,
                                                   date, jobRequested, skipJobs, pageIDRange, chunkToDo, checkpointFile)

        if wiki is not None and wiki:
            # process any per-project configuration options
            config.parseConfFilePerProject(wiki.dbName)

            if date == 'last':
                dumps = sorted(wiki.dumpDirs())
                if dumps:
                    date = dumps[-1]
                else:
                    date = None

            if date is None or not date:
                date = TimeUtils.today()
            wiki.setDate(date)

            if afterCheckpoint:
                f = DumpFilename(wiki)
                f.newFromFilename(checkpointFile)
                if not f.isCheckpointFile:
                    usage("--aftercheckpoint option requires the name of a checkpoint file, bad filename provided")
                pageIDRange = str(int(f.lastPageID) + 1)
                chunkToDo = f.chunkInt
                # now we don't need this.
                checkpointFile = None
                afterCheckpointJobs = ['articlesdump', 'metacurrentdump', 'metahistorybz2dump']
                if not jobRequested or not jobRequested in ['articlesdump', 'metacurrentdump', 'metahistorybz2dump']:
                    usage("--aftercheckpoint option requires --job option with one of %s" % ", ".join(afterCheckpointJobs))

            runner = Runner(wiki, prefetch, spawn, jobRequested, skipJobs, restart, htmlNotice, dryrun, enableLogging, chunkToDo, checkpointFile, pageIDRange, skipdone, verbose)

            if restart:
                sys.stderr.write("Running %s, restarting from job %s...\n" % (wiki.dbName, jobRequested))
            elif jobRequested:
                sys.stderr.write("Running %s, job %s...\n" % (wiki.dbName, jobRequested))
            else:
                sys.stderr.write("Running %s...\n" % wiki.dbName)
            result = runner.run()
            if result is not None and result:
                exitcode = 0
            # if we are doing one piece only of the dump, we don't unlock either
            if locksEnabled:
                wiki.unlock()
        elif wiki is not None:
            sys.stderr.write("Wikis available to run but prereqs not complete.\n")
            exitcode = 0
        else:
            sys.stderr.write("No wikis available to run.\n")
            exitcode = 255
    finally:
        cleanup()
    sys.exit(exitcode)
