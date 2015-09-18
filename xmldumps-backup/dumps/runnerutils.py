# Worker process, does the actual dumping

import getopt, hashlib, os, re, sys, errno, time
import subprocess, select
import shutil, stat, signal, glob
import Queue, thread, traceback, socket

from os.path import exists
from subprocess import Popen, PIPE
from WikiDump import FileUtils, MiscUtils, TimeUtils
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel
from dumps.jobs import *
                                                                                                                
def xmlEscape(text):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

class Maintenance(object):

    def inMaintenanceMode():
        """Use this to let callers know that we really should not
        be running.  Callers should try to exit the job
        they are running as soon as possible."""
        return exists("maintenance.txt")

    def exitIfInMaintenanceMode(message = None):
        """Call this from possible exit points of running jobs
        in order to exit if we need to"""
        if Maintenance.inMaintenanceMode():
            if message:
                raise BackupError(message)
            else:
                raise BackupError("In maintenance mode, exiting.")

    inMaintenanceMode = staticmethod(inMaintenanceMode)
    exitIfInMaintenanceMode = staticmethod(exitIfInMaintenanceMode)


class Checksummer(object):
    def __init__(self,wiki,dumpDir, enabled = True, verbose = False):
        self.wiki = wiki
        self.dumpDir = dumpDir
        self.verbose = verbose
        self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self._enabled = enabled

    def prepareChecksums(self):
        """Create a temporary md5 checksum file.
        Call this at the start of the dump run, and move the file
        into the final location at the completion of the dump run."""
        if (self._enabled):
            checksumFileName = self._getChecksumFileNameTmp()
            output = file(checksumFileName, "w")

    def checksum(self, fileObj, runner):
        """Run checksum for an output file, and append to the list."""
        if (self._enabled):
            checksumFileName = self._getChecksumFileNameTmp()
            output = file(checksumFileName, "a")
            runner.debug("Checksumming %s" % fileObj.filename)
            dumpfile = DumpFile(self.wiki, runner.dumpDir.filenamePublicPath(fileObj),None,self.verbose)
            checksum = dumpfile.md5Sum()
            if checksum != None:
                output.write("%s  %s\n" % (checksum, fileObj.filename))
            output.close()

    def moveMd5FileIntoPlace(self):
        if (self._enabled):
            tmpFileName = self._getChecksumFileNameTmp()
            realFileName = self._getChecksumFileName()
            os.rename(tmpFileName, realFileName)

    def cpMd5TmpFileToPermFile(self):
        if (self._enabled):
            tmpFileName = self._getChecksumFileNameTmp()
            realFileName = self._getChecksumFileName()
            text = FileUtils.readFile(tmpFileName)
            FileUtils.writeFile(self.wiki.config.tempDir, realFileName, text, self.wiki.config.fileperms)

    def getChecksumFileNameBasename(self):
        return ("md5sums.txt")

    #
    # functions internal to the class
    #
    def _getChecksumFileName(self):
        fileObj = DumpFilename(self.wiki, None, self.getChecksumFileNameBasename())
        return (self.dumpDir.filenamePublicPath(fileObj))

    def _getChecksumFileNameTmp(self):
        fileObj = DumpFilename(self.wiki, None, self.getChecksumFileNameBasename() + "." + self.timestamp + ".tmp")
        return (self.dumpDir.filenamePublicPath(fileObj))

    def _getMd5FileDirName(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date)


# everything that has to do with reporting the status of a piece
# of a dump is collected here
class Status(object):
    def __init__(self, wiki, dumpDir, items, checksums, enabled, email = True, noticeFile = None, errorCallback=None, verbose = False):
        self.wiki = wiki
        self.dbName = wiki.dbName
        self.dumpDir = dumpDir
        self.items = items
        self.checksums = checksums
        self.noticeFile = noticeFile
        self.errorCallback = errorCallback
        self.failCount = 0
        self.verbose = verbose
        self._enabled = enabled
        self.email = email

    def updateStatusFiles(self, done=False):
        if self._enabled:
            self._saveStatusSummaryAndDetail(done)

    def reportFailure(self):
        if self._enabled and self.email:
            if self.wiki.config.adminMail and self.wiki.config.adminMail.lower() != 'nomail':
                subject = "Dump failure for " + self.dbName
                message = self.wiki.config.readTemplate("errormail.txt") % {
                    "db": self.dbName,
                    "date": self.wiki.date,
                    "time": TimeUtils.prettyTime(),
                    "url": "/".join((self.wiki.config.webRoot, self.dbName, self.wiki.date, ''))}
                self.wiki.config.mail(subject, message)

    # this is a per-dump-item report (well, per file generated by the item)
    # Report on the file size & item status of the current output and output a link if we are done
    def reportFile(self, fileObj, itemStatus):
        filename = self.dumpDir.filenamePublicPath(fileObj)
        if (exists(filename)):
            size = os.path.getsize(filename)
        else:
            itemStatus = "missing"
            size = 0
        size = FileUtils.prettySize(size)
        if itemStatus == "in-progress":
            return "<li class='file'>%s %s (written) </li>" % (fileObj.filename, size)
        elif itemStatus == "done":
            webpathRelative = self.dumpDir.webPathRelative(fileObj)
            return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpathRelative, fileObj.filename, size)
        else:
            return "<li class='missing'>%s</li>" % fileObj.filename

    #
    # functions internal to the class
    #
    def _saveStatusSummaryAndDetail(self, done=False):
        """Write out an HTML file with the status for this wiki's dump
        and links to completed files, as well as a summary status in a separate file."""
        try:
            # Comprehensive report goes here
            self.wiki.writePerDumpIndex(self._reportDatabaseStatusDetailed(done))
            # Short line for report extraction goes here
            self.wiki.writeStatus(self._reportDatabaseStatusSummary(done))
        except:
            if (self.verbose):
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            message = "Couldn't update status files. Continuing anyways"
            if self.errorCallback:
                self.errorCallback(message)
            else:
                sys.stderr.write("%s\n" % message)

    def _reportDatabaseStatusSummary(self, done = False):
        """Put together a brief status summary and link for the current database."""
        status = self._reportStatusSummaryLine(done)
        html = self.wiki.reportStatusLine(status)

        activeItems = [x for x in self.items if x.status() == "in-progress"]
        if activeItems:
            return html + "<ul>" + "\n".join([self._reportItem(x) for x in activeItems]) + "</ul>"
        else:
            return html

    def _reportDatabaseStatusDetailed(self, done = False):
        """Put together a status page for this database, with all its component dumps."""
        self.noticeFile.refreshNotice()
        statusItems = [self._reportItem(item) for item in self.items]
        statusItems.reverse()
        html = "\n".join(statusItems)
        f = DumpFilename(self.wiki, None, self.checksums.getChecksumFileNameBasename())
        return self.wiki.config.readTemplate("report.html") % {
            "db": self.dbName,
            "date": self.wiki.date,
            "notice": self.noticeFile.notice,
            "status": self._reportStatusSummaryLine(done),
            "previous": self._reportPreviousDump(done),
            "items": html,
            "checksum": self.dumpDir.webPathRelative(f),
            "index": self.wiki.config.index}

    def _reportPreviousDump(self, done):
        """Produce a link to the previous dump, if any"""
        # get the list of dumps for this wiki in order, find me in the list, find the one prev to me.
        # why? we might be rerunning a job from an older dumps. we might have two
        # runs going at once (think en pedia, one finishing up the history, another
        # starting at the beginning to get the new abstracts and stubs).
        try:
            dumpsInOrder = self.wiki.latestDump(all=True)
            meIndex = dumpsInOrder.index(self.wiki.date)
            # don't wrap around to the newest dump in the list!
            if (meIndex > 0):
                rawDate = dumpsInOrder[meIndex-1]
            elif (meIndex == 0):
                # We are the first item in the list. This is not an error, but there is no
                # previous dump
                return "No prior dumps of this database stored."
            else:
                raise(ValueError)
        except:
            if (self.verbose):
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return "No prior dumps of this database stored."
        prettyDate = TimeUtils.prettyDate(rawDate)
        if done:
            prefix = ""
            message = "Last dumped on"
        else:
            prefix = "This dump is in progress; see also the "
            message = "previous dump from"
        return "%s<a href=\"../%s/\">%s %s</a>" % (prefix, rawDate, message, prettyDate)

    def _reportStatusSummaryLine(self, done=False):
        if (done == "done"):
            classes = "done"
            text = "Dump complete"
        elif (done == "partialdone"):
            classes = "partial-dump"
            text = "Partial dump"
        else:
            classes = "in-progress"
            text = "Dump in progress"
        if self.failCount > 0:
            classes += " failed"
            if self.failCount == 1:
                ess = ""
            else:
                ess = "s"
            text += ", %d item%s failed" % (self.failCount, ess)
        return "<span class='%s'>%s</span>" % (classes, text)

    def _reportItem(self, item):
        """Return an HTML fragment with info on the progress of this item."""
        item.status()
        item.updated()
        item.description()
        html = "<li class='%s'><span class='updates'>%s</span> <span class='status'>%s</span> <span class='title'>%s</span>" % (item.status(), item.updated(), item.status(), item.description())
        if item.progress:
            html += "<div class='progress'>%s</div>\n" % item.progress
        fileObjs = item.listOutputFilesToPublish(self.dumpDir)
        if fileObjs:
            listItems = [self.reportFile(fileObj, item.status()) for fileObj in fileObjs]
            html += "<ul>"
            detail = item.detail()
            if detail:
                html += "<li class='detail'>%s</li>\n" % detail
            html += "\n".join(listItems)
            html += "</ul>"
        html += "</li>"
        return html

class NoticeFile(object):
    def __init__(self, wiki, notice, enabled):
        self.wiki = wiki
        self.notice = notice
        self._enabled = enabled
        self.writeNoticeFile()

    def writeNoticeFile(self):
        if (self._enabled):
            noticeFile = self._getNoticeFilename()
            # delnotice.  toss any existing file
            if self.notice == False:
                if exists(noticeFile):
                    os.remove(noticeFile)
                self.notice = ""
            # addnotice, stuff notice in a file for other jobs etc
            elif self.notice != "":
                noticeDir = self._getNoticeDir()
                FileUtils.writeFile(self.wiki.config.tempDir, noticeFile, self.notice, self.wiki.config.fileperms)
            # default case. if there is a file get the contents, otherwise
            # we have empty contents, all good
            else:
                if exists(noticeFile):
                    self.notice = FileUtils.readFile(noticeFile)

    def refreshNotice(self):
        # if the notice file has changed or gone away, we comply.
        noticeFile = self._getNoticeFilename()
        if exists(noticeFile):
            self.notice = FileUtils.readFile(noticeFile)
        else:
            self.notice = ""


    #
    # functions internal to class
    #
    def _getNoticeFilename(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date, "notice.txt")

    def _getNoticeDir(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date)


class SymLinks(object):
    def __init__(self, wiki, dumpDir, logfn, debugfn, enabled):
        self.wiki = wiki
        self.dumpDir = dumpDir
        self._enabled = enabled
        self.logfn = logfn
        self.debugfn = debugfn

    def makeDir(self, dir):
        if (self._enabled):
            if exists(dir):
                self.debugfn("Checkdir dir %s ..." % dir)
            else:
                self.debugfn("Creating %s ..." % dir)
                os.makedirs(dir)

    def saveSymlink(self, dumpFile):
        if (self._enabled):
            self.makeDir(self.dumpDir.latestDir())
            realfile = self.dumpDir.filenamePublicPath(dumpFile)
            latestFilename = dumpFile.newFilename(dumpFile.dumpName, dumpFile.fileType, dumpFile.fileExt, 'latest', dumpFile.chunk, dumpFile.checkpoint, dumpFile.temp)
            link = os.path.join(self.dumpDir.latestDir(), latestFilename)
            if exists(link) or os.path.islink(link):
                if os.path.islink(link):
                    oldrealfile = os.readlink(link)
                    # format of these links should be...  ../20110228/elwikidb-20110228-templatelinks.sql.gz
                    rellinkpattern = re.compile('^\.\./(20[0-9]+)/')
                    dateinlink = rellinkpattern.search(oldrealfile)
                    if (dateinlink):
                        dateoflinkedfile = dateinlink.group(1)
                        dateinterval = int(self.wiki.date) - int(dateoflinkedfile)
                    else:
                        dateinterval = 0
                    # no file or it's older than ours... *then* remove the link
                    if not exists(os.path.realpath(link)) or dateinterval > 0:
                        self.debugfn("Removing old symlink %s" % link)
                        os.remove(link)
                else:
                    self.logfn("What the hell dude, %s is not a symlink" % link)
                    raise BackupError("What the hell dude, %s is not a symlink" % link)
            relative = FileUtils.relativePath(realfile, os.path.dirname(link))
            # if we removed the link cause it's obsolete, make the new one
            if exists(realfile) and not exists(link):
                self.debugfn("Adding symlink %s -> %s" % (link, relative))
                os.symlink(relative, link)

    def cleanupSymLinks(self):
        if (self._enabled):
            latestDir = self.dumpDir.latestDir()
            files = os.listdir(latestDir)
            for f in files:
                link = os.path.join(latestDir,f)
                if os.path.islink(link):
                    realfile = os.readlink(link)
                    if not exists(os.path.join(latestDir,realfile)):
                        os.remove(link)

    # if the args are False or None, we remove all the old links for all values of the arg.
    # example: if chunk is False or None then we remove all old values for all chunks
    # "old" means "older than the specified datestring".
    def removeSymLinksFromOldRuns(self, dateString, dumpName=None, chunk=None, checkpoint=None, onlychunks=False):
        # fixme this needs to do more work if there are chunks or checkpoint files linked in here from
        # earlier dates. checkpoint ranges change, and configuration of chunks changes too, so maybe
        # old files still exist and the links need to be removed because we have newer files for the
        # same phase of the dump.

        if (self._enabled):
            latestDir = self.dumpDir.latestDir()
            files = os.listdir(latestDir)
            for f in files:
                link = os.path.join(latestDir,f)
                if os.path.islink(link):
                    realfile = os.readlink(link)
                    fileObj = DumpFilename(self.dumpDir._wiki)
                    fileObj.newFromFilename(os.path.basename(realfile))
                    if fileObj.date < dateString:
                        # fixme check that these are ok if the value is None
                        if dumpName and (fileObj.dumpName != dumpName):
                            continue
                        if (chunk or onlychunks) and (fileObj.chunk != chunk):
                            continue
                        if checkpoint and (fileObj.checkpoint != checkpoint):
                            continue
                        self.debugfn("Removing old symlink %s -> %s" % (link, realfile))
                        os.remove(link)

class Feeds(object):
    def __init__(self, wiki, dumpDir, dbName, debugfn, enabled):
        self.wiki = wiki
        self.dumpDir = dumpDir
        self.dbName = dbName
        self.debugfn = debugfn
        self._enabled = enabled

    def makeDir(self, dirname):
        if (self._enabled):
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def saveFeed(self, fileObj):
        if (self._enabled):
            self.makeDir(self.dumpDir.latestDir())
            filenameAndPath = self.dumpDir.webPath(fileObj)
            webPath = os.path.dirname(filenameAndPath)
            rssText = self.wiki.config.readTemplate("feed.xml") % {
                "chantitle": fileObj.basename,
                "chanlink": webPath,
                "chandesc": "Wikimedia dump updates for %s" % self.dbName,
                "title": webPath,
                "link": webPath,
                "description": xmlEscape("<a href=\"%s\">%s</a>" % (filenameAndPath, fileObj.filename)),
                "date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()) }
            directory = self.dumpDir.latestDir()
            rssPath = os.path.join(self.dumpDir.latestDir(), self.dbName + "-latest-" + fileObj.basename + "-rss.xml")
            self.debugfn("adding rss feed file %s " % rssPath)
            FileUtils.writeFile(self.wiki.config.tempDir, rssPath, rssText, self.wiki.config.fileperms)

    def cleanupFeeds(self):
        # call this after sym links in this dir have been cleaned up.
        # we should probably fix this so there is no such dependency,
        # but it would mean parsing the contents of the rss file, bleah
        if (self._enabled):
            latestDir = self.dumpDir.latestDir()
            files = os.listdir(latestDir)
            for f in files:
                if f.endswith("-rss.xml"):
                    filename = f[:-8]
                    link = os.path.join(latestDir,filename)
                    if not exists(link):
                        self.debugfn("Removing old rss feed %s for link %s" % (os.path.join(latestDir,f), link))
                        os.remove(os.path.join(latestDir,f))

