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

def xml_escape(text):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

class Maintenance(object):

    def in_maintenance_mode():
        """Use this to let callers know that we really should not
        be running.  Callers should try to exit the job
        they are running as soon as possible."""
        return exists("maintenance.txt")

    def exit_if_in_maintenance_mode(message=None):
        """Call this from possible exit points of running jobs
        in order to exit if we need to"""
        if Maintenance.in_maintenance_mode():
            if message:
                raise BackupError(message)
            else:
                raise BackupError("In maintenance mode, exiting.")

    in_maintenance_mode = staticmethod(in_maintenance_mode)
    exit_if_in_maintenance_mode = staticmethod(exit_if_in_maintenance_mode)


class Checksummer(object):
    def __init__(self, wiki, dump_dir, enabled=True, verbose=False):
        self.wiki = wiki
        self.dumpDir = dump_dir
        self.verbose = verbose
        self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self._enabled = enabled

    def prepare_checksums(self):
        """Create a temporary md5 checksum file.
        Call this at the start of the dump run, and move the file
        into the final location at the completion of the dump run."""
        if self._enabled:
            checksum_filename = self._get_checksum_filename_tmp()
            output = file(checksum_filename, "w")

    def checksum(self, file_obj, runner):
        """Run checksum for an output file, and append to the list."""
        if self._enabled:
            checksum_filename = self._get_checksum_filename_tmp()
            output = file(checksum_filename, "a")
            runner.debug("Checksumming %s" % file_obj.filename)
            dumpfile = DumpFile(self.wiki, runner.dumpDir.filenamePublicPath(file_obj), None, self.verbose)
            checksum = dumpfile.md5sum()
            if checksum != None:
                output.write("%s  %s\n" % (checksum, file_obj.filename))
            output.close()

    def move_md5file_into_place(self):
        if self._enabled:
            tmp_filename = self._get_checksum_filename_tmp()
            real_filename = self._get_checksum_filename()
            os.rename(tmp_filename, real_filename)

    def cp_md5_tmpfile_to_permfile(self):
        if self._enabled:
            tmp_filename = self._get_checksum_filename_tmp()
            real_filename = self._get_checksum_filename()
            text = FileUtils.readFile(tmp_filename)
            FileUtils.writeFile(self.wiki.config.tempDir, real_filename, text, self.wiki.config.fileperms)

    def get_checksum_filename_basename(self):
        return "md5sums.txt"

    #
    # functions internal to the class
    #
    def _get_checksum_filename(self):
        file_obj = DumpFilename(self.wiki, None, self.get_checksum_filename_basename())
        return self.dumpDir.filenamePublicPath(file_obj)

    def _get_checksum_filename_tmp(self):
        file_obj = DumpFilename(self.wiki, None, self.get_checksum_filename_basename() + "." + self.timestamp + ".tmp")
        return self.dumpDir.filenamePublicPath(file_obj)

    def _getmd5file_dir_name(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date)


# everything that has to do with reporting the status of a piece
# of a dump is collected here
class Status(object):
    def __init__(self, wiki, dump_dir, items, checksums, enabled, email=True, notice_file=None, error_callback=None, verbose=False):
        self.wiki = wiki
        self.dbName = wiki.dbName
        self.dumpDir = dump_dir
        self.items = items
        self.checksums = checksums
        self.notice_file = notice_file
        self.error_callback = error_callback
        self.fail_count = 0
        self.verbose = verbose
        self._enabled = enabled
        self.email = email

    def update_status_files(self, done=False):
        if self._enabled:
            self._save_status_summary_and_detail(done)

    def report_failure(self):
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
    def report_file(self, file_obj, item_status):
        filename = self.dumpDir.filenamePublicPath(file_obj)
        if exists(filename):
            size = os.path.getsize(filename)
        else:
            item_status = "missing"
            size = 0
        size = FileUtils.prettySize(size)
        if item_status == "in-progress":
            return "<li class='file'>%s %s (written) </li>" % (file_obj.filename, size)
        elif item_status == "done":
            webpath_relative = self.dumpDir.web_path_relative(file_obj)
            return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpath_relative, file_obj.filename, size)
        else:
            return "<li class='missing'>%s</li>" % file_obj.filename

    #
    # functions internal to the class
    #
    def _save_status_summary_and_detail(self, done=False):
        """Write out an HTML file with the status for this wiki's dump
        and links to completed files, as well as a summary status in a separate file."""
        try:
            # Comprehensive report goes here
            self.wiki.writePerDumpIndex(self._report_database_status_detailed(done))
            # Short line for report extraction goes here
            self.wiki.writeStatus(self._report_database_status_summary(done))
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            message = "Couldn't update status files. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)

    def _report_database_status_summary(self, done=False):
        """Put together a brief status summary and link for the current database."""
        status = self._report_status_summary_line(done)
        html = self.wiki.reportStatusLine(status)

        active_items = [x for x in self.items if x.status() == "in-progress"]
        if active_items:
            return html + "<ul>" + "\n".join([self._report_item(x) for x in active_items]) + "</ul>"
        else:
            return html

    def _report_database_status_detailed(self, done=False):
        """Put together a status page for this database, with all its component dumps."""
        self.notice_file.refresh_notice()
        status_items = [self._report_item(item) for item in self.items]
        status_items.reverse()
        html = "\n".join(status_items)
        fname = DumpFilename(self.wiki, None, self.checksums.get_checksum_filename_basename())
        return self.wiki.config.readTemplate("report.html") % {
            "db": self.dbName,
            "date": self.wiki.date,
            "notice": self.notice_file.notice,
            "status": self._report_status_summary_line(done),
            "previous": self._report_previous_dump(done),
            "items": html,
            "checksum": self.dumpDir.web_path_relative(fname),
            "index": self.wiki.config.index}

    def _report_previous_dump(self, done):
        """Produce a link to the previous dump, if any"""
        # get the list of dumps for this wiki in order, find me in the list, find the one prev to me.
        # why? we might be rerunning a job from an older dumps. we might have two
        # runs going at once (think en pedia, one finishing up the history, another
        # starting at the beginning to get the new abstracts and stubs).
        try:
            dumps_in_order = self.wiki.latestDump(all=True)
            me_index = dumps_in_order.index(self.wiki.date)
            # don't wrap around to the newest dump in the list!
            if me_index > 0:
                raw_date = dumps_in_order[me_index-1]
            elif me_index == 0:
                # We are the first item in the list. This is not an error, but there is no
                # previous dump
                return "No prior dumps of this database stored."
            else:
                raise ValueError
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return "No prior dumps of this database stored."
        pretty_date = TimeUtils.prettyDate(raw_date)
        if done:
            prefix = ""
            message = "Last dumped on"
        else:
            prefix = "This dump is in progress; see also the "
            message = "previous dump from"
        return "%s<a href=\"../%s/\">%s %s</a>" % (prefix, raw_date, message, pretty_date)

    def _report_status_summary_line(self, done=False):
        if done == "done":
            classes = "done"
            text = "Dump complete"
        elif done == "partialdone":
            classes = "partial-dump"
            text = "Partial dump"
        else:
            classes = "in-progress"
            text = "Dump in progress"
        if self.fail_count > 0:
            classes += " failed"
            if self.fail_count == 1:
                ess = ""
            else:
                ess = "s"
            text += ", %d item%s failed" % (self.fail_count, ess)
        return "<span class='%s'>%s</span>" % (classes, text)

    def _report_item(self, item):
        """Return an HTML fragment with info on the progress of this item."""
        item.status()
        item.updated()
        item.description()
        html = "<li class='%s'><span class='updates'>%s</span> <span class='status'>%s</span> <span class='title'>%s</span>" % (item.status(), item.updated(), item.status(), item.description())
        if item.progress:
            html += "<div class='progress'>%s</div>\n" % item.progress
        file_objs = item.listOutputFilesToPublish(self.dumpDir)
        if file_objs:
            list_items = [self.report_file(file_obj, item.status()) for file_obj in file_objs]
            html += "<ul>"
            detail = item.detail()
            if detail:
                html += "<li class='detail'>%s</li>\n" % detail
            html += "\n".join(list_items)
            html += "</ul>"
        html += "</li>"
        return html

class NoticeFile(object):
    def __init__(self, wiki, notice, enabled):
        self.wiki = wiki
        self.notice = notice
        self._enabled = enabled
        self.write_notice_file()

    def write_notice_file(self):
        if self._enabled:
            notice_file = self._get_notice_filename()
            # delnotice.  toss any existing file
            if self.notice == False:
                if exists(notice_file):
                    os.remove(notice_file)
                self.notice = ""
            # addnotice, stuff notice in a file for other jobs etc
            elif self.notice != "":
                notice_dir = self._get_notice_dir()
                FileUtils.writeFile(self.wiki.config.tempDir, notice_file, self.notice, self.wiki.config.fileperms)
            # default case. if there is a file get the contents, otherwise
            # we have empty contents, all good
            else:
                if exists(notice_file):
                    self.notice = FileUtils.readFile(notice_file)

    def refresh_notice(self):
        # if the notice file has changed or gone away, we comply.
        notice_file = self._get_notice_filename()
        if exists(notice_file):
            self.notice = FileUtils.readFile(notice_file)
        else:
            self.notice = ""


    #
    # functions internal to class
    #
    def _get_notice_filename(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date, "notice.txt")

    def _get_notice_dir(self):
        return os.path.join(self.wiki.publicDir(), self.wiki.date)


class SymLinks(object):
    def __init__(self, wiki, dump_dir, logfn, debugfn, enabled):
        self.wiki = wiki
        self.dumpDir = dump_dir
        self._enabled = enabled
        self.logfn = logfn
        self.debugfn = debugfn

    def make_dir(self, dir):
        if self._enabled:
            if exists(dir):
                self.debugfn("Checkdir dir %s ..." % dir)
            else:
                self.debugfn("Creating %s ..." % dir)
                os.makedirs(dir)

    def save_symlink(self, dumpfile):
        if self._enabled:
            self.make_dir(self.dumpDir.latest_dir())
            realfile = self.dumpDir.filenamePublicPath(dumpfile)
            latest_filename = dumpfile.newFilename(dumpfile.dumpName, dumpfile.file_type, dumpfile.file_ext, 'latest', dumpfile.chunk, dumpfile.checkpoint, dumpfile.temp)
            link = os.path.join(self.dumpDir.latest_dir(), latest_filename)
            if exists(link) or os.path.islink(link):
                if os.path.islink(link):
                    oldrealfile = os.readlink(link)
                    # format of these links should be...  ../20110228/elwikidb-20110228-templatelinks.sql.gz
                    rellinkpattern = re.compile('^\.\./(20[0-9]+)/')
                    dateinlink = rellinkpattern.search(oldrealfile)
                    if dateinlink:
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

    def cleanup_symlinks(self):
        if self._enabled:
            latest_dir = self.dumpDir.latest_dir()
            files = os.listdir(latest_dir)
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfile = os.readlink(link)
                    if not exists(os.path.join(latest_dir, realfile)):
                        os.remove(link)

    # if the args are False or None, we remove all the old links for all values of the arg.
    # example: if chunk is False or None then we remove all old values for all chunks
    # "old" means "older than the specified datestring".
    def remove_symlinks_from_old_runs(self, date_string, dump_name=None, chunk=None, checkpoint=None, onlychunks=False):
        # fixme this needs to do more work if there are chunks or checkpoint files linked in here from
        # earlier dates. checkpoint ranges change, and configuration of chunks changes too, so maybe
        # old files still exist and the links need to be removed because we have newer files for the
        # same phase of the dump.

        if self._enabled:
            latest_dir = self.dumpDir.latest_dir()
            files = os.listdir(latest_dir)
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfile = os.readlink(link)
                    file_obj = DumpFilename(self.dumpDir._wiki)
                    file_obj.newFromFilename(os.path.basename(realfile))
                    if file_obj.date < date_string:
                        # fixme check that these are ok if the value is None
                        if dump_name and (file_obj.dumpName != dump_name):
                            continue
                        if (chunk or onlychunks) and (file_obj.chunk != chunk):
                            continue
                        if checkpoint and (file_obj.checkpoint != checkpoint):
                            continue
                        self.debugfn("Removing old symlink %s -> %s" % (link, realfile))
                        os.remove(link)

class Feeds(object):
    def __init__(self, wiki, dump_dir, dbname, debugfn, enabled):
        self.wiki = wiki
        self.dumpDir = dump_dir
        self.dbName = dbname
        self.debugfn = debugfn
        self._enabled = enabled

    def make_dir(self, dirname):
        if self._enabled:
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def save_feed(self, file_obj):
        if self._enabled:
            self.make_dir(self.dumpDir.latest_dir())
            filename_and_path = self.dumpDir.webPath(file_obj)
            web_path = os.path.dirname(filename_and_path)
            rss_text = self.wiki.config.readTemplate("feed.xml") % {
                "chantitle": file_obj.basename,
                "chanlink": web_path,
                "chandesc": "Wikimedia dump updates for %s" % self.dbName,
                "title": web_path,
                "link": web_path,
                "description": xml_escape("<a href=\"%s\">%s</a>" % (filename_and_path, file_obj.filename)),
                "date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()) }
            directory = self.dumpDir.latest_dir()
            rss_path = os.path.join(self.dumpDir.latest_dir(), self.dbName + "-latest-" + file_obj.basename + "-rss.xml")
            self.debugfn("adding rss feed file %s " % rss_path)
            FileUtils.writeFile(self.wiki.config.tempDir, rss_path, rss_text, self.wiki.config.fileperms)

    def cleanup_feeds(self):
        # call this after sym links in this dir have been cleaned up.
        # we should probably fix this so there is no such dependency,
        # but it would mean parsing the contents of the rss file, bleah
        if self._enabled:
            latest_dir = self.dumpDir.latest_dir()
            files = os.listdir(latest_dir)
            for fname in files:
                if fname.endswith("-rss.xml"):
                    filename = fname[:-8]
                    link = os.path.join(latest_dir, filename)
                    if not exists(link):
                        self.debugfn("Removing old rss feed %s for link %s" % (os.path.join(latest_dir, fname), link))
                        os.remove(os.path.join(latest_dir, fname))

