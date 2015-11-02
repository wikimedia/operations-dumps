# Worker process, does the actual dumping
import os
import re
import sys
import time
import traceback

from os.path import exists
from dumps.WikiDump import FileUtils, TimeUtils
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFile, DumpFilename


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
    NAME = "checksum"
    HASHTYPES = ['md5', 'sha1']

    def get_checksum_filename_basename(htype):
        if htype == "md5":
            return "md5sums.txt"
        elif htype == "sha1":
            return "sha1sums.txt"
        else:
            return None
    get_checksum_filename_basename = staticmethod(get_checksum_filename_basename)

    def __init__(self, wiki, dump_dir, enabled, verbose=False):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.verbose = verbose
        self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self._enabled = enabled

    def prepare_checksums(self):
        """Create a temporary md5 checksum file.
        Call this at the start of the dump run, and move the file
        into the final location at the completion of the dump run."""
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                checksum_filename = self._get_checksum_filename_tmp(htype)
                output = open(checksum_filename, "w")
                output.close()

    def checksums(self, file_obj, dumpjobdata):
        """Run checksum for an output file, and append to the list."""
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                checksum_filename = self._get_checksum_filename_tmp(htype)
                output = file(checksum_filename, "a")
                dumpjobdata.debugfn("Checksumming %s via %s" % (file_obj.filename, htype))
                dumpfile = DumpFile(self.wiki, dumpjobdata.dump_dir.filename_public_path(file_obj),
                                    None, self.verbose)
                checksum = dumpfile.checksum(htype)
                if checksum is not None:
                    output.write("%s  %s\n" % (checksum, file_obj.filename))
                output.close()

    def move_chksumfiles_into_place(self):
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                tmp_filename = self._get_checksum_filename_tmp(htype)
                real_filename = self._get_checksum_filename(htype)
                os.rename(tmp_filename, real_filename)

    def cp_chksum_tmpfiles_to_permfile(self):
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                tmp_filename = self._get_checksum_filename_tmp(htype)
                real_filename = self._get_checksum_filename(htype)
                text = FileUtils.read_file(tmp_filename)
                FileUtils.write_file(self.wiki.config.temp_dir, real_filename, text,
                                     self.wiki.config.fileperms)

    #
    # functions internal to the class
    #

    def _get_checksum_filename(self, htype):
        file_obj = DumpFilename(self.wiki, None, Checksummer.get_checksum_filename_basename(htype))
        return self.dump_dir.filename_public_path(file_obj)

    def _get_checksum_filename_tmp(self, htype):
        file_obj = DumpFilename(self.wiki, None, Checksummer.get_checksum_filename_basename(htype) +
                                "." + self.timestamp + ".tmp")
        return self.dump_dir.filename_public_path(file_obj)

    def _getmd5file_dir_name(self):
        return os.path.join(self.wiki.public_dir(), self.wiki.date)


# everything that has to do with reporting the status of a piece
# of a dump is collected here
class Status(object):
    NAME = "status"

    def __init__(self, wiki, dump_dir, items, dumpjobdata, enabled, email=True,
                 error_callback=None, verbose=False):
        self.wiki = wiki
        self.db_name = wiki.db_name
        self.dump_dir = dump_dir
        self.items = items
        self.dumpjobdata = dumpjobdata
        self.error_callback = error_callback
        self.fail_count = 0
        self.verbose = verbose
        self._enabled = enabled
        self.email = email

    def update_status_files(self, done=False):
        if Status.NAME in self._enabled:
            self._save_status_summary_and_detail(done)

    def report_failure(self):
        if Status.NAME in self._enabled and self.email:
            if self.wiki.config.admin_mail and self.wiki.config.admin_mail.lower() != 'nomail':
                subject = "Dump failure for " + self.db_name
                message = self.wiki.config.read_template("errormail.txt") % {
                    "db": self.db_name,
                    "date": self.wiki.date,
                    "time": TimeUtils.pretty_time(),
                    "url": "/".join((self.wiki.config.web_root, self.db_name, self.wiki.date, ''))}
                self.wiki.config.mail(subject, message)

    # this is a per-dump-item report (well, per file generated by the item)
    # Report on the file size & item status of the current output and output a link if we are done
    def report_file(self, file_obj, item_status):
        filename = self.dump_dir.filename_public_path(file_obj)
        if exists(filename):
            size = os.path.getsize(filename)
        else:
            item_status = "missing"
            size = 0
        size = FileUtils.pretty_size(size)
        if item_status == "in-progress":
            return "<li class='file'>%s %s (written) </li>" % (file_obj.filename, size)
        elif item_status == "done":
            webpath_relative = self.dump_dir.web_path_relative(file_obj)
            return ("<li class='file'><a href=\"%s\">%s</a> %s</li>"
                    % (webpath_relative, file_obj.filename, size))
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
            self.wiki.write_perdump_index(self._report_dbstatus_detailed(done))
            # Short line for report extraction goes here
            self.wiki.write_status(self._report_database_status_summary(done))
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't update status files. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)

    def _report_database_status_summary(self, done=False):
        """Put together a brief status summary and link for the current database."""
        status = self._report_status_summary_line(done)
        html = self.wiki.report_statusline(status)

        active_items = [x for x in self.items if x.status() == "in-progress"]
        if active_items:
            return html + "<ul>" + "\n".join([self._report_item(x) for x in active_items]) + "</ul>"
        else:
            return html

    def get_checksum_html(self, htype):
        basename = Checksummer.get_checksum_filename_basename(htype)
        path = DumpFilename(self.wiki, None, basename)
        web_path = self.dump_dir.web_path_relative(path)
        return '<a href="%s">(%s)</a>' % (web_path, htype)

    def _report_dbstatus_detailed(self, done=False):
        """Put together a status page for this database, with all its component dumps."""
        self.dumpjobdata.noticefile.refresh_notice()
        status_items = [self._report_item(item) for item in self.items]
        status_items.reverse()
        html = "\n".join(status_items)
        checksums = [self.get_checksum_html(htype)
                     for htype in Checksummer.HASHTYPES]
        checksums_html = ", ".join(checksums)
        return self.wiki.config.read_template("report.html") % {
            "db": self.db_name,
            "date": self.wiki.date,
            "notice": self.dumpjobdata.noticefile.notice,
            "status": self._report_status_summary_line(done),
            "previous": self._report_previous_dump(done),
            "items": html,
            "checksum": checksums_html,
            "index": self.wiki.config.index}

    def _report_previous_dump(self, done):
        """Produce a link to the previous dump, if any"""
        # get the list of dumps for this wiki in order, find me in the list,
        # find the one prev to me.
        # why? we might be rerunning a job from an older dumps. we might have two
        # runs going at once (think en pedia, one finishing up the history, another
        # starting at the beginning to get the new abstracts and stubs).
        try:
            dumps_in_order = self.wiki.latest_dump(return_all=True)
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
                sys.stderr.write(repr(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return "No prior dumps of this database stored."
        pretty_date = TimeUtils.pretty_date(raw_date)
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
        html = ("<li class='%s'><span class='updates'>%s</span> "
                "<span class='status'>%s</span> <span class='title'>%s</span>" % (
                    item.status(), item.updated(), item.status(), item.description()))
        if item.progress:
            html += "<div class='progress'>%s</div>\n" % item.progress
        file_objs = item.list_outfiles_to_publish(self.dump_dir)
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
    NAME = "noticefile"

    def __init__(self, wiki, notice, enabled):
        self.wiki = wiki
        self.notice = notice
        self._enabled = enabled
        self.write_notice_file()

    def write_notice_file(self):
        if NoticeFile.NAME in self._enabled:
            notice_file = self._get_notice_filename()
            # delnotice.  toss any existing file
            if self.notice is False:
                if exists(notice_file):
                    os.remove(notice_file)
                self.notice = ""
            # addnotice, stuff notice in a file for other jobs etc
            elif self.notice != "":
                # notice_dir = self._get_notice_dir()
                FileUtils.write_file(self.wiki.config.temp_dir, notice_file, self.notice,
                                     self.wiki.config.fileperms)
            # default case. if there is a file get the contents, otherwise
            # we have empty contents, all good
            else:
                if exists(notice_file):
                    self.notice = FileUtils.read_file(notice_file)

    def refresh_notice(self):
        # if the notice file has changed or gone away, we comply.
        notice_file = self._get_notice_filename()
        if exists(notice_file):
            self.notice = FileUtils.read_file(notice_file)
        else:
            self.notice = ""

    #
    # functions internal to class
    #
    def _get_notice_filename(self):
        return os.path.join(self.wiki.public_dir(), self.wiki.date, "notice.txt")

#    def _get_notice_dir(self):
#        return os.path.join(self.wiki.public_dir(), self.wiki.date)


class SymLinks(object):
    NAME = "symlinks"

    def __init__(self, wiki, dump_dir, logfn, debugfn, enabled):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self._enabled = enabled
        self.logfn = logfn
        self.debugfn = debugfn

    def make_dir(self, dirname):
        if SymLinks.NAME in self._enabled:
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def save_symlink(self, dumpfile):
        if SymLinks.NAME in self._enabled:
            self.make_dir(self.dump_dir.latest_dir())
            realfile = self.dump_dir.filename_public_path(dumpfile)
            latest_filename = dumpfile.new_filename(dumpfile.dumpname, dumpfile.file_type,
                                                    dumpfile.file_ext, 'latest',
                                                    dumpfile.chunk, dumpfile.checkpoint,
                                                    dumpfile.temp)
            link = os.path.join(self.dump_dir.latest_dir(), latest_filename)
            if exists(link) or os.path.islink(link):
                if os.path.islink(link):
                    oldrealfile = os.readlink(link)
                    # format of these links should be...
                    # ../20110228/elwikidb-20110228-templatelinks.sql.gz
                    rellinkpattern = re.compile(r'^\.\./(20[0-9]+)/')
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
            relative = FileUtils.relative_path(realfile, os.path.dirname(link))
            # if we removed the link cause it's obsolete, make the new one
            if exists(realfile) and not exists(link):
                self.debugfn("Adding symlink %s -> %s" % (link, relative))
                os.symlink(relative, link)

    def cleanup_symlinks(self):
        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
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
    def remove_symlinks_from_old_runs(self, date_string, dump_name=None, chunk=None,
                                      checkpoint=None, onlychunks=False):
        # fixme
        # this needs to do more work if there are chunks or checkpoint files linked in here from
        # earlier dates. checkpoint ranges change, and configuration of chunks changes too, so maybe
        # old files still exist and the links need to be removed because we have newer files for the
        # same phase of the dump.

        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfile = os.readlink(link)
                    file_obj = DumpFilename(self.dump_dir._wiki)
                    file_obj.new_from_filename(os.path.basename(realfile))
                    if file_obj.date < date_string:
                        # fixme check that these are ok if the value is None
                        if dump_name and (file_obj.dumpname != dump_name):
                            continue
                        if (chunk or onlychunks) and (file_obj.chunk != chunk):
                            continue
                        if checkpoint and (file_obj.checkpoint != checkpoint):
                            continue
                        self.debugfn("Removing old symlink %s -> %s" % (link, realfile))
                        os.remove(link)


class Feeds(object):
    NAME = "feeds"

    def __init__(self, wiki, dump_dir, dbname, debugfn, enabled):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.db_name = dbname
        self.debugfn = debugfn
        self._enabled = enabled

    def make_dir(self, dirname):
        if Feeds.NAME in self._enabled:
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def save_feed(self, file_obj):
        if Feeds.NAME in self._enabled:
            self.make_dir(self.dump_dir.latest_dir())
            filename_and_path = self.dump_dir.web_path(file_obj)
            web_path = os.path.dirname(filename_and_path)
            rss_text = self.wiki.config.read_template("feed.xml") % {
                "chantitle": file_obj.basename,
                "chanlink": web_path,
                "chandesc": "Wikimedia dump updates for %s" % self.db_name,
                "title": web_path,
                "link": web_path,
                "description": xml_escape("<a href=\"%s\">%s</a>" % (
                    filename_and_path, file_obj.filename)),
                "date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            }
            rss_path = os.path.join(self.dump_dir.latest_dir(),
                                    self.db_name + "-latest-" + file_obj.basename +
                                    "-rss.xml")
            self.debugfn("adding rss feed file %s " % rss_path)
            FileUtils.write_file(self.wiki.config.temp_dir, rss_path,
                                 rss_text, self.wiki.config.fileperms)

    def cleanup_feeds(self):
        # call this after sym links in this dir have been cleaned up.
        # we should probably fix this so there is no such dependency,
        # but it would mean parsing the contents of the rss file, bleah
        if Feeds.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            for fname in files:
                if fname.endswith("-rss.xml"):
                    filename = fname[:-8]
                    link = os.path.join(latest_dir, filename)
                    if not exists(link):
                        self.debugfn("Removing old rss feed %s for link %s" % (
                            os.path.join(latest_dir, fname), link))
                        os.remove(os.path.join(latest_dir, fname))


class DumpRunJobData(object):
    def __init__(self, wiki, dump_dir, notice, logfn=None, debugfn=None,
                 enabled=None, verbose=False):
        if enabled is None:
            enabled = {}
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.logfn = logfn
        self.debugfn = debugfn
        self.enabled = enabled
        self.verbose = verbose
        self.runinfofile = RunInfoFile(wiki, enabled, verbose)
        self.checksummer = Checksummer(wiki, dump_dir, enabled, verbose)
        self.feeds = Feeds(wiki, dump_dir, wiki.db_name, debugfn, enabled)
        self.symlinks = SymLinks(wiki, dump_dir, logfn, debugfn, enabled)
        self.noticefile = NoticeFile(wiki, notice, enabled)

    def do_before_dump(self):
        self.checksummer.prepare_checksums()

    def do_after_dump(self, dump_items):
        self.checksummer.move_chksumfiles_into_place()

        # note that it's possible for links in "latest" to point to
        # files from different runs, in which case the checksum files
        # will have accurate checksums for the run for which it was
        # produced, but not the other files. FIXME
        for htype in Checksummer.HASHTYPES:
            dumpfile = DumpFilename(
                self.wiki, None, self.checksummer.get_checksum_filename_basename(htype))
            self.symlinks.save_symlink(dumpfile)
            self.symlinks.cleanup_symlinks()
        for item in dump_items:
            self.runinfofile.save_dump_runinfo_file(RunInfoFile.report_dump_runinfo(dump_items))
            if item.to_run():
                dump_names = item.list_dumpnames()
                if type(dump_names).__name__ != 'list':
                    dump_names = [dump_names]
                if item._chunks_enabled:
                    # if there is a specific chunk, we want to only clear out
                    # old files for that piece, because new files for the other
                    # pieces may not have been generated yet.
                    chunk = item._chunk_todo
                else:
                    chunk = None

                checkpoint = None
                if item._checkpoints_enabled:
                    if item.checkpoint_file is not None:
                        # if there's a specific checkpoint file we are
                        # rerunning, we would only clear out old copies
                        # of that very file. meh. how likely is it that we
                        # have one? these files are time based and the start/end pageids
                        # are going to fluctuate. whatever
                        checkpoint = item.checkpoint_file.checkpoint

                for dump in dump_names:
                    self.symlinks.remove_symlinks_from_old_runs(
                        self.wiki.date, dump, chunk, checkpoint, onlychunks=item.onlychunks)

                self.feeds.cleanup_feeds()

    def do_before_job(self, dump_items):
        self.runinfofile.save_dump_runinfo_file(
            RunInfoFile.report_dump_runinfo(dump_items))

    def do_after_job(self, item):
        self.checksummer.cp_chksum_tmpfiles_to_permfile()
        # this will include checkpoint files if they are enabled.
        for file_obj in item.list_outfiles_to_publish(self.dump_dir):
            if exists(self.dump_dir.filename_public_path(file_obj)):
                # why would the file not exist? because we changed chunk numbers in the
                # middle of a run, and now we list more files for the next stage than there
                # were for earlier ones
                self.symlinks.save_symlink(file_obj)
                self.feeds.save_feed(file_obj)
                self.checksummer.checksums(file_obj, self)
                self.symlinks.cleanup_symlinks()
                self.feeds.cleanup_feeds()

    def do_latest_job(self):
        self.symlinks.remove_symlinks_from_old_runs(self.wiki.date)
        self.feeds.cleanup_feeds()


class RunInfoFile(object):
    NAME = "runinfofile"

    def report_dump_runinfo(dump_items):
        """Put together a dump run info listing for this database, with all its component dumps."""
        runinfo_lines = ["name:%s; status:%s; updated:%s" %
                         (item.name(), item.status(), item.updated())
                         for item in dump_items]
        runinfo_lines.reverse()
        text = "\n".join(runinfo_lines)
        text = text + "\n"
        return text
    report_dump_runinfo = staticmethod(report_dump_runinfo)

    def __init__(self, wiki, enabled, verbose=False):
        self.wiki = wiki
        self._enabled = enabled
        self.verbose = verbose

    def save_dump_runinfo_file(self, text):
        """Write out a simple text file with the status for this wiki's dump."""
        if RunInfoFile.NAME in self._enabled:
            try:
                self._write_dump_runinfo_file(text)
            except:
                if self.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    sys.stderr.write(repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback)))
                sys.stderr.write("Couldn't save dump run info file. Continuing anyways\n")

    def status_of_old_dump_is_done(self, runner, date, job_name, job_desc):
        old_dump_runinfo_filename = self._get_dump_runinfo_filename(date)
        status = self._get_status_from_runinfo(old_dump_runinfo_filename, job_name)
        if status == "done":
            return 1
        elif status is not None:
            # failure, in progress, some other useless thing
            return 0

        # ok, there was no info there to be had, try the index file. yuck.
        index_filename = os.path.join(runner.wiki.public_dir(),
                                      date, runner.wiki.config.perdump_index)
        status = self._get_status_from_html(index_filename, job_desc)
        if status == "done":
            return 1
        else:
            return 0

    def get_old_runinfo_from_file(self):
        # read the dump run info file in, if there is one, and get info about which dumps
        # have already been run and whether they were successful
        dump_runinfo_filename = self._get_dump_runinfo_filename()
        results = []

        if not os.path.exists(dump_runinfo_filename):
            return False

        try:
            infile = open(dump_runinfo_filename, "r")
            for line in infile:
                results.append(self._get_old_runinfo_from_line(line))
            infile.close()
            return results
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return False

    #
    # functions internal to the class
    #
    def _get_dump_runinfo_filename(self, date=None):
        # sometimes need to get this info for an older run to check status of a file for
        # possible prefetch
        if date:
            return os.path.join(self.wiki.public_dir(), date, "dumpruninfo.txt")
        else:
            return os.path.join(self.wiki.public_dir(), self.wiki.date, "dumpruninfo.txt")

    def _get_dump_runinfo_dirname(self, date=None):
        if date:
            return os.path.join(self.wiki.public_dir(), date)
        else:
            return os.path.join(self.wiki.public_dir(), self.wiki.date)

    # format: name:%; updated:%; status:%
    def _get_old_runinfo_from_line(self, line):
        # get rid of leading/trailing/blanks
        line = line.strip(" ")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        dump_runinfo = {}
        for field in fields:
            field = field.strip(" ")
            (fieldname, sep_unused, field_value) = field.partition(':')
            if fieldname in ["name", "status", "updated"]:
                dump_runinfo[fieldname] = field_value
        return dump_runinfo

    def _write_dump_runinfo_file(self, text):
        dump_runinfo_filename = self._get_dump_runinfo_filename()
#        FileUtils.write_file(directory, dumpRunInfoFilename, text, self.wiki.config.fileperms)
        FileUtils.write_file_in_place(dump_runinfo_filename, text, self.wiki.config.fileperms)

    # format: name:%; updated:%; status:%
    def _get_status_from_runinfo_line(self, line, job_name):
        # get rid of leading/trailing/embedded blanks
        line = line.replace(" ", "")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        for field in fields:
            (fieldname, sep_unused, field_value) = field.partition(':')
            if fieldname == "name":
                if not field_value == job_name:
                    return None
            elif fieldname == "status":
                return field_value

    def _get_status_from_runinfo(self, filename, job_name=""):
        # read the dump run info file in, if there is one, and find out whether
        # a particular job (one step only, not a multiple piece job) has been
        # already run and whether it was successful (use to examine status
        # of step from some previous run)
        try:
            infile = open(filename, "r")
            for line in infile:
                result = self._get_status_from_runinfo_line(line, job_name)
                if result is not None:
                    return result
            infile.close()
            return None
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None

    # find desc in there, look for "class='done'"
    def _get_status_from_html_line(self, line, desc):
        if ">"+desc+"<" not in line:
            return None
        if "<li class='done'>" in line:
            return "done"
        else:
            return "other"

    def _get_status_from_html(self, filename, desc):
        # read the index file in, if there is one, and find out whether
        # a particular job (one step only, not a multiple piece job) has been
        # already run and whether it was successful (use to examine status
        # of step from some previous run)
        try:
            infile = open(filename, "r")
            for line in infile:
                result = self._get_status_from_html_line(line, desc)
                if result is not None:
                    return result
            infile.close()
            return None
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None


