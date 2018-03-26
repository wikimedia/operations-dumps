# Worker process, does the actual dumping
import os
from os.path import exists
import re
import sys
import time
import traceback
from email.mime import text as MIMEText
import smtplib
import json

from dumps.exceptions import BackupError
from dumps.fileutils import DumpContents, DumpFilename, FileUtils
from dumps.utils import TimeUtils
from dumps.specialfilesregistry import Registered


def xml_escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class Maintenance(object):

    @staticmethod
    def in_maintenance_mode():
        """Use this to let callers know that we really should not
        be running.  Callers should try to exit the job
        they are running as soon as possible."""
        return exists("maintenance.txt")

    @staticmethod
    def exit_if_in_maintenance_mode(message=None):
        """Call this from possible exit points of running jobs
        in order to exit if we need to"""
        if Maintenance.in_maintenance_mode():
            if message:
                raise BackupError(message)
            else:
                raise BackupError("In maintenance mode, exiting.")


class Checksummer(Registered):
    NAME = "checksum"
    HASHTYPES = ['md5', 'sha1']
    FORMATS = ['txt', 'json']

    @staticmethod
    def get_checksum_filename_basename(htype, fmt="txt"):
        if fmt == "json":
            ext = "json"
        else:
            # default
            ext = "txt"

        if htype == "md5":
            return "md5sums." + ext
        elif htype == "sha1":
            return "sha1sums." + ext
        else:
            return None

    @staticmethod
    def get_hashinfo(filename, jsoninfo):
        """
        given json output from the checksum json file,
        find and return list of tuples (hashtype, sum) for the
        file
        """
        results = []
        if not jsoninfo:
            return results
        for htype in jsoninfo:
            if filename in jsoninfo[htype]["files"]:
                results.append((htype, jsoninfo[htype]["files"][filename]))
        return results

    @staticmethod
    def get_empty_json():
        return {}

    def __init__(self, wiki, enabled, dump_dir=None, verbose=False):
        super(Checksummer, self).__init__()
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
                for fmt in Checksummer.FORMATS:
                    checksum_filename = self._get_checksum_filename_tmp(htype, fmt)
                    with open(checksum_filename, "w") as output_fhandle:
                        if fmt == "json":
                            output_fhandle.write(json.dumps({htype: {"files": {}}}))
                        output_fhandle.close()

    def checksums(self, dfname, dumpjobdata):
        """
        Run checksum for an output file, and append to the list.
        args:
            DumpFilename, ...
        """
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                checksum_filename_txt = self._get_checksum_filename_tmp(htype, "txt")
                checksum_filename_json = self._get_checksum_filename_tmp(htype, "json")
                output_txt = file(checksum_filename_txt, "a")
                # for txt file, append our new line. for json file, must read
                # previous contents, stuff our new info into the dict, write it
                # back out
                input_json = file(checksum_filename_json, "a")
                output = {}
                try:
                    with open(checksum_filename_json, "r") as fhandle:
                        contents = fhandle.read()
                        output = json.loads(contents)
                except Exception:
                    # might be empty file, as at the start of a run
                    pass
                if not output:
                    # at least let's not write new bad content into a
                    # possibly corrupt file.
                    output = {htype: {"files": {}}}
                output_json = file(checksum_filename_json, "w")
                dumpjobdata.debugfn("Checksumming %s via %s" % (dfname.filename, htype))
                dcontents = DumpContents(
                    self.wiki, dumpjobdata.dump_dir.filename_public_path(dfname),
                    None, self.verbose)
                checksum = dcontents.checksum(htype)
                if checksum is not None:
                    output_txt.write("%s  %s\n" % (checksum, dfname.filename))
                    output[htype]["files"][dfname.filename] = checksum
                # always write a json stanza, even if no file info included.
                output_json.write(json.dumps(output))
                output_txt.close()
                output_json.close()

    def move_chksumfiles_into_place(self):
        # after the run we move the temp file into the permanent
        # location, as we have finished
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                for fmt in Checksummer.FORMATS:
                    tmp_filename = self._get_checksum_filename_tmp(htype, fmt)
                    real_filename = self._get_checksum_path(htype, fmt)
                    os.rename(tmp_filename, real_filename)

    def cp_chksum_tmpfiles_to_permfile(self):
        # during the run we copy what we've done into the permanent location
        # after each job etc
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                for fmt in Checksummer.FORMATS:
                    tmp_filename = self._get_checksum_filename_tmp(htype, fmt)
                    real_filename = self._get_checksum_path(htype, fmt)
                    content = FileUtils.read_file(tmp_filename)
                    FileUtils.write_file(self.wiki.config.temp_dir, real_filename, content,
                                         self.wiki.config.fileperms)

    def get_all_output_files(self):
        """
        return a list of all checksum files in all formats
        """
        files = []
        for htype in Checksummer.HASHTYPES:
            for fmt in Checksummer.FORMATS:
                files.append(self._get_checksum_filename(htype, fmt))
        return files

    #
    # functions internal to the class
    #

    def _get_checksum_filename(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt))
        return dfname.filename

    def _get_checksum_path(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            full path of output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt))
        return self.dump_dir.filename_public_path(dfname)

    def _get_checksum_filename_tmp(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            full path of a unique-enough temporary output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt) +
                              "." + self.timestamp + ".tmp")
        return self.dump_dir.filename_public_path(dfname)

    def _getmd5file_dir_name(self):
        """
        returns:
            directory for dump wiki and date, in which hashfiles reside
        """
        return os.path.join(self.wiki.public_dir(), self.wiki.date)


class Report(Registered):
    '''
    methods for generation of the index.html file and the json file for a dump
    run for a given wiki and date
    '''
    NAME = "report"
    JSONFILE = "report.json"

    @staticmethod
    def report_dump_step_status(dump_dir, item):
        """Return an HTML fragment and a json object with info on the progress of this dump step."""
        item.status()
        item.updated()
        item.description()
        html = ("<li class='%s'><span class='updates'>%s</span> "
                "<span class='status'>%s</span> <span class='title'>%s</span>" % (
                    item.status(), item.updated(), item.status(), item.description()))
        if item.progress:
            html += "<div class='progress'>%s</div>\n" % item.progress
        dfnames = item.list_outfiles_to_publish(dump_dir)
        if dfnames:
            list_items = [Report.report_file_size_status(dump_dir, dfname, item.status())
                          for dfname in dfnames]
            html += "<ul>"
            detail = item.detail()
            if detail:
                html += "<li class='detail'>%s</li>\n" % detail
            html += "\n".join([entry['txt'] for entry in list_items])
            html += "</ul>"
            json_out = {item.name():
                        {'files':
                         {entry['json']['name']:
                          dict((key, entry['json'][key]) for key in entry['json'] if key != 'name')
                          for entry in list_items}}}
        else:
            json_out = {'job': item.name()}
        html += "</li>"
        content = {'html': html, 'json': json_out}
        return content

    # this is a per-dump-item report (well, per file generated by the item)
    # Report on the file size & item status of the current output and output a link if we are done
    @staticmethod
    def report_file_size_status(dump_dir, dfname, item_status):
        """
        args:
            DumpDir
            DumpFilename
            status ("in-progress", "missing", ...)
        """
        filename = dump_dir.filename_public_path(dfname)
        size = None
        if exists(filename):
            size = os.path.getsize(filename)
        elif item_status == "in-progress":
            # note that because multiple files may be produced for a single dump
            # job, some may be complete while others are still in progress.
            # therefore we check the normal name first, falling back to the
            # inprogress name.
            filename = filename + DumpFilename.INPROG
            if exists(filename):
                size = os.path.getsize(filename)
        if size is None:
            item_status = "missing"
            size = 0
        pretty_size = FileUtils.pretty_size(size)
        if item_status == "in-progress":
            txt = "<li class='file'>%s %s (written) </li>" % (dfname.filename, pretty_size)
            json_out = {'name': dfname.filename, 'size': size}
        elif item_status == "done":
            webpath_relative = dump_dir.web_path_relative(dfname)
            txt = ("<li class='file'><a href=\"%s\">%s</a> %s</li>"
                   % (webpath_relative, dfname.filename, pretty_size))
            json_out = {'name': dfname.filename, 'size': size,
                        'url': webpath_relative}
        else:
            txt = "<li class='missing'>%s</li>" % dfname.filename
            json_out = {'name': dfname.filename}
        content = {'txt': txt, 'json': json_out}
        return content

    @staticmethod
    def get_jobs(jsoninfo):
        """
        given json output from report file, return the list
        of job names covered in the output
        """
        if jsoninfo is None or "jobs" not in jsoninfo:
            return []
        else:
            return jsoninfo['jobs'].keys()

    @staticmethod
    def get_fileinfo_for_job(jobname, reportinfo):
        """
        given json output from report file, and a job name,
        return info about the files associated with that job
        """
        try:
            return reportinfo['jobs'][jobname]["files"]
        except Exception:
            return {}

    @staticmethod
    def get_filenames_for_job(jobname, reportinfo):
        return Report.get_fileinfo_for_job(jobname, reportinfo).keys()

    def __init__(self, wiki, enabled, dump_dir=None, items=None, dumpjobdata=None,
                 failhandler=None, error_callback=None, verbose=False):
        super(Report, self).__init__()
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.items = items
        self.dumpjobdata = dumpjobdata
        self.error_callback = error_callback
        self.verbose = verbose
        self._enabled = enabled
        self.failhandler = failhandler

    @staticmethod
    def add_file_property(jobname, filename, prop, value, reportinfo):
        """
        given json output from report file, a job name, a filename, and
        a property and value, add the property and value to the filename
        specified for the given job
        """
        try:
            if "files" not in reportinfo["jobs"][jobname]:
                reportinfo["jobs"][jobname] = {}
            reportinfo["jobs"][jobname]["files"][filename][prop] = value
        except Exception:
            pass

    def report_previous_dump_link(self, done):
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
                raw_date = dumps_in_order[me_index - 1]
            elif me_index == 0:
                # We are the first item in the list. This is not an error, but there is no
                # previous dump
                return "No prior dumps of this database stored."
            else:
                raise ValueError
        except Exception as ex:
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

    def get_checksum_html(self, htype):
        basename = Checksummer.get_checksum_filename_basename(htype)
        path = DumpFilename(self.wiki, None, basename)
        web_path = self.dump_dir.web_path_relative(path)
        return '<a href="%s">(%s)</a>' % (web_path, htype)

    def update_index_html_and_json(self, dump_status=""):
        '''
        generate the index.html file for the wiki's dump run which contains
        information on each dump step as well as links to completed files
        for download, hash files, etc. and links to completed files;
        generate the json file with the same information as well'''
        if Report.NAME in self._enabled:

            self.dumpjobdata.notice.refresh_notice()
            status_items = [Report.report_dump_step_status(self.dump_dir, item)
                            for item in self.items]
            status_items_html = [item['html'] for item in status_items]
            status_items_html.reverse()
            html = "\n".join(status_items_html)
            checksums = [self.get_checksum_html(htype)
                         for htype in Checksummer.HASHTYPES]
            checksums_html = ", ".join(checksums)
            failed_jobs = sum(1 for item in self.items if item.status() == "failed")
            txt = self.wiki.config.read_template("report.html") % {
                "db": self.wiki.db_name,
                "date": self.wiki.date,
                "notice": self.dumpjobdata.notice.notice,
                "status": StatusHtml.report_dump_status(failed_jobs, dump_status),
                "previous": self.report_previous_dump_link(dump_status),
                "items": html,
                "checksum": checksums_html,
                "index": self.wiki.config.index}

            json_out = {'jobs': {}}
            for item in status_items:
                for jobname in item['json']:
                    json_out['jobs'][jobname] = item['json'][jobname]
            try:
                indexpath = os.path.join(self.wiki.public_dir(), self.wiki.date,
                                         self.wiki.config.perdump_index)
                FileUtils.write_file_in_place(indexpath, txt, self.wiki.config.fileperms)
                json_filepath = os.path.join(self.wiki.public_dir(), self.wiki.date,
                                             Report.JSONFILE)
                FileUtils.write_file_in_place(json_filepath, json.dumps(json_out),
                                              self.wiki.config.fileperms)
            except Exception as ex:
                if self.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                     exc_traceback)))
                message = "Couldn't update status files. Continuing anyways"
                if self.error_callback:
                    self.error_callback(message)
                else:
                    sys.stderr.write("%s\n" % message)

    def get_all_output_files(self):
        """
        return list of all report files in all known formats
        """
        files = []
        files.append(self.wiki.config.perdump_index)
        files.append(Report.JSONFILE)
        return files


class FailureHandler(object):
    '''
    do the right thing on notification of a failure for a dump step
    '''
    def __init__(self, wiki, email):
        self.wiki = wiki
        # number of times jobs have failed on this run
        self.failure_count = 0
        self.email = email

    def report_failure(self):
        if self.email:
            if self.wiki.config.admin_mail and self.wiki.config.admin_mail.lower() != 'nomail':
                subject = "Dump failure for " + self.wiki.db_name
                message = self.wiki.config.read_template("errormail.txt") % {
                    "db": self.wiki.db_name,
                    "date": self.wiki.date,
                    "time": TimeUtils.pretty_time(),
                    "url": "/".join((self.wiki.config.web_root, self.wiki.db_name,
                                     self.wiki.date, ''))}
                self.mail(subject, message)

    def mail(self, subject, body):
        """Send out a quickie email."""
        message = MIMEText.MIMEText(body)
        message["Subject"] = subject
        message["From"] = self.wiki.config.mail_from
        message["To"] = self.wiki.config.admin_mail
        try:
            server = smtplib.SMTP(self.wiki.config.smtp_server)
            server.sendmail(self.wiki.config.mail_from, self.wiki.config.admin_mail,
                            message.as_string())
            server.close()
        except Exception as ex:
            print "MAIL SEND FAILED! GODDAMIT! Was sending this mail:"
            print message


class StatusHtml(Registered):
    NAME = "status"

    @staticmethod
    def get_statusfilename():
        return "status.html"

    @staticmethod
    def report_dump_status(num_jobs_failed, dump_status=""):
        if dump_status == "done":
            classes = "done"
            text = "Dump complete"
        elif dump_status == "partialdone":
            classes = "partial-dump"
            text = "Partial dump"
        else:
            classes = "in-progress"
            text = "Dump in progress"
        if num_jobs_failed > 0:
            classes += " failed"
            if num_jobs_failed == 1:
                ess = ""
            else:
                ess = "s"
            text += ", %d item%s failed" % (num_jobs_failed, ess)
        return "<span class='%s'>%s</span>" % (classes, text)

    @staticmethod
    def get_statusfile_path(wiki, date):
        return os.path.join(wiki.public_dir(), date, StatusHtml.get_statusfilename())

    @staticmethod
    def status_line(wiki, aborted=False):
        date = wiki.latest_dump()
        if date:
            if aborted:
                return StatusHtml.report_statusline(
                    wiki, "<span class=\"failed\">dump aborted</span>")

            status = StatusHtml.get_statusfile_path(wiki, date)
            try:
                return FileUtils.read_file(status)
            except Exception as ex:
                return StatusHtml.report_statusline(wiki, "missing status record")
        else:
            return StatusHtml.report_statusline(wiki, "has not yet been dumped")

    @staticmethod
    def report_statusline(wiki, status, error=False):
        if error:
            # No state information, hide the timestamp
            stamp = "<span style=\"visible: none\">" + TimeUtils.pretty_time() + "</span>"
        else:
            stamp = TimeUtils.pretty_time()
        if wiki.is_private():
            link = "%s (private data)" % wiki.db_name
        else:
            if wiki.date:
                link = "<a href=\"%s/%s\">%s</a>" % (wiki.db_name, wiki.date, wiki.db_name)
            else:
                link = "%s (new)" % wiki.db_name
            if wiki.is_closed():
                link = link + " (closed)"
        return "<li>%s %s: %s</li>\n" % (stamp, link, status)

    @staticmethod
    def write_status(wiki, message):
        index = StatusHtml.get_statusfile_path(wiki, wiki.date)
        FileUtils.write_file_in_place(index, message, wiki.config.fileperms)

    def __init__(self, wiki, enabled, dump_dir=None, items=None, dumpjobdata=None,
                 failhandler=None, error_callback=None, verbose=False):
        super(StatusHtml, self).__init__()
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.items = items
        self.dumpjobdata = dumpjobdata
        self.error_callback = error_callback
        self.failhandler = failhandler
        self.verbose = verbose
        self._enabled = enabled

    def update_status_file(self, done=False):
        """Write out a status HTML file with the status for this wiki's dump;
        this file is used by the monitor to generate an index.html covering dumps
        of all wikis."""
        if StatusHtml.NAME in self._enabled:

            try:
                # Short line for report extraction goes here
                StatusHtml.write_status(self.wiki, self._report_dump_status_html(done))
            except Exception as ex:
                if self.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                     exc_traceback)))
                message = "Couldn't update status html file. Continuing anyways"
                if self.error_callback:
                    self.error_callback(message)
                else:
                    sys.stderr.write("%s\n" % message)

    def _report_dump_status_html(self, done=False):
        """Put together a brief status summary and link for the current database."""
        failed_jobs = sum(1 for item in self.items if item.status() == "failed")
        status = StatusHtml.report_dump_status(failed_jobs, done)
        html = StatusHtml.report_statusline(self.wiki, status)

        active_items = [x for x in self.items if x.status() == "in-progress"]
        if active_items:
            return html + "<ul>" + "\n".join([
                Report.report_dump_step_status(self.dump_dir, x)['html']
                for x in active_items]) + "</ul>"
        else:
            return html

    def get_all_output_files(self):
        return [self.get_statusfilename()]


class Notice(object):
    NAME = "notice"

    def __init__(self, wiki, notice, enabled):
        self.wiki = wiki
        self.notice = notice
        self._enabled = enabled
        self.write_notice()

    def write_notice(self):
        if Notice.NAME in self._enabled:
            notice_filepath = self._get_notice_filename()
            # delnotice.  toss any existing file
            if self.notice is False:
                if exists(notice_filepath):
                    os.remove(notice_filepath)
                self.notice = ""
            # addnotice, stuff notice in a file for other jobs etc
            elif self.notice != "":
                # notice_dir = self._get_notice_dir()
                FileUtils.write_file(self.wiki.config.temp_dir, notice_filepath, self.notice,
                                     self.wiki.config.fileperms)
            # default case. if there is a file get the contents, otherwise
            # we have empty contents, all good
            else:
                if exists(notice_filepath):
                    self.notice = FileUtils.read_file(notice_filepath)

    def refresh_notice(self):
        # if the notice file has changed or gone away, we comply.
        notice_filepath = self._get_notice_filename()
        if exists(notice_filepath):
            self.notice = FileUtils.read_file(notice_filepath)
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
            realfilepath = self.dump_dir.filename_public_path(dumpfile)
            latest_filename = dumpfile.new_filename(dumpfile.dumpname, dumpfile.file_type,
                                                    dumpfile.file_ext, 'latest',
                                                    dumpfile.partnum, dumpfile.checkpoint,
                                                    dumpfile.temp)
            link = os.path.join(self.dump_dir.latest_dir(), latest_filename)
            if exists(link) or os.path.islink(link):
                if os.path.islink(link):
                    oldrealfilepath = os.readlink(link)
                    # format of these links should be...
                    # ../20110228/elwikidb-20110228-templatelinks.sql.gz
                    rellinkpattern = re.compile(r'^\.\./(20[0-9]+)/')
                    dateinlink = rellinkpattern.search(oldrealfilepath)
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
            relative = FileUtils.relative_path(realfilepath, os.path.dirname(link))
            # if we removed the link cause it's obsolete, make the new one
            if exists(realfilepath) and not exists(link):
                self.debugfn("Adding symlink %s -> %s" % (link, relative))
                os.symlink(relative, link)

    def cleanup_symlinks(self):
        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfilepath = os.readlink(link)
                    if not exists(os.path.join(latest_dir, realfilepath)):
                        os.remove(link)

    # if the args are False or None, we remove all the old links for all values of the arg.
    # example: if partnum is False or None then we remove all old values for all file parts
    # "old" means "older than the specified datestring".
    def remove_symlinks_from_old_runs(self, date_string, dump_name=None, partnum=None,
                                      checkpoint=None, onlyparts=False, keepextra=1):
        # fixme
        # this needs to do more work if there are file parts or checkpoint files linked in here from
        # earlier dates. checkpoint ranges change, and configuration of parallel jobs for file parts
        # changes too, so maybe old files still exist and the links need to be removed because we
        # have newer files for the same phase of the dump.
        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            dates = []

            files_for_cleanup = []
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfilepath = os.readlink(link)
                    dfname = DumpFilename(self.dump_dir._wiki)
                    dfname.new_from_filename(os.path.basename(realfilepath))
                    files_for_cleanup.append({'link': link, 'dfname': dfname, 'path': realfilepath})
                    dates.append(dfname.date)
            try:
                index = dates.index(date_string)
                prev_run_date = dates[index - 1]
            except Exception:
                if len(dates) >= 2:
                    prev_run_date = dates[-2]
                else:
                    prev_run_date = None
            for item in files_for_cleanup:
                if item['dfname'].date < date_string:
                    if dump_name and (item['dfname'].dumpname != dump_name):
                        continue
                    if prev_run_date is None or item['dfname'].date == prev_run_date:
                        # for the previous run, or the only existing run, if different
                        # from the current one, we are very careful. For all older runs
                        # we pretty much want to toss everything

                        # fixme check that these are ok if the value is None
                        if (partnum or onlyparts) and (item['dfname'].partnum != partnum):
                            continue
                        if checkpoint and (item['dfname'].checkpoint != checkpoint):
                            continue
                    self.debugfn("Removing old symlink %s -> %s" % (item['link'], item['path']))
                    os.remove(item['link'])


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

    def save_feed(self, dfname):
        """
        args:
            DumpFilename
        """
        if Feeds.NAME in self._enabled:
            self.make_dir(self.dump_dir.latest_dir())
            filename_and_path = self.dump_dir.web_path(dfname)
            web_path = os.path.dirname(filename_and_path)
            rss_text = self.wiki.config.read_template("feed.xml") % {
                "chantitle": dfname.basename,
                "chanlink": web_path,
                "chandesc": "Wikimedia dump updates for %s" % self.db_name,
                "title": web_path,
                "link": web_path,
                "description": xml_escape("<a href=\"%s\">%s</a>" % (
                    filename_and_path, dfname.filename)),
                "date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            }
            rss_path = os.path.join(self.dump_dir.latest_dir(),
                                    self.db_name + "-latest-" + dfname.basename +
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


class RunSettings(object):
    NAME = 'runsettings'

    def __init__(self, wiki, dump_dir, logfn=None, debugfn=None,
                 enabled=None, verbose=False):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.logfn = logfn
        self.debugfn = debugfn
        self.enabled = enabled
        self.verbose = verbose

    def get_settings_path(self):
        """
        returns:
            full path to runsettings file
        """
        dfname = DumpFilename(self.wiki, None, "runsettings.txt")
        return self.dump_dir.filename_public_path(dfname)

    def get_settings_from_config(self):
        return [self.wiki.config.parts_enabled,
                self.wiki.config.pages_per_filepart_history,
                self.wiki.config.revs_per_filepart_history,
                self.wiki.config.numparts_for_abstract,
                self.wiki.config.numparts_for_pagelogs,
                self.wiki.config.pages_per_filepart_abstract,
                self.wiki.config.recombine_metacurrent,
                self.wiki.config.recombine_history,
                self.wiki.config.checkpoint_time]

    def write_settings(self):
        '''
        stash current run settings in file in dump directory if
        such file does not already exist
        '''
        if RunSettings.NAME not in self.enabled:
            return

        settings_path = self.get_settings_path()
        if os.path.exists(settings_path):
            return
        setting_info = self.get_settings_from_config()

        if not os.path.exists(os.path.join(self.wiki.public_dir(), self.wiki.date)):
            os.makedirs(os.path.join(self.wiki.public_dir(), self.wiki.date))
        if not os.path.exists(os.path.join(self.wiki.private_dir(), self.wiki.date)):
            os.makedirs(os.path.join(self.wiki.private_dir(), self.wiki.date))

        with open(settings_path, "w+") as settings_fhandle:
            settings_fhandle.write(json.dumps(setting_info) + "\n")

    def read_settings(self):
        '''
        retrieve current run settings from file in dump directory
        '''
        settings_path = self.get_settings_path()
        if not os.path.exists(settings_path):
            return None
        with open(settings_path, "r") as settings_fhandle:
            contents = settings_fhandle.read()
            settings_fhandle.close()
        if contents[-1] == '\n':
            contents = contents[:-1]
        return json.loads(contents)

    def apply_settings_to_config(self, settings=None):
        '''
        apply settings to wiki configuration, retrieving
        them from the settings stash file first if they are
        not passed in as an argument
        '''
        if settings is None:
            settings = self.read_settings()
        if settings is None:
            return
        self.wiki.config.parts_enabled = settings[0]
        self.wiki.config.pages_per_filepart_history = settings[1]
        self.wiki.config.revs_per_filepart_history = settings[2]
        self.wiki.config.numparts_for_abstract = settings[3]
        self.wiki.config.numparts_for_pagelogs = settings[4]
        self.wiki.config.pages_per_filepart_abstract = settings[5]
        self.wiki.config.recombine_metacurrent = settings[6]
        self.wiki.config.recombine_history = settings[7]
        self.wiki.config.checkpoint_time = settings[8]


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

        # write config settings down if not already present
        self.settings_stash = RunSettings(wiki, dump_dir, logfn, debugfn, enabled, verbose)
        self.settings_stash.write_settings()
        # if there was a settings stash, use it to override config values
        self.settings_stash.apply_settings_to_config()

        # now we can set up everything else
        self.runinfo = RunInfo(wiki, enabled, verbose)
        self.checksummer = Checksummer(wiki, enabled, dump_dir, verbose)
        self.feeds = Feeds(wiki, dump_dir, wiki.db_name, debugfn, enabled)
        self.symlinks = SymLinks(wiki, dump_dir, logfn, debugfn, enabled)
        self.notice = Notice(wiki, notice, enabled)

    def do_before_dump(self):
        self.checksummer.prepare_checksums()

    def do_after_dump(self, dump_items):
        # note that it's possible for links in "latest" to point to
        # files from different runs, in which case the checksum files
        # will have accurate checksums for the run for which it was
        # produced, but not the other files. FIXME
        for htype in Checksummer.HASHTYPES:
            dfname = DumpFilename(
                self.wiki, None, self.checksummer.get_checksum_filename_basename(htype))
            self.symlinks.save_symlink(dfname)
            self.symlinks.cleanup_symlinks()
        for item in dump_items:
            self.runinfo.save_dump_runinfo(RunInfo.report_dump_runinfo(dump_items))
            if item.to_run():
                dump_names = item.list_dumpnames()
                if type(dump_names).__name__ != 'list':
                    dump_names = [dump_names]
                if item._parts_enabled:
                    # if there is a specific part we are doing, we want to only clear out
                    # old files for that part, because new files for the other
                    # parts may not have been generated yet.
                    partnum = item._partnum_todo
                else:
                    partnum = None

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
                        self.wiki.date, dump, partnum, checkpoint, onlyparts=item.onlyparts)

                self.feeds.cleanup_feeds()

    def do_before_job(self, dump_items):
        self.runinfo.save_dump_runinfo(
            RunInfo.report_dump_runinfo(dump_items))

    def do_after_job(self, item, dump_items):
        """
        args:
            Dump, list of Dump
        """
        self.checksummer.cp_chksum_tmpfiles_to_permfile()
        # this will include checkpoint files if they are enabled.
        for dfname in item.list_outfiles_to_publish(self.dump_dir):
            if exists(self.dump_dir.filename_public_path(dfname)):
                # why would the file not exist? because we changed number of file parts in the
                # middle of a run, and now we list more files for the next stage than there
                # were for earlier ones
                self.symlinks.save_symlink(dfname)
                self.feeds.save_feed(dfname)
                self.checksummer.checksums(dfname, self)
                self.symlinks.cleanup_symlinks()
                self.feeds.cleanup_feeds()
        self.runinfo.save_dump_runinfo(
            RunInfo.report_dump_runinfo(dump_items))

    def do_latest_job(self):
        self.symlinks.remove_symlinks_from_old_runs(self.wiki.date)
        self.feeds.cleanup_feeds()


class RunInfo(Registered):
    NAME = "runinfo"
    FORMATS = ['txt', 'json']

    @staticmethod
    def get_runinfo_basename():
        return "dumpruninfo"

    @staticmethod
    def report_dump_runinfo(dump_items):
        """Put together a dump run info listing for this database, with all its component dumps."""
        runinfo_lines = ["name:%s; status:%s; updated:%s" %
                         (item.name(), item.status(), item.updated())
                         for item in dump_items]
        runinfo_lines.reverse()
        txt_content = "\n".join(runinfo_lines)
        content = {}
        content['txt'] = txt_content + "\n"
        # {"jobs": {name: {"status": stuff, "updated": stuff}}, othername: {...}, ...}
        content_json = {"jobs": {}}
        for item in sorted(dump_items, reverse=True):
            content_json["jobs"][item.name()] = {'status': item.status(), 'updated': item.updated()}
        content['json'] = json.dumps(content_json)
        return content

    @staticmethod
    def add_job_property(jobname, jproperty, value, dumpruninfo):
        """
        given the json formatted dumpruninfo file contents, and
        a property and value that should be added to a given job,
        add it
        """
        if "jobs" not in dumpruninfo:
            dumpruninfo["jobs"] = {}
        if jobname not in dumpruninfo["jobs"]:
            dumpruninfo["jobs"][jobname] = {}
        dumpruninfo["jobs"][jobname][jproperty] = value

    @staticmethod
    def get_empty_json():
        return {"jobs": {}}

    @staticmethod
    def get_jobs(dumpruninfo):
        """
        given the json formatted dumpruninfo file contents,
        return the jobnames covered by it
        """
        if "jobs" not in dumpruninfo:
            return []
        else:
            return dumpruninfo["jobs"].keys()

    def __init__(self, wiki, enabled, verbose=False):
        super(RunInfo, self).__init__()
        self.wiki = wiki
        self._enabled = enabled
        self.verbose = verbose

    def save_dump_runinfo(self, content):
        """Write out a simple text file with the status for this wiki's dump."""
        if RunInfo.NAME in self._enabled:
            try:
                self._write_dump_runinfo(content)
            except Exception as ex:
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
            input_fhandle = open(dump_runinfo_filename, "r")
            for line in input_fhandle:
                results.append(self._get_old_runinfo_from_line(line))
            input_fhandle.close()
            return results
        except Exception as ex:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return False

    def get_all_output_files(self):
        """
        return list of all runinfo files in all formats
        """
        files = []
        for fmt in RunInfo.FORMATS:
            files.append(RunInfo.get_runinfo_basename() + "." + fmt)
        return files

    #
    # functions internal to the class
    #
    def _get_dump_runinfo_filename(self, date=None, fmt='txt'):
        # sometimes need to get this info for an older run to check status of a file for
        # possible prefetch
        if fmt == 'json':
            ext = "json"
        else:
            ext = "txt"
        if date:
            return os.path.join(self.wiki.public_dir(), date,
                                RunInfo.get_runinfo_basename() + "." + ext)
        else:
            return os.path.join(self.wiki.public_dir(), self.wiki.date,
                                RunInfo.get_runinfo_basename() + "." + ext)

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

    def _write_dump_runinfo(self, content):
        for fmt in RunInfo.FORMATS:
            dump_runinfo_filename = self._get_dump_runinfo_filename(fmt=fmt)
            #  FileUtils.write_file(directory, dumpRunInfoFilename, text,
            #    self.wiki.config.fileperms)
            FileUtils.write_file_in_place(dump_runinfo_filename, content[fmt],
                                          self.wiki.config.fileperms)

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
            input_fhandle = open(filename, "r")
            for line in input_fhandle:
                result = self._get_status_from_runinfo_line(line, job_name)
                if result is not None:
                    return result
            input_fhandle.close()
            return None
        except Exception as ex:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None

    # find desc in there, look for "class='done'"
    def _get_status_from_html_line(self, line, desc):
        if ">" + desc + "<" not in line:
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
            input_fhandle = open(filename, "r")
            for line in input_fhandle:
                result = self._get_status_from_html_line(line, desc)
                if result is not None:
                    return result
            input_fhandle.close()
            return None
        except Exception as ex:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None
