#!/usr/bin/python3
"""
Misc utils and classes needed primarily for dump Runner class
"""
import os
import sys
import traceback
from email.mime import text as MIMEText
import smtplib
import json

from dumps.exceptions import BackupError
from dumps.checksummers import Checksummer
from dumps.fileutils import DumpFilename, FileUtils
from dumps.symlinks import SymLinks, Feeds
from dumps.utils import TimeUtils
from dumps.specialfilesregistry import Registered


class Maintenance():
    """
    setting and notifying about maintenance mode
    via existence of a file in current working directory
    (really? maybe it should be somewhere else, ugh)
    """

    @staticmethod
    def in_maintenance_mode():
        """Use this to let callers know that we really should not
        be running.  Callers should try to exit the job
        they are running as soon as possible."""
        return os.path.exists("maintenance.txt")

    @staticmethod
    def exit_if_in_maintenance_mode(message=None):
        """Call this from possible exit points of running jobs
        in order to exit if we need to"""
        if Maintenance.in_maintenance_mode():
            if message:
                raise BackupError(message)
            raise BackupError("In maintenance mode, exiting.")


class FailureHandler():
    '''
    do the right thing on notification of a failure for a dump step
    '''
    def __init__(self, wiki, email):
        self.wiki = wiki
        # number of times jobs have failed on this run
        self.failure_count = 0
        self.email = email

    def report_failure(self):
        """
        send email reporting a failure for the given wiki and date
        """
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
        """Send out a quickie email given the subject line and the email text"""
        message = MIMEText.MIMEText(body)
        message["Subject"] = subject
        message["From"] = self.wiki.config.mail_from
        message["To"] = self.wiki.config.admin_mail
        try:
            server = smtplib.SMTP(self.wiki.config.smtp_server)
            server.sendmail(self.wiki.config.mail_from, self.wiki.config.admin_mail,
                            message.as_string())
            server.close()
        except Exception:
            print("MAIL SEND FAILED! GODDAMIT! Was sending this mail:")
            print(message)


class Notice():
    """
    management of notice file, the contents of which will be inserted in
    dump run index.html file for the given wiki, if it exists
    """
    NAME = "notice"

    def __init__(self, wiki, notice, enabled):
        self.wiki = wiki
        self.notice = notice
        self._enabled = enabled
        self.write_notice()

    def write_notice(self):
        '''
        write notice file if self.notice has contents,
        or remove if it self.notice is false,
        or read existing file and stash contents, if self.notice is empty str
        '''
        if Notice.NAME in self._enabled:
            notice_filepath = self._get_notice_filename()
            # delnotice.  toss any existing file
            if self.notice is False:
                if os.path.exists(notice_filepath):
                    os.remove(notice_filepath)
                self.notice = ""
            # addnotice, stuff notice in a file for other jobs etc
            elif self.notice != "":
                FileUtils.write_file(
                    FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
                    notice_filepath, self.notice,
                    self.wiki.config.fileperms)
            # default case. if there is a file get the contents, otherwise
            # we have empty contents, all good
            else:
                if os.path.exists(notice_filepath):
                    self.notice = FileUtils.read_file(notice_filepath)

    def refresh_notice(self):
        '''
        if the notice file has changed or gone away, we comply.
        '''
        notice_filepath = self._get_notice_filename()
        if os.path.exists(notice_filepath):
            self.notice = FileUtils.read_file(notice_filepath)
        else:
            self.notice = ""

    #
    # functions internal to class
    #
    def _get_notice_filename(self):
        '''
        return the full path to the notice filename
        '''
        return os.path.join(self.wiki.public_dir(), self.wiki.date, "notice.txt")


class RunSettings():
    """
    management of cache file containing certain config settings for
    the dump run for the specific wiki, so that if the config file
    is changed midway through the dump, nothing will break
    """
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
        """
        return the settings from wiki config that will be stashed
        in run settings cache file
        """
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
        if not contents:
            return None
        if contents[-1] == '\n':
            contents = contents[:-1]
        try:
            return json.loads(contents)
        except json.JSONDecodeError:
            return None

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


class DumpRunJobData():
    """
    management of all metadata around a dump run for the specified wiki
    this includes info about the jobs run (dumpruninfo files),
    setup of checksum files, rss feeds, 'latest' symlinks, etc.
    """
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
        """
        tasks that should be done before any dump jobs start
        """
        self.checksummer.prepare_checksums()

    def do_after_dump(self, dump_items):
        """
        tasks that should be done after all dump jobs have completed
        """
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
        """
        tasks that should be done once per job before each
        dump job runs
        args:
            Dump, list of Dump instances (these correspond to jobs)
        """
        self.runinfo.save_dump_runinfo(
            RunInfo.report_dump_runinfo(dump_items))

    def do_after_job(self, item, dump_items):
        """
        tasks that should be done once per job after each
        dump job runs
        args:
            Dump, list of Dump instances (these correspond to jobs)
        """
        self.checksummer.cp_chksum_tmpfiles_to_permfile()
        # this will include checkpoint files if they are enabled.
        for dfname in item.oflister.list_outfiles_to_publish(item.oflister.makeargs(self.dump_dir)):
            if os.path.exists(self.dump_dir.filename_public_path(dfname)):
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
        """
        clean up stuff from old runs in the 'latest' directory, which
        would have symlinks to dump content files, and rss feed files
        for these same symlinks
        """
        self.symlinks.remove_symlinks_from_old_runs(self.wiki.date)
        self.feeds.cleanup_feeds()


class RunInfo(Registered):
    """
    management of files containing minimal run info about each dump job
    for a given wiki (job name, status, when entry last updated)
    """
    NAME = "runinfo"
    FORMATS = ['txt', 'json']

    @staticmethod
    def get_runinfo_basename():
        """
        return base filename where dump run info is stashed
        """
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
        for item in sorted(dump_items, reverse=True, key=lambda job: job.name()):
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
        """
        return dict suitable for conversion to json file of
        dump run info with no jobs in it
        """
        return {"jobs": {}}

    @staticmethod
    def get_jobs(dumpruninfo):
        """
        given the json formatted dumpruninfo file contents,
        return the jobnames covered by it
        """
        if "jobs" not in dumpruninfo:
            return []
        return dumpruninfo["jobs"].keys()

    def __init__(self, wiki, enabled, verbose=False):
        super().__init__()
        self.wiki = wiki
        self._enabled = enabled
        self.verbose = verbose

    def save_dump_runinfo(self, content):
        """Write out a simple text file with the status for this wiki's dump."""
        if RunInfo.NAME in self._enabled:
            try:
                self._write_dump_runinfo(content)
            except Exception:
                if self.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    sys.stderr.write(repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback)))
                sys.stderr.write("Couldn't save dump run info file. Continuing anyways\n")

    def status_of_old_dump_is_done(self, runner, date, job_name, job_desc):
        """
        return 1 if the dump of the specified date is 'done',
        0 otherwise.
        this tries to use the dumpruninfo file, but if none is available
        it will fall back to trying to dig crap out of the index.html
        file for that run.
        """
        old_dump_runinfo_filename = self._get_dump_runinfo_filename(date)
        status = self._get_status_from_runinfo(old_dump_runinfo_filename, job_name)
        if status == "done":
            return 1
        if status is not None:
            # failure, in progress, some other useless thing
            return 0

        # ok, there was no info there to be had, try the index file. yuck.
        index_filename = os.path.join(runner.wiki.public_dir(),
                                      date, runner.wiki.config.perdump_index)
        status = self._get_status_from_html(index_filename, job_desc)
        if status == "done":
            return 1
        return 0

    def get_old_runinfo_from_file(self):
        """
        read the dump run info file in, if there is one, and get info about which dumps
        have already been run and whether they were successful
        """
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
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return False

    @staticmethod
    def get_all_output_files():
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
        return os.path.join(self.wiki.public_dir(), self.wiki.date,
                            RunInfo.get_runinfo_basename() + "." + ext)

    def _get_dump_runinfo_dirname(self, date=None):
        if date:
            return os.path.join(self.wiki.public_dir(), date)
        return os.path.join(self.wiki.public_dir(), self.wiki.date)

    @staticmethod
    def _get_old_runinfo_from_line(line):
        # format: name:%; updated:%; status:%
        # get rid of leading/trailing/blanks
        line = line.strip(" ")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        dump_runinfo = {}
        for field in fields:
            field = field.strip(" ")
            (fieldname, _sep, field_value) = field.partition(':')
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

    @staticmethod
    def _get_status_from_runinfo_line(line, job_name):
        # format: name:%; updated:%; status:%
        # get rid of leading/trailing/embedded blanks
        line = line.replace(" ", "")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        for field in fields:
            (fieldname, _sep, field_value) = field.partition(':')
            if fieldname == "name":
                if not field_value == job_name:
                    return None
            elif fieldname == "status":
                return field_value
        return None

    def _get_status_from_runinfo(self, filename, job_name=""):
        # read the dump run info file in, if there is one, and find out whether
        # a particular job (one step only, not a multiple piece job) has been
        # already run and whether it was successful (use to examine status
        # of step from some previous run)
        try:
            with open(filename, "r") as input_fhandle:
                for line in input_fhandle:
                    result = self._get_status_from_runinfo_line(line, job_name)
                    if result is not None:
                        return result
                input_fhandle.close()
                return None
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None

    @staticmethod
    def _get_status_from_html_line(line, desc):
        # find desc in there, look for "class='done'"
        if ">" + desc + "<" not in line:
            return None
        if "<li class='done'>" in line:
            return "done"
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
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            return None
