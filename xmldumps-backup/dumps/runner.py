"""
Classes and methods for managing the run of
dump jobs for a given wiki, and logging about
their progress and results
"""
import os
from os.path import exists
import sys
import shutil
import threading
import traceback
import Queue

from dumps.CommandManagement import CommandsInParallel
from dumps.exceptions import BackupError
from dumps.fileutils import DumpDir, DumpFilename

from dumps.checksummers import Checksummer
from dumps.report import Report, StatusHtml
from dumps.symlinks import SymLinks, Feeds
from dumps.runnerutils import RunSettings, Notice, FailureHandler
from dumps.runnerutils import Maintenance, RunInfo, DumpRunJobData

from dumps.utils import DbServerInfo, FilePartInfo, TimeUtils
from dumps.runstatusapi import StatusAPI
from dumps.specialfileinfo import SpecialFileInfo
from dumps.dumpitemlist import DumpItemList


class Logger(threading.Thread):
    """
    logging to a file for dump runs, with a queue
    for log entries so there's no clobbering
    """
    def __init__(self, log_filepath=None):
        threading.Thread.__init__(self)

        if log_filepath:
            self.log_fhandle = open(log_filepath, "a")
        else:
            self.log_fhandle = None
        self.queue = Queue.Queue()
        self.jobs_done = "JOBSDONE"

    def log_write(self, line=None):
        '''
        write entry to log file if there is one
        '''
        if self.log_fhandle is not None:
            self.log_fhandle.write(line)
            self.log_fhandle.flush()

    def log_close(self):
        '''
        close log file if there is one
        '''
        if self.log_fhandle is not None:
            self.log_fhandle.close()
            # return 1 if logging terminated, 0 otherwise

    def do_job_on_log_queue(self):
        '''
        grab an entry on logging queue and log it
        '''
        line = self.queue.get()
        if line == self.jobs_done:
            self.log_close()
            return 1
        else:
            self.log_write(line)
            return 0

    def add_to_log_queue(self, line=None):
        '''
        add entry to be logged, to logging queue
        '''
        if line:
            self.queue.put_nowait(line)

    def indicate_jobs_done(self):
        '''
        set in order to have logging thread clean up and exit
        '''
        self.queue.put_nowait(self.jobs_done)

    def run(self):
        '''
        process log queue jobs until done, if there is a queue
        '''
        if self.log_fhandle is None:
            return
        done = False
        while not done:
            done = self.do_job_on_log_queue()


class Runner(object):
    """
    running one or many dump jobs for a given wiki
    """
    def __init__(self, wiki, prefetch=True, prefetchdate=None, spawn=True,
                 job=None, skip_jobs=None,
                 restart=False, notice="", dryrun=False, enabled=None,
                 partnum_todo=None, checkpoint_file=None, page_id_range=None,
                 skipdone=False, cleanup=False, do_prereqs=False, verbose=False):
        self.wiki = wiki
        self.db_name = wiki.db_name
        self.prefetch = prefetch
        self.prefetchdate = prefetchdate
        self.spawn = spawn
        self.filepart_info = FilePartInfo(wiki, self.db_name, self.log_and_print)
        self.restart = restart
        self.html_notice_file = None
        self.log = None
        self.dryrun = dryrun
        self._partnum_todo = partnum_todo
        self.checkpoint_file = checkpoint_file
        self.page_id_range = page_id_range
        self.skipdone = skipdone
        self.verbose = verbose
        self.enabled = enabled
        self.cleanup_old_files = cleanup
        self.do_prereqs = do_prereqs

        if self.checkpoint_file is not None:
            dfname = DumpFilename(self.wiki)
            dfname.new_from_filename(checkpoint_file)
            # we should get file partnum if any
            if self._partnum_todo is None and dfname.partnum_int:
                self._partnum_todo = dfname.partnum_int
            elif (self._partnum_todo is not None and dfname.partnum_int and
                  self._partnum_todo != dfname.partnum_int):
                raise BackupError("specifed partnum to do does not match part number "
                                  "of checkpoint file %s to redo", self.checkpoint_file)
            self.checkpoint_file = dfname

        if self.enabled is None:
            self.enabled = {}
        for setting in [StatusHtml.NAME, Report.NAME, Checksummer.NAME,
                        RunInfo.NAME, SymLinks.NAME, RunSettings.NAME,
                        Feeds.NAME, Notice.NAME, StatusAPI.NAME,
                        SpecialFileInfo.NAME,
                        "makedir", "clean_old_dumps", "cleanup_old_files",
                        "check_trunc_files", "cleanup_tmp_files"]:
            self.enabled[setting] = True

        if not self.cleanup_old_files:
            if "cleanup_old_files" in self.enabled:
                del self.enabled["cleanup_old_files"]

        if self.dryrun or self._partnum_todo is not None or self.checkpoint_file is not None:
            for setting in [StatusHtml.NAME, Report.NAME, Checksummer.NAME,
                            StatusAPI.NAME, SpecialFileInfo.NAME,
                            RunInfo.NAME, SymLinks.NAME, RunSettings.NAME,
                            Feeds.NAME, Notice.NAME, "makedir", "clean_old_dumps"]:
                if setting in self.enabled:
                    del self.enabled[setting]

        if self.dryrun:
            for setting in ["check_trunc_files", "cleanup_tmp_files"]:
                if setting in self.enabled:
                    del self.enabled[setting]
            if "logging" in self.enabled:
                del self.enabled["logging"]

        self.job_requested = job

        if self.job_requested == "latestlinks":
            for setting in [StatusHtml.NAME, Report.NAME, RunInfo.NAME, StatusAPI.NAME,
                            SpecialFileInfo.NAME, "cleanup_old_files", "cleanup_tmp_files"]:
                if setting in self.enabled:
                    del self.enabled[setting]

        if self.job_requested == "createdirs":
            for setting in [SymLinks.NAME, Feeds.NAME, RunSettings.NAME, StatusAPI.NAME,
                            SpecialFileInfo.NAME]:
                if setting in self.enabled:
                    del self.enabled[setting]

        if self.job_requested == "latestlinks" or self.job_requested == "createdirs":
            for setting in [Checksummer.NAME, Notice.NAME, "makedir",
                            "clean_old_dumps", "check_trunc_files"]:
                if setting in self.enabled:
                    del self.enabled[setting]

        if self.job_requested == "noop":
            for setting in ["clean_old_dumps", "check_trunc_files"]:
                if setting in self.enabled:
                    del self.enabled[setting]

        self.skip_jobs = skip_jobs
        if skip_jobs is None:
            self.skip_jobs = []

        self.db_server_info = DbServerInfo(self.wiki, self.db_name, self.log_and_print)
        self.dump_dir = DumpDir(self.wiki, self.db_name)

        # these must come after the dumpdir setup so we know which directory we are in
        if "logging" in self.enabled and "makedir" in self.enabled:
            dfname = DumpFilename(self.wiki)
            dfname.new_from_filename(self.wiki.config.log_file)
            self.log_filepath = self.dump_dir.filename_private_path(dfname)
            self.make_dir(os.path.join(self.wiki.private_dir(), self.wiki.date))
            self.log = Logger(self.log_filepath)
            # thread should die horribly when main script dies. no exceptions.
            self.log.daemon = True
            self.log.start()

        self.dumpjobdata = DumpRunJobData(self.wiki, self.dump_dir, notice,
                                          self.log_and_print, self.debug, self.enabled,
                                          self.verbose)

        # some or all of these dump_items will be marked to run
        self.dump_item_list = DumpItemList(self.wiki, self.prefetch, self.prefetchdate,
                                           self.spawn,
                                           self._partnum_todo, self.checkpoint_file,
                                           self.job_requested, self.skip_jobs,
                                           self.filepart_info, self.page_id_range,
                                           self.dumpjobdata, self.dump_dir, self.verbose)
        # only send email failure notices for full runs
        if self.job_requested:
            email = False
        else:
            email = True
        self.failurehandler = FailureHandler(self.wiki, email)
        self.statushtml = StatusHtml(self.wiki, self.enabled, self.dump_dir,
                                     self.dump_item_list.dump_items,
                                     self.dumpjobdata, self.failurehandler,
                                     self.log_and_print, self.verbose)
        self.report = Report(self.wiki, self.enabled, self.dump_dir,
                             self.dump_item_list.dump_items,
                             self.dumpjobdata, self.failurehandler,
                             self.log_and_print, self.verbose)

        self.runstatus_updater = StatusAPI(self.wiki, self.enabled, "json",
                                           self.log_and_print, self.verbose)
        self.specialfiles_updater = SpecialFileInfo(self.wiki, self.enabled, "json",
                                                    self.log_and_print, self.verbose)

    def log_queue_reader(self, log):
        """
        if there is a log queue, do entries on its queue until done
        """
        if not log:
            return
        done = False
        while not done:
            done = log.do_job_on_log_queue()

    def log_and_print(self, message):
        """
        queue an entry for logging if there is a log queue, and
        display the same to stderr
        """
        if hasattr(self, 'log') and self.log and "logging" in self.enabled:
            self.log.add_to_log_queue("%s\n" % message)
        sys.stderr.write("%s\n" % message)

    def html_update_callback(self):
        """
        when a long dump content production job is running, this
        will get called at regular intervals to update various
        status files that could be provided for download to users
        so they can track the job's progress
        """
        self.report.update_index_html_and_json()
        self.statushtml.update_status_file()
        self.runstatus_updater.write_statusapi_file()
        self.specialfiles_updater.write_specialfilesinfo_file()

    def get_save_command_series(self, commands, outfilepath):
        """
        if a command pipeline is supposed to redirect output to a file
        via shell, add the redirection args to the command
        and return it as a command series with one pipeline in it
        """
        commands[-1].extend([">", outfilepath])
        return [commands]

    # returns 0, None on success, 1, commands on error
    def save_command(self, series, completion_callback=None):
        """For one pipeline of commands, redirect output to a given file."""
        if self.dryrun:
            self.pretty_print_commands([series])
            return 0
        else:
            return self.run_command([series], callback_timed=self.html_update_callback,
                                    callback_on_completion=completion_callback)

    def pretty_print_commands(self, command_series_list):
        """
        for a series of command pipelines, print each pipeline nicely
        to stdout
        """
        for series in command_series_list:
            for pipeline in series:
                command_strings = []
                for command in pipeline:
                    command_strings.append(" ".join(command))
                pipeline_string = " | ".join(command_strings)
                print "Command to run: ", pipeline_string

    # command series list: list of (commands plus args)
    # is one pipeline. list of pipelines = 1 series.
    # this function wants a list of series.
    # be a list (the command name and the various args)
    # If the shell option is true, all pipelines will be run under the shell.
    # callbackinterval: how often we will call callback_timed (in milliseconds),
    # defaults to every 5 secs
    def run_command(self, command_series_list, callback_stderr=None,
                    callback_stderr_arg=None, callback_timed=None,
                    callback_timed_arg=None, shell=False, callback_interval=5000,
                    callback_on_completion=None):
        """Nonzero return code from the shell from any command in any pipeline will cause
        this function to print an error message and return 1, indicating error.
        Returns 0 on success.
        If a callback function is passed, it will receive lines of
        output from the call.  If the callback function takes another argument (which will
        be passed before the line of output) must be specified by the arg paraemeter.
        If no callback is provided, and no output file is specified for a given
        pipe, the output will be written to stderr. (Do we want that?)
        This function spawns multiple series of pipelines  in parallel.

        """
        if self.dryrun:
            self.pretty_print_commands(command_series_list)
            return 0, None

        else:
            commands = CommandsInParallel(command_series_list, callback_stderr=callback_stderr,
                                          callback_stderr_arg=callback_stderr_arg,
                                          callback_timed=callback_timed,
                                          callback_timed_arg=callback_timed_arg,
                                          shell=shell, callback_interval=callback_interval,
                                          callback_on_completion=callback_on_completion)
            commands.run_commands()
            if commands.exited_successfully():
                return 0, None
            else:
                problem_commands = commands.commands_with_errors()
                error_string = "Error from command(s): "
                for cmd in problem_commands:
                    error_string = error_string + "%s " % cmd
                self.log_and_print(error_string)
                return 1, commands.commands_with_errors(stringfmt=False)

    def debug(self, stuff):
        """
        display a debugging message with wiki name and time,
        log it also if logging is enabled
        """
        self.log_and_print("%s: %s %s" % (TimeUtils.pretty_time(), self.db_name, stuff))

    def run_handle_failure(self):
        """
        if a dump job failed, add to the failure count of jobs
        for the run for this wiki, and send mail if mail is enabled
        and it's the first failure of this run of the script
        """
        if self.failurehandler.failure_count < 1:
            # Email the site administrator just once per database
            self.failurehandler.report_failure()
        self.failurehandler.failure_count += 1

    def do_run_item(self, item):
        """
        run the specified dump job (item) if it is marked to be run
        """
        prereq_job = None

        Maintenance.exit_if_in_maintenance_mode(
            "In maintenance mode, exiting dump of %s at step %s"
            % (self.db_name, item.name()))
        if item.to_run():
            item.start()
            self.report.update_index_html_and_json()
            self.statushtml.update_status_file()
            self.runstatus_updater.write_statusapi_file()
            self.specialfiles_updater.write_specialfilesinfo_file()

            self.dumpjobdata.do_before_job(self.dump_item_list.dump_items)

            try:
                item.dump(self)
            except Exception as ex:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if self.verbose:
                    sys.stderr.write(repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback)))
                if (exc_type.__name__ == 'BackupPrereqError' or
                        exc_type.__name__ == 'BackupError'):
                    error_message = str(ex)
                    if error_message.startswith("Required job "):
                        prereq_job = error_message.split(" ")[2]
                        self.debug(error_message)
                if prereq_job is None:
                    # exception that doesn't have to do with missing prereqs.
                    self.debug("*** exception! " + str(ex))
                    self.debug(repr(traceback.format_exception(
                        exc_type, exc_value, exc_traceback)))
                    item.set_status("failed")

        if item.status() == "done":
            self.dumpjobdata.do_after_job(item, self.dump_item_list.dump_items)
        elif item.status() == "waiting" or item.status() == "skipped":
            # don't update the checksum files for this item.
            pass
        else:
            # Here for example status is "failed". But maybe also
            # "in-progress", if an item chooses to override dump(...) and
            # forgets to set the status. This is a failure as well.
            self.run_handle_failure()
        return prereq_job

    def run(self):
        """
        mark which dump jobs should run
        clean up old dump run files
        set up directories for the run
        run each dump job
        """
        if self.job_requested:
            if not self.dump_item_list.old_runinfo_retrieved and self.wiki.exists_perdump_index():

                # There was a previous run of all or part of this date, but...
                # There was no old RunInfo to be had (or an error was encountered getting it)
                # so we can't rerun a step and keep all the status information
                # about the old run around.
                # In this case ask the user if they reeeaaally want to go ahead
                print "No information about the previous run for this date could be retrieved."
                print "This means that the status information about the old run will be lost, and"
                print "only the information about the current (and future) runs will be kept."
                reply = raw_input("Continue anyways? [y/N]: ")
                if reply not in ["y", "Y"]:
                    raise RuntimeError("No run information available for previous dump, exiting")

            if not self.dump_item_list.mark_dumps_to_run(self.job_requested, self.skipdone):
                # probably no such job
                sys.stderr.write("No job marked to run, exiting")
                return None
            if self.restart:
                # mark all the following jobs to run as well
                self.dump_item_list.mark_following_jobs_to_run(self.skipdone)
        else:
            self.dump_item_list.mark_all_jobs_to_run(self.skipdone)

        Maintenance.exit_if_in_maintenance_mode(
            "In maintenance mode, exiting dump of %s" % self.db_name)

        self.make_dir(os.path.join(self.wiki.public_dir(), self.wiki.date))
        self.make_dir(os.path.join(self.wiki.private_dir(), self.wiki.date))

        self.show_runner_state("Cleaning up old dumps for %s" % self.db_name)
        self.clean_old_dumps()
        self.clean_old_dumps(private=True)

        # Informing what kind backup work we are about to do
        if self.job_requested:
            if self.restart:
                self.log_and_print("Preparing for restart from job %s of %s"
                                   % (self.job_requested, self.db_name))
            else:
                self.log_and_print("Preparing for job %s of %s" %
                                   (self.job_requested, self.db_name))
        else:
            self.show_runner_state("Starting backup of %s" % self.db_name)

        self.dumpjobdata.do_before_dump()

        for item in self.dump_item_list.dump_items:
            prereq_job = self.do_run_item(item)
            if self.do_prereqs and prereq_job is not None:
                doing = []
                doing.append(item)
                # we have the lock so we might as well run the prereq job now.
                # there may be a string of prereqs not met,
                # i.e. articlesrecombine -> articles -> stubs
                # so we're willing to walk back up the list up to five items,
                # assume there's something really broken if it takes more than that
                while prereq_job is not None and len(doing) < 5:
                    new_item = self.dump_item_list.find_item_by_name(prereq_job)
                    new_item.set_to_run(True)
                    prereq_job = self.do_run_item(new_item)
                    if prereq_job is not None:
                        # this job has a dependency too, add to the todo stack
                        doing.insert(0, new_item)
                # back up the stack and do the dependents if stack isn't too long.
                if len(doing) < 5:
                    for item in doing:
                        self.do_run_item(item)

        # special case
        if self.job_requested == "createdirs":
            if not os.path.exists(os.path.join(self.wiki.public_dir(), self.wiki.date)):
                os.makedirs(os.path.join(self.wiki.public_dir(), self.wiki.date))
            if not os.path.exists(os.path.join(self.wiki.private_dir(), self.wiki.date)):
                os.makedirs(os.path.join(self.wiki.private_dir(), self.wiki.date))

        # we must do this here before the checksums are used for status reports below
        self.dumpjobdata.checksummer.move_chksumfiles_into_place()

        if self.dump_item_list.all_possible_jobs_done():
            # All jobs are either in status "done", "waiting", "failed", "skipped"
            self.report.update_index_html_and_json("done")
            self.statushtml.update_status_file("done")
            self.runstatus_updater.write_statusapi_file()
            self.specialfiles_updater.write_specialfilesinfo_file()
        else:
            # This may happen if we start a dump now and abort before all items are
            # done. Then some are left for example in state "waiting". When
            # afterwards running a specific job, all (but one) of the jobs
            # previously in "waiting" are still in status "waiting"
            self.report.update_index_html_and_json("partialdone")
            self.statushtml.update_status_file("partialdone")
            self.runstatus_updater.write_statusapi_file()
            self.specialfiles_updater.write_specialfilesinfo_file()

        self.dumpjobdata.do_after_dump(self.dump_item_list.dump_items)

        # special case
        if (self.job_requested and self.job_requested == "latestlinks" and
                self.dump_item_list.all_possible_jobs_done()):
            self.dumpjobdata.do_latest_job()

        # Informing about completion
        if self.job_requested:
            if self.restart:
                self.show_runner_state("Completed run restarting from job %s for %s"
                                       % (self.job_requested, self.db_name))
            else:
                self.show_runner_state("Completed job %s for %s"
                                       % (self.job_requested, self.db_name))
        else:
            self.show_runner_state_complete()

        # let caller know if this was a successful run
        if sum(1 for item in self.dump_item_list.dump_items if item.status() == "failed"):
            return False
        else:
            return True

    def clean_old_dumps(self, private=False):
        """Removes all but the wiki.config.keep last dumps of this wiki.
        If there is already a directory for todays dump, this is omitted in counting
        and not removed."""
        if "clean_old_dumps" in self.enabled:
            if private:
                old = self.wiki.dump_dirs(private=True)
                dumptype = 'private'
            else:
                old = self.wiki.dump_dirs()
                dumptype = 'public'
            if old:
                if old[-1] == self.wiki.date:
                    # If we're re-running today's (or jobs from a given day's) dump, don't count
                    # it as one of the old dumps to keep... or delete it halfway through!
                    old = old[:-1]
                if self.wiki.config.keep > 0:
                    # Keep the last few
                    old = old[:-(self.wiki.config.keep)]
            if old:
                for dump in old:
                    self.show_runner_state("Purging old %s dump %s for %s" %
                                           (dumptype, dump, self.db_name))
                    if private:
                        base = os.path.join(self.wiki.private_dir(), dump)
                    else:
                        base = os.path.join(self.wiki.public_dir(), dump)
                    shutil.rmtree("%s" % base)
            else:
                self.show_runner_state("No old %s dumps to purge." % dumptype)

    def show_runner_state(self, message):
        """
        display to stdout and possibly log a message
        describing what is happening with the dump run
        """
        self.debug(message)

    def show_runner_state_complete(self):
        """
        display to stdout and possibly log a message
        that the dump run is complete
        """
        self.debug("SUCCESS: done.")

    def make_dir(self, dirname):
        """
        make a directory if it doesn't already exist,
        displaying and possibly logging a message
        about this
        """
        if "makedir" in self.enabled:
            if exists(dirname):
                self.debug("Checkdir dir %s ..." % dirname)
            else:
                self.debug("Creating %s ..." % dirname)
                os.makedirs(dirname)
