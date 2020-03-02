#!/usr/bin/python3
'''
base class dump job is defined here
'''

import os
from os.path import exists
import sys
import traceback

from dumps.exceptions import BackupError, BackupPrereqError
from dumps.fileutils import DumpContents, DumpFilename, FileUtils
from dumps.utils import TimeUtils
from dumps.filelister import JobFileLister


class Dump():
    def __init__(self, name, desc, verbose=False):
        self._desc = desc
        self.verbose = verbose
        self.progress = ""
        self.runinfo = {"name": name, "status": "waiting", "updated": ""}
        self.dumpname = self.get_dumpname()
        self.file_type = self.get_filetype()
        self.file_ext = self.get_file_ext()
        self.commands_submitted = []
        # if var hasn't been defined by a derived class already.  (We get
        # called last by child classes in their constructor, so that
        # their functions overriding things like the dumpbName can
        # be set up before we use them to set class attributes.)
        if not hasattr(self, 'onlyparts'):
            self.onlyparts = False
        if not hasattr(self, '_parts_enabled'):
            self._parts_enabled = False
        if not hasattr(self, '_checkpoints_enabled'):
            self._checkpoints_enabled = False
        if not hasattr(self, 'checkpoint_file'):
            self.checkpoint_file = None
        if not hasattr(self, '_partnum_todo'):
            self._partnum_todo = None
        if not hasattr(self, '_prerequisite_items'):
            self._prerequisite_items = []
        if not hasattr(self, '_pages_per_part'):
            self._pages_per_part = None
        self.flister = JobFileLister(self.dumpname, self.file_type, self.file_ext,
                                     self.get_fileparts_list(), self.checkpoint_file)

    def get_output_dir(self, runner):
        return os.path.join(runner.wiki.public_dir(), runner.wiki.date)

    def setup_command_info(self, runner, command_series, output_dfnames, output_dir=None):
        command_info = {}
        command_info['runner'] = runner
        command_info['series'] = command_series
        command_info['output_files'] = [dfname.filename + DumpFilename.INPROG
                                        for dfname in output_dfnames if dfname is not None]
        if output_dir is not None:
            command_info['output_dir'] = output_dir
        else:
            command_info['output_dir'] = os.path.join(runner.wiki.public_dir(),
                                                      runner.wiki.date)
        self.commands_submitted.append(command_info)

    def check_truncation(self):
        # Automatic checking for truncation of produced files is
        # (due to dump_dir handling) only possible for public dir
        # right now. So only set this to True, when all files of
        # the item end in the public dir.
        return False

    def name(self):
        if "name" in self.runinfo:
            return self.runinfo["name"]
        return None

    def status(self):
        if "status" in self.runinfo:
            return self.runinfo["status"]
        return None

    def updated(self):
        if "updated" in self.runinfo:
            return self.runinfo["updated"]
        return None

    def to_run(self):
        if "to_run" in self.runinfo:
            return self.runinfo["to_run"]
        return None

    def set_name(self, name):
        self.runinfo["name"] = name

    def set_to_run(self, to_run):
        self.runinfo["to_run"] = to_run

    def set_skipped(self):
        self.set_status("skipped")
        self.set_to_run(False)

    # sometimes this will be called to fill in data from an old
    # dump run; in those cases we don't want to clobber the timestamp
    # with the current time.
    def set_status(self, status, set_updated=True):
        self.runinfo["status"] = status
        if set_updated:
            self.runinfo["updated"] = TimeUtils.pretty_time()

    def set_updated(self, updated):
        self.runinfo["updated"] = updated

    def description(self):
        return self._desc

    def detail(self):
        """Optionally return additional text to appear under the heading."""
        return None

    def get_dumpname(self):
        """Return the dumpname as it appears in output files for this phase of the dump
        e.g. pages-meta-history, all-titles-in-ns0, etc"""
        return ""

    def list_dumpnames(self):
        """Returns a list of names as they appear in output files for this phase of the dump
        e.g. [pages-meta-history], or [stub-meta-history, stub-meta-current, stub-articles], etc"""
        return [self.get_dumpname()]

    def get_file_ext(self):
        """Return the extension of output files for this phase of the dump
        e.g. bz2 7z etc"""
        return ""

    def get_filetype(self):
        """Return the type of output files for this phase of the dump
        e.g. sql xml etc"""
        return ""

    def start(self):
        """Set the 'in progress' flag so we can output status."""
        self.set_status("in-progress")

    def dump(self, runner):
        """Attempt to run the operation, updating progress/status info."""
        try:
            for prerequisite_item in self._prerequisite_items:
                if prerequisite_item.status() == "failed":
                    raise BackupError("Required job %s failed, not starting job %s" %
                                      (prerequisite_item.name(), self.name()))
                elif prerequisite_item.status() != "done":
                    raise BackupPrereqError("Required job "
                                            "%s not marked as done, not starting job %s" %
                                            (prerequisite_item.name(), self.name()))

            self.run(runner)
            self.post_run(runner)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if self.verbose:
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))
            if exc_type.__name__ == 'BackupPrereqError':
                self.set_status("waiting")
            else:
                self.set_status("failed")
            raise

        self.set_status("done")

    def run(self, runner):
        """Actually do something!"""
        pass

    def post_run(self, runner):
        """Common tasks to run after performing this item's actual dump"""
        # Checking for truncated files
        truncated_files_count = self.check_for_truncated_files(runner)
        if truncated_files_count:
            raise BackupError("Encountered %d truncated files for %s" %
                              (truncated_files_count, self.dumpname))

    def is_larger(self, dfname, numpages):
        """
        given a DumpFilename and a number of pages cutoff, return True
        if the file covers more pages than the cutoff, False otherwise

        page coverage is determined from the filename (first and last
        page id embedded); if one or both of those are missing, False
        will always be returned.
        """
        if dfname.first_page_id is None or dfname.last_page_id is None:
            return False
        return numpages > int(dfname.last_page_id) - int(dfname.first_page_id)

    def move_if_truncated(self, runner, dfname, emptycheck=0, tmpdir=False):
        """
        check if the given file (DumpFile) is truncated or empty
        if so, move it out of the way and return True
        return False otherwise

        if emptycheck is set to a number, the file will only be checked to
        seee if it is empty, if the file covers a page range with more
        pages than the specific number. Eg a file named
        elwikivoyage-20180618-pages-meta-history2.xml-p140p150.bz2
        would be checked for emptycheck = 8 but not for 12; files that
        don't have page start and end numbers in the filename would not
        be checked at all.

        if emptycheck is left as 0, the file will be checked to see if
        it is empty always.

        if file is located in the temp dir, set tmpdir=True for it to
        be found there; otherwise the public xml/sql dump output dir
        (or private, if the wiki is private), will be checked for the file.
        """
        if "check_trunc_files" not in runner.enabled or not self.check_truncation():
            return False

        if tmpdir:
            path = os.path.join(
                FileUtils.wiki_tempdir(runner.wiki.db_name, runner.wiki.config.temp_dir),
                dfname.filename)
        elif runner.wiki.is_private():
            path = runner.dump_dir.filename_private_path(dfname)
        else:
            path = runner.dump_dir.filename_public_path(dfname)
        dcontents = DumpContents(runner.wiki, path)

        file_truncated = True
        if os.path.exists(dcontents.filename):
            # for some file types we will check that the file has the right closing tag
            last_tag = None
            if ('.xml' in dcontents.filename and
                    ('.bz2' in dcontents.filename or '.gz' in dcontents.filename)):
                last_tag = b'</mediawiki>'

            # fixme hardcoded at 200? mmmm. but otoh configurable is kinda dumb
            if (not emptycheck or self.is_larger(dfname, 200)) and dcontents.check_if_empty():
                # file exists and is empty, move it out of the way
                dcontents.rename(dcontents.filename + ".empty")
            elif dcontents.check_if_truncated(last_tag):
                # The file exists and is truncated, move it out of the way
                dcontents.rename(dcontents.filename + ".truncated")
            elif dcontents.check_if_binary_crap():
                # The file exists and has binary junk in it, move it out of the way
                dcontents.rename(dcontents.filename + ".truncated")
            else:
                # The file exists and is not truncated and doesn't have random crap.
                # Heck, it's a good file!
                file_truncated = False
        else:
            # file doesn't exist, move on
            file_truncated = False
        return file_truncated

    def check_for_truncated_files(self, runner):
        """
        Returns the number of files that have been detected to be truncated
        or empty.
        This function expects that "move_if_truncated" has been called on
        all output files first.
        """
        ret = 0

        if "check_trunc_files" not in runner.enabled or not self.check_truncation():
            return ret
        return len(self.list_truncated_empty_outfiles(
            self.flister.makeargs(runner.dump_dir)))

    def progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files."""
        # pass through...
        if line:
            if runner.log:
                runner.log.add_to_log_queue(line.decode('utf-8'))
            sys.stderr.write(line.decode('utf-8'))
        self.progress = line.strip()
        runner.report.update_index_html_and_json()
        runner.statushtml.update_status_file()
        runner.dumpjobdata.runinfo.save_dump_runinfo(
            runner.dumpjobdata.runinfo.report_dump_runinfo(runner.dump_item_list.dump_items))

    def time_to_wait(self):
        # we use wait this many secs for a command to complete that
        # doesn't produce output
        return 5

    def wait_alarm_handler(self, signum, frame):
        pass

    def command_completion_callback(self, series):
        """
        if the series of commands ran successfully to completion,
        mv produced output files from temporary to permanent
        names

        we write the data into temporary locations initially so that
        as each command series completes, the output files can
        be made available as done immediately, rather than waiting
        for all the parallel processes of a dump step to complete
        first.

        args: CommandSeries for which all commands have
              completed
        """
        if not series.exited_successfully():
            return

        for commands in self.commands_submitted:
            if commands['series'] == series._command_series:
                if not commands['output_files']:
                    return
                for inprogress_filename in commands['output_files']:
                    if not inprogress_filename.endswith(DumpFilename.INPROG):
                        continue
                    final_dfname = DumpFilename(commands['runner'].wiki)
                    final_dfname.new_from_filename(
                        inprogress_filename[:-1 * len(DumpFilename.INPROG)])

                    in_progress_path = os.path.join(commands['output_dir'], inprogress_filename)
                    final_path = os.path.join(commands['output_dir'], final_dfname.filename)
                    try:
                        os.rename(in_progress_path, final_path)
                    except Exception:
                        if self.verbose:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            sys.stderr.write(repr(
                                traceback.format_exception(exc_type, exc_value, exc_traceback)))
                        continue
                    # sanity check of file contents, move if bad
                    self.move_if_truncated(commands['runner'], final_dfname)

    def remove_output_file(self, dump_dir, dfname):
        """
        remove the output file and any temporary file related to it,
        either in the public dir or the private one, depending on
        where it is
        """
        if exists(dump_dir.filename_public_path(dfname)):
            os.remove(dump_dir.filename_public_path(dfname))
        elif exists(dump_dir.filename_private_path(dfname)):
            os.remove(dump_dir.filename_private_path(dfname))
        if exists(dump_dir.filename_public_path(dfname) + DumpFilename.INPROG):
            os.remove(dump_dir.filename_public_path(dfname) + DumpFilename.INPROG)
        elif exists(dump_dir.filename_private_path(dfname) + DumpFilename.INPROG):
            os.remove(dump_dir.filename_private_path(dfname) + DumpFilename.INPROG)

    def cleanup_old_files(self, dump_dir, runner):
        if "cleanup_old_files" in runner.enabled:
            if self.checkpoint_file is not None:
                # we only rerun this one, so just remove this one
                if exists(dump_dir.filename_public_path(self.checkpoint_file)):
                    os.remove(dump_dir.filename_public_path(self.checkpoint_file))
                elif exists(dump_dir.filename_private_path(self.checkpoint_file)):
                    os.remove(dump_dir.filename_private_path(self.checkpoint_file))
            dfnames = self.list_outfiles_for_cleanup(self.flister.makeargs(dump_dir))
            for dfname in dfnames:
                self.remove_output_file(dump_dir, dfname)

    def cleanup_inprog_files(self, dump_dir, runner):
        if self.checkpoint_file is not None:
            # we only rerun this one, so just remove this one
            pub_path = DumpFilename.get_inprogress_name(
                dump_dir.filename_public_path(self.checkpoint_file))
            priv_path = DumpFilename.get_inprogress_name(
                dump_dir.filename_private_path(self.checkpoint_file))
            if os.path.exists(pub_path):
                if runner.dryrun:
                    print("would remove", pub_path)
                else:
                    os.remove(pub_path)
            elif os.path.exists(priv_path):
                if runner.dryrun:
                    print("would remove", priv_path)
                else:
                    os.remove(priv_path)

        dfnames = self.list_inprog_files_for_cleanup(self.flister.makeargs(dump_dir))
        if runner.dryrun:
            print("would remove ", [dfname.filename for dfname in dfnames])
        else:
            for dfname in dfnames:
                self.remove_output_file(dump_dir, dfname)

    def get_fileparts_list(self):
        if self._parts_enabled:
            if self._partnum_todo:
                return [self._partnum_todo]
            return range(1, len(self._pages_per_part) + 1)
        return None

# these routines are all used for listing output files for various purposes...

    def list_outfiles_to_publish(self, args):
        '''
        this is the complete list of files produced by a dump step.
        Includes: checkpoints, parts, complete files, temp files if they
        exist. At end of run temp files must be gone.
        even if only one file part (one subjob) is being rerun, this lists all output files,
        not just those for the one part.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''

        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args['parts'] = self.get_fileparts_list()
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_checkpt_files_for_filepart(args))
            dfnames.extend(self.flister.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.get_reg_files_for_filepart_possible(args))
        return dfnames

    def list_truncated_empty_outfiles(self, args):
        '''
        lists all files that have been found to be truncated or empty and renamed
        as such
        Includes: checkpoint files, file parts, whole files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        args['parts'] = self.get_fileparts_list()
        if self.checkpoint_file is not None:
            problems = self.flister.get_truncated_empty_reg_files_for_filepart(args)
            if self.checkpoint_file.filename in [problem.filename for problem in problems]:
                dfnames.append(self.checkpoint_file)
                return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_truncated_empty_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.get_truncated_empty_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_build_command(self, args):
        '''
        called when the job command is generated.
        Includes: parts, whole files, temp files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args['parts'] = self.get_fileparts_list()
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.get_reg_files_for_filepart_possible(args))
        return dfnames

    def list_outfiles_for_cleanup(self, args):
        '''
        called before job run to cleanup old files left around from any previous run(s)
        Includes: checkpoints, parts, whole files, temp files if they exist.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []

        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args['parts'] = self.get_fileparts_list()
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_checkpt_files_for_filepart(args))
            dfnames.extend(self.flister.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.list_reg_files_for_filepart(args))
        return dfnames

    def list_inprog_files_for_cleanup(self, args):
        """
        list output files 'in progress' generated from a dump step,
        presumably left lying around from an earlier failed attempt
        at the step.

        expects:
            dump_dir, dump_names=None
        returns: list of DumpFilename
        """
        self.flister.set_defaults(args, ['dump_names'])
        args['dump_names'] = self.list_dumpnames()
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args['parts'] = self.get_fileparts_list()
        args['inprog'] = True
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.list_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_input(self, args):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        args['parts'] = self.get_fileparts_list()
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.list_reg_files_for_filepart(args))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, args):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        returns only truncated or empty files
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.flister.set_defaults(args, ['dump_names'])
        if args['dump_names'] is None:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        args['parts'] = self.get_fileparts_list()
        if self._checkpoints_enabled:
            dfnames.extend(self.flister.list_truncated_empty_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.flister.list_truncated_empty_reg_files_for_filepart(args))
        return dfnames
