'''
base class dump job is defined here
'''

import os
from os.path import exists
import sys
import traceback

from dumps.exceptions import BackupError, BackupPrereqError
from dumps.fileutils import DumpContents, DumpFilename
from dumps.utils import TimeUtils


def get_truncated_empty_checkpt_files(dump_dir, dump_names, file_type, file_ext,
                                      date=None, parts=None):
    '''
    return all checkpoint files that exist
    returns:
        list of DumpFilename
    '''
    dfnames = []
    for dump_name in dump_names:
        dfnames.extend(dump_dir.get_checkpt_files(
            date, dump_name, file_type, file_ext, parts, temp=False))
    return dfnames


def get_checkpt_files(dump_dir, dump_names, file_type, file_ext, date=None,
                      parts=None):
    '''
    return all checkpoint files that exist
    returns:
        list of DumpFilename
    '''
    dfnames = []
    for dump_name in dump_names:
        dfnames.extend(dump_dir.get_checkpt_files(
            date, dump_name, file_type, file_ext, parts, temp=False))
    return dfnames


def get_reg_files(dump_dir, dump_names, file_type, file_ext, date=None, parts=None):
    '''
    get all regular output files that exist
    returns:
        list of DumpFilename
    '''
    dfnames = []
    for dump_name in dump_names:
        dfnames.extend(dump_dir.get_reg_files(
            date, dump_name, file_type, file_ext, parts, temp=False))
    return dfnames


class Dump(object):
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
        if not hasattr(self, '_parts'):
            self._parts = False

    def get_output_dir(self, runner):
        if runner.wiki.is_private():
            return os.path.join(runner.wiki.private_dir(), runner.wiki.date)
        else:
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
            if runner.wiki.is_private():
                command_info['output_dir'] = os.path.join(runner.wiki.private_dir(),
                                                          runner.wiki.date)
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
        else:
            return None

    def status(self):
        if "status" in self.runinfo:
            return self.runinfo["status"]
        else:
            return None

    def updated(self):
        if "updated" in self.runinfo:
            return self.runinfo["updated"]
        else:
            return None

    def to_run(self):
        if "to_run" in self.runinfo:
            return self.runinfo["to_run"]
        else:
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

    def move_if_truncated(self, runner, dfname):
        """
        check if the given file (DumpFile) is truncated or empty
        if so, move it out of the way and return True
        return False otherwise

        This function expects that the file to check for truncation lives in the public dir
        """
        if "check_trunc_files" not in runner.enabled or not self.check_truncation():
            return

        if runner.wiki.is_private():
            dcontents = DumpContents(runner.wiki, runner.dump_dir.filename_private_path(dfname))
        else:
            dcontents = DumpContents(runner.wiki, runner.dump_dir.filename_public_path(dfname))

        file_truncated = True
        if os.path.exists(dcontents.filename):
            if dcontents.check_if_empty():
                # file exists and is empty, move it out of the way
                dcontents.rename(dcontents.filename + ".empty")
            elif dcontents.check_if_truncated():
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
        return len(self.list_truncated_empty_outfiles(runner.dump_dir))

    def progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files."""
        # pass through...
        if line:
            if runner.log:
                runner.log.add_to_log_queue(line)
            sys.stderr.write(line)
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
        if not series.exited_successfully:
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
            dfnames = self.list_outfiles_for_cleanup(dump_dir)
            for dfname in dfnames:
                self.remove_output_file(dump_dir, dfname)

    def get_fileparts_list(self):
        if self._parts_enabled:
            if self._partnum_todo:
                return [self._partnum_todo]
            else:
                return range(1, len(self._parts) + 1)
        else:
            return False

    def list_reg_files(self, dump_dir, dump_names=None, date=None, parts=None):
        '''
        list all regular output files that exist
        returns:
            list of DumpFilename
        '''
        if not dump_names:
            dump_names = [self.dumpname]
        return get_reg_files(dump_dir, dump_names, self.file_type,
                             self.file_ext, date, parts)

    def list_checkpt_files(self, dump_dir, dump_names=None, date=None, parts=None):
        '''
        list all checkpoint files that exist
        returns:
            list of DumpFilename
        '''
        if not dump_names:
            dump_names = [self.dumpname]
        return get_checkpt_files(dump_dir, dump_names, self.file_type,
                                 self.file_ext, date, parts)

    def list_truncated_empty_checkpt_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''
        list checkpoint files that have been produced for specified file part(s)
        that are either empty or truncated

        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_truncated_empty_checkpt_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False))
        return dfnames

    def list_checkpt_files_for_filepart(self, dump_dir, parts, dump_names=None, inprog=False):
        '''
        list checkpoint files that have been produced for specified file part(s)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_checkpt_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False, inprog=inprog))
        return dfnames

    def list_reg_files_for_filepart(self, dump_dir, parts, dump_names=None, inprog=False):
        '''
        list noncheckpoint files that have been produced for specified file part(s)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False, inprog=inprog))
        return dfnames

    def list_truncated_empty_reg_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''
        list noncheckpoint files that have been produced for specified file part(s)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_truncated_empty_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False))
        return dfnames

    def list_temp_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''
        list temp output files that have been produced for specified file part(s)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_checkpt_files(
                None, dname, self.file_type, self.file_ext, parts, temp=True))
            dfnames.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=True))
        return dfnames

    def _get_files_possible(self, dump_dir, date=None, dumpname=None,
                            file_type=None, file_ext=None, parts=None, temp=False,
                            suffix=None):
        '''
        internal function which all the public get_*_possible functions call
        list all files that could be created for the given dumpname, filtering by the given args.
        by definition, checkpoint files are never returned in such a list, as we don't
        know where a checkpoint might be taken (which pageId start/end).

        if we get None for an arg then we accept all values for that arg in the filename
        if we get False for an arg (parts, temp), we reject any filename
        which contains a value for that arg
        if we get True for an arg (temp), we accept only filenames which contain a value for the arg
        parts should be a list of value(s), or True / False / None
        returns:
            list of DumpFilename
        '''

        dfnames = []
        if dumpname is None:
            dumpname = self.dumpname
        if suffix is not None:
            # additional suffix tacked on after the normal file extension, if any
            # for example, truncated files might end in ".truncated"
            file_ext += suffix

        if parts is None or parts is False:
            dfnames.append(DumpFilename(dump_dir._wiki, date, dumpname,
                                        file_type, file_ext, None, None, temp))
        if parts is True or parts is None:
            parts = self.get_fileparts_list()
        if parts:
            for partnum in parts:
                dfnames.append(DumpFilename(dump_dir._wiki, date, dumpname,
                                            file_type, file_ext, partnum, None, temp))
        return dfnames

    def get_reg_files_for_filepart_possible(self, dump_dir, parts, dump_names=None):
        '''
        based on dump name, parts, etc. get all the
        output files we expect to generate for these parts
        returns:
            list of DumpFilename
        '''
        if not dump_names:
            dump_names = [self.dumpname]
        dfnames = []
        for dname in dump_names:
            dfnames.extend(self._get_files_possible(
                dump_dir, None, dname, self.file_type, self.file_ext, parts, temp=False))
        return dfnames

    def get_truncated_empty_reg_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''
        based on dump name, parts, etc. get all the
        output files we expect to generate for these parts
        returns:
            list of DumpFilename
        '''
        dfnames = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            dfnames.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False, suffix=".truncated"))
            dfnames.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False, suffix=".empty"))
        return dfnames

# these routines are all used for listing output files for various purposes...

    def list_outfiles_to_publish(self, dump_dir, dump_names=None):
        '''
        this is the complete list of files produced by a dump step.
        Includes: checkpoints, parts, complete files, temp files if they
        exist. At end of run temp files must be gone.
        even if only one file part (one subjob) is being rerun, this lists all output files,
        not just those for the one part.
        returns:
            list of DumpFilename
        '''

        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
            dfnames.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_truncated_empty_outfiles(self, dump_dir, dump_names=None):
        '''
        lists all files that have been found to be truncated or empty and renamed
        as such
        Includes: checkpoint files, file parts, whole files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.list_truncated_empty_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.get_truncated_empty_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir, dump_names=None):
        '''
        lists all files that will be examined at the end of the run to be sure
        they were written out completely.
        Includes: checkpoint files, file parts, whole files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir, dump_names=None):
        '''
        called when the job command is generated.
        Includes: parts, whole files, temp files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        '''
        called before job run to cleanup old files left around from any previous run(s)
        Includes: checkpoints, parts, whole files, temp files if they exist.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []

        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        if self._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
            dfnames.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, dump_dir, dump_names=None):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        returns only truncated or empty files
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self._checkpoints_enabled:
            dfnames.extend(self.list_truncated_empty_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.list_truncated_empty_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames
