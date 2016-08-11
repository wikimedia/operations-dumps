'''
base class dump job is defined here
'''

import os
from os.path import exists
import sys
import signal
import traceback

from dumps.CommandManagement import CommandPipeline

from dumps.exceptions import BackupError, BackupPrereqError
from dumps.fileutils import DumpFile, DumpFilename
from dumps.utils import TimeUtils, MiscUtils


class Dump(object):
    def __init__(self, name, desc, verbose=False):
        self._desc = desc
        self.verbose = verbose
        self.progress = ""
        self.runinfo = {"name": name, "status": "waiting", "updated": ""}
        self.dumpname = self.get_dumpname()
        self.file_type = self.get_filetype()
        self.file_ext = self.get_file_ext()
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
        if not hasattr(self, '_check_truncation'):
            # Automatic checking for truncation of produced files is
            # (due to dump_dir handling) only possible for public dir
            # right now. So only set this to True, when all files of
            # the item end in the public dir.
            self._check_truncation = False
        if not hasattr(self, '_parts'):
            self._parts = False

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

    def check_for_truncated_files(self, runner):
        """Returns the number of files that have been detected to be truncated.
        This function expects that all files to check for truncation live in the public dir"""
        ret = 0

        if "check_trunc_files" not in runner.enabled or not self._check_truncation:
            return ret

        for dump_fname in self.list_outfiles_to_check_for_truncation(
                runner.dump_dir):
            dfile = DumpFile(runner.wiki, runner.dump_dir.filename_public_path(
                dump_fname), dump_fname)

            file_truncated = True
            if exists(dfile.filename):
                if dfile.check_if_truncated():
                    # The file exists and is truncated, we move it out of the way
                    dfile.rename(dfile.filename + ".truncated")

                    # We detected a failure and could abort right now. However,
                    # there might still be some further file parts, that are good.
                    # Hence, we go on treating the remaining files and in the end
                    # /all/ truncated files have been moved out of the way. So we
                    # see, which parts (instead of the whole job) need a rerun.
                else:
                    # The file exists and is not truncated. Heck, it's a good file!
                    file_truncated = False

            if file_truncated:
                ret += 1

        return ret

    def progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files."""
        # pass through...
        if line:
            if runner.log:
                runner.log.add_to_log_queue(line)
            sys.stderr.write(line)
        self.progress = line.strip()
        runner.indexhtml.update_index_html()
        runner.statushtml.update_status_file()
        runner.dumpjobdata.runinfofile.save_dump_runinfo_file(
            runner.dumpjobdata.runinfofile.report_dump_runinfo(runner.dump_item_list.dump_items))

    def time_to_wait(self):
        # we use wait this many secs for a command to complete that
        # doesn't produce output
        return 5

    def wait_alarm_handler(self, signum, frame):
        pass

    def build_recombine_command_string(self, runner, files, output_file, compression_command,
                                       uncompression_command, end_header_marker="</siteinfo>"):
        output_filename = runner.dump_dir.filename_public_path(output_file)
        partnum = 0
        recombines = []
        if not exists(runner.wiki.config.head):
            raise BackupError("head command %s not found" % runner.wiki.config.head)
        head = runner.wiki.config.head
        if not exists(runner.wiki.config.tail):
            raise BackupError("tail command %s not found" % runner.wiki.config.tail)
        tail = runner.wiki.config.tail
        if not exists(runner.wiki.config.grep):
            raise BackupError("grep command %s not found" % runner.wiki.config.grep)
        grep = runner.wiki.config.grep

        # we assume the result is always going to be run in a subshell.
        # much quicker than this script trying to read output
        # and pass it to a subprocess
        output_filename_esc = MiscUtils.shell_escape(output_filename)
        head_esc = MiscUtils.shell_escape(head)
        tail_esc = MiscUtils.shell_escape(tail)
        grep_esc = MiscUtils.shell_escape(grep)

        uncompression_command_esc = uncompression_command[:]
        for command in uncompression_command_esc:
            command = MiscUtils.shell_escape(command)
        for command in compression_command:
            command = MiscUtils.shell_escape(command)

        if not files:
            raise BackupError("No files for the recombine step found in %s." % self.name())

        for file_obj in files:
            # uh oh FIXME
            # f = MiscUtils.shell_escape(file_obj.filename)
            fpath = runner.dump_dir.filename_public_path(file_obj)
            partnum = partnum + 1
            pipeline = []
            uncompress_this_file = uncompression_command[:]
            uncompress_this_file.append(fpath)
            pipeline.append(uncompress_this_file)
            # warning: we figure any header (<siteinfo>...</siteinfo>)
            # is going to be less than 2000 lines!
            pipeline.append([head, "-2000"])
            pipeline.append([grep, "-n", end_header_marker])
            # without shell
            proc = CommandPipeline(pipeline, quiet=True)
            proc.run_pipeline_get_output()
            if ((proc.output()) and
                    (proc.exited_successfully() or
                     proc.get_failed_cmds_with_retcode() ==
                     [[-signal.SIGPIPE, uncompress_this_file]] or
                     proc.get_failed_cmds_with_retcode() ==
                     [[signal.SIGPIPE + 128, uncompress_this_file]])):
                (header_end_num, junk_unused) = proc.output().split(":", 1)
                # get header_end_num
            else:
                raise BackupError("Could not find 'end of header' marker for %s" % fpath)
            recombine = " ".join(uncompress_this_file)
            header_end_num = int(header_end_num) + 1
            if partnum == 1:
                # first file, put header and contents
                recombine = recombine + " | %s -n -1 " % head
            elif partnum == len(files):
                # last file, put footer
                recombine = recombine + (" | %s -n +%s" % (tail, header_end_num))
            else:
                # put contents only
                recombine = recombine + (" | %s -n +%s" % (tail, header_end_num))
                recombine = recombine + " | %s -n -1 " % head
            recombines.append(recombine)
        recombine_command_string = ("(" + ";".join(recombines) + ")" + "|" +
                                    "%s %s" % (compression_command, output_filename))
        return recombine_command_string

    def cleanup_old_files(self, dump_dir, runner):
        if "cleanup_old_files" in runner.enabled:
            if self.checkpoint_file is not None:
                # we only rerun this one, so just remove this one
                if exists(dump_dir.filename_public_path(self.checkpoint_file)):
                    os.remove(dump_dir.filename_public_path(self.checkpoint_file))
                elif exists(dump_dir.filename_private_path(self.checkpoint_file)):
                    os.remove(dump_dir.filename_private_path(self.checkpoint_file))
            files = self.list_outfiles_for_cleanup(dump_dir)
            for finfo in files:
                if exists(dump_dir.filename_public_path(finfo)):
                    os.remove(dump_dir.filename_public_path(finfo))
                elif exists(dump_dir.filename_private_path(finfo)):
                    os.remove(dump_dir.filename_private_path(finfo))

    def get_fileparts_list(self):
        if self._parts_enabled:
            if self._partnum_todo:
                return [self._partnum_todo]
            else:
                return range(1, len(self._parts) + 1)
        else:
            return False

    def list_reg_files(self, dump_dir, dump_names=None, date=None, parts=None):
        '''list all regular output files that exist'''
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_reg_files(
                date, dname, self.file_type, self.file_ext, parts, temp=False))
        return files

    def list_checkpt_files(self, dump_dir, dump_names=None, date=None, parts=None):
        '''list all checkpoint files that exist'''
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files(
                date, dname, self.file_type, self.file_ext, parts, temp=False))
        return files

    def list_checkpt_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''list checkpoint files that have been produced for specified file part(s)'''
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False))
        return files

    def list_reg_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''list noncheckpoint files that have been produced for specified file part(s)'''
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=False))
        return files

    def list_temp_files_for_filepart(self, dump_dir, parts, dump_names=None):
        '''list temp output files that have been produced for specified file part(s)'''
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files(
                None, dname, self.file_type, self.file_ext, parts, temp=True))
            files.extend(dump_dir.get_reg_files(
                None, dname, self.file_type, self.file_ext, parts, temp=True))
        return files

    def _get_files_possible(self, dump_dir, date=None, dumpname=None,
                            file_type=None, file_ext=None, parts=None, temp=False):
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
        '''

        files = []
        if dumpname is None:
            dumpname = self.dumpname
        if parts is None or parts is False:
            files.append(DumpFilename(dump_dir._wiki, date, dumpname,
                                      file_type, file_ext, None, None, temp))
        if parts is True or parts is None:
            parts = self.get_fileparts_list()
        if parts:
            for partnum in parts:
                files.append(DumpFilename(dump_dir._wiki, date, dumpname,
                                          file_type, file_ext, partnum, None, temp))
        return files

    def get_reg_files_for_filepart_possible(self, dump_dir, parts, dump_names=None):
        '''
        based on dump name, parts, etc. get all the
        output files we expect to generate for these parts
        '''
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(
                dump_dir, None, dname, self.file_type, self.file_ext, parts, temp=False))
        return files

# these routines are all used for listing output files for various purposes...

    def list_outfiles_to_publish(self, dump_dir, dump_names=None):
        '''
        this is the complete list of files produced by a dump step.
        Includes: checkpoints, parts, complete files, temp files if they
        exist. At end of run temp files must be gone.
        even if only one file part (one subjob) is being rerun, this lists all output files,
        not just those for the one part.
        '''

        if dump_names is None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file is not None:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            files.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
            files.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            files.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir, dump_names=None):
        '''
        lists all files that will be examined at the end of the run to be sure
        they were written out completely.
        Includes: checkpoint files, file parts, whole files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file is not None:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            files.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            files.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return files

    def list_outfiles_for_build_command(self, dump_dir, dump_names=None):
        '''
        called when the job command is generated.
        Includes: parts, whole files, temp files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file is not None:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            files.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            files.extend(self.get_reg_files_for_filepart_possible(
                dump_dir, self.get_fileparts_list(), dump_names))
        return files

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        '''
        called before job run to cleanup old files left around from any previous run(s)
        Includes: checkpoints, parts, whole files, temp files if they exist.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        files = []

        if self.checkpoint_file is not None:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            files.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
            files.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            files.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return files

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        files = []
        if self._checkpoints_enabled:
            files.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            files.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return files
