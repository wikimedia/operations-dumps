'''
All dump jobs are defined here
from xml page dumps to dumps of private mysql tables
to recombining multiple stub dumps into one file
'''

import getopt, hashlib, os, re, sys, errno, time
import subprocess, select
import shutil, stat, signal, glob
import Queue, thread, traceback, socket

from os.path import exists
from subprocess import Popen, PIPE
from dumps.WikiDump import FileUtils, MiscUtils, TimeUtils
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel

from dumps.exceptions import *
from dumps.fileutils import *
from dumps.utils import RunInfo, MultiVersion, RunInfoFile, Chunk

class Dump(object):
    def __init__(self, name, desc, verbose=False):
        self._desc = desc
        self.verbose = verbose
        self.progress = ""
        self.runinfo = RunInfo(name, "waiting", "")
        self.dumpname = self.get_dumpname()
        self.file_type = self.get_filetype()
        self.file_ext = self.get_file_ext()
        # if var hasn't been defined by a derived class already.  (We get
        # called last by child classes in their constructor, so that
        # their functions overriding things like the dumpbName can
        # be set up before we use them to set class attributes.)
        if not hasattr(self, 'onlychunks'):
            self.onlychunks = False
        if not hasattr(self, '_chunks_enabled'):
            self._chunks_enabled = False
        if not hasattr(self, '_checkpoints_enabled'):
            self._checkpoints_enabled = False
        if not hasattr(self, 'checkpoint_file'):
            self.checkpoint_file = False
        if not hasattr(self, '_chunk_todo'):
            self._chunk_todo = False
        if not hasattr(self, '_prerequisite_items'):
            self._prerequisite_items = []
        if not hasattr(self, '_check_truncation'):
            # Automatic checking for truncation of produced files is
            # (due to dump_dir handling) only possible for public dir
            # right now. So only set this to True, when all files of
            # the item end in the public dir.
            self._check_truncation = False

    def name(self):
        return self.runinfo.name()

    def status(self):
        return self.runinfo.status()

    def updated(self):
        return self.runinfo.updated()

    def to_run(self):
        return self.runinfo.to_run()

    def set_name(self, name):
        self.runinfo.set_name(name)

    def set_to_run(self, to_run):
        self.runinfo.set_to_run(to_run)

    def set_skipped(self):
        self.set_status("skipped")
        self.set_to_run(False)

    # sometimes this will be called to fill in data from an old
    # dump run; in those cases we don't want to clobber the timestamp
    # with the current time.
    def set_status(self, status, set_updated=True):
        self.runinfo.set_status(status)
        if set_updated:
            self.runinfo.set_updated(TimeUtils.prettyTime())

    def set_updated(self, updated):
        self.runinfo.set_updated(updated)

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

    def start(self, runner):
        """Set the 'in progress' flag so we can output status."""
        self.set_status("in-progress")

    def dump(self, runner):
        """Attempt to run the operation, updating progress/status info."""
        try:
            for prerequisite_item in self._prerequisite_items:
                if prerequisite_item.status() == "failed":
                    raise BackupError("Required job %s failed, not starting job %s" % (prerequisite_item.name(), self.name()))
                elif prerequisite_item.status() != "done":
                    raise BackupPrereqError("Required job %s not marked as done, not starting job %s" % (prerequisite_item.name(), self.name()))

            self.run(runner)
            self.post_run(runner)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if self.verbose:
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
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
            raise BackupError("Encountered %d truncated files for %s" % (truncated_files_count, self.dumpname))

    def check_for_truncated_files(self, runner):
        """Returns the number of files that have been detected to be truncated. This function expects that all files to check for truncation live in the public dir"""
        ret = 0

        if not runner._check_for_trunc_files_enabled or not self._check_truncation:
            return ret

        for dump_fname in self.list_outfiles_to_check_for_truncation(runner.dump_dir):
            dfile = DumpFile(runner.wiki, runner.dump_dir.filename_public_path(dump_fname), dump_fname);

            file_truncated=True;
            if exists(dfile.filename):
                if dfile.check_if_truncated():
                    # The file exists and is truncated, we move it out of the way
                    dfile.rename(dfile.filename + ".truncated")

                    # We detected a failure and could abort right now. However,
                    # there might still be some further chunk files, that are good.
                    # Hence, we go on treating the remaining files and in the end
                    # /all/ truncated files have been moved out of the way. So we
                    # see, which chunks (instead of the whole job) need a rerun.
                else:
                    # The file exists and is not truncated. Heck, it's a good file!
                    file_truncated=False

            if file_truncated:
                ret+=1

        return ret

    def progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files."""
        # pass through...
        if line:
            if runner.log:
                runner.log.add_to_log_queue(line)
            sys.stderr.write(line)
        self.progress = line.strip()
        runner.status.update_status_files()
        runner.runinfo_file.save_dump_runinfo_file(runner.dump_item_list.report_dump_runinfo())

    def time_to_wait(self):
        # we use wait this many secs for a command to complete that
        # doesn't produce output
        return 5

    def wait_alarm_handler(self, signum, frame):
        pass

    def build_recombine_command_string(self, runner, files, output_file, compression_command, uncompression_command, end_header_marker="</siteinfo>"):
        output_filename = runner.dump_dir.filename_public_path(output_file)
        chunknum = 0
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
        output_filename_esc = MiscUtils.shellEscape(output_filename)
        head_esc = MiscUtils.shellEscape(head)
        tail_esc = MiscUtils.shellEscape(tail)
        grep_esc = MiscUtils.shellEscape(grep)

        uncompression_command_esc = uncompression_command[:]
        for command in uncompression_command_esc:
            command = MiscUtils.shellEscape(command)
        for command in compression_command:
            command = MiscUtils.shellEscape(command)

        if not files:
            raise BackupError("No files for the recombine step found in %s." % self.name())

        for file_obj in files:
            # uh oh FIXME
#            f = MiscUtils.shellEscape(file_obj.filename)
            fpath = runner.dump_dir.filename_public_path(file_obj)
            chunknum = chunknum + 1
            pipeline = []
            uncompress_this_file = uncompression_command[:]
            uncompress_this_file.append(fpath)
            pipeline.append(uncompress_this_file)
            # warning: we figure any header (<siteinfo>...</siteinfo>) is going to be less than 2000 lines!
            pipeline.append([head, "-2000"])
            pipeline.append([grep, "-n", end_header_marker])
            # without shell
            proc = CommandPipeline(pipeline, quiet=True)
            proc.run_pipeline_get_output()
            if (proc.output()) and (proc.exited_successfully() or proc.get_failed_commands_with_exit_value() == [[-signal.SIGPIPE, uncompress_this_file]] or proc.get_failed_commands_with_exit_value() == [[signal.SIGPIPE + 128, uncompress_this_file]]):
                (header_end_num, junk) = proc.output().split(":", 1)
                # get header_end_num
            else:
                raise BackupError("Could not find 'end of header' marker for %s" % fpath)
            recombine = " ".join(uncompress_this_file)
            header_end_num = int(header_end_num) + 1
            if chunknum == 1:
                # first file, put header and contents
                recombine = recombine + " | %s -n -1 " % head_esc
            elif chunknum == len(files):
                # last file, put footer
                recombine = recombine + (" | %s -n +%s" % (tail_esc, header_end_num))
            else:
                # put contents only
                recombine = recombine + (" | %s -n +%s" % (tail_esc, header_end_num))
                recombine = recombine + " | %s -n -1 " % head
            recombines.append(recombine)
        recombine_command_string = "(" + ";".join(recombines) + ")" + "|" + "%s %s" % (compression_command, output_filename)
        return recombine_command_string

    def cleanup_old_files(self, dump_dir, runner, chunks=False):
        if runner._cleanup_old_files_enabled:
            if self.checkpoint_file:
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

    def get_chunk_list(self):
        if self._chunks_enabled:
            if self._chunk_todo:
                return [self._chunk_todo]
            else:
                return range(1, len(self._chunks)+1)
        else:
            return False

    # list all regular output files that exist
    def list_reg_files_existing(self, dump_dir, dump_names=None, date=None, chunks=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_reg_files_existing(date, dname, self.file_type, self.file_ext, chunks, temp=False))
        return files

    # list all checkpoint files that exist
    def list_checkpt_files_existing(self, dump_dir, dump_names=None, date=None, chunks=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files_existing(date, dname, self.file_type, self.file_ext, chunks, temp=False))
        return files

    # unused
    # list all temp output files that exist
    def list_temp_files_existing(self, dump_dir, dump_names=None, date=None, chunks=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks=None, temp=True))
            files.extend(dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks=None, temp=True))
        return files

    # list checkpoint files that have been produced for specified chunk(s)
    def list_checkpt_files_per_chunk_existing(self, dump_dir, chunks, dump_names=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks, temp=False))
        return files

    # list noncheckpoint files that have been produced for specified chunk(s)
    def list_reg_files_per_chunk_existing(self, dump_dir, chunks, dump_names=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks, temp=False))
        return files

    # list temp output files that have been produced for specified chunk(s)
    def list_temp_files_per_chunk_existing(self, dump_dir, chunks, dump_names=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks, temp=True))
            files.extend(dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks, temp=True))
        return files


    # unused
    # list noncheckpoint chunk files that have been produced
    def list_reg_files_chunked_existing(self, dump_dir, runner, dump_names=None, date=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(runner.dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks=self.get_chunk_list(), temp=False))
        return files

    # unused
    # list temp output chunk files that have been produced
    def list_temp_files_chunked_existing(self, runner, dump_names=None):
        files = []
        if not dump_names:
            dump_names = [self.dumpname]
        for dname in dump_names:
            files.extend(runner.dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks=self.get_chunk_list(), temp=True))
            files.extend(runner.dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks=self.get_chunk_list(), temp=True))
        return files

    # unused
    # list checkpoint files that have been produced for chunkless run
    def list_checkpt_files_nochunk_existing(self, runner, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(runner.dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks=False, temp=False))
        return files

    # unused
    # list non checkpoint files that have been produced for chunkless run
    def list_reg_files_nochunk_existing(self, runner, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(runner.dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks=False, temp=False))
        return files

    # unused
    # list non checkpoint files that have been produced for chunkless run
    def list_tempfiles_nochunk_existing(self, runner, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(runner.dump_dir.get_checkpt_files_existing(None, dname, self.file_type, self.file_ext, chunks=False, temp=True))
            files.extend(runner.dump_dir.get_reg_files_existing(None, dname, self.file_type, self.file_ext, chunks=False, temp=True))
        return files


    # internal function which all the public get*Possible functions call
    # list all files that could be created for the given dumpname, filtering by the given args.
    # by definition, checkpoint files are never returned in such a list, as we don't
    # know where a checkpoint might be taken (which pageId start/end).
    #
    # if we get None for an arg then we accept all values for that arg in the filename
    # if we get False for an arg (chunk, temp), we reject any filename which contains a value for that arg
    # if we get True for an arg (temp), we accept only filenames which contain a value for the arg
    # chunks should be a list of value(s), or True / False / None
    def _get_files_possible(self, dump_dir, date=None, dumpname=None, file_type=None, file_ext=None, chunks=None, temp=False):
        files = []
        if dumpname == None:
            dumpname = self.dumpname
        if chunks == None or chunks == False:
            files.append(DumpFilename(dump_dir._wiki, date, dumpname, file_type, file_ext, None, None, temp))
        if chunks == True or chunks == None:
            chunks = self.get_chunk_list()
        if chunks:
            for chunk in chunks:
                files.append(DumpFilename(dump_dir._wiki, date, dumpname, file_type, file_ext, chunk, None, temp))
        return files

    # unused
    # based on dump name, get all the output files we expect to generate except for temp files
    def get_reg_files_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=None, temp=False))
        return files

    # unused
    # based on dump name, get all the temp output files we expect to generate
    def get_temp_files_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=None, temp=True))
        return files

    # based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
    def get_reg_files_per_chunk_possible(self, dump_dir, chunks, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks, temp=False))
        return files

    # unused
    # based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
    def get_temp_files_per_chunk_possible(self, dump_dir, chunks, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks, temp=True))
        return files


    # unused
    # based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
    def get_reg_files_chunked_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=True, temp=False))
        return files

    # unused
    # based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
    def get_temp_files_per_chunked_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=True, temp=True))
        return files

    # unused
    # list noncheckpoint files that should be produced for chunkless run
    def get_reg_files_nochunk_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=False, temp=False))
        return files

    # unused
    # list temp output files that should be produced for chunkless run
    def get_temp_files_nochunk_possible(self, dump_dir, dump_names=None):
        if not dump_names:
            dump_names = [self.dumpname]
        files = []
        for dname in dump_names:
            files.extend(self._get_files_possible(dump_dir, None, dname, self.file_type, self.file_ext, chunks=False, temp=True))
        return files

################################
#
# these routines are all used for listing output files for various purposes...
#
#
    # Used for updating md5/sha1 lists, index.html
    # Includes: checkpoints, chunks, chunkless, temp files if they exist. At end of run temp files must be gone.
    # This is *all* output files for the dumpname, regardless of what's being re-run.
    def list_outfiles_to_publish(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
            files.extend(self.list_temp_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
                # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.get_reg_files_per_chunk_possible(dump_dir, self.get_chunk_list(), dump_names))
        return files

    # called at end of job run to see if results are intact or are garbage and must be tossed/rerun.
    # Includes: checkpoints, chunks, chunkless.  Not included: temp files.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def list_outfiles_to_check_for_truncation(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.get_reg_files_per_chunk_possible(dump_dir, self.get_chunk_list(), dump_names))
        return files

    # called when putting together commands to produce output for the job.
    # Includes: chunks, chunkless, temp files.   Not included: checkpoint files.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def list_outfiles_for_build_command(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_temp_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
                # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.get_reg_files_per_chunk_possible(dump_dir, self.get_chunk_list(), dump_names))
        return files

    # called before job run to cleanup old files left around from any previous run(s)
    # Includes: checkpoints, chunks, chunkless, temp files if they exist.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self.checkpoint_file:
            files.append(self.checkpoint_file)
            return files

        if self._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
            files.extend(self.list_temp_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_reg_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        return files

    # used to generate list of input files for other phase (e.g. recombine, recompress)
    # Includes: checkpoints, chunks/chunkless files depending on whether chunks are enabled. Not included: temp files.
    # This is *all* output files for the job, regardless of what's being re-run. The caller can sort out which
    # files go to which chunk, in case input is needed on a per chunk basis. (Is that going to be annoying? Nah,
    # and we only do it once per job so who cares.)
    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self._checkpoints_enabled:
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
            files.extend(self.list_reg_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        return files

class PublicTable(Dump):
    """Dump of a table using MySQL's mysqldump utility."""

    def __init__(self, table, name, desc):
        self._table = table
        self._chunks_enabled = False
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return self._table

    def get_filetype(self):
        return "sql"

    def get_file_ext(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpname)
        output_file = files[0]
        error = self.save_table(self._table, runner.dump_dir.filename_public_path(output_file), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_table(self._table, runner.dump_dir.filename_public_path(output_file), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    # returns 0 on success, 1 on error
    def save_table(self, table, outfile, runner):
        """Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        commands = runner.db_server_info.build_sqldump_command(table, runner.wiki.config.gzip)
        return runner.save_command(commands, outfile)

class PrivateTable(PublicTable):
    """Hidden table dumps for private data."""

    def __init__(self, table, name, desc):
        # Truncation checks require output to public dir, hence we
        # cannot use them. The default would be 'False' anyways, but
        # if that default changes, we still cannot use automatic
        # truncation checks.
        self._check_truncation = False
        PublicTable.__init__(self, table, name, desc)

    def description(self):
        return self._desc + " (private)"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpname)
        output_file = files[0]
        error = self.save_table(self._table, runner.dump_dir.filename_private_path(output_file), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_table(self._table, runner.dump_dir.filename_private_path(output_file), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    def list_outfiles_to_publish(self, dump_dir):
        """Private table won't have public files to list."""
        return []

class XmlStub(Dump):
    """Create lightweight skeleton dumps, minus bulk text.
    A second pass will import text from prior dumps or the database to make
    full files for the public."""

    def __init__(self, name, desc, chunkToDo, chunks=False, checkpoints=False):
        self._chunk_todo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
        self.history_dump_name = "stub-meta-history"
        self.current_dump_name = "stub-meta-current"
        self.articles_dump_name = "stub-articles"
        if checkpoints:
            self._checkpoints_enabled = True
        self._check_truncation = True
        Dump.__init__(self, name, desc)

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_dumpname(self):
        return 'stub'

    def list_dumpnames(self):
        dump_names =  [self.history_dump_name, self.current_dump_name, self.articles_dump_name]
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_build_command(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_cleanup(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        if dump_names == None:
            dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return files

    def build_command(self, runner, outf):
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        articles_file = runner.dump_dir.filename_public_path(outf)
        history_file = runner.dump_dir.filename_public_path(DumpFilename(runner.wiki, outf.date, self.history_dump_name, outf.file_type, outf.file_ext, outf.chunk, outf.checkpoint, outf.temp))
        current_file = runner.dump_dir.filename_public_path(DumpFilename(runner.wiki, outf.date, self.current_dump_name, outf.file_type, outf.file_ext, outf.chunk, outf.checkpoint, outf.temp))
        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        command = ["/usr/bin/python", "xmlstubs.py", "--config", runner.wiki.config.files[0], "--wiki", runner.db_name,
                    "--articles", articles_file,
                    "--history", history_file, "--current", current_file]

        if outf.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._chunks[i] for i in range(0, outf.chunk_int-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if outf.chunk_int < len(self._chunks):
                end = sum([self._chunks[i] for i in range(0, outf.chunk_int)]) +1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        for fname in files:
            # choose arbitrarily one of the dump_names we do (= articles_dump_name)
            # buildcommand will figure out the files for the rest
            if fname.dumpname == self.articles_dump_name:
                series = self.build_command(runner, fname)
                commands.append(series)
        error = runner.run_command(commands, callback_stderr=self.progress_callback, callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing stub files")

class RecombineXmlStub(Dump):
    def __init__(self, name, desc, item_for_xml_stubs):
        self.item_for_xml_stubs = item_for_xml_stubs
        self._prerequisite_items = [self.item_for_xml_stubs]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def list_dumpnames(self):
        return self.item_for_xml_stubs.list_dumpnames()

    def list_outfiles_to_publish(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return files

    def get_filetype(self):
        return self.item_for_xml_stubs.get_filetype()

    def get_file_ext(self):
        return self.item_for_xml_stubs.get_file_ext()

    def get_dumpname(self):
        return self.item_for_xml_stubs.get_dumpname()

    def run(self, runner):
        error=0
        files = self.item_for_xml_stubs.list_outfiles_for_input(runner.dump_dir)
        output_file_list = self.list_outfiles_for_build_command(runner.dump_dir, self.list_dumpnames())
        for output_file_obj in output_file_list:
            input_files = []
            for in_file in files:
                if in_file.dumpname == output_file_obj.dumpname:
                    input_files.append(in_file)
            if not len(input_files):
                self.set_status("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(runner.wiki.config.gzip):
                raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
            compression_command = runner.wiki.config.gzip
            compression_command = "%s > " % runner.wiki.config.gzip
            uncompression_command = ["%s" % runner.wiki.config.gzip, "-dc"]
            recombine_command_string = self.build_recombine_command_string(runner, input_files, output_file_obj, compression_command, uncompression_command)
            recombine_command = [recombine_command_string]
            recombine_pipeline = [recombine_command]
            series = [recombine_pipeline]
            result = runner.run_command([series], callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining stub files")

class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, chunks=False):
        Dump.__init__(self, "xmlpagelogsdump", desc)

    def detail(self):
        return "This contains the log of actions performed on pages and users."

    def get_dumpname(self):
        return "pages-logging"

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_temp_filename(self, name, number):
        return name + "-" + str(number)

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("logging table job wants to produce more than one output file")
        output_file_obj = files[0]
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)
        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        logging = runner.dump_dir.filename_public_path(output_file_obj)

        command = ["/usr/bin/python", "xmllogs.py", "--config", runner.wiki.config.files[0], "--wiki", runner.db_name,
                   "--outfile", logging]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], callback_stderr=self.progress_callback, callback_stderr_arg=runner)
        if error:
            raise BackupError("error dumping log files")

class XmlDump(Dump):
    """Primary XML dumps, one section at a time."""
    def __init__(self, subset, name, desc, detail, item_for_stubs, prefetch, spawn, wiki, chunkToDo, chunks=False, checkpoints=False, checkpoint_file=None, page_id_range=None, verbose=False):
        self._subset = subset
        self._detail = detail
        self._desc = desc
        self._prefetch = prefetch
        self._spawn = spawn
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
        self._page_id = {}
        self._chunk_todo = chunkToDo

        self.wiki = wiki
        self.item_for_stubs = item_for_stubs
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        if self.checkpoint_file:
            # we don't checkpoint the checkpoint file.
            self._checkpoints_enabled = False
        self.page_id_range = page_id_range
        self._prerequisite_items = [self.item_for_stubs]
        self._check_truncation = True
        Dump.__init__(self, name, desc)

    def get_dumpname_base(self):
        return 'pages-'

    def get_dumpname(self):
        return self.get_dumpname_base() + self._subset

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "bz2"

    def run(self, runner):
        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        # just get the files pertaining to our dumpname, which is *one* of articles, pages-current, pages-history.
        # stubs include all of them together.
        if not self.dumpname.startswith(self.get_dumpname_base()):
            raise BackupError("dumpname %s of unknown form for this job" % self.dumpname)
        dumpname = self.dumpname[len(self.get_dumpname_base()):]
        stub_dumpnames = self.item_for_stubs.list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_files = self.item_for_stubs.list_outfiles_for_input(runner.dump_dir, [stub_dumpname])
        if self._chunks_enabled and self._chunk_todo:
            # reset inputfiles to just have the one we want.
            for inp_file in input_files:
                if inp_file.chunk_int == self._chunk_todo:
                    input_files = [inp_file]
                    break
            if len(input_files) > 1:
                raise BackupError("Trouble finding stub files for xml dump run")

        if self.checkpoint_file:
            # fixme this should be an input file, not the output checkpoint file. move
            # the code out of build_command that does the conversion and put it here.
            series = self.build_command(runner, self.checkpoint_file)
            commands.append(series)
        else:
            for inp_file in input_files:
                output_file = DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext)
                series = self.build_command(runner, inp_file)
                commands.append(series)

        error = runner.run_command(commands, callback_stderr=self.progress_callback, callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing xml file(s) %s" % self.dumpname)

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--current"

    # takes name of the output file
    def build_filters(self, runner, inp_file):
        """Construct the output filter options for dumpTextPass.php"""
        # do we need checkpoints? ummm
        xmlbz2 = runner.dump_dir.filename_public_path(inp_file)

        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if self.wiki.config.bzip2[-6:] == "dbzip2":
            bz2mode = "dbzip2"
        else:
            bz2mode = "bzip2"
        return "--output=%s:%s" % (bz2mode, xmlbz2)

    def write_partial_stub(self, input_file, output_file, start_page_id, end_page_id, runner):
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" % self.wiki.config.writeuptopageid)
        writeuptopageid = self.wiki.config.writeuptopageid

        inputfile_path = runner.dump_dir.filename_public_path(input_file)
        output_file_path = os.path.join(self.wiki.config.tempDir, output_file.filename)
        if input_file.file_ext == "gz":
            command1 =  "%s -dc %s" % (self.wiki.config.gzip, inputfile_path)
            command2 = "%s > %s" % (self.wiki.config.gzip, output_file_path)
        elif input_file.file_ext == '7z':
            command1 =  "%s e -si %s" % (self.wiki.config.sevenzip, inputfile_path)
            command2 =  "%s e -so %s" % (self.wiki.config.sevenzip, output_file_path)
        elif input_file.file_ext == 'bz':
            command1 =  "%s -dc %s" % (self.wiki.config.bzip2, inputfile_path)
            command2 =  "%s > %s" % (self.wiki.config.bzip2, output_file_path)
        else:
            raise BackupError("unknown stub file extension %s" % input_file.file_ext)
        if end_page_id:
            command = [command1 + ("| %s %s %s |" % (self.wiki.config.writeuptopageid, start_page_id, end_page_id)) + command2]
        else:
            # no lastpageid? read up to eof of the specific stub file that's used for input
            command = [command1 + ("| %s %s |" % (self.wiki.config.writeuptopageid, start_page_id)) + command2]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], shell=True)
        if error:
            raise BackupError("failed to write partial stub file %s" % output_file.filename)

    def build_command(self, runner, outfile):
        """Build the command line for the dump, minus output and filter options"""

        if self.checkpoint_file:
            output_file = outfile
        elif self._checkpoints_enabled:
            # we write a temp file, it will be checkpointed every so often.
            output_file = DumpFilename(self.wiki, outfile.date, self.dumpname, outfile.file_type, self.file_ext, outfile.chunk, outfile.checkpoint, temp=True)
        else:
            # we write regular files
            output_file = DumpFilename(self.wiki, outfile.date, self.dumpname, outfile.file_type, self.file_ext, outfile.chunk, checkpoint=False, temp=False)

        # Page and revision data pulled from this skeleton dump...
        # FIXME we need the stream wrappers for proper use of writeupto. this is a hack.
        if self.checkpoint_file or self.page_id_range:
            # fixme I now have this code in a couple places, make it a function.
            if not self.dumpname.startswith(self.get_dumpname_base()):
                raise BackupError("dumpname %s of unknown form for this job" % self.dumpname)
            dumpname = self.dumpname[len(self.get_dumpname_base()):]
            stub_dumpnames = self.item_for_stubs.list_dumpnames()
            for sname in stub_dumpnames:
                if sname.endswith(dumpname):
                    stub_dumpname = sname

        if self.checkpoint_file:
            stub_input_filename = self.checkpoint_file.new_filename(stub_dumpname, self.item_for_stubs.get_filetype(), self.item_for_stubs.get_file_ext(), self.checkpoint_file.date, self.checkpoint_file.chunk)
            stub_input_file = DumpFilename(self.wiki)
            stub_input_file.new_from_filename(stub_input_filename)
            stub_output_filename = self.checkpoint_file.new_filename(stub_dumpname, self.item_for_stubs.get_filetype(), self.item_for_stubs.get_file_ext(), self.checkpoint_file.date, self.checkpoint_file.chunk, self.checkpoint_file.checkpoint)
            stub_output_file = DumpFilename(self.wiki)
            stub_output_file.new_from_filename(stub_output_filename)
            self.write_partial_stub(stub_input_file, stub_output_file, self.checkpoint_file.first_page_id, str(int(self.checkpoint_file.last_page_id) + 1), runner)
            stub_option = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stub_output_file.filename)
        elif self.page_id_range:
            # two cases. redoing a specific chunk, OR no chunks, redoing the whole output file. ouch, hope it isn't huge.
            if self._chunk_todo or not self._chunks_enabled:
                stub_input_file = outfile

            stub_output_filename = stub_input_file.new_filename(stub_dumpname, self.item_for_stubs.get_filetype(), self.item_for_stubs.get_file_ext(), stub_input_file.date, stub_input_file.chunk, stub_input_file.checkpoint)
            stub_output_file = DumpFilename(self.wiki)
            stub_output_file.new_from_filename(stub_output_filename)
            if ',' in self.page_id_range:
                (first_page_id, last_page_id) = self.page_id_range.split(',', 2)
            else:
                first_page_id = self.page_id_range
                last_page_id = None
            self.write_partial_stub(stub_input_file, stub_output_file, first_page_id, last_page_id, runner)

            stub_option = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stub_output_file.filename)
        else:
            stub_option = "--stub=gzip:%s" % runner.dump_dir.filename_public_path(outfile)

        # Try to pull text from the previous run; most stuff hasn't changed
        #Source=$OutputDir/pages_$section.xml.bz2
        sources = []
        possible_sources = None
        if self._prefetch:
            possible_sources = self._find_previous_dump(runner, outfile.chunk)
            # if we have a list of more than one then we need to check existence for each and put them together in a string
            if possible_sources:
                for sourcefile in possible_sources:
                    sname = runner.dump_dir.filename_public_path(sourcefile, sourcefile.date)
                    if exists(sname):
                        sources.append(sname)
        if outfile.chunk:
            chunkinfo = "%s" % outfile.chunk
        else:
            chunkinfo =""
        if len(sources) > 0:
            source = "bzip2:%s" % (";".join(sources))
            runner.show_runner_state("... building %s %s XML dump, with text prefetch from %s..." % (self._subset, chunkinfo, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.show_runner_state("... building %s %s XML dump, no text prefetch..." % (self._subset, chunkinfo))
            prefetch = ""

        if self._spawn:
            spawn = "--spawn=%s" % (self.wiki.config.php)
        else:
            spawn = ""

        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)

        if self._checkpoints_enabled:
            checkpoint_time = "--maxtime=%s" % (self.wiki.config.checkpoint_time)
            checkpoint_file = "--checkpointfile=%s" % output_file.new_filename(output_file.dumpname, output_file.file_type, output_file.file_ext, output_file.date, output_file.chunk, "p%sp%s", None)
        else:
            checkpoint_time = ""
            checkpoint_file = ""
        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpTextPass.php")
        dump_command = ["%s" % self.wiki.config.php, "-q"]
        dump_command.extend(script_command)
        dump_command.extend(["--wiki=%s" % runner.db_name,
                    "%s" % stub_option,
                    "%s" % prefetch,
                    "%s" % checkpoint_time,
                    "%s" % checkpoint_file,
                    "--report=1000",
                    "%s" % spawn
                   ])

        dump_command = filter(None, dump_command)
        command = dump_command
        filters = self.build_filters(runner, output_file)
        eta = self.build_eta(runner)
        command.extend([filters, eta])
        pipeline = [command]
        series = [pipeline]
        return series

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_nicely(self, mylist):
        """ Sort the given list in the way that humans expect.
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        mylist.sort(key=alphanum_key)

    def get_relevant_prefetch_files(self, file_list, start_page_id, end_page_id, date, runner):
        possibles = []
        if len(file_list):
            # (a) nasty hack, see below (b)
            maxchunks = 0
            for file_obj in file_list:
                if file_obj.is_chunk_file and file_obj.chunk_int > maxchunks:
                    maxchunks = file_obj.chunk_int
                if not file_obj.first_page_id:
                    fname = DumpFile(self.wiki, runner.dump_dir.filename_public_path(file_obj, date), file_obj, self.verbose)
                    file_obj.first_page_id = fname.find_first_page_id_in_file()

                        # get the files that cover our range
                for file_obj in file_list:
                # If some of the file_objs in file_list could not be properly be parsed, some of
                # the (int) conversions below will fail. However, it is of little use to us,
                # which conversion failed. /If any/ conversion fails, it means, that that we do
                # not understand how to make sense of the current file_obj. Hence we cannot use
                # it as prefetch object and we have to drop it, to avoid passing a useless file
                # to the text pass. (This could days as of a comment below, but by not passing
                # a likely useless file, we have to fetch more texts from the database)
                #
                # Therefore try...except-ing the whole block is sufficient: If whatever error
                # occurs, we do not abort, but skip the file for prefetch.
                    try:
                        # If we could properly parse
                        first_page_id_in_file = int(file_obj.first_page_id)

                        # fixme what do we do here? this could be very expensive. is that worth it??
                        if not file_obj.last_page_id:
                            # (b) nasty hack, see (a)
                            # it's not a checkpoint fle or we'd have the pageid in the filename
                            # so... temporary hack which will give expensive results
                            # if chunk file, and it's the last chunk, put none
                            # if it's not the last chunk, get the first pageid in the next chunk and subtract 1
                            # if not chunk, put none.
                            if file_obj.is_chunk_file and file_obj.chunk_int < maxchunks:
                                for fname in file_list:
                                    if fname.chunk_int == file_obj.chunk_int + 1:
                                        # not true!  this could be a few past where it really is
                                        # (because of deleted pages that aren't included at all)
                                        file_obj.last_page_id = str(int(fname.first_page_id) - 1)
                        if file_obj.last_page_id:
                            last_page_id_in_file = int(file_obj.last_page_id)
                        else:
                            last_page_id_in_file = None

                            # FIXME there is no point in including files that have just a few rev ids in them
                            # that we need, and having to read through the whole file... could take
                            # hours or days (later it won't matter, right? but until a rewrite, this is important)
                            # also be sure that if a critical page is deleted by the time we try to figure out ranges,
                            # that we don't get hosed
                        if (first_page_id_in_file <= int(start_page_id) and (last_page_id_in_file == None or last_page_id_in_file >= int(start_page_id))) or (first_page_id_in_file >= int(start_page_id) and (end_page_id == None or first_page_id_in_file <= int(end_page_id))):
                            possibles.append(file_obj)
                    except:
                        runner.debug("Could not make sense of %s for prefetch. Format update? Corrupt file?" % file_obj.filename)
        return possibles

    # this finds the content file or files from the first previous successful dump
    # to be used as input ("prefetch") for this run.
    def _find_previous_dump(self, runner, chunk=None):
        """The previously-linked previous successful dump."""
        if chunk:
            start_page_id = sum([self._chunks[i] for i in range(0, int(chunk)-1)]) + 1
            if len(self._chunks) > int(chunk):
                end_page_id = sum([self._chunks[i] for i in range(0, int(chunk))])
            else:
                end_page_id = None
        else:
            start_page_id = 1
            end_page_id = None

        dumps = self.wiki.dumpDirs()
        dumps.sort()
        dumps.reverse()
        for date in dumps:
            if date == self.wiki.date:
                runner.debug("skipping current dump for prefetch of job %s, date %s" % (self.name(), self.wiki.date))
                continue

            # see if this job from that date was successful
            if not runner.runinfo_file.status_of_old_dump_is_done(runner, date, self.name(), self._desc):
                runner.debug("skipping incomplete or failed dump for prefetch date %s" % date)
                continue

            # first check if there are checkpoint files from this run we can use
            files = self.list_checkpt_files_existing(runner.dump_dir, [self.dumpname], date, chunks=None)
            possible_prefetch_list = self.get_relevant_prefetch_files(files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

            # ok, let's check for chunk files instead, from any run (may not conform to our numbering
            # for this job)
            files = self.list_reg_files_existing(runner.dump_dir, [self.dumpname], date, chunks=True)
            possible_prefetch_list = self.get_relevant_prefetch_files(files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

                    # last shot, get output file that contains all the pages, if there is one
            files = self.list_reg_files_existing(runner.dump_dir, [self.dumpname], date, chunks=False)
            # there is only one, don't bother to check for relevance :-P
            possible_prefetch_list = files
            files = []
            for prefetch in possible_prefetch_list:
                possible = runner.dump_dir.filename_public_path(prefetch, date)
                size = os.path.getsize(possible)
                if size < 70000:
                    runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, possible))
                    continue
                else:
                    files.append(prefetch)
            if len(files):
                return files

        runner.debug("Could not locate a prefetchable dump.")
        return None

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        files = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        files_to_return = []
        if self.page_id_range:
            if ',' in self.page_id_range:
                (first_page_id, last_page_id) = self.page_id_range.split(',', 2)
                first_page_id = int(first_page_id)
                last_page_id = int(last_page_id)
            else:
                first_page_id = int(self.page_id_range)
                last_page_id = None
            # filter any checkpoint files, removing from the list any with
            # page range outside of the page range this job will cover
            for fname in files:
                if fname.is_checkpoint_file:
                    if not first_page_id or (fname.first_page_id and (int(fname.first_page_id) >= first_page_id)):
                        if not last_page_id or (fname.last_page_id and (int(fname.last_page_id) <= last_page_id)):
                            files_to_return.append(fname)
                else:
                    files_to_return.append(fname)
        return files_to_return

class RecombineXmlDump(XmlDump):
    def __init__(self, name, desc, detail, item_for_xml_dumps):
        # no prefetch, no spawn
        self.item_for_xml_dumps = item_for_xml_dumps
        self._detail = detail
        self._prerequisite_items = [self.item_for_xml_dumps]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def list_dumpnames(self):
        return self.item_for_xml_dumps.list_dumpnames()

    def get_filetype(self):
        return self.item_for_xml_dumps.get_filetype()

    def get_file_ext(self):
        return self.item_for_xml_dumps.get_file_ext()

    def get_dumpname(self):
        return self.item_for_xml_dumps.get_dumpname()

    def run(self, runner):
        files = self.item_for_xml_dumps.list_outfiles_for_input(runner.dump_dir)
        output_files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(output_files) > 1:
            raise BackupError("recombine XML Dump trying to produce more than one output file")

        error=0
        if not exists(runner.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % runner.wiki.config.bzip2)
        compression_command = runner.wiki.config.bzip2
        compression_command = "%s > " % runner.wiki.config.bzip2
        uncompression_command = ["%s" % runner.wiki.config.bzip2, "-dc"]
        recombine_command_string = self.build_recombine_command_string(runner, files, output_files[0], compression_command, uncompression_command)
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        error = runner.run_command([series], callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)

        if error:
            raise BackupError("error recombining xml bz2 files")

class XmlMultiStreamDump(XmlDump):
    """Take a .bz2 and recompress it as multistream bz2, 100 pages per stream."""

    def __init__(self, subset, name, desc, detail, item_for_recompression, wiki, chunkToDo, chunks=False, checkpoints=False, checkpoint_file=None):
        self._subset = subset
        self._detail = detail
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
        self._chunk_todo = chunkToDo
        self.wiki = wiki
        self.item_for_recompression = item_for_recompression
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self._prerequisite_items = [self.item_for_recompression]
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return "pages-" + self._subset

    def list_dumpnames(self):
        dname = self.get_dumpname();
        return [self.get_dumpname_multistream(dname), self.get_dumpname_multistream_index(dname)];

    def get_filetype(self):
        return "xml"

    def get_index_filetype(self):
        return "txt"

    def get_file_ext(self):
        return "bz2"

    def get_dumpname_multistream(self, name):
        return name + "-multistream"

    def get_dumpname_multistream_index(self, name):
        return self.get_dumpname_multistream(name) + "-index"

    def get_multistream_fname(self, fname):
        """assuming that fname is the name of an input file,
        return the name of the associated multistream output file"""
        return DumpFilename(self.wiki, fname.date, self.get_dumpname_multistream(fname.dumpname), fname.file_type, self.file_ext, fname.chunk, fname.checkpoint, fname.temp)

    def get_multistream_index_fname(self, fname):
        """assuming that fname is the name of a multistream output file,
        return the name of the associated index file"""
        return DumpFilename(self.wiki, fname.date, self.get_dumpname_multistream_index(fname.dumpname), self.get_index_filetype(), self.file_ext, fname.chunk, fname.checkpoint, fname.temp)

    # output files is a list of checkpoint files, otherwise it is a list of one file.
    # checkpoint files get done one at a time. we can't really do parallel recompression jobs of
    # 200 files, right?
    def build_command(self, runner, output_files):
        # FIXME need shell escape
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.recompressxml):
            raise BackupError("recompressxml command %s not found" % self.wiki.config.recompressxml)

        command_series = []
        for fname in output_files:
            input_file = DumpFilename(self.wiki, None, fname.dumpname, fname.file_type, self.item_for_recompression.file_ext, fname.chunk, fname.checkpoint)
            outfile = runner.dump_dir.filename_public_path(self.get_multistream_fname(fname))
            outfile_index = runner.dump_dir.filename_public_path(self.get_multistream_index_fname(fname))
            infile = runner.dump_dir.filename_public_path(input_file)
            command_pipe = [["%s -dc %s | %s --pagesperstream 100 --buildindex %s > %s"  % (self.wiki.config.bzip2, infile, self.wiki.config.recompressxml, outfile_index, outfile)]]
            command_series.append(command_pipe)
        return command_series

    def run(self, runner):
        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        if self.checkpoint_file:
            output_file = DumpFilename(self.wiki, None, self.checkpoint_file.dumpname, self.checkpoint_file.file_type, self.file_ext, self.checkpoint_file.chunk, self.checkpoint_file.checkpoint)
            series = self.build_command(runner, [output_file])
            commands.append(series)
        elif self._chunks_enabled and not self._chunk_todo:
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for chunknum in range(1, len(self._chunks)+1):
                output_files = self.list_outfiles_for_build_command(runner.dump_dir, chunknum)
                series = self.build_command(runner, output_files)
                commands.append(series)
        else:
            output_files = self.list_outfiles_for_build_command(runner.dump_dir)
            series = self.build_command(runner, output_files)
            commands.append(series)

        error = runner.run_command(commands, callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    # shows all files possible if we don't have checkpoint files. without temp files of course
    def list_outfiles_to_publish(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            files.append(self.get_multistream_fname(inp_file))
            files.append(self.get_multistream_index_fname(inp_file))
        return files

    # shows all files possible if we don't have checkpoint files. without temp files of course
    # only the chunks we are actually supposed to do (if there is a limit)
    def list_outfiles_to_check_for_truncation(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            if self._chunk_todo and inp_file.chunk_int != self._chunk_todo:
                continue
            files.append(self.get_multistream_fname(inp_file))
            files.append(self.get_multistream_index_fname(inp_file))
        return files

    # shows all files possible if we don't have checkpoint files. no temp files.
    # only the chunks we are actually supposed to do (if there is a limit)
    def list_outfiles_for_build_command(self, dump_dir, chunk=None):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            # if this param is set it takes priority
            if chunk and inp_file.chunk_int != chunk:
                continue
            elif self._chunk_todo and inp_file.chunk_int != self._chunk_todo:
                continue
            # we don't convert these names to the final output form, we'll do that in the build command
            # (i.e. add "multistream" and "index" to them)
            files.append(DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext, inp_file.chunk, inp_file.checkpoint, inp_file.temp))
        return files

    # shows all files possible if we don't have checkpoint files. should include temp files
    # does just the chunks we do if there is a limit
    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        multistream_names = []
        for dname in dump_names:
            multistream_names.extend([self.get_dumpname_multistream(dname), self.get_dumpname_multistream_index(dname)])

        files = []
        if self.item_for_recompression._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), multistream_names))
            files.extend(self.list_temp_files_per_chunk_existing(dump_dir, self.get_chunk_list(), multistream_names))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_reg_files_per_chunk_existing(dump_dir, self.get_chunk_list(), multistream_names))
        return files

    # must return all output files that could be produced by a full run of this stage,
    # not just whatever we happened to produce (if run for one chunk, say)
    def list_outfiles_for_input(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            files.append(self.get_multistream_fname(inp_file))
            files.append(self.get_multistream_index_fname(inp_file))
        return files

class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"

class XmlRecompressDump(Dump):
    """Take a .bz2 and recompress it as 7-Zip."""

    def __init__(self, subset, name, desc, detail, item_for_recompression, wiki, chunkToDo, chunks=False, checkpoints=False, checkpoint_file=None):
        self._subset = subset
        self._detail = detail
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
        self._chunk_todo = chunkToDo
        self.wiki = wiki
        self.item_for_recompression = item_for_recompression
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self._prerequisite_items = [self.item_for_recompression]
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return "pages-" + self._subset

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "7z"

    # output files is a list of checkpoint files, otherwise it is a list of one file.
    # checkpoint files get done one at a time. we can't really do parallel recompression jobs of
    # 200 files, right?
    def build_command(self, runner, output_files):
        # FIXME need shell escape
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.sevenzip):
            raise BackupError("7zip command %s not found" % self.wiki.config.sevenzip)

        command_series = []
        for outfile in output_files:
            input_file = DumpFilename(self.wiki, None, outfile.dumpname, outfile.file_type, self.item_for_recompression.file_ext, outfile.chunk, outfile.checkpoint)
            outfilepath = runner.dump_dir.filename_public_path(outfile)
            infilepath = runner.dump_dir.filename_public_path(input_file)
            command_pipe = [["%s -dc %s | %s a -mx=4 -si %s"  % (self.wiki.config.bzip2, infilepath, self.wiki.config.sevenzip, outfilepath)]]
            command_series.append(command_pipe)
        return command_series

    def run(self, runner):
        commands = []
        # Remove prior 7zip attempts; 7zip will try to append to an existing archive
        self.cleanup_old_files(runner.dump_dir, runner)
        if self.checkpoint_file:
            output_file = DumpFilename(self.wiki, None, self.checkpoint_file.dumpname, self.checkpoint_file.file_type, self.file_ext, self.checkpoint_file.chunk, self.checkpoint_file.checkpoint)
            series = self.build_command(runner, [output_file])
            commands.append(series)
        elif self._chunks_enabled and not self._chunk_todo:
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for chunknum in range(1, len(self._chunks)+1):
                output_files = self.list_outfiles_for_build_command(runner.dump_dir, chunknum)
                series = self.build_command(runner, output_files)
                commands.append(series)
        else:
            output_files = self.list_outfiles_for_build_command(runner.dump_dir)
            series = self.build_command(runner, output_files)
            commands.append(series)

        error = runner.run_command(commands, callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    # shows all files possible if we don't have checkpoint files. without temp files of course
    def list_outfiles_to_publish(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            files.append(DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext, inp_file.chunk, inp_file.checkpoint, inp_file.temp))
        return files

    # shows all files possible if we don't have checkpoint files. without temp files of course
    # only the chunks we are actually supposed to do (if there is a limit)
    def list_outfiles_to_check_for_truncation(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            if self._chunk_todo and inp_file.chunk_int != self._chunk_todo:
                continue
            files.append(DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext, inp_file.chunk, inp_file.checkpoint, inp_file.temp))
        return files

    # shows all files possible if we don't have checkpoint files. no temp files.
    # only the chunks we are actually supposed to do (if there is a limit)
    def list_outfiles_for_build_command(self, dump_dir, chunk=None):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            # if this param is set it takes priority
            if chunk and inp_file.chunk_int != chunk:
                continue
            elif self._chunk_todo and inp_file.chunk_int != self._chunk_todo:
                continue
            files.append(DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext, inp_file.chunk, inp_file.checkpoint, inp_file.temp))
        return files

    # shows all files possible if we don't have checkpoint files. should include temp files
    # does just the chunks we do if there is a limit
    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        # some stages (eg XLMStubs) call this for several different dump_names
        if dump_names == None:
            dump_names = [self.dumpname]
        files = []
        if self.item_for_recompression._checkpoints_enabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_checkpt_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
            files.extend(self.list_temp_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.list_reg_files_per_chunk_existing(dump_dir, self.get_chunk_list(), dump_names))
        return files

    # must return all output files that could be produced by a full run of this stage,
    # not just whatever we happened to produce (if run for one chunk, say)
    def list_outfiles_for_input(self, dump_dir):
        files = []
        input_files = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_file in input_files:
            files.append(DumpFilename(self.wiki, inp_file.date, inp_file.dumpname, inp_file.file_type, self.file_ext, inp_file.chunk, inp_file.checkpoint, inp_file.temp))
        return files


class RecombineXmlRecompressDump(Dump):
    def __init__(self, name, desc, detail, item_for_recombine, wiki):
        self._detail = detail
        self._desc = desc
        self.wiki = wiki
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False
        self._chunks_enabled = False

        def get_filetype(self):
            return self.item_for_recombine.get_filetype()

        def get_file_ext(self):
            return self.item_for_recombine.get_file_ext()

        def get_dumpname(self):
            return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        error = 0
        self.cleanup_old_files(runner.dump_dir, runner)
        output_file_list = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_file in output_file_list:
            input_files = []
            files = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
            for in_file in files:
                if in_file.dumpname == output_file.dumpname:
                    input_files.append(in_file)
            if not len(input_files):
                self.set_status("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(self.wiki.config.sevenzip):
                raise BackupError("sevenzip command %s not found" % self.wiki.config.sevenzip)
            compression_command = "%s a -mx=4 -si" % self.wiki.config.sevenzip
            uncompression_command = ["%s" % self.wiki.config.sevenzip, "e", "-so"]

            recombine_command_string = self.build_recombine_command_string(runner, files, output_file, compression_command, uncompression_command)
            recombine_command = [recombine_command_string]
            recombine_pipeline = [recombine_command]
            series = [recombine_pipeline]
            result = runner.run_command([series], callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining xml bz2 file(s)")

class AbstractDump(Dump):
    """XML dump for Yahoo!'s Active Abstracts thingy"""

    def __init__(self, name, desc, chunkToDo, db_name, chunks=False):
        self._chunk_todo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
        self.db_name = db_name
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return "abstract"

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return ""

    def build_command(self, runner, fname):
        command = ["/usr/bin/python", "xmlabstracts.py", "--config", runner.wiki.config.files[0],
                    "--wiki", self.db_name]

        outputs = []
        variants = []
        for variant in self._variants():
            variant_option = self._variant_option(variant)
            dumpname = self.dumpname_from_variant(variant)
            file_obj = DumpFilename(runner.wiki, fname.date, dumpname, fname.file_type, fname.file_ext, fname.chunk, fname.checkpoint)
            outputs.append(runner.dump_dir.filename_public_path(file_obj))
            variants.append(variant_option)

            command.extend(["--outfiles=%s" % ",".join(outputs),
                              "--variants=%s" %  ",".join(variants)])

        if fname.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._chunks[i] for i in range(0, fname.chunk_int-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if fname.chunk_int < len(self._chunks):
                end = sum([self._chunks[i] for i in range(0, fname.chunk_int)]) +1
                endopt = "--end=%s" % end
                command.append(endopt)
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        # choose the empty variant to pass to buildcommand, it will fill in the rest if needed
        output_files = self.list_outfiles_for_build_command(runner.dump_dir)
        dumpname0 = self.list_dumpnames()[0]
        for fname in output_files:
            if fname.dumpname == dumpname0:
                series = self.build_command(runner, fname)
                commands.append(series)
        error = runner.run_command(commands, callback_stderr=self.progress_callback, callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing abstract dump")

    # If the database name looks like it's marked as Chinese language,
    # return a list including Simplified and Traditional versions, so
    # we can build separate files normalized to each orthography.
    def _variants(self):
        if self.db_name[0:2] == "zh" and self.db_name[2:3] != "_":
            variants = ["", "zh-cn", "zh-tw"]
        else:
            variants = [""]
        return variants

    def _variant_option(self, variant):
        if variant == "":
            return ""
        else:
            return ":variant=%s" % variant

    def dumpname_from_variant(self, variant):
        dumpname_base = 'abstract'
        if variant == "":
            return dumpname_base
        else:
            return dumpname_base + "-" + variant

    def list_dumpnames(self):
        # need this first for build_command and other such
        dump_names = []
        variants = self._variants()
        for variant in variants:
            dump_names.append(self.dumpname_from_variant(variant))
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_build_command(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_cleanup(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_input(self, dump_dir):
        dump_names =  self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return files


class RecombineAbstractDump(Dump):
    def __init__(self, name, desc, item_for_recombine):
        # no chunkToDo, no chunks generally (False, False), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        error = 0
        files = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        output_file_list = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_file in output_file_list:
            input_files = []
            for in_file in files:
                if in_file.dumpname == output_file.dumpname:
                    input_files.append(in_file)
            if not len(input_files):
                self.set_status("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(runner.wiki.config.cat):
                raise BackupError("cat command %s not found" % runner.wiki.config.cat)
            compression_command = "%s > " % runner.wiki.config.cat
            uncompression_command = ["%s" % runner.wiki.config.cat]
            recombine_command_string = self.build_recombine_command_string(runner, input_files, output_file, compression_command, uncompression_command, "<feed>")
            recombine_command = [recombine_command_string]
            recombine_pipeline = [recombine_command]
            series = [recombine_pipeline]
            result = runner.run_command([series], callback_timed=self.progress_callback, callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining abstract dump files")

class TitleDump(Dump):
    """This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""

    def get_dumpname(self):
        return "all-titles-in-ns0"

    def get_filetype(self):
        return ""

    def get_file_ext(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        query="select page_title from page where page_namespace=0;"
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("page title dump trying to produce more than one output file")
        file_obj = files[0]
        out_filename = runner.dump_dir.filename_public_path(file_obj)
        error = self.save_sql(query, out_filename, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_sql(query, out_filename, runner)
        if error:
            raise BackupError("error dumping titles list")

    def save_sql(self, query, outfile, runner):
        """Pass some SQL commands to the server for this DB and save output to a gzipped file."""
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        command = runner.db_server_info.build_sql_command(query, runner.wiki.config.gzip)
        return runner.save_command(command, outfile)

class AllTitleDump(TitleDump):

    def get_dumpname(self):
        return "all-titles"

    def run(self, runner):
        retries = 0
        maxretries = 3
        query="select page_title from page;"
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("all titles dump trying to produce more than one output file")
        file_obj = files[0]
        out_filename = runner.dump_dir.filename_public_path(file_obj)
        error = self.save_sql(query, out_filename, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_sql(query, out_filename, runner)
        if error:
            raise BackupError("error dumping all titles list")
