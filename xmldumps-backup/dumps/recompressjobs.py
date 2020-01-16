#!/usr/bin/python3
'''
All dump jobs that recompress the output
from other dump jobs are defined here
'''

from os.path import exists
import os
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump
from dumps.outfilelister import OutputFileLister


class RecompressDump(Dump):
    """Given bz2 input files, recompress them in various ways."""
    def __init__(self, subset, name, desc, detail, item_for_recompress,
                 wiki, partnum_todo, pages_per_part=None, checkpoints=False, checkpoint_file=None):
        self._subset = subset
        self._detail = detail
        self._pages_per_part = pages_per_part
        if self._pages_per_part:
            self._parts_enabled = True
        self._partnum_todo = partnum_todo
        self.wiki = wiki
        self.item_for_recompress = item_for_recompress
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self._prerequisite_items = [self.item_for_recompress]
        Dump.__init__(self, name, desc)
        self.oflister = RecompressFileLister(self.dumpname, self.file_type, self.file_ext,
                                             self.get_fileparts_list(), self.checkpoint_file,
                                             self._checkpoints_enabled, self.list_dumpnames,
                                             self._partnum_todo, self.item_for_recompress)

    def get_filetype(self):
        return "xml"


class RecompressFileLister(OutputFileLister):
    """
    special methods for recompression dump jobs

    these get dump names and other things from the job that produced
    the files to be recompressed
    """
    def __init__(self, dumpname, file_type, file_ext, fileparts_list,
                 checkpoint_file, checkpoints_enabled, list_dumpnames=None,
                 partnum_todo=None, item_for_recompress=None):
        super().__init__(dumpname, file_type, file_ext, fileparts_list,
                         checkpoint_file, checkpoints_enabled, list_dumpnames)
        self.partnum_todo = partnum_todo
        self.item_for_recompress = item_for_recompress

    def list_outfiles_for_build_command(self, args):
        '''
        shows all files possible if we don't have checkpoint files. no temp files.
        only the parts we are actually supposed to do (if there is a limit)
        expects: args.dump_dir, optional args.partnum
        returns: list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompress.oflister.list_outfiles_for_input(args)
        for inp_dfname in input_dfnames:
            # if this param is set it takes priority
            if args.partnum and inp_dfname.partnum_int != args.partnum:
                continue
            if self.partnum_todo and inp_dfname.partnum_int != self.partnum_todo:
                continue
            dfnames.append(DumpFilename(inp_dfname.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext,
                                        inp_dfname.partnum, inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames


class XmlMultiStreamDump(RecompressDump):
    """Take a .bz2 and recompress it as multistream bz2, 100 pages per stream."""

    INDEX_FILETYPE = "txt"

    def __init__(self, subset, name, desc, detail, item_for_recompress,
                 wiki, partnum_todo, pages_per_part=None, checkpoints=False, checkpoint_file=None):
        super().__init__(subset, name, desc, detail, item_for_recompress,
                         wiki, partnum_todo, pages_per_part, checkpoints, checkpoint_file)

        self.oflister = XmlMultiStreamFileLister(self.dumpname, self.file_type, self.file_ext,
                                                 self.get_fileparts_list(), self.checkpoint_file,
                                                 self._checkpoints_enabled, self.list_dumpnames,
                                                 self._partnum_todo, self.item_for_recompress)

    @staticmethod
    def get_dumpname_multistream(name):
        '''return the base part of the name of output files
        containing page content'''
        return name + "-multistream"

    @staticmethod
    def get_dumpname_multistream_index(name):
        '''return the base part of the name of multistream index files'''
        return XmlMultiStreamDump.get_dumpname_multistream(name) + "-index"

    @staticmethod
    def get_file_ext():
        return "bz2"

    @staticmethod
    def get_multistream_dfname(dfname, suffix=None):
        """
        assuming that dfname is an input file,
        return the name of the associated multistream output file
        args:
            DumpFilename
        returns:
            DumpFilename
        """
        if suffix is not None:
            file_ext = XmlMultiStreamDump.get_file_ext() + suffix
        else:
            file_ext = XmlMultiStreamDump.get_file_ext()
        return DumpFilename(dfname.wiki, dfname.date,
                            XmlMultiStreamDump.get_dumpname_multistream(dfname.dumpname),
                            dfname.file_type, file_ext, dfname.partnum,
                            dfname.checkpoint, dfname.temp)

    @staticmethod
    def get_index_filetype():
        return "txt"

    @staticmethod
    def get_multistream_index_dfname(dfname):
        """
        assuming that dfname is a multistream output file,
        return the name of the associated index file
        args:
            DumpFilename
        returns:
            DumpFilename
        """
        return DumpFilename(dfname.wiki, dfname.date,
                            XmlMultiStreamDump.get_dumpname_multistream_index(dfname.dumpname),
                            XmlMultiStreamDump.INDEX_FILETYPE, XmlMultiStreamDump.get_file_ext(),
                            dfname.partnum, dfname.checkpoint, dfname.temp)

    def get_dumpname(self):
        return "pages-" + self._subset

    def list_dumpnames(self):
        dname = self.get_dumpname()
        return [self.get_dumpname_multistream(dname),
                self.get_dumpname_multistream_index(dname)]

    def build_command(self, runner, output_dfname):
        '''
        arguments:
        runner: Runner object
        output_dfname: output file that will be produced
        '''

        input_dfname = DumpFilename(self.wiki, None, output_dfname.dumpname,
                                    output_dfname.file_type,
                                    self.item_for_recompress.file_ext,
                                    output_dfname.partnum, output_dfname.checkpoint)
        outfilepath = runner.dump_dir.filename_public_path(
            self.get_multistream_dfname(output_dfname))
        outfilepath_index = runner.dump_dir.filename_public_path(
            self.get_multistream_index_dfname(output_dfname))
        infilepath = runner.dump_dir.filename_public_path(input_dfname)
        command_pipe = [["%s -dc %s | %s --pagesperstream 100 --buildindex %s -o %s" %
                         (self.wiki.config.bzip2, infilepath, self.wiki.config.recompressxml,
                          DumpFilename.get_inprogress_name(outfilepath_index),
                          DumpFilename.get_inprogress_name(outfilepath))]]
        return [command_pipe]

    def run_in_batches(self, runner):
        '''
        generate one multistream content/index file pair for each numbered
        or numbered/checkpointed content input file, doing batches of these
        at a time
        '''
        # new code cobbled together
        commands = []
        for partnum in range(1, len(self._pages_per_part) + 1):
            content_dfnames = self.oflister.list_outfiles_for_build_command(self.oflister.makeargs(
                runner.dump_dir, partnum=partnum))
            for content_dfname in content_dfnames:
                command_series = self.build_command(runner, content_dfname)
                commands.append(command_series)
                output_dfnames = [self.get_multistream_dfname(content_dfname),
                                  self.get_multistream_index_dfname(content_dfname)]
                self.setup_command_info(runner, command_series, output_dfnames)
        # now we have all the commands, run them in batches til we are done
        batchsize = len(self._pages_per_part)
        errors = False
        while commands:
            command_batch = commands[:batchsize]
            error, broken = runner.run_command(
                command_batch, callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=True,
                callback_on_completion=self.command_completion_callback)
            if error:
                for series in broken:
                    for pipeline in series:
                        runner.log_and_print("error from commands: %s" % " ".join(pipeline))
                errors = True
            commands = commands[batchsize:]
        if errors:
            raise BackupError("error recompressing bz2 file(s)")

    def run(self, runner):
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.recompressxml):
            raise BackupError("recompressxml command %s not found" %
                              self.wiki.config.recompressxml)

        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        if self.checkpoint_file is not None:
            content_dfname = DumpFilename(self.wiki, None, self.checkpoint_file.dumpname,
                                          self.checkpoint_file.file_type, self.file_ext,
                                          self.checkpoint_file.partnum,
                                          self.checkpoint_file.checkpoint)
            command_series = self.build_command(runner, content_dfname)
            commands.append(command_series)
            output_dfnames = [self.get_multistream_dfname(content_dfname),
                              self.get_multistream_index_dfname(content_dfname)]
            self.setup_command_info(runner, command_series, output_dfnames)
        elif self._parts_enabled and not self._partnum_todo:
            self.run_in_batches(runner)
            return
        else:
            content_dfnames = self.oflister.list_outfiles_for_build_command(
                self.oflister.makeargs(runner.dump_dir))
            for content_dfname in content_dfnames:
                command_series = self.build_command(runner, content_dfname)
                output_dfnames = [self.get_multistream_dfname(content_dfname),
                                  self.get_multistream_index_dfname(content_dfname)]
                self.setup_command_info(runner, command_series, output_dfnames)
                commands.append(command_series)

        error, _broken = runner.run_command(commands, callback_timed=self.progress_callback,
                                            callback_timed_arg=runner, shell=True,
                                            callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error recompressing bz2 file(s)")


class XmlMultiStreamFileLister(RecompressFileLister):
    """
    special methods for listing xml multistream output files

    these get many fields from the regular page content dump jobs,
    which produce the input files for the multistream ones
    """
    def __init__(self, dumpname, file_type, file_ext, fileparts_list,
                 checkpoint_file, checkpoints_enabled, list_dumpnames=None,
                 partnum_todo=None, item_for_recompress=None):
        super().__init__(dumpname, file_type, file_ext, fileparts_list,
                         checkpoint_file, checkpoints_enabled, list_dumpnames)
        self.partnum_todo = partnum_todo
        self.item_for_recompress = item_for_recompress

    def list_outfiles_to_publish(self, args):
        '''
        shows all files possible if we don't have checkpoint files.
        without temp files of course
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_outfiles_for_input(args)
        return [item for pair in [(XmlMultiStreamDump.get_multistream_dfname(inp_dfname),
                                   XmlMultiStreamDump.get_multistream_index_dfname(inp_dfname))
                                  for inp_dfname in input_dfnames] for item in pair]

    def list_truncated_empty_outfiles(self, args):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        but that might be empty or truncated
        only the parts we are actually supposed to do (if there is a limit)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompress.oflister.list_truncated_empty_outfiles_for_input(
            args)
        for inp_dfname in input_dfnames:
            if self.partnum_todo and inp_dfname.partnum_int != self.partnum_todo:
                continue
            dfnames.append(XmlMultiStreamDump.get_multistream_dfname(inp_dfname))
            dfnames.append(XmlMultiStreamDump.get_multistream_index_dfname(inp_dfname))
        return dfnames

    def list_outfiles_for_cleanup(self, args):
        '''
        shows all files possible if we don't have checkpoint files. should include temp files
        does just the parts we do if there is a limit
        expects: args.dump_dir, optional args.dump_names
        returns: list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        multistream_names = []
        for dname in args.dump_names:
            multistream_names.extend([XmlMultiStreamDump.get_dumpname_multistream(dname),
                                      XmlMultiStreamDump.get_dumpname_multistream_index(dname)])

        args = args._replace(parts=self.fileparts_list)
        args = args._replace(dump_names=multistream_names)
        dfnames = []
        if self.item_for_recompress.oflister.checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_input(self, args):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_outfiles_for_input(args)
        return [item for pair in [(XmlMultiStreamDump.get_multistream_dfname(inp_dfname),
                                   XmlMultiStreamDump.get_multistream_index_dfname(inp_dfname))
                                  for inp_dfname in input_dfnames] for item in pair]

    def list_truncated_empty_outfiles_for_input(self, args):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_truncated_empty_outfiles_for_input(
            args)
        return [item for pair in [(XmlMultiStreamDump.get_multistream_dfname(inp_dfname),
                                   XmlMultiStreamDump.get_multistream_index_dfname(inp_dfname))
                                  for inp_dfname in input_dfnames] for item in pair]


class XmlRecompressDump(RecompressDump):
    """Take a .bz2 and recompress it as 7-Zip."""

    def __init__(self, subset, name, desc, detail, item_for_recompress,
                 wiki, partnum_todo, pages_per_part=None, checkpoints=False, checkpoint_file=None):
        super().__init__(subset, name, desc, detail, item_for_recompress,
                         wiki, partnum_todo, pages_per_part, checkpoints, checkpoint_file)

        self.oflister = XmlRecompressFileLister(self.dumpname, self.file_type, self.file_ext,
                                                self.get_fileparts_list(), self.checkpoint_file,
                                                self._checkpoints_enabled, self.list_dumpnames,
                                                self._partnum_todo, self.item_for_recompress)

    def get_dumpname(self):
        return "pages-" + self._subset

    def get_file_ext(self):
        return "7z"

    def build_command(self, runner, output_dfnames):
        '''
        arguments:
        runner: Runner object
        output_dfnames: if checkpointing of files is enabled, this should be a
                        list of checkpoint files (DumpFilename), otherwise it
                        should be a list of the one file that will be produced
                        by the dump
        Note that checkpoint files get done one at a time, not in parallel
        '''
        # FIXME need shell escape
        if self.wiki.config.lbzip2threads:
            if not exists(self.wiki.config.lbzip2):
                raise BackupError("lbzip2 command %s not found" % self.wiki.config.lbzip2)
        elif not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.sevenzip):
            raise BackupError("7zip command %s not found" % self.wiki.config.sevenzip)

        command_series = []
        for out_dfname in output_dfnames:
            input_dfname = DumpFilename(self.wiki, None, out_dfname.dumpname, out_dfname.file_type,
                                        self.item_for_recompress.file_ext, out_dfname.partnum,
                                        out_dfname.checkpoint)
            outfilepath = runner.dump_dir.filename_public_path(out_dfname)
            infilepath = runner.dump_dir.filename_public_path(input_dfname)

            if self.wiki.config.lbzip2threads:
                # one thread only, as these already run in parallel
                decompr_command = "{lbzip2} -dc -n 1 {infile}".format(
                    lbzip2=self.wiki.config.lbzip2, infile=infilepath)
            else:
                decompr_command = "{bzip2} -dc {infile}".format(bzip2=self.wiki.config.bzip2,
                                                                infile=infilepath)
            command_pipe = [["{decompr} | {sevenzip} a -mx=4 -si {ofile}".format(
                decompr=decompr_command, sevenzip=self.wiki.config.sevenzip,
                ofile=DumpFilename.get_inprogress_name(outfilepath))]]
            command_series.append(command_pipe)
        return command_series

    def get_final_output_dfname(self, command_series, runner):
        """given a command series that produces one output file,
        return the dfname for the output file as given in the appropriate
        command_info element in self.commands_submitted, and without any
        INPROG marker etc. Returns None if none found"""
        for command_info in self.commands_submitted:
            if command_info['series'] == command_series:
                filenames = command_info['output_files']
        if len(filenames) != 1:
            return None
        # turn the one file into a dfname without INPROG marker and return it
        filename = filenames[0]
        if filename.endswith(DumpFilename.INPROG):
            filename = filename[:-1 * len(DumpFilename.INPROG)]
        dfname = DumpFilename(runner.wiki)
        dfname.new_from_filename(filename)
        return dfname

    def filter_commands(self, commands, runner):
        """for the every command series in the list,
        check that its expected final output file does not
        now exist, and if it does, assume it was produced
        somewhere else (a manual run?) and remove it from the
        list. Returns the filtered list, possibly empty."""
        commands_filtered = []
        for command_series in commands:
            # each series produces one output file only, and we want the name without INPROG markers
            final_output_dfname = self.get_final_output_dfname(command_series, runner)
            # if the file is already there, move on, don't rerun.
            if final_output_dfname is None or not exists(
                    os.path.join(runner.dump_dir.filename_public_path(final_output_dfname))):
                commands_filtered.append(command_series)
        return commands_filtered

    def get_command_batch(self, commands, runner):
        '''
        return a batch of commands, filtered so that any which
        produce an output file that already exists, are omitted;
        this prevents us from interfering with runs on another
        host or manual runs that we may not know about
        '''
        commands = self.filter_commands(commands, runner)
        batchsize = len(self._pages_per_part)
        commands_todo = commands[:batchsize]
        commands_left = commands[batchsize:]
        return (commands_todo, commands_left)

    def do_one_batch(self, batch, runner):
        '''
        run one batch of commands, whine about errors
        return True (success) if no errors, False otherwise
        if there are no commands, return True
        '''
        if not batch:
            return True

        error, broken = runner.run_command(
            batch, callback_timed=self.progress_callback,
            callback_timed_arg=runner, shell=True,
            callback_on_completion=self.command_completion_callback)
        if error:
            for series in broken:
                for pipeline in series:
                    runner.log_and_print("error from commands: %s" % " ".join(pipeline))
            return False
        return True

    def get_all_commands(self, runner):
        '''
        get and return all the commands to generate all the dump output files
        '''
        commands = []
        for partnum in range(1, len(self._pages_per_part) + 1):
            output_dfnames = self.oflister.list_outfiles_for_build_command(
                self.oflister.makeargs(runner.dump_dir, partnum=partnum))
            for output_dfname in output_dfnames:
                if not exists(runner.dump_dir.filename_public_path(output_dfname)):
                    series = self.build_command(runner, [output_dfname])
                    commands.append(series)
                    self.setup_command_info(runner, series, [output_dfname])
        return commands

    def run_in_batches(self, runner):
        """
        queue up a bunch of commands to compress files with part numbers
        and possibly also page ranges;
        run them in batches of no more than self._parts at once

        no auto-retry for these, if something went wrong we probably
        want human intervention
        """
        commands = self.get_all_commands(runner)
        errors = False
        commands_left = commands
        while commands_left:
            commands_todo, commands_left = self.get_command_batch(commands_left, runner)
            success = self.do_one_batch(commands_todo, runner)
            if not success:
                errors = True
        if errors:
            raise BackupError("error recompressing bz2 file(s) %s")

    def toss_inprog_files(self, dump_dir, runner):
        """
        delete partially written 7z files from previous failed attempts, if
        any; 7z will otherwise blithely append onto them
        """
        if self.checkpoint_file is not None:
            # we only rerun this one, so just remove this one
            if exists(dump_dir.filename_public_path(self.checkpoint_file)):
                if runner.dryrun:
                    print("would remove", dump_dir.filename_public_path(self.checkpoint_file))
                else:
                    os.remove(dump_dir.filename_public_path(self.checkpoint_file))

        dfnames = self.oflister.list_outfiles_for_cleanup(self.oflister.makeargs(dump_dir))
        if runner.dryrun:
            print("would remove ", [dfname.filename for dfname in dfnames])
        else:
            for dfname in dfnames:
                self.remove_output_file(dump_dir, dfname)

    def run(self, runner):
        commands = []
        # Remove prior 7zip attempts; 7zip will try to append to an existing archive
        self.toss_inprog_files(runner.dump_dir, runner)

        self.cleanup_old_files(runner.dump_dir, runner)
        if self.checkpoint_file is not None:
            output_dfname = DumpFilename(self.wiki, None, self.checkpoint_file.dumpname,
                                         self.checkpoint_file.file_type, self.file_ext,
                                         self.checkpoint_file.partnum,
                                         self.checkpoint_file.checkpoint)
            series = self.build_command(runner, [output_dfname])
            commands.append(series)
            self.setup_command_info(runner, series, [output_dfname])
        elif self._parts_enabled and not self._partnum_todo:
            self.run_in_batches(runner)
            return
        else:
            output_dfnames_possible = self.oflister.list_outfiles_for_build_command(
                self.oflister.makeargs(runner.dump_dir))
            output_dfnames = [name for name in output_dfnames_possible
                              if not exists(runner.dump_dir.filename_public_path(name))]
            series = self.build_command(runner, output_dfnames)
            commands.append(series)
            self.setup_command_info(runner, series, output_dfnames)

        error, _broken = runner.run_command(commands, callback_timed=self.progress_callback,
                                            callback_timed_arg=runner, shell=True,
                                            callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error recompressing bz2 file(s)")


class XmlRecompressFileLister(RecompressFileLister):
    """
    special methods for recompression of page content jobs to 7z

    many fields used here come from the job that produces the page
    content files used as input for recompression
    """
    def __init__(self, dumpname, file_type, file_ext, fileparts_list,
                 checkpoint_file, checkpoints_enabled, list_dumpnames=None,
                 partnum_todo=None, item_for_recompress=None):
        super().__init__(dumpname, file_type, file_ext, fileparts_list,
                         checkpoint_file, checkpoints_enabled, list_dumpnames)
        self.partnum_todo = partnum_todo
        self.item_for_recompress = item_for_recompress

    def list_outfiles_to_publish(self, args):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_outfiles_for_input(args)
        return [DumpFilename(inp_dfname.wiki, inp_dfname.date, inp_dfname.dumpname,
                             inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                             inp_dfname.checkpoint, inp_dfname.temp)
                for inp_dfname in input_dfnames]

    def list_truncated_empty_outfiles(self, args):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        which would be truncated or empty
        only the parts we are actually supposed to do (if there is a limit)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompress.oflister.list_truncated_empty_outfiles_for_input(
            args)
        for inp_dfname in input_dfnames:
            if self.partnum_todo and inp_dfname.partnum_int != self.partnum_todo:
                continue
            dfnames.append(DumpFilename(inp_dfname.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_outfiles_for_cleanup(self, args):
        '''
        shows all files possible if we don't have checkpoint files. should include temp files
        does just the parts we do if there is a limit
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        args = args._replace(parts=self.fileparts_list)
        dfnames = []
        if self.item_for_recompress.oflister.checkpoints_enabled:
            args = args._replace(inprog=True)
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
            args = args._replace(inprog=False)
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            args = args._replace(inprog=True)
            dfnames.extend(self.list_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_input(self, args):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_outfiles_for_input(args)
        return [DumpFilename(inp_dfname.wiki, inp_dfname.date, inp_dfname.dumpname,
                             inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                             inp_dfname.checkpoint, inp_dfname.temp)
                for inp_dfname in input_dfnames]

    def list_truncated_empty_outfiles_for_input(self, args):
        '''
        must return all output files that could be produced by a full run of this stage,
        that are truncated or empty
        not just whatever we happened to produce (if run for one file part, say)
        expects: args.dump_dir
        returns: list of DumpFilename
        '''
        input_dfnames = self.item_for_recompress.oflister.list_truncated_empty_outfiles_for_input(
            args)
        return [DumpFilename(inp_dfname.wiki, inp_dfname.date, inp_dfname.dumpname,
                             inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                             inp_dfname.checkpoint, inp_dfname.temp)
                for inp_dfname in input_dfnames]
