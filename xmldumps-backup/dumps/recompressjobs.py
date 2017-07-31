'''
All dump jobs that recompress the output
from other dump jobs are defined here
'''

from os.path import exists
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump


class XmlMultiStreamDump(Dump):
    """Take a .bz2 and recompress it as multistream bz2, 100 pages per stream."""

    def __init__(self, subset, name, desc, detail, item_for_recompression,
                 wiki, partnum_todo, parts=False, checkpoints=False, checkpoint_file=None):
        self._subset = subset
        self._detail = detail
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
        self._partnum_todo = partnum_todo
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
        dname = self.get_dumpname()
        return [self.get_dumpname_multistream(dname),
                self.get_dumpname_multistream_index(dname)]

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

    def get_multistream_dfname(self, dfname, suffix=None):
        """
        assuming that dfname is an input file,
        return the name of the associated multistream output file
        args:
            DumpFilename
        returns:
            DumpFilename
        """
        if suffix is not None:
            file_ext = self.file_ext + suffix
        else:
            file_ext = self.file_ext
        return DumpFilename(self.wiki, dfname.date,
                            self.get_dumpname_multistream(dfname.dumpname),
                            dfname.file_type, file_ext, dfname.partnum,
                            dfname.checkpoint, dfname.temp)

    def get_multistream_index_dfname(self, dfname):
        """
        assuming that dfname is a multistream output file,
        return the name of the associated index file
        args:
            DumpFilename
        returns:
            DumpFilename
        """
        return DumpFilename(self.wiki, dfname.date,
                            self.get_dumpname_multistream_index(dfname.dumpname),
                            self.get_index_filetype(), self.file_ext, dfname.partnum,
                            dfname.checkpoint, dfname.temp)

    def build_command(self, runner, output_dfname):
        '''
        arguments:
        runner: Runner object
        output_dfname: output file that will be produced, whether one of
                       a series of checkpoint files or a regular file
        '''

        input_dfname = DumpFilename(self.wiki, None, output_dfname.dumpname,
                                    output_dfname.file_type,
                                    self.item_for_recompression.file_ext,
                                    output_dfname.partnum, output_dfname.checkpoint)
        if runner.wiki.is_private():
            outfilepath = runner.dump_dir.filename_private_path(
                self.get_multistream_dfname(output_dfname))
            outfilepath_index = runner.dump_dir.filename_private_path(
                self.get_multistream_index_dfname(output_dfname))
            infilepath = runner.dump_dir.filename_private_path(input_dfname)
        else:
            outfilepath = runner.dump_dir.filename_public_path(
                self.get_multistream_dfname(output_dfname))
            outfilepath_index = runner.dump_dir.filename_public_path(
                self.get_multistream_index_dfname(output_dfname))
            infilepath = runner.dump_dir.filename_public_path(input_dfname)
        command_pipe = [["%s -dc %s | %s --pagesperstream 100 --buildindex %s > %s" %
                         (self.wiki.config.bzip2, infilepath, self.wiki.config.recompressxml,
                          self.get_inprogress_name(outfilepath_index),
                          self.get_inprogress_name(outfilepath))]]
        return [command_pipe]

    def run(self, runner):
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.recompressxml):
            raise BackupError("recompressxml command %s not found" %
                              self.wiki.config.recompressxml)

        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        if self.checkpoint_file is not None:
            output_dfname = DumpFilename(self.wiki, None, self.checkpoint_file.dumpname,
                                         self.checkpoint_file.file_type, self.file_ext,
                                         self.checkpoint_file.partnum,
                                         self.checkpoint_file.checkpoint)
            command_series = self.build_command(runner, output_dfname)
            commands.append(command_series)
            self.setup_command_info(runner, command_series, [output_dfname])
        elif self._parts_enabled and not self._partnum_todo:
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for partnum in range(1, len(self._parts) + 1):
                output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir, partnum)
                command_series_for_part = []
                for output_dfname in output_dfnames:
                    command_series = self.build_command(runner, output_dfnames)
                    command_series_for_part.extend(command_series)
                commands.append(command_series_for_part)
                # this means that only when the whole series is complete do we get to
                # post-command-completion callbacks and such, bad news for checking
                # and making available new file content
                self.setup_command_info(runner, command_series_for_part, output_dfnames)
        else:
            output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
            for output_dfname in output_dfnames:
                command_series = self.build_command(runner, output_dfname)
                self.setup_command_info(runner, command_series, [output_dfname])
                commands.append(command_series)

        error, broken = runner.run_command(commands, callback_timed=self.progress_callback,
                                           callback_timed_arg=runner, shell=True,
                                           callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    def list_outfiles_to_publish(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files.
        without temp files of course
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(self.get_multistream_dfname(inp_dfname))
            dfnames.append(self.get_multistream_index_dfname(inp_dfname))
        return dfnames

    def list_truncated_empty_outfiles(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        but that might be empty or truncated
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_truncated_empty_outfiles_for_input(
            dump_dir)
        for inp_dfname in input_dfnames:
            if self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            dfnames.append(self.get_multistream_dfname(inp_dfname))
            dfnames.append(self.get_multistream_index_dfname(inp_dfname))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course,
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            if self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            dfnames.append(self.get_multistream_dfname(inp_dfname))
            dfnames.append(self.get_multistream_index_dfname(inp_dfname))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir, partnum=None):
        '''
        shows all files possible if we don't have checkpoint files. no temp files.
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            # if this param is set it takes priority
            if partnum and inp_dfname.partnum_int != partnum:
                continue
            elif self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            # we don't convert these names to the final output form,
            # we'll do that in the build command
            # (i.e. add "multistream" and "index" to them)
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext,
                                        inp_dfname.partnum, inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        '''
        shows all files possible if we don't have checkpoint files. should include temp files
        does just the parts we do if there is a limit
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        multistream_names = []
        for dname in dump_names:
            multistream_names.extend([self.get_dumpname_multistream(dname),
                                      self.get_dumpname_multistream_index(dname)])

        dfnames = []
        if self.item_for_recompression._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), multistream_names))
            dfnames.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), multistream_names))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), multistream_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(self.get_multistream_dfname(inp_dfname))
            dfnames.append(self.get_multistream_index_dfname(inp_dfname))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, dump_dir):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_truncated_empty_outfiles_for_input(
            dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(self.get_multistream_dfname(inp_dfname))
            dfnames.append(self.get_multistream_index_dfname(inp_dfname))
        return dfnames


class XmlRecompressDump(Dump):
    """Take a .bz2 and recompress it as 7-Zip."""

    def __init__(self, subset, name, desc, detail, item_for_recompression, wiki,
                 partnum_todo, parts=False, checkpoints=False, checkpoint_file=None):
        self._subset = subset
        self._detail = detail
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
        self._partnum_todo = partnum_todo
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
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.sevenzip):
            raise BackupError("7zip command %s not found" % self.wiki.config.sevenzip)

        command_series = []
        for out_dfname in output_dfnames:
            input_dfname = DumpFilename(self.wiki, None, out_dfname.dumpname, out_dfname.file_type,
                                        self.item_for_recompression.file_ext, out_dfname.partnum,
                                        out_dfname.checkpoint)
            if runner.wiki.is_private():
                outfilepath = runner.dump_dir.filename_private_path(out_dfname)
                infilepath = runner.dump_dir.filename_private_path(input_dfname)
            else:
                outfilepath = runner.dump_dir.filename_public_path(out_dfname)
                infilepath = runner.dump_dir.filename_public_path(input_dfname)
            command_pipe = [["%s -dc %s | %s a -mx=4 -si %s" %
                             (self.wiki.config.bzip2, infilepath,
                              self.wiki.config.sevenzip,
                              self.get_inprogress_name(outfilepath))]]
            command_series.append(command_pipe)
        return command_series

    def run(self, runner):
        commands = []
        # Remove prior 7zip attempts; 7zip will try to append to an existing archive
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
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for partnum in range(1, len(self._parts) + 1):
                output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir, partnum)
                series = self.build_command(runner, output_dfnames)
                commands.append(series)
                self.setup_command_info(runner, series, output_dfnames)
        else:
            output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
            series = self.build_command(runner, output_dfnames)
            commands.append(series)
            self.setup_command_info(runner, series, output_dfnames)

        error, broken = runner.run_command(commands, callback_timed=self.progress_callback,
                                           callback_timed_arg=runner, shell=True,
                                           callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    def list_outfiles_to_publish(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_truncated_empty_outfiles(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        which would be truncated or empty
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_truncated_empty_outfiles_for_input(
            dump_dir)
        for inp_dfname in input_dfnames:
            if self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        '''
        shows all files possible if we don't have checkpoint files. without temp files of course
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            if self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir, partnum=None):
        '''
        shows all files possible if we don't have checkpoint files. no temp files.
        only the parts we are actually supposed to do (if there is a limit)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            # if this param is set it takes priority
            if partnum and inp_dfname.partnum_int != partnum:
                continue
            elif self._partnum_todo and inp_dfname.partnum_int != self._partnum_todo:
                continue
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        '''
        shows all files possible if we don't have checkpoint files. should include temp files
        does just the parts we do if there is a limit
        returns:
            list of DumpFilename
        '''
        if dump_names is None:
            dump_names = [self.dumpname]
        dfnames = []
        if self.item_for_recompression._checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
            dfnames.extend(self.list_temp_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(
                dump_dir, self.get_fileparts_list(), dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir):
        '''
        must return all output files that could be produced by a full run of this stage,
        not just whatever we happened to produce (if run for one file part, say)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_outfiles_for_input(dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, dump_dir):
        '''
        must return all output files that could be produced by a full run of this stage,
        that are truncated or empty
        not just whatever we happened to produce (if run for one file part, say)
        returns:
            list of DumpFilename
        '''
        dfnames = []
        input_dfnames = self.item_for_recompression.list_truncated_empty_outfiles_for_input(
            dump_dir)
        for inp_dfname in input_dfnames:
            dfnames.append(DumpFilename(self.wiki, inp_dfname.date, inp_dfname.dumpname,
                                        inp_dfname.file_type, self.file_ext, inp_dfname.partnum,
                                        inp_dfname.checkpoint, inp_dfname.temp))
        return dfnames
