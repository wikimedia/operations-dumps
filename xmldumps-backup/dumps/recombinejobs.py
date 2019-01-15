#!/usr/bin/python3
'''
All dump jobs that recombine output from other
dump jobs are defined here
'''

import os
from os.path import exists
import signal
from dumps.exceptions import BackupError
from dumps.jobs import Dump
from dumps.fileutils import DumpFilename
from dumps.commandmanagement import CommandPipeline


class RecombineDump(Dump):
    GZIPMARKER = b'\x1f\x8b\x08\x00'

    @staticmethod
    def get_file_size(filename):
        try:
            filesize = os.stat(filename).st_size
        except Exception:
            return None
        return filesize

    def get_header_offset(self, filename):
        with open(filename, "rb") as infile:
            # skip the first byte
            try:
                infile.seek(1, os.SEEK_SET)
                max_offset = 1000000
                buffer = infile.read(max_offset)
            except IOError:
                return None
            buffer_offset = buffer.find(self.GZIPMARKER)
            if buffer_offset >= 0:
                # because we skipped the first byte, add that here
                return buffer_offset + 1
        return None

    def get_footer_offset(self, filename):
        with open(filename, "rb") as infile:
            # empty files or files with only a footer will return None
            # here (too short) and that's ok, we might as well fail out on them
            # by now they should have already been moved out of the way
            # by the previous job but, just in case...
            max_offset = 100
            try:
                filesize = infile.seek(0, os.SEEK_END)
                infile.seek(filesize - max_offset, os.SEEK_SET)
                buffer = infile.read()
            except IOError:
                return None
            buffer_offset = buffer.find(self.GZIPMARKER)
            if buffer_offset >= 0:
                return filesize - (len(buffer) - buffer_offset)
        return None

    @staticmethod
    def get_dd_command(runner, filename, outfile, header_offset, footer_offset):
        # return it as a CommandPipeline with one command in it
        return [[runner.wiki.config.dd, 'if=' + filename, 'of=' + outfile,
                 'skip=' + str(header_offset),
                 'count=' + str(footer_offset - header_offset),
                 'iflag=skip_bytes,count_bytes',
                 'oflag=append',
                 'conv=notrunc', 'bs=256k']]

    def get_dump_body_command(self, runner, filename, outfile):
        header_offset = self.get_header_offset(filename)
        footer_offset = self.get_footer_offset(filename)
        return self.get_dd_command(runner, filename, outfile, header_offset, footer_offset)

    def get_dump_header_command(self, runner, filename, outfile):
        header_offset = self.get_header_offset(filename)
        return self.get_dd_command(runner, filename, outfile, 0, header_offset)

    def get_dump_footer_command(self, runner, filename, outfile):
        footer_offset = self.get_footer_offset(filename)
        infile_size = self.get_file_size(filename)
        return self.get_dd_command(runner, filename, outfile, footer_offset, infile_size)

    def build_recombine_command_string(self, runner, dfnames, output_file, compression_command,
                                       uncompression_command, end_header_marker="</siteinfo>"):
        """
        args:
            Runner, list of DumpFilename, ...
        """
        if runner.wiki.is_private():
            output_filename = runner.dump_dir.filename_private_path(output_file)
        else:
            output_filename = runner.dump_dir.filename_public_path(output_file)
        partnum = 0
        recombines = []
        for utility in [runner.wiki.config.head, runner.wiki.config.tail, runner.wiki.config.grep]:
            if not exists(utility):
                raise BackupError("command %s not found" % utility)
        head = runner.wiki.config.head
        tail = runner.wiki.config.tail
        grep = runner.wiki.config.grep

        if not dfnames:
            raise BackupError("No files for the recombine step found in %s." % self.name())

        for dfname in dfnames:
            if runner.wiki.is_private():
                fpath = runner.dump_dir.filename_private_path(dfname)
            else:
                fpath = runner.dump_dir.filename_public_path(dfname)
            partnum = partnum + 1
            pipeline = []
            uncompression_todo = uncompression_command + [fpath]
            pipeline.append(uncompression_todo)
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
                     [[-signal.SIGPIPE, uncompression_todo]] or
                     proc.get_failed_cmds_with_retcode() ==
                     [[signal.SIGPIPE + 128, uncompression_todo]])):
                (header_end_num, _junk) = proc.output().decode('utf-8').split(":", 1)
                # get header_end_num
            else:
                raise BackupError("Could not find 'end of header' marker for %s" % fpath)
            recombine = " ".join(uncompression_todo)
            header_end_num = int(header_end_num) + 1
            if partnum == 1:
                # first file, put header and contents
                recombine = recombine + " | %s -n -1 " % head
            elif partnum == len(dfnames):
                # last file, put footer
                recombine = recombine + (" | %s -n +%s" % (tail, header_end_num))
            else:
                # put contents only
                recombine = recombine + (" | %s -n +%s" % (tail, header_end_num))
                recombine = recombine + " | %s -n -1 " % head
            recombines.append(recombine)
        recombine_command_string = ("(" + ";".join(recombines) + ")" + "|" +
                                    "%s %s" % (compression_command,
                                               DumpFilename.get_inprogress_name(output_filename)))
        return recombine_command_string

    def build_command_for_dd(self, runner, dfnames, output_dfname):
        """
        args:
            Runner, list of DumpFilename, ...
        """
        if runner.wiki.is_private():
            output_filename = runner.dump_dir.filename_private_path(output_dfname)
        else:
            output_filename = runner.dump_dir.filename_public_path(output_dfname)
        partnum = 0

        if not dfnames:
            raise BackupError("No files for the recombine step found in %s." % self.name())

        if not exists(runner.wiki.config.dd):
            raise BackupError("dd command %s not found" %
                              runner.wiki.config.dd)

        outpath_inprog = DumpFilename.get_inprogress_name(output_filename)

        series = []
        for dfname in dfnames:
            if runner.wiki.is_private():
                fpath = runner.dump_dir.filename_private_path(dfname)
            else:
                fpath = runner.dump_dir.filename_public_path(dfname)
            partnum = partnum + 1
            if partnum == 1:
                # first file, put header, body
                series.append(self.get_dump_header_command(runner, fpath, outpath_inprog))
                series.append(self.get_dump_body_command(runner, fpath, outpath_inprog))
            elif partnum == len(dfnames):
                # last file, put body, footer
                series.append(self.get_dump_body_command(runner, fpath, outpath_inprog))
                series.append(self.get_dump_footer_command(runner, fpath, outpath_inprog))
            else:
                # put contents only
                series.append(self.get_dump_body_command(runner, fpath, outpath_inprog))
        return series

    def dd_recombine(self, runner, dfnames, output_dfnames, dumptype):
        error = 0
        for output_dfname in output_dfnames:
            input_dfnames = []
            for in_dfname in dfnames:
                if in_dfname.dumpname == output_dfname.dumpname:
                    input_dfnames.append(in_dfname)
            if not input_dfnames:
                self.set_status("failed")
                raise BackupError("No input files for %s found" % self.name())
            command_series = self.build_command_for_dd(runner, input_dfnames, output_dfname)

            self.setup_command_info(runner, command_series, [output_dfname])
            result, _broken = runner.run_command(
                [command_series], callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=False,
                callback_on_completion=self.command_completion_callback)
            if result:
                error = result
        if error:
            raise BackupError("error recombining {dumptype} files".format(dumptype=dumptype))


class RecombineXmlStub(RecombineDump):
    def __init__(self, name, desc, item_for_xml_stubs):
        self.item_for_xml_stubs = item_for_xml_stubs
        self._prerequisite_items = [self.item_for_xml_stubs]
        super().__init__(name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def list_dumpnames(self):
        return self.item_for_xml_stubs.list_dumpnames()

    def list_outfiles_to_publish(self, dump_dir):
        """
        returns:
            list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns:
            list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return dfnames

    def get_filetype(self):
        return self.item_for_xml_stubs.get_filetype()

    def get_file_ext(self):
        return self.item_for_xml_stubs.get_file_ext()

    def get_dumpname(self):
        return self.item_for_xml_stubs.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_xml_stubs.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(
            runner.dump_dir, self.list_dumpnames())
        self.dd_recombine(runner, dfnames, output_dfnames, 'stubs')


class RecombineXmlDump(RecombineDump):
    def __init__(self, name, desc, detail, item_for_xml_dumps):
        # no prefetch, no spawn
        self.item_for_xml_dumps = item_for_xml_dumps
        self._detail = detail
        self._prerequisite_items = [self.item_for_xml_dumps]
        super().__init__(name, desc)
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

    def build_command(self, runner, input_dfnames, output_dfname):
        if runner.wiki.config.lbzip2threads:
            if not exists(runner.wiki.config.lbzip2):
                raise BackupError("lbzip2 command %s not found" %
                                  runner.wiki.config.lbzip2)
            compression_command = "{lbzip2} -n {threads} > ".format(
                lbzip2=runner.wiki.config.lbzip2, threads=runner.wiki.config.lbzip2threads)
        else:
            if not exists(runner.wiki.config.bzip2):
                raise BackupError("bzip2 command %s not found" %
                                  runner.wiki.config.bzip2)
            compression_command = "{bzip2} > ".format(bzip2=runner.wiki.config.bzip2)
        uncompression_command = [runner.wiki.config.bzip2, "-dc"]
        recombine_command_string = self.build_recombine_command_string(
            runner, input_dfnames, output_dfname, compression_command, uncompression_command)
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        return series

    def run(self, runner):
        input_dfnames = self.item_for_xml_dumps.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(output_dfnames) > 1:
            raise BackupError("recombine XML Dump trying to "
                              "produce more than one output file")

        command_series = self.build_command(runner, input_dfnames, output_dfnames[0])
        self.setup_command_info(runner, command_series, [output_dfnames[0]])

        error = 0
        error, _broken = runner.run_command(
            [command_series], callback_timed=self.progress_callback,
            callback_timed_arg=runner, shell=True,
            callback_on_completion=self.command_completion_callback)

        if error:
            raise BackupError("error recombining xml bz2 files")


class RecombineXmlRecompressDump(RecombineDump):
    def __init__(self, name, desc, detail, item_for_recombine, wiki):
        self._detail = detail
        self._desc = desc
        self.wiki = wiki
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False
        self._parts_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def build_command(self, runner, output_dfname):
        input_dfnames = []
        dfnames = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        for in_dfname in dfnames:
            if in_dfname.dumpname == output_dfname.dumpname:
                input_dfnames.append(in_dfname)
        if not input_dfnames:
            self.set_status("failed")
            raise BackupError("No input files for %s found" % self.name())
        if not exists(self.wiki.config.sevenzip):
            raise BackupError("sevenzip command %s not found" % self.wiki.config.sevenzip)
        compression_command = "%s a -mx=4 -si" % self.wiki.config.sevenzip
        uncompression_command = ["%s" % self.wiki.config.sevenzip, "e", "-so"]

        recombine_command_string = self.build_recombine_command_string(
            runner, dfnames, output_dfname, compression_command, uncompression_command)
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        return series

    def run(self, runner):
        error = 0
        self.cleanup_old_files(runner.dump_dir, runner)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_dfname in output_dfnames:
            command_series = self.build_command(runner, output_dfname)
            self.setup_command_info(runner, command_series, [output_dfname])
            result, _broken = runner.run_command(
                [command_series], callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=True,
                callback_on_completion=self.command_completion_callback)
            if result:
                error = result
        if error:
            raise BackupError("error recombining xml bz2 file(s)")


class RecombineAbstractDump(RecombineDump):
    def __init__(self, name, desc, item_for_recombine):
        # no partnum_todo, no parts generally (False, False), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        self.dd_recombine(runner, dfnames, output_dfnames, 'abstract')


class RecombineXmlLoggingDump(RecombineDump):
    def __init__(self, name, desc, item_for_recombine):
        # no partnum_todo, no parts generally (False, False), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc)

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        self.dd_recombine(runner, dfnames, output_dfnames, 'log event')
