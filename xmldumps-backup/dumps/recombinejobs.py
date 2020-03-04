#!/usr/bin/python3
'''
All dump jobs that recombine output from other
dump jobs are defined here
'''

import os
from os.path import exists
import bz2
from dumps.exceptions import BackupError
from dumps.jobs import Dump
from dumps.fileutils import DumpFilename
from dumps.commandmanagement import CommandPipeline
from dumps.utils import MiscUtils
from dumps.outfilelister import OutputFileLister


class RecombineDump(Dump):
    GZIPMARKER = b'\x1f\x8b\x08\x00'
    BZIP2MARKER = b'\x42\x5a\x68\x39\x31\x41\x59\x26\x53\x59'

    def __init__(self, name, desc, compresstype=None):
        if compresstype == 'gz':
            self.marker = self.GZIPMARKER
        elif compresstype == 'bz2':
            self.marker = self.BZIP2MARKER
        else:
            self.marker = None
        super().__init__(name, desc)

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
            buffer_offset = buffer.find(self.marker)
            if buffer_offset >= 0:
                # because we skipped the first byte, add that here
                return buffer_offset + 1
        return None

    def get_footer_offset(self, filename):
        with open(filename, "rb") as infile:
            max_offset = 100
            try:
                filesize = infile.seek(0, os.SEEK_END)
                infile.seek(filesize - max_offset, os.SEEK_SET)
                buffer = infile.read()
            except IOError:
                try:
                    # files with empty feeds are ok because we may have wikis where all the
                    # content in the desired namespace is unabstractable and so
                    # empty abstract files are written due to configuration.
                    # completely empty files (0 bytes) are not ok, these should have
                    # been moved out of the way already by the previous job but eh.
                    filesize = infile.seek(0, os.SEEK_END)
                    infile.seek(40, os.SEEK_SET)
                    buffer = infile.read()
                except IOError:
                    return None
            buffer_offset = buffer.find(self.marker)
            if buffer_offset >= 0:
                return filesize - (len(buffer) - buffer_offset)
        return None

    @staticmethod
    def get_dd_command(runner, filename, outfile, header_offset, footer_offset):
        # return it as a CommandPipeline with one command in it
        return [[runner.wiki.config.ddpath, 'if=' + filename, 'of=' + outfile,
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
                     proc.get_failed_cmds_with_retcode() in [
                         [[pipevalue, uncompression_todo]]
                         for pipevalue in MiscUtils.get_sigpipe_values()]
                     )):
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

        if not exists(runner.wiki.config.ddpath):
            raise BackupError("dd command %s not found" %
                              runner.wiki.config.ddpath)

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
                body_command = self.get_dump_body_command(runner, fpath, outpath_inprog)
                if body_command:
                    series.append(body_command)
            elif partnum == len(dfnames):
                # last file, put body, footer
                body_command = self.get_dump_body_command(runner, fpath, outpath_inprog)
                if body_command:
                    series.append(body_command)
                series.append(self.get_dump_footer_command(runner, fpath, outpath_inprog))
            else:
                # put contents only
                body_command = self.get_dump_body_command(runner, fpath, outpath_inprog)
                if body_command:
                    series.append(body_command)
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
        super().__init__(name, desc, 'gz')
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False
        self.oflister = RecombineXmlStubFileLister(self.dumpname, self.file_type, self.file_ext,
                                                   self.get_fileparts_list(), self.checkpoint_file,
                                                   self._checkpoints_enabled, self.list_dumpnames)

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def list_dumpnames(self):
        return self.item_for_xml_stubs.list_dumpnames()

    def get_filetype(self):
        return self.item_for_xml_stubs.get_filetype()

    def get_file_ext(self):
        return self.item_for_xml_stubs.get_file_ext()

    def get_dumpname(self):
        return self.item_for_xml_stubs.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_xml_stubs.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
        output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir, self.list_dumpnames()))
        self.dd_recombine(runner, dfnames, output_dfnames, 'stubs')


class RecombineXmlStubFileLister(OutputFileLister):
    """
    special methods for output file listings for recombining xml stubs

    the stubs files have multiple basenames (dump names) so those
    must be accounted for here
    """
    def list_outfiles_to_publish(self, args):
        """
        expects:
            dump_dir
        returns:
            list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        dfnames = []
        dfnames.extend(super().list_outfiles_to_publish(args))
        return dfnames


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
        input_dfnames = self.item_for_xml_dumps.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
        output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
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
        dfnames = self.item_for_recombine.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
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
        output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
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
        # no partnum_todo, no parts generally (False, None), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc, 'gz')
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_recombine.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
        output_dfnames = self.oflister.list_outfiles_for_build_command(self.oflister.makeargs(
            runner.dump_dir))
        self.dd_recombine(runner, dfnames, output_dfnames, 'abstract')


class RecombineXmlLoggingDump(RecombineDump):
    def __init__(self, name, desc, item_for_recombine):
        # no partnum_todo, no parts generally (False, None), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc, 'gz')

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        dfnames = self.item_for_recombine.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
        output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        self.dd_recombine(runner, dfnames, output_dfnames, 'log event')


class RecombineXmlMultiStreamDump(RecombineDump):
    INDEX_FILETYPE = "txt"

    def __init__(self, name, desc, item_for_recombine):
        # no partnum_todo, no parts generally (False, None), even though input may have it
        self.item_for_recombine = item_for_recombine
        self._prerequisite_items = [self.item_for_recombine]
        super().__init__(name, desc, 'bz2')

    @staticmethod
    def get_dumpname_multistream(name):
        return name + "-multistream"

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def get_dumpname_multistream_index(self, name):
        return self.get_dumpname_multistream(name) + "-index"

    def get_filepath(self, runner, dfname):
        if runner.wiki.is_private():
            return runner.dump_dir.filename_private_path(dfname)
        return runner.dump_dir.filename_public_path(dfname)

    def get_content_dfname_from_index(self, runner, index_dfname):
        '''
        given a multistream index dfname, return the corresponding
        multistream content dfname
        '''
        content_dfname = DumpFilename(
            runner.wiki, index_dfname.date,
            self.get_dumpname_multistream(self.get_dumpname()),
            'xml', index_dfname.file_ext,
            index_dfname.partnum, index_dfname.checkpoint,
            index_dfname.temp)
        return content_dfname

    def get_new_offset(self, runner, input_dfname, offset):
        footer_marker = self.get_footer_offset(self.get_filepath(runner, input_dfname))
        header_marker = self.get_header_offset(self.get_filepath(runner, input_dfname))
        body_size = footer_marker - header_marker
        # offset in index file is relative to the specific file and includes its header
        # we are modifying it. we must add the relative amount from the previous files
        # (first header, all bodies)
        # plus the current offset - the current file's header
        # the filter (mawk) command just adds something to the offset listed in that index file
        return offset + body_size

    def do_one_indexfile_recombine(self, runner, input_dfnames, output_dfname):
        '''
        recombine index files to produce the specified output file
        with the combined index file having the correct offsets into the
        combined content file that is produced separately

        we do this without the usual progress callback that shows the file
        size as it grows, because a) it's easier and b) as of this writing
        it takes all of 2 minutes to write the combined index file so who cares.

        return False on error, True otherwise
        '''
        if not exists(runner.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % runner.wiki.config.bzip2)

        # initially the offset into the combined page content file is 0 plus whatever
        # the first index file says, for any page; this will change as we move into the
        # part of the combined page content file that has the contents of the second
        # page content part and the corresponding index file that has offsets starting
        # again from 0, etc.
        offset = 0

        first_content_dfname = self.get_content_dfname_from_index(runner, input_dfnames[0])
        first_header_size = self.get_header_offset(self.get_filepath(runner, first_content_dfname))
        output_prog_path = DumpFilename.get_inprogress_name(
            self.get_filepath(runner, output_dfname))
        combined_indexfile_inprog = bz2.open(output_prog_path, 'wt', encoding='utf-8')

        for infile_counter, input_dfname in enumerate(input_dfnames):
            content_dfname = self.get_content_dfname_from_index(runner, input_dfname)
            if infile_counter == 1:
                offset += first_header_size
            if infile_counter:
                header_size = self.get_header_offset(self.get_filepath(runner, content_dfname))
            else:
                # include the header count from the first file, it gets written
                header_size = 0
            input_path = self.get_filepath(runner, input_dfname)
            with bz2.open(input_path, mode='rt', encoding='utf-8') as partial_indexfile:
                added_offset = offset - header_size
                for line in partial_indexfile:
                    if line:
                        partial_offset, title = line.split(':', 1)
                        partial_offset = str(int(partial_offset) + added_offset)
                        # title will still have the newline on the end of it
                        combined_indexfile_inprog.write(partial_offset + ":" + title)
            offset = self.get_new_offset(runner, content_dfname, offset)
        combined_indexfile_inprog.close()
        os.rename(output_prog_path, self.get_filepath(runner, output_dfname))
        if self.move_if_truncated(runner, output_dfname):
            return False
        return True

    def index_files_recombine(self, runner, dfnames, output_dfnames):
        '''
        recombine index files to produce specified output files
        with the combined index file having the correct offsets into the
        combined content file that is produced separately
        '''
        error = False
        for output_dfname in output_dfnames:
            input_dfnames = []
            if 'index' in output_dfname.filename:
                for in_dfname in dfnames:
                    if in_dfname.dumpname == output_dfname.dumpname:
                        input_dfnames.append(in_dfname)
                if not input_dfnames:
                    self.set_status("failed")
                    raise BackupError("No input files for %s found" % self.name())
            if not self.do_one_indexfile_recombine(runner, input_dfnames, output_dfname):
                error = True
        if error:
            raise BackupError("error recombining multistream files")

    def run(self, runner):
        dfnames = self.item_for_recombine.oflister.list_outfiles_for_input(
            self.oflister.makeargs(runner.dump_dir))
        content_dfnames = [dfname for dfname in dfnames if 'index' not in dfname.filename]
        index_dfnames = [dfname for dfname in dfnames if 'index' in dfname.filename]
        content_output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir, [self.get_dumpname_multistream(self.dumpname)]))

        # FIXME EWWW
        self.oflister.file_type = self.INDEX_FILETYPE
        index_output_dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir,
                                   [self.get_dumpname_multistream_index(self.dumpname)]))
        self.oflister.file_type = self.get_filetype()

        self.dd_recombine(runner, content_dfnames, content_output_dfnames, 'multistream')
        self.index_files_recombine(runner, index_dfnames, index_output_dfnames)


class RecombineXmlMultiStreamFileLister(OutputFileLister):
    """
    special methods for recombining xml multistream dumps

    we produce the content file plus an index into the content,
    so this requires special handling.
    """

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
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(args))
        return dfnames

    def list_outfiles_to_publish(self, args):
        """
        expects:
            dump_dir
        returns:
            list of DumpFilename
        """
        dfnames = []
        args = args._replace(dump_names=[RecombineXmlMultiStreamDump.get_dumpname_multistream(
            self.dumpname)])
        dfnames.extend(super().list_outfiles_to_publish(args))
        # FIXME EWWW
        real_filetype = self.file_type
        self.file_type = RecombineXmlMultiStreamDump.INDEX_FILETYPE
        dfnames.extend(super().list_outfiles_to_publish(args))
        self.file_type = real_filetype

        return dfnames
