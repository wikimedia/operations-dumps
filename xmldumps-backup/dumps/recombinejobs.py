'''
All dump jobs that recombine output from other
dump jobs are defined here
'''

from os.path import exists
import signal
from dumps.exceptions import BackupError
from dumps.jobs import Dump
from dumps.CommandManagement import CommandPipeline


class RecombineDump(Dump):
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
                (header_end_num, junk_unused) = proc.output().split(":", 1)
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
                                               self.get_inprogress_name(output_filename)))
        return recombine_command_string


class RecombineXmlStub(RecombineDump):
    def __init__(self, name, desc, item_for_xml_stubs):
        self.item_for_xml_stubs = item_for_xml_stubs
        self._prerequisite_items = [self.item_for_xml_stubs]
        super(RecombineXmlStub, self).__init__(name, desc)
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

    def build_command(self, runner, dfnames, output_dfname):
        input_dfnames = []
        for in_dfname in dfnames:
            if in_dfname.dumpname == output_dfname.dumpname:
                input_dfnames.append(in_dfname)
        if not len(input_dfnames):
            self.set_status("failed")
            raise BackupError("No input files for %s found" % self.name())
        if not exists(runner.wiki.config.cat):
            raise BackupError("cat command %s not found" % runner.wiki.config.cat)
        compression_command = runner.wiki.config.cat
        compression_command = "%s > " % runner.wiki.config.cat
        uncompression_command = ["%s" % runner.wiki.config.cat]
        recombine_command_string = self.build_recombine_command_string(
            runner, input_dfnames, output_dfname, compression_command, uncompression_command)
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        return series

    def run(self, runner):
        error = 0
        dfnames = self.item_for_xml_stubs.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(
            runner.dump_dir, self.list_dumpnames())
        for output_dfname in output_dfnames:
            command_series = self.build_command(runner, dfnames, output_dfname)

            self.setup_command_info(runner, command_series, [output_dfname])
            result, broken = runner.run_command(
                [command_series], callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=True,
                callback_on_completion=self.command_completion_callback)
            if result:
                error = result
        if error:
            raise BackupError("error recombining stub files")


class RecombineXmlDump(RecombineDump):
    def __init__(self, name, desc, detail, item_for_xml_dumps):
        # no prefetch, no spawn
        self.item_for_xml_dumps = item_for_xml_dumps
        self._detail = detail
        self._prerequisite_items = [self.item_for_xml_dumps]
        super(RecombineXmlDump, self).__init__(name, desc)
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
        if not exists(runner.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" %
                              runner.wiki.config.bzip2)
        compression_command = runner.wiki.config.bzip2
        compression_command = "%s > " % runner.wiki.config.bzip2
        uncompression_command = ["%s" % runner.wiki.config.bzip2, "-dc"]
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
        error, broken = runner.run_command(
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
        super(RecombineXmlRecompressDump, self).__init__(name, desc)
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
        if not len(input_dfnames):
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
            result, broken = runner.run_command(
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
        super(RecombineAbstractDump, self).__init__(name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpoints_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def build_command(self, runner, to_recombine_dfnames, output_dfname):
        input_dfnames = []
        for in_dfname in to_recombine_dfnames:
            if in_dfname.dumpname == output_dfname.dumpname:
                input_dfnames.append(in_dfname)
        if not len(input_dfnames):
            self.set_status("failed")
            raise BackupError("No input files for %s found" % self.name())
        if not exists(runner.wiki.config.cat):
            raise BackupError("cat command %s not found" % runner.wiki.config.cat)
        compression_command = "%s > " % runner.wiki.config.cat
        uncompression_command = ["%s" % runner.wiki.config.cat]
        recombine_command_string = self.build_recombine_command_string(
            runner, input_dfnames, output_dfname, compression_command,
            uncompression_command, "<feed>")
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        return series

    def run(self, runner):
        error = 0
        to_recombine_dfnames = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_dfname in output_dfnames:
            command_series = self.build_command(runner, to_recombine_dfnames, output_dfname)
            self.setup_command_info(runner, command_series, [output_dfname])
            result, broken = runner.run_command(
                [command_series], callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=True,
                callback_on_completion=self.command_completion_callback)
            if result:
                error = result
        if error:
            raise BackupError("error recombining abstract dump files")
