'''
All dump jobs that recombine output from other
dump jobs are defined here
'''

from os.path import exists
from dumps.exceptions import BackupError
from dumps.jobs import Dump
from dumps.xmlcontentjobs import XmlDump


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
        """
        returns:
            list of DumpFile
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns:
            list of DumpFile
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
        error = 0
        dfnames = self.item_for_xml_stubs.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(
            runner.dump_dir, self.list_dumpnames())
        for output_dfname in output_dfnames:
            input_dfnames = []
            for in_dfname in dfnames:
                if in_dfname.dumpname == output_dfname.dumpname:
                    input_dfnames.append(in_dfname)
            if not len(input_dfnames):
                self.set_status("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(runner.wiki.config.gzip):
                raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
            compression_command = runner.wiki.config.gzip
            compression_command = "%s > " % runner.wiki.config.gzip
            uncompression_command = ["%s" % runner.wiki.config.gzip, "-dc"]
            recombine_command_string = self.build_recombine_command_string(
                runner, input_dfnames, output_dfname, compression_command, uncompression_command)
            recombine_command = [recombine_command_string]
            recombine_pipeline = [recombine_command]
            series = [recombine_pipeline]
            result = runner.run_command([series], callback_timed=self.progress_callback,
                                        callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining stub files")


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
        dfnames = self.item_for_xml_dumps.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(output_dfnames) > 1:
            raise BackupError("recombine XML Dump trying to "
                              "produce more than one output file")

        error = 0
        if not exists(runner.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" %
                              runner.wiki.config.bzip2)
        compression_command = runner.wiki.config.bzip2
        compression_command = "%s > " % runner.wiki.config.bzip2
        uncompression_command = ["%s" % runner.wiki.config.bzip2, "-dc"]
        recombine_command_string = self.build_recombine_command_string(
            runner, dfnames, output_dfnames[0], compression_command, uncompression_command)
        recombine_command = [recombine_command_string]
        recombine_pipeline = [recombine_command]
        series = [recombine_pipeline]
        error = runner.run_command(
            [series], callback_timed=self.progress_callback,
            callback_timed_arg=runner, shell=True)

        if error:
            raise BackupError("error recombining xml bz2 files")


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
        self._parts_enabled = False

    def get_filetype(self):
        return self.item_for_recombine.get_filetype()

    def get_file_ext(self):
        return self.item_for_recombine.get_file_ext()

    def get_dumpname(self):
        return self.item_for_recombine.get_dumpname()

    def run(self, runner):
        error = 0
        self.cleanup_old_files(runner.dump_dir, runner)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_dfname in output_dfnames:
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
            result = runner.run_command(
                [series], callback_timed=self.progress_callback,
                callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining xml bz2 file(s)")


class RecombineAbstractDump(Dump):
    def __init__(self, name, desc, item_for_recombine):
        # no partnum_todo, no parts generally (False, False), even though input may have it
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
        to_recombine_dfnames = self.item_for_recombine.list_outfiles_for_input(runner.dump_dir)
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        for output_dfname in output_dfnames:
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
            result = runner.run_command([series], callback_timed=self.progress_callback,
                                        callback_timed_arg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining abstract dump files")
