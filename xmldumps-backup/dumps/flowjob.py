#!/usr/bin/python3
'''
Dumps of Flow pages
'''

import os
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump, ProgressCallback


class FlowDump(Dump):
    """Dump the flow pages."""

    def __init__(self, name, desc, history=False):
        self.history = history
        Dump.__init__(self, name, desc)

    def detail(self):
        return "These files contain flow page content in xml format."

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "bz2"

    def get_dumpname(self):
        if self.history:
            return 'flowhistory'
        return 'flow'

    def build_command(self, runner, output_dfname):
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        flow_output_fpath = runner.dump_dir.filename_public_path(output_dfname)

        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", "xmlflow.py", "--config",
                   config_file_arg, "--wiki", runner.db_name,
                   "--outfile", DumpFilename.get_inprogress_name(flow_output_fpath)]

        if self.history:
            command.append("--history")

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        if len(dfnames) > 1:
            raise BackupError("flow content step wants to produce more than one output file")
        output_dfname = dfnames[0]
        command_series = self.build_command(runner, output_dfname)
        self.setup_command_info(runner, command_series, [output_dfname])
        prog = ProgressCallback()
        error, _broken = runner.run_command([command_series],
                                            callback_stderr=prog.progress_callback,
                                            callback_stderr_arg=runner,
                                            callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error dumping flow page files")
        return True
