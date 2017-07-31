'''
Dumps of Flow pages
'''

import os
from dumps.exceptions import BackupError
from dumps.utils import MultiVersion
from dumps.jobs import Dump


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
        else:
            return 'flow'

    def build_command(self, runner, output_dfname):
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        if runner.wiki.is_private():
            flow_output_fpath = runner.dump_dir.filename_private_path(output_dfname)
        else:
            flow_output_fpath = runner.dump_dir.filename_public_path(output_dfname)
        script_command = MultiVersion.mw_script_as_array(
            runner.wiki.config, "extensions/Flow/maintenance/dumpBackup.php")

        command = [runner.wiki.config.php]
        command.extend(script_command)
        command.extend(["--wiki=%s" % runner.db_name,
                        "--current", "--report=1000",
                        "--output=bzip2:%s" % self.get_inprogress_name(flow_output_fpath)])
        if self.history:
            command.append("--full")
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("flow content step wants to produce more than one output file")
        output_dfname = dfnames[0]
        command_series = self.build_command(runner, output_dfname)
        self.setup_command_info(runner, command_series, [output_dfname])
        error, broken = runner.run_command([command_series], callback_stderr=self.progress_callback,
                                           callback_stderr_arg=runner,
                                           callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error dumping flow page files")
