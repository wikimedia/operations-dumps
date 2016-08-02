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

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("flow content step wants to produce more than one output file")
        output_file_obj = files[0]
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        flow_output_file = runner.dump_dir.filename_public_path(output_file_obj)
        script_command = MultiVersion.mw_script_as_array(
            runner.wiki.config, "extensions/Flow/maintenance/dumpBackup.php")

        command = [runner.wiki.config.php]
        command.extend(script_command)
        command.extend(["--wiki=%s" % runner.db_name,
                        "--current", "--report=1000",
                        "--output=bzip2:%s" % flow_output_file])
        if self.history:
            command.append("--full")

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error dumping flow page files")
