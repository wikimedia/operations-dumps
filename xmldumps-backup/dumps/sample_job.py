#!/usr/bin/python3
'''
Sample job for illustrative purposes only

Dumps of site lists
In reality, this needs to run only on one wiki, but as an
example that can be run against an existing maintenance script
and for which one has to fiddle a bit with the command to
get the output file right, it's useful
'''

import os
import time
from dumps.exceptions import BackupError
from dumps.utils import MultiVersion
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump


class SitelistDump(Dump):
    """Dump the sites list in xml format"""

    def __init__(self, name, desc):
        Dump.__init__(self, name, desc)

    def detail(self):
        # this text shows up on the index.html page for the dump run for the wiki.
        return "These files contain a list of wikifarm sites in xml format."

    # the following settings ensure that the output filename will be of
    # the form <wiki>-<YYYYMMDD>-sitelist.xml.gz

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_dumpname(self):
        return 'sitelist'

    @staticmethod
    def build_command(runner, output_dfname):
        '''
        construct a list of commands in a pipeline which will run
        the desired script and piping all output to gzip
        '''
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        # the desired script is a maintenance script in MediaWiki core; no additional
        # path info is needed.
        script_command = MultiVersion.mw_script_as_array(
            runner.wiki.config, "exportSites.php")

        # the script does not write compressed output, we must arrange for that via
        # a pipeline, consisting of the script and the following gzip command.
        commands = [runner.wiki.config.php]
        commands.extend(script_command)
        commands.extend(["--wiki={wiki}".format(wiki=runner.db_name),
                         "php://stdout"])
        pipeline = [commands, [runner.wiki.config.gzip]]
        return pipeline

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)

        dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        if len(dfnames) > 1:
            raise BackupError("Site list dump job wants to produce more than one output file")
        output_dfname = dfnames[0]

        command_pipeline = self.build_command(runner, output_dfname)

        # we write to the "in progress" name, so that cleanup of unfinished
        # files is easier in the case of error, and also so that rsyncers can
        # pick up only the completed files

        # the save command series is just a list of the single pipeline but
        # with a redirection to the output file tacked onto the end.
        # this is useful for adding a compression step on the end when
        # scripts don't write compressed data directly.
        command_series = runner.get_save_command_series(
            command_pipeline, DumpFilename.get_inprogress_name(
                runner.dump_dir.filename_public_path(output_dfname)))
        self.setup_command_info(runner, command_series, [output_dfname])

        retries = 0
        maxretries = runner.wiki.config.max_retries

        # this command will invoke the html_update_callback as a timed callback, which
        # allows updates to various status files to be written every so often
        # (typically every 5 seconds) so that these updates can be seen by the users
        error, _broken = runner.save_command(command_series, self.command_completion_callback)

        # retry immediately, don't wait for some scheduler to find an open slot days later.
        # this catches things like network hiccups or a db being pulled out of the pool.
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error, _broken = runner.save_command(command_series)
        if error:
            raise BackupError("error dumping Sites list for wiki {wiki}".format(
                wiki=runner.db_name))
        return True
