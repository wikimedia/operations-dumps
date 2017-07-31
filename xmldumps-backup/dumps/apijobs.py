import time
from dumps.exceptions import BackupError
from dumps.jobs import Dump


class SiteInfoDump(Dump):
    '''Dump of siteinfo properties using MediaWiki api'''
    def __init__(self, properties, name, desc):
        self._properties = properties
        self._parts_enabled = False
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return "siteinfo-" + self.name()

    def get_filetype(self):
        return "json"

    def get_file_ext(self):
        return ""

    def run(self, runner):
        retries = 0
        maxretries = 3
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("siteinfo dump %s trying to produce more than one file" %
                              self.dumpname)
        output_dfname = dfnames[0]
        commands = self.build_command(runner)
        if runner.wiki.is_private():
            command_series = runner.get_save_command_series(
                commands, self.get_inprogress_name(
                    runner.dump_dir.filename_private_path(output_dfname)))
        else:
            command_series = runner.get_save_command_series(
                commands, self.get_inprogress_name(
                    runner.dump_dir.filename_public_path(output_dfname)))
        self.setup_command_info(runner, command_series, [output_dfname])

        error, broken = runner.save_command(command_series, self.command_completion_callback)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error, broken = runner.save_command(command_series)
        if error:
            raise BackupError("error dumping siteinfo props %s" % ','.join(self._properties))

    def build_command(self, runner):
        #  https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo
        #         &siprop=namespaces|namespacealiases|magicwords&format=json
        base_url = runner.db_server_info.apibase
        properties = '|'.join(self._properties)
        api_url = "{baseurl}?action=query&meta=siteinfo&siprop={props}&format=json"
        url = api_url.format(baseurl=base_url, props=properties)
        command = [["/usr/bin/curl", "-s", url]]
        return command
