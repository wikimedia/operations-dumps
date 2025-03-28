#!/usr/bin/python3
import time
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump
from dumps.utils import is_runnning_in_kubernetes


class SiteInfoDump(Dump):
    '''Dump of siteinfo properties using MediaWiki api'''
    def __init__(self, properties, name, desc):
        self._properties = properties
        self._parts_enabled = False
        Dump.__init__(self, name, desc)
        self._version = "1"

    def get_dumpname(self):
        return "siteinfo-" + self.name()

    def get_filetype(self):
        return "json"

    def get_file_ext(self):
        return "gz"

    def run(self, runner):
        retries = 0
        maxretries = runner.wiki.config.max_retries
        dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        if len(dfnames) > 1:
            raise BackupError("siteinfo dump %s trying to produce more than one file" %
                              self.dumpname)
        output_dfname = dfnames[0]
        commands = self.build_command(runner)
        command_series = runner.get_save_command_series(
            commands, DumpFilename.get_inprogress_name(
                runner.dump_dir.filename_public_path(output_dfname)))
        self.setup_command_info(runner, command_series, [output_dfname])

        error, _broken = runner.save_command(command_series, self.command_completion_callback)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error, _broken = runner.save_command(command_series)
        if error:
            raise BackupError("error dumping siteinfo props %s" % ','.join(self._properties))
        return True

    def build_command(self, runner):
        """
        assemble the command that will retrieve the appropriate properties
        for the wiki via the MediaWiki ai
        """
        #  https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo
        #         &siprop=namespaces|namespacealiases|magicwords&format=json*formatversion=1
        base_url = runner.db_server_info.get_attr('apibase')
        properties = '|'.join(self._properties)
        api_url = "{baseurl}?action=query&meta=siteinfo&siprop={props}&format=json&formatversion={vers}"
        url = api_url.format(baseurl=base_url, props=properties, vers=self._version)
        extra_flags = []
        # In Kubernetes, the curl command is sent to envoy, and requires a 'Host' header set to the
        # public wiki domain
        if is_runnning_in_kubernetes():
            api_host = runner.db_server_info.get_attr("api_host")
            extra_flags.extend(['-H', f'Host:{api_host}'])
        curl_command = ["/usr/bin/curl", "-s", url] + extra_flags
        command = [curl_command, [runner.wiki.config.gzip]]
        return command


class SiteInfoV2Dump(SiteInfoDump):
    """
    Dump of siteinfo properties using API formatversion=2

    https://www.mediawiki.org/wiki/API:JSON_version_2
    """
    def __init__(self, properties, name, desc):
        super().__init__(properties, name, desc)
        self._version = "2"

    def get_dumpname(self):
        return "siteinfo2-" + self.name()
