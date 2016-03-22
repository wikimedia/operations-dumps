import os
import sys
import ConfigParser


class ArchiveUploaderConfig(object):
    """Read contents of config file, if any.
    If no filename is provided, the default name 'archiveuploader.conf' will
    be checked.  If it is not present, the files /etc/archiveuploader.conf and
    .archiveuploader.conf will be checked, in that order."""

    def __init__(self, config_file=None):
        """Constructor. Args:
        config_file -- path to configuration file. If not passed,
                      the default 'archiveuploader.conf' will be checked."""

        self.project_name = False
        self.access_key = None
        self.secret_key = None

        home = os.path.dirname(sys.argv[0])
        if config_file is None:
            config_file = "archiveuploader.conf"
        self.files = [
            os.path.join(home, config_file),
            "/etc/archiveuploader.conf",
            os.path.join(os.getenv("HOME"), ".archiveuploader.conf")]

        self.conf = ConfigParser.SafeConfigParser(self.get_config_defaults())
        self.conf.read(self.files)
        self.settings = self.parse_conf_file()

    @staticmethod
    def get_config_defaults():
        """return a dict of configuration setting defaults usable
        by the Config module"""
        defaults = {
            # "auth": {
            "accesskey": "",
            "secretkey": "",
            "username": "",
            "password": "",
            # "output": {
            "sitematrixfile": "",
            # "web": {
            "apiurl": "http://en.wikipedia.org/w/api.php",
            "curl": "/usr/bin/curl",
            "itemnameformat": "%%s",
            "licenseurl": "http://wikimediafoundation.org/wiki/Terms_of_Use",
            "creator": "the Wikimedia Foundation",
            "downloadurl": "http://dumps.wikimedia.org"
            }
        return defaults

    def parse_conf_file(self):
        """Get contents of config file, using new values to overwrite
        corresponding defaults."""
        settings = {}
        settings['access_key'] = self.conf.get("auth", "accesskey")
        settings['secret_key'] = self.conf.get("auth", "secretkey")
        settings['username'] = self.conf.get("auth", "username")
        settings['password'] = self.conf.get("auth", "password")
        settings['site_matrix_file'] = self.conf.get("output", "sitematrixfile")
        settings['api_url'] = self.conf.get("web", "apiurl")
        settings['curl'] = self.conf.get("web", "curl")
        settings['item_name_format'] = self.conf.get("web", "itemnameformat")
        settings['license_url'] = self.conf.get("web", "licenseurl")
        settings['creator'] = self.conf.get("web", "creator")
        settings['downloadurl'] = self.conf.get("web", "downloadurl")
        return settings
