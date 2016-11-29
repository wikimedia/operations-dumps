# shared classes for misc dumps (incrementals, html, etc)
import os
from os.path import exists
import sys
import re
import socket
import shutil
import time
import ConfigParser
import dumps.WikiDump
from dumps.WikiDump import FileUtils, MiscUtils


STATUS_TODO = 1
STATUS_FAILED = -1
STATUS_GOOD = 0


def log(verbose, message):
    if verbose:
        print message


def safe(item):
    if item is not None:
        return item
    else:
        return "None"


def make_link(path, link_text):
    return '<a href = "' + path + '">' + link_text + "</a>"


class ContentFile(object):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.dump_dir = MiscDumpDir(self._config, date)
        self.wikiname = wikiname

    # override this.
    def get_filename(self):
        return "content.txt"

    def get_path(self):
        return os.path.join(self.dump_dir.get_dumpdir(self.wikiname), self.get_filename())

    def get_fileinfo(self):
        return FileUtils.file_info(self.get_path())


class StatusFile(ContentFile):
    def get_filename(self):
        return "status.txt"

    def get_path(self, date=None):
        return os.path.join(self.dump_dir.get_dumpdir(self.wikiname, date), self.get_filename())


class LockFile(ContentFile):
    def get_filename(self):
        return "%s-%s.lock" % (self.wikiname, self.date)

    def get_path(self):
        return os.path.join(self.dump_dir.get_dumpdir_no_date(self.wikiname), self.get_filename())


class MaxRevIDLockFile(LockFile):
    def get_filename(self):
        return "%s-%s-maxrevid.lock" % (self.wikiname, self.date)


class MiscDumpLockFile(LockFile):
    def get_filename(self):
        return "%s-%s-miscdump.lock" % (self.wikiname, self.date)


class MD5File(ContentFile):
    def get_filename(self):
        return "%s-%s-md5sums.txt" % (self.wikiname, self.date)


class IndexFile(ContentFile):
    def __init__(self, config):
        self._config = config
        self.dump_dir = MiscDumpDir(self._config)

    def get_filename(self):
        return "index.html"

    def get_path(self):
        return os.path.join(self.dump_dir.get_dumpdir_base(), self.get_filename())


class StatusInfo(object):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.status_file = StatusFile(self._config, self.date, self.wikiname)

    def get_status(self, date=None):
        status = ""
        if exists(self.status_file.get_path(date)):
            status = FileUtils.read_file(self.status_file.get_path(date)).rstrip()
        return status

    def set_status(self, status):
        FileUtils.write_file_in_place(self.status_file.get_path(), status, self._config.fileperms)


class Lock(object):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = LockFile(self._config, self.date, self.wikiname)

    def is_locked(self):
        return exists(self.lockfile.get_path())

    def get_lock(self):
        try:
            if not exists(self._config.dump_dir):
                os.makedirs(self._config.dump_dir)
            fhandle = FileUtils.atomic_create(self.lockfile.get_path(), "w")
            fhandle.write("%s %d" % (socket.getfqdn(), os.getpid()))
            fhandle.close()
            return True
        except Exception as ex:
            return False

    def is_stale_lock(self):
        if not self.is_locked():
            return False
        try:
            timestamp = os.stat(self.lockfile.get_path()).st_mtime
        except Exception as ex:
            return False
        return (time.time() - timestamp) > self._config.stale_interval

    def unlock(self):
        os.remove(self.lockfile.get_path())


class MiscDumpLock(Lock):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = MiscDumpLockFile(self._config, self.date, self.wikiname)


class MaxRevIDLock(Lock):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = MaxRevIDLockFile(self._config, self.date, self.wikiname)


class Config(dumps.WikiDump.Config):
    def __init__(self, defaults=None, config_file=None):
        self.project_name = False

        home = os.path.dirname(sys.argv[0])
        if config_file is None:
            config_file = "miscdumps.conf"
        self.files = [
            os.path.join(home, config_file),
            "/etc/miscdumps.conf",
            os.path.join(os.getenv("HOME"), ".miscdumps.conf")]

        self.conf = ConfigParser.SafeConfigParser(defaults)
        self.conf.read(self.files)

        if not self.conf.has_section("wiki"):
            print "The mandatory configuration section 'wiki' was not defined."
            raise ConfigParser.NoSectionError('wiki')

        if not self.conf.has_option("wiki", "mediawiki"):
            print "The mandatory setting 'mediawiki' in the section 'wiki' was not defined."
            raise ConfigParser.NoOptionError('wiki', 'mediawiki')

        self.db_user = None
        self.db_password = None

        self.parse_conffile()

    def parse_conffile(self):
        self.mediawiki = self.conf.get("wiki", "mediawiki")
        self.wiki_dir = self.mediawiki
        self.all_wikis_list = MiscUtils.db_list(self.conf.get("wiki", "allwikislist"))
        self.private_wikis_list = MiscUtils.db_list(self.conf.get("wiki", "privatewikislist"))
        self.closed_wikis_list = MiscUtils.db_list(self.conf.get("wiki", "closedwikislist"))
        self.skip_wikis_list = MiscUtils.db_list(self.conf.get("wiki", "skipwikislist"))

        if not self.conf.has_section('output'):
            self.conf.add_section('output')
        self.dump_dir = self.conf.get("output", "dumpdir")
        self.temp_dir = self.conf.get("output", "temp")
        self.indextmpl = self.conf.get("output", "indextmpl")
        self.template_dir = self.conf.get("output", "templatedir")
        self.webroot = self.conf.get("output", "webroot")
        fileperms = self.conf.get("output", "fileperms")
        self.fileperms = int(fileperms, 0)
        stale_interval = self.conf.get("output", "maxrevidstaleinterval")
        self.stale_interval = int(stale_interval, 0)

        if not self.conf.has_section('tools'):
            self.conf.add_section('tools')
        self.php = self.conf.get("tools", "php")
        self.gzip = self.conf.get("tools", "gzip")
        self.bzip2 = self.conf.get("tools", "bzip2")
        self.mysql = self.conf.get("tools", "mysql")
        self.checkforbz2footer = self.conf.get("tools", "checkforbz2footer")
        self.writeuptopageid = self.conf.get("tools", "writeuptopageid")
        self.multiversion = self.conf.get("tools", "multiversion")

        if not self.conf.has_section('cleanup'):
            self.conf.add_section('cleanup')
        self.keep = self.conf.getint("cleanup", "keep")

        self.wiki_dir = self.mediawiki  # the parent class methods want this
        self.db_user = None
        self.db_password = None
        if not self.conf.has_section('database'):
            self.conf.add_section('database')
        if self.conf.has_option('database', 'user'):
            self.db_user = self.conf.get("database", "user")
        if self.conf.has_option('database', 'password'):
            self.db_password = self.conf.get("database", "password")
        self.get_db_user_and_password()  # get from MW adminsettings file if not set in conf file
        self.max_allowed_packet = self.conf.get("database", "max_allowed_packet")

    def read_template(self, name):
        template = os.path.join(self.template_dir, name)
        return FileUtils.read_file(template)


class MiscDumpDir(object):
    def __init__(self, config, date=None):
        self._config = config
        self.date = date

    def get_dumpdir_base(self):
        return self._config.dump_dir

    def get_dumpdir_no_date(self, wikiname):
        return os.path.join(self.get_dumpdir_base(), wikiname)

    def get_dumpdir(self, wikiname, date=None):
        if date is None:
            return os.path.join(self.get_dumpdir_base(), wikiname, self.date)
        else:
            return os.path.join(self.get_dumpdir_base(), wikiname, date)


class MiscDumpDirs(object):
    def __init__(self, config, wikiname):
        self._config = config
        self.wikiname = wikiname
        self.dump_dir = MiscDumpDir(self._config)

    def get_misc_dumpdirs(self):
        base = self.dump_dir.get_dumpdir_no_date(self.wikiname)
        digits = re.compile(r"^\d{4}\d{2}\d{2}$")
        dates = []
        try:
            for dirname in os.listdir(base):
                if digits.match(dirname):
                    dates.append(dirname)
        except OSError:
            return []
        dates = sorted(dates)
        return dates

    def cleanup_old_dumps(self, date):
        old = self.get_misc_dumpdirs()
        if old:
            if old[-1] == date:
                old = old[:-1]
            if self._config.keep > 0:
                old = old[:-(self._config.keep)]
            for dump in old:
                to_remove = os.path.join(self.dump_dir.get_dumpdir_no_date(self.wikiname), dump)
                shutil.rmtree("%s" % to_remove)

    def get_latest_dump_date(self, dumpok=False):
        # find the most recent dump
        dirs = self.get_misc_dumpdirs()
        if dirs:
            if dumpok:
                for dump in reversed(dirs):
                    status_info = StatusInfo(self._config, dump, self.wikiname)
                    if status_info.get_status(dump).startswith("done"):
                        return dump
            else:
                return dirs[-1]
        else:
            return None


def get_config_defaults():
    return {
        # "wiki": {
        "allwikislist": "",
        "privatewikislist": "",
        "closedwikislist": "",
        "skipwikislist": "",
        # "output": {
        "dumpsdir": "/dumps/public/misc",
        "templatedir": "/dumps/templates",
        "indextmpl": "miscdumps-index.tmpl",
        "temp": "/dumps/temp",
        "webroot": "http://localhost/dumps/misc",
        "fileperms": "0640",
        "maxrevidstaleinterval": "3600",
        # "database": {
        # moved defaults to get_db_user_and_password
        "max_allowed_packet": "16M",
        # "tools": {
        "mediawiki": "",
        "php": "/bin/php",
        "gzip": "/usr/bin/gzip",
        "bzip2": "/usr/bin/bzip2",
        "mysql": "/usr/bin/mysql",
        "checkforbz2footer": "/usr/local/bin/checkforbz2footer",
        "writeuptopageid": "/usr/local/bin/writeuptopageid",
        "multiversion": "",
        # "cleanup": {
        "keep": "3",
    }
