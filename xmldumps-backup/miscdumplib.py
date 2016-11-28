# shared classes for misc dumps (incrementals, html, etc)
import os
import sys
import re
import ConfigParser
import dumps.WikiDump
from dumps.WikiDump import FileUtils, MiscUtils
from dumps.exceptions import BackupError
from dumps.utils import MultiVersion, RunSimpleCommand
from os.path import exists
import socket
import shutil
import time


class ContentFile(object):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.dump_dir = MiscDir(self._config, date)
        self.wikiname = wikiname

    # override this.
    def get_filename(self):
        return "content.txt"

    def get_path(self):
        return os.path.join(self.dump_dir.get_dumpdir(self.wikiname), self.get_filename())

    def get_fileinfo(self):
        return FileUtils.file_info(self.get_path())


class MaxRevIDFile(ContentFile):
    def get_filename(self):
        return "maxrevid.txt"


class StubFile(ContentFile):
    def get_filename(self):
        return "%s-%s-stubs-meta-hist-incr.xml.gz" % (self.wikiname, self.date)


class RevsFile(ContentFile):
    def get_filename(self):
        return "%s-%s-pages-meta-hist-incr.xml.bz2" % (self.wikiname, self.date)


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


class IncrDumpLockFile(LockFile):
    def get_filename(self):
        return "%s-%s-incrdump.lock" % (self.wikiname, self.date)


class MD5File(ContentFile):
    def get_filename(self):
        return "%s-%s-md5sums.txt" % (self.wikiname, self.date)


class IndexFile(ContentFile):
    def __init__(self, config):
        self._config = config
        self.dump_dir = MiscDir(self._config)

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
        if (time.time() - timestamp) > self._config.stale_interval:
            return True
        else:
            return False

    def unlock(self):
        os.remove(self.lockfile.get_path())


class IncrDumpLock(Lock):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = IncrDumpLockFile(self._config, self.date, self.wikiname)


class MaxRevIDLock(Lock):
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = MaxRevIDLockFile(self._config, self.date, self.wikiname)


class Config(dumps.WikiDump.Config):
    def __init__(self, config_file=None):
        self.project_name = False

        home = os.path.dirname(sys.argv[0])
        if config_file is None:
            config_file = "dumpincr.conf"
        self.files = [
            os.path.join(home, config_file),
            "/etc/dumpincrementals.conf",
            os.path.join(os.getenv("HOME"), ".dumpincr.conf")]
        defaults = {
            # "wiki": {
            "allwikislist": "",
            "privatewikislist": "",
            "closedwikislist": "",
            "skipwikislist": "",
            # "output": {
            "dumpdir": "/dumps/public/incr",
            "templatedir": home,
            "temp": "/dumps/temp",
            "webroot": "http://localhost/dumps/incr",
            "fileperms": "0640",
            "delay": "43200",
            "maxrevidstaleinterval": "3600",
            # "database": {
            # moved defaults to get_db_user_and_password
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
        self.template_dir = self.conf.get("output", "templatedir")
        self.webroot = self.conf.get("output", "webroot")
        self.fileperms = self.conf.get("output", "fileperms")
        self.fileperms = int(self.fileperms, 0)
        self.delay = self.conf.get("output", "delay")
        self.delay = int(self.delay, 0)
        self.stale_interval = self.conf.get("output", "maxrevidstaleinterval")
        self.stale_interval = int(self.stale_interval, 0)

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

    def read_template(self, name):
        template = os.path.join(self.template_dir, name)
        return FileUtils.read_file(template)


class DBServer(object):
    def __init__(self, config, wikiname):
        self.config = config
        self.wikiname = wikiname
        self.db_server = self.default_server()

    def default_server(self):
        if not exists(self.config.php):
            raise BackupError("php command %s not found" % self.config.php)
        command_list = MultiVersion.mw_script_as_array(self.config, "getSlaveServer.php")
        command = [self.config.php]
        command.extend(command_list)
        command.extend(["--wiki=%s" % self.wikiname, "--group=dump"])
        return RunSimpleCommand.run_with_output(command, shell=False).rstrip()

    def build_sql_command(self, query):
        """Put together a command to execute an sql query to the server for this DB."""
        if not exists(self.config.mysql):
            raise BackupError("mysql command %s not found" % self.config.mysql)
        command = ("/bin/echo '%s' | %s -h %s -u %s " %
                   (query, self.config.mysql, self.db_server, self.config.db_user))
        if self.config.db_password != "":
            command = command + "-p" + self.config.db_password
        command = command + " -r --silent " + self.wikiname
        return command


class MiscDir(object):
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
        self.dump_dir = MiscDir(self._config)

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

    def cleanup_old_incrdumps(self, date):
        old = self.get_misc_dumpdirs()
        if old:
            if old[-1] == date:
                old = old[:-1]
            if self._config.keep > 0:
                old = old[:-(self._config.keep)]
            for dump in old:
                to_remove = os.path.join(self.dump_dir.get_dumpdir_no_date(self.wikiname), dump)
                shutil.rmtree("%s" % to_remove)

    def get_prev_incrdate(self, date, dumpok=False, revidok=False):
        # find the most recent incr dump before the
        # specified date
        # if "dumpok" is True, find most recent dump that completed successfully
        # if "revidok" is True, find most recent dump that has a populated maxrevid.txt file
        previous = None
        old = self.get_misc_dumpdirs()
        if old:
            for dump in old:
                if dump == date:
                    return previous
                else:
                    if dumpok:
                        status_info = StatusInfo(self._config, dump, self.wikiname)
                        if status_info.get_status(dump) == "done":
                            previous = dump
                    elif revidok:
                        max_revid_file = MaxRevIDFile(self._config, dump, self.wikiname)
                        if exists(max_revid_file.get_path()):
                            revid = FileUtils.read_file(max_revid_file.get_path().rstrip())
                            if int(revid) > 0:
                                previous = dump
                    else:
                        previous = dump
        return previous

    def get_latest_dump_date(self, dumpok=False):
        # find the most recent incr dump
        dirs = self.get_misc_dumpdirs()
        if dirs:
            if dumpok:
                for dump in reversed(dirs):
                    status_info = StatusInfo(self._config, dump, self.wikiname)
                    if status_info.get_status(dump) == "done":
                        return dump
            else:
                return dirs[-1]
        else:
            return None
