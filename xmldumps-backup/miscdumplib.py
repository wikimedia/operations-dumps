'''
shared classes for misc dumps (incrementals, html, etc)
'''
import os
from os.path import exists
import sys
import re
import socket
import shutil
import time
import hashlib
import ConfigParser
import logging
import logging.config
from dumps.WikiDump import FileUtils, MiscUtils, Config


# pylint: disable=broad-except


STATUS_TODO = 1
STATUS_FAILED = -1
STATUS_GOOD = 0


log = logging.getLogger(__name__)    # pylint: disable=invalid-name
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        }
    }
})


def safe(item):
    '''
    given a string or None, return a printable string
    '''
    if item is not None:
        return item
    else:
        return "None"


def make_link(path, link_text):
    '''
    return html link for the path which displays link_text
    '''
    return '<a href = "' + path + '">' + link_text + "</a>"


class ContentFile(object):
    '''
    manage dump output file for given wiki and date
    '''
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.dump_dir = MiscDumpDir(self._config, date)
        self.wikiname = wikiname

    def get_filename(self):
        '''
        this should be overrided by subclasses.
        '''
        return "content.txt"

    def get_path(self, date=None):
        '''
        return full path to the file, including wikiname, date
        '''
        return os.path.join(self.dump_dir.get_dumpdir(self.wikiname, date), self.get_filename())

    def get_fileinfo(self):
        '''
        return a FileInfo object corresponding to the file
        '''
        return FileUtils.file_info(self.get_path())


class StatusFile(ContentFile):
    '''
    for file containing the status (done:all etc)
    of the dump for the wiki and date
    '''
    def get_filename(self):
        return "status.txt"


class MiscDumpLockFile(ContentFile):
    '''
    for dump lockfile for the given wiki and date
    '''
    def get_filename(self):
        return "%s-%s-miscdump.lock" % (self.wikiname, self.date)

    def get_path(self, date=None):
        return os.path.join(self.dump_dir.get_dumpdir_no_date(self.wikiname), self.get_filename())


class MD5File(ContentFile):
    '''
    for file of md5sums of dump output files for wiki and date
    '''
    def get_filename(self):
        return "%s-%s-md5sums.txt" % (self.wikiname, self.date)


class IndexFile(object):
    '''
    for index.html file for dumps of all wikis for all dates
    '''
    def __init__(self, config):
        self._config = config
        self.dump_dir = MiscDumpDir(self._config)

    def get_filename(self):
        '''
        return basename of index.html file
        '''
        return "index.html"

    def get_path(self):
        '''
        return full path to index.html file
        '''
        return os.path.join(self.dump_dir.get_dumpdir_base(), self.get_filename())


def md5sum_one_file(filename):
    '''
    generate and return md5 sum of specified file
    '''
    summer = hashlib.md5()
    infile = file(filename, "rb")
    bufsize = 4192 * 32
    buff = infile.read(bufsize)
    while buff:
        summer.update(buff)
        buff = infile.read(bufsize)
    infile.close()
    return summer.hexdigest()


def md5sums(wiki, fileperms, files, mandatory):
    '''
    generate md5sums for specified files for dump of
    given wiki and specific date, and save them to
    output file
    '''
    md5file = MD5File(wiki.config, wiki.date, wiki.db_name)
    text = ""
    errors = False
    for fname in files:
        try:
            text = text + "%s\n" % md5sum_one_file(fname)
            FileUtils.write_file_in_place(md5file.get_path(),
                                          text, fileperms)
        except Exception as ex:
            log.info("Error encountered in md5sum for %s", fname, exc_info=ex)
            if fname in mandatory:
                errors = True
    return not errors


class StatusInfo(object):
    '''
    manage dump status for the given wiki and date
    '''
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.status_file = StatusFile(self._config, self.date, self.wikiname)

    def get_status(self, date=None):
        '''
        return the status of the dump run for the given wiki and date,
        or the empty string if there is no run or no information available
        '''
        status = ""
        if exists(self.status_file.get_path(date)):
            status = FileUtils.read_file(self.status_file.get_path(date)).rstrip()
        return status

    def set_status(self, status):
        '''
        write out the status information supplied for the dump run
        '''
        FileUtils.write_file_in_place(self.status_file.get_path(), status, self._config.fileperms)


class MiscDumpLock(object):
    '''
    lock handling for the dump runs, in case more than one process on one
    or more servers runs dump at the same time
    '''
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = MiscDumpLockFile(self._config, self.date, self.wikiname)

    def is_locked(self):
        '''
        return True if the wiki is locked, False otherwise
        '''
        return exists(self.lockfile.get_path())

    def get_lock(self):
        '''
        acquire lock for wiki and return True
        return False if lock could not be acquired
        '''
        try:
            if not exists(self._config.dump_dir):
                os.makedirs(self._config.dump_dir)
            fhandle = FileUtils.atomic_create(self.lockfile.get_path(), "w")
            fhandle.write("%s %d" % (socket.getfqdn(), os.getpid()))
            fhandle.close()
            return True
        except Exception as ex:
            log.info("Error encountered getting lock", exc_info=ex)
            return False

    def is_stale_lock(self):
        '''
        return True if lock is older than config setting for stale locks,
        False otherwise or if no information is available
        '''
        if not self.is_locked():
            return False
        try:
            timestamp = os.stat(self.lockfile.get_path()).st_mtime
        except Exception as ex:
            log.info("Error encountered statting lock", exc_info=ex)
            return False
        return (time.time() - timestamp) > self._config.stale_interval

    def unlock(self):
        '''
        remove the lock for the wiki. Returns True on success, False otherwise
        '''
        try:
            os.remove(self.lockfile.get_path())
        except Exception as ex:
            log.info("Error encountered removing lock", exc_info=ex)
            return False
        return True


class MiscDumpConfig(object):
    '''
    configuration information for dumps
    '''
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
        '''
        grab values from configuration and assign them to appropriate variables
        '''
        self.wiki_dir = self.conf.get("wiki", "mediawiki")
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

        self.db_user = None
        self.db_password = None
        if not self.conf.has_section('database'):
            self.conf.add_section('database')
        if self.conf.has_option('database', 'user'):
            self.db_user = self.conf.get("database", "user")
        if self.conf.has_option('database', 'password'):
            self.db_password = self.conf.get("database", "password")
        # get from MW adminsettings file if not set in conf file
        if not self.db_user:
            self.db_user, self.db_password = Config.get_db_user_and_password(
                self.conf, self.wiki_dir)
        self.max_allowed_packet = self.conf.get("database", "max_allowed_packet")

    def read_template(self, name):
        '''
        read a file out of the configured template dir and return the contents
        '''
        template = os.path.join(self.template_dir, name)
        return FileUtils.read_file(template)


class MiscDumpDir(object):
    '''
    info about dump directory for a given date, wiki, and config settings
    '''
    def __init__(self, config, date=None):
        self._config = config
        self.date = date

    def get_dumpdir_base(self):
        '''
        return path of dir tree below which dumps of all wikis are found
        '''
        return self._config.dump_dir

    def get_dumpdir_no_date(self, wikiname):
        '''
        return path of dump dir for wiki without the date subdir
        '''
        return os.path.join(self.get_dumpdir_base(), wikiname)

    def get_dumpdir(self, wikiname, date=None):
        '''
        return path of dump dir for wiki and date
        '''
        if date is None:
            return os.path.join(self.get_dumpdir_base(), wikiname, self.date)
        else:
            return os.path.join(self.get_dumpdir_base(), wikiname, date)


class MiscDumpDirs(object):
    '''
    info about all the directories of dumps for all dates for a given wiki
    '''
    def __init__(self, config, wikiname):
        self._config = config
        self.wikiname = wikiname
        self.dump_dir = MiscDumpDir(self._config)

    def get_misc_dumpdirs(self):
        '''
        get and return list of basenames of wiki dump dirs;
        these names must at least pretend to be dates (8 digits),
        anything else will be silently skipped
        '''
        base = self.dump_dir.get_dumpdir_no_date(self.wikiname)
        digits = re.compile(r"^\d{4}\d{2}\d{2}$")
        dates = []
        try:
            for dirname in os.listdir(base):
                if digits.match(dirname):
                    dates.append(dirname)
        except OSError as ex:
            log.info("Error encountered listing %s", base, exc_info=ex)
            return []
        dates = sorted(dates)
        return dates

    def cleanup_old_dumps(self, date):
        '''
        remove 'extra' old dump directories and their contents for the wiki
        if there are more than the configured number to keep
        '''
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
        '''
        get and return the subdir (yyyymmdd format) of the most recent
        dump run.  most recent is determined by the subdir name, not by
        actual run times.
        '''
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
    '''
    get and return default configuration values for misc dumps
    '''
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


def skip_wiki(wikiname, config):
    '''
    return True if we should skip the given wiki instead of
    dumping it.
    '''
    return (wikiname in config.private_wikis_list or
            wikiname in config.closed_wikis_list or
            wikiname in config.skip_wikis_list)


class MiscDumpBase(object):
    '''
    base class for misc dumps to inherit from
    override the methods marked 'override this'
    '''
    def __init__(self, wiki, dryrun=False, args=None):
        '''
        wiki:     WikiDump object with date set
        dryrun:   whether or not to run commands or display what would have been done
        args:     dict of additional args 'revsonly' and/or 'stubsonly'
                  indicating whether or not to dump rev content and/or stubs
        '''
        self.wiki = wiki
        self.dirs = MiscDumpDirs(self.wiki.config, self.wiki.db_name)
        self.dryrun = dryrun
        self.args = args
        self.steps = self.get_steps()

    def get_steps(self):
        '''
        return dict of steps the dump may run and files that each step generates
        note that this assumes each step generates only one file. for now.
        override this
        '''
        steps = {'sample': {'file': 'full_path_to_file', 'run': True}}
        return steps

    def run(self):
        '''
        dump all steps marked as 'run': True
        return True if all requested steps of dump complete, False otherwise
        override this
        '''
        return True

    def get_steps_done(self):
        '''
        return comma-sep list of steps that are complete, in case not all are.
        if all are complete, return 'all'
        'complete' for the purposes of our check means the relevant output
        file exists.  we don't do checks on the content itself
        '''
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)

        steps_done = []
        for dump_step in self.steps:
            if exists(os.path.join(outputdir, self.steps[dump_step]['file'])):
                steps_done.append(dump_step)
        if len(steps_done) == len(self.steps):
            return 'all'
        elif len(steps_done):
            return ','.join(steps_done)
        else:
            return ''

    def get_output_files(self):
        '''
        return list of files that a full dump will produce, and a list of
        files that are expected to be generated
        by the current run or pre-existing as conditions for the current run
        '''
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        filenames = [self.steps[dump_step]['file'] for dump_step in self.steps]
        expected = [self.steps[dump_step]['file'] for dump_step in self.steps
                    if self.steps[dump_step]['run']]
        return [os.path.join(outputdir, filename) for filename in filenames], expected