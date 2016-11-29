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
import fcntl
import ConfigParser
import logging
import logging.config
from dumps.WikiDump import FileUtils, MiscUtils, Config
from dumps.utils import DbServerInfo, RunSimpleCommand


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
        override this

        must return basename of the file. example:

        return "content.txt"
        '''
        raise NotImplementedError

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
        self.basename = "index.html"

    def get_filename(self):
        '''
        return basename of index.html file
        '''
        return self.basename

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

    methods to:
       get lockfile for dump run for a given wiki
       update the mtime so the lockfile isn't stale
       remove lockfile if created by us
       remove lockfile if older than cutoff seconds

    works with: unix (linux). nfs3 or local fs. nothing else guaranteed.
    '''
    def __init__(self, config, date, wikiname):
        self._config = config
        self.date = date
        self.wikiname = wikiname
        self.lockfile = MiscDumpLockFile(self._config, self.date, self.wikiname)

    def get_lock(self):
        '''
        acquire lock for wiki and return True.
        if it does not exist, create it
        return False if lock could not be acquired
        '''
        try:
            if not exists(self._config.dump_dir):
                os.makedirs(self._config.dump_dir)
            fhandle = FileUtils.atomic_create(self.lockfile.get_path(), "w")
            fcntl.lockf(fhandle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fhandle.write("%s %d" % (socket.getfqdn(), os.getpid()))
            fhandle.close()
            return True
        except Exception as ex:
            log.info("Error encountered getting lock", exc_info=ex)
            return False

    def _get_lockfile_contents(self):
        try:
            contents = FileUtils.read_file(self.lockfile.get_path(self.date))
            return contents.split()
        except Exception:
            return None, None

    def _check_lock_owner(self):
        '''
        check if this process owns the lock
        returns false if not, if no lock, or
        if attempt to check failed
        '''
        try:
            myhostname = socket.getfqdn()
            mypid = str(os.getpid())
            lock_host, lock_pid = self._get_lockfile_contents()
            if myhostname == lock_host and mypid == lock_pid:
                return True
        except Exception:
            pass
        return False

    def unlock_if_owner(self):
        '''
        remove lock if we are the owner (hostname and
        pid match up with file contents)
        this assumes no other caller removes it by force, after
        we checked ownership, and then creates it underneath us.
        to avoid that happening, refresh the lock periodically
        and only allow other callers to remove if stale.
        '''
        try:
            if self._check_lock_owner():
                self._unlock()
                return True
        except Exception:
            pass
        return False

    def _unlock(self):
        '''
        remove lock unconditionally
        '''
        try:
            os.unlink(self.lockfile.get_path())
            return True
        except Exception:
            pass
        return False

    def remove_if_stale(self, cutoff):
        '''
        given number of seconds, see if file is older than this many seconds
        and remove file if so.  we do this by opening the file exclusively first,
        stat on the open fdesc, then remove path if it checks out.
        return True on removal, False for anything else including errors.
        '''
        removed = False
        try:
            # we're not going to write anything but have to open for write
            # in order to get LOCK_EX
            fdesc = open(self.lockfile.get_path(), "a+")
            # try to get the lock. if we can't then we give up
            try:
                fcntl.lockf(fdesc, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception:
                # fail to get lock or some other error
                fdesc.close()
                return removed
            if self._is_stale(cutoff, fdesc.fileno()):
                removed = self._unlock()
            else:
                # if the file did not exist, our open call would have created
                # it, and then we would have an empty file.  See if that's the
                # case and if so, clean it up
                contents = FileUtils.read_file(self.lockfile.get_path(self.date))
                if not contents:
                    removed = self._unlock()
            # lock removed now
            fdesc.close()
            return removed
        except Exception:
            pass
        return False

    def _is_stale(self, cutoff, fdesc=None):
        '''
        given number of seconds, see if file is older than this many seconds
        and return True if so, on any other result or error return False
        '''
        try:
            if fdesc is None:
                filetime = os.stat(self.lockfile.get_path()).st_mtime
            else:
                filetime = os.fstat(fdesc).st_mtime
            now = time.time()
            if now - filetime > cutoff:
                return True
        except Exception:
            pass
        return False

    def refresh(self):
        '''
        update the mtime on the lockfile so that it's
        no longer stale
        '''
        now = time.time()
        os.utime(self.lockfile.get_path(), (now, now))


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
        lock_stale = self.conf.get("output", "lockstale")
        self.lock_stale = int(lock_stale, 0)
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


def run_simple_query(query, wiki):
    '''
    run a mysql query which returns only one field from
    one row.
    return the value of that one field (as a string)
    '''
    db_info = DbServerInfo(wiki, wiki.db_name)
    commands = db_info.build_sql_command(query)
    echocmd = commands[0]
    mysqlcmd = commands[1]
    to_run = " ".join(echocmd) + " | " + " ".join(mysqlcmd) + " --silent"
    log.info("running with no output: " + to_run)
    return RunSimpleCommand.run_with_output(to_run, shell=True)


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
        self.lock = None

    def get_steps(self):
        '''
        override this

        return dict of steps the dump may run and files that each step generates
        note that this assumes each step generates only one file. for now.

        example:

        steps = {'sample': {'file': 'full_path_to_file', 'run': True}}
        return steps
        '''
        raise NotImplementedError

    def run(self):
        '''
        override this

        dump all steps marked as 'run': True
        return True if all requested steps of dump complete, False otherwise
        '''
        raise NotImplementedError

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

    def set_lockinfo(self, lock):
        '''
        pass MiscDumpLock object; we use this for refreshing the lock
        while dump commands run
        '''
        self.lock = lock

    def periodic_callback(self, output=None, error=None):
        '''
        This is meant just to refresh the lock periodically
        so it doesn't get stale.  But we might as well
        log any output or error messages if there are any
        '''
        if output:
            log.info(output)
        if error:
            log.info(error)
        self.lock.refresh()

    def get_lock_timeout_interval(self):
        '''
        how often in milliseconds should we try to refresh?
        sooner than the stale interval so it doesn't expire

        it would be nice if users configured the stale
        interval greater than 1 second, but just in case
        we try to be polite about that
        '''
        timeout_interval = self.wiki.config.lock_stale * 1000
        if timeout_interval > 2000:
            timeout_interval -= 1000
        elif timeout_interval > 0:
            timeout_interval -= 500
        return timeout_interval
