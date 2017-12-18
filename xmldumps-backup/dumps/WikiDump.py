import ConfigParser
import os
import re
import socket
import sys
import threading
import time
import yaml
import traceback

from dumps.runnerutils import StatusHtml
from dumps.fileutils import FileUtils
from dumps.utils import MiscUtils, TimeUtils, DbServerInfo, RunSimpleCommand


class Config(object):
    def __init__(self, config_file=None):
        self.project_name = None
        self.db_user = None
        self.db_password = None
        self.override_section = None

        home = os.path.dirname(sys.argv[0])
        if config_file and ':' in config_file:
            config_file, self.override_section = config_file.split(':')

        if not config_file:
            config_file = "wikidump.conf"
        self.files = [
            os.path.join(home, config_file),
            "/etc/wikidump.conf"
        ]
        home = os.getenv("HOME")
        if home is not None:
            self.files.append(os.path.join(os.getenv("HOME"),
                                           ".wikidump.conf"))

        self.conf = ConfigParser.SafeConfigParser()
        self.conf.readfp(open('defaults.conf', "rb"))
        self.conf.read(self.files)

        if not self.conf.has_section("wiki"):
            print "The mandatory configuration section 'wiki' was not defined."
            raise ConfigParser.NoSectionError('wiki')

        if not self.conf.has_option("wiki", "dir"):
            print "The mandatory setting 'dir' in the section 'wiki' was not defined."
            raise ConfigParser.NoOptionError('wiki', 'dir')

        self.parse_conffile_overrideables()
        self.parse_conffile_globally()
        self.parse_conffile_per_project()
        # get from MW adminsettings file if not set in conf file
        if not self.db_user:
            self.db_user, self.db_password = Config.get_db_user_and_password(
                self.conf, self.wiki_dir)

    @staticmethod
    def parse_php_assignment(line):
        # not so much parse as grab a string to the right of the equals sign,
        # we expect a line that has  ... = "somestring" ;
        # with single or double quotes, spaes or not.  but nothing more complicated.
        equalspattern = r"=\s*(\"|')(.+)(\"|')\s*;"
        result = re.search(equalspattern, line)
        if result:
            return result.group(2)
        else:
            return ""

    @staticmethod
    def get_db_user_and_password(conf, wiki_dir):
        # check MW adminsettings file for these,
        # failing that we fall back on defaults specified here

        default_dbuser = "root"
        default_dbpassword = ""

        if not conf.has_option("wiki", "adminsettings"):
            db_user = default_dbuser
            db_password = default_dbpassword
            return db_user, db_password

        adminfhandle = open(os.path.join(wiki_dir, conf.get("wiki", "adminsettings")), "r")
        lines = adminfhandle.readlines()
        adminfhandle.close()

        # we are digging through a php file and expecting to find
        # lines more or less like the below.. anything more complicated we're not going to handle.
        # $wgDBadminuser = 'something';
        # $wgDBuser = $wgDBadminuser = "something" ;

        for line in lines:
            if "$wgDBadminuser" in line:
                db_user = Config.parse_php_assignment(line)
            elif "$wgDBuser" in line:
                default_dbuser = Config.parse_php_assignment(line)
            elif "$wgDBadminpassword" in line:
                db_password = Config.parse_php_assignment(line)
            elif "$wgDBpassword" in line:
                default_dbpassword = Config.parse_php_assignment(line)

        if not db_user:
            db_user = default_dbuser
        if not db_password:
            db_password = default_dbpassword
        return db_user, db_password

    def get_opt_from_sections(self, sections_to_check, item_name, is_int):
        """
        for each section name in sections_to_check:
            if the section isn't None and it exists in the config file,
            and the config setting is in that section, return the value
            otherwise move on to the next section in list
        returns int value if is_int is false, string otherwise, or
        None if the setting can't be found at all, not even a default
        """
        for section in sections_to_check:
            if section is None or not section:
                continue
            if not self.conf.has_section(section):
                continue
            if not self.conf.has_option(section, item_name):
                continue
            if is_int:
                return self.conf.getint(section, item_name)
            else:
                return self.conf.get(section, item_name)
        return None

    def get_opt_in_overrides_or_default(self, section_name, item_name, is_int):
        """
        look for option in the override section, if one was
        provided. if not provided or not found, look for it
        in the global (usual) section.
        """
        return self.get_opt_from_sections(
            [self.override_section, section_name],
            item_name, is_int)

    def get_opt_for_proj_or_default(self, section_name, item_name, is_int):
        """
        look for option in the project name section, if one was
        provided. if not provided or not found, look for it
        in the overrides section, if there is one. if there
        was no overrides section provided, or there is no
        such section in the config file, or the setting isn't
        in that section either, look for it in the global (usual)
        section.
        """
        return self.get_opt_from_sections(
            [self.project_name, self.override_section, section_name],
            item_name, is_int)

    def get_skipdbs(self, filenames):
        """
        permit comma-separated list of files so that eg some script
        can skip all private and/or closed wikis in addition to some
        other exclusion list
        """
        if ',' in filenames:
            skipfiles = filenames.split(',')
        else:
            skipfiles = [filenames]
        skip_db_list = []
        for skipfile in skipfiles:
            skip_db_list.extend(MiscUtils.db_list(skipfile))
        return list(set(skip_db_list))

    def parse_conffile_overrideables(self):
        """
        globals like entries in 'wiki' or 'output' that can
        be overriden by a specific named section
        """
        self.db_list_unsorted = MiscUtils.db_list(self.get_opt_in_overrides_or_default(
            "wiki", "dblist", 0), nosort=True)
        # permit comma-separated list of files so that eg some script
        # can skip all private and/or closed wikis in addition to some
        # other exclusion list
        to_skip = self.get_opt_in_overrides_or_default("wiki", "skipdblist", 0)
        self.skip_db_list = self.get_skipdbs(to_skip)

        self.private_list = MiscUtils.db_list(self.get_opt_in_overrides_or_default(
            "wiki", "privatelist", 0))
        self.closed_list = MiscUtils.db_list(self.get_opt_in_overrides_or_default(
            "wiki", "closedlist", 0))
        self.flow_list = MiscUtils.db_list(self.get_opt_in_overrides_or_default(
            "wiki", "flowlist", 0))
        self.tablejobs = self.get_opt_in_overrides_or_default(
            "wiki", "tablejobs", 0)
        self.apijobs = self.get_opt_in_overrides_or_default(
            "wiki", "apijobs", 0)

        self.db_list_unsorted = [dbname for dbname in self.db_list_unsorted
                                 if dbname not in self.skip_db_list]
        self.db_list = sorted(self.db_list_unsorted)

        if not self.conf.has_section('output'):
            self.conf.add_section('output')
        self.public_dir = self.get_opt_in_overrides_or_default("output", "public", 0)
        self.private_dir = self.get_opt_in_overrides_or_default("output", "private", 0)
        self.temp_dir = self.get_opt_in_overrides_or_default("output", "temp", 0)
        self.web_root = self.get_opt_in_overrides_or_default("output", "webroot", 0)
        self.index = self.get_opt_in_overrides_or_default("output", "index", 0)
        self.template_dir = self.get_opt_in_overrides_or_default("output", "templatedir", 0)
        self.perdump_index = self.get_opt_in_overrides_or_default("output", "perdumpindex", 0)
        self.log_file = self.get_opt_in_overrides_or_default("output", "logfile", 0)
        self.fileperms = self.get_opt_in_overrides_or_default("output", "fileperms", 0)
        self.fileperms = int(self.fileperms, 0)

        if not self.conf.has_section('misc'):
            self.conf.add_section('misc')
        self.fixed_dump_order = self.get_opt_in_overrides_or_default("misc", "fixeddumporder", 0)
        self.fixed_dump_order = int(self.fixed_dump_order, 0)

    def parse_conffile_globally(self):

        if not self.conf.has_section('database'):
            self.conf.add_section('database')
        self.max_allowed_packet = self.conf.get("database", "max_allowed_packet")

        if not self.conf.has_section('reporting'):
            self.conf.add_section('reporting')
        self.admin_mail = self.conf.get("reporting", "adminmail")
        self.mail_from = self.conf.get("reporting", "mailfrom")
        self.smtp_server = self.conf.get("reporting", "smtpserver")
        self.stale_age = self.conf.getint("reporting", "staleage")
        self.skip_privatetables = self.conf.getint("reporting", "skipprivatetables")

        if not self.conf.has_section('tools'):
            self.conf.add_section('tools')
        self.php = self.conf.get("tools", "php")
        self.gzip = self.conf.get("tools", "gzip")
        self.bzip2 = self.conf.get("tools", "bzip2")
        self.sevenzip = self.conf.get("tools", "sevenzip")
        self.mysql = self.conf.get("tools", "mysql")
        self.mysqldump = self.conf.get("tools", "mysqldump")
        self.head = self.conf.get("tools", "head")
        self.tail = self.conf.get("tools", "tail")
        self.cat = self.conf.get("tools", "cat")
        self.grep = self.conf.get("tools", "grep")
        self.checkforbz2footer = self.conf.get("tools", "checkforbz2footer")
        self.writeuptopageid = self.conf.get("tools", "writeuptopageid")
        self.recompressxml = self.conf.get("tools", "recompressxml")

        if not self.conf.has_section('query'):
            self.conf.add_section('query')
        self.queryfile = self.conf.get("query", "queryfile")

    def parse_conffile_per_project(self, project_name=None):
        if project_name:
            self.project_name = project_name

        if not self.conf.has_section('database'):
            self.conf.add_section('database')

        dbuser = self.get_opt_for_proj_or_default("database", "user", 0)
        if dbuser:
            self.db_user = dbuser
        dbpassword = self.get_opt_for_proj_or_default("database", "password", 0)
        if dbpassword:
            self.db_password = dbpassword
        max_allowed_packet = self.get_opt_for_proj_or_default(
            "database", "max_allowed_packet", 0)
        if max_allowed_packet:
            self.max_allowed_packet = max_allowed_packet

        if not self.conf.has_section('cleanup'):
            self.conf.add_section('cleanup')
        self.keep = self.get_opt_for_proj_or_default("cleanup", "keep", 1)

        if not self.conf.has_section('chunks'):
            self.conf.add_section('chunks')
        self.parts_enabled = self.get_opt_for_proj_or_default(
            "chunks", "chunksEnabled", 1)
        self.jobsperbatch = self.get_opt_for_proj_or_default(
            "chunks", "jobsperbatch", 0)
        self.pages_per_filepart_history = self.get_opt_for_proj_or_default(
            "chunks", "pagesPerChunkHistory", 0)
        self.revs_per_filepart_history = self.get_opt_for_proj_or_default(
            "chunks", "revsPerChunkHistory", 0)
        self.numparts_for_abstract = self.get_opt_for_proj_or_default(
            "chunks", "chunksForAbstract", 0)
        self.pages_per_filepart_abstract = self.get_opt_for_proj_or_default(
            "chunks", "pagesPerChunkAbstract", 0)
        self.recombine_history = self.get_opt_for_proj_or_default(
            "chunks", "recombineHistory", 1)
        self.checkpoint_time = self.get_opt_for_proj_or_default(
            "chunks", "checkpointTime", 1)
        self.revs_per_job = self.get_opt_for_proj_or_default(
            "chunks", "revsPerJob", 1)
        self.max_retries = self.get_opt_for_proj_or_default(
            "chunks", "maxRetries", 1)
        self.retry_wait = self.get_opt_for_proj_or_default(
            "chunks", "retryWait", 1)
        self.revs_margin = self.get_opt_for_proj_or_default(
            "chunks", "revsMargin", 1)

        if not self.conf.has_section('otherformats'):
            self.conf.add_section('otherformats')
        self.multistream_enabled = self.get_opt_for_proj_or_default(
            'otherformats', 'multistream', 1)
        if not self.conf.has_section('stubs'):
            self.conf.add_section('stubs')
        self.stubs_orderrevs = self.get_opt_for_proj_or_default(
            'stubs', 'orderrevs', 1)
        self.stubs_minpages = self.get_opt_for_proj_or_default(
            'stubs', 'minpages', 1)
        self.stubs_maxrevs = self.get_opt_for_proj_or_default(
            'stubs', 'maxrevs', 1)

        if not self.conf.has_section('wiki'):
            self.conf.add_section('wiki')
        self.wiki_dir = self.get_opt_for_proj_or_default("wiki", "dir", 0)

    def db_latest_status(self):
        '''
        return list of tuples for each wiki:
            status of latest wiki dump or None if wiki never dumped,
            wiki name
        '''
        dbinfo = []
        for dbname in self.db_list:
            wiki = Wiki(self, dbname)
            last = wiki.latest_dump()
            status = ''
            if last:
                dump_status = StatusHtml.get_statusfile_path(wiki, last)
                try:
                    status = FileUtils.read_file(dump_status)
                except Exception as ex:
                    status = 'failed'
                for value in ['missing', 'not yet', 'failed', 'aborted',
                              'progress', 'partial', 'complete']:
                    if value in status:
                        status = value
                        break
            else:
                status = None
            dbinfo.append((dbname, status, last))
        return dbinfo

    def db_list_by_age(self, use_status_time=False):
        '''
        return just the db names, sorted in reverse order of last successful dump
        '''
        available = self.db_info_by_age(use_status_time)
        return [dbname for (_failed, _date, _age, dbname) in available]

    def db_info_by_age(self, use_status_time=False):
        """
        Sort wikis in reverse order of last successful dump and return
        tuples of information for each wiki:
          * whether the dump failed,
          * the date of the run as found in dump dir string OR
            as determined by time of status file, if use_status_time is True,
          * age of status file if any,
          * wiki name

        Order is (DumpFailed, Age), and False < True:
        First, wikis whose latest dump was successful, most recent dump first
        Then, wikis whose latest dump failed, most recent dump first.
        Finally, wikis which have never been dumped.

        According to that sort, the last item of this list is, when applicable,
        the oldest failed dump attempt.

        If some error occurs checking a dump status, that dump is put last in the
        list (sort value is (True, maxsize) )

        Note that we now sort this list by the date of the dump directory, not the
        last date that a dump file in that directory may have been touched. This
        allows us to rerun jobs to completion from older runs, for example
        an en pedia history urn that failed in the middle, without borking the
        index page links.
        """
        available = []
        today = int(TimeUtils.today())
        for dbname in self.db_list:
            wiki = Wiki(self, dbname)

            age = sys.maxsize
            date = sys.maxsize
            last = wiki.latest_dump()
            status = ''
            if last:
                dump_status = StatusHtml.get_statusfile_path(wiki, last)
                try:
                    if use_status_time:
                        # only use the status file time, not the dir date
                        date = today
                    else:
                        date = today - int(last)
                    # tack on the file mtime so that if we have multiple wikis
                    # dumped on the same day, they get ordered properly
                    age = FileUtils.file_age(dump_status)
                    status = FileUtils.read_file(dump_status)
                except Exception as ex:
                    print "dump dir missing status file %s?" % dump_status
            dump_failed = (status == '') or ('dump aborted' in status)
            available.append((dump_failed, date, age, dbname))
        available = sorted(available)
        return available

    def read_template(self, name):
        template = os.path.join(self.template_dir, name)
        return FileUtils.read_file(template)

    def get_tablejobs_from_conf(self):
        try:
            if self.tablejobs:
                contents = open(self.tablejobs).read()
            else:
                contents = open("default_tables.yaml").read()
            return yaml.load(contents)
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(
                exc_type, exc_value, exc_traceback)))
            return {}

    def get_apijobs_from_conf(self):
        try:
            if self.apijobs:
                contents = open(self.apijobs).read()
            else:
                contents = open("default_api.yaml").read()
            return yaml.load(contents)
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(
                exc_type, exc_value, exc_traceback)))
            return {}


class Wiki(object):
    def __init__(self, config, db_name):
        self.config = config
        self.db_name = db_name
        self.date = None
        self.watchdog = None

    def is_private(self):
        return self.db_name in self.config.private_list

    def is_closed(self):
        return self.db_name in self.config.closed_list

    def has_flow(self):
        return self.db_name in self.config.flow_list

    # Paths and directories...

    def public_dir(self):
        if self.is_private():
            return self.private_dir()
        else:
            return os.path.join(self.config.public_dir, self.db_name)

    def private_dir(self):
        return os.path.join(self.config.private_dir, self.db_name)

    def web_dir(self):
        web_root = self.config.web_root
        if web_root[-1] == '/':
            web_root = web_root[:-1]
        return "/".join((web_root, self.db_name))

    def web_dir_relative(self):
        web_root_rel = self.web_dir()
        i = web_root_rel.find("://")
        if i >= 0:
            web_root_rel = web_root_rel[i + 3:]
        i = web_root_rel.find("/")
        if i >= 0:
            web_root_rel = web_root_rel[i:]
        return web_root_rel

    # Actions!

    def set_date(self, date):
        self.date = date

    def exists_perdump_index(self):
        index = os.path.join(self.public_dir(), self.date, self.config.perdump_index)
        return os.path.exists(index)

    def latest_dump(self, index=-1, return_all=False):
        """Find the last (or slightly less than last) dump for a db."""
        dirs = self.dump_dirs()
        if dirs:
            if return_all:
                return dirs
            else:
                return dirs[index]
        else:
            return None

    def date_touched_latest_dump(self):
        mtime = 0
        last = self.latest_dump()
        if last:
            dump_status = StatusHtml.get_statusfile_path(self, last)
            try:
                mtime = os.stat(dump_status).st_mtime
            except Exception as ex:
                pass
        return time.strftime("%Y%m%d", time.gmtime(mtime))

    def dump_dirs(self, private=False):
        """List all dump directories for the given database."""
        if private:
            base = self.private_dir()
        else:
            base = self.public_dir()
        digits = re.compile(r"^\d{4}\d{2}\d{2}$")
        dates = []
        try:
            for dirname in os.listdir(base):
                if digits.match(dirname):
                    dates.append(dirname)
        except OSError as ex:
            return []
        dates = sorted(dates)
        return dates

    def get_known_tables(self):
        dbserver = DbServerInfo(self, self.db_name)
        commands = dbserver.build_sql_command("'show tables'")
        echocmd = commands[0]
        mysqlcmd = commands[1]
        to_run = " ".join(echocmd) + " | " + " ".join(mysqlcmd) + " --silent"
        results = RunSimpleCommand.run_with_output(to_run, shell=True)
        return results.splitlines()


class Locker(object):
    def __init__(self, wiki, date=None):
        self.wiki = wiki
        self.watchdog = None
        self.date = self.get_date(date)

    def get_date(self, date):
        if date == 'last':
            dumps = sorted(self.wiki.dump_dirs())
            if dumps:
                date = dumps[-1]
            else:
                date = None
        if date is None:
            date = TimeUtils.today()
        return date

    def is_locked(self, all_locks=False):
        '''
        Return list of lockfiles for the given wiki,
        either for the one date for this instance,
        or for all dates. If there are no lockfiles
        an empty list will be returned
        '''
        if all_locks:
            return self.get_locks()
        else:
            if os.path.exists(self.get_lock_file_path()):
                return [self.get_lock_file_path()]
            else:
                return []

    def get_locks(self):
        '''
        get and return list of all lockfiles for the given
        wiki, regardless of date
        '''
        lockfiles = []
        entries = os.listdir(self.wiki.private_dir())
        for entry in entries:
            if entry.startswith('lock_'):
                lockfiles.append(os.path.join(self.wiki.private_dir(), entry))
        return lockfiles

    def is_stale(self, all_locks=False):
        '''
        check whether the wiki lockfile for the given
        date or for all dates are stale, return the
        list of stale lockfiles or an empty list if
        there are none
        '''
        stale_locks = []
        if all_locks:
            lockfiles = self.get_locks()
        else:
            lockfiles = [self.get_lock_file_path()]

        if not lockfiles:
            return stale_locks
        for lockfile in lockfiles:
            try:
                age = self.lock_age(lockfile)
                if age > self.wiki.config.stale_age:
                    stale_locks.append(lockfile)
            except Exception as ex:
                # Lock file vanished while we were looking
                continue
        return stale_locks

    def lock(self):
        '''
        create lock file for the given wiki and date, also
        set up a watchdog that will update its timestamp
        every minute.
        '''
        if not os.path.isdir(self.wiki.private_dir()):
            try:
                os.makedirs(self.wiki.private_dir())
            except Exception as ex:
                # Maybe it was just created (race condition)?
                if not os.path.isdir(self.wiki.private_dir()):
                    raise
        lockf = FileUtils.atomic_create(self.get_lock_file_path(), "w")
        lockf.write("%s %d" % (socket.getfqdn(), os.getpid()))
        lockf.close()

        self.watchdog = LockWatchdog(self.get_lock_file_path())
        # when the main script dies this thread must die too, horribly if needed.
        self.watchdog.daemon = True
        self.watchdog.start()
        return True

    def check_owner(self, lockfile, pid):
        '''
        check if the specified pid created the lockfile
        (it would be recorded in the lockfile)
        '''
        if pid is None:
            return True

        try:
            with open(lockfile, "r") as fhandle:
                lines = fhandle.read().splitlines()
                # if there's more than one line it's garbage or wrong file,
                # don't touch
                if len(lines) == 1:
                    lockpid = lines[0].split(" ", 1)[1]
                    if pid == lockpid:
                        return True
        except Exception as ex:
            # don't care what the error is, file is off limits for us
            pass
        return False

    def unlock(self, lockfiles, owner=False):
        '''
        remove all specified lockfiles.
        if 'owner' is True, check contents of each lockfile
        and only remove it if this process is the owner
        (its pid is recorded in lockfile)

        if more than one lockfile is to be removed, they had better be
        'stale' (no longer being updated by a watchdog) or this will fail
        '''
        if self.watchdog is not None:
            self.watchdog.stop_watching()
            self.watchdog = None
        if owner:
            pid = str(os.getpid())
        else:
            pid = None
        for lockfile in lockfiles:
            try:
                if self.check_owner(lockfile, pid):
                    os.remove(lockfile)
            except Exception as ex:
                # someone else removed it?
                pass

    def get_date_from_lockfilename(self, lockfile):
        return lockfile.split('_')[1]

    def cleanup_stale_locks(self, lockfiles=None):
        for lockfile in lockfiles:
            date = self.get_date_from_lockfilename(lockfile)
            if date:
                self.wiki.set_date(date)
                try:
                    StatusHtml.write_status(self.wiki, StatusHtml.status_line(
                        self.wiki, aborted=True))
                except Exception as ex:
                    # may be no directory to write into, if
                    # the dump failed early enough
                    pass
        self.unlock(lockfiles)

    # private....
    def get_lock_file_path(self):
        return os.path.join(self.wiki.private_dir(), "lock_{0}".format(self.date))

    def lock_age(self, lockfile=None):
        if lockfile is not None:
            return FileUtils.file_age(lockfile)
        else:
            return FileUtils.file_age(self.get_lock_file_path())


class LockWatchdog(threading.Thread):
    """Touch the given file every 10 seconds until asked to stop."""

    # For emergency aborts
    threads = []

    def __init__(self, lockfile):
        threading.Thread.__init__(self)
        self.lockfile = lockfile
        self.trigger = threading.Event()
        self.finished = threading.Event()

    def stop_watching(self):
        """Run me outside..."""
        # Ask the thread to stop...
        self.trigger.set()

        # Then wait for it, to ensure that the lock file
        # doesn't get touched again after we delete it on
        # the main thread.
        self.finished.wait(10)
        self.finished.clear()

    def run(self):
        LockWatchdog.threads.append(self)
        while not self.trigger.isSet():
            self.touch_lock()
            self.trigger.wait(10)
        self.trigger.clear()
        self.finished.set()
        LockWatchdog.threads.remove(self)

    def touch_lock(self):
        """Run me inside..."""
        os.utime(self.lockfile, None)


def cleanup():
    """Call cleanup handlers for any background threads..."""
    for watchdog in LockWatchdog.threads:
        watchdog.stop_watching()


if __name__ == "__main__":
    config_unused = Config()
    print "Config load ok!"
