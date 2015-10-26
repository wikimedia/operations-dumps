import ConfigParser
from email.mime import text as MIMEText
import os
import shutil
import re
import smtplib
import socket
import sys
import threading
import time
import tempfile


class FileUtils(object):

    def file_age(filename):
        return time.time() - os.stat(filename).st_mtime

    def atomic_create(filename, mode='w'):
        """Create a file, aborting if it already exists..."""
        fhandle = os.open(filename, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
        return os.fdopen(fhandle, mode)

    def write_file(dirname, filename, text, perms=0):
        """Write text to a file, as atomically as possible,
        via a temporary file in a specified directory.
        Arguments: dirname = where temp file is created,
        filename = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation"""

        if not os.path.isdir(dirname):
            try:
                os.makedirs(dirname)
            except:
                raise IOError("The given directory '%s' is neither "
                              "a directory nor can it be created" % dirname)

        (fhandle, temp_filename) = tempfile.mkstemp("_txt", "wikidump_", dirname)
        os.write(fhandle, text)
        os.close(fhandle)
        if perms:
            os.chmod(temp_filename, perms)
        # This may fail across filesystems or on Windows.
        # Of course nothing else will work on Windows. ;)
        shutil.move(temp_filename, filename)

    def write_file_in_place(filename, text, perms=0):
        """Write text to a file, after opening it for write with truncation.
        This assumes that only one process or thread accesses the given file at a time.
        Arguments: filename = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation,
        if it did not exist already"""

        filehdl = open(filename, "wt")
        filehdl.write(text)
        filehdl.close()
        if perms:
            os.chmod(filename, perms)

    def read_file(filename):
        """Read text from a file in one fell swoop."""
        filehdl = open(filename, "r")
        text = filehdl.read()
        filehdl.close()
        return text

    def split_path(path):
        # For some reason, os.path.split only does one level.
        parts = []
        (path, filename) = os.path.split(path)
        if not filename:
            # Probably a final slash
            (path, filename) = os.path.split(path)
        while filename:
            parts.insert(0, filename)
            (path, filename) = os.path.split(path)
        return parts

    def relative_path(path, base):
        """Return a relative path to 'path' from the directory 'base'."""
        path = FileUtils.split_path(path)
        base = FileUtils.split_path(base)
        while base and path[0] == base[0]:
            path.pop(0)
            base.pop(0)
        for prefix_unused in base:
            path.insert(0, "..")
        return os.path.join(*path)

    def pretty_size(size):
        """Return a string with an attractively formatted file size."""
        quanta = ("%d bytes", "%d KB", "%0.1f MB", "%0.1f GB", "%0.1f TB")
        return FileUtils._pretty_size(size, quanta)

    def _pretty_size(size, quanta):
        if size < 1024 or len(quanta) == 1:
            return quanta[0] % size
        else:
            return FileUtils._pretty_size(size / 1024.0, quanta[1:])

    def file_info(path):
        """Return a tuple of date/time and size of a file, or None, None"""
        try:
            timestamp = time.gmtime(os.stat(path).st_mtime)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
            size = os.path.getsize(path)
            return (timestamp, size)
        except:
            return(None, None)

    file_age = staticmethod(file_age)
    atomic_create = staticmethod(atomic_create)
    write_file = staticmethod(write_file)
    write_file_in_place = staticmethod(write_file_in_place)
    read_file = staticmethod(read_file)
    split_path = staticmethod(split_path)
    relative_path = staticmethod(relative_path)
    pretty_size = staticmethod(pretty_size)
    _pretty_size = staticmethod(_pretty_size)
    file_info = staticmethod(file_info)


class TimeUtils(object):

    def today():
        return time.strftime("%Y%m%d", time.gmtime())

    def pretty_time():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def pretty_date(key):
        "Prettify a MediaWiki date key"
        return "-".join((key[0:4], key[4:6], key[6:8]))

    today = staticmethod(today)
    pretty_time = staticmethod(pretty_time)
    pretty_date = staticmethod(pretty_date)


class MiscUtils(object):
    def db_list(filename):
        """Read database list from a file"""
        if not filename:
            return []
        infile = open(filename)
        dbs = []
        for line in infile:
            line = line.strip()
            if line != "":
                dbs.append(line)
        infile.close()
        dbs.sort()
        return dbs

    def shell_escape(param):
        """Escape a string parameter, or set of strings, for the shell."""
        if isinstance(param, basestring):
            return "'" + param.replace("'", "'\\''") + "'"
        elif param is None:
            # A blank string might actually be needed; None means we can leave it out
            return ""
        else:
            return tuple([MiscUtils.shell_escape(x) for x in param])

    db_list = staticmethod(db_list)
    shell_escape = staticmethod(shell_escape)


class Config(object):
    def __init__(self, config_file=False):
        self.project_name = False

        home = os.path.dirname(sys.argv[0])
        if not config_file:
            config_file = "wikidump.conf"
        self.files = [
            os.path.join(home, config_file),
            "/etc/wikidump.conf",
            os.path.join(os.getenv("HOME"), ".wikidump.conf")]
        defaults = {
            # "wiki": {
            "dblist": "",
            "privatelist": "",
            "flaggedrevslist": "",
            "wikidatalist": "",
            "globalusagelist": "",
            "wikidataclientlist": "",
            # "dir": "",
            "forcenormal": "0",
            "halt": "0",
            "skipdblist": "",
            # "output": {
            "public": "/dumps/public",
            "private": "/dumps/private",
            "temp": "/dumps/temp",
            "webroot": "http://localhost/dumps",
            "index": "index.html",
            "templatedir": home,
            "perdumpindex": "index.html",
            "logfile": "dumplog.txt",
            "fileperms": "0640",
            # "reporting": {
            "adminmail": "root@localhost",
            "mailfrom": "root@localhost",
            "smtpserver": "localhost",
            "staleage": "3600",
            # "database": {
            # these are now set in get_db_user_and_password() if needed
            "user": "",
            "password": "",
            # "tools": {
            "php": "/bin/php",
            "gzip": "/usr/bin/gzip",
            "bzip2": "/usr/bin/bzip2",
            "sevenzip": "/bin/7za",
            "mysql": "/usr/bin/mysql",
            "mysqldump": "/usr/bin/mysqldump",
            "head": "/usr/bin/head",
            "tail": "/usr/bin/tail",
            "cat": "/bin/cat",
            "grep": "/bin/grep",
            "checkforbz2footer": "/usr/local/bin/checkforbz2footer",
            "writeuptopageid": "/usr/local/bin/writeuptopageid",
            "recoompressxml": "/usr/local/bin/recompressxml",
            # "cleanup": {
            "keep": "3",
            # "chunks": {
            # set this to 1 to enable runing the various xml dump stages as chunks in parallel
            "chunksEnabled": "0",
            # for page history runs, number of pages for each chunk, specified separately
            # e.g. "1000,10000,100000,2000000,2000000,2000000,2000000,2000000,2000000,2000000"
            # would define 10 chunks with the specified number of pages in each and any extra in
            # a final 11th chunk
            "pagesPerChunkHistory": False,
            # revs per chunk (roughly, it will be split along page lines)
            # for history and current dumps
            # values: positive integer, "compute",
            # this field is overriden by pagesPerChunkHistory
            # CURRENTLY NOT COMPLETE so please don't use this.
            "revsPerChunkHistory": False,
            # pages per chunk for abstract runs
            "pagesPerChunkAbstract": False,
            # number of chunks for abstract dumps, overrides pagesPerChunkAbstract
            "chunksForAbstract": 0,
            # whether or not to recombine the history pieces
            "recombineHistory": "1",
            # do we write out checkpoint files at regular intervals?
            # (article/metacurrent/metahistory dumps only.)
            "checkpointTime": "0",
            # "otherformats": {
            "multistream": "0",
            }
        self.conf = ConfigParser.SafeConfigParser(defaults)
        self.conf.read(self.files)

        if not self.conf.has_section("wiki"):
            print "The mandatory configuration section 'wiki' was not defined."
            raise ConfigParser.NoSectionError('wiki')

        if not self.conf.has_option("wiki", "dir"):
            print "The mandatory setting 'dir' in the section 'wiki' was not defined."
            raise ConfigParser.NoOptionError('wiki', 'dir')

        self.db_user = None
        self.db_password = None
        self.parse_conffile_globally()
        self.parse_conffile_per_project()
        self.get_db_user_and_password()  # get from MW adminsettings file if not set in conf file

    def parse_php_assignment(self, line):
        # not so much parse as grab a string to the right of the equals sign,
        # we expect a line that has  ... = "somestring" ;
        # with single or double quotes, spaes or not.  but nothing more complicated.
        equalspattern = r"=\s*(\"|')(.+)(\"|')\s*;"
        result = re.search(equalspattern, line)
        if result:
            return result.group(2)
        else:
            return ""

    def get_db_user_and_password(self):
        # check MW adminsettings file for these if we didn't have values for
        # them in the conf file; failing that we fall back on defaults specified
        # here

        if self.db_user:  # already set via conf file, don't override
            return

        default_dbuser = "root"
        default_dbpassword = ""

        if not self.conf.has_option("wiki", "adminsettings"):
            self.db_user = default_dbuser
            self.db_password = default_dbpassword
            return

        adminfile = open(os.path.join(self.wiki_dir, self.conf.get("wiki", "adminsettings")), "r")
        lines = adminfile.readlines()
        adminfile.close()

        # we are digging through a php file and expecting to find
        # lines more or less like the below.. anything more complicated we're not going to handle.
        # $wgDBadminuser = 'something';
        # $wgDBuser = $wgDBadminuser = "something" ;

        for line in lines:
            if "$wgDBadminuser" in line:
                self.db_user = self.parse_php_assignment(line)
            elif "$wgDBuser" in line:
                default_dbuser = self.parse_php_assignment(line)
            elif "$wgDBadminpassword" in line:
                self.db_password = self.parse_php_assignment(line)
            elif "$wgDBpassword" in line:
                default_dbpassword = self.parse_php_assignment(line)

        if not self.db_user:
            self.db_user = default_dbuser
        if not self.db_password:
            self.db_password = default_dbpassword
        return

    def parse_conffile_globally(self):
        self.db_list = MiscUtils.db_list(self.conf.get("wiki", "dblist"))
        self.skip_db_list = MiscUtils.db_list(self.conf.get("wiki", "skipdblist"))
        self.private_list = MiscUtils.db_list(self.conf.get("wiki", "privatelist"))
        self.flagged_revs_list = MiscUtils.db_list(self.conf.get("wiki", "flaggedrevslist"))
        self.wikidata_list = MiscUtils.db_list(self.conf.get("wiki", "wikidatalist"))
        self.global_usage_list = MiscUtils.db_list(self.conf.get("wiki", "globalusagelist"))
        self.wikidata_client_list = MiscUtils.db_list(self.conf.get("wiki", "wikidataclientlist"))
        self.halt = self.conf.getint("wiki", "halt")

        self.db_list = list(set(self.db_list) - set(self.skip_db_list))

        if not self.conf.has_section('output'):
            self.conf.add_section('output')
        self.public_dir = self.conf.get("output", "public")
        self.private_dir = self.conf.get("output", "private")
        self.temp_dir = self.conf.get("output", "temp")
        self.web_root = self.conf.get("output", "webroot")
        self.index = self.conf.get("output", "index")
        self.template_dir = self.conf.get("output", "templatedir")
        self.perdump_index = self.conf.get("output", "perdumpindex")
        self.log_file = self.conf.get("output", "logfile")
        self.fileperms = self.conf.get("output", "fileperms")
        self.fileperms = int(self.fileperms, 0)
        if not self.conf.has_section('reporting'):
            self.conf.add_section('reporting')
        self.admin_mail = self.conf.get("reporting", "adminmail")
        self.mail_from = self.conf.get("reporting", "mailfrom")
        self.smtp_server = self.conf.get("reporting", "smtpserver")
        self.stale_age = self.conf.getint("reporting", "staleage")

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

        if not self.conf.has_section('cleanup'):
            self.conf.add_section('cleanup')
        self.keep = self.conf.getint("cleanup", "keep")

    def parse_conffile_per_project(self, project_name=False):
        # we need to read from the project section without falling back
        # to the defaults, which has_option() normally does, ugh.  so set
        # up a local conf instance without the defaults
        conf = ConfigParser.SafeConfigParser()
        conf.read(self.files)

        if project_name:
            self.project_name = project_name

        if not self.conf.has_section('database'):
            self.conf.add_section('database')

        dbuser = self.get_opt_for_proj_or_default(conf, "database", "user", 0)
        if dbuser:
            self.db_user = dbuser
        dbpassword = self.get_opt_for_proj_or_default(conf, "database", "password", 0)
        if dbpassword:
            self.db_password = dbpassword

        if not self.conf.has_section('chunks'):
            self.conf.add_section('chunks')
        self.chunks_enabled = self.get_opt_for_proj_or_default(
            conf, "chunks", "chunksEnabled", 1)
        self.pages_per_chunk_history = self.get_opt_for_proj_or_default(
            conf, "chunks", "pagesPerChunkHistory", 0)
        self.revs_per_chunk_history = self.get_opt_for_proj_or_default(
            conf, "chunks", "revsPerChunkHistory", 0)
        self.chunks_for_abstract = self.get_opt_for_proj_or_default(
            conf, "chunks", "chunksForAbstract", 0)
        self.pages_per_chunk_abstract = self.get_opt_for_proj_or_default(
            conf, "chunks", "pagesPerChunkAbstract", 0)
        self.recombine_history = self.get_opt_for_proj_or_default(
            conf, "chunks", "recombineHistory", 1)
        self.checkpoint_time = self.get_opt_for_proj_or_default(
            conf, "chunks", "checkpointTime", 1)

        if not self.conf.has_section('otherformats'):
            self.conf.add_section('otherformats')
        self.multistream_enabled = self.get_opt_for_proj_or_default(
            conf, 'otherformats', 'multistream', 1)

        if not self.conf.has_section('wiki'):
            self.conf.add_section('wiki')
        self.wiki_dir = self.get_opt_for_proj_or_default(conf, "wiki", "dir", 0)

    def get_opt_for_proj_or_default(self, conf, section_name, item_name, is_int):
        if conf.has_section(self.project_name):
            if conf.has_option(self.project_name, item_name):
                if is_int:
                    return conf.getint(self.project_name, item_name)
                else:
                    return conf.get(self.project_name, item_name)
        if is_int:
            return self.conf.getint(section_name, item_name)
        else:
            return self.conf.get(section_name, item_name)

    def db_list_by_age(self, use_status_time=False):
        """
        Sort wikis in reverse order of last successful dump :

        Order is (DumpFailed, Age), and False < True :
        First, wikis whose latest dump was successful, most recent dump first
        Then, wikis whose latest dump failed, most recent dump first.
        Finally, wikis which have never been dumped.

        According to that sort, the last item of this list is, when applicable,
        the oldest failed dump attempt.

        If some error occurs checking a dump status, that dump is put last in the
        list (sort value is (True, maxint) )

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

            age = sys.maxint
            date = sys.maxint
            last = wiki.latest_dump()
            status = ''
            if last:
                dump_status = os.path.join(wiki.public_dir(), last, "status.html")
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
                except:
                    print "dump dir missing status file %s?" % dump_status
            dump_failed = (status == '') or ('dump aborted' in status)
            available.append((dump_failed, date, age, dbname))
        available.sort()
        return [dbname for (_failed, _date, _age, dbname) in available]

    def read_template(self, name):
        template = os.path.join(self.template_dir, name)
        return FileUtils.read_file(template)

    def mail(self, subject, body):
        """Send out a quickie email."""
        message = MIMEText.MIMEText(body)
        message["Subject"] = subject
        message["From"] = self.mail_from
        message["To"] = self.admin_mail

        try:
            server = smtplib.SMTP(self.smtp_server)
            server.sendmail(self.mail_from, self.admin_mail, message.as_string())
            server.close()
        except:
            print "MAIL SEND FAILED! GODDAMIT! Was sending this mail:"
            print message


class Wiki(object):
    def __init__(self, config, db_name):
        self.config = config
        self.db_name = db_name
        self.date = None
        self.watchdog = None

    def is_private(self):
        return self.db_name in self.config.private_list

    def has_flagged_revs(self):
        return self.db_name in self.config.flagged_revs_list

    def has_wikidata(self):
        return self.db_name in self.config.wikidata_list

    def has_global_usage(self):
        return self.db_name in self.config.global_usage_list

    def is_wikidata_client(self):
        return self.db_name in self.config.wikidata_client_list

    def is_locked(self):
        return os.path.exists(self.lock_file())

    def is_stale(self):
        if not self.is_locked():
            return False
        try:
            age = self.lock_age()
            return age > self.config.stale_age
        except:
            # Lock file vanished while we were looking
            return False

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
            web_root_rel = web_root_rel[i+3:]
        i = web_root_rel.find("/")
        if i >= 0:
            web_root_rel = web_root_rel[i:]
        return web_root_rel

    # Actions!

    def lock(self):
        if not os.path.isdir(self.private_dir()):
            try:
                os.makedirs(self.private_dir())
            except:
                # Maybe it was just created (race condition)?
                if not os.path.isdir(self.private_dir()):
                    raise
        lockf = FileUtils.atomic_create(self.lock_file(), "w")
        lockf.write("%s %d" % (socket.getfqdn(), os.getpid()))
        lockf.close()

        self.watchdog = LockWatchdog(self.lock_file())
        self.watchdog.start()
        return True

    def unlock(self):
        if self.watchdog:
            self.watchdog.stop_watching()
            self.watchdog = None
        os.remove(self.lock_file())

    def set_date(self, date):
        self.date = date

    def cleanup_stale_lock(self):
        date = self.latest_dump()
        if date:
            self.set_date(date)
            self.write_status(self.report_statusline(
                "<span class=\"failed\">dump aborted</span>"))
        self.unlock()

    def write_perdump_index(self, html):
        index = os.path.join(self.public_dir(), self.date, self.config.perdump_index)
        FileUtils.write_file_in_place(index, html, self.config.fileperms)

    def exists_perdump_index(self):
        index = os.path.join(self.public_dir(), self.date, self.config.perdump_index)
        return os.path.exists(index)

    def write_status(self, message):
        index = os.path.join(self.public_dir(), self.date, "status.html")
        FileUtils.write_file_in_place(index, message, self.config.fileperms)

    def status_line(self):
        date = self.latest_dump()
        if date:
            status = os.path.join(self.public_dir(), date, "status.html")
            try:
                return FileUtils.read_file(status)
            except:
                return self.report_statusline("missing status record")
        else:
            return self.report_statusline("has not yet been dumped")

    def report_statusline(self, status, error=False):
        if error:
            # No state information, hide the timestamp
            stamp = "<span style=\"visible: none\">" + TimeUtils.pretty_time() + "</span>"
        else:
            stamp = TimeUtils.pretty_time()
        if self.is_private():
            link = "%s (private data)" % self.db_name
        else:
            if self.date:
                link = "<a href=\"%s/%s\">%s</a>" % (self.db_name, self.date, self.db_name)
            else:
                link = "%s (new)" % self.db_name
        return "<li>%s %s: %s</li>\n" % (stamp, link, status)

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
            dump_status = os.path.join(self.public_dir(), last, "status.html")
            try:
                mtime = os.stat(dump_status).st_mtime
            except:
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
        except OSError:
            return []
        dates.sort()
        return dates

    # private....
    def lock_file(self):
        return os.path.join(self.private_dir(), "lock")

    def lock_age(self):
        return FileUtils.file_age(self.lock_file())


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
