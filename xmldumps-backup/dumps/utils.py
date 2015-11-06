'''
misc utils for dumps
'''

import os
import re
import sys
import time
import traceback
import socket

from os.path import exists
from subprocess import Popen, PIPE
from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError


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


class MultiVersion(object):
    def mw_script_as_string(config, maintenance_script):
        return " ".join(MultiVersion.mw_script_as_array(config, maintenance_script))

    def mw_script_as_array(config, maintenance_script):
        mw_script_location = os.path.join(config.wiki_dir, "multiversion", "MWScript.php")
        if exists(mw_script_location):
            return [mw_script_location, maintenance_script]
        else:
            return ["%s/maintenance/%s" % (config.wiki_dir, maintenance_script)]

    def mw_version(config, db_name):
        get_version_location = os.path.join(config.wiki_dir, "multiversion", "getMWVersion")
        if exists(get_version_location):
            # run the command for the wiki and get the version
            command = get_version_location + " " + db_name
            version = RunSimpleCommand.run_with_output(command, shell=True)
            if version:
                version = version.rstrip()
                return version
        return None

    mw_script_as_string = staticmethod(mw_script_as_string)
    mw_script_as_array = staticmethod(mw_script_as_array)
    mw_version = staticmethod(mw_version)


class DbServerInfo(object):
    def __init__(self, wiki, db_name, error_callback=None):
        self.wiki = wiki
        self.db_name = db_name
        self.error_callback = error_callback
        self.db_table_prefix = None
        self.get_db_server_and_prefix()

    def get_db_server_and_prefix(self):
        """Get the name of a slave server for our cluster; also get
        the prefix for all tables for the specific wiki ($wgDBprefix)"""
        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)
        command_list = MultiVersion.mw_script_as_array(self.wiki.config, "getSlaveServer.php")
        php_command = MiscUtils.shell_escape(self.wiki.config.php)
        db_name = MiscUtils.shell_escape(self.db_name)
        for i in range(0, len(command_list)):
            command_list[i] = MiscUtils.shell_escape(command_list[i])
        command = " ".join(command_list)
        command = "%s -q %s --wiki=%s --group=dump --globals" % (php_command, command, db_name)
        results = RunSimpleCommand.run_with_output(command, shell=True, log_callback=self.error_callback).strip()
        if not results:
            raise BackupError("Failed to get database connection " +
                              "information for %s, bailing." % self.wiki.config.php)
        # first line is the server, the second is an array of the globals, we need
        # the db table prefix out of those
        lines = results.splitlines()
        self.db_server = lines[0]
        self.db_port = None
        if ':' in self.db_server:
            self.db_server, _, self.db_port = self.db_server.rpartition(':')

        #       [wgDBprefix] =>
        wgdb_prefix_pattern = re.compile(r"\s+\[wgDBprefix\]\s+=>\s+(?P<prefix>.*)$")
        for line in lines:
            match = wgdb_prefix_pattern.match(line)
            if match:
                self.db_table_prefix = match.group('prefix').strip()
        if self.db_table_prefix is None:
            # if we didn't see this in the globals list, something is broken.
            raise BackupError("Failed to get database table prefix for %s, bailing."
                              % self.wiki.config.php)

    def mysql_standard_parameters(self):
        host = self.db_server
        if self.db_port and self.db_server.strip() == "localhost":
            # MySQL tools ignore port settings for host "localhost" and instead use IPC sockets,
            # so we rewrite the localhost to it's ip address
            host = socket.gethostbyname(self.db_server)

        params = ["-h", "%s" % host]  # Host
        if self.db_port:
            params += ["--port", "%s" % self.db_port]  # Port
        params += ["-u", "%s" % self.wiki.config.db_user]  # Username
        params += ["%s" % self.password_option()]  # Password
        return params

    def build_sql_command(self, query, pipeto=None):
        """Put together a command to execute an sql query to the server for this DB."""
        if not exists(self.wiki.config.mysql):
            raise BackupError("mysql command %s not found" % self.wiki.config.mysql)
        command = [["/bin/echo", "%s" % query],
                   ["%s" % self.wiki.config.mysql] + self.mysql_standard_parameters() + [
                       "%s" % self.db_name,
                       "-r"]]
        if pipeto:
            command.append([pipeto])
        return command

    def build_sqldump_command(self, table, pipeto=None):
        """Put together a command to dump a table from the current DB with mysqldump
        and save to a gzipped sql file."""
        if not exists(self.wiki.config.mysqldump):
            raise BackupError("mysqldump command %s not found" % self.wiki.config.mysqldump)
        command = [["%s" % self.wiki.config.mysqldump] + self.mysql_standard_parameters() + [
            "--opt", "--quick",
            "--skip-add-locks", "--skip-lock-tables",
            "%s" % self.db_name,
            "%s" % self.db_table_prefix + table]]
        if pipeto:
            command.append([pipeto])
        return command

    def run_sql_and_get_output(self, query):
        command = self.build_sql_command(query)
        proc = CommandPipeline(command, quiet=True)
        proc.run_pipeline_get_output()
        # fixme best to put the return code someplace along with any errors....
        if proc.exited_successfully() and (proc.output()):
            return proc.output()
        else:
            return None

    def password_option(self):
        """If you pass '-pfoo' mysql uses the password 'foo',
        but if you pass '-p' it prompts. Sigh."""
        if self.wiki.config.db_password == "":
            return None
        else:
            return "-p" + self.wiki.config.db_password


class RunSimpleCommand(object):
    def run_with_output(command, maxtries=3, shell=False, log_callback=None, retry_delay=5):
        """Run a command and return the output as a string.
        Raises BackupError on non-zero return code."""

        if type(command).__name__ == 'list':
            command_string = " ".join(command)
        else:
            command_string = command

        success = False
        error = "unknown"
        tries = 0
        while not success and tries < maxtries:
            proc = Popen(command, bufsize=64, shell=shell, stdout=PIPE, stderr=PIPE)
            output, error = proc.communicate()
            if not proc.returncode:
                success = True
            else:
                if log_callback is not None:
                    log_callback("Non-zero return code from '%s'" % command_string)
                time.sleep(retry_delay)
            tries = tries + 1

        if not success:
            if log_callback is not None:
                log_callback("Non-zero return code from '%s' after max retries" % command_string)
            if proc:
                raise BackupError("command '" + command_string +
                                  ("' failed with return code %s " % proc.returncode) +
                                  " and error '" + error + "'")
            else:
                raise BackupError("command '" + command_string +
                                  ("' failed") + " and error '" + error + "'")
        else:
            return output

    def run_with_no_output(command, maxtries=3, shell=False, log_callback=None, retry_delay=5):
        """Run a command, expecting no output.
        Raises BackupError on non-zero return code."""

        if type(command).__name__ == 'list':
            command_string = " ".join(command)
        else:
            command_string = command

        success = False
        error = "unknown"
        tries = 0
        while (not success) and tries < maxtries:
            proc = Popen(command, shell=shell, stderr=PIPE)
            # output will be None, we can ignore it
            output_unused, error = proc.communicate()
            if not proc.returncode:
                success = True
            else:
                time.sleep(retry_delay)
            tries = tries + 1
        if not success:
            if log_callback is not None:
                log_callback("Non-zero return code from '%s' after max retries" % command_string)
            raise BackupError("command '" + command_string +
                              ("' failed with return code %s " %
                               proc.returncode) + " and error '" + error + "'")

    run_with_output = staticmethod(run_with_output)
    run_with_no_output = staticmethod(run_with_no_output)


class PageAndEditStats(object):
    def __init__(self, wiki, db_name, error_callback=None):
        self.total_pages = None
        self.total_edits = None
        self.wiki = wiki
        self.db_name = db_name
        self.db_server_info = DbServerInfo(wiki, db_name, error_callback)
        self.get_statistics()

    def get_statistics(self):
        """Get statistics for the wiki"""

        query = "select MAX(page_id) from %spage;" % self.db_server_info.db_table_prefix
        results = None
        retries = 0
        maxretries = 5
        results = self.db_server_info.run_sql_and_get_output(query)
        while results is None and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            results = self.db_server_info.run_sql_and_get_output(query)
        if not results:
            return 1

        lines = results.splitlines()
        if lines and lines[1]:
            self.total_pages = int(lines[1])
        query = "select MAX(rev_id) from %srevision;" % self.db_server_info.db_table_prefix
        retries = 0
        results = None
        results = self.db_server_info.run_sql_and_get_output(query)
        while results is None and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            results = self.db_server_info.run_sql_and_get_output(query)
        if not results:
            return 1

        lines = results.splitlines()
        if lines and lines[1]:
            self.total_edits = int(lines[1])
        return 0

    def get_total_pages(self):
        return self.total_pages

    def get_total_edits(self):
        return self.total_edits


# so if the pages/revs_per_chunk_abstr/_history are just one number it means
# use that number for all the chunks, figure out yourself how many.
# otherwise we get passed alist that says "here's now many for each chunk and it's this many chunks.
# extra pages/revs go in the last chunk, stuck on the end. too bad. :-P
class Chunk(object,):
    def __init__(self, wiki, db_name, error_callback=None):

        self._db_name = db_name
        self.wiki = wiki
        self._chunks_enabled = self.wiki.config.chunks_enabled
        if self._chunks_enabled:
            self.stats = PageAndEditStats(self.wiki, self._db_name, error_callback)
            if not self.stats.total_edits or not self.stats.total_pages:
                raise BackupError("Failed to get DB stats, exiting")
            if self.wiki.config.chunks_for_abstract:
                # we add 200 padding to cover new pages that may be added
                pages_per_chunk = 200 + self.stats.total_pages/int(
                    self.wiki.config.chunks_for_abstract)
                self._pages_per_chunk_abstract = [pages_per_chunk for i in range(
                    0, int(self.wiki.config.chunks_for_abstract))]
            else:
                self._pages_per_chunk_abstract = self.convert_comma_sep(
                    self.wiki.config.pages_per_chunk_abstract)

            self._pages_per_chunk_history = self.convert_comma_sep(
                self.wiki.config.pages_per_chunk_history)
            self._revs_per_chunk_history = self.convert_comma_sep(
                self.wiki.config.revs_per_chunk_history)
            self._recombine_history = self.wiki.config.recombine_history
        else:
            self._pages_per_chunk_history = False
            self._revs_per_chunk_history = False
            self._pages_per_chunk_abstract = False
            self._recombine_history = False
        if self._chunks_enabled:
            if self._revs_per_chunk_history:
                if len(self._revs_per_chunk_history) == 1:
                    self._num_chunks_history = self.get_num_chunks_for_xml_dumps(
                        self.stats.total_edits, self._pages_per_chunk_history[0])
                    self._revs_per_chunk_history = [self._revs_per_chunk_history[0]
                                                    for i in range(self._num_chunks_history)]
                else:
                    self._num_chunks_history = len(self._revs_per_chunk_history)
                # here we should generate the number of pages per chunk based on number of revs.
                # ...next code update! FIXME
                # self._pages_per_chunk_history = ....
            elif self._pages_per_chunk_history:
                if len(self._pages_per_chunk_history) == 1:
                    self._num_chunks_history = self.get_num_chunks_for_xml_dumps(
                        self.stats.total_pages, self._pages_per_chunk_history[0])
                    self._pages_per_chunk_history = [self._pages_per_chunk_history[0]
                                                     for i in range(self._num_chunks_history)]
                else:
                    self._num_chunks_history = len(self._pages_per_chunk_history)
            else:
                self._num_chunks_history = 0

            if self._pages_per_chunk_abstract:
                if len(self._pages_per_chunk_abstract) == 1:
                    self._num_chunks_abstract = self.get_num_chunks_for_xml_dumps(
                        self.stats.total_pages, self._pages_per_chunk_abstract[0])
                    self._pages_per_chunk_abstract = [self._pages_per_chunk_abstract[0]
                                                      for i in range(self._num_chunks_abstract)]
                else:
                    self._num_chunks_abstract = len(self._pages_per_chunk_abstract)
            else:
                self._num_chunks_abstract = 0

    def convert_comma_sep(self, line):
        if line == "":
            return False
        result = line.split(',')
        numbers = []
        for field in result:
            field = field.strip()
            numbers.append(int(field))
        return numbers

    def get_pages_per_chunk_abstract(self):
        return self._pages_per_chunk_abstract

    def get_num_chunks_abstract(self):
        return self._num_chunks_abstract

    def get_pages_per_chunk_history(self):
        return self._pages_per_chunk_history

    def get_num_chunks_history(self):
        return self._num_chunks_history

    def chunks_enabled(self):
        return self._chunks_enabled

    def recombine_history(self):
        return self._recombine_history

    # args: total (pages or revs), and the number of (pages or revs) per chunk.
    def get_num_chunks_for_xml_dumps(self, total, per_chunk):
        if not total:
            # default: no chunking.
            return 0
        else:
            chunks = int(total/per_chunk)
            # more smaller chunks are better, we want speed
            if (total - (chunks * per_chunk)) > 0:
                chunks = chunks + 1
            if chunks == 1:
                return 0
            return chunks
