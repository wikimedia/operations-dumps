'''
misc utils for dumps
'''

import os
from os.path import exists
import re
import time
import socket

from subprocess import Popen, PIPE
from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError


class MiscUtils(object):
    @staticmethod
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

    @staticmethod
    def shell_escape(param):
        """Escape a string parameter, or set of strings, for the shell."""
        if isinstance(param, basestring):
            return "'" + param.replace("'", "'\\''") + "'"
        elif param is None:
            # A blank string might actually be needed; None means we can leave it out
            return ""
        else:
            return tuple([MiscUtils.shell_escape(x) for x in param])


class TimeUtils(object):
    @staticmethod
    def today():
        return time.strftime("%Y%m%d", time.gmtime())

    @staticmethod
    def pretty_time():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    @staticmethod
    def pretty_date(key):
        "Prettify a MediaWiki date key"
        return "-".join((key[0:4], key[4:6], key[6:8]))


class MultiVersion(object):
    @staticmethod
    def mw_script_as_string(config, maintenance_script):
        return " ".join(MultiVersion.mw_script_as_array(config, maintenance_script))

    @staticmethod
    def mw_script_as_array(config, maintenance_script):
        mw_script_location = os.path.join(config.wiki_dir, "multiversion", "MWScript.php")
        if exists(mw_script_location):
            return [mw_script_location, maintenance_script]
        else:
            return ["%s/maintenance/%s" % (config.wiki_dir, maintenance_script)]

    @staticmethod
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


class DbServerInfo(object):
    def __init__(self, wiki, db_name, error_callback=None):
        self.wiki = wiki
        self.db_name = db_name
        self.error_callback = error_callback
        self.db_table_prefix = None
        self.db_server = None
        self.db_port = None
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
        command = "%s %s --wiki=%s --group=dump --globals" % (php_command, command, db_name)
        results = RunSimpleCommand.run_with_output(
            command, shell=True, log_callback=self.error_callback).strip()
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
    @staticmethod
    def run_with_output(command, maxtries=3, shell=False, log_callback=None,
                        retry_delay=5, verbose=False):
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

    @staticmethod
    def run_with_no_output(command, maxtries=3, shell=False, log_callback=None,
                           retry_delay=5, verbose=False):
        """Run a command, expecting no output.
        Raises BackupError on non-zero return code."""

        if type(command).__name__ == 'list':
            command_string = " ".join(command)
        else:
            command_string = command
        if verbose:
            print "command to be run with no output: ", command_string
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
        return success


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


# so if the pages/revs_per_filepart_abstr/_history are just one number it means
# use that number for all the parts, figure out yourself how many.
# otherwise we get passed alist that says "here's now many for each filepart and it's this many parts.
# extra pages/revs go in the last filepart, stuck on the end. too bad. :-P
class FilePartInfo(object,):
    '''
    when we split a job into a set of subjobs to be run in parallel, each
    producing one part of the output, these parts of the output file could
    and sometimes are recombined to make the single output file that would
    have been produced had we not split the job up.
    each of these file 'parts' is numbered so that we know which order
    is right for recombining them.
    additionally any one of these parts can be regenerated by itself if
    something goes wrong, without the need for regenerating all the rest.
    typically one of these parts is produced by requesting that a job run
    over a specified page id range rather than over all pages, but that is
    not our concern here.
    '''
    def __init__(self, wiki, db_name, error_callback=None):

        self._db_name = db_name
        self.wiki = wiki
        self._parts_enabled = self.wiki.config.parts_enabled
        if self._parts_enabled:
            self.stats = PageAndEditStats(self.wiki, self._db_name, error_callback)
            if not self.stats.total_edits or not self.stats.total_pages:
                raise BackupError("Failed to get DB stats, exiting")
            if self.wiki.config.numparts_for_abstract:
                # we add 200 padding to cover new pages that may be added
                pages_per_filepart = 200 + self.stats.total_pages / int(
                    self.wiki.config.numparts_for_abstract)
                self._pages_per_filepart_abstract = [pages_per_filepart for i in range(
                    0, int(self.wiki.config.numparts_for_abstract))]
            else:
                self._pages_per_filepart_abstract = self.convert_comma_sep(
                    self.wiki.config.pages_per_filepart_abstract)

            self._pages_per_filepart_history = self.convert_comma_sep(
                self.wiki.config.pages_per_filepart_history)
            self._revs_per_filepart_history = self.convert_comma_sep(
                self.wiki.config.revs_per_filepart_history)
            self._recombine_history = self.wiki.config.recombine_history
        else:
            self._pages_per_filepart_history = False
            self._revs_per_filepart_history = False
            self._pages_per_filepart_abstract = False
            self._recombine_history = False
        if self._parts_enabled:
            if self._revs_per_filepart_history:
                if len(self._revs_per_filepart_history) == 1:
                    self._num_parts_history = self.get_num_parts_for_xml_dumps(
                        self.stats.total_edits, self._pages_per_filepart_history[0])
                    self._revs_per_filepart_history = [self._revs_per_filepart_history[0]
                                                       for i in range(self._num_parts_history)]
                else:
                    self._num_parts_history = len(self._revs_per_filepart_history)
                # here we should generate the number of pages per filepart based on number of revs.
                # ...next code update! FIXME
                # self._pages_per_filepart_history = ....
            elif self._pages_per_filepart_history:
                if len(self._pages_per_filepart_history) == 1:
                    self._num_parts_history = self.get_num_parts_for_xml_dumps(
                        self.stats.total_pages, self._pages_per_filepart_history[0])
                    self._pages_per_filepart_history = [self._pages_per_filepart_history[0]
                                                        for i in range(self._num_parts_history)]
                else:
                    self._num_parts_history = len(self._pages_per_filepart_history)
            else:
                self._num_parts_history = 0

            if self._pages_per_filepart_abstract:
                if len(self._pages_per_filepart_abstract) == 1:
                    self._num_parts_abstract = self.get_num_parts_for_xml_dumps(
                        self.stats.total_pages, self._pages_per_filepart_abstract[0])
                    self._pages_per_filepart_abstract = [self._pages_per_filepart_abstract[0]
                                                         for i in range(self._num_parts_abstract)]
                else:
                    self._num_parts_abstract = len(self._pages_per_filepart_abstract)
            else:
                self._num_parts_abstract = 0

    def convert_comma_sep(self, line):
        if line == "":
            return False
        result = line.split(',')
        numbers = []
        for field in result:
            field = field.strip()
            numbers.append(int(field))
        return numbers

    def get_pages_per_filepart_abstract(self):
        return self._pages_per_filepart_abstract

    def get_num_parts_abstract(self):
        return self._num_parts_abstract

    def get_pages_per_filepart_history(self):
        return self._pages_per_filepart_history

    def get_num_parts_history(self):
        return self._num_parts_history

    def parts_enabled(self):
        return self._parts_enabled

    def recombine_history(self):
        return self._recombine_history

    # args: total (pages or revs), and the number of (pages or revs) per filepart.
    def get_num_parts_for_xml_dumps(self, total, per_filepart):
        if not total:
            # default: no file parts.
            return 0
        else:
            parts = int(total / per_filepart)
            # more smaller parts are better, we want speed
            if (total - (parts * per_filepart)) > 0:
                parts = parts + 1
            if parts == 1:
                return 0
            return parts
