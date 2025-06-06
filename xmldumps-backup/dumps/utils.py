#!/usr/bin/python3
'''
misc utils for dumps
'''

import json
import os
from os.path import exists
import time
import socket
import signal

from subprocess import Popen, PIPE
from dumps.commandmanagement import CommandPipeline
from dumps.exceptions import BackupError


class MiscUtils():
    @staticmethod
    def db_list(path, nosort=False):
        """Read database list from a file"""
        if not path:
            return []
        infhandle = open(path)
        dbs = []
        for line in infhandle:
            line = line.strip()
            if line != "" and not line.startswith('#'):
                dbs.append(line)
        infhandle.close()
        if not nosort:
            dbs = sorted(dbs)
        return dbs

    @staticmethod
    def shell_escape(param):
        """Escape a string parameter, or set of strings, for the shell."""
        if isinstance(param, str):
            return "'" + param.replace("'", "'\\''") + "'"
        if param is None:
            # A blank string might actually be needed; None means we can leave it out
            return ""
        return tuple([MiscUtils.shell_escape(x) for x in param])

    @staticmethod
    def get_sigpipe_values():
        """Return a list of the two numeric values that we could get back from
        a process interrupted with SIGPIPE, depending on python implementation;
        signal.SIGPIPE changed in python 3.5 from int to enum"""
        try:
            sigpipe_value = signal.SIGPIPE.value
        except AttributeError:
            sigpipe_value = signal.SIGPIPE
        return [-1 * sigpipe_value, sigpipe_value + 128]


class TimeUtils():
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


class MultiVersion():
    @staticmethod
    def mw_script_as_array(config, maintenance_script):
        mw_script_location = os.path.join(config.multiversion, "MWScript.php")
        if exists(mw_script_location):
            return [mw_script_location, maintenance_script]
        if maintenance_script.startswith('extensions'):
            return ["%s/%s" % (config.wiki_dir, maintenance_script)]
        return ["%s/maintenance/%s" % (config.wiki_dir, maintenance_script)]

    @staticmethod
    def mw_version(config, db_name):
        get_version_location = os.path.join(config.multiversion, "getMWVersion")
        if exists(get_version_location):
            # run the command for the wiki and get the version
            command = get_version_location + " " + db_name
            version = RunSimpleCommand.run_with_output(command, shell=True)
            if version:
                version = version.rstrip().decode('utf-8')
                return version
        return None


class DbServerInfo():
    def __init__(self, wiki, db_name, error_callback=None):
        self.wiki = wiki
        self.db_name = db_name
        self.error_callback = error_callback
        self.db_table_prefix = None
        self.db_server = None
        self.db_port = None
        self.apibase = None

    def get_db_server_and_prefix(self, do_globals=True):
        """
        Get the name of a db replica for our cluster; also get
        the prefix for all tables for the specific wiki ($wgDBprefix),
        and set attributes db_server, db_port

        if do_globals is True, also get global variables
        and set attributes db_table_prefix, apibase
        """
        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)
        command_list = MultiVersion.mw_script_as_array(self.wiki.config, "getReplicaServer.php")
        command = "{php} {command} --wiki={dbname} --group=dump".format(
            php=MiscUtils.shell_escape(self.wiki.config.php),
            command=" ".join(command_list),
            dbname=MiscUtils.shell_escape(self.db_name))
        results = RunSimpleCommand.run_with_output(
            command, shell=True, log_callback=self.error_callback).strip()
        if not results:
            raise BackupError("Failed to get database connection " +
                              "information for %s, bailing." % self.wiki.config.php)
        # first line is the server, the second is an array of the globals (if any), we need
        # the db table prefix out of those
        lines = results.decode('utf-8').splitlines()
        self.db_server = lines[0]
        self.db_port = ""
        if ':' in self.db_server:
            self.db_server, _, self.db_port = self.db_server.rpartition(':')

        if do_globals:
            self.get_config_variables()

    def get_config_variables(self):
        '''
        retrieve settings for certain MW global variables
        via maintenance script
        then set the db table prefix, and the base path to
        the MW api for the wiki
        '''
        command_list = MultiVersion.mw_script_as_array(self.wiki.config, "getConfiguration.php")
        pull_vars = ["wgDBprefix", "wgCanonicalServer", "wgScriptPath"]
        command = "{php} {command} --wiki={dbname} --format=json --regex='{vars}'"
        command = command.format(
            php=MiscUtils.shell_escape(self.wiki.config.php),
            command=" ".join(command_list),
            dbname=MiscUtils.shell_escape(self.db_name),
            vars="|".join(pull_vars))
        results = RunSimpleCommand.run_with_output(
            command, shell=True, log_callback=self.error_callback).strip()
        settings = json.loads(results.decode('utf-8'))
        if not settings or len(settings) != 3:
            raise BackupError(
                "Failed to get values for wgDBprefix, wgCanonicalServer, " +
                "wgScriptPath for {wiki}".format(wiki=self.db_name))

        self.db_table_prefix = settings['wgDBprefix']
        self.api_host = settings['wgCanonicalServer'].replace('https://', '')
        if is_runnning_in_kubernetes():
            wgcanonserver = os.environ['ENVOY_MW_API_HOST']
        else:
            wgcanonserver = settings['wgCanonicalServer']
        wgscriptpath = settings['wgScriptPath']

        self.apibase = "/".join([
            wgcanonserver.rstrip('/'),
            wgscriptpath.strip('/'),
            "api.php"])

    def mysql_standard_parameters(self):
        host = self.get_attr('db_server')
        if self.get_attr('db_port') and self.get_attr('db_server').strip() == "localhost":
            # MySQL tools ignore port settings for host "localhost" and instead use IPC sockets,
            # so we rewrite the localhost to it's ip address
            host = socket.gethostbyname(self.get_attr('db_server'))

        params = ["-h", "%s" % host]  # Host
        if self.get_attr('db_port'):
            params += ["--port", self.get_attr('db_port')]
        self.wiki.set_db_creds()
        params += ["-u", self.wiki.db_user, self.password_option()]
        params += ["--max_allowed_packet=%s" % self.wiki.config.max_allowed_packet]
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
            "%s" % self.get_attr('db_table_prefix') + table]]
        if pipeto:
            command.append([pipeto])
        return command

    def run_sql_and_get_output(self, query):
        if self.db_server is None:
            self.get_db_server_and_prefix()
        command = self.build_sql_command(query)
        proc = CommandPipeline(command, quiet=True)
        proc.run_pipeline_get_output()
        # fixme best to put the return code someplace along with any errors....
        if proc.exited_successfully() and (proc.output()):
            return proc.output()
        return None

    def run_sql_query_with_retries(self, query, maxretries=3):
        """
        run the supplied sql query, retrying up to
        maxtretries times, with sleep of 5 secs in between
        this will recheck the db config in case a db server
        has been removed from the pool suddenly
        """
        if self.db_server is None:
            self.get_db_server_and_prefix()
        results = None
        retries = 0
        while results is None and retries < maxretries:
            retries = retries + 1
            results = self.run_sql_and_get_output(query)
            if results is None:
                time.sleep(5)
                # get db config again in case something's changed
                self.get_db_server_and_prefix()
                continue
            return results
        return results

    def password_option(self):
        """If you pass '-pfoo' mysql uses the password 'foo',
        but if you pass '-p' it prompts. Sigh."""
        if self.wiki.db_password == "":
            return None
        return "-p" + self.wiki.db_password

    def get_attr(self, attribute):
        """
        lazyload expensive attributes, only get them when requested
        mw maintenance script gets all of them at once
        """
        if getattr(self, attribute) is None:
            self.get_db_server_and_prefix()  # mw maintenance script
        return getattr(self, attribute)


class RunSimpleCommand():
    @staticmethod
    def run_with_output(command, maxtries=3, shell=False, log_callback=None,
                        retry_delay=5):
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
                                  " and error '" + error.decode('utf-8') + "'")
            raise BackupError("command '" + command_string +
                              ("' failed") + " and error '" + error.decode('utf-8') + "'")
        return output

    @staticmethod
    def run_with_no_output(command, maxtries=3, shell=False, log_callback=None,
                           retry_delay=5, verbose=False, timeout=None,
                           timeout_callback=None):
        """Run a command, expecting no output.
        Raises BackupError on non-zero return code."""

        if type(command).__name__ == 'list':
            command_string = " ".join(command)
        else:
            command_string = command
        if verbose:
            print("command to be run with no output: ", command_string)
        success = False
        error = "unknown"
        tries = 0
        while (not success) and tries < maxtries:
            proc = Popen(command, shell=shell, stderr=PIPE)
            returncode = None
            while returncode is None:
                output, error = proc.communicate(timeout=timeout)
                if error is not None:
                    error = error.decode('utf-8')
                if output is not None:
                    output = output.decode('utf-8')
                # FIXME we need to catch errors n stuff
                returncode = proc.returncode
                if timeout_callback is not None:
                    timeout_callback(output, error)
            if not returncode:
                success = True
            else:
                time.sleep(retry_delay)
            tries = tries + 1
        if not success:
            if log_callback is not None:
                log_callback("Non-zero return code from '%s' after max retries" % command_string)
            raise BackupError("command '" + command_string +
                              ("' failed with return code %s " %
                               returncode) + " and error '" + error + "'")
        return success


class PageAndEditStats():
    def __init__(self, wiki, db_name, error_callback=None):
        self.total_pages = None
        self.total_logitems = None
        self.total_edits = None
        self.wiki = wiki
        self.db_name = db_name
        self.db_server_info = DbServerInfo(wiki, db_name, error_callback)
        self.get_statistics()

    def get_statistics(self):
        """Get statistics for the wiki"""

        query = "select MAX(page_id) from %spage;" % self.db_server_info.get_attr('db_table_prefix')
        results = None
        retries = 0
        maxretries = self.wiki.config.max_retries
        results = self.db_server_info.run_sql_and_get_output(query)
        while results is None and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            results = self.db_server_info.run_sql_and_get_output(query)
        if not results:
            return 1

        lines = results.splitlines()
        if lines and lines[1] and lines[1] != b'NULL':
            self.total_pages = int(lines[1])

        query = "select MAX(rev_id) from %srevision;" % self.db_server_info.get_attr(
            'db_table_prefix')
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
        if lines and lines[1] and lines[1] != b'NULL':
            self.total_edits = int(lines[1])

        query = "select MAX(log_id) from %slogging;" % self.db_server_info.get_attr(
            'db_table_prefix')
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
        if lines and lines[1] and lines[1] != b'NULL':
            self.total_logitems = int(lines[1])

        return 0

    def get_total_pages(self):
        if self.total_pages is None:
            self.get_statistics()
        return self.total_pages

    def get_total_logitems(self):
        if self.total_logitems is None:
            self.get_statistics()
        return self.total_logitems

    def get_total_edits(self):
        if self.total_edits is None:
            self.get_statistics()
        return self.total_edits


# so if the pages/revs_per_filepart_abstr/_history are just one number it means
# use that number for all the parts, figure out yourself how many.
# otherwise we get passed a list that says "here's now many for each filepart
# and it's this many parts.
# extra pages/revs go in the last filepart, stuck on the end. too bad. :-P
class FilePartInfo():
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
    @staticmethod
    def convert_comma_sep(line):
        if line == "":
            return False
        result = line.split(',')
        numbers = []
        for field in result:
            field = field.strip()
            numbers.append(int(field))
        return numbers

    def __init__(self, wiki, db_name, error_callback=None):
        self._db_name = db_name
        self.error_callback = error_callback
        self.wiki = wiki
        self._parts_enabled = self.wiki.config.parts_enabled
        self.stats = None
        self._logitems_per_filepart_pagelogs = None
        self._pages_per_filepart_history = None
        self._revs_per_filepart_history = None
        self._logitems_per_filepart_pagelogs = None
        self._recombine_metacurrent = None
        self._recombine_history = None
        self._num_parts_pagelogs = None
        self._num_parts_history = None
        self.init_cheap_stuff()

    def init_for_parts(self):
        self._pages_per_filepart_history = self.convert_comma_sep(
            self.wiki.config.pages_per_filepart_history)
        self._revs_per_filepart_history = self.convert_comma_sep(
            self.wiki.config.revs_per_filepart_history)
        self._recombine_metacurrent = self.wiki.config.recombine_metacurrent
        self._recombine_history = self.wiki.config.recombine_history

    def get_some_stats(self):
        """
        if all goes well, polls the db and sets the following attributes:
        _logitems_per_filepart_pagelogs
        """
        if not self.stats:
            self.stats = PageAndEditStats(self.wiki, self._db_name, self.error_callback)
        if not self.stats.get_total_edits() or not self.stats.get_total_pages():
            raise BackupError("Failed to get DB stats, exiting")

        if (self.wiki.config.numparts_for_pagelogs and
                self.wiki.config.numparts_for_pagelogs != "0"):
            # we add 200 padding to cover new log entries that may be added
            logitems_per_filepart = 200 + self.stats.get_total_logitems() / int(
                self.wiki.config.numparts_for_pagelogs)
            self._logitems_per_filepart_pagelogs = [logitems_per_filepart for i in range(
                0, int(self.wiki.config.numparts_for_pagelogs))]
        else:
            self._logitems_per_filepart_pagelogs = self.convert_comma_sep(
                self.wiki.config.logitems_per_filepart_pagelogs)

    def set_stats_false(self):
        self._pages_per_filepart_history = False
        self._revs_per_filepart_history = False
        self._logitems_per_filepart_pagelogs = False
        self._recombine_metacurrent = False
        self._recombine_history = False

    def set_pagelog_parts(self):
        """
        set the _num_parts_pagelogs attribute
        """
        if self._logitems_per_filepart_pagelogs:
            if (len(self._logitems_per_filepart_pagelogs) == 1 and
                    self._logitems_per_filepart_pagelogs[0]):
                self._num_parts_pagelogs = self.get_num_parts_for_xml_dumps(
                    self.stats.get_total_logitems(), self._logitems_per_filepart_pagelogs[0])
                self._logitems_per_filepart_pagelogs = [
                    self._logitems_per_filepart_pagelogs[0]
                    for i in range(self._num_parts_pagelogs)]
            else:
                self._num_parts_pagelogs = len(self._logitems_per_filepart_pagelogs)
        else:
            self._num_parts_pagelogs = 0

    def set_revs_or_pages_parts(self):
        """
        set the attributes:
        _revs_per_filepart_history
        _num_parts_history
        _pages_per_filepart_history
        _num_parts_history
        """
        if self._revs_per_filepart_history:
            if (len(self._revs_per_filepart_history) == 1 and
                    self._revs_per_filepart_history[0]):
                self._num_parts_history = self.get_num_parts_for_xml_dumps(
                    self.stats.get_total_edits(), self._pages_per_filepart_history[0])
                self._revs_per_filepart_history = [self._revs_per_filepart_history[0]
                                                   for i in range(self._num_parts_history)]
            else:
                self._num_parts_history = len(self._revs_per_filepart_history)
                # here we should generate the number of pages per filepart based on number of revs.
                # ...next code update! FIXME
                # self._pages_per_filepart_history = ....
        elif self._pages_per_filepart_history:
            if (len(self._pages_per_filepart_history) == 1 and
                    self._pages_per_filepart_history[0]):
                self._num_parts_history = self.get_num_parts_for_xml_dumps(
                    self.stats.get_total_pages(), self._pages_per_filepart_history[0])
                self._pages_per_filepart_history = [self._pages_per_filepart_history[0]
                                                    for i in range(self._num_parts_history)]
            else:
                self._num_parts_history = len(self._pages_per_filepart_history)
        else:
            self._num_parts_history = 0

    def init_cheap_stuff(self):
        if self._parts_enabled:
            self.init_for_parts()
        else:
            self.set_stats_false()

    def get_expensive_stuff(self):
        """
        set all attributes that need a db lookup to get things like
        number of pages, log entries, etc
        """
        if self._parts_enabled:
            self.get_some_stats()  # db access
            self.set_revs_or_pages_parts()
            self.set_pagelog_parts()

    def get_attr(self, attribute):
        """
        lazyload expensive attributes, only get them when requested
        db query gets all of them at once
        """
        if getattr(self, attribute) is None:
            self.get_expensive_stuff()
        return getattr(self, attribute)

    def parts_enabled(self):
        return self._parts_enabled

    # args: total (pages or revs), and the number of (pages or revs) per filepart.
    def get_num_parts_for_xml_dumps(self, total, per_filepart):
        if not total:
            # default: no file parts.
            return 0
        parts = int(total / per_filepart)
        # more smaller parts are better, we want speed
        if (total - (parts * per_filepart)) > 0:
            parts = parts + 1
        if parts == 1:
            return 0
        return parts


def redact_command_parameters(command):
    redacted_command = []
    for part in command:
        if 'mysql' in command[0] and part.startswith('-p'):
            part = '-p[REDACTED]'
        redacted_command.append(part)
    return redacted_command


def is_runnning_in_kubernetes():
    return (
        # This file always exists in Kubernetes containers
        exists("/run/secrets/kubernetes.io/serviceaccount/token")
    )


def is_nested_list_empty(nested_list):
    """Return true if every sublist in the argument list is recursively composed of empty lists"""
    if isinstance(nested_list, list):
        return all(map(is_nested_list_empty, nested_list))
    return False
