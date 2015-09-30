'''
misc utils for dumps
'''

import os, re, sys, time
import traceback, socket

from os.path import exists
from subprocess import Popen, PIPE
from dumps.WikiDump import FileUtils, MiscUtils
from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError

class MultiVersion(object):
    def MWScriptAsString(config, maintenance_script):
        return " ".join(MultiVersion.MWScriptAsArray(config, maintenance_script))

    def MWScriptAsArray(config, maintenance_script):
        mw_script_location = os.path.join(config.wikiDir, "multiversion", "MWScript.php")
        if exists(mw_script_location):
            return [mw_script_location, maintenance_script]
        else:
            return ["%s/maintenance/%s" % (config.wikiDir, maintenance_script)]

    def MWVersion(config, db_name):
        get_version_location = os.path.join(config.wikiDir, "multiversion", "getMWVersion")
        if exists(get_version_location):
            # run the command for the wiki and get the version
            command =  get_version_location + " " +  db_name
            version = RunSimpleCommand.runAndReturn(command)
            if version:
                version = version.rstrip()
                return version
        return None

    MWScriptAsString = staticmethod(MWScriptAsString)
    MWScriptAsArray = staticmethod(MWScriptAsArray)
    MWVersion = staticmethod(MWVersion)

class DbServerInfo(object):
    def __init__(self, wiki, db_name, error_callback=None):
        self.wiki = wiki
        self.dbName = db_name
        self.errorCallback = error_callback
        self.db_table_prefix = None
        self.get_db_server_and_prefix()

    def get_db_server_and_prefix(self):
        """Get the name of a slave server for our cluster; also get
        the prefix for all tables for the specific wiki ($wgDBprefix)"""
        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)
        command_list = MultiVersion.MWScriptAsArray(self.wiki.config, "getSlaveServer.php")
        php_command = MiscUtils.shellEscape(self.wiki.config.php)
        db_name = MiscUtils.shellEscape(self.dbName)
        for i in range(0, len(command_list)):
            command_list[i] = MiscUtils.shellEscape(command_list[i])
        command = " ".join(command_list)
        command = "%s -q %s --wiki=%s --group=dump --globals" % (php_command, command, db_name)
        results = RunSimpleCommand.runAndReturn(command, self.errorCallback).strip()
        if not results:
            raise BackupError("Failed to get database connection information for %s, bailing." % self.wiki.config.php)
        # first line is the server, the second is an array of the globals, we need the db table prefix out of those
        lines = results.splitlines()
        self.dbServer = lines[0]
        self.db_port = None
        if ':' in self.dbServer:
            self.dbServer, _, self.db_port = self.dbServer.rpartition(':')

        #       [wgDBprefix] =>
        wgdb_prefix_pattern = re.compile("\s+\[wgDBprefix\]\s+=>\s+(?P<prefix>.*)$")
        for line in lines:
            match = wgdb_prefix_pattern.match(line)
            if match:
                self.db_table_prefix = match.group('prefix').strip()
        if self.db_table_prefix == None:
            # if we didn't see this in the globals list, something is broken.
            raise BackupError("Failed to get database table prefix for %s, bailing." % self.wiki.config.php)

    def mysql_standard_parameters(self):
        host = self.dbServer
        if self.db_port and self.dbServer.strip() == "localhost":
            # MySQL tools ignore port settings for host "localhost" and instead use IPC sockets,
            # so we rewrite the localhost to it's ip address
            host = socket.gethostbyname(self.dbServer);

        params = ["-h", "%s" % host] # Host
        if self.db_port:
            params += ["--port", "%s" % self.db_port] # Port
        params += ["-u", "%s" % self.wiki.config.dbUser] # Username
        params += ["%s" % self.password_option()] # Password
        return params

    def buildSqlCommand(self, query, pipeto=None):
        """Put together a command to execute an sql query to the server for this DB."""
        if not exists(self.wiki.config.mysql):
            raise BackupError("mysql command %s not found" % self.wiki.config.mysql)
        command = [["/bin/echo", "%s" % query],
                   ["%s" % self.wiki.config.mysql] + self.mysql_standard_parameters() + [
                       "%s" % self.dbName,
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
            "%s" % self.dbName,
            "%s" % self.db_table_prefix + table]]
        if pipeto:
            command.append([pipeto])
        return command

    def run_sql_and_get_output(self, query):
        command = self.buildSqlCommand(query)
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
        if self.wiki.config.dbPassword == "":
            return None
        else:
            return "-p" + self.wiki.config.dbPassword

class RunSimpleCommand(object):
    def runAndReturn(command, log_callback=None):
        """Run a command and return the output as a string.
        Raises BackupError on non-zero return code."""
        retval = 1
        retries=0
        maxretries=3
        proc = Popen(command, bufsize=64, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        retval = proc.returncode
        while retval and retries < maxretries:
            if log_callback:
                log_callback("Non-zero return code from '%s'" % command)
            time.sleep(5)
            proc = Popen(command, bufsize=64, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = proc.communicate()
            retval = proc.returncode
            retries = retries + 1
        if retval:
            if log_callback:
                log_callback("Non-zero return code from '%s'" % command)
            raise BackupError("Non-zero return code from '%s'" % command)
        else:
            return output

    runAndReturn = staticmethod(runAndReturn)

class PageAndEditStats(object):
    def __init__(self, wiki, db_name, error_callback=None):
        self.total_pages = None
        self.total_edits = None
        self.wiki = wiki
        self.dbName = db_name
        self.db_server_info = DbServerInfo(wiki, db_name, error_callback)
        self.get_statistics(self.wiki.config, db_name)

    def get_statistics(self, db_name, ignore):
        """Get statistics for the wiki"""

        query = "select MAX(page_id) from %spage;" % self.db_server_info.db_table_prefix
        results = None
        retries = 0
        maxretries = 5
        results = self.db_server_info.run_sql_and_get_output(query)
        while results == None and retries < maxretries:
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
        while results == None and retries < maxretries:
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


class RunInfoFile(object):
    def __init__(self, wiki, enabled, verbose=False):
        self.wiki = wiki
        self._enabled = enabled
        self.verbose = verbose

    def save_dump_runinfo_file(self, text):
        """Write out a simple text file with the status for this wiki's dump."""
        if self._enabled:
            try:
                self._write_dump_runinfo_file(text)
            except:
                if self.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                sys.stderr.write("Couldn't save dump run info file. Continuing anyways\n")

    def status_of_old_dump_is_done(self, runner, date, job_name, job_desc):
        old_dump_runinfo_filename=self._get_dump_runinfo_filename(date)
        status = self._get_job_status_from_runinfo(old_dump_runinfo_filename, job_name)
        if status == "done":
            return 1
        elif not status == None:
            # failure, in progress, some other useless thing
            return 0

        # ok, there was no info there to be had, try the index file. yuck.
        index_filename = os.path.join(runner.wiki.publicDir(), date, runner.wiki.config.perDumpIndex)
        status = self._get_job_status_from_html(index_filename, job_desc)
        if status == "done":
            return 1
        else:
            return 0

    def get_old_runinfo_from_file(self):
        # read the dump run info file in, if there is one, and get info about which dumps
        # have already been run and whether they were successful
        dump_runinfo_filename = self._get_dump_runinfo_filename()
        results = []

        if not os.path.exists(dump_runinfo_filename):
            return False

        try:
            infile = open(dump_runinfo_filename, "r")
            for line in infile:
                results.append(self._get_old_runinfo_from_line(line))
            infile.close
            return results
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return False

    #
    # functions internal to the class
    #
    def _get_dump_runinfo_filename(self, date=None):
        # sometimes need to get this info for an older run to check status of a file for
        # possible prefetch
        if date:
            return os.path.join(self.wiki.publicDir(), date, "dumpruninfo.txt")
        else:
            return os.path.join(self.wiki.publicDir(), self.wiki.date, "dumpruninfo.txt")

    def _get_dump_runinfo_dirname(self, date=None):
        if date:
            return os.path.join(self.wiki.publicDir(), date)
        else:
            return os.path.join(self.wiki.publicDir(), self.wiki.date)

    # format: name:%; updated:%; status:%
    def _get_old_runinfo_from_line(self, line):
        # get rid of leading/trailing/blanks
        line = line.strip(" ")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        dump_runinfo = RunInfo()
        for field in fields:
            field = field.strip(" ")
            (fieldname, separator, field_value)  = field.partition(':')
            if fieldname == "name":
                dump_runinfo.set_name(field_value)
            elif fieldname == "status":
                dump_runinfo.set_status(field_value, False)
            elif fieldname == "updated":
                dump_runinfo.set_updated(field_value)
        return dump_runinfo

    def _write_dump_runinfo_file(self, text):
        directory = self._get_dump_runinfo_dirname()
        dump_runinfo_filename = self._get_dump_runinfo_filename()
#        FileUtils.writeFile(directory, dumpRunInfoFilename, text, self.wiki.config.fileperms)
        FileUtils.writeFileInPlace(dump_runinfo_filename, text, self.wiki.config.fileperms)

    # format: name:%; updated:%; status:%
    def _get_job_status_from_runinfo_line(self, line, job_name):
        # get rid of leading/trailing/embedded blanks
        line = line.replace(" ", "")
        line = line.replace("\n", "")
        fields = line.split(';', 2)
        for field in fields:
            (fieldname, separator, field_value)  = field.partition(':')
            if fieldname == "name":
                if not field_value == job_name:
                    return None
            elif fieldname == "status":
                return field_value

    def _get_job_status_from_runinfo(self, filename, job_name=""):
        # read the dump run info file in, if there is one, and find out whether
        # a particular job (one step only, not a multiple piece job) has been
        # already run and whether it was successful (use to examine status
        # of step from some previous run)
        try:
            infile = open(filename, "r")
            for line in infile:
                result = self._get_job_status_from_runinfo_line(line, job_name)
                if not result == None:
                    return result
            infile.close
            return None
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None

    # find desc in there, look for "class='done'"
    def _get_job_status_from_html_line(self, line, desc):
        if not ">"+desc+"<" in line:
            return None
        if "<li class='done'>" in line:
            return "done"
        else:
            return "other"

    def _get_job_status_from_html(self, filename, desc):
        # read the index file in, if there is one, and find out whether
        # a particular job (one step only, not a multiple piece job) has been
        # already run and whether it was successful (use to examine status
        # of step from some previous run)
        try:
            infile = open(filename, "r")
            for line in infile:
                result = self._get_job_status_from_html_line(line, desc)
                if not result == None:
                    return result
            infile.close
            return None
        except:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None


class RunInfo(object):
    def __init__(self, name="", status="", updated="", to_run=False):
        self._name = name
        self._status = status
        self._updated = updated
        self._to_run = to_run

    def name(self):
        return self._name

    def status(self):
        return self._status

    def updated(self):
        return self._updated

    def to_run(self):
        return self._to_run

    def set_name(self, name):
        self._name = name

    def set_status(self, status, set_updated=True):
        self._status = status

    def set_updated(self, updated):
        self._updated = updated

    def set_to_run(self, to_run):
        self._to_run = to_run


# so if the pages/revsPerChunkAbstract/History are just one number it means
# use that number for all the chunks, figure out yourself how many.
# otherwise we get passed alist that says "here's now many for each chunk and it's this many chunks.
# extra pages/revs go in the last chunk, stuck on the end. too bad. :-P
class Chunk(object,):
    def __init__(self, wiki, db_name, error_callback=None):

        self._dbName = db_name
        self.wiki = wiki
        self._chunks_enabled = self.wiki.config.chunksEnabled
        if self._chunks_enabled:
            self.Stats = PageAndEditStats(self.wiki, self._dbName, error_callback)
            if not self.Stats.total_edits or not self.Stats.total_pages:
                raise BackupError("Failed to get DB stats, exiting")
            if self.wiki.config.chunksForAbstract:
                # we add 200 padding to cover new pages that may be added
                pagesPerChunk = self.Stats.total_pages/int(self.wiki.config.chunksForAbstract) + 200
                self._pages_per_chunk_abstract = [pagesPerChunk for i in range(0, int(self.wiki.config.chunksForAbstract))]
            else:
                self._pages_per_chunk_abstract = self.convert_comma_sep(self.wiki.config.pagesPerChunkAbstract)

            self._pages_per_chunk_history = self.convert_comma_sep(self.wiki.config.pagesPerChunkHistory)
            self._revs_per_chunk_history = self.convert_comma_sep(self.wiki.config.revsPerChunkHistory)
            self._recombine_history = self.wiki.config.recombineHistory
        else:
            self._pages_per_chunk_history = False
            self._revs_per_chunk_history = False
            self._pages_per_chunk_abstract = False
            self._recombine_history = False
        if self._chunks_enabled:
            if self._revs_per_chunk_history:
                if len(self._revs_per_chunk_history) == 1:
                    self._num_chunks_history = self.getNumberOfChunksForXMLDumps(self.Stats.total_edits, self._pages_per_chunk_history[0])
                    self._revs_per_chunk_history = [self._revs_per_chunk_history[0] for i in range(self._num_chunks_history)]
                else:
                    self._num_chunks_history = len(self._revs_per_chunk_history)
                # here we should generate the number of pages per chunk based on number of revs.
                # ...next code update! FIXME
                # self._pages_per_chunk_history = ....
            elif self._pages_per_chunk_history:
                if len(self._pages_per_chunk_history) == 1:
                    self._num_chunks_history = self.getNumberOfChunksForXMLDumps(self.Stats.total_pages, self._pages_per_chunk_history[0])
                    self._pages_per_chunk_history = [self._pages_per_chunk_history[0] for i in range(self._num_chunks_history)]
                else:
                    self._num_chunks_history = len(self._pages_per_chunk_history)
            else:
                self._num_chunks_history = 0

            if self._pages_per_chunk_abstract:
                if len(self._pages_per_chunk_abstract) == 1:
                    self._num_chunks_abstract = self.getNumberOfChunksForXMLDumps(self.Stats.total_pages, self._pages_per_chunk_abstract[0])
                    self._pages_per_chunk_abstract = [self._pages_per_chunk_abstract[0] for i in range(self._num_chunks_abstract)]
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

    def getPagesPerChunkHistory(self):
        return self._pages_per_chunk_history

    def get_num_chunks_history(self):
        return self._num_chunks_history

    def chunksEnabled(self):
        return self._chunks_enabled

    def recombineHistory(self):
        return self._recombine_history

    # args: total (pages or revs), and the number of (pages or revs) per chunk.
    def getNumberOfChunksForXMLDumps(self, total, per_chunk):
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
