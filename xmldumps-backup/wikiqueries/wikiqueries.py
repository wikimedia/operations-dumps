# for every wiki, run a specified query, gzipping the output.
# there's a config file which needs to be set up.

import getopt
import os
import re
import sys
import dumps.WikiDump
import time
from os.path import exists
import traceback
from dumps.exceptions import BackupError
from dumps.utils import TimeUtils, RunSimpleCommand, DbServerInfo
from dumps.fileutils import FileUtils


class WQDbServerInfo(DbServerInfo):
    def build_sql_command_tofile(self, query, out_file):
        """Put together a command to execute an sql query
        to the server for this DB."""
        if not exists(self.wiki.config.mysql):
            raise BackupError("mysql command %s not found" % self.wiki.config.mysql)
        command = ("/bin/echo '%s' | %s -h %s -u %s "
                   % (query, self.wiki.config.mysql, self.db_server,
                      self.wiki.config.db_user))
        if self.wiki.config.db_password != "":
            command = command + "-p" + self.wiki.config.db_password
        command = command + " -r --silent " + self.db_name
        command = command + "| %s > %s" % (self.wiki.config.gzip, out_file)
        return command


class WikiQuery(object):
    def __init__(self, config, query, wiki_name, file_name_format, date,
                 overwrite, dryrun, verbose):
        self._config = config
        self.wiki_name = wiki_name
        self.query = query
        if not self.query:
            query = FileUtils.read_file(self._config.query_file)
        self.file_name_format = file_name_format
        self.date = date
        if not self.date:
            self.date = TimeUtils.today()
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose

    def do_one_wiki(self):
        """returns true on success"""

        query_dir = self._config.wiki_queries_dir.format(w=self.wiki_name, d=self.date)
        if (self.wiki_name not in self._config.private_list and
                self.wiki_name not in self._config.closed_list and
                self.wiki_name not in self._config.skip_db_list):
            if not exists(query_dir):
                os.makedirs(query_dir)
            try:
                if self.verbose:
                    print "Doing run for wiki: ", self.wiki_name
                if not self.dryrun:
                    if not self.run_wiki_query():
                        return False
            except:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                return False
        if self.verbose:
            print "Success!  Wiki", self.wiki_name, "query complete."
        return True

    def run_wiki_query(self):

        out_file = self.file_name_format.format(w=self.wiki_name, d=self.date)
        query_dir = self._config.wiki_queries_dir.format(w=self.wiki_name, d=self.date)
        full_path = os.path.join(query_dir, out_file)

        if not self.overwrite and exists(full_path):
            # don't overwrite existing file, just return a happy value
            if self.verbose:
                print ("Skipping wiki %s, file %s exists already"
                       % (self.wiki_name, full_path))
            return True
        wiki = dumps.WikiDump.Wiki(self._config, self.wiki_name)
        server = WQDbServerInfo(wiki, self.wiki_name)
        return RunSimpleCommand.run_with_no_output(server.build_sql_command_tofile(
            self.query, full_path), maxtries=1, shell=True,
            verbose=self.verbose)


class WikiQueryLoop(object):
    def __init__(self, config, query, file_name_format, date, overwrite,
                 dryrun, verbose):
        self._config = config
        self.query = query
        self.date = date
        if not self.date:
            self.date = TimeUtils.today()
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose
        self.file_name_format = file_name_format
        self.wikis_to_do = self._config.db_list

    def do_run_on_all_wikis(self):
        for wiki in self.wikis_to_do[:]:
            query = WikiQuery(self._config, self.query, wiki,
                              self.file_name_format, self.date,
                              self.overwrite, self.dryrun, self.verbose)
            if query.do_one_wiki():
                self.wikis_to_do.remove(wiki)

    def do_all_wikis_til_done(self, num_fails):
        """Run through all wikis, retrying up to num_fails
        times in case of error"""
        fails = 0
        while 1:
            self.do_run_on_all_wikis()
            if not len(self.wikis_to_do):
                break
            fails = fails + 1
            if fails > num_fails:
                raise BackupError("Too many failures, giving up")
            # wait 5 minutes and try another loop
            time.sleep(300)


def usage(message=None):
    if message:
        sys.stderr.write(message + "\n")
    usage_message = """Usage: python wikiqueries.py [options] [wikidbname]
Options:

--configfile:     Specify config file to read
                  Default: wikiqueries.conf
--date:           date that will appear in filename and/or dirname as specified
                  Format: YYYYMMDD  If not specified, today's date will be used
--dryrun:         Don't execute commands, show the commands that would be run
--filenameformat: Format string for the name of each file, with {w} for
                  wikiname and optional {d} for date
                  Default: {w}-{d}-wikiquery.gz
--outdir:         Put output files for all projects in this directory; it will
                  be created if it does not exist.  Accepts '{W}' and '{d}' for
                  substituting wiki and date into the name.
                  Default: the value given for 'querydir' in the config file
--nooverwrite:    Do not overwrite existing file of the same name, skip run for
                  the specific wiki
--query:          MySQL query to run on each project.
                  Default: the contents of the file specified by 'queryfile' in
                  the config file
--retries:        Number of times to try running the query on all wikis in case
                  of error, before giving up
                  Default: 3
--verbose:        Print various informative messages
wikiname:         Run the query only for the specific wiki
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    config_file = False
    dryrun = False
    date = None
    output_dir = None
    overwrite = True
    query = None
    retries = "3"
    verbose = False
    file_name_format = "{w}-{d}-wikiquery.gz"
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ['configfile=', "date=", 'filenameformat=',
                               "outdir=", "query=", "retries=", 'dryrun',
                               "nooverwrite", 'verbose'])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--configfile":
            config_file = val
        elif opt == "--date":
            date = val
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--filenameformat":
            file_name_format = val
        elif opt == "--outdir":
            output_dir = val
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--query":
            query = val
        elif opt == "--retries":
            if not retries.isdigit():
                usage("A positive number must be specified for retries.")
            retries = val
        elif opt == "--verbose":
            verbose = True

    if date and not re.match("^20[0-9]{6}$", date):
        usage("Date must be in the format YYYYMMDD"
              " (four digit year, two digit month, two digit date)")

    retries = int(retries)

    if config_file:
        config = dumps.WikiDump.Config(config_file)
    else:
        config = dumps.WikiDump.Config()

    if output_dir:
        config.wiki_queries_dir = output_dir

    if len(remainder) > 0:
        query = WikiQuery(config, query, remainder[0], file_name_format,
                          date, overwrite, dryrun, verbose)
        query.do_one_wiki()
    else:
        queries = WikiQueryLoop(config, query, file_name_format, date,
                                overwrite, dryrun, verbose)
        queries.do_all_wikis_til_done(retries)


if __name__ == "__main__":
    do_main()
