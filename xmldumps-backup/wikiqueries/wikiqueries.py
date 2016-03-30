# for every wiki, run a specified query, gzipping the output.
# there's a config file which needs to be set up.

import getopt
import os
import re
import sys
import ConfigParser
import dumps.WikiDump
import subprocess
import socket
import time
from subprocess import Popen, PIPE
from dumps.utils import TimeUtils, RunSimpleCommand, DbServerInfo
from dumps.fileutils import FileUtils
from os.path import exists
import hashlib
import traceback
import shutil


class WQDbServerInfo(DbServerInfo):
    def buildSqlCommand(self, query, out_file):
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


class WikiQueriesError(Exception):
    pass


class QueryDir(object):
    def __init__(self, config):
        self._config = config

    def getQueryDir(self, wiki, date):
        return self._config.wiki_queries_dir.format(w=wiki, d=date)


class WikiQuery(object):
    def __init__(self, config, query, wikiName, fileNameFormat, date,
                 overwrite, dryrun, verbose):
        self._config = config
        self.wikiName = wikiName
        self.query = query
        self.queryDir = QueryDir(self._config)
        if not self.query:
            query = FileUtils.read_file(self._config.queryFile)
        self.fileNameFormat = fileNameFormat
        self.date = date
        if not self.date:
            self.date = TimeUtils.today()
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose

    def doOneWiki(self):
        """returns true on success"""
        if (self.wikiName not in self._config.privateWikisList and
                self.wikiName not in self._config.closedWikisList and
                self.wikiName not in self._config.skipWikisList):
            if not exists(self.queryDir.getQueryDir(self.wikiName, self.date)):
                os.makedirs(self.queryDir.getQueryDir(self.wikiName,
                                                      self.date))
            try:
                if self.verbose:
                    print "Doing run for wiki: ", self.wikiName
                if not self.dryrun:
                    if not self.runWikiQuery():
                        return False
            except:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                return False
        if self.verbose:
            print "Success!  Wiki", self.wikiName, "query complete."
        return True

    def runWikiQuery(self):
        outFile = self.fileNameFormat.format(w=self.wikiName, d=self.date)
        queryDir = self._config.wiki_queries_dir.format(w=self.wikiName, d=self.date)
        fullPath = os.path.join(queryDir, outFile)
        if not self.overwrite and exists(fullPath):
            # don't overwrite existing file, just return a happy value
            if self.verbose:
                print ("Skipping wiki %s, file %s exists already"
                       % (self.wikiName, fullPath))
            return True
        wiki = dumps.WikiDump.Wiki(self._config, self.wikiName)
        db = WQDbServerInfo(wiki, self.wikiName)
        return RunSimpleCommand.run_with_no_output(db.buildSqlCommand(
            self.query, fullPath), maxtries=1, shell=True,
            verbose=self.verbose)


class WikiQueryLoop(object):
    def __init__(self, config, query, fileNameFormat, date, overwrite,
                 dryrun, verbose):
        self._config = config
        self.query = query
        self.date = date
        if not self.date:
            self.date = TimeUtils.today()
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose
        self.fileNameFormat = fileNameFormat
        self.wikisToDo = self._config.allWikisList

    def doRunOnAllWikis(self):
        failures = 0
        for w in self.wikisToDo[:]:
            query = WikiQuery(self._config, self.query, w,
                              self.fileNameFormat, self.date,
                              self.overwrite, self.dryrun, self.verbose)
            if query.doOneWiki():
                self.wikisToDo.remove(w)

    def doAllWikisTilDone(self, numFails):
        """Run through all wikis, retrying up to numFails
        times in case of error"""
        fails = 0
        while 1:
            self.doRunOnAllWikis()
            if not len(self.wikisToDo):
                break
            fails = fails + 1
            if fails > numFails:
                raise WikiQueriesError("Too many failures, giving up")
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
    configFile = False
    result = False
    dryrun = False
    date = None
    outputDir = None
    query = None
    retries = 3
    verbose = False
    fileNameFormat = "{w}-{d}-wikiquery.gz"
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ['configfile=', "date=", 'filenameformat=',
                               "outdir=", "query=", "retries=", 'dryrun',
                               "nooverwrite", 'verbose'])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--configfile":
            configFile = val
        elif opt == "--date":
            date = val
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--filenameformat":
            fileNameFormat = val
        elif opt == "--outdir":
            outputDir = val
        elif opt == "--query":
            query = val
        elif opt == "--retries":
            if not retries.isdigit():
                usage("A positive number must be specified for retries.")
            retries = int(val)
        elif opt == "--verbose":
            verbose = True

    if date and not re.match("^20[0-9]{6}$", date):
        usage("Date must be in the format YYYYMMDD"
              " (four digit year, two digit month, two digit date)")

    if configFile:
        config = dumps.WikiDump.Config(configFile)
    else:
        config = dumps.WikiDump.Config()

    if outputDir:
        config.wiki_queries_dir = outputDir

    if len(remainder) > 0:
        query = WikiQuery(config, query, remainder[0], fileNameFormat,
                          date, overwrite, dryrun, verbose)
        query.doOneWiki()
    else:
        queries = WikiQueryLoop(config, query, fileNameFormat, date,
                                overwrite, dryrun, verbose)
        queries.doAllWikisTilDone(retries)

if __name__ == "__main__":
    do_main()
