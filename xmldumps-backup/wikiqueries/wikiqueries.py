# for every wiki, run a specified query, gzipping the output.
# there's a config file which needs to be set up.

import getopt
import os
import re
import sys
import ConfigParser
import subprocess
import socket
import time
from subprocess import Popen, PIPE
from os.path import exists
import hashlib
import traceback
import shutil

class ContentFile(object):
    def __init__(self, config, date, wikiName):
        self._config = config
        self.date = date
        self.queryDir = QueryDir(self._config)
        self.wikiName = wikiName

    # override this.
    def getFileName(self):
        return "content.txt"

    def getPath(self):
        return os.path.join(self.queryDir.getQueryDir(),self.getFileName())

class OutputFile(ContentFile):
    def __init__(self, config, date, wikiName, fileNameFormat):
        super( OutputFile, self ).__init__(config, date, wikiName)
        self.fileNameFormat = fileNameFormat

    def getFileName(self):
        return fileNameFormat.format(w=self.wikiName, d=self.date)

class Config(object):
    def __init__(self, configFile=False):
        self.projectName = False

        home = os.path.dirname(sys.argv[0])
        if (not configFile):
            configFile = "wikiqueries.conf"
        self.files = [
            os.path.join(home,configFile),
            "/etc/wikqueries.conf",
            os.path.join(os.getenv("HOME"), ".wikiqueries.conf")]
        defaults = {
            #"wiki": {
            "allwikislist": "",
            "privatewikislist": "",
            "closedwikislist": "",
            #"output": {
            "wikiqueriesdir": "/wikiqueries",
            "temp":"/wikiqueries/temp",
            "fileperms": "0640",
            #"database": {
            "user": "root",
            "password": "",
            #"tools": {
            "php": "/bin/php",
            "gzip": "/usr/bin/gzip",
            "bzip2": "/usr/bin/bzip2",
            "mysql": "/usr/bin/mysql",
            "multiversion": "",
            #"query":{
            "queryfile": "wikiquery.sql"
            }

        self.conf = ConfigParser.SafeConfigParser(defaults)
        self.conf.read(self.files)

        if not self.conf.has_section("wiki"):
            print "The mandatory configuration section 'wiki' was not defined."
            raise ConfigParser.NoSectionError('wiki')

        if not self.conf.has_option("wiki","mediawiki"):
            print "The mandatory setting 'mediawiki' in the section 'wiki' was not defined."
            raise ConfigParser.NoOptionError('wiki','mediawiki')

        self.parseConfFile()

    def parseConfFile(self):
        self.mediawiki = self.conf.get("wiki", "mediawiki")
        self.allWikisList = MiscUtils.dbList(self.conf.get("wiki", "allwikislist"))
        self.privateWikisList = MiscUtils.dbList(self.conf.get("wiki", "privatewikislist"))
        self.closedWikisList = MiscUtils.dbList(self.conf.get("wiki", "closedwikislist"))

        if not self.conf.has_section('output'):
            self.conf.add_section('output')
        self.wikiQueriesDir = self.conf.get("output", "wikiqueriesdir")
        self.tempDir = self.conf.get("output", "temp")
        self.fileperms = self.conf.get("output", "fileperms")
        self.fileperms = int(self.fileperms,0)

        if not self.conf.has_section('database'):
            self.conf.add_section('database')
        self.dbUser = self.conf.get("database", "user")
        self.dbPassword = self.conf.get("database", "password")

        if not self.conf.has_section('tools'):
            self.conf.add_section('tools')
        self.php = self.conf.get("tools", "php")
        self.gzip = self.conf.get("tools", "gzip")
        self.bzip2 = self.conf.get("tools", "bzip2")
        self.mysql = self.conf.get("tools", "mysql")
        self.multiversion = self.conf.get("tools","multiversion")

        if not self.conf.has_section('query'):
            self.conf.add_section('query')
        self.queryFile = self.conf.get("query","queryfile")

class MultiVersion(object):
    def MWScriptAsString(config, maintenanceScript):
        return(" ".join(MultiVersion.MWScriptAsArray(config, maintenanceScript)))

    def MWScriptAsArray(config, maintenanceScript):
        if config.multiversion != "":
            if exists(config.multiversion):
                return [ config.multiversion, maintenanceScript ]
        return [ "%s/maintenance/%s" % (config.mediawiki, maintenanceScript) ]

    MWScriptAsString = staticmethod(MWScriptAsString)
    MWScriptAsArray = staticmethod(MWScriptAsArray)

class MiscUtils(object):
    def dbList(filename):
        """Read database list from a file"""
        if (not filename):
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

    def shellEscape(param):
        """Escape a string parameter, or set of strings, for the shell."""
        if isinstance(param, basestring):
            return "'" + param.replace("'", "'\\''") + "'"
        elif param is None:
            # A blank string might actually be needed; None means we can leave it out
            return ""
        else:
            return tuple([MiscUtils.shellEscape(x) for x in param])

    def today():
        return time.strftime("%Y%m%d", time.gmtime())

    def readFile(filename):
        """Read text from a file in one fell swoop."""
        file = open(filename, "r")
        text = file.read()
        file.close()
        return text

    dbList = staticmethod(dbList)
    shellEscape = staticmethod(shellEscape)
    today = staticmethod(today)
    readFile = staticmethod(readFile)

class RunSimpleCommand(object):
    def runWithOutput(command, maxtries = 3, shell=False, verbose = False):
        """Run a command and return the output as a string.
        Raises WikiQueriesError on non-zero return code."""

        if type(command).__name__=='list':
            commandString = " ".join([ "'" + c + "'" for c in command ])
        else:
            commandString = command
        if verbose:
            print "command to be run: ", commandString
        success = False
        tries = 0
        while (not success and tries < maxtries):
            proc = Popen(command, shell = shell, stdout = PIPE, stderr = PIPE)
            output, error = proc.communicate()
            if not proc.returncode:
                success = True
            tries = tries + 1
        if not success:
            if proc:
                raise WikiQueriesError("command '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
            else:
                raise WikiQueriesError("command '" + commandString + ( "' failed"  ) + " and error '" + error + "'")
        return output

    def runWithNoOutput(command, maxtries = 3, shell=False, verbose=False):
        """Run a command, expecting no output.
        Raises WikiQueriesError on non-zero return code."""

        if type(command).__name__=='list':
            commandString = " ".join([ "'" + c + "'" for c in command ])
        else:
            commandString = command
        if verbose:
            print "command to be run with no output: ", commandString
        success = False
        tries = 0
        while ((not success) and tries < maxtries):
            proc = Popen(command, shell = shell, stderr = PIPE)
            # output will be None, we can ignore it
            output, error = proc.communicate()
            if not proc.returncode:
                success = True
            tries = tries + 1
        if not success:
            raise WikiQueriesError("command '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
        return success

    runWithOutput = staticmethod(runWithOutput)
    runWithNoOutput = staticmethod(runWithNoOutput)

class DBServer(object):
    def __init__(self, config, wikiName):
        self.config = config
        self.wikiName = wikiName
        self.dbServer = self.defaultServer()

    def defaultServer(self):
        if (not exists( self.config.php ) ):
            raise BackupError("php command %s not found" % self.config.php)
        commandList = MultiVersion.MWScriptAsArray(self.config, "getSlaveServer.php")
        command =  [ self.config.php, "-q" ]
        command.extend(commandList)
        command.extend( [ "--wiki=%s" % self.wikiName, "--group=dump" ])
        return RunSimpleCommand.runWithOutput(command, shell=False).rstrip()

    def buildSqlCommand(self, query, outFile):
        """Put together a command to execute an sql query to the server for this DB."""
        if (not exists( self.config.mysql ) ):
            raise BackupError("mysql command %s not found" % self.config.mysql)
        command =  "/bin/echo '%s' | %s -h %s -u %s " % ( query, self.config.mysql, self.dbServer, self.config.dbUser ) 
        if self.config.dbPassword != "":
            command = command + "-p" + self.config.dbPassword
        command = command + " -r --silent " + self.wikiName
        command = command + "| %s > %s" % ( self.config.gzip, outFile )
        return command

class WikiQueriesError(Exception):
    pass

class QueryDir(object):
    def __init__(self, config):
        self._config = config

    def getQueryDir(self):
        return self._config.wikiQueriesDir

class WikiQuery(object):
    def __init__(self,config, query, wikiName, fileNameFormat, overwrite, dryrun, verbose):
        self._config = config
        self.wikiName = wikiName
        self.query = query
        self.queryDir = QueryDir(self._config)
        if not self.query:
            query = MiscUtils.readFile(self._config.queryFile)
        self.fileNameFormat = fileNameFormat
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose

    def doOneWiki(self):
        """returns true on success"""
        if self.wikiName not in self._config.privateWikisList and self.wikiName not in self._config.closedWikisList:
            if not exists(self.queryDir.getQueryDir()):
                os.makedirs(self.queryDir.getQueryDir())
            try:
		if (self.verbose):
                    print "Doing run for wiki: ",self.wikiName
                if not dryrun:
                    if not self.runWikiQuery():
                        return False
            except:
                if (self.verbose):
                    traceback.print_exc(file=sys.stdout)
                return False
        if (self.verbose):
            print "Success!  Wiki", self.wikiName, "query complete."
        return True

    def runWikiQuery(self):
        outFile = OutputFile(self._config, MiscUtils.today(), self.wikiName, self.fileNameFormat)
        if not self.overwrite and exists(outFile.getPath()):
            # don't overwrite existing file, just return a happy value
            if self.verbose:
                print "Skipping wiki %s, file exists already" % self.wikiName 
                return True
        db = DBServer(self._config, self.wikiName)
        return RunSimpleCommand.runWithNoOutput(db.buildSqlCommand(self.query, outFile.getPath()), maxtries=1, shell=True, verbose=self.verbose)

class WikiQueryLoop(object):
    def __init__(self, config, query, fileNameFormat, overwrite, dryrun, verbose):
        self._config = config
        self.query = query
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.verbose = verbose
        self.fileNameFormat = fileNameFormat
        self.wikisToDo = self._config.allWikisList

    def doRunOnAllWikis(self):
        failures = 0
        for w in self.wikisToDo[:]:
            query = WikiQuery(self._config, self.query, w, self.fileNameFormat, self.overwrite, self.dryrun, self.verbose)
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
            fails  = fails + 1
            if fails > numFails:
                raise WikiQueriesError("Too many failures, giving up")
            # wait 5 minutes and try another loop
            time.sleep(300)

def usage(message = None):
    if message:
        print message
        print "Usage: python wikiqueries.py [options] [wikidbname]"
        print "Options: --configfile, --dryrun, --filenameformat, --outdir, --query, --retries, --verbose"
        print "--configfile:     Specify config file to read."
        print "                  Default: wikiqueries.conf"
        print "--dryrun:         Don't actually run anything but print the commands that would be run."
        print "--filenameformat: Format string for the name of each file, with {w} for wikiname and optional"
        print "                  {d} for date."
        print "                  Default: {w}-{d}-wikiquery.gz"
        print "--outdir:         Put output files for all projects in this directory; it will be created if"
        print "                  it does not exist."
        print "                  Default: the value given for 'querydir' in the config file"
        print "--nooverwrite:    Do not overwrite existing file of the same name, skip run for the specific wiki"
        print "--query:          MySQL query to run on each project."
        print "                  Default: the contents of the file specified by 'queryfile' in the config file"
        print "--retries:        Number of times to try running the query on all wikis in case of error, before giving up."
        print "                  Default: 3"
        print "--verbose:        Print various informative messages."
        print "wikiname:         Run the query only for the specific wiki."
        sys.exit(1)

if __name__ == "__main__":
    configFile = False
    result = False
    dryrun = False
    outputDir = None
    overwrite = True
    query = None
    retries = 3
    verbose = False
    fileNameFormat = "{w}-{d}-wikiquery.gz"
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 [ 'configfile=', 'filenameformat=', "outdir=", "query=", "retries=", 'dryrun', "nooverwrite", 'verbose' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--configfile":
            configFile = val
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--filenameformat":
            fileNameFormat = val
        elif opt == "--outdir":
            outputDir = val
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--query":
            query = val
        elif opt == "--retries":
            if not retries.isdigit():
                usage("A positive number must be specified for retries.")
            retries = int(val)
        elif opt == "--verbose":
            verbose = True
        
    if (configFile):
        config = Config(configFile)
    else:
        config = Config()

    if outputDir:
        config.wikiQueriesDir = outputDir

    if len(remainder) > 0:
        query = WikiQuery(config, query, remainder[0], fileNameFormat, overwrite, dryrun, verbose)
        query.doOneWiki()
    else:
        queries = WikiQueryLoop(config, query, fileNameFormat, overwrite, dryrun, verbose)
        queries.doAllWikisTilDone(retries)
