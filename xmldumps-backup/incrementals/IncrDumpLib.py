# shared classes for incrementals
import os
import sys
import re
import ConfigParser
import WikiDump
from WikiDump import FileUtils, TimeUtils, MiscUtils
from os.path import exists
import socket
import subprocess
from subprocess import Popen, PIPE
import shutil

class ContentFile(object):
    def __init__(self, config, date, wikiName):
        self._config = config
        self.date = date
        self.incrDir = IncrementDir(self._config, date)
        self.wikiName = wikiName

    # override this.
    def getFileName(self):
        return "content.txt"

    def getPath(self):
        return os.path.join(self.incrDir.getIncDir(self.wikiName),self.getFileName())

    def getFileInfo(self):
        return FileUtils.fileInfo(self.getPath())
    
class MaxRevIDFile(ContentFile):
    def getFileName(self):
        return "maxrevid.txt"

class StubFile(ContentFile):
    def getFileName(self):
        return "%s-%s-stubs-meta-hist-incr.xml.gz" % ( self.wikiName, self.date )

class RevsFile(ContentFile):
    def getFileName(self):
        return "%s-%s-pages-meta-hist-incr.xml.bz2" % ( self.wikiName, self.date )

class StatusFile(ContentFile):
    def getFileName(self):
        return "status.txt"

    def getPath(self, date = None):
        return os.path.join(self.incrDir.getIncDir(self.wikiName, date),self.getFileName())

class LockFile(ContentFile):
    def getFileName(self):
        return "%s-%s.lock" % ( self.wikiName, self.date )

    def getPath(self):
        return os.path.join(self.incrDir.getIncDirNoDate(self.wikiName),self.getFileName())

class  MaxRevIDLockFile(LockFile):
    def getFileName(self):
        return "%s-%s-maxrevid.lock" % ( self.wikiName, self.date )
        
class  IncrDumpLockFile(LockFile):
    def getFileName(self):
        return "%s-%s-incrdump.lock" % ( self.wikiName, self.date )

class MD5File(ContentFile):
    def getFileName(self):
        return "%s-%s-md5sums.txt" % ( self.wikiName, self.date )

class IndexFile(ContentFile):
    def __init__(self, config):
        self._config = config
        self.incrDir = IncrementDir(self._config)

    def getFileName(self):
        return "index.html"

    def getPath(self):
        return os.path.join(self.incrDir.getIncDirBase(),self.getFileName())

class StatusInfo(object):
    def __init__(self, config, date, wikiName):
        self._config = config
        self.date = date
        self.wikiName = wikiName
        self.statusFile = StatusFile(self._config, self.date, self.wikiName)

    def getStatus(self, date = None):
        if exists(self.statusFile.getPath(date)):
            status = FileUtils.readFile(self.statusFile.getPath(date)).rstrip()
            if status == "done":
                return True
        return False

    def setStatus(self, status):
        FileUtils.writeFileInPlace(self.statusFile.getPath(),status, self._config.fileperms)

class Lock(object):
    def __init__(self, config, date, wikiName):
        self._config = config
        self.date = date
        self.wikiName = wikiName
        self.lockFile = LockFile(self._config, self.date, self.wikiName)

    def isLocked(self):
        return exists(self.lockFile.getPath())

    def getLock(self):
        try:
            if not exists(self._config.incrementalsDir):
                os.makedirs(self._config.incrementalsDir)
            f = FileUtils.atomicCreate(self.lockFile.getPath(), "w")
            f.write("%s %d" % (socket.getfqdn(), os.getpid()))
            f.close()
            return True
        except:
            return False

    def unlock(self):
        os.remove(self.lockFile.getPath())

    def getLockInfo(self):
        try:
            timestamp = os.stat(self.lockFile.getPath()).st_mtime
            return time.strftime("%Y-%m-%d %H:%M:%S",timestamp)
        except:
            return None

class IncrDumpLock(Lock):
    def __init__(self, config, date, wikiName):
        self._config = config
        self.date = date
        self.wikiName = wikiName
        self.lockFile = IncrDumpLockFile(self._config, self.date, self.wikiName)

class MaxRevIDLock(Lock):
    def __init__(self,config, date, wikiName):
        self._config = config
        self.date = date
        self.wikiName = wikiName
        self.lockFile = MaxRevIDLockFile(self._config, self.date, self.wikiName)

class Config(object):
    def __init__(self, configFile=False):
        self.projectName = False

        home = os.path.dirname(sys.argv[0])
        if (not configFile):
            configFile = "dumpincr.conf"
        self.files = [
            os.path.join(home,configFile),
            "/etc/dumpincrementals.conf",
            os.path.join(os.getenv("HOME"), ".dumpincr.conf")]
        defaults = {
            #"wiki": {
            "allwikislist": "",
            "privatewikislist": "",
            "closedwikislist": "",
            #"output": {
            "incrementalsdir": "/dumps/public/incr",
            "templatedir": home,
            "temp":"/dumps/temp",
            "webroot": "http://localhost/dumps/incr",
            "fileperms": "0640",
            "delay": "43200",
            #"database": {
            "user": "root",
            "password": "",
            #"tools": {
            "mediawiki" : "",
            "php": "/bin/php",
            "gzip": "/usr/bin/gzip",
            "bzip2": "/usr/bin/bzip2",
            "mysql": "/usr/bin/mysql",
            "checkforbz2footer": "/usr/local/bin/checkforbz2footer",
            "writeuptopageid": "/usr/local/bin/writeuptopageid",
            "multiversion": "",
            #"cleanup": {
            "keep": "3",
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
        self.incrementalsDir = self.conf.get("output", "incrementalsdir")
        self.tempDir = self.conf.get("output", "temp")
        self.templateDir = self.conf.get("output", "templateDir")
        self.webRoot = self.conf.get("output", "webroot")
        self.fileperms = self.conf.get("output", "fileperms")
        self.fileperms = int(self.fileperms,0)
        self.delay = self.conf.get("output", "delay")
        self.delay = int(self.delay,0)

        if not self.conf.has_section('tools'):
            self.conf.add_section('tools')
        self.php = self.conf.get("tools", "php")
        self.gzip = self.conf.get("tools", "gzip")
        self.bzip2 = self.conf.get("tools", "bzip2")
        self.mysql = self.conf.get("tools", "mysql")
        self.checkforbz2footer = self.conf.get("tools","checkforbz2footer")
        self.writeuptopageid = self.conf.get("tools","writeuptopageid")
        self.multiversion = self.conf.get("tools","multiversion")

        if not self.conf.has_section('cleanup'):
            self.conf.add_section('cleanup')
        self.keep = self.conf.getint("cleanup", "keep")

        if not self.conf.has_section('database'):
            self.conf.add_section('database')
        self.dbUser = self.conf.get("database", "user")
        self.dbPassword = self.conf.get("database", "password")

    def readTemplate(self, name):
        template = os.path.join(self.templateDir, name)
        return FileUtils.readFile(template)

class RunSimpleCommand(object):
    def runWithOutput(command, maxtries = 3, shell=False):
        """Run a command and return the output as a string.
        Raises IncrementDumpsError on non-zero return code."""
        success = False
        tries = 0
        while (not success and tries < maxtries):
            proc = Popen(command, shell = shell, stdout = PIPE, stderr = PIPE)
            output, error = proc.communicate()
            if not proc.returncode:
                success = True
            tries = tries + 1
        if not success:
            if type(command).__name__=='list':
                commandString = " ".join(command)
            else:
                commandString = command
            if proc:
                raise IncrementDumpsError("command '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
            else:
                raise IncrementDumpsError("command '" + commandString + ( "' failed"  ) + " and error '" + error + "'")
        return output

    def runWithNoOutput(command, maxtries = 3, shell=False):
        """Run a command, expecting no output.
        Raises IncrementDumpsError on non-zero return code."""
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
            if type(command).__name__=='list':
                commandString = " ".join(command)
            else:
                commandString = command
            raise IncrementDumpsError("command '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
 
    runWithOutput = staticmethod(runWithOutput)
    runWithNoOutput = staticmethod(runWithNoOutput)

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

    def buildSqlCommand(self, query):
        """Put together a command to execute an sql query to the server for this DB."""
        if (not exists( self.config.mysql ) ):
            raise BackupError("mysql command %s not found" % self.config.mysql)
        command =  "/bin/echo '%s' | %s -h %s -u %s " % ( query, self.config.mysql, self.dbServer, self.config.dbUser ) 
        if self.config.dbPassword != "":
            command = command + "-p" + self.config.dbPassword
        command = command + " -r --silent " + self.wikiName
        return command

class IncrementDumpsError(Exception):
    pass

class IncrementDir(object):
    def __init__(self, config, date = None):
        self._config = config
        self.date = date

    def getIncDirBase(self):
        return self._config.incrementalsDir

    def getIncDirNoDate(self, wikiName):
            return os.path.join(self.getIncDirBase(), wikiName)

    def getIncDir(self, wikiName, date = None):
        if (date == None):
            return os.path.join(self.getIncDirBase(), wikiName, self.date)
        else:
            return os.path.join(self.getIncDirBase(), wikiName, date)

class IncrementDumpsError(Exception):
    pass

class IncDumpDirs(object):
    def __init__(self, config, wikiName):
        self._config = config
        self.wikiName = wikiName
        self.incrDir = IncrementDir(self._config)

    def getIncDumpDirs(self):
        base = self.incrDir.getIncDirNoDate(self.wikiName)
        digits = re.compile(r"^\d{4}\d{2}\d{2}$")
        dates = []
        try:
            for dir in os.listdir(base):
                if digits.match(dir):
                    dates.append(dir)
        except OSError:
            return []
        dates.sort()
        return dates

    def cleanupOldIncrDumps(self, date):
        old = self.getIncDumpDirs()
        if old:
            if old[-1] == date:
                old = old[:-1]
            if self._config.keep > 0:
                old = old[:-(self._config.keep)]
            for dump in old:
                toRemove = os.path.join(self.incrDir.getIncDirNoDate(self.wikiName), dump)
                shutil.rmtree("%s" % toRemove)

    def getPrevIncrDate(self, date):
        # find the most recent incr dump before the
        # specified date that completed successfully
        previous = None
        old = self.getIncDumpDirs()
        if old:
            for dump in old:
                if dump == date:
                    return previous
                else:
                    statusInfo = StatusInfo(self._config, dump, self.wikiName)
                    if statusInfo.getStatus(dump) == "done":
                        previous = dump
        return previous

    def getLatestIncrDate(self):
        # find the most recent incr dump 
        dirs = self.getIncDumpDirs()
        if dirs:
            return(dirs[-1])
        else:
            return(None)
