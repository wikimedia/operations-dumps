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
	def MWScriptAsString(config, maintenanceScript):
		return(" ".join(MultiVersion.MWScriptAsArray(config, maintenanceScript)))

	def MWScriptAsArray(config, maintenanceScript):
		MWScriptLocation = os.path.join(config.wikiDir,"multiversion","MWScript.php")
		if exists(MWScriptLocation):
			return [ MWScriptLocation, maintenanceScript ]
		else:
			return [ "%s/maintenance/%s" % (config.wikiDir, maintenanceScript) ]

	def MWVersion(config, dbName):
		getVersionLocation = os.path.join(config.wikiDir,"multiversion","getMWVersion")
		if exists(getVersionLocation):
			# run the command for the wiki and get the version
			command =  getVersionLocation + " " +  dbName
			version = RunSimpleCommand.runAndReturn(command)
			if version:
				version = version.rstrip()
				return version
		return None

	MWScriptAsString = staticmethod(MWScriptAsString)
	MWScriptAsArray = staticmethod(MWScriptAsArray)
	MWVersion = staticmethod(MWVersion)

class DbServerInfo(object):
	def __init__(self, wiki, dbName, errorCallback = None):
		self.wiki = wiki
		self.dbName = dbName
		self.errorCallback = errorCallback
		self.dBTablePrefix = None
		self.getDefaultServerAndDBprefix()

	def getDefaultServerAndDBprefix(self):
		"""Get the name of a slave server for our cluster; also get
		the prefix for all tables for the specific wiki ($wgDBprefix)"""
		if (not exists( self.wiki.config.php ) ):
			raise BackupError("php command %s not found" % self.wiki.config.php)
		commandList = MultiVersion.MWScriptAsArray(self.wiki.config, "getSlaveServer.php")
		phpCommand = MiscUtils.shellEscape(self.wiki.config.php)
		dbName = MiscUtils.shellEscape(self.dbName)
		for i in range(0,len(commandList)):
			commandList[i] = MiscUtils.shellEscape(commandList[i])
		command = " ".join(commandList)
		command = "%s -q %s --wiki=%s --group=dump --globals" % (phpCommand, command, dbName)
		results = RunSimpleCommand.runAndReturn(command, self.errorCallback).strip()
		if not results:
			raise BackupError("Failed to get database connection information for %s, bailing." % self.wiki.config.php)
		# first line is the server, the second is an array of the globals, we need the db table prefix out of those
		lines = results.splitlines()
		self.dbServer = lines[0]
		self.dbPort = None
		if ':' in self.dbServer:
			self.dbServer, _, self.dbPort = self.dbServer.rpartition(':')

		#       [wgDBprefix] =>
		wgdbprefixPattern = re.compile("\s+\[wgDBprefix\]\s+=>\s+(?P<prefix>.*)$")
		for l in lines:
			match = wgdbprefixPattern.match(l)
			if match:
				self.dBTablePrefix = match.group('prefix').strip()
		if self.dBTablePrefix == None:
			# if we didn't see this in the globals list, something is broken.
			raise BackupError("Failed to get database table prefix for %s, bailing." % self.wiki.config.php)

	def mysqlStandardParameters( self ):
		host = self.dbServer
		if self.dbPort and self.dbServer.strip() == "localhost":
			# MySQL tools ignore port settings for host "localhost" and instead use IPC sockets,
			# so we rewrite the localhost to it's ip address
			host = socket.gethostbyname( self.dbServer );

		params = [ "-h", "%s" % host ] # Host
		if self.dbPort:
			params += [ "--port", "%s" % self.dbPort ] # Port
		params += [ "-u", "%s" % self.wiki.config.dbUser ] # Username
		params += [ "%s" % self.passwordOption() ] # Password
		return params

	def buildSqlCommand(self, query, pipeto = None):
		"""Put together a command to execute an sql query to the server for this DB."""
		if (not exists( self.wiki.config.mysql ) ):
			raise BackupError("mysql command %s not found" % self.wiki.config.mysql)
		command = [ [ "/bin/echo", "%s" % query ],
			    [ "%s" % self.wiki.config.mysql ] + self.mysqlStandardParameters() + [
			      "%s" % self.dbName,
			      "-r" ] ]
		if (pipeto):
			command.append([ pipeto ])
		return command

	def buildSqlDumpCommand(self, table, pipeto = None):
		"""Put together a command to dump a table from the current DB with mysqldump
		and save to a gzipped sql file."""
		if (not exists( self.wiki.config.mysqldump ) ):
			raise BackupError("mysqldump command %s not found" % self.wiki.config.mysqldump)
		command = [ [ "%s" % self.wiki.config.mysqldump ] + self.mysqlStandardParameters() + [
			       "--opt", "--quick",
			       "--skip-add-locks", "--skip-lock-tables",
			       "%s" % self.dbName,
			       "%s" % self.dBTablePrefix + table ] ]
		if (pipeto):
			command.append([ pipeto ])
		return command

	def runSqlAndGetOutput(self, query):
		command = self.buildSqlCommand(query)
		p = CommandPipeline(command, quiet=True)
		p.runPipelineAndGetOutput()
		# fixme best to put the return code someplace along with any errors....
		if p.exitedSuccessfully() and (p.output()):
			return(p.output())
		else:
			return None

	def passwordOption(self):
		"""If you pass '-pfoo' mysql uses the password 'foo',
		but if you pass '-p' it prompts. Sigh."""
		if self.wiki.config.dbPassword == "":
			return None
		else:
			return "-p" + self.wiki.config.dbPassword

class RunSimpleCommand(object):
	def runAndReturn(command, logCallback = None):
		"""Run a command and return the output as a string.
		Raises BackupError on non-zero return code."""
		retval = 1
		retries=0
		maxretries=3
		proc = Popen(command, bufsize=64, shell = True, stdout = PIPE, stderr = PIPE)
		output, error = proc.communicate()
		retval = proc.returncode
		while (retval and retries < maxretries):
			if logCallback:
				logCallback("Non-zero return code from '%s'" % command)
			time.sleep(5)
			proc = Popen(command, bufsize=64, shell = True, stdout = PIPE, stderr = PIPE)
			output, error = proc.communicate()
			retval = proc.returncode
			retries = retries + 1
		if retval:
			if logCallback:
				logCallback("Non-zero return code from '%s'" % command)
			raise BackupError("Non-zero return code from '%s'" % command)
		else:
			return output

	runAndReturn = staticmethod(runAndReturn)

class PageAndEditStats(object):
	def __init__(self, wiki, dbName, errorCallback = None):
		self.totalPages = None
		self.totalEdits = None
		self.wiki = wiki
		self.dbName = dbName
		self.dbServerInfo = DbServerInfo(wiki, dbName, errorCallback)
		self.getStatistics(self.wiki.config,dbName)

	def getStatistics(self, dbName, ignore):
		"""Get statistics for the wiki"""

		query = "select MAX(page_id) from %spage;" % self.dbServerInfo.dBTablePrefix
		results = None
		retries = 0
		maxretries = 5
		results = self.dbServerInfo.runSqlAndGetOutput(query)
		while (results == None and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			results = self.dbServerInfo.runSqlAndGetOutput(query)
		if (not results):
			return(1)

		lines = results.splitlines()
		if (lines and lines[1]):
			self.totalPages = int(lines[1])
		query = "select MAX(rev_id) from %srevision;" % self.dbServerInfo.dBTablePrefix
		retries = 0
		results = None
		results = self.dbServerInfo.runSqlAndGetOutput(query)
		while (results == None and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			results = self.dbServerInfo.runSqlAndGetOutput(query)
		if (not results):
			return(1)

		lines = results.splitlines()
		if (lines and lines[1]):
			self.totalEdits = int(lines[1])
		return(0)

	def getTotalPages(self):
		return self.totalPages

	def getTotalEdits(self):
		return self.totalEdits


class RunInfoFile(object):
	def __init__(self, wiki, enabled, verbose = False):
		self.wiki = wiki
		self._enabled = enabled
		self.verbose = verbose

	def saveDumpRunInfoFile(self, text):
		"""Write out a simple text file with the status for this wiki's dump."""
		if (self._enabled):
			try:
				self._writeDumpRunInfoFile(text)
			except:
				if (self.verbose):
					exc_type, exc_value, exc_traceback = sys.exc_info()
					sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
				sys.stderr.write("Couldn't save dump run info file. Continuing anyways\n")

	def statusOfOldDumpIsDone(self, runner, date, jobName, jobDesc):
		oldDumpRunInfoFilename=self._getDumpRunInfoFileName(date)
		status = self._getStatusForJobFromRunInfoFile(oldDumpRunInfoFilename, jobName)
		if (status == "done"):
			return 1
		elif (not status == None):
			# failure, in progress, some other useless thing
			return 0

		# ok, there was no info there to be had, try the index file. yuck.
		indexFilename = os.path.join(runner.wiki.publicDir(), date, runner.wiki.config.perDumpIndex)
		status = self._getStatusForJobFromIndexFile(indexFilename, jobDesc)
		if (status == "done"):
			return 1
		else:
			return 0

	def getOldRunInfoFromFile(self):
		# read the dump run info file in, if there is one, and get info about which dumps
		# have already been run and whether they were successful
		dumpRunInfoFileName = self._getDumpRunInfoFileName()
		results = []

		if not os.path.exists(dumpRunInfoFileName):
			return False

		try:
			infile = open(dumpRunInfoFileName,"r")
			for line in infile:
				results.append(self._getOldRunInfoFromLine(line))
			infile.close
			return results
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
			return False

	#
	# functions internal to the class
	#
	def _getDumpRunInfoFileName(self, date = None):
		# sometimes need to get this info for an older run to check status of a file for
		# possible prefetch
		if (date):
			return os.path.join(self.wiki.publicDir(), date, "dumpruninfo.txt")
		else:
			return os.path.join(self.wiki.publicDir(), self.wiki.date, "dumpruninfo.txt")

	def _getDumpRunInfoDirName(self, date=None):
		if (date):
			return os.path.join(self.wiki.publicDir(), date)
		else:
			return os.path.join(self.wiki.publicDir(), self.wiki.date)

	# format: name:%; updated:%; status:%
	def _getOldRunInfoFromLine(self, line):
		# get rid of leading/trailing/blanks
		line = line.strip(" ")
		line = line.replace("\n","")
		fields = line.split(';',2)
		dumpRunInfo = RunInfo()
		for field in fields:
			field = field.strip(" ")
			(fieldName, separator, fieldValue)  = field.partition(':')
			if (fieldName == "name"):
				dumpRunInfo.setName(fieldValue)
			elif (fieldName == "status"):
				dumpRunInfo.setStatus(fieldValue,False)
			elif (fieldName == "updated"):
				dumpRunInfo.setUpdated(fieldValue)
		return(dumpRunInfo)

	def _writeDumpRunInfoFile(self, text):
		directory = self._getDumpRunInfoDirName()
		dumpRunInfoFilename = self._getDumpRunInfoFileName()
#		FileUtils.writeFile(directory, dumpRunInfoFilename, text, self.wiki.config.fileperms)
		FileUtils.writeFileInPlace(dumpRunInfoFilename, text, self.wiki.config.fileperms)

	# format: name:%; updated:%; status:%
	def _getStatusForJobFromRunInfoFileLine(self, line, jobName):
		# get rid of leading/trailing/embedded blanks
		line = line.replace(" ","")
		line = line.replace("\n","")
		fields = line.split(';',2)
		for field in fields:
			(fieldName, separator, fieldValue)  = field.partition(':')
			if (fieldName == "name"):
				if (not fieldValue == jobName):
					return None
			elif (fieldName == "status"):
				return fieldValue

	def _getStatusForJobFromRunInfoFile(self, filename, jobName = ""):
		# read the dump run info file in, if there is one, and find out whether
		# a particular job (one step only, not a multiple piece job) has been
		# already run and whether it was successful (use to examine status
		# of step from some previous run)
		try:
			infile = open(filename,"r")
			for line in infile:
				result = self._getStatusForJobFromRunInfoFileLine(line, jobName)
				if (not result == None):
					return result
			infile.close
			return None
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
			return None

	# find desc in there, look for "class='done'"
	def _getStatusForJobFromIndexFileLine(self, line, desc):
		if not(">"+desc+"<" in line):
			return None
		if "<li class='done'>" in line:
			return "done"
		else:
			return "other"

	def _getStatusForJobFromIndexFile(self, filename, desc):
		# read the index file in, if there is one, and find out whether
		# a particular job (one step only, not a multiple piece job) has been
		# already run and whether it was successful (use to examine status
		# of step from some previous run)
		try:
			infile = open(filename,"r")
			for line in infile:
				result = self._getStatusForJobFromIndexFileLine(line, desc)
				if (not result == None):
					return result
			infile.close
			return None
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
			return None


class RunInfo(object):
	def __init__(self, name="", status="", updated="", toBeRun = False):
		self._name = name
		self._status = status
		self._updated = updated
		self._toBeRun = toBeRun

	def name(self):
		return self._name

	def status(self):
		return self._status

	def updated(self):
		return self._updated

	def toBeRun(self):
		return self._toBeRun

	def setName(self,name):
		self._name = name

	def setStatus(self,status,setUpdated=True):
		self._status = status

	def setUpdated(self,updated):
		self._updated = updated

	def setToBeRun(self,toBeRun):
		self._toBeRun = toBeRun


# so if the pages/revsPerChunkAbstract/History are just one number it means
# use that number for all the chunks, figure out yourself how many.
# otherwise we get passed alist that says "here's now many for each chunk and it's this many chunks.
# extra pages/revs go in the last chunk, stuck on the end. too bad. :-P
class Chunk(object, ):
	def __init__(self, wiki, dbName, errorCallback = None):

		self._dbName = dbName
		self.wiki = wiki
		self._chunksEnabled = self.wiki.config.chunksEnabled
		if (self._chunksEnabled):
			self.Stats = PageAndEditStats(self.wiki,dbName, errorCallback)
			if (not self.Stats.totalEdits or not self.Stats.totalPages):
				raise BackupError("Failed to get DB stats, exiting")
                        if self.wiki.config.chunksForAbstract:
                                # we add 200 padding to cover new pages that may be added
                                pagesPerChunk = self.Stats.totalPages/int(self.wiki.config.chunksForAbstract) + 200
                                self._pagesPerChunkAbstract = [ pagesPerChunk for i in range(0, int(self.wiki.config.chunksForAbstract)) ]
                        else:
			        self._pagesPerChunkAbstract = self.convertCommaSepLineToNumbers(self.wiki.config.pagesPerChunkAbstract)

			self._pagesPerChunkHistory = self.convertCommaSepLineToNumbers(self.wiki.config.pagesPerChunkHistory)
			self._revsPerChunkHistory = self.convertCommaSepLineToNumbers(self.wiki.config.revsPerChunkHistory)
			self._recombineHistory = self.wiki.config.recombineHistory
		else:
			self._pagesPerChunkHistory = False
			self._revsPerChunkHistory = False
			self._pagesPerChunkAbstract = False
			self._recombineHistory = False
		if (self._chunksEnabled):
			if (self._revsPerChunkHistory):
				if (len(self._revsPerChunkHistory) == 1):
					self._numChunksHistory = self.getNumberOfChunksForXMLDumps(self.Stats.totalEdits, self._pagesPerChunkHistory[0])
					self._revsPerChunkHistory = [ self._revsPerChunkHistory[0] for i in range(self._numChunksHistory)]
				else:
					self._numChunksHistory = len(self._revsPerChunkHistory)
				# here we should generate the number of pages per chunk based on number of revs.
				# ...next code update! FIXME
				# self._pagesPerChunkHistory = ....
			elif (self._pagesPerChunkHistory):
				if (len(self._pagesPerChunkHistory) == 1):
					self._numChunksHistory = self.getNumberOfChunksForXMLDumps(self.Stats.totalPages, self._pagesPerChunkHistory[0])
					self._pagesPerChunkHistory = [ self._pagesPerChunkHistory[0] for i in range(self._numChunksHistory)]
				else:
					self._numChunksHistory = len(self._pagesPerChunkHistory)
			else:
				self._numChunksHistory = 0

			if (self._pagesPerChunkAbstract):
				if (len(self._pagesPerChunkAbstract) == 1):
					self._numChunksAbstract = self.getNumberOfChunksForXMLDumps(self.Stats.totalPages, self._pagesPerChunkAbstract[0])
					self._pagesPerChunkAbstract = [ self._pagesPerChunkAbstract[0] for i in range(self._numChunksAbstract)]
				else:
					self._numChunksAbstract = len(self._pagesPerChunkAbstract)
			else:
				self._numChunksAbstract = 0

	def convertCommaSepLineToNumbers(self, line):
		if (line == ""):
			return(False)
		result = line.split(',')
		numbers = []
		for field in result:
			field = field.strip()
			numbers.append(int(field))
		return(numbers)

	def getPagesPerChunkAbstract(self):
		return self._pagesPerChunkAbstract

	def getNumChunksAbstract(self):
		return self._numChunksAbstract

	def getPagesPerChunkHistory(self):
		return self._pagesPerChunkHistory

	def getNumChunksHistory(self):
		return self._numChunksHistory

	def chunksEnabled(self):
		return self._chunksEnabled

	def recombineHistory(self):
		return self._recombineHistory

	# args: total (pages or revs), and the number of (pages or revs) per chunk.
	def getNumberOfChunksForXMLDumps(self, total, perChunk):
		if (not total):
			# default: no chunking.
			return 0
		else:
			chunks = int(total/perChunk)
			# more smaller chunks are better, we want speed
			if (total - (chunks * perChunk)) > 0:
				chunks = chunks + 1
			if chunks == 1:
				return 0
			return chunks
