# Worker process, does the actual dumping

import getopt
import hashlib
import os
import re
import sys
import time
import subprocess
import select
import shutil
import stat
import signal
import errno
import glob
import WikiDump
import CommandManagement
import Queue
import thread
import traceback
import socket

from os.path import exists
from subprocess import Popen, PIPE
from WikiDump import FileUtils, MiscUtils, TimeUtils
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel

class Maintenance(object):

	def inMaintenanceMode():
		"""Use this to let callers know that we really should not
		be running.  Callers should try to exit the job
		they are running as soon as possible."""
		return exists("maintenance.txt")

	def exitIfInMaintenanceMode(message = None):
		"""Call this from possible exit points of running jobs
		in order to exit if we need to"""
		if Maintenance.inMaintenanceMode():
			if message:
				raise BackupError(message)
			else:
				raise BackupError("In maintenance mode, exiting.")
			
	inMaintenanceMode = staticmethod(inMaintenanceMode)
	exitIfInMaintenanceMode = staticmethod(exitIfInMaintenanceMode)
	
class Logger(object):

	def __init__(self, logFileName=None):
		if (logFileName):
			self.logFile = open(logFileName, "a")
		else:
			self.logFile = None
		self.queue = Queue.Queue()
		self.JobsDone = "JOBSDONE"

	def logWrite(self, line=None):
		if (self.logFile):
			self.logFile.write(line)
			self.logFile.flush()

	def logClose(self):
		if (logfile):
			self.logFile.close()

	# return 1 if logging terminated, 0 otherwise
	def doJobOnLogQueue(self):
		line = self.queue.get()
		if (line == self.JobsDone):
			self.logClose()
			return 1
		else:
			self.logWrite(line)
			return 0

	def addToLogQueue(self,line=None):
		if (line):
			self.queue.put_nowait(line)

	# set in order to have logging thread clean up and exit
	def indicateJobsDone(self):
		self.queue.put_nowait(self.JobsDone)

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
			self._pagesPerChunkHistory = self.convertCommaSepLineToNumbers(self.wiki.config.pagesPerChunkHistory)
			self._revsPerChunkHistory = self.convertCommaSepLineToNumbers(self.wiki.config.revsPerChunkHistory)
			self._pagesPerChunkAbstract = self.convertCommaSepLineToNumbers(self.wiki.config.pagesPerChunkAbstract)
			self._recombineHistory = self.wiki.config.recombineHistory
		else:
			self._pagesPerChunkHistory = False
			self._revsPerChunkHistory = False
			self._pagesPerChunkAbstract = False
			self._recombineHistory = False
		if (self._chunksEnabled):
			self.Stats = PageAndEditStats(self.wiki,dbName, errorCallback)
			if (not self.Stats.totalEdits or not self.Stats.totalPages):
				raise BackupError("Failed to get DB stats, exiting")
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
		return self._numChunksAbtsract

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

		query = "select MAX(page_id) from page;"
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
		query = "select MAX(rev_id) from revision;"
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

class BackupError(Exception):
	pass

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
					print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
				print "Couldn't save dump run info file. Continuing anyways"

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
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
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
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
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
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
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

class DumpItemList(object):
	def __init__(self, wiki, prefetch, spawn, chunkToDo, checkpointFile, singleJob, chunkInfo, pageIDRange, runInfoFile, dumpDir):
		self.wiki = wiki
		self._hasFlaggedRevs = self.wiki.hasFlaggedRevs()
		self._prefetch = prefetch
		self._spawn = spawn
		self.chunkInfo = chunkInfo
		self.checkpointFile = checkpointFile
		self._chunkToDo = chunkToDo
		self._singleJob = singleJob
		self._runInfoFile = runInfoFile
		self.dumpDir = dumpDir
		self.pageIDRange = pageIDRange

		if self.wiki.config.checkpointTime:
			checkpoints = True
		else:
			checkpoints = False
		
		if (self._singleJob and self._chunkToDo):
			if (self._singleJob[-5:] == 'table' or 
			    self._singleJob[-9:] == 'recombine' or 
			    self._singleJob == 'noop' or 
			    self._singleJob == 'latestlinks' or 
			    self._singleJob == 'xmlpagelogsdump' or
			    self._singleJob == 'pagetitlesdump' or
			    self._singleJob.endswith('recombine')):
				raise BackupError("You cannot specify a chunk with the job %s, exiting.\n" % self._singleJob)

		if (self._singleJob and self.checkpointFile):
			if (self._singleJob[-5:] == 'table' or 
			    self._singleJob[-9:] == 'recombine' or 
			    self._singleJob == 'noop' or 
			    self._singleJob == 'latestlinks' or 
			    self._singleJob == 'xmlpagelogsdump' or
			    self._singleJob == 'pagetitlesdump' or
			    self._singleJob == 'abstractsdump' or
			    self._singleJob == 'xmlstubsdump' or
			    self._singleJob.endswith('recombine')):
				raise BackupError("You cannot specify a checkpoint file with the job %s, exiting.\n" % self._singleJob)

		self.dumpItems = [PrivateTable("user", "usertable", "User account data."),
			PrivateTable("watchlist", "watchlisttable", "Users' watchlist settings."),
			PrivateTable("ipblocks", "ipblockstable", "Data for blocks of IP addresses, ranges, and users."),
			PrivateTable("archive", "archivetable", "Deleted page and revision data."),
#			PrivateTable("updates", "updatestable", "Update dataset for OAI updater system."),
			PrivateTable("logging", "loggingtable", "Data for various events (deletions, uploads, etc)."),
			#PrivateTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded images."),
			#PrivateTable("filearchive", "filearchivetable", "Deleted image data"),

			PublicTable("site_stats", "sitestatstable", "A few statistics such as the page count."),
			PublicTable("image", "imagetable", "Metadata on current versions of uploaded media/files."),
			PublicTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded media/files."),
			PublicTable("pagelinks", "pagelinkstable", "Wiki page-to-page link records."),
			PublicTable("categorylinks", "categorylinkstable", "Wiki category membership link records."),
			PublicTable("imagelinks", "imagelinkstable", "Wiki media/files usage records."),
			PublicTable("templatelinks", "templatelinkstable", "Wiki template inclusion link records."),
			PublicTable("externallinks", "externallinkstable", "Wiki external URL link records."),
			PublicTable("langlinks", "langlinkstable", "Wiki interlanguage link records."),
			PublicTable("interwiki", "interwikitable", "Set of defined interwiki prefixes and links for this wiki."),
			PublicTable("user_groups", "usergroupstable", "User group assignments."),
			PublicTable("category", "categorytable", "Category information."),

			PublicTable("page", "pagetable", "Base per-page data (id, title, old restrictions, etc)."),
			PublicTable("page_restrictions", "pagerestrictionstable", "Newer per-page restrictions table."),
			PublicTable("page_props", "pagepropstable", "Name/value pairs for pages."),
			PublicTable("protected_titles", "protectedtitlestable", "Nonexistent pages that have been protected."),
			#PublicTable("revision", #revisiontable", "Base per-revision data (does not include text)."), // safe?
			#PrivateTable("text", "texttable", "Text blob storage. May be compressed, etc."), // ?
			PublicTable("redirect", "redirecttable", "Redirect list"),
			PublicTable("iwlinks", "iwlinkstable", "Interwiki link tracking records"),

			TitleDump("pagetitlesdump", "List of page titles"),

			AbstractDump("abstractsdump","Extracted page abstracts for Yahoo", self._getChunkToDo("abstractsdump"), self.chunkInfo.getPagesPerChunkAbstract())]

		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineAbstractDump("abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo", self.findItemByName('abstractsdump')))

		self.dumpItems.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps", self._getChunkToDo("xmlstubsdump"), self.chunkInfo.getPagesPerChunkHistory()))
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlStub("xmlstubsdumprecombine", "Recombine first-pass for page XML data dumps",self.findItemByName('xmlstubsdump')))

		# NOTE that the chunkInfo thing passed here is irrelevant, these get generated from the stubs which are all done in one pass
		self.dumpItems.append(
			XmlDump("articles",
				"articlesdump",
				"<big><b>Articles, templates, media/file descriptions, and primary meta-pages.</b></big>",
				"This contains current versions of article content, and is the archive most mirror sites will probably want.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("articlesdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlDump("articlesdumprecombine", "<big><b>Recombine articles, templates, media/file descriptions, and primary meta-pages.</b></big>","This contains current versions of article content, and is the archive most mirror sites will probably want.",  self.findItemByName('articlesdump')))
		
		self.dumpItems.append(
			XmlDump("meta-current",
				"metacurrentdump",
				"All pages, current versions only.",
				"Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("metacurrentdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))
			
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlDump("metacurrentdumprecombine", "Recombine all pages, current versions only.","Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.findItemByName('metacurrentdump')))

		self.dumpItems.append(
			XmlLogging("Log events to all pages and users."))
			
		if self._hasFlaggedRevs:
			self.dumpItems.append(
				PublicTable( "flaggedpages", "flaggedpagestable","This contains a row for each flagged article, containing the stable revision ID, if the lastest edit was flagged, and how long edits have been pending." ))
			self.dumpItems.append(
				PublicTable( "flaggedrevs", "flaggedrevstable","This contains a row for each flagged revision, containing who flagged it, when it was flagged, reviewer comments, the flag values, and the quality tier those flags fall under." ))
					      
		self.dumpItems.append(
			BigXmlDump("meta-history",
				   "metahistorybz2dump",
				   "All pages with complete page edit history (.bz2)",
				   "These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
				   "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._getChunkToDo("metahistorybz2dump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile, self.pageIDRange))
		if (self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory()):
			self.dumpItems.append(
				RecombineXmlDump("metahistorybz2dumprecombine",
						 "Recombine all pages with complete edit history (.bz2)",
						 "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
						 "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistorybz2dump')))
		self.dumpItems.append(
			XmlRecompressDump("meta-history",
					  "metahistory7zdump",
					  "All pages with complete edit history (.7z)",
					  "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
					  "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistorybz2dump'), self.wiki, self._getChunkToDo("metahistory7zdump"), self.chunkInfo.getPagesPerChunkHistory(), checkpoints, self.checkpointFile))
		if (self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory()):
			self.dumpItems.append(
				RecombineXmlRecompressDump("metahistory7zdumprecombine",
							   "Recombine all pages with complete edit history (.7z)",
							   "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
							   "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.findItemByName('metahistory7zdump'), self.wiki))
		# doing this only for recombined/full articles dump
		if self.wiki.config.multistreamEnabled:
			if (self.chunkInfo.chunksEnabled()):
				inputForMultistream = "articlesdumprecombine"
			else:
				inputForMultistream = "articlesdump"
			self.dumpItems.append(
				XmlMultiStreamDump("articles",
					   "articlesmultistreamdump",
					   "Articles, templates, media/file descriptions, and primary meta-pages, in multiple bz2 streams, 100 pages per stream",
					   "This contains current versions of article content, in concatenated bz2 streams, 100 pages per stream, plus a separate" +
					   "index of page titles/ids and offsets into the file.  Useful for offline readers, or for parallel processing of pages.",
					   self.findItemByName(inputForMultistream), self.wiki, None))

		results = self._runInfoFile.getOldRunInfoFromFile()
		if (results):
			for runInfoObj in results:
				self._setDumpItemRunInfo(runInfoObj)
			self.oldRunInfoRetrieved = True
		else:
			self.oldRunInfoRetrieved = False
		
	def reportDumpRunInfo(self, done=False):
		"""Put together a dump run info listing for this database, with all its component dumps."""
		runInfoLines = [self._reportDumpRunInfoLine(item) for item in self.dumpItems]
		runInfoLines.reverse()
		text = "\n".join(runInfoLines)
		text = text + "\n"
		return text

	def allPossibleJobsDone(self):
		for item in self.dumpItems:
			if (item.status() != "done" and item.status() != "failed"):
				return False
		return True

	# determine list of dumps to run ("table" expands to all table dumps,
	# the rest of the names expand to single items)
	# and mark the items in the list as such
	# return False if there is no such dump or set of dumps
        def markDumpsToRun(self,job):
		if (job == "tables"):
			for item in self.dumpItems:
				if (item.name()[-5:] == "table"):
					item.setToBeRun(True)
			return True
		else:
			for item in self.dumpItems:
				if (item.name() == job):
					item.setToBeRun(True)
					return True
		if job == "noop" or job == "latestlinks":
			return True
		print "No job of the name specified exists. Choose one of the following:"
		print "noop (runs no job but rewrites md5sums file and resets latest links)"
		print "latestlinks (runs no job but resets latest links)"
		print "tables (includes all items below that end in 'table')"
		for item in self.dumpItems:
			print "%s " % item.name()
	        return False

	def markFollowingJobsToRun(self):
		# find the first one marked to run, mark the following ones
		i = 0;
		for item in self.dumpItems:
			i = i + 1;
			if item.toBeRun():
				for j in range(i,len(self.dumpItems)):
					self.dumpItems[j].setToBeRun(True)
				break

	def markAllJobsToRun(self):
		"""Marks each and every job to be run"""
		for item in self.dumpItems:
			item.setToBeRun( True )
					      
	def findItemByName(self, name):
		for item in self.dumpItems:
			if (item.name() == name):
				return item
		return None

	def _getChunkToDo(self, jobName):
		if (self._singleJob):
			if (self._singleJob == jobName):
				return(self._chunkToDo)
		return(False)

	# read in contents from dump run info file and stuff into dumpItems for later reference
	def _setDumpItemRunInfo(self, runInfo):
		if (not runInfo.name()):
			return False
		for item in self.dumpItems:
			if (item.name() == runInfo.name()):
				item.setStatus(runInfo.status(),False)
				item.setUpdated(runInfo.updated())
				item.setToBeRun(runInfo.toBeRun())
				return True
		return False

	# write dump run info file 
	# (this file is rewritten with updates after each dumpItem completes)
	def _reportDumpRunInfoLine(self, item):
		# even if the item has never been run we will at least have "waiting" in the status
		return "name:%s; status:%s; updated:%s" % (item.name(), item.status(), item.updated())

class Checksummer(object):
	def __init__(self,wiki,dumpDir, enabled = True, verbose = False):
		self.wiki = wiki
		self.dumpDir = dumpDir
		self.verbose = verbose
		self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
		self._enabled = enabled

	def prepareChecksums(self):
		"""Create a temporary md5 checksum file.
		Call this at the start of the dump run, and move the file
		into the final location at the completion of the dump run."""
		if (self._enabled):
			checksumFileName = self._getChecksumFileNameTmp()
			output = file(checksumFileName, "w")

	def checksum(self, fileObj, runner):
		"""Run checksum for an output file, and append to the list."""
		if (self._enabled):
			checksumFileName = self._getChecksumFileNameTmp()
			output = file(checksumFileName, "a")
			runner.debug("Checksumming %s" % fileObj.filename)
			dumpfile = DumpFile(self.wiki, runner.dumpDir.filenamePublicPath(fileObj),None,self.verbose)
			checksum = dumpfile.md5Sum()
			if checksum != None:
				output.write( "%s  %s\n" % (checksum, fileObj.filename))
			output.close()

	def moveMd5FileIntoPlace(self):
		if (self._enabled):
			tmpFileName = self._getChecksumFileNameTmp()
			realFileName = self._getChecksumFileName()
			os.rename(tmpFileName, realFileName)

	def cpMd5TmpFileToPermFile(self):
		if (self._enabled):
			tmpFileName = self._getChecksumFileNameTmp()
			realFileName = self._getChecksumFileName()
			text = FileUtils.readFile(tmpFileName)
			FileUtils.writeFile(self.wiki.config.tempDir, realFileName, text, self.wiki.config.fileperms)

	def getChecksumFileNameBasename(self):
		return ("md5sums.txt")

	#
	# functions internal to the class
	#
	def _getChecksumFileName(self):
		fileObj = DumpFilename(self.wiki, None, self.getChecksumFileNameBasename())
		return (self.dumpDir.filenamePublicPath(fileObj))

	def _getChecksumFileNameTmp(self):
		fileObj = DumpFilename(self.wiki, None, self.getChecksumFileNameBasename() + "." + self.timestamp + ".tmp")
		return (self.dumpDir.filenamePublicPath(fileObj))

	def _getMd5FileDirName(self):
		return os.path.join(self.wiki.publicDir(), self.wiki.date)

class DumpDir(object):
	def __init__(self, wiki, dbName):
		self._wiki = wiki
		self._dbName = dbName
		self._dirCache = {}
		self._dirCacheTime = {}
		self._chunkFileCache = {}
		self._checkpointFileCache = {}

	def filenamePrivatePath(self, dumpFile, dateString = None):
		"""Given a DumpFilename object, produce the full path to the filename in the date subdir 
		of the the private dump dir for the selected database.
		If a different date is specified, use that instead"""
		if (not dateString):
			dateString = self._wiki.date
		return os.path.join(self._wiki.privateDir(), dateString, dumpFile.filename)

	def filenamePublicPath(self, dumpFile, dateString = None):
		"""Given a DumpFilename object produce the full path to the filename in the date subdir 
		of the public dump dir for the selected database.
		If this database is marked as private, use the private dir instead.
		If a different date is specified, use that instead"""
		if (not dateString):
			dateString = self._wiki.date
		return os.path.join(self._wiki.publicDir(), dateString, dumpFile.filename)

	def latestDir(self):
		"""Return 'latest' directory for the current project being dumped, e.g. 
		if the current project is enwiki, this would return something like 
		/mnt/data/xmldatadumps/public/enwiki/latest (if the directory /mnt/data/xmldatadumps/public 
		is the path to the directory for public dumps)."""
		return os.path.join(self._wiki.publicDir(), "latest")

	def webPath(self, dumpFile, dateString = None):
		"""Given a DumpFilename object produce the full url to the filename for the date of
		the dump for the selected database."""
		if (not dateString):
			dateString = self._wiki.date
		return os.path.join(self._wiki.webDir(), dateString, dumpFile.filename)


	def webPathRelative(self, dumpFile, dateString = None):
		"""Given a DumpFilename object produce the url relative to the docroot for the filename for the date of
		the dump for the selected database."""
		if (not dateString):
			dateString = self._wiki.date
		return os.path.join(self._wiki.webDirRelative(), dateString, dumpFile.filename)

	def dirCacheOutdated(self, date):
		if not date:
			date = self._wiki.date
		directory = os.path.join(self._wiki.publicDir(), date)
		if exists(directory):
			dirTimeStamp = os.stat(directory).st_mtime
			if (not date in self._dirCache or dirTimeStamp > self._dirCacheTime[date]):
				return True
			else:
				return False
		else:
			return True

	# warning: date can also be "latest"
	def getFilesInDir(self, date = None):
		if not date:
			date = self._wiki.date
		if (self.dirCacheOutdated(date)):
			directory = os.path.join(self._wiki.publicDir(),date)
			if exists(directory):
				dirTimeStamp = os.stat(directory).st_mtime
				files = os.listdir(directory)
				fileObjs = []
				for f in files:
					fileObj = DumpFilename(self._wiki)
					fileObj.newFromFilename(f)
					fileObjs.append(fileObj)
				self._dirCache[date] = fileObjs
				# The directory listing should get cached. However, some tyical file
				# system's (eg. ext2, ext3) mtime's resolution is 1s. If we would
				# unconditionally cache, it might happen that we cache at x.1 seconds
				# (with mtime x). If a new file is added to the filesystem at x.2,
				# the directory's mtime would still be set to x. Hence we would not
				# detect that the cache needs to be purged. Therefore, we cache only,
				# if adding a file now would yield a /different/ mtime.
				if time.time() >= dirTimeStamp + 1:
					self._dirCacheTime[date] = dirTimeStamp
				else:
					# By setting _dirCacheTime to 0, we provoke an outdated cache
					# on the next check. Hence, we effectively do not cache.
					self._dirCacheTime[date] = 0
			else:
				self._dirCache[date] = []
		return(self._dirCache[date])

	# list all files that exist, filtering by the given args. 
	# if we get None for an arg then we accept all values for that arg in the filename, including missing
	# if we get False for an arg (chunk, temp, checkpoint), we reject any filename which contains a value for that arg
	# if we get True for an arg (chunk, temp, checkpoint), we include only filenames which contain a value for that arg
	# chunks should be a list of value(s) or True / False / None
	#
	# note that we ignore files with ".truncated". these are known to be bad.
	def _getFilesFiltered(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = None, temp = None, checkpoint = None ):
		if not date:
			date = self._wiki.date
		fileObjs = self.getFilesInDir(date)
		filesMatched = []
		for f in fileObjs:
			# fixme this is a bit hackish
			if f.filename.endswith("truncated"):
				continue

			if dumpName and f.dumpName != dumpName:
				continue
			if fileType != None and f.fileType != fileType:
				continue
			if fileExt != None and f.fileExt != fileExt:
				continue
			if (chunks == False and f.isChunkFile):
				continue
			if (chunks == True and not f.isChunkFile):
				continue
			# chunks is a list...
			if (chunks and chunks != True and not f.chunkInt in chunks):
				continue
			if (temp == False and f.isTempFile) or (temp and not f.isTempFile):
				continue
			if (checkpoint == False and f.isCheckpointFile) or (checkpoint and not f.isCheckpointFile):
				continue
			filesMatched.append(f)
			self.sort_fileobjs(filesMatched)
		return filesMatched

	# taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
	def sort_fileobjs(self, l): 
		""" Sort the given list in the way that humans expect. 
		""" 
		convert = lambda text: int(text) if text.isdigit() else text 
		alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key.filename) ] 
		l.sort( key=alphanum_key ) 

	# list all checkpoint files that exist, filtering by the given args. 
	# if we get None for an arg then we accept all values for that arg in the filename
	# if we get False for an arg (chunks, temp), we reject any filename which contains a value for that arg
	# if we get True for an arg (chunk, temp), we accept only filenames which contain a value for the arg
	# chunks should be a list of value(s), or True / False / None
	def getCheckpointFilesExisting(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = False, temp = False ):
		return self._getFilesFiltered(date, dumpName, fileType, fileExt, chunks, temp, checkpoint = True)

	# list all non-checkpoint files that exist, filtering by the given args. 
	# if we get None for an arg then we accept all values for that arg in the filename
	# if we get False for an arg (chunk, temp), we reject any filename which contains a value for that arg
	# if we get True for an arg (chunk, temp), we accept only filenames which contain a value for the arg
	# chunks should be a list of value(s), or True / False / None
	def getRegularFilesExisting(self, date = None, dumpName = None, fileType = None, fileExt = None, chunks = False, temp = False ):
		return self._getFilesFiltered(date, dumpName, fileType, fileExt, chunks, temp, checkpoint = False)


class DumpFilename(object):
	"""
	filename without directory name, and the methods that go with it,
	primarily for filenames that follow the standard naming convention, i.e. 
	projectname-date-dumpName.sql/xml.gz/bz2/7z (possibly with a chunk 
	number, possibly with start/end page id information embedded in the name).

	Constructor:
	DumpFilename(dumpName, date = None, filetype, ext, chunk = None, checkpoint = None, temp = False) -- pass in dumpName and 
	                              filetype/extension at least. filetype is one of xml sql, extension is one of
                                      bz2/gz/7z.  Or you can pass in the entire string without project name and date,
				      e.g. pages-meta-history5.xml.bz2
				      If dumpName is not passed, no member variables will be initialized, and
				      the caller is expected to invoke newFromFilename as an alternate
				      constructor before doing anything else with the object.
				      
	newFromFilename(filename)  -- pass in full filename. This is called by the regular constructor and is 
                                      what sets all attributes

	attributes:

	isCheckpointFile  filename of form dbname-date-dumpName-pxxxxpxxxx.xml.bz2
	isChunkFile       filename of form dbname-date-dumpNamex.xml.gz/bz2/7z
	isTempFile        filename of form dbname-date-dumpName.xml.gz/bz2/7z-tmp
	firstPageID       for checkpoint files, taken from value in filename
	lastPageID        for checkpoint files, value taken from filename
	filename          full filename
	basename          part of the filename after the project name and date (for 
                          "enwiki-20110722-pages-meta-history12.xml.bz2" this would be
                          "pages-meta-history12.xml.bz2")
	fileExt           extension (everything after the last ".") of the file
	date              date embedded in filename
	dumpName          dump name embedded in filename (eg "pages-meta-history"), if any
	chunk             chunk number of file as string (for "pages-meta-history5.xml.bz2" this would be "5")
	chinkInt          chunk number as int
	"""

	def __init__(self, wiki, date = None, dumpName = None, filetype = None, ext = None, chunk = None, checkpoint = None, temp = False):
		"""Constructor.  Arguments: the dump name as it should appear in the filename, 
		the date if different than the date of the dump run, the chunk number
		if there is one, and temp which is true if this is a temp file (ending in "-tmp") 
		Alternatively, one can leave off all other other stuff and just pass the entire 
		filename minus the dbname and the date. Returns true on success, false otherwise.."""
		self.wiki = wiki
		# if dumpName is not set, the caller can call newFromFilename to initialize various values instead
		if dumpName:
			filename =  self.newFilename(dumpName, filetype, ext, date, chunk, checkpoint, temp)
			self.newFromFilename(filename)

	def isExt(self,ext):
		if ext == "gz" or ext == "bz2" or ext == "7z" or ext == "html" or ext == "txt":
			return True
		else:
			return False

	# returns True if successful, False otherwise (filename is not in the canonical form that we manage)
	def newFromFilename(self, filename):
		"""Constructor.  Arguments: the full file name including the chunk, the extension, etc BUT NOT the dir name. """
		self.filename = filename

		self.dbName = None
		self.date = None
		self.dumpName = None

		self.basename = None
		self.fileExt = None
		self.fileType = None

		self.filePrefix = ""
		self.filePrefixLength = 0

		self.isChunkFile = False
		self.chunk = None
		self.chunkInt = 0

		self.isCheckpointFile = False
		self.checkpoint = None
		self.firstPageID = None
		self.lastPageID = None

		self.isTempFile = False
		self.temp = None

		# example filenames:
		# elwikidb-20110729-all-titles-in-ns0.gz
		# elwikidb-20110729-abstract.xml
		# elwikidb-20110727-pages-meta-history2.xml-p000048534p000051561.bz2

		# we need to handle cases without the projectname-date stuff in them too, as this gets used
		# for all files now
		if self.filename.endswith("-tmp"):
			self.isTempFile = True
			self.temp = "-tmp"

		if ('.' in self.filename):
			(fileBase, self.fileExt) = self.filename.rsplit('.',1)
			if (self.temp):
				self.fileExt = self.fileExt[:-4];
		else:
			return False

		if not self.isExt(self.fileExt):
			self.fileType = self.fileExt
#			self.fileExt = None
			self.fileExt = ""
		else:
			if '.' in fileBase:
				(fileBase, self.fileType) = fileBase.split('.',1)

		# some files are not of this form, we skip them
		if not '-' in fileBase:
			return False

		(self.dbName, self.date, self.dumpName) = fileBase.split('-',2)
		if not self.date or not self.dumpName:
			self.dumpName = fileBase
		else:
			self.filePrefix = "%s-%s-" % (self.dbName, self.date)
			self.filePrefixLength = len(self.filePrefix)

		if self.filename.startswith(self.filePrefix):
			self.basename = self.filename[self.filePrefixLength:]

		self.checkpointPattern = "-p(?P<first>[0-9]+)p(?P<last>[0-9]+)\." + self.fileExt + "$"
		self.compiledCheckpointPattern = re.compile(self.checkpointPattern)
		result = self.compiledCheckpointPattern.search(self.filename)

		if result:
			self.isCheckpointFile = True
			self.firstPageID = result.group('first')
			self.lastPageID = result.group('last')
			self.checkpoint = "p" + self.firstPageID + "p" + self.lastPageID
			if self.fileType and self.fileType.endswith("-" + self.checkpoint):
				self.fileType = self.fileType[:-1 * ( len(self.checkpoint) + 1 ) ]

		self.chunkPattern = "(?P<chunk>[0-9]+)$"
		self.compiledChunkPattern = re.compile(self.chunkPattern)
		result = self.compiledChunkPattern.search(self.dumpName)
		if result:
			self.isChunkFile = True
			self.chunk = result.group('chunk')
			self.chunkInt = int(self.chunk)
			# the dumpName has the chunk in it so lose it
			self.dumpName = self.dumpName.rstrip('0123456789')

		return True

	def newFilename(self, dumpName, filetype, ext, date = None, chunk = None, checkpoint = None, temp = None):
		if not chunk:
			chunk = ""
		if not date:
			date = self.wiki.date
		# fixme do the right thing in case no filetype or no ext
		parts = []
		parts.append(self.wiki.dbName + "-" + date + "-" + dumpName + "%s" % chunk)
		if checkpoint:
			filetype = filetype + "-" + checkpoint
		if filetype:
			parts.append(filetype)
		if ext:
			parts.append(ext)
		filename = ".".join(parts)
		if temp:
			filename = filename + "-tmp"
		return filename

class DumpFile(file):
	"""File containing output created by any job of a jump run.  This includes
	any file that follows the standard naming convention, i.e. 
	projectname-date-dumpName.sql/xml.gz/bz2/7z (possibly with a chunk 
	number, possibly with start/end page id information embedded in the name).

	Methods:

	md5Sum(): return md5sum of the file contents.
	checkIfTruncated(): for compressed files, check if the file is truncated (stops
	   abruptly before the end of the compressed data) or not, and set and return 
  	   self.isTruncated accordingly.  This is fast for bzip2 files 
	   and slow for gz and 7z fles, since for the latter two types it must serially
	   read through the file to determine if it is truncated or not.
	getSize(): returns the current size of the file in bytes
	rename(newname): rename the file. Arguments: the new name of the file without 
	   the directory.
	findFirstPageIDInFile(): set self.firstPageID by examining the file contents, 
	   returning the value, or None if there is no pageID.  We uncompress the file 
	   if needed and look through the first 500 lines.

	plus the usual file methods (read, write, open, close)

	useful variables:

	firstPageID       Determined by examining the first few hundred lines of the contents,
                          looking for page and id tags, wihout other tags in between. (hmm)
	filename          full filename with directory
	"""
	def __init__(self, wiki, filename, fileObj = None, verbose = False):
		"""takes full filename including path"""
		self._wiki = wiki
		self.filename = filename
		self.firstLines = None
		self.isTruncated = None
		self.firstPageID = None
		self.dirname = os.path.dirname(filename)
		if fileObj:
			self.fileObj = fileObj
		else:
			self.fileObj = DumpFilename(wiki)
			self.fileObj.newFromFilename(os.path.basename(filename))

	def md5Sum(self):
		if not self.filename:
			return None
		summer = hashlib.md5()
		infile = file(self.filename, "rb")
		bufsize = 4192 * 32
		buffer = infile.read(bufsize)
		while buffer:
			summer.update(buffer)
			buffer = infile.read(bufsize)
		infile.close()
		return summer.hexdigest()

	def getFirst500Lines(self):
		if self.firstLines:
			return(self.firstLines)

		if not self.filename or not exists(self.filename):
			return None

		pipeline = self.setupUncompressionCommand()

		if (not exists( self._wiki.config.head ) ):
			raise BackupError("head command %s not found" % self._wiki.config.head)
		head = self._wiki.config.head
		headEsc = MiscUtils.shellEscape(head)
		pipeline.append([ head, "-500" ])
		# without shell
		p = CommandPipeline(pipeline, quiet=True)
		p.runPipelineAndGetOutput()
		if p.exitedSuccessfully() or p.getFailedCommandsWithExitValue() == [[ -signal.SIGPIPE, pipeline[0] ]] or p.getFailedCommandsWithExitValue() == [[ signal.SIGPIPE + 128, pipeline[0] ]]:
			self.firstLines = p.output()
		return(self.firstLines)

	# unused
	# xml, sql, text
	def determineFileContentsType(self):
		output = self.getFirst500Lines()
		if (output):
			pageData = output
			if (pageData.startswith('<mediawiki')):
				return('xml')
			if (pageData.startswith('-- MySQL dump')):
				return('sql')
			return('txt')
		return(None)

	def setupUncompressionCommand(self):
		if not self.filename or not exists(self.filename):
			return None
		pipeline = []
		if self.fileObj.fileExt == 'bz2':
			command = [ self._wiki.config.bzip2, '-dc' ]
		elif self.fileObj.fileExt == 'gz':
			command = [ self._wiki.config.gzip, '-dc' ]
		elif self.fileObj.fileExt == '7z':
			command = [ self._wiki.config.sevenzip, "e", "-so" ]
		else:
			command = [ self._wiki.config.cat ]

		if (not exists( command[0] ) ):
			raise BackupError( "command %s to uncompress/read file not found" % command[0] )
		command.append( self.filename )
		pipeline.append(command)
		return(pipeline)

	# unused
	# return its first and last page ids from name or from contents, depending
	# return its date 
	
	# fixme what happens if this is not an xml dump? errr. must detect and bail immediately? 
	# maybe instead of all that we should just open the file ourselves, read a few lines... oh.
	# right. stupid compressed files. um.... do we have stream wrappers? no. this is python
	# what's the easy was to read *some* compressed data into a buffer?
	def findFirstPageIDInFile(self):
		if (self.firstPageID):
			return(self.firstPageID)
		output = self.getFirst500Lines()
		if (output):
			pageData = output
			titleAndIDPattern = re.compile('<title>(?P<title>.+?)</title>\s*' + '(<ns>[0-9]+</ns>\s*)?' + '<id>(?P<pageid>\d+?)</id>')
			result = titleAndIDPattern.search(pageData)
			if (result):
				self.firstPageID = result.group('pageid')
		return(self.firstPageID)

	def checkIfTruncated(self):
		if self.isTruncated:
			return self.isTruncated

		# Setting up the pipeline depending on the file extension
		if self.fileObj.fileExt == "bz2":
			if (not exists( self._wiki.config.checkforbz2footer ) ):
				raise BackupError("checkforbz2footer command %s not found" % runner.wiki.config.checkforbz2footer)
			checkforbz2footer = self._wiki.config.checkforbz2footer
			pipeline = []
			pipeline.append([ checkforbz2footer, self.filename ])
		else:
			if self.fileObj.fileExt == 'gz':
				pipeline = [ [ self._wiki.config.gzip, "-dc", self.filename, ">", "/dev/null" ] ]
			elif self.fileObj.fileExt == '7z':
				# Note that 7z does return 0, if archive contains
				# garbage /after/ the archive end
				pipeline = [ [ self._wiki.config.sevenzip, "e", "-so", self.filename, ">", "/dev/null" ] ]
			else:
				# we do't know how to handle this type of file.
				return self.isTruncated

		# Run the perpared pipeline
		p = CommandPipeline(pipeline, quiet=True)
		p.runPipelineAndGetOutput()
		self.isTruncated = not p.exitedSuccessfully()

		return self.isTruncated

	def getSize(self):
		if (exists(self.filename)):
			return os.path.getsize(self.filename)
		else:
			return None

	def rename(self, newname):
		try:
			os.rename(self.filename, os.path.join(self.dirname,newname))
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			raise BackupError("failed to rename file %s" % self.filename)

		self.filename = os.path.join(self.dirname,newname)

# everything that has to do with reporting the status of a piece
# of a dump is collected here
class Status(object):
	def __init__(self, wiki, dumpDir, items, checksums, enabled, email = True, noticeFile = None, errorCallback=None, verbose = False):
		self.wiki = wiki
		self.dbName = wiki.dbName
		self.dumpDir = dumpDir
		self.items = items
		self.checksums = checksums
		self.noticeFile = noticeFile
		self.errorCallback = errorCallback
		self.failCount = 0
		self.verbose = verbose
		self._enabled = enabled
		self.email = email

	def updateStatusFiles(self, done=False):
		if self._enabled:
			self._saveStatusSummaryAndDetail(done)
		
	def reportFailure(self):
		if self._enabled and self.email:
			if self.wiki.config.adminMail and self.wiki.config.adminMail.lower() != 'nomail':
				subject = "Dump failure for " + self.dbName
				message = self.wiki.config.readTemplate("errormail.txt") % {
					"db": self.dbName,
					"date": self.wiki.date,
					"time": TimeUtils.prettyTime(),
					"url": "/".join((self.wiki.config.webRoot, self.dbName, self.wiki.date, ''))}
				self.wiki.config.mail(subject, message)

	# this is a per-dump-item report (well, per file generated by the item)
	# Report on the file size & item status of the current output and output a link if we are done
	def reportFile(self, fileObj, itemStatus):
		filename = self.dumpDir.filenamePublicPath(fileObj)
		if (exists(filename)):
			size = os.path.getsize(filename)
		else:
			itemStatus = "missing"
			size = 0
		size = FileUtils.prettySize(size)
		if itemStatus == "in-progress":
			return "<li class='file'>%s %s (written) </li>" % (fileObj.filename, size)
		elif itemStatus == "done":
			webpathRelative = self.dumpDir.webPathRelative(fileObj)
			return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpathRelative, fileObj.filename, size)
		else:
			return "<li class='missing'>%s</li>" % fileObj.filename

	#
	# functions internal to the class
	#
	def _saveStatusSummaryAndDetail(self, done=False):
		"""Write out an HTML file with the status for this wiki's dump 
		and links to completed files, as well as a summary status in a separate file."""
		try:
			# Comprehensive report goes here
			self.wiki.writePerDumpIndex(self._reportDatabaseStatusDetailed(done))
			# Short line for report extraction goes here
			self.wiki.writeStatus(self._reportDatabaseStatusSummary(done))
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			message = "Couldn't update status files. Continuing anyways"
			if self.errorCallback:
				self.errorCallback(message)
			else:
				print(message)

	def _reportDatabaseStatusSummary(self, done = False):
		"""Put together a brief status summary and link for the current database."""
		status = self._reportStatusSummaryLine(done)
		html = self.wiki.reportStatusLine(status)

		activeItems = [x for x in self.items if x.status() == "in-progress"]
		if activeItems:
			return html + "<ul>" + "\n".join([self._reportItem(x) for x in activeItems]) + "</ul>"
		else:
			return html

	def _reportDatabaseStatusDetailed(self, done = False):
		"""Put together a status page for this database, with all its component dumps."""
		self.noticeFile.refreshNotice()
		statusItems = [self._reportItem(item) for item in self.items]
		statusItems.reverse()
		html = "\n".join(statusItems)
		f = DumpFilename(self.wiki, None, self.checksums.getChecksumFileNameBasename())
		return self.wiki.config.readTemplate("report.html") % {
			"db": self.dbName,
			"date": self.wiki.date,
			"notice": self.noticeFile.notice,
			"status": self._reportStatusSummaryLine(done),
			"previous": self._reportPreviousDump(done),
			"items": html,
			"checksum": self.dumpDir.webPathRelative(f),
			"index": self.wiki.config.index}

	def _reportPreviousDump(self, done):
		"""Produce a link to the previous dump, if any"""
		# get the list of dumps for this wiki in order, find me in the list, find the one prev to me.
		# why? we might be rerunning a job from an older dumps. we might have two
		# runs going at once (think en pedia, one finishing up the history, another
		# starting at the beginning to get the new abstracts and stubs).
		try:
			dumpsInOrder = self.wiki.latestDump(all=True)
			meIndex = dumpsInOrder.index(self.wiki.date)
			# don't wrap around to the newest dump in the list!
			if (meIndex > 0):
				rawDate = dumpsInOrder[meIndex-1]
			elif (meIndex == 0):
				# We are the first item in the list. This is not an error, but there is no
				# previous dump
				return "No prior dumps of this database stored."
			else:
				raise(ValueException)
		except:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			return "No prior dumps of this database stored."
		prettyDate = TimeUtils.prettyDate(rawDate)
		if done:
			prefix = ""
			message = "Last dumped on"
		else:
			prefix = "This dump is in progress; see also the "
			message = "previous dump from"
		return "%s<a href=\"../%s/\">%s %s</a>" % (prefix, rawDate, message, prettyDate)
				      
	def _reportStatusSummaryLine(self, done=False):
		if (done == "done"):
			classes = "done"
			text = "Dump complete"
		elif (done == "partialdone"):
			classes = "partial-dump"
			text = "Partial dump"
		else:
			classes = "in-progress"
			text = "Dump in progress"
		if self.failCount > 0:
			classes += " failed"
			if self.failCount == 1:
				ess = ""
			else:
				ess = "s"
			text += ", %d item%s failed" % (self.failCount, ess)
		return "<span class='%s'>%s</span>" % (classes, text)

	def _reportItem(self, item):
		"""Return an HTML fragment with info on the progress of this item."""
		item.status()
		item.updated()
		item.description()
		html = "<li class='%s'><span class='updates'>%s</span> <span class='status'>%s</span> <span class='title'>%s</span>" % (item.status(), item.updated(), item.status(), item.description())
		if item.progress:
			html += "<div class='progress'>%s</div>\n" % item.progress
		fileObjs = item.listOutputFilesToPublish(self.dumpDir)
		if fileObjs:
			listItems = [self.reportFile(fileObj, item.status()) for fileObj in fileObjs]
			html += "<ul>"
			detail = item.detail()
			if detail:
				html += "<li class='detail'>%s</li>\n" % detail
			html += "\n".join(listItems)
			html += "</ul>"
		html += "</li>"
		return html

class NoticeFile(object):
	def __init__(self, wiki, notice, enabled):
		self.wiki = wiki
		self.notice = notice
		self._enabled = enabled
		self.writeNoticeFile()

	def writeNoticeFile(self):
		if (self._enabled):
			noticeFile = self._getNoticeFilename()
			# delnotice.  toss any existing file 
			if self.notice == False:
				if exists(noticeFile):
					os.remove(noticeFile)
				self.notice = ""
			# addnotice, stuff notice in a file for other jobs etc
			elif self.notice != "":
				noticeDir = self._getNoticeDir()
				FileUtils.writeFile(self.wiki.config.tempDir, noticeFile, self.notice, self.wiki.config.fileperms)
			# default case. if there is a file get the contents, otherwise
			# we have empty contents, all good
			else:
				if exists(noticeFile):
					self.notice = FileUtils.readFile(noticeFile)

	def refreshNotice(self):
		# if the notice file has changed or gone away, we comply.
		noticeFile = self._getNoticeFilename()
		if exists(noticeFile):
			self.notice = FileUtils.readFile(noticeFile)
		else:
			self.notice = ""


	#
	# functions internal to class
	#
	def _getNoticeFilename(self):
		return os.path.join(self.wiki.publicDir(), self.wiki.date, "notice.txt")

	def _getNoticeDir(self):
		return os.path.join(self.wiki.publicDir(), self.wiki.date)

class Runner(object):
	def __init__(self, wiki, prefetch=True, spawn=True, job=None, restart=False, notice="", dryrun = False, loggingEnabled=False, chunkToDo = False, checkpointFile = None, pageIDRange = None, verbose = False):
		self.wiki = wiki
		self.dbName = wiki.dbName
		self.prefetch = prefetch
		self.spawn = spawn
		self.chunkInfo = Chunk(wiki, self.dbName, self.logAndPrint)
		self.restart = restart
		self.htmlNoticeFile = None
		self.log = None
		self.dryrun = dryrun
		self._chunkToDo = chunkToDo
		self.checkpointFile = checkpointFile
		self.pageIDRange = pageIDRange
		self.verbose = verbose

		if (self.checkpointFile):
			f = DumpFilename(self.wiki)
			f.newFromFilename(checkpointFile)
			# we should get chunk if any
			if not self._chunkToDo and f.chunkInt:
				self._chunkToDo = f.chunkInt
			elif self._chunkToDo and f.chunkInt and self._chunkToDo != f.chunkInt:
				raise BackupError("specifed chunk to do does not match chunk of checkpoint file %s to redo", self.checkpointFile)
			self.checkpointFile = f

		self._loggingEnabled = loggingEnabled
		self._statusEnabled = True
		self._checksummerEnabled = True
		self._runInfoFileEnabled = True
		self._symLinksEnabled = True
		self._feedsEnabled = True
		self._noticeFileEnabled = True
		self._makeDirEnabled = True
		self._cleanOldDumpsEnabled = True
		self._cleanupOldFilesEnabled = True
		self._checkForTruncatedFilesEnabled = True

		if self.dryrun or self._chunkToDo:
			self._statusEnabled = False
			self._checksummerEnabled = False
			self._runInfoFileEnabled = False
			self._symLinksEnabled = False
			self._feedsEnabled = False
			self._noticeFileEnabled = False
			self._makeDirEnabled = False
			self._cleanOldDumpsEnabled = False

		if self.dryrun:
			self._loggingEnabled = False
			self._checkForTruncatedFilesEnabled = False
			self._cleanupOldFilesEnabled = False

		if self.checkpointFile:
			self._statusEnabled = False
			self._checksummerEnabled = False
			self._runInfoFileEnabled = False
			self._symLinksEnabled = False
			self._feedsEnabled = False
			self._noticeFileEnabled = False
			self._makeDirEnabled = False
			self._cleanOldDumpsEnabled = False

		if self.pageIDRange:
			self._statusEnabled = False
			self._checksummerEnabled = False
			self._runInfoFileEnabled = False
			self._symLinksEnabled = False
			self._feedsEnabled = False
			self._noticeFileEnabled = False
			self._makeDirEnabled = False
			self._cleanupOldFilesEnabled = True

		self.jobRequested = job

		if self.jobRequested == "latestlinks":
			self._statusEnabled = False
			self._checksummerEnabled = False
			self._runInfoFileEnabled = False
			self._noticeFileEnabled = False
			self._makeDirEnabled = False
			self._cleanOldDumpsEnabled = False
			self._cleanupOldFilesEnabled = False
			self._checkForTruncatedFilesEnabled = False

		if self.jobRequested == "noop":
			self._cleanOldDumpsEnabled = False
			self._cleanupOldFilesEnabled = False
			self._checkForTruncatedFilesEnabled = False
				
		self.dbServerInfo = DbServerInfo(self.wiki, self.dbName, self.logAndPrint)
		self.dumpDir = DumpDir(self.wiki, self.dbName)

		# these must come after the dumpdir setup so we know which directory we are in 
		if (self._loggingEnabled and self._makeDirEnabled):
			fileObj = DumpFilename(self.wiki)
			fileObj.newFromFilename(self.wiki.config.logFile)
			self.logFileName = self.dumpDir.filenamePrivatePath(fileObj)
			self.makeDir(os.path.join(self.wiki.privateDir(), self.wiki.date))
			self.log = Logger(self.logFileName)
			thread.start_new_thread(self.logQueueReader,(self.log,))
		self.runInfoFile = RunInfoFile(wiki,self._runInfoFileEnabled, self.verbose)
		self.symLinks = SymLinks(self.wiki, self.dumpDir, self.logAndPrint, self.debug, self._symLinksEnabled)
		self.feeds = Feeds(self.wiki,self.dumpDir, self.dbName, self.debug, self._feedsEnabled)
		self.htmlNoticeFile = NoticeFile(self.wiki, notice, self._noticeFileEnabled)
		self.checksums = Checksummer(self.wiki, self.dumpDir, self._checksummerEnabled, self.verbose)

		# some or all of these dumpItems will be marked to run
		self.dumpItemList = DumpItemList(self.wiki, self.prefetch, self.spawn, self._chunkToDo, self.checkpointFile, self.jobRequested, self.chunkInfo, self.pageIDRange, self.runInfoFile, self.dumpDir)
		# only send email failure notices for full runs
		if (self.jobRequested):
			email = False
		else:
			email = True
		self.status = Status(self.wiki, self.dumpDir, self.dumpItemList.dumpItems, self.checksums, self._statusEnabled, email, self.htmlNoticeFile, self.logAndPrint, self.verbose)

	def logQueueReader(self,log):
		if not log:
			return
		done = False
		while not done:
			done = log.doJobOnLogQueue()
		
	def logAndPrint(self, message):
		if hasattr(self,'log') and self.log and self._loggingEnabled:
			self.log.addToLogQueue("%s\n" % message)
		print message

	def forceNormalOption(self):
		if self.wiki.config.forceNormal:
			return "--force-normal"
		else:
			return ""

	# returns 0 on success, 1 on error
	def saveCommand(self, commands, outfile):
		"""For one pipeline of commands, redirect output to a given file."""
		commands[-1].extend( [ ">" , outfile ] )
		series = [ commands ]
		if (self.dryrun):
			self.prettyPrintCommands([ series ])
			return 0
		else:
			return self.runCommand([ series ], callbackTimed = self.status.updateStatusFiles)

	def prettyPrintCommands(self, commandSeriesList):
		for series in commandSeriesList:
			for pipeline in series:
				commandStrings = []
				for command in pipeline:
					commandStrings.append(" ".join(command))
				pipelineString = " | ".join(commandStrings)
				print "Command to run: ", pipelineString

	# command series list: list of (commands plus args) is one pipeline. list of pipelines = 1 series. 
	# this function wants a list of series.
	# be a list (the command name and the various args)
	# If the shell option is true, all pipelines will be run under the shell.
	# callbackinterval: how often we will call callbackTimed (in milliseconds), defaults to every 5 secs
	def runCommand(self, commandSeriesList, callbackStderr=None, callbackStderrArg=None, callbackTimed=None, callbackTimedArg=None, shell = False, callbackInterval=5000):
		"""Nonzero return code from the shell from any command in any pipeline will cause this
		function to print an error message and return 1, indicating error.
		Returns 0 on success.
		If a callback function is passed, it will receive lines of
		output from the call.  If the callback function takes another argument (which will
		be passed before the line of output) must be specified by the arg paraemeter.
		If no callback is provided, and no output file is specified for a given 
		pipe, the output will be written to stderr. (Do we want that?)
		This function spawns multiple series of pipelines  in parallel.

		"""
		if self.dryrun:
			self.prettyPrintCommands(commandSeriesList)
			return 0

		else:
			commands = CommandsInParallel(commandSeriesList, callbackStderr=callbackStderr, callbackStderrArg=callbackStderrArg, callbackTimed=callbackTimed, callbackTimedArg=callbackTimedArg, shell=shell, callbackInterval=callbackInterval)
			commands.runCommands()
			if commands.exitedSuccessfully():
				return 0
			else:
				problemCommands = commands.commandsWithErrors()
				errorString = "Error from command(s): "
				for cmd in problemCommands: 
					errorString = errorString + "%s " % cmd
				self.logAndPrint(errorString)
				return 1

	def debug(self, stuff):
		self.logAndPrint("%s: %s %s" % (TimeUtils.prettyTime(), self.dbName, stuff))

	def runHandleFailure(self):
		if self.status.failCount < 1:
			# Email the site administrator just once per database
			self.status.reportFailure()
		self.status.failCount += 1

	def runUpdateItemFileInfo(self, item):
		# this will include checkpoint files if they are enabled.
		for fileObj in item.listOutputFilesToPublish(self.dumpDir):
			if exists(self.dumpDir.filenamePublicPath(fileObj)):
				# why would the file not exist? because we changed chunk numbers in the
				# middle of a run, and now we list more files for the next stage than there
				# were for earlier ones
				self.symLinks.saveSymlink(fileObj)
				self.feeds.saveFeed(fileObj)
				self.checksums.checksum(fileObj, self)
				self.symLinks.cleanupSymLinks()
				self.feeds.cleanupFeeds()

	def run(self):
		if (self.jobRequested):
			if ((not self.dumpItemList.oldRunInfoRetrieved) and (self.wiki.existsPerDumpIndex())):

				# There was a previous run of all or part of this date, but...
				# There was no old RunInfo to be had (or an error was encountered getting it)
				# so we can't rerun a step and keep all the status information about the old run around.
				# In this case ask the user if they reeeaaally want to go ahead
				print "No information about the previous run for this date could be retrieved."
				print "This means that the status information about the old run will be lost, and"
				print "only the information about the current (and future) runs will be kept."
				reply = raw_input("Continue anyways? [y/N]: ")
				if (not reply in [ "y", "Y" ]):
					raise RuntimeError( "No run information available for previous dump, exiting" )

			if (not self.dumpItemList.markDumpsToRun(self.jobRequested)):
			# probably no such job
				raise RuntimeError( "No job marked to run, exiting" )
			if (restart):
				# mark all the following jobs to run as well 
				self.dumpItemList.markFollowingJobsToRun()
		else:
			self.dumpItemList.markAllJobsToRun();

		Maintenance.exitIfInMaintenanceMode("In maintenance mode, exiting dump of %s" % self.dbName )

		self.makeDir(os.path.join(self.wiki.publicDir(), self.wiki.date))
		self.makeDir(os.path.join(self.wiki.privateDir(), self.wiki.date))

		self.showRunnerState("Cleaning up old dumps for %s" % self.dbName)
		self.cleanOldDumps()

		# Informing what kind backup work we are about to do
		if (self.jobRequested):
			if (self.restart):
				self.logAndPrint("Preparing for restart from job %s of %s" % (self.jobRequested, self.dbName))
			else:
				self.logAndPrint("Preparing for job %s of %s" % (self.jobRequested, self.dbName))
		else:
			self.showRunnerState("Starting backup of %s" % self.dbName)

		self.checksums.prepareChecksums()
		
		for item in self.dumpItemList.dumpItems:
			Maintenance.exitIfInMaintenanceMode("In maintenance mode, exiting dump of %s at step %s" % ( self.dbName, item.name() ) )
			if (item.toBeRun()):
				item.start(self)
				self.status.updateStatusFiles()
				self.runInfoFile.saveDumpRunInfoFile(self.dumpItemList.reportDumpRunInfo())
				try:
					item.dump(self)
				except Exception, ex:
					exc_type, exc_value, exc_traceback = sys.exc_info()
					if (self.verbose):
						print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
					else:
						self.debug("*** exception! " + str(ex))
					item.setStatus("failed")

			if item.status() == "done":
				self.checksums.cpMd5TmpFileToPermFile()
 				self.runUpdateItemFileInfo(item)
			else:
				# Here for example status is "failed". But maybe also
				# "in-progress", if an item chooses to override dump(...) and
				# forgets to set the status. This is a failure as well.
				self.runHandleFailure()

		if (self.dumpItemList.allPossibleJobsDone()):
			# All jobs are either in status "done" or "failed"
			self.status.updateStatusFiles("done")
		else:
			# This may happen if we start a dump now and abort before all items are
			# done. Then some are left for example in state "waiting". When
			# afterwards running a specific job, all (but one) of the jobs
			# previously in "waiting" are still in status "waiting"
			self.status.updateStatusFiles("partialdone")

		self.runInfoFile.saveDumpRunInfoFile(self.dumpItemList.reportDumpRunInfo())
											
		# if any job succeeds we might as well make the sym link
		if (self.status.failCount < 1):
			self.completeDump()

		if (self.jobRequested):
			# special case...
			if self.jobRequested == "latestlinks":
				if (self.dumpItemList.allPossibleJobsDone()):
					self.symLinks.removeSymLinksFromOldRuns(self.wiki.date)
					self.feeds.cleanupFeeds()

		# Informing about completion
		if (self.jobRequested):
			if (self.restart):
				self.showRunnerState("Completed run restarting from job %s for %s" % (self.jobRequested, self.dbName))
			else:
				self.showRunnerState("Completed job %s for %s" % (self.jobRequested, self.dbName))
		else:
			self.showRunnerStateComplete()

		# let caller know if this was a successful run
		if self.status.failCount > 0:
			return False
		else:
			return True

	def cleanOldDumps(self):
		"""Removes all but the wiki.config.keep last dumps of this wiki.
		If there is already a directory for todays dump, this is omitted in counting and
		not removed."""
		if self._cleanOldDumpsEnabled:
			old = self.wiki.dumpDirs()
			if old:
				if old[-1] == self.wiki.date:
					# If we're re-running today's (or jobs from a given day's) dump, don't count it as one
					# of the old dumps to keep... or delete it halfway through!
					old = old[:-1]
				if self.wiki.config.keep > 0:
					# Keep the last few
					old = old[:-(self.wiki.config.keep)]
			if old:
				for dump in old:
					self.showRunnerState("Purging old dump %s for %s" % (dump, self.dbName))
					base = os.path.join(self.wiki.publicDir(), dump)
					shutil.rmtree("%s" % base)
			else:
				self.showRunnerState("No old dumps to purge.")

	def showRunnerState(self, message):
		self.debug(message)

	def showRunnerStateComplete(self):
		self.debug("SUCCESS: done.")

	def completeDump(self):
		# note that it's possible for links in "latest" to point to 
		# files from different runs, in which case the md5sums file
		# will have accurate checksums for the run for which it was
		# produced, but not the other files. FIXME
		self.checksums.moveMd5FileIntoPlace()
		dumpFile = DumpFilename(self.wiki, None, self.checksums.getChecksumFileNameBasename())
		self.symLinks.saveSymlink(dumpFile)
		self.symLinks.cleanupSymLinks()

		for item in self.dumpItemList.dumpItems:
			if (item.toBeRun()):
				dumpNames = item.listDumpNames()
				if type(dumpNames).__name__!='list':
					dumpNames = [ dumpNames ]

				if (item._chunksEnabled):
					# if there is a specific chunk, we want to only clear out
					# old files for that piece, because new files for the other
					# pieces may not have been generated yet.
					chunk = item._chunkToDo
				else:
					chunk = None

				checkpoint = None
				if (item._checkpointsEnabled):
					if (item.checkpointFile):
						# if there's a specific checkpoint file we are
						# rerunning, we would only clear out old copies
						# of that very file. meh. how likely is it that we 
						# have one? these files are time based and the start/end pageids
						# are going to fluctuate. whatever
						checkpoint = item.checkpointFile.checkpoint

				for d in dumpNames:
					self.symLinks.removeSymLinksFromOldRuns(self.wiki.date, d, chunk, checkpoint )

				self.feeds.cleanupFeeds()

	def makeDir(self, dir):
		if self._makeDirEnabled:
			if exists(dir):
				self.debug("Checkdir dir %s ..." % dir)
			else:
				self.debug("Creating %s ..." % dir)
				os.makedirs(dir)

class SymLinks(object):
	def __init__(self, wiki, dumpDir, logfn, debugfn, enabled):
		self.wiki = wiki
		self.dumpDir = dumpDir
		self._enabled = enabled
		self.logfn = logfn
		self.debugfn = debugfn

	def makeDir(self, dir):
		if (self._enabled):
			if exists(dir):
				self.debugfn("Checkdir dir %s ..." % dir)
			else:
				self.debugfn("Creating %s ..." % dir)
				os.makedirs(dir)

	def saveSymlink(self, dumpFile):
		if (self._enabled):
			self.makeDir(self.dumpDir.latestDir())
			realfile = self.dumpDir.filenamePublicPath(dumpFile)
			latestFilename = dumpFile.newFilename(dumpFile.dumpName, dumpFile.fileType, dumpFile.fileExt, 'latest', dumpFile.chunk, dumpFile.checkpoint, dumpFile.temp)
			link = os.path.join(self.dumpDir.latestDir(), latestFilename)
			if exists(link) or os.path.islink(link):
				if os.path.islink(link):
					oldrealfile = os.readlink(link)
					# format of these links should be...  ../20110228/elwikidb-20110228-templatelinks.sql.gz
					rellinkpattern = re.compile('^\.\./(20[0-9]+)/')
					dateinlink = rellinkpattern.search(oldrealfile)
					if (dateinlink):
						dateoflinkedfile = dateinlink.group(1)
						dateinterval = int(self.wiki.date) - int(dateoflinkedfile)
					else:
						dateinterval = 0
					# no file or it's older than ours... *then* remove the link
					if not exists(os.path.realpath(link)) or dateinterval > 0:
						self.debugfn("Removing old symlink %s" % link)
						os.remove(link)
				else:
					self.logfn("What the hell dude, %s is not a symlink" % link)
					raise BackupError("What the hell dude, %s is not a symlink" % link)
			relative = FileUtils.relativePath(realfile, os.path.dirname(link))
			# if we removed the link cause it's obsolete, make the new one
			if exists(realfile) and not exists(link):
				self.debugfn("Adding symlink %s -> %s" % (link, relative))
				os.symlink(relative, link)
			
	def cleanupSymLinks(self):
		if (self._enabled):
			latestDir = self.dumpDir.latestDir()
			files = os.listdir(latestDir)
			for f in files:
				link = os.path.join(latestDir,f)
				if os.path.islink(link):
					realfile = os.readlink(link)
					if not exists(os.path.join(latestDir,realfile)):
						os.remove(link)

	# if the args are False or None, we remove all the old links for all values of the arg.
	# example: if chunk is False or None then we remove all old values for all chunks
	# "old" means "older than the specified datestring".
	def removeSymLinksFromOldRuns(self, dateString, dumpName=None, chunk=None, checkpoint=None):
		# fixme this needs to do more work if there are chunks or checkpoint files linked in here from 
		# earlier dates. checkpoint ranges change, and configuration of chunks changes too, so maybe
		# old files still exist and the links need to be removed because we have newer files for the
		# same phase of the dump.

		if (self._enabled):
			latestDir = self.dumpDir.latestDir()
			files = os.listdir(latestDir)
			for f in files:
				link = os.path.join(latestDir,f)
				if os.path.islink(link):
					realfile = os.readlink(link)
					fileObj = DumpFilename(self.dumpDir._wiki)
					fileObj.newFromFilename(os.path.basename(realfile))
					if fileObj.date < dateString:
						# fixme check that these are ok if the value is None
						if dumpName and (fileObj.dumpName != dumpName):
							continue
						if chunk and (fileObj.chunk != chunk):
							continue
						if checkpoint and (fileObj.checkpoint != checkpoint):
							continue
						self.debugfn("Removing old symlink %s -> %s" % (link, realfile))
						os.remove(link)

class Feeds(object):
	def __init__(self, wiki, dumpDir, dbName, debugfn, enabled):
		self.wiki = wiki
		self.dumpDir = dumpDir
		self.dbName = dbName
		self.debugfn = debugfn
		self._enabled = enabled

	def makeDir(self, dirname):
		if (self._enabled):
			if exists(dirname):
				self.debugfn("Checkdir dir %s ..." % dirname)
			else:
				self.debugfn("Creating %s ..." % dirname)
				os.makedirs(dirname)

	def saveFeed(self, fileObj):
		if (self._enabled):
			self.makeDir(self.dumpDir.latestDir())
			filenameAndPath = self.dumpDir.webPath(fileObj)
			webPath = os.path.dirname(filenameAndPath)
			rssText = self.wiki.config.readTemplate("feed.xml") % {
				"chantitle": fileObj.basename,
				"chanlink": webPath,
				"chandesc": "Wikimedia dump updates for %s" % self.dbName,
				"title": webPath,
				"link": webPath,
				"description": xmlEscape("<a href=\"%s\">%s</a>" % (filenameAndPath, fileObj.filename)),
				"date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()) }
			directory = self.dumpDir.latestDir()
			rssPath = os.path.join(self.dumpDir.latestDir(), self.dbName + "-latest-" + fileObj.basename + "-rss.xml")
			self.debugfn( "adding rss feed file %s " % rssPath )
			FileUtils.writeFile(self.wiki.config.tempDir, rssPath, rssText, self.wiki.config.fileperms)

	def cleanupFeeds(self):
		# call this after sym links in this dir have been cleaned up.
		# we should probably fix this so there is no such dependency, 
		# but it would mean parsing the contents of the rss file, bleah
		if (self._enabled):
			latestDir = self.dumpDir.latestDir()
			files = os.listdir(latestDir)
			for f in files:
				if f.endswith("-rss.xml"):
					filename = f[:-8]
					link = os.path.join(latestDir,filename)
					if not exists(link):
						self.debugfn("Removing old rss feed %s for link %s" % (os.path.join(latestDir,f), link))
						os.remove(os.path.join(latestDir,f))

class Dump(object):
	def __init__(self, name, desc, verbose = False):
		self._desc = desc
		self.verbose = verbose
		self.progress = ""
		self.runInfo = RunInfo(name,"waiting","")
		self.dumpName = self.getDumpName()
		self.fileType = self.getFileType()
		self.fileExt = self.getFileExt()
		# if var hasn't been defined by a derived class already.  (We get
		# called last by child classes in their constructor, so that
		# their functions overriding things like the dumpbName can 
		# be set up before we use them to set class attributes.)
		if not hasattr(self, '_chunksEnabled'):
			self._chunksEnabled = False
		if not hasattr(self, '_checkpointsEnabled'):
			self._checkpointsEnabled = False
		if not hasattr(self, 'checkpointFile'):
			self.checkpointFile = False
		if not hasattr(self, '_chunkToDo'):
			self._chunkToDo = False
		if not hasattr(self, '_prerequisiteItems'):
			self._prerequisiteItems = []
		if not hasattr(self, '_checkTruncation'):
			# Automatic checking for truncation of produced files is
			# (due to dumpDir handling) only possible for public dir 
			# right now. So only set this to True, when all files of
			# the item end in the public dir.
			self._checkTruncation = False

	def name(self):
		return self.runInfo.name()

	def status(self):
		return self.runInfo.status()

	def updated(self):
		return self.runInfo.updated()

	def toBeRun(self):
		return self.runInfo.toBeRun()

	def setName(self,name):
		self.runInfo.setName(name)

	def setToBeRun(self,toBeRun):
		self.runInfo.setToBeRun(toBeRun)
				      
	# sometimes this will be called to fill in data from an old
	# dump run; in those cases we don't want to clobber the timestamp
	# with the current time.
	def setStatus(self,status,setUpdated = True):
		self.runInfo.setStatus(status)
		if (setUpdated):
			self.runInfo.setUpdated(TimeUtils.prettyTime())

	def setUpdated(self, updated):
		self.runInfo.setUpdated(updated)

	def description(self):
		return self._desc

	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return None

	def getDumpName(self):
		"""Return the dumpName as it appears in output files for this phase of the dump
		e.g. pages-meta-history, all-titles-in-ns0, etc"""
		return ""

	def listDumpNames(self):
		"""Returns a list of names as they appear in output files for this phase of the dump
		e.g. [ pages-meta-history ], or [ stub-meta-history, stub-meta-current, stub-articles], etc"""
		return [ self.getDumpName() ]

	def getFileExt(self):
		"""Return the extension of output files for this phase of the dump
		e.g. bz2 7z etc"""
		return ""

	def getFileType(self):
		"""Return the type of output files for this phase of the dump
		e.g. sql xml etc"""
		return ""

	def start(self, runner):
		"""Set the 'in progress' flag so we can output status."""
		self.setStatus("in-progress")
				      
	def dump(self, runner):
		"""Attempt to run the operation, updating progress/status info."""
		try:
			for prerequisiteItem in self._prerequisiteItems:
				if prerequisiteItem.status() != "done":
					raise BackupError("Required job %s not marked as done, not starting job %s" % ( prerequisiteItem.name(),self.name() ) )

			self.run(runner)
			self.postRun(runner)
		except Exception:
			if (self.verbose):
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
			self.setStatus("failed")
			raise
		self.setStatus("done")

	def run(self, runner):
		"""Actually do something!"""
		pass

	def postRun(self, runner):
		"""Common tasks to run after performing this item's actual dump"""
		# Checking for truncated files
		truncatedFilesCount = self.checkForTruncatedFiles(runner)
		if truncatedFilesCount:
			raise BackupError("Encountered %d truncated files for %s" % ( truncatedFilesCount, self.dumpName ) )
	
	def checkForTruncatedFiles(self, runner):
		"""Returns the number of files that have been detected to be truncated. This function expects that all files to check for truncation live in the public dir"""
		ret = 0

		if not runner._checkForTruncatedFilesEnabled or not self._checkTruncation:
			return ret

		# dfn is the DumpFilename
		# df  is the DumpFile
		for dfn in self.listOutputFilesToCheckForTruncation(runner.dumpDir):
			df = DumpFile(runner.wiki, runner.dumpDir.filenamePublicPath(dfn), dfn );

			fileTruncated=True;				
			if exists(df.filename):
				if df.checkIfTruncated():
					# The file exists and is truncated, we move it out of the way
					df.rename( df.filename + ".truncated" )
					
					# We detected a failure and could abort right now. However,
					# there might still be some further chunk files, that are good.
					# Hence, we go on treating the remaining files and in the end
					# /all/ truncated files have been moved out of the way. So we
					# see, which chunks (instead of the whole job) need a rerun.
				else:
					# The file exists and is not truncated. Heck, it's a good file!
					fileTruncated=False
					
			if fileTruncated:
				ret+=1

		return ret

	def progressCallback(self, runner, line=""):
		"""Receive a status line from a shellout and update the status files."""
		# pass through...
		if (line):
			if (runner.log):
				runner.log.addToLogQueue(line)
			sys.stderr.write(line)
		self.progress = line.strip()
		runner.status.updateStatusFiles()
		runner.runInfoFile.saveDumpRunInfoFile(runner.dumpItemList.reportDumpRunInfo())

	def timeToWait(self):
		# we use wait this many secs for a command to complete that
		# doesn't produce output
		return 5

	def waitAlarmHandler(self, signum, frame):
		pass

	def buildRecombineCommandString(self, runner, files, outputFile, compressionCommand, uncompressionCommand, endHeaderMarker="</siteinfo>"):
                outputFilename = runner.dumpDir.filenamePublicPath(outputFile)
		chunkNum = 0
		recombines = []
		if (not exists( runner.wiki.config.head ) ):
			raise BackupError("head command %s not found" % runner.wiki.config.head)
		head = runner.wiki.config.head
		if (not exists( runner.wiki.config.tail ) ):
			raise BackupError("tail command %s not found" % runner.wiki.config.tail)
		tail = runner.wiki.config.tail
		if (not exists( runner.wiki.config.grep ) ):
			raise BackupError("grep command %s not found" % runner.wiki.config.grep)
		grep = runner.wiki.config.grep

		# we assume the result is always going to be run in a subshell. 
		# much quicker than this script trying to read output
		# and pass it to a subprocess
		outputFilenameEsc = MiscUtils.shellEscape(outputFilename)
		headEsc = MiscUtils.shellEscape(head)
		tailEsc = MiscUtils.shellEscape(tail)
		grepEsc = MiscUtils.shellEscape(grep)

		uncompressionCommandEsc = uncompressionCommand[:]
		for u in uncompressionCommandEsc:
			u = MiscUtils.shellEscape(u)
		for u in compressionCommand:
			u = MiscUtils.shellEscape(u)

		if not files:
			raise BackupError("No files for the recombine step found in %s." % self.name())

		for fileObj in files:
			# uh oh FIXME
#			f = MiscUtils.shellEscape(fileObj.filename)
			f = runner.dumpDir.filenamePublicPath(fileObj)
			chunkNum = chunkNum + 1
			pipeline = []
			uncompressThisFile = uncompressionCommand[:]
			uncompressThisFile.append(f)
			pipeline.append(uncompressThisFile)
			# warning: we figure any header (<siteinfo>...</siteinfo>) is going to be less than 2000 lines!
			pipeline.append([ head, "-2000"])
			pipeline.append([ grep, "-n", endHeaderMarker ])
			# without shell
			p = CommandPipeline(pipeline, quiet=True)
			p.runPipelineAndGetOutput()
			if (p.output()) and (p.exitedSuccessfully() or p.getFailedCommandsWithExitValue() == [[ -signal.SIGPIPE, uncompressThisFile ]] or p.getFailedCommandsWithExitValue() == [[ signal.SIGPIPE + 128, uncompressThisFile ]]):
				(headerEndNum, junk) = p.output().split(":",1)
				# get headerEndNum
			else:
				raise BackupError( "Could not find 'end of header' marker for %s" % f )
			recombine = " ".join(uncompressThisFile)
			headerEndNum = int(headerEndNum) + 1
			if (chunkNum == 1):
				# first file, put header and contents
				recombine = recombine + " | %s -n -1 " % headEsc
			elif (chunkNum == len(files)):
				# last file, put footer
				recombine = recombine + (" | %s -n +%s" % (tailEsc, headerEndNum))
			else:
				# put contents only
				recombine = recombine + (" | %s -n +%s" % (tailEsc, headerEndNum))
				recombine = recombine + " | %s -n -1 " % head
			recombines.append(recombine)
		recombineCommandString = "(" + ";".join(recombines) + ")" + "|" + "%s %s" % (compressionCommand, outputFilename)
		return(recombineCommandString)

	def cleanupOldFiles(self, dumpDir, chunks = False):
		if (runner._cleanupOldFilesEnabled):
			if (self.checkpointFile):
				# we only rerun this one, so just remove this one
				if exists(dumpDir.filenamePublicPath(self.checkpointFile)):
					os.remove(dumpDir.filenamePublicPath(self.checkpointFile))
			files = self.listOutputFilesForCleanup(dumpDir)
			for f in files:
				if exists(dumpDir.filenamePublicPath(f)):
					os.remove(dumpDir.filenamePublicPath(f))

	def getChunkList(self):
		if self._chunksEnabled:
			if self._chunkToDo:
				return [ self._chunkToDo ]
			else:
				return range(1, len(self._chunks)+1)
		else:
			return False

	# list all regular output files that exist
	def listRegularFilesExisting(self, dumpDir, dumpNames = None, date = None, chunks = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend( dumpDir.getRegularFilesExisting(date, d, self.fileType, self.fileExt, chunks, temp = False))
		return files

	# list all checkpoint files that exist
	def listCheckpointFilesExisting(self, dumpDir, dumpNames = None, date = None, chunks = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend(dumpDir.getCheckpointFilesExisting(date, d, self.fileType, self.fileExt, chunks, temp = False))
		return files

	# unused
	# list all temp output files that exist
	def listTempFilesExisting(self, dumpDir, dumpNames = None, date = None, chunks = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend( dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks = None, temp = True) )
			files.extend( dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks = None, temp = True) )
		return files

	# list checkpoint files that have been produced for specified chunk(s)
	def listCheckpointFilesPerChunkExisting(self, dumpDir, chunks, dumpNames = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend(dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp = False))
		return files

	# list noncheckpoint files that have been produced for specified chunk(s)
	def listRegularFilesPerChunkExisting(self, dumpDir, chunks, dumpNames = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend( dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp = False))
		return files

	# list temp output files that have been produced for specified chunk(s)
	def listTempFilesPerChunkExisting(self, dumpDir, chunks, dumpNames = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend( runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp = True) )
			files.extend( runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp = True) )
		return files


	# unused
	# list noncheckpoint chunk files that have been produced
	def listRegularFilesChunkedExisting(self, dumpDir, dumpNames = None, date = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks= self.getChunkList(), temp = False))
		return files

	# unused
	# list temp output chunk files that have been produced
	def listTempFilesChunkedExisting(self, runner, dumpNames = None):
		files = []
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		for d in dumpNames:
			files.extend( runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks = self.getChunkList(), temp = True) )
			files.extend( runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks = self.getChunkList(), temp = True) )
		return files

	# unused
	# list checkpoint files that have been produced for chunkless run
	def listCheckpointFilesChunklessExisting(self, runner, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks = False, temp = False))
		return files

	# unused
	# list non checkpoint files that have been produced for chunkless run
	def listRegularFilesChunklessExisting(self, runner, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks = False, temp = False))
		return files

	# unused
	# list non checkpoint files that have been produced for chunkless run
	def listTempFilesChunklessExisting(self, runner, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks = False, temp = True))
			files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks = False, temp = True))
		return files


	# internal function which all the public get*Possible functions call
	# list all files that could be created for the given dumpName, filtering by the given args. 
	# by definition, checkpoint files are never returned in such a list, as we don't
	# know where a checkpoint might be taken (which pageId start/end).
	#
	# if we get None for an arg then we accept all values for that arg in the filename
	# if we get False for an arg (chunk, temp), we reject any filename which contains a value for that arg
	# if we get True for an arg (temp), we accept only filenames which contain a value for the arg
	# chunks should be a list of value(s), or True / False / None
	def _getFilesPossible(self, dumpDir, date = None, dumpName = None, fileType = None, fileExt = None, chunks = None, temp = False ):
		files = []
		if dumpName == None:
			dumpname = self.dumpName
		if chunks == None or chunks == False:
			files.append(DumpFilename(dumpDir._wiki, date, dumpName, fileType, fileExt, None, None, temp))
		if chunks == True or chunks == None:
			chunks = self.getChunksList()
		if chunks:
			for i in chunks:
				files.append(DumpFilename(dumpDir._wiki, date, dumpName, fileType, fileExt, i, None, temp))
		return files

	# unused
	# based on dump name, get all the output files we expect to generate except for temp files
	def getRegularFilesPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend( self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = None, temp = False) )
		return files

	# unused
	# based on dump name, get all the temp output files we expect to generate
	def getTempFilesPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend( self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = None, temp = True ) )
		return files

	# based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
	def getRegularFilesPerChunkPossible(self, dumpDir, chunks, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend( self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks, temp = False) )
		return files

	# unused
	# based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
	def getTempFilesPerChunkPossible(self, dumpDir, chunks, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks, temp = True))
		return files


	# unused
	# based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
	def getRegularFilesChunkedPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend( self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = True, temp = False) )
		return files

	# unused
	# based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
	def getTempFilesPerChunkedPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = True, temp = True))
		return files

	# unused
	# list noncheckpoint files that should be produced for chunkless run
	def getRegularFilesChunklessPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = False, temp = False))
		return files

	# unused
	# list temp output files that should be produced for chunkless run
	def getTempFilesChunklessPossible(self, dumpDir, dumpNames = None):
		if not dumpNames:
			dumpNames = [ self.dumpName ]
		files = []
		for d in dumpNames:
			files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks = False, temp = True))
		return files

################################
#
# these routines are all used for listing output files for various purposes...
#
#
	# Used for updating md5 lists, index.html
	# Includes: checkpoints, chunks, chunkless, temp files if they exist. At end of run temp files must be gone.
	# This is *all* output files for the dumpName, regardless of what's being re-run. 
	def listOutputFilesToPublish(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self.checkpointFile):
			files.append(self.checkpointFile)
			return files

		if (self._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
			files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
		        # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
		return files

	# called at end of job run to see if results are intact or are garbage and must be tossed/rerun.
	# Includes: checkpoints, chunks, chunkless.  Not included: temp files. 
	# This is only the files that should be produced from this run. So it is limited to a specific
	# chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
	# files if there are no chunks enabled.
	def listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self.checkpointFile):
			files.append(self.checkpointFile)
			return files

		if (self._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
		return files

	# called when putting together commands to produce output for the job.
	# Includes: chunks, chunkless, temp files.   Not included: checkpoint files.
	# This is only the files that should be produced from this run. So it is limited to a specific
	# chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
	# files if there are no chunks enabled.
	def listOutputFilesForBuildCommand(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self.checkpointFile):
			files.append(self.checkpointFile)
			return files

		if (self._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
		        # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
		return files

	# called before job run to cleanup old files left around from any previous run(s)
	# Includes: checkpoints, chunks, chunkless, temp files if they exist.
	# This is only the files that should be produced from this run. So it is limited to a specific
	# chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
	# files if there are no chunks enabled.
	def listOutputFilesForCleanup(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self.checkpointFile):
			files.append(self.checkpointFile)
			return files

 		if (self._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
			files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		return files

	# used to generate list of input files for other phase (e.g. recombine, recompress)
	# Includes: checkpoints, chunks/chunkless files depending on whether chunks are enabled. Not included: temp files.
	# This is *all* output files for the job, regardless of what's being re-run. The caller can sort out which
	# files go to which chunk, in case input is needed on a per chunk basis. (Is that going to be annoying? Nah,
	# and we only do it once per job so who cares.)
	def listOutputFilesForInput(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self._checkpointsEnabled):
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
			files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		return files

class PublicTable(Dump):
	"""Dump of a table using MySQL's mysqldump utility."""
				      
	def __init__(self, table, name, desc):
		self._table = table
		self._chunksEnabled = False
		Dump.__init__(self, name, desc)

	def getDumpName(self):
		return(self._table)

	def getFileType(self):
		return "sql"

	def getFileExt(self):
		return "gz"

	def run(self, runner):
		retries = 0
		# try this initially and see how it goes
		maxretries = 3 
		files = self.listOutputFilesForBuildCommand(runner.dumpDir)
		if (len(files) > 1):
			raise BackupError("table dump %s trying to produce more than one file" % self.dumpName)
		outputFile = files[0]
		error = self.saveTable(self._table, runner.dumpDir.filenamePublicPath(outputFile), runner)
		while (error and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			error = self.saveTable(self._table, runner.dumpDir.filenamePublicPath(outputFile), runner)
		if (error):
			raise BackupError("error dumping table %s" % self._table)

	# returns 0 on success, 1 on error
	def saveTable(self, table, outfile, runner):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		if (not exists( runner.wiki.config.gzip ) ):
			raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
		commands = runner.dbServerInfo.buildSqlDumpCommand(table, runner.wiki.config.gzip)
		return runner.saveCommand(commands, outfile)

class PrivateTable(PublicTable):
	"""Hidden table dumps for private data."""

	def __init__(self, table, name, desc):
		# Truncation checks require output to public dir, hence we
		# cannot use them. The default would be 'False' anyways, but
		# if that default changes, we still cannot use automatic
		# truncation checks.
		self._checkTruncation = False
		PublicTable.__init__(self, table, name, desc)

	def description(self):
		return self._desc + " (private)"

	def run(self, runner):
		retries = 0
		# try this initially and see how it goes
		maxretries = 3 
		files = self.listOutputFilesForBuildCommand(runner.dumpDir)
		if (len(files) > 1):
			raise BackupError("table dump %s trying to produce more than one file" % self.dumpName)
		outputFile = files[0]
		error = self.saveTable(self._table, runner.dumpDir.filenamePrivatePath(outputFile), runner)
		while (error and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			error = self.saveTable(self._table, runner.dumpDir.filenamePrivatePath(outputFile), runner)
		if (error):
			raise BackupError("error dumping table %s" % self._table)

	def listOutputFilesToPublish(self, dumpDir):
		"""Private table won't have public files to list."""
		return []

class XmlStub(Dump):
	"""Create lightweight skeleton dumps, minus bulk text.
	A second pass will import text from prior dumps or the database to make
	full files for the public."""
				      
	def __init__(self, name, desc, chunkToDo, chunks = False, checkpoints = False):
		self._chunkToDo = chunkToDo
		self._chunks = chunks
		if self._chunks:
			self._chunksEnabled = True
		self.historyDumpName = "stub-meta-history"
		self.currentDumpName = "stub-meta-current"
		self.articlesDumpName = "stub-articles"
		if checkpoints:
			self._checkpointsEnabled = True
		self._checkTruncation = True
		Dump.__init__(self, name, desc)

	def detail(self):
		return "These files contain no page text, only revision metadata."

	def getFileType(self):
		return "xml"

	def getFileExt(self):
		return "gz"

	def getDumpName(self):
		return 'stub'

	def listDumpNames(self):
		dumpNames =  [ self.historyDumpName, self.currentDumpName, self.articlesDumpName ]
		return dumpNames

	def listOutputFilesToPublish(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
		return files

	def listOutputFilesToCheckForTruncation(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForBuildCommand(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForBuildCommand(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForCleanup(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForInput(self, dumpDir, dumpNames = None):
		if dumpNames == None:
			dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForInput(self, dumpDir, dumpNames))
		return files

	def buildCommand(self, runner, f):
		if (not exists( runner.wiki.config.php ) ):
			raise BackupError("php command %s not found" % runner.wiki.config.php)

		articlesFile = runner.dumpDir.filenamePublicPath(f)
		historyFile = runner.dumpDir.filenamePublicPath(DumpFilename(runner.wiki, f.date, self.historyDumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint, f.temp))
		currentFile = runner.dumpDir.filenamePublicPath(DumpFilename(runner.wiki, f.date, self.currentDumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint, f.temp))
		scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpBackup.php")
		command = [ "%s" % runner.wiki.config.php, "-q" ]
		command.extend(scriptCommand)
		command.extend(["--wiki=%s" % runner.dbName,
				"--full", "--stub", "--report=10000",
				"%s" % runner.forceNormalOption(),
				"--output=gzip:%s" % historyFile,
				"--output=gzip:%s" % currentFile,
				"--filter=latest", "--output=gzip:%s" % articlesFile,
				"--filter=latest", "--filter=notalk", "--filter=namespace:!NS_USER"
				])

		if (f.chunk):
			# set up start end end pageids for this piece
			# note there is no page id 0 I guess. so we start with 1
			# start = runner.pagesPerChunk()*(chunk-1) + 1
			start = sum([ self._chunks[i] for i in range(0,f.chunkInt-1)]) + 1
			startopt = "--start=%s" % start
			# if we are on the last chunk, we should get up to the last pageid, 
			# whatever that is. 
			command.append(startopt)
			if f.chunkInt < len(self._chunks):
				end = sum([ self._chunks[i] for i in range(0,f.chunkInt)]) +1
				endopt = "--end=%s" % end
				command.append(endopt)

		pipeline = [ command ]
		series = [ pipeline ]
		return(series)

	def run(self, runner):
		commands = []
		self.cleanupOldFiles(runner.dumpDir)
		files = self.listOutputFilesForBuildCommand(runner.dumpDir)
		for f in files:
			# choose arbitrarily one of the dumpNames we do (= articlesDumpName)
			# buildcommand will figure out the files for the rest
			if (f.dumpName == self.articlesDumpName):
				series = self.buildCommand(runner, f)
				commands.append(series)
		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if (error):
			raise BackupError("error producing stub files" % self._subset)

class RecombineXmlStub(Dump):
	def __init__(self, name, desc, itemForXmlStubs):
		self.itemForXmlStubs = itemForXmlStubs
		self._prerequisiteItems = [ self.itemForXmlStubs ]
		Dump.__init__(self, name, desc)
		# the input may have checkpoints but the output will not.
		self._checkpointsEnabled = False

	def detail(self):
		return "These files contain no page text, only revision metadata."

	def listDumpNames(self):
		return self.itemForXmlStubs.listDumpNames()

	def listOutputFilesToPublish(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
		return files

	def listOutputFilesToCheckForTruncation(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
		return files

	def getFileType(self):
		return self.itemForXmlStubs.getFileType()

	def getFileExt(self):
		return self.itemForXmlStubs.getFileExt()

	def getDumpName(self):
		return self.itemForXmlStubs.getDumpName()

	def run(self, runner):
		error=0
		files = self.itemForXmlStubs.listOutputFilesForInput(runner.dumpDir)
		outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir, self.listDumpNames())
		for outputFileObj in outputFileList:
			inputFiles = []
			for inFile in files:
				if inFile.dumpName == outputFileObj.dumpName:
					inputFiles.append(inFile)
			if not len(inputFiles):
				self.setStatus("failed")
				raise BackupError("No input files for %s found" % self.name())
			if (not exists( runner.wiki.config.gzip ) ):
				raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
			compressionCommand = runner.wiki.config.gzip
			compressionCommand = "%s > " % runner.wiki.config.gzip
			uncompressionCommand = [ "%s" % runner.wiki.config.gzip, "-dc" ] 
			recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFileObj, compressionCommand, uncompressionCommand )
			recombineCommand = [ recombineCommandString ]
			recombinePipeline = [ recombineCommand ]
			series = [ recombinePipeline ]
			result = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
			if result:
				error = result
		if (error):
			raise BackupError("error recombining stub files")

class XmlLogging(Dump):
	""" Create a logging dump of all page activity """

	def __init__(self, desc, chunks = False):
		Dump.__init__(self, "xmlpagelogsdump", desc)

	def detail(self):
		return "This contains the log of actions performed on pages and users."

	def getDumpName(self):
		return("pages-logging")

	def getFileType(self):
		return "xml"

	def getFileExt(self):
		return "gz"

	def getMaxLogID(self, runner):
		dbServerInfo = DbServerInfo(runner.wiki, runner.dbName)
		query = "select MAX(log_id) from logging;"
		results = None
		retries = 0
		maxretries = 5
		results = dbServerInfo.runSqlAndGetOutput(query)
		while (results == None and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			results = dbServerInfo.runSqlAndGetOutput(query)
		if (not results):
			return None
		lines = results.splitlines()
		if (lines and lines[1]):
			return int(lines[1])
		else:
			return None

	def getTempFilename(self, name, number):
		return name + "-" + str(number)

	def run(self, runner):
		self.cleanupOldFiles(runner.dumpDir)
		files = self.listOutputFilesForBuildCommand(runner.dumpDir)
		if (len(files) > 1):
			raise BackupError("logging table job wants to produce more than one output file")
		outputFileObj = files[0]
		if (not exists( runner.wiki.config.php ) ):
			raise BackupError("php command %s not found" % runner.wiki.config.php)
		scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpBackup.php")

		# do logging table in batches to avoid taking days to dump (wikidata for example)
		maxLogId = self.getMaxLogID(runner)
		if not maxLogId:
			raise BackupError("error retrieving max id from logging table")

		batchsize = runner.wiki.config.loggingBatchsize
		if batchsize:
			startId = 0
			tempFiles = []
			tempFileObjs = []
			while startId < maxLogId:
				endId = startId + batchsize
				fileObjThisBatch = DumpFilename(runner.wiki, outputFileObj.date, self.getTempFilename(outputFileObj.dumpName,startId), outputFileObj.fileType, outputFileObj.fileExt)
				tempFileObjs.append(fileObjThisBatch)
				logging = runner.dumpDir.filenamePublicPath(fileObjThisBatch)
				tempFiles.append(logging)
				command = [ "%s" % runner.wiki.config.php, "-q" ]
				command.extend(scriptCommand)
				command.extend(["--wiki=%s" % runner.dbName,
						"--logs", "--report=10000",
						"%s" % runner.forceNormalOption(),
						"--start=%s" % startId,
						"--end=%s" % endId,
						"--output=gzip:%s" % logging ])
				pipeline = [ command ]
				series = [ pipeline ]
				error = runner.runCommand([ series ], callbackStderr=self.progressCallback,
						  callbackStderrArg=runner)
				if (error):
					raise BackupError("error dumping log files")
				startId = endId
			# recombine these now
			if (not exists( runner.wiki.config.gzip ) ):
				raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
			compressionCommand = runner.wiki.config.gzip
			compressionCommand = "%s > " % runner.wiki.config.gzip
			uncompressionCommand = [ "%s" % runner.wiki.config.gzip, "-dc" ]
			recombineCommandString = self.buildRecombineCommandString(runner, tempFileObjs, outputFileObj, compressionCommand, uncompressionCommand )
			recombineCommand = [ recombineCommandString ]
			recombinePipeline = [ recombineCommand ]
			series = [ recombinePipeline ]
			result = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
			if result:
				error = result
			if (error):
				raise BackupError("error recombining pages-logging files")
			# clean up those intermediate files now
			for f in tempFiles:
				os.remove(f)
		else:
			logging = runner.dumpDir.filenamePublicPath(outputFileObj)
			command = [ "%s" % runner.wiki.config.php, "-q" ]
			command.extend(scriptCommand)
			command.extend(["--wiki=%s" % runner.dbName,
					"--logs", "--report=10000",
					"%s" % runner.forceNormalOption(),
					"--output=gzip:%s" % logging ])
			pipeline = [ command ]
			series = [ pipeline ]
			error = runner.runCommand([ series ], callbackStderr=self.progressCallback, callbackStderrArg=runner)
			if (error):
				raise BackupError("error dmping log files")

class XmlDump(Dump):
	"""Primary XML dumps, one section at a time."""
	def __init__(self, subset, name, desc, detail, itemForStubs,  prefetch, spawn, wiki, chunkToDo, chunks = False, checkpoints = False, checkpointFile = None, pageIDRange = None, verbose = False):
		self._subset = subset
		self._detail = detail
		self._desc = desc
		self._prefetch = prefetch
		self._spawn = spawn
		self._chunks = chunks
		if self._chunks:
			self._chunksEnabled = True
		self._pageID = {}
		self._chunkToDo = chunkToDo

		self.wiki = wiki
		self.itemForStubs = itemForStubs
		if checkpoints:
			self._checkpointsEnabled = True
		self.checkpointFile = checkpointFile
		if self.checkpointFile:
			# we don't checkpoint the checkpoint file.
			self._checkpointsEnabled = False
		self.pageIDRange = pageIDRange
		self._prerequisiteItems = [ self.itemForStubs ]
		self._checkTruncation = True
		Dump.__init__(self, name, desc)

	def getDumpNameBase(self):
		return('pages-')

	def getDumpName(self):
		return(self.getDumpNameBase() + self._subset)

	def getFileType(self):
		return "xml"

	def getFileExt(self):
		return "bz2"

	def run(self, runner):
		commands = []
		self.cleanupOldFiles(runner.dumpDir)
		# just get the files pertaining to our dumpName, which is *one* of articles, pages-current, pages-history.
		# stubs include all of them together.
		if not self.dumpName.startswith(self.getDumpNameBase()):
			raise BackupError("dumpName %s of unknown form for this job" % self.dumpName)
		dumpName = self.dumpName[len(self.getDumpNameBase()):]
		stubDumpNames = self.itemForStubs.listDumpNames()
		for s in stubDumpNames:
			if s.endswith(dumpName):
				stubDumpName = s
		inputFiles = self.itemForStubs.listOutputFilesForInput(runner.dumpDir, [ stubDumpName ])
		if self._chunksEnabled and self._chunkToDo:
			# reset inputfiles to just have the one we want. 
			for f in inputFiles:
				if f.chunkInt == self._chunkToDo:
					inputFiles = [ f ]
					break
			if len(inputFiles) > 1:
				raise BackupError("Trouble finding stub files for xml dump run")

		if self.checkpointFile:
			# fixme this should be an input file, not the output checkpoint file. move
			# the code out of buildCommand that does the conversion and put it here.
			series = self.buildCommand(runner, self.checkpointFile)
			commands.append(series)
		else:
			for f in inputFiles:
				outputFile = DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt)
				series = self.buildCommand(runner, f)
				commands.append(series)

		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if error:
			raise BackupError("error producing xml file(s) %s" % self.dumpName)

	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--current"

	# takes name of the output file
	def buildFilters(self, runner, f):
		"""Construct the output filter options for dumpTextPass.php"""
		# do we need checkpoints? ummm
		xmlbz2 = runner.dumpDir.filenamePublicPath(f)

		if (not exists( self.wiki.config.bzip2 ) ):
			raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
		if self.wiki.config.bzip2[-6:] == "dbzip2":
			bz2mode = "dbzip2"
		else:
			bz2mode = "bzip2"
		return "--output=%s:%s" % (bz2mode, xmlbz2)

	def writePartialStub(self, inputFile, outputFile, startPageID, endPageID):
		if (not exists( self.wiki.config.writeuptopageid ) ):
			raise BackupError("writeuptopageid command %s not found" % self.wiki.config.writeuptopageid)
		writeuptopageid = self.wiki.config.writeuptopageid

		inputFilePath = runner.dumpDir.filenamePublicPath(inputFile)
		outputFilePath = os.path.join(self.wiki.config.tempDir,outputFile.filename)
		if inputFile.fileExt == "gz":
			command1 =  "%s -dc %s" % (self.wiki.config.gzip, inputFilePath )
			command2 = "%s > %s" % (self.wiki.config.gzip, outputFilePath )
		elif inputFile.fileExt == '7z':
			command1 =  "%s e -si %s" % (self.wiki.config.sevenzip, inputFilePath )
			command2 =  "%s e -so %s" % (self.wiki.config.sevenzip, outputFilePath )
		elif inputFile.fileExt == 'bz':
			command1 =  "%s -dc %s" % (self.wiki.config.bzip2, inputFilePath )
			command2 =  "%s > %s" % (self.wiki.config.bzip2, outputFilePath ) 
		else:
			raise BackupError("unknown stub file extension %s" % inputFile.fileExt)
		if (endPageID):
			command = [ command1 + ( "| %s %s %s |" % (self.wiki.config.writeuptopageid, startPageID, endPageID) ) + command2 ]
		else:
			# no lastpageid? read up to eof of the specific stub file that's used for input
			command = [ command1 + ( "| %s %s |" % (self.wiki.config.writeuptopageid, startPageID) ) + command2 ]

		pipeline = [ command ]
		series = [ pipeline ]
		error = runner.runCommand([ series ], shell = True)
		if (error):
			raise BackupError("failed to write partial stub file %s" % outputFile.filename)

	def buildCommand(self, runner, f):
		"""Build the command line for the dump, minus output and filter options"""

		if (self.checkpointFile):
			outputFile = f
		elif (self._checkpointsEnabled):
			# we write a temp file, it will be checkpointed every so often.
			outputFile = DumpFilename(self.wiki, f.date, self.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, temp = True)
		else:
			# we write regular files
			outputFile = DumpFilename(self.wiki, f.date, self.dumpName, f.fileType, self.fileExt, f.chunk, checkpoint = False, temp = False)

		# Page and revision data pulled from this skeleton dump...
		# FIXME we need the stream wrappers for proper use of writeupto. this is a hack.
		if (self.checkpointFile or self.pageIDRange):
			# fixme I now have this code in a couple places, make it a function.
			if not self.dumpName.startswith(self.getDumpNameBase()):
				raise BackupError("dumpName %s of unknown form for this job" % self.dumpName)
			dumpName = self.dumpName[len(self.getDumpNameBase()):]
			stubDumpNames = self.itemForStubs.listDumpNames()
			for s in stubDumpNames:
				if s.endswith(dumpName):
					stubDumpName = s

		if (self.checkpointFile):
			stubInputFilename = self.checkpointFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), self.checkpointFile.date, self.checkpointFile.chunk)
			stubInputFile = DumpFilename(self.wiki)
			stubInputFile.newFromFilename(stubInputFilename)
			stubOutputFilename = self.checkpointFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), self.checkpointFile.date, self.checkpointFile.chunk, self.checkpointFile.checkpoint)
			stubOutputFile = DumpFilename(self.wiki)
			stubOutputFile.newFromFilename(stubOutputFilename)
			self.writePartialStub(stubInputFile, stubOutputFile, self.checkpointFile.firstPageID, str(int(self.checkpointFile.lastPageID) + 1))
			stubOption = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stubOutputFile.filename)
		elif (self.pageIDRange):
			# two cases. redoing a specific chunk, OR no chunks, redoing the whole output file. ouch, hope it isn't huge.
			if (self._chunkToDo or not self._chunksEnabled):
				stubInputFile = f

			stubOutputFilename = stubInputFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), stubInputFile.date, stubInputFile.chunk, stubInputFile.checkpoint)
			stubOutputFile = DumpFilename(self.wiki)
			stubOutputFile.newFromFilename(stubOutputFilename)
			if (',' in self.pageIDRange):
				( firstPageID, lastPageID ) = self.pageIDRange.split(',',2)
			else:
				firstPageID = self.pageIDRange
				lastPageID = None
			self.writePartialStub(stubInputFile, stubOutputFile, firstPageID, lastPageID)

			stubOption = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stubOutputFile.filename)
		else:
			stubOption = "--stub=gzip:%s" % runner.dumpDir.filenamePublicPath(f)

		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		sources = []
		possibleSources = None
		if self._prefetch:
			possibleSources = self._findPreviousDump(runner, f.chunk)
			# if we have a list of more than one then we need to check existence for each and put them together in a string
			if possibleSources:
				for sourceFile in possibleSources:
					s = runner.dumpDir.filenamePublicPath(sourceFile, sourceFile.date)
					if exists(s):
						sources.append(s)
		if (f.chunk):
			chunkinfo = "%s" % f.chunk
		else:
			chunkinfo =""
		if (len(sources) > 0):
			source = "bzip2:%s" % (";".join(sources) )
			runner.showRunnerState("... building %s %s XML dump, with text prefetch from %s..." % (self._subset, chunkinfo, source))
			prefetch = "--prefetch=%s" % (source)
		else:
			runner.showRunnerState("... building %s %s XML dump, no text prefetch..." % (self._subset, chunkinfo))
			prefetch = ""

		if self._spawn:
			spawn = "--spawn=%s" % (self.wiki.config.php)
		else:
			spawn = ""

		if (not exists( self.wiki.config.php ) ):
			raise BackupError("php command %s not found" % self.wiki.config.php)

		if (self._checkpointsEnabled):
			checkpointTime = "--maxtime=%s" % (self.wiki.config.checkpointTime)
			checkpointFile = "--checkpointfile=%s" % outputFile.newFilename(outputFile.dumpName, outputFile.fileType, outputFile.fileExt, outputFile.date, outputFile.chunk, "p%sp%s", None)
		else:
			checkpointTime = ""
			checkpointFile = ""
		scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpTextPass.php")
		dumpCommand = [ "%s" % self.wiki.config.php, "-q" ]
		dumpCommand.extend(scriptCommand)
		dumpCommand.extend(["--wiki=%s" % runner.dbName,
				    "%s" % stubOption,
				    "%s" % prefetch,
				    "%s" % runner.forceNormalOption(),
				    "%s" % checkpointTime,
				    "%s" % checkpointFile,
				    "--report=1000",
				    "%s" % spawn 
				    ])
	
		dumpCommand = filter(None, dumpCommand) 
		command = dumpCommand
		filters = self.buildFilters(runner, outputFile)
		eta = self.buildEta(runner)
		command.extend([ filters, eta ])
		pipeline = [ command ]
		series = [ pipeline ]
		return series

	# taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
	def sort_nicely(self, l): 
		""" Sort the given list in the way that humans expect. 
		""" 
		convert = lambda text: int(text) if text.isdigit() else text 
		alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
		l.sort( key=alphanum_key ) 

	def getRelevantPrefetchFiles(self, fileList, startPageID, endPageID, date, runner):
		possibles = []
	        if (len(fileList)):
			# (a) nasty hack, see below (b)
			maxchunks = 0
		        for fileObj in fileList:
				if fileObj.isChunkFile and fileObj.chunkInt > maxchunks:
					maxchunks = fileObj.chunkInt
				if not fileObj.firstPageID:
					f = DumpFile(self.wiki, runner.dumpDir.filenamePublicPath(fileObj, date), fileObj, self.verbose)
					fileObj.firstPageID = f.findFirstPageIDInFile()

                        # get the files that cover our range
		        for fileObj in fileList:
				# If some of the fileObjs in fileList could not be properly be parsed, some of
				# the (int) conversions below will fail. However, it is of little use to us,
				# which conversion failed. /If any/ conversion fails, it means, that that we do
				# not understand how to make sense of the current fileObj. Hence we cannot use 
				# it as prefetch object and we have to drop it, to avoid passing a useless file
				# to the text pass. (This could days as of a comment below, but by not passing
				# a likely useless file, we have to fetch more texts from the database)
				#
				# Therefore try...except-ing the whole block is sufficient: If whatever error
				# occurs, we do not abort, but skip the file for prefetch.
				try:
					# If we could properly parse 
					firstPageIdInFile = int(fileObj.firstPageID)

					# fixme what do we do here? this could be very expensive. is that worth it??
					if not fileObj.lastPageID:
						# (b) nasty hack, see (a)
						# it's not a checkpoint fle or we'd have the pageid in the filename
						# so... temporary hack which will give expensive results
						# if chunk file, and it's the last chunk, put none
						# if it's not the last chunk, get the first pageid in the next chunk and subtract 1
						# if not chunk, put none.
						if fileObj.isChunkFile and fileObj.chunkInt < maxchunks:
							for f in fileList:
								if f.chunkInt == fileObj.chunkInt + 1:
									# not true!  this could be a few past where it really is
									# (because of deleted pages that aren't included at all)
									fileObj.lastPageID = str(int(f.firstPageID) - 1)
					if fileObj.lastPageID:
						lastPageIdInFile = int(fileObj.lastPageID)
					else:
						lastPageIdInFile = None
				
						# FIXME there is no point in including files that have just a few rev ids in them
						# that we need, and having to read through the whole file... could take
						# hours or days (later it won't matter, right? but until a rewrite, this is important)
						# also be sure that if a critical page is deleted by the time we try to figure out ranges,
						# that we don't get hosed
					if ( firstPageIdInFile <= int(startPageID) and (lastPageIdInFile == None or lastPageIdInFile >= int(startPageID)) ) or ( firstPageIdInFile >= int(startPageID) and ( endPageID == None or firstPageIdInFile <= int(endPageID) ) ):
						possibles.append(fileObj)
				except:
					runner.debug( "Could not make sense of %s for prefetch. Format update? Corrupt file?" % fileObj.filename )
		return possibles
		
	# this finds the content file or files from the first previous successful dump
	# to be used as input ("prefetch") for this run.
	def _findPreviousDump(self, runner, chunk = None):
		"""The previously-linked previous successful dump."""
		if (chunk):
			startPageID = sum([ self._chunks[i] for i in range(0,int(chunk)-1)]) + 1
			if (len(self._chunks) > int(chunk)):
				endPageID = sum([ self._chunks[i] for i in range(0,int(chunk))])
			else:
				endPageID = None
		else:
			startPageID = 1
			endPageID = None

		dumps = self.wiki.dumpDirs()
		dumps.sort()
		dumps.reverse()
		for date in dumps:
			if (date == self.wiki.date):
				runner.debug("skipping current dump for prefetch of job %s, date %s" % (self.name(), self.wiki.date))
				continue

			# see if this job from that date was successful
			if not runner.runInfoFile.statusOfOldDumpIsDone(runner, date, self.name(), self._desc):
				runner.debug("skipping incomplete or failed dump for prefetch date %s" % date)
				continue

			# first check if there are checkpoint files from this run we can use
			files = self.listCheckpointFilesExisting(runner.dumpDir, [ self.dumpName ], date, chunks = None)
                        possiblePrefetchList = self.getRelevantPrefetchFiles(files, startPageID, endPageID, date, runner)
			if (len(possiblePrefetchList)):
				return(possiblePrefetchList)

			# ok, let's check for chunk files instead, from any run (may not conform to our numbering
			# for this job)
			files = self.listRegularFilesExisting(runner.dumpDir,[ self.dumpName ], date, chunks = True)
                        possiblePrefetchList = self.getRelevantPrefetchFiles(files, startPageID, endPageID, date, runner)
			if (len(possiblePrefetchList)):
				return(possiblePrefetchList)

	                # last shot, get output file that contains all the pages, if there is one
			files = self.listRegularFilesExisting(runner.dumpDir, [ self.dumpName ], date, chunks = False)
			# there is only one, don't bother to check for relevance :-P
                        possiblePrefetchList = files
			files = []
			for p in possiblePrefetchList: 
				possible = runner.dumpDir.filenamePublicPath(p, date)
				size = os.path.getsize(possible)
				if size < 70000:
					runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, possible))
					continue
				else:
					files.append(p)
			if (len(files)):
				return(files)

		runner.debug("Could not locate a prefetchable dump.")
		return None

	def listOutputFilesForCleanup(self, dumpDir, dumpNames = None):
		files = Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames)
		filesToReturn = []
		if self.pageIDRange:
			if (',' in self.pageIDRange):
				( firstPageID, lastPageID ) = self.pageIDRange.split(',',2)
				firstPageID = int(firstPageID)
				lastPageID = int(lastPageID)
			else:
				firstPageID = int(self.pageIDRange)
				lastPageID = None
			# filter any checkpoint files, removing from the list any with
			# page range outside of the page range this job will cover
			for f in files:
				if f.isCheckpointFile:
					if not firstPageID or (f.firstPageID and (int(f.firstPageID) >= firstPageID)):
						if not lastPageID or (f.lastPageID and (int(f.lastPageID) <= lastPageID)):
							filesToReturn.append(f)
				else:
					filesToReturn.append(f)
		return filesToReturn

class RecombineXmlDump(XmlDump):
	def __init__(self, name, desc, detail, itemForXmlDumps):
		# no prefetch, no spawn
		self.itemForXmlDumps = itemForXmlDumps
		self._detail = detail
		self._prerequisiteItems = [ self.itemForXmlDumps ]
		Dump.__init__(self, name, desc)
		# the input may have checkpoints but the output will not.
		self._checkpointsEnabled = False

	def listDumpNames(self):
		return self.itemForXmlDumps.listDumpNames()

	def getFileType(self):
		return self.itemForXmlDumps.getFileType()

	def getFileExt(self):
		return self.itemForXmlDumps.getFileExt()

	def getDumpName(self):
		return self.itemForXmlDumps.getDumpName()

	def run(self, runner):
		files = self.itemForXmlDumps.listOutputFilesForInput(runner.dumpDir)
		outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
		if (len(outputFiles) > 1):
			raise BackupError("recombine XML Dump trying to produce more than one output file")

		error=0
		if (not exists( runner.wiki.config.bzip2 ) ):
			raise BackupError("bzip2 command %s not found" % runner.wiki.config.bzip2)
		compressionCommand = runner.wiki.config.bzip2
		compressionCommand = "%s > " % runner.wiki.config.bzip2
		uncompressionCommand = [ "%s" % runner.wiki.config.bzip2, "-dc" ] 
		recombineCommandString = self.buildRecombineCommandString(runner, files, outputFiles[0], compressionCommand, uncompressionCommand )
		recombineCommand = [ recombineCommandString ]
		recombinePipeline = [ recombineCommand ]
		series = [ recombinePipeline ]
		error = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)

		if (error):
			raise BackupError("error recombining xml bz2 files")

class XmlMultiStreamDump(XmlDump):
	"""Take a .bz2 and recompress it as multistream bz2, 100 pages per stream."""

	def __init__(self, subset, name, desc, detail, itemForRecompression, wiki, chunkToDo, chunks = False, checkpoints = False, checkpointFile = None):
		self._subset = subset
		self._detail = detail
		self._chunks = chunks
		if self._chunks:
			self._chunksEnabled = True
		self._chunkToDo = chunkToDo
		self.wiki = wiki
		self.itemForRecompression = itemForRecompression
		if checkpoints:
			self._checkpointsEnabled = True
		self.checkpointFile = checkpointFile
		self._prerequisiteItems = [ self.itemForRecompression ]
		Dump.__init__(self, name, desc)

	def getDumpName(self):
		return "pages-" + self._subset

	def listDumpNames(self):
		d = self.getDumpName();
		return [ self.getDumpNameMultistream(d), self.getDumpNameMultistreamIndex(d) ];

	def getFileType(self):
		return "xml"

	def getIndexFileType(self):
		return "txt"

	def getFileExt(self):
		return "bz2"

	def getDumpNameMultistream(self, name):
		return name + "-multistream"

	def getDumpNameMultistreamIndex(self, name):
		return self.getDumpNameMultistream(name) + "-index"

	def getFileMultistreamName(self, f):
		"""assuming that f is the name of an input file,
		return the name of the associated multistream output file"""
		return DumpFilename(self.wiki, f.date, self.getDumpNameMultistream(f.dumpName), f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp) 

	def getFileMultistreamIndexName(self, f):
		"""assuming that f is the name of a multistream output file,
		return the name of the associated index file"""
		return DumpFilename(self.wiki, f.date, self.getDumpNameMultistreamIndex(f.dumpName), self.getIndexFileType(), self.fileExt, f.chunk, f.checkpoint, f.temp) 

	# output files is a list of checkpoint files, otherwise it is a list of one file. 
	# checkpoint files get done one at a time. we can't really do parallel recompression jobs of 
	# 200 files, right?
	def buildCommand(self, runner, outputFiles):
		# FIXME need shell escape
		if (not exists( self.wiki.config.bzip2 ) ):
			raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
		if (not exists( self.wiki.config.recompressxml ) ):
			raise BackupError("recompressxml command %s not found" % self.wiki.config.recompressxml)

		commandSeries = []
		for f in outputFiles:
			inputFile = DumpFilename(self.wiki, None, f.dumpName, f.fileType, self.itemForRecompression.fileExt, f.chunk, f.checkpoint) 
			outfile = runner.dumpDir.filenamePublicPath(self.getFileMultistreamName(f))
			outfileIndex = runner.dumpDir.filenamePublicPath(self.getFileMultistreamIndexName(f))
			infile = runner.dumpDir.filenamePublicPath(inputFile)
			commandPipe = [ [ "%s -dc %s | %s --pagesperstream 100 --buildindex %s > %s"  % (self.wiki.config.bzip2, infile, self.wiki.config.recompressxml, outfileIndex, outfile) ] ]
			commandSeries.append(commandPipe)
		return(commandSeries)

	def run(self, runner):
		commands = []
		self.cleanupOldFiles(runner.dumpDir)
		if self.checkpointFile:
			outputFile = DumpFilename(self.wiki, None, self.checkpointFile.dumpName, self.checkpointFile.fileType, self.fileExt, self.checkpointFile.chunk, self.checkpointFile.checkpoint) 
			series = self.buildCommand(runner, [ outputFile ])
			commands.append(series)
		elif self._chunksEnabled and not self._chunkToDo:
			# must set up each parallel job separately, they may have checkpoint files that
			# need to be processed in series, it's a special case
			for i in range(1, len(self._chunks)+1):
				outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir, i)
				series = self.buildCommand(runner, outputFiles)
				commands.append(series)
		else:
			outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
			series = self.buildCommand(runner, outputFiles)
			commands.append(series)
			
		error = runner.runCommand(commands, callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
		if (error):
			raise BackupError("error recompressing bz2 file(s)")

	# shows all files possible if we don't have checkpoint files. without temp files of course
	def listOutputFilesToPublish(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			files.append(self.getFileMultistreamName(f))
			files.append(self.getFileMultistreamIndexName(f))
		return files

	# shows all files possible if we don't have checkpoint files. without temp files of course
	# only the chunks we are actually supposed to do (if there is a limit)
	def listOutputFilesToCheckForTruncation(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			if self._chunkToDo and f.chunkInt != self._chunkToDo:
				continue
			files.append(self.getFileMultistreamName(f))
			files.append(self.getFileMultistreamIndexName(f))
		return files

	# shows all files possible if we don't have checkpoint files. no temp files.
	# only the chunks we are actually supposed to do (if there is a limit)
	def listOutputFilesForBuildCommand(self, dumpDir, chunk = None):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			# if this param is set it takes priority
			if chunk and f.chunkInt != chunk:
				continue
			elif self._chunkToDo and f.chunkInt != self._chunkToDo:
				continue
			# we don't convert these names to the final output form, we'll do that in the build command
			# (i.e. add "multistream" and "index" to them)
			files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
		return files

	# shows all files possible if we don't have checkpoint files. should include temp files 
	# does just the chunks we do if there is a limit
	def listOutputFilesForCleanup(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		multistreamNames = []
		for d in dumpNames:
			multistreamNames.extend( [ self.getDumpNameMultistream(d), self.getDumpNameMultistreamIndex(d) ] )

		files = []
		if (self.itemForRecompression._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
			files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
		else:
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
		return files

	# must return all output files that could be produced by a full run of this stage, 
	# not just whatever we happened to produce (if run for one chunk, say)
	def listOutputFilesForInput(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			files.append(self.getFileMultistreamName(f))
			files.append(self.getFileMultistreamIndexName(f))
		return files

class BigXmlDump(XmlDump):
	"""XML page dump for something larger, where a 7-Zip compressed copy
	could save 75% of download time for some users."""

	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--full"

class XmlRecompressDump(Dump):
	"""Take a .bz2 and recompress it as 7-Zip."""

	def __init__(self, subset, name, desc, detail, itemForRecompression, wiki, chunkToDo, chunks = False, checkpoints = False, checkpointFile = None):
		self._subset = subset
		self._detail = detail
		self._chunks = chunks
		if self._chunks:
			self._chunksEnabled = True
		self._chunkToDo = chunkToDo
		self.wiki = wiki
		self.itemForRecompression = itemForRecompression
		if checkpoints:
			self._checkpointsEnabled = True
		self.checkpointFile = checkpointFile
		self._prerequisiteItems = [ self.itemForRecompression ]
		Dump.__init__(self, name, desc)

	def getDumpName(self):
		return "pages-" + self._subset

	def getFileType(self):
		return "xml"

	def getFileExt(self):
		return "7z"

	# output files is a list of checkpoint files, otherwise it is a list of one file. 
	# checkpoint files get done one at a time. we can't really do parallel recompression jobs of 
	# 200 files, right?
	def buildCommand(self, runner, outputFiles):
		# FIXME need shell escape
		if (not exists( self.wiki.config.bzip2 ) ):
			raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
		if (not exists( self.wiki.config.sevenzip ) ):
			raise BackupError("7zip command %s not found" % self.wiki.config.sevenzip)

		commandSeries = []
		for f in outputFiles:
			inputFile = DumpFilename(self.wiki, None, f.dumpName, f.fileType, self.itemForRecompression.fileExt, f.chunk, f.checkpoint) 
			outfile = runner.dumpDir.filenamePublicPath(f)
			infile = runner.dumpDir.filenamePublicPath(inputFile)
			commandPipe = [ [ "%s -dc %s | %s a -si %s"  % (self.wiki.config.bzip2, infile, self.wiki.config.sevenzip, outfile) ] ]
			commandSeries.append(commandPipe)
		return(commandSeries)

	def run(self, runner):
		commands = []
		# Remove prior 7zip attempts; 7zip will try to append to an existing archive
		self.cleanupOldFiles(runner.dumpDir)
		if self.checkpointFile:
			outputFile = DumpFilename(self.wiki, None, self.checkpointFile.dumpName, self.checkpointFile.fileType, self.fileExt, self.checkpointFile.chunk, self.checkpointFile.checkpoint) 
			series = self.buildCommand(runner, [ outputFile ])
			commands.append(series)
		elif self._chunksEnabled and not self._chunkToDo:
			# must set up each parallel job separately, they may have checkpoint files that
			# need to be processed in series, it's a special case
			for i in range(1, len(self._chunks)+1):
				outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir, i)
				series = self.buildCommand(runner, outputFiles)
				commands.append(series)
		else:
			outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
			series = self.buildCommand(runner, outputFiles)
			commands.append(series)
			
		error = runner.runCommand(commands, callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
		if (error):
			raise BackupError("error recompressing bz2 file(s)")

	# shows all files possible if we don't have checkpoint files. without temp files of course
	def listOutputFilesToPublish(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			files.append(DumpFilename(wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
		return files

	# shows all files possible if we don't have checkpoint files. without temp files of course
	# only the chunks we are actually supposed to do (if there is a limit)
	def listOutputFilesToCheckForTruncation(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			if self._chunkToDo and f.chunkInt != self._chunkToDo:
				continue
			files.append(DumpFilename(wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
		return files

	# shows all files possible if we don't have checkpoint files. no temp files.
	# only the chunks we are actually supposed to do (if there is a limit)
	def listOutputFilesForBuildCommand(self, dumpDir, chunk = None):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			# if this param is set it takes priority
			if chunk and f.chunkInt != chunk:
				continue
			elif self._chunkToDo and f.chunkInt != self._chunkToDo:
				continue
			files.append(DumpFilename(wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
		return files

	# shows all files possible if we don't have checkpoint files. should include temp files 
	# does just the chunks we do if there is a limit
	def listOutputFilesForCleanup(self, dumpDir, dumpNames = None):
		# some stages (eg XLMStubs) call this for several different dumpNames
		if (dumpNames == None):
			dumpNames = [ self.dumpName ]
		files = []
		if (self.itemForRecompression._checkpointsEnabled):
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
			files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		else:
			# we will pass list of chunks or chunkToDo, or False, depending on the job setup.
			files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
		return files

	# must return all output files that could be produced by a full run of this stage, 
	# not just whatever we happened to produce (if run for one chunk, say)
	def listOutputFilesForInput(self, dumpDir):
		files = []
		inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
		for f in inputFiles:
			files.append(DumpFilename(wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
		return files


class RecombineXmlRecompressDump(Dump):
	def __init__(self, name, desc, detail, itemForRecombine, wiki):
		self._detail = detail
		self._desc = desc
		self.wiki = wiki
		self.itemForRecombine = itemForRecombine
		self._prerequisiteItems = [ self.itemForRecombine ]
		Dump.__init__(self, name, desc)
		# the input may have checkpoints but the output will not.
		self._checkpointsEnabled = False
		self._chunksEnabled = False

        def getFileType(self):
		return self.itemForRecombine.getFileType()

        def getFileExt(self):
                return self.itemForRecombine.getFileExt()

        def getDumpName(self):
                return self.itemForRecombine.getDumpName()

	def run(self, runner):
		error = 0
		self.cleanupOldFiles(runner.dumpDir)
		outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir)
		for outputFile in outputFileList:
			inputFiles = []
			files = self.itemForRecombine.listOutputFilesForInput(runner.dumpDir)
			for inFile in files:
				if inFile.dumpName == outputFile.dumpName:
					inputFiles.append(inFile)
			if not len(inputFiles):
				self.setStatus("failed")
				raise BackupError("No input files for %s found" % self.name())
			if (not exists( self.wiki.config.sevenzip ) ):
				raise BackupError("sevenzip command %s not found" % self.wiki.config.sevenzip)
			compressionCommand = "%s a -si" % self.wiki.config.sevenzip
			uncompressionCommand = [ "%s" % self.wiki.config.sevenzip, "e", "-so" ] 

			recombineCommandString = self.buildRecombineCommandString(runner, files, outputFile, compressionCommand, uncompressionCommand )
			recombineCommand = [ recombineCommandString ]
			recombinePipeline = [ recombineCommand ]
			series = [ recombinePipeline ]
			result = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
			if result:
				error = result
		if (error):
			raise BackupError("error recombining xml bz2 file(s)")

class AbstractDump(Dump):
	"""XML dump for Yahoo!'s Active Abstracts thingy"""

        def __init__(self, name, desc, chunkToDo, chunks = False):
		self._chunkToDo = chunkToDo
		self._chunks = chunks
		if self._chunks:
			self._chunksEnabled = True
		Dump.__init__(self, name, desc)

	def getDumpName(self):
		return "abstract"

	def getFileType(self):
		return "xml"

	def getFileExt(self):
		return ""

        def buildCommand(self, runner, f):
		if (not exists( runner.wiki.config.php ) ):
			raise BackupError("php command %s not found" % runner.wiki.config.php)
		scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpBackup.php")
		command = [ "%s" % runner.wiki.config.php, "-q" ]
		command.extend(scriptCommand)
		version = MultiVersion.MWVersion(runner.wiki.config, runner.dbName)
		if version:
			abstractFilterCommand = "--plugin=AbstractFilter:%s/%s/extensions/ActiveAbstract/AbstractFilter.php" % (runner.wiki.config.wikiDir, version)
		else:
			abstractFilterCommand = "--plugin=AbstractFilter:%s/extensions/ActiveAbstract/AbstractFilter.php" % runner.wiki.config.wikiDir
		command.extend([ "--wiki=%s" % runner.dbName,
				 abstractFilterCommand,
				 "--current", "--report=1000", "%s" % runner.forceNormalOption(),
				 ])

		for v in self._variants():
			variantOption = self._variantOption(v)
			dumpName = self.dumpNameFromVariant(v)
			fileObj = DumpFilename(runner.wiki, f.date, dumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint)
			command.extend( [ "--output=file:%s" % runner.dumpDir.filenamePublicPath(fileObj),
					  "--filter=namespace:NS_MAIN", "--filter=noredirect", 
					  "--filter=abstract%s" % variantOption ] )
		if (f.chunk):
			# set up start end end pageids for this piece
			# note there is no page id 0 I guess. so we start with 1
			# start = runner.pagesPerChunk()*(chunk-1) + 1
			start = sum([ self._chunks[i] for i in range(0,f.chunkInt-1)]) + 1
			startopt = "--start=%s" % start
			# if we are on the last chunk, we should get up to the last pageid, 
			# whatever that is. 
			command.append(startopt)
			if f.chunkInt < len(self._chunks):
				# end = start + runner.pagesPerChunk()
				end = sum([ self._chunks[i] for i in range(0,f.chunkInt)]) +1
				endopt = "--end=%s" % end
				command.append(endopt)
		pipeline = [ command ]
		series = [ pipeline ]
		return(series)

	def run(self, runner):
		commands = []
		# choose the empty variant to pass to buildcommand, it will fill in the rest if needed
		outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
		dumpName0 = self.listDumpNames()[0]
		for f in outputFiles:
			if f.dumpName == dumpName0:
				series = self.buildCommand(runner, f)
				commands.append(series)
		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if (error):
			raise BackupError("error producing abstract dump")

	# If the database name looks like it's marked as Chinese language,
	# return a list including Simplified and Traditional versions, so
	# we can build separate files normalized to each orthography.
	def _variants(self):
		if runner.dbName[0:2] == "zh" and runner.dbName[2:3] != "_":
			variants = [ "", "zh-cn", "zh-tw"]
		else:
			variants = [ "" ]
		return variants

	def _variantOption(self, variant):
		if variant == "":
			return ""
		else:
			return ":variant=%s" % variant

	def dumpNameFromVariant(self, v):
		dumpNameBase = 'abstract'
		if v == "":
			return dumpNameBase
		else:
			return dumpNameBase + "-" + v

	def listDumpNames(self):
		# need this first for buildCommand and other such
		dumpNames = [ ]
		variants = self._variants()
		for v in variants:
			dumpNames.append(self.dumpNameFromVariant(v))
		return dumpNames

	def listOutputFilesToPublish(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
		return files

	def listOutputFilesToCheckForTruncation(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForBuildCommand(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForBuildCommand(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForCleanup(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames))
		return files

	def listOutputFilesForInput(self, dumpDir):
		dumpNames =  self.listDumpNames()
		files = []
		files.extend(Dump.listOutputFilesForInput(self, dumpDir, dumpNames))
		return files


class RecombineAbstractDump(Dump):
	def __init__(self, name, desc, itemForRecombine):
		# no chunkToDo, no chunks generally (False, False), even though input may have it
		self.itemForRecombine = itemForRecombine
		self._prerequisiteItems = [ self.itemForRecombine ]
		Dump.__init__(self, name, desc)
		# the input may have checkpoints but the output will not.
		self._checkpointsEnabled = False

        def getFileType(self):
		return self.itemForRecombine.getFileType()

        def getFileExt(self):
                return self.itemForRecombine.getFileExt()

        def getDumpName(self):
                return self.itemForRecombine.getDumpName()

	def run(self, runner):
		error = 0
		files = self.itemForRecombine.listOutputFilesForInput(runner.dumpDir)
		outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir)
		for outputFile in outputFileList:
			inputFiles = []
			for inFile in files:
				if inFile.dumpName == outputFile.dumpName:
					inputFiles.append(inFile)
			if not len(inputFiles):
				self.setStatus("failed")
				raise BackupError("No input files for %s found" % self.name())
			if (not exists( runner.wiki.config.cat ) ):
				raise BackupError("cat command %s not found" % runner.wiki.config.cat)
			compressionCommand = "%s > " % runner.wiki.config.cat
			uncompressionCommand = [ "%s" % runner.wiki.config.cat ] 
			recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFile, compressionCommand, uncompressionCommand, "<feed>" )
			recombineCommand = [ recombineCommandString ]
			recombinePipeline = [ recombineCommand ]
			series = [ recombinePipeline ]
			result = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
			if result:
				error = result
		if (error):
			raise BackupError("error recombining abstract dump files")

class TitleDump(Dump):
	"""This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""

	def getDumpName(self):
		return "all-titles-in-ns0"

	def getFileType(self):
		return ""

	def getFileExt(self):
		return "gz"

	def run(self, runner):
		retries = 0
		# try this initially and see how it goes
		maxretries = 3 
		query="select page_title from page where page_namespace=0;"
		files = self.listOutputFilesForBuildCommand(runner.dumpDir)
		if len(files) > 1:
			raise BackupError("page title dump trying to produce more than one output file")
		fileObj = files[0]
		outFilename = runner.dumpDir.filenamePublicPath(fileObj)
		error = self.saveSql(query, outFilename, runner)
		while (error and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			error = self.saveSql(query, outFilename, runner)
		if (error):
			raise BackupError("error dumping titles list")

	def saveSql(self, query, outfile, runner):
		"""Pass some SQL commands to the server for this DB and save output to a gzipped file."""
		if (not exists( runner.wiki.config.gzip ) ):
			raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
		command = runner.dbServerInfo.buildSqlCommand(query, runner.wiki.config.gzip)
		return runner.saveCommand(command, outfile)

def findAndLockNextWiki(config, locksEnabled):
	if config.halt:
		print "Dump process halted by config."
		return None

	next = config.dbListByAge()
	next.reverse()

	print "Finding oldest unlocked wiki..."

	for db in next:
		wiki = WikiDump.Wiki(config, db)
		try:
			if (locksEnabled):
				wiki.lock()
			return wiki
		except:
			print "Couldn't lock %s, someone else must have got it..." % db
			continue
	return None

def xmlEscape(text):
	return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def usage(message = None):
	if message:
		print message
	print "Usage: python worker.py [options] [wikidbname]"
	print "Options: --aftercheckpoint, --checkpoint, --chunk, --configfile, --date, --job, --addnotice, --delnotice, --force, --noprefetch, --nospawn, --restartfrom, --log"
	print "--aftercheckpoint: Restart thie job from the after specified checkpoint file, doing the"
	print "               rest of the job for the appropriate chunk if chunks are configured"
	print "               or for the all the rest of the revisions if no chunks are configured;"
	print "               only for jobs articlesdump, metacurrentdump, metahistorybz2dump."
	print "--checkpoint:  Specify the name of the checkpoint file to rerun (requires --job,"
	print "               depending on the file this may imply --chunk)"
	print "--chunk:       Specify the number of the chunk to rerun (use with a specific job"
	print "               to rerun, only if parallel jobs (chunks) are enabled)."
	print "--configfile:  Specify an alternative configuration file to read."
	print "               Default config file name: wikidump.conf"
	print "--date:        Rerun dump of a given date (probably unwise)"
	print "--addnotice:   Text message that will be inserted in the per-dump-run index.html"
	print "               file; use this when rerunning some job and you want to notify the"
	print "               potential downloaders of problems, for example.  This option "
	print "               remains in effective for the specified wiki and date until"
	print "               the delnotice option is given."
	print "--delnotice:   Remove any notice that has been specified by addnotice, for"
	print "               the given wiki and date."
	print "--job:         Run just the specified step or set of steps; for the list,"
	print "               give the option --job help"
	print "               This option requires specifiying a wikidbname on which to run."
	print "               This option cannot be specified with --force."
	print "--dryrun:      Don't really run the job, just print what would be done (must be used"
	print "               with a specified wikidbname on which to run"
	print "--force:       remove a lock file for the specified wiki (dangerous, if there is"
	print "               another process running, useful if you want to start a second later"
	print "               run while the first dump from a previous date is still going)"
	print "               This option cannot be specified with --job."
	print "--noprefetch:  Do not use a previous file's contents for speeding up the dumps"
	print "               (helpful if the previous files may have corrupt contents)"
	print "--nospawn:     Do not spawn a separate process in order to retrieve revision texts"
	print "--restartfrom: Do all jobs after the one specified via --job, including that one"
	print "--log:         Log progress messages and other output to logfile in addition to"
	print "               the usual console output"
	print "--verbose:     Print lots of stuff (includes printing full backtraces for any exception)"
	print "               This is used primarily for debugging"

	sys.exit(1)

if __name__ == "__main__":
	try:
		date = None
		configFile = False
		forceLock = False
		prefetch = True
		spawn = True
		restart = False
		jobRequested = None
		enableLogging = False
		log = None
		htmlNotice = ""
		dryrun = False
		chunkToDo = False
		afterCheckpoint = False
		checkpointFile = None
		pageIDRange = None
		result = False
		verbose = False

		try:
			(options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
								 ['date=', 'job=', 'configfile=', 'addnotice=', 'delnotice', 'force', 'dryrun', 'noprefetch', 'nospawn', 'restartfrom', 'aftercheckpoint=', 'log', 'chunk=', 'checkpoint=', 'pageidrange=', 'verbose' ])
		except:
			usage("Unknown option specified")

		for (opt, val) in options:
			if opt == "--date":
				date = val
			elif opt == "--configfile":
				configFile = val
			elif opt == '--checkpoint':
				checkpointFile = val
			elif opt == '--chunk':
				chunkToDo = int(val)
			elif opt == "--force":
				forceLock = True
			elif opt == '--aftercheckpoint':
				afterCheckpoint = True
				checkpointFile = val
			elif opt == "--noprefetch":
				prefetch = False
			elif opt == "--nospawn":
				spawn = False
			elif opt == "--dryrun":
				dryrun = True
			elif opt == "--job":
				jobRequested = val
			elif opt == "--restartfrom":
				restart = True
			elif opt == "--log":
				enableLogging = True
			elif opt == "--addnotice":
				htmlNotice = val
			elif opt == "--delnotice":
				htmlNotice = False
			elif opt == "--pageidrange":
				pageIDRange = val
			elif opt == "--verbose":
				verbose = True

		if dryrun and (len(remainder) == 0):
			usage("--dryrun requires the name of a wikidb to be specified")
		if jobRequested and (len(remainder) == 0):
			usage("--job option requires the name of a wikidb to be specified")
		if (jobRequested and forceLock):
	       		usage("--force cannot be used with --job option")
		if (restart and not jobRequested):
			usage("--restartfrom requires --job and the job from which to restart")
		if (chunkToDo and not jobRequested):
			usage("--chunk option requires a specific job for which to rerun that chunk")
		if (chunkToDo and restart):
			usage("--chunk option can be specified only for one specific job")
		if checkpointFile and (len(remainder) == 0):
			usage("--checkpoint option requires the name of a wikidb to be specified")
		if checkpointFile and not jobRequested:
			usage("--checkpoint option requires --job and the job from which to restart")
		if pageIDRange and not jobRequested:
			usage("--pageidrange option requires --job and the job from which to restart")
		if pageIDRange and checkpointFile:
			usage("--pageidrange option cannot be used with --checkpoint option")

		# allow alternate config file
		if (configFile):
			config = WikiDump.Config(configFile)
		else:
			config = WikiDump.Config()

		if dryrun or chunkToDo or (jobRequested and not restart):
			locksEnabled = False
		else:
			locksEnabled = True

		if dryrun:
			print "***"
			print "Dry run only, no files will be updated."
			print "***"

		if len(remainder) > 0:
			wiki = WikiDump.Wiki(config, remainder[0])
			if locksEnabled:
				if forceLock and wiki.isLocked():
					wiki.unlock()
				if locksEnabled:
					wiki.lock()

		else:
			wiki = findAndLockNextWiki(config, locksEnabled)

		if wiki:
			# process any per-project configuration options
			config.parseConfFilePerProject(wiki.dbName)

			if not date:
				date = TimeUtils.today()
			wiki.setDate(date)

			if (afterCheckpoint):
				f = DumpFilename(wiki) 
				f.newFromFilename(checkpointFile)
				if not f.isCheckpointFile:
					usage("--aftercheckpoint option requires the name of a checkpoint file, bad filename provided")
				pageIDRange = str( int(f.lastPageID) + 1 )
				chunkToDo = f.chunkInt
				# now we don't need this. 
				checkpointFile = None
				afterCheckpointJobs = [ 'articlesdump', 'metacurrentdump', 'metahistorybz2dump' ]
				if not jobRequested or not jobRequested in [ 'articlesdump', 'metacurrentdump', 'metahistorybz2dump' ]:
					usage("--aftercheckpoint option requires --job option with one of %s" % ", ".join(afterCheckpointJobs))
					
			runner = Runner(wiki, prefetch, spawn, jobRequested, restart, htmlNotice, dryrun, enableLogging, chunkToDo, checkpointFile, pageIDRange, verbose)
			if (restart):
				print "Running %s, restarting from job %s..." % (wiki.dbName, jobRequested)
			elif (jobRequested):
				print "Running %s, job %s..." % (wiki.dbName, jobRequested)
			else:
				print "Running %s..." % wiki.dbName
			result = runner.run()
			# if we are doing one piece only of the dump, we don't unlock either
			if locksEnabled:
				wiki.unlock()
		else:
			print "No wikis available to run."
			result = True
	finally:
		WikiDump.cleanup()
	if result == False:
		sys.exit(1)
	else:
		sys.exit(0)
