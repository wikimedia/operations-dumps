# Worker process, does the actual dumping

import getopt
import md5
import os
import popen2
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

from os.path import dirname, exists, getsize, join, realpath
from subprocess import Popen, PIPE
from WikiDump import FileUtils, MiscUtils, TimeUtils
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel

def xmlEscape(text):
	return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");

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
		self._chunksEnabled = wiki.config.chunksEnabled
		self._pagesPerChunkHistory = self.convertCommaSepLineToNumbers(wiki.config.pagesPerChunkHistory)
		self._revsPerChunkHistory = self.convertCommaSepLineToNumbers(wiki.config.revsPerChunkHistory)
		self._pagesPerChunkAbstract = self.convertCommaSepLineToNumbers(wiki.config.pagesPerChunkAbstract)
		self._recombineHistory = wiki.config.recombineHistory

		if (self._chunksEnabled):
			self.Stats = PageAndEditStats(wiki,dbName, errorCallback)
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

class DbServerInfo(object):
	def __init__(self, wiki, dbName, errorCallback = None):
		self.config = wiki.config
		self.dbName = dbName
		self.errorCallback = errorCallback
		self.selectDatabaseServer()

	def defaultServer(self):
		# if this fails what do we do about it? Not a bleeping thing. *ugh* FIXME!!
		command = "%s -q %s/maintenance/getSlaveServer.php --wiki=%s --group=dump" % MiscUtils.shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		return RunSimpleCommand.runAndReturn(command, self.errorCallback).strip()

	def selectDatabaseServer(self):
		self.dbServer = self.defaultServer()

	def buildSqlCommand(self, query, pipeto = None):
		"""Put together a command to execute an sql query to the server for this DB."""
		command = [ [ "/bin/echo", "%s" % query ], 
			    [ "%s" % self.config.mysql, "-h", 
			      "%s" % self.dbServer,
			      "-u", "%s" % self.config.dbUser,
			      "%s" % self.passwordOption(),
			      "%s" % self.dbName, 
			      "-r" ] ]
		if (pipeto):
			command.append([ pipeto ])
		return command

	def buildSqlDumpCommand(self, table, pipeto = None):
		"""Put together a command to dump a table from the current DB with mysqldump
		and save to a gzipped sql file."""
		command = [ [ "%s" % self.config.mysqldump, "-h", 
			       "%s" % self.dbServer, "-u", 
			       "%s" % self.config.dbUser, 
			       "%s" % self.passwordOption(), "--opt", "--quick", 
			       "--skip-add-locks", "--skip-lock-tables", 
			       "%s" % self.dbName, 
			       "%s" % self.getDBTablePrefix() + table ] ]
		if (pipeto):
			command.append([ pipeto ])
		return command

	def runSqlAndGetOutput(self, query):
		command = self.buildSqlCommand(query)
		p = CommandPipeline(command, quiet=True)
		p.runPipelineAndGetOutput()
		# fixme best to put the return code someplace along with any errors....
		if (p.output()):
			return(p.output())
		else:
			return None

	def passwordOption(self):
		"""If you pass '-pfoo' mysql uses the password 'foo',
		but if you pass '-p' it prompts. Sigh."""
		if self.config.dbPassword == "":
			return None
		else:
			return "-p" + self.config.dbPassword

	def getDBTablePrefix(self):
		"""Get the prefix for all tables for the specific wiki ($wgDBprefix)"""
		# FIXME later full path
		command = "echo 'print $wgDBprefix; ' | %s -q %s/maintenance/eval.php --wiki=%s" % MiscUtils.shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		return RunSimpleCommand.runAndReturn(command, self.errorCallback).strip()
				      

class RunSimpleCommand(object):

	# FIXME rewrite to not use popen2
	def runAndReturn(command, logCallback = None):
		"""Run a command and return the output as a string.
		Raises BackupError on non-zero return code."""
		# FIXME convert all these calls so they just use runCommand now
		retval = 1
		retries=0
		maxretries=3
		proc = popen2.Popen4(command, 64)
		output = proc.fromchild.read()
		retval = proc.wait()
		while (retval and retries < maxretries):
			if logCallback:
				logCallback("Non-zero return code from '%s'" % command)
			time.sleep(5)
			proc = popen2.Popen4(command, 64)
			output = proc.fromchild.read()
			retval = proc.wait()
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
		self.config = wiki.config
		self.dbName = dbName
		self.dbServerInfo = DbServerInfo(wiki, dbName, errorCallback)
		self.getStatistics(config,dbName)

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
	
	def __init__(self, wiki, prefetch, spawn, date, chunkInfo):
		self.date = date
		self.wiki = wiki
		self._hasFlaggedRevs = self.wiki.hasFlaggedRevs()
		self._isBig = self.wiki.isBig()
		self._prefetch = prefetch
		self._spawn = spawn
		self.chunkInfo = chunkInfo

		self.dumpItems = [PrivateTable("user", "usertable", "User account data."),
			PrivateTable("watchlist", "watchlisttable", "Users' watchlist settings."),
			PrivateTable("ipblocks", "ipblockstable", "Data for blocks of IP addresses, ranges, and users."),
			PrivateTable("archive", "archivetable", "Deleted page and revision data."),
#			PrivateTable("updates", "updatestable", "Update dataset for OAI updater system."),
			PrivateTable("logging", "loggingtable", "Data for various events (deletions, uploads, etc)."),
			#PrivateTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded images."),
			#PrivateTable("filearchive", "filearchivetable", "Deleted image data"),

			PublicTable("site_stats", "sitestatstable", "A few statistics such as the page count."),
			PublicTable("image", "imagetable", "Metadata on current versions of uploaded images."),
			PublicTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded images."),
			PublicTable("pagelinks", "pagelinkstable", "Wiki page-to-page link records."),
			PublicTable("categorylinks", "categorylinkstable", "Wiki category membership link records."),
			PublicTable("imagelinks", "imagelinkstable", "Wiki image usage records."),
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

			AbstractDump("abstractsdump","Extracted page abstracts for Yahoo", self.chunkInfo.getPagesPerChunkAbstract())]

		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineAbstractDump("abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo", self.chunkInfo.getPagesPerChunkAbstract()))

		self.dumpItems.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps", self.chunkInfo.getPagesPerChunkHistory()))
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlStub("xmlstubsdumprecombine", "Recombine first-pass for page XML data dumps", self.chunkInfo.getPagesPerChunkHistory()))

		# NOTE that the chunkInfo thing passed here is irrelevant, these get generated from the stubs which are all done in one pass
		self.dumpItems.append(
			XmlDump("articles",
				"articlesdump",
				"<big><b>Articles, templates, image descriptions, and primary meta-pages.</b></big>",
				"This contains current versions of article content, and is the archive most mirror sites will probably want.", self._prefetch, self._spawn, self.chunkInfo.getPagesPerChunkHistory()))
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlDump("articles","articlesdumprecombine", "<big><b>Recombine articles, templates, image descriptions, and primary meta-pages.</b></big>","This contains current versions of article content, and is the archive most mirror sites will probably want.", self.chunkInfo.getPagesPerChunkHistory()))

		self.dumpItems.append(
			XmlDump("meta-current",
				"metacurrentdump",
				"All pages, current versions only.",
				"Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self._prefetch, self._spawn, self.chunkInfo.getPagesPerChunkHistory()))
			
		if (self.chunkInfo.chunksEnabled()):
			self.dumpItems.append(RecombineXmlDump("meta-current","metacurrentdumprecombine", "Recombine all pages, current versions only.","Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.chunkInfo.getPagesPerChunkHistory()))

		self.dumpItems.append(
			XmlLogging("Log events to all pages."))
			
		if self._hasFlaggedRevs:
			self.dumpItems.append(
				PublicTable( "flaggedpages", "flaggedpagestable","This contains a row for each flagged article, containing the stable revision ID, if the lastest edit was flagged, and how long edits have been pending." ))
			self.dumpItems.append(
				PublicTable( "flaggedrevs", "flaggedrevstable","This contains a row for each flagged revision, containing who flagged it, when it was flagged, reviewer comments, the flag values, and the quality tier those flags fall under." ))

		if not self._isBig:
			self.dumpItems.append(
				BigXmlDump("meta-history",
					"metahistorybz2dump",
					"All pages with complete page edit history (.bz2)",
					"These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
					"Suitable for archival and statistical use, most mirror sites won't want or need this.", self._prefetch, self._spawn, self.chunkInfo.getPagesPerChunkHistory()))
			if (self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory()):
				self.dumpItems.append(
					RecombineXmlDump("meta-history",
								   "metahistorybz2dumprecombine",
								   "Recombine all pages with complete edit history (.bz2)",
								   "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
								   "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.chunkInfo.getPagesPerChunkHistory()))
			self.dumpItems.append(
				XmlRecompressDump("meta-history",
					"metahistory7zdump",
					"All pages with complete edit history (.7z)",
					"These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
					"Suitable for archival and statistical use, most mirror sites won't want or need this.", self.chunkInfo.getPagesPerChunkHistory()))
			if (self.chunkInfo.chunksEnabled() and self.chunkInfo.recombineHistory()):
				self.dumpItems.append(
					RecombineXmlRecompressDump("meta-history",
								   "metahistory7zdumprecombine",
								   "Recombine all pages with complete edit history (.7z)",
								   "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
								   "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.chunkInfo.getPagesPerChunkHistory()))
		self.oldRunInfoRetrieved = self._getOldRunInfoFromFile()


				      
	# read in contents from dump run info file and stuff into dumpItems for later reference

	# sometimes need to get this info for an older run to check status of a file for
	# possible prefetch
	def _getDumpRunInfoFileName(self, date=None):
		if (date):
			return os.path.join(self.wiki.publicDir(), date, "dumpruninfo.txt")
		else:
			return os.path.join(self.wiki.publicDir(), self.date, "dumpruninfo.txt")

	def _getDumpRunInfoDirName(self, date=None):
		if (date):
			return os.path.join(self.wiki.publicDir(), date);
		else:
			return os.path.join(self.wiki.publicDir(), self.date);

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

	# format: name:%; updated:%; status:%
	def _getOldRunInfoFromLine(self, line):
		# get rid of leading/trailing/blanks
		line = line.strip(" ")
		line = line.replace("\n","")
		fields = line.split(';',3)
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
		self._setDumpItemRunInfo(dumpRunInfo)

	def _getOldRunInfoFromFile(self):
		# read the dump run info file in, if there is one, and get info about which dumps
		# have already been run and whether they were successful
		dumpRunInfoFileName = self._getDumpRunInfoFileName()
		try:
			infile = open(dumpRunInfoFileName,"r")
			for line in infile:
				self._getOldRunInfoFromLine(line)
			infile.close
			return True
		except:
			return False

	# write dump run info file 
	# (this file is rewritten with updates after each dumpItem completes)
				      
	def _reportDumpRunInfoLine(self, item):
		# even if the item has never been run we will at least have "waiting" in the status
		return "name:%s; status:%s; updated:%s" % (item.name(), item.status(), item.updated())

	def _reportDumpRunInfo(self, done=False):
		"""Put together a dump run info listing for this database, with all its component dumps."""
		runInfoLines = [self._reportDumpRunInfoLine(item) for item in self.dumpItems]
		runInfoLines.reverse()
		text = "\n".join(runInfoLines)
		text = text + "\n"
		return text

	def writeDumpRunInfoFile(self, text):
		directory = self._getDumpRunInfoDirName()
		dumpRunInfoFilename = self._getDumpRunInfoFileName()
		FileUtils.writeFile(directory, dumpRunInfoFilename, text, self.wiki.config.fileperms)

	def saveDumpRunInfoFile(self, done=False):
		"""Write out a simple text file with the status for this wiki's dump."""
		try:
			self.writeDumpRunInfoFile(self._reportDumpRunInfo(done))
		except:
			print "Couldn't save dump run info file. Continuing anyways"

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
		if job == "noop":
			return True
		print "No job of the name specified exists. Choose one of the following:"
		print "noop (runs no job but rewrites md5sums file and resets latest links"
		print "tables (includes all items below that end in 'table'"
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

	# see whether job needs previous jobs that have not completed successfully

	def jobDoneSuccessfully(self, job):
		for item in self.dumpItems:
			if (item.name() == job):
				if (item.status() == "done"):
					return True
				else:
					return False
		return False

	def checkJobDependencies(self, job):
		# dump of any pages meta history etc requires stubs.
		# recompress requires earlier bz2.
		if (job == "abstractsdumprecombine"):
			if (not self.jobDoneSuccessfully("abstractsdump")):
				return False
		if (job == "xmlstubsdumprecombine"):
			if (not self.jobDoneSuccessfully("xmlstubsdump")):
				return False
		if (job == "articlesdumprecombine"):
			if (not self.jobDoneSuccessfully("articlesdump")):
				return False
		if (job == "metacurrentdumprecombine"):
			if (not self.jobDoneSuccessfully("metacurrentdump")):
				return False
		if (job == "metahistory7zdumprecombine"):
			if (not self.jobDoneSuccessfully("metahistory7zdump")):
				return False
		if (job == "metahistorybz2dumprecombine"):
			if (not self.jobDoneSuccessfully("metahistorybz2dump")):
				return False
		if (job == "metahistory7zdump"):
			if (not self.jobDoneSuccessfully("xmlstubsdump") or not self.jobDoneSuccessfully("metahistorybz2dump")):
				return False
		if ((job == "metahistorybz2dump") or (job == "metacurrentdump") or (job == "articlesdump")):
			if (not self.jobDoneSuccessfully("xmlstubsdump")):
				return False
		return True
				      
class Checksummer(object):
	def __init__(self,wiki,dumpDir):
		self.wiki = wiki
		self.dumpDir = dumpDir
		self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
	
	def getChecksumFileNameBasename(self):
		return ("md5sums.txt")

	def getChecksumFileName(self):
		return (self.dumpDir.publicPath(self.getChecksumFileNameBasename()))

	def getChecksumFileNameTmp(self):
		return (self.dumpDir.publicPath(self.getChecksumFileNameBasename() + "." + self.timestamp + ".tmp"))

	def prepareChecksums(self):
		"""Create a temporary md5 checksum file.
		Call this at the start of the dump run, and move the file
		into the final location at the completion of the dump run."""
		checksumFileName = self.getChecksumFileNameTmp()
		output = file(checksumFileName, "w")

	def md5File(self, filename):
		summer = md5.new()
		infile = file(filename, "rb")
		bufsize = 4192 * 32
		buffer = infile.read(bufsize)
		while buffer:
			summer.update(buffer)
			buffer = infile.read(bufsize)
		infile.close()
		return summer.hexdigest()

	def md5FileLine(self, filename):
		return "%s  %s\n" % (self.md5File(filename), os.path.basename(filename))

	def saveChecksum(self, file, output, runner):
		runner.debug("Checksumming %s" % file)
		path = self.dumpDir.publicPath(file)
		if os.path.exists(path):
			checksum = self.md5FileLine(path)
			output.write(checksum)

	def checksum(self, filename, runner):
		"""Run checksum for an output file, and append to the list."""
		checksumFileName = self.getChecksumFileNameTmp()
		output = file(checksumFileName, "a")
		self.saveChecksum(filename, output, runner)
		output.close()

	def moveMd5FileIntoPlace(self):
		tmpFileName = self.getChecksumFileNameTmp()
		realFileName = self.getChecksumFileName()
		os.rename(tmpFileName, realFileName)

class DumpDir(object):
	def __init__(self, wiki, dbName, date):
		self._wiki = wiki
		self._dbName = dbName
		self._date = date

	def buildDir(self, base, version):
		return join(base, version)

	def buildPath(self, base, version, filename):
		return join(base, version, "%s-%s-%s" % (self._dbName, version, filename))

	def privatePath(self, filename):
		"""Take a given filename in the private dump dir for the selected database."""
		return self.buildPath(self._wiki.privateDir(), self._date, filename)

	def publicPath(self, filename):
		"""Take a given filename in the public dump dir for the selected database.
		If this database is marked as private, will use the private dir instead.
		"""
		return self.buildPath(self._wiki.publicDir(), self._date, filename)

	def latestDir(self):
		return self.buildDir(self._wiki.publicDir(), "latest")

	def latestPath(self, filename):
		return self.buildPath(self._wiki.publicDir(), "latest", filename)

	def webPath(self, filename):
		return self.buildPath(self._wiki.webDir(), self._date, filename)
				      
# everything that has to do with reporting the status of a piece
# of a dump is collected here
class Status(object):
	def __init__(self, wiki, dumpDir, date, items, checksums, noticeFile = None, errorCallback=None):
		self.wiki = wiki
		self.config = wiki.config
		self.dbName = wiki.dbName
		self.dumpDir = dumpDir
		self.items = items
		self.checksums = checksums
		self.date = date
		# this is just a glorified name for "give me a logging facility"
		self.noticeFile = noticeFile
		self.errorCallback = errorCallback
		self.failCount = 0

	def saveStatusSummaryAndDetail(self, done=False):
		"""Write out an HTML file with the status for this wiki's dump 
		and links to completed files, as well as a summary status in a separate file."""
		try:
			# Comprehensive report goes here
			self.wiki.writePerDumpIndex(self.reportDatabaseStatusDetailed(done))

			# Short line for report extraction goes here
			self.wiki.writeStatus(self.reportDatabaseStatusSummary(done))
		except:
			message = "Couldn't update status files. Continuing anyways"
			if self.errorCallback:
				self.errorCallback(message)
			else:
				print(message)

	def updateStatusFiles(self, done=False):
		self.saveStatusSummaryAndDetail(done)

	def reportDatabaseStatusSummary(self, done=False):
		"""Put together a brief status summary and link for the current database."""
		status = self.reportStatusSummaryLine(done)
		html = self.wiki.reportStatusLine(status)

		activeItems = [x for x in self.items if x.status() == "in-progress"]
		if activeItems:
			return html + "<ul>" + "\n".join([self.reportItem(x) for x in activeItems]) + "</ul>"
		else:
			return html

	def reportDatabaseStatusDetailed(self, done=False):
		"""Put together a status page for this database, with all its component dumps."""
		self.noticeFile.refreshNotice()
		statusItems = [self.reportItem(item) for item in self.items]
		statusItems.reverse()
		html = "\n".join(statusItems)
		return self.config.readTemplate("report.html") % {
			"db": self.dbName,
			"date": self.date,
			"notice": self.noticeFile.notice,
			"status": self.reportStatusSummaryLine(done),
			"previous": self.reportPreviousDump(done),
			"items": html,
			"checksum": self.dumpDir.webPath(self.checksums.getChecksumFileNameBasename()),
			"index": self.config.index}

	def reportFailure(self):
		if self.config.adminMail:
			subject = "Dump failure for " + self.dbName
			message = self.config.readTemplate("errormail.txt") % {
				"db": self.dbName,
				"date": self.date,
				"time": TimeUtils.prettyTime(),
				"url": "/".join((self.config.webRoot, self.dbName, self.date, ''))}
			config.mail(subject, message)

	def reportPreviousDump(self, done):
		"""Produce a link to the previous dump, if any"""
		# get the list of dumps for this wiki in order, find me in the list, find the one prev to me.
		# why? we might be rerunning a job from an older dumps. we might have two
		# runs going at once (think en pedia, one finishing up the history, another
		# starting at the beginning to get the new abstracts and stubs).
		try:
			dumpsInOrder = self.wiki.latestDump(all=True)
			meIndex = dumpsInOrder.index(self.date)
			# don't wrap around to the newest dump in the list!
			if (meIndex > 0):
				rawDate = dumpsInOrder[meIndex-1]
			else:
				raise(ValueException)
		except:
			return "No prior dumps of this database stored."
		prettyDate = TimeUtils.prettyDate(rawDate)
		if done:
			prefix = ""
			message = "Last dumped on"
		else:
			prefix = "This dump is in progress; see also the "
			message = "previous dump from"
		return "%s<a href=\"../%s/\">%s %s</a>" % (prefix, rawDate, message, prettyDate)
				      
	def reportStatusSummaryLine(self, done=False):
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

	def reportItem(self, item):
		"""Return an HTML fragment with info on the progress of this item."""
		html = "<li class='%s'><span class='updates'>%s</span> <span class='status'>%s</span> <span class='title'>%s</span>" % (item.status(), item.updated(), item.status(), item.description())
		if item.progress:
			html += "<div class='progress'>%s</div>\n" % item.progress
		files = item.listFiles(self)
		if files:
			listItems = [self.reportFile(file, item.status()) for file in files]
			html += "<ul>"
			detail = item.detail()
			if detail:
				html += "<li class='detail'>%s</li>\n" % detail
			html += "\n".join(listItems)
			html += "</ul>"
		html += "</li>"
		return html

	# this is a per-dump-item report (well per file generated by the item)
	# Report on the file size & item status of the current output and output a link if we are done
	def reportFile(self, file, itemStatus):
		filepath = self.dumpDir.publicPath(file)
		if itemStatus == "in-progress" and exists (filepath):
			size = FileUtils.prettySize(getsize(filepath))
			return "<li class='file'>%s %s (written) </li>" % (file, size)
		elif itemStatus == "done" and exists(filepath):
			size = FileUtils.prettySize(getsize(filepath))
			webpath = self.dumpDir.webPath(file)
			return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpath, file, size)
		else:
			return "<li class='missing'>%s</li>" % file

class NoticeFile(object):
	def __init__(self, wiki, date, notice):
		self.wiki = wiki
		self.date = date
		self.notice = notice

		noticeFile = self.getNoticeFilename()
		# delnotice.  toss any existing file 
		if self.notice == False:
			if exists(noticeFile):
				os.remove(noticeFile)
			self.notice = ""
		# addnotice, stuff notice in a file for other jobs etc
		elif self.notice != "":
			noticeDir = self.getNoticeDir()
			FileUtils.writeFile(noticeDir, noticeFile, self.notice, self.wiki.config.fileperms)
		# default case. if there is a file get the contents, otherwise
		# we have empty contents, all good
		else:
			if exists(noticeFile):
				self.notice = FileUtils.readFile(noticeFile)
		
	def getNoticeFilename(self):
		return os.path.join(self.wiki.publicDir(), self.date, "notice.txt")

	def getNoticeDir(self):
		return os.path.join(self.wiki.publicDir(), self.date);

	def refreshNotice(self):
		# if the notice file has changed or gone away, we comply.
		noticeFile = self.getNoticeFilename()
		if exists(noticeFile):
			self.notice = FileUtils.readFile(noticeFile)
		else:
			self.notice = ""

class Runner(object):

	def __init__(self, wiki, date=None, prefetch=True, spawn=True, job=None, restart=False, notice="", dryrun = False, loggingEnabled=False):
		self.wiki = wiki
		self.config = wiki.config
		self.dbName = wiki.dbName
		self.prefetch = prefetch
		self.spawn = spawn
		self.chunkInfo = Chunk(wiki, self.dbName, self.logAndPrint)
		self.restart = restart
		self.loggingEnabled = loggingEnabled
		self.htmlNoticeFile = None
		self.log = None
		self.dryrun = dryrun

		if date:
			# Override, continuing a past dump?
			self.date = date
		else:
			self.date = TimeUtils.today()
		wiki.setDate(self.date)

		self.lastFailed = False

		self.jobRequested = job
		self.dbServerInfo = DbServerInfo(self.wiki, self.dbName, self.logAndPrint)

		self.dumpDir = DumpDir(self.wiki, self.dbName, self.date)

		# this must come after the dumpdir setup so we know which directory we are in 
		# for the log file.
		if (loggingEnabled and not self.dryrun):
			self.logFileName = self.dumpDir.publicPath(config.logFile)
			self.makeDir(join(self.wiki.publicDir(), self.date))
			self.log = Logger(self.logFileName)
			thread.start_new_thread(self.logQueueReader,(self.log,))

		# have to handle the notice file here instead of main, it goes in the per-run directory
		if not dryrun:
			self.htmlNoticeFile = NoticeFile(self.wiki, self.date, notice)

		if not dryrun:
			self.checksums = Checksummer(self.wiki, self.dumpDir)

		# some or all of these dumpItems will be marked to run
		self.dumpItemList = DumpItemList(self.wiki, self.prefetch, self.spawn, self.date, self.chunkInfo);

		if not self.dryrun:
			self.status = Status(self.wiki, self.dumpDir, self.date, self.dumpItemList.dumpItems, self.checksums, self.htmlNoticeFile, self.logAndPrint)

	def logQueueReader(self,log):
		if not log:
			return
		done = False
		while not done:
			done = log.doJobOnLogQueue()
		
	def logAndPrint(self, message):
		if hasattr(self,'log') and self.log and not self.dryrun:
			self.log.addToLogQueue("%s\n" % message)
		print message

	def forceNormalOption(self):
		if self.config.forceNormal:
			return "--force-normal"
		else:
			return ""

	def remove(self, filename):
		if not self.dryrun:
			os.remove(filename)

	# returns 0 on success, 1 on error
	def saveTable(self, table, outfile):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		commands = self.dbServerInfo.buildSqlDumpCommand(table, self.config.gzip)
		return self.saveCommand(commands, outfile)

	def saveSql(self, query, outfile):
		"""Pass some SQL commands to the server for this DB and save output to a gzipped file."""
		command = self.dbServerInfo.buildSqlCommand(query, self.config.gzip)
		return self.saveCommand(command, outfile)

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
	#			raise BackupError(errorString)
			return 1

	def debug(self, stuff):
		self.logAndPrint("%s: %s %s" % (TimeUtils.prettyTime(), self.dbName, stuff))
#		print "%s: %s %s" % (MiscUtils.prettyTime(), self.dbName, stuff)

	def runHandleFailure(self):
		if self.status.failCount < 1:
			# Email the site administrator just once per database
			self.status.reportFailure()
			self.status.failCount += 1
			self.lastFailed = True

	def runUpdateItemFileInfo(self, item):
		for f in item.listFiles(self):
			print f
			if exists(self.dumpDir.publicPath(f)):
				# why would the file not exist? because we changed chunk numbers in the
				# middle of a run, and now we list more files for the next stage than there
				# were for earlier ones
				self.saveSymlink(f)
				self.saveFeed(f)
				self.checksums.checksum(f, self)

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
				if (not reply in "y", "Y"):
					raise RuntimeError( "No run information available for previous dump, exiting" )
			if (not self.wiki.existsPerDumpIndex()):
				# AFAWK this is a new run (not updating or rerunning an old run), 
				# so we should see about cleaning up old dumps
				self.showRunnerState("Cleaning up old dumps for %s" % self.dbName)
				self.cleanOldDumps()

			if (not self.dumpItemList.markDumpsToRun(self.jobRequested)):
			# probably no such job
				raise RuntimeError( "No job marked to run, exiting" )
			# job has dependent steps that weren't already run
			if (not self.dumpItemList.checkJobDependencies(self.jobRequested)):
				raise RuntimeError( "Job dependencies not run beforehand, exiting" )
			if (restart):
				# mark all the following jobs to run as well 
				self.dumpItemList.markFollowingJobsToRun()

		if not self.dryrun:
			self.makeDir(join(self.wiki.publicDir(), self.date))
			self.makeDir(join(self.wiki.privateDir(), self.date))

		if (self.restart):
			self.logAndPrint("Preparing for restart from job %s of %s" % (self.jobRequested, self.dbName))
		elif (self.jobRequested):
			self.logAndPrint("Preparing for job %s of %s" % (self.jobRequested, self.dbName))
		else:
			self.showRunnerState("Cleaning up old dumps for %s" % self.dbName)
			self.cleanOldDumps()
			self.showRunnerState("Starting backup of %s" % self.dbName)

		files = self.listFilesFor(self.dumpItemList.dumpItems)

		if (self.jobRequested):
			if not self.dryrun:
				self.checksums.prepareChecksums()

			for item in self.dumpItemList.dumpItems:
				if (item.toBeRun()):
					item.start(self)
					if not self.dryrun:
						self.status.updateStatusFiles()
						self.dumpItemList.saveDumpRunInfoFile()
					try:
						item.dump(self)
					except Exception, ex:
						self.debug("*** exception! " + str(ex))
						item.setStatus("failed")
					if item.status() == "failed" and not self.dryrun:
						self.runHandleFailure()
					else:
						self.lastFailed = False
				# this ensures that, previous run or new one, the old or new md5sums go to the file
				if item.status() == "done" and not self.dryrun:
					self.runUpdateItemFileInfo(item)

			if not self.dryrun:
				if (self.dumpItemList.allPossibleJobsDone()):
					self.status.updateStatusFiles("done")
				else:
					self.status.updateStatusFiles("partialdone")
				self.dumpItemList.saveDumpRunInfoFile()

				# if any job succeeds we might as well make the sym link
				if (self.status.failCount < 1):
					self.completeDump(files)
											
			if (self.restart):
				self.showRunnerState("Completed run restarting from job %s for %s" % (self.jobRequested, self.dbName))
			else:
				self.showRunnerState("Completed job %s for %s" % (self.jobRequested, self.dbName))

		else:
			if not self.dryrun:
				self.checksums.prepareChecksums()

			for item in self.dumpItemList.dumpItems:
				item.start(self)
				if not self.dryrun:
					self.status.updateStatusFiles()
					self.dumpItemList.saveDumpRunInfoFile()
				try:
					item.dump(self)
				except Exception, ex:
					self.debug("*** exception! " + str(ex))
					item.setStatus("failed")
				if item.status() == "failed" and not self.dryrun:
					self.runHandleFailure()
				else:
					if not self.dryrun:
						self.runUpdateItemFileInfo(item)
					self.lastFailed = False

			if not self.dryrun:
				self.status.updateStatusFiles("done")
				self.dumpItemList.saveDumpRunInfoFile()
				if self.status.failCount < 1:
					self.completeDump(files)
											
			self.showRunnerStateComplete()

	def cleanOldDumps(self):
		old = self.wiki.dumpDirs()
		if old:
			if old[-1] == self.date:
				# If we're re-running today's (or jobs from a given day's) dump, don't count it as one
				# of the old dumps to keep... or delete it halfway through!
				old = old[:-1]
			if self.config.keep > 0:
				# Keep the last few
				old = old[:-(self.config.keep)]
		if old:
			for dump in old:
				self.showRunnerState("Purging old dump %s for %s" % (dump, self.dbName))
				if not self.dryrun:
					base = os.path.join(self.wiki.publicDir(), dump)
					shutil.rmtree("%s" % base)
		else:
			self.showRunnerState("No old dumps to purge.")

	def listFilesFor(self, items):
		files = []
		for item in items:
			for file in item.listFiles(self):
				files.append(file)
		return files


	def lockFileName(self):
		return self.dumpDir.publicPath("lock")

	def doneFileName(self):
		return self.dumpDir.publicPath("done")

	def lock(self):
		self.showRunnerState("Creating lock file.")
		lockfile = self.lockFileName()
		donefile = self.doneFileName()
		if exists(lockfile):
			raise BackupError("Lock file %s already exists" % lockfile)
		if exists(donefile):
			self.showRunnerState("Removing completion marker %s" % donefile)
			os.remove(donefile)
		try:
			os.remove(lockfile)
		except:
			# failure? let it die
			pass
				      
	def unlock(self):
		self.showRunnerState("Marking complete.")

	def showRunnerState(self, message):
		self.debug(message)

	def showRunnerStateComplete(self):
		self.debug("SUCCESS: done.")

	def completeDump(self, files):
		# note that it's possible for links in "latest" to point to 
		# files from different runs, in which case the md5sums file
		# will have accurate checksums for the run for which it was
		# produced, but not the other files. FIXME
		self.checksums.moveMd5FileIntoPlace()
		self.saveSymlink(self.checksums.getChecksumFileNameBasename())

	def makeDir(self, dir):
		if exists(dir):
			self.debug("Checkdir dir %s ..." % dir)
		else:
			self.debug("Creating %s ..." % dir)
			os.makedirs(dir)

	def saveSymlink(self, file):
		self.makeDir(join(self.wiki.publicDir(), 'latest'))
		real = self.dumpDir.publicPath(file)
		link = self.dumpDir.latestPath(file)
		if exists(link) or os.path.islink(link):
			if os.path.islink(link):
				realfile = os.readlink(link)
				# format of these links should be...  ../20110228/elwikidb-20110228-templatelinks.sql.gz
				rellinkpattern = re.compile('^\.\./(20[0-9]+)/');
				dateinlink = rellinkpattern.search(realfile)
				if (dateinlink):
					dateoflinkedfile = dateinlink.group(1)
					dateinterval = int(self.date) - int(dateoflinkedfile)
				else:
					dateinterval = 0
				# no file or it's older than ours... *then* remove the link
				if not exists(os.path.realpath(link)) or dateinterval > 0:
					self.debug("Removing old symlink %s" % link)
					os.remove(link)
			else:
				self.logAndPrint("What the hell dude, %s is not a symlink" % link)
				raise BackupError("What the hell dude, %s is not a symlink" % link)
		relative = FileUtils.relativePath(real, dirname(link))
		# if we removed the link cause it's obsolete, make the new one
		if exists(real) and not exists(link):
			self.debug("Adding symlink %s -> %s" % (link, relative))
			os.symlink(relative, link)
			
	def saveFeed(self, file):
		self.makeDir(join(self.wiki.publicDir(), 'latest'))
		filePath = self.dumpDir.webPath(file)
		fileName = os.path.basename(filePath)
		webPath = os.path.dirname(filePath)
		rssText = self.config.readTemplate("feed.xml") % {
			"chantitle": file,
			"chanlink": webPath,
			"chandesc": "Wikimedia dump updates for %s" % self.dbName,
			"title": webPath,
			"link": webPath,
			"description": xmlEscape("<a href=\"%s\">%s</a>" % (filePath, fileName)),
			"date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())}
		directory = self.dumpDir.latestDir()
		rssPath = self.dumpDir.latestPath(file + "-rss.xml")
		FileUtils.writeFile(directory, rssPath, rssText, self.config.fileperms)

class Dump(object):
	def __init__(self, name, desc):
		self._desc = desc
		self.progress = ""
		self.runInfo = RunInfo(name,"waiting","")

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

	def listFiles(self, runner):
		"""Return a list of filenames which should be exported and checksummed"""
		return []

	def start(self, runner):
		"""Set the 'in progress' flag so we can output status."""
		self.setStatus("in-progress")
				      
	def dump(self, runner):
		"""Attempt to run the operation, updating progress/status info."""
		try:
			self.run(runner)
		except Exception, ex:
			self.setStatus("failed")
			raise ex
		self.setStatus("done")

	def run(self, runner):
		"""Actually do something!"""
		pass

	def progressCallback(self, runner, line=""):
		"""Receive a status line from a shellout and update the status files."""
		# pass through...
		if (line):
			if (runner.log):
				runner.log.addToLogQueue(line)
			sys.stderr.write(line)
		self.progress = line.strip()
		runner.status.updateStatusFiles()
		runner.dumpItemList.saveDumpRunInfoFile()

	def timeToWait(self):
		# we use wait this many secs for a command to complete that
		# doesn't produce output
		return 5

	def waitAlarmHandler(self, signum, frame):
		pass

	def buildRecombineCommandString(self, runner, files, outputFileBasename, compressionCommand, uncompressionCommand, endHeaderMarker="</siteinfo>"):
#		outputFilename = self.buildOutputFilename(runner, outputFileBasename)
                outputFilename = runner.dumpDir.publicPath(outputFileBasename)
		chunkNum = 0
		recombines = []
		head = runner.config.head
		tail = runner.config.tail
		grep = runner.config.grep

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
		for f in files:
			f = MiscUtils.shellEscape(f)

		for f in files:
			f = runner.dumpDir.publicPath(f)
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
			if (p.output()):
				(headerEndNum, junk) = p.output().split(":",1)
				# get headerEndNum
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

	def cleanupOldFiles(self, runner, outputFileBasename):
		outputFilename = self.buildOutputFilename(runner, outputFileBasename)
		if exists(outputFilename):
			runner.remove(outputFilename)

	def buildOutputFilename(self, runner, outputFileBasename):
		return outputFilename

class PublicTable(Dump):
	"""Dump of a table using MySQL's mysqldump utility."""
				      
	def __init__(self, table, name, desc):
		Dump.__init__(self, name, desc)
		self._table = table

	def _file(self):
		return self._table + ".sql.gz"

	def _path(self, runner):
		return runner.dumpDir.publicPath(self._file())

	def run(self, runner):
		retries = 0
		# try this initially and see how it goes
		maxretries = 3 
		error = runner.saveTable(self._table, self._path(runner)) 
		while (error and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			error = runner.saveTable(self._table, self._path(runner)) 
		if (error):
			raise BackupError("error dumping table %s" % self._table)

	def listFiles(self, runner):
		return [self._file()]

class PrivateTable(PublicTable):
	"""Hidden table dumps for private data."""

	def description(self):
		return self._desc + " (private)"

	def _path(self, runner):
		return runner.dumpDir.privatePath(self._file())

	def listFiles(self, runner):
		"""Private table won't have public files to list."""
		return []


class XmlStub(Dump):
	"""Create lightweight skeleton dumps, minus bulk text.
	A second pass will import text from prior dumps or the database to make
	full files for the public."""
				      
	def __init__(self, name, desc, chunks = False):
		Dump.__init__(self, name, desc)
		self._chunks = chunks

	def detail(self):
		return "These files contain no page text, only revision metadata."

	def listFiles(self, runner, unnumbered=False):
		if (self._chunks) and not unnumbered:
		        files = []
			for i in range(1, len(self._chunks) + 1):
				files.append("stub-meta-history%s.xml.gz" % i)
				files.append("stub-meta-current%s.xml.gz" % i)
				files.append("stub-articles%s.xml.gz" % i)
			return files
		else:
			return ["stub-meta-history.xml.gz",
				"stub-meta-current.xml.gz",
				"stub-articles.xml.gz"]

	def buildCommand(self, runner, chunk = 0):
		history = self.buildHistoryOutputFilename(runner, chunk)
		current = self.buildCurrentOutputFilename(runner, chunk)
		articles = self.buildArticlesOutputFilename(runner, chunk)

		command = [ "%s" % runner.config.php,
			    "-q", "%s/maintenance/dumpBackup.php" % runner.config.wikiDir,
			    "--wiki=%s" % runner.dbName,
			    "--full", "--stub", "--report=10000",
			    "%s" % runner.forceNormalOption(),
			    "--output=gzip:%s" % history,
			    "--output=gzip:%s" % current,
			    "--filter=latest", "--output=gzip:%s" % articles,
			    "--filter=latest", "--filter=notalk", "--filter=namespace:!NS_USER" ]
		if (chunk):
			# set up start end end pageids for this piece
			# note there is no page id 0 I guess. so we start with 1
			# start = runner.pagesPerChunk()*(chunk-1) + 1
			start = sum([ self._chunks[i] for i in range(0,chunk-1)]) + 1
			startopt = "--start=%s" % start
			# if we are on the last chunk, we should get up to the last pageid, 
			# whatever that is. 
			command.append(startopt)
			if chunk < len(self._chunks):
				# end = start + runner.pagesPerChunk()
				end = sum([ self._chunks[i] for i in range(0,chunk)]) +1
				endopt = "--end=%s" % end
				command.append(endopt)

		pipeline = [ command ]
		series = [ pipeline ]
		return(series)

	def cleanupOldFiles(self, runner, chunk = 0):
		fileList = self.buildOutputFilenames(runner, chunk)
		for filename in fileList:
			 if exists(filename):
				runner.remove(filename)

	def buildHistoryOutputFilename(self, runner, chunk = 0):
		if (chunk):
			chunkinfo = "%s" % chunk
		else:
			 chunkinfo = ""
		history = runner.dumpDir.publicPath("stub-meta-history" + chunkinfo + ".xml.gz")
		return history

	def buildCurrentOutputFilename(self, runner, chunk = 0):
		if (chunk):
			chunkinfo = "%s" % chunk
		else:
			 chunkinfo = ""
		current = runner.dumpDir.publicPath("stub-meta-current" + chunkinfo + ".xml.gz")
		return current

	def buildArticlesOutputFilename(self, runner, chunk = 0):
		if (chunk):
			chunkinfo = "%s" % chunk
		else:
			 chunkinfo = ""
		articles = runner.dumpDir.publicPath("stub-articles" + chunkinfo + ".xml.gz")
		return articles

	def buildOutputFilenames(self, runner, chunk = 0):
		history = self.buildHistoryOutputFilename(runner, chunk)
		current = self.buildCurrentOutputFilename(runner, chunk)
		articles = self.buildArticlesOutputFilename(runner, chunk)
		return([ history, current, articles ])

	def run(self, runner):
		commands = []
		if self._chunks:
			for i in range(1, len(self._chunks)+1):
				self.cleanupOldFiles(runner,i)
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			self.cleanupOldFiles(runner)
			series = self.buildCommand(runner)
			commands.append(series)
		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if (error):
			raise BackupError("error producing stub files" % self._subset)

class RecombineXmlStub(XmlStub):
	def __init__(self, name, desc, chunks):
		XmlStub.__init__(self, name, desc, chunks)
		# this is here only so that a callback can capture output from some commands
		# related to recombining files if we did parallel runs of the recompression
		self._output = None

	# oh crap we need to be able to produce a list of output files, what else? 
	def listFiles(self, runner):
		return(XmlStub.listFiles(self, runner, unnumbered=True))

	def run(self, runner):
		error=0
		if (self._chunks):
			files = XmlStub.listFiles(self,runner)
			outputFileList = self.listFiles(runner)
			for outputFile in outputFileList:
				inputFiles = []
				for inFile in files:
					(base, rest) = inFile.split('.',1)
					base = re.sub("\d+$", "", base)
					if base + "." + rest == outputFile:
						inputFiles.append(inFile)
				if not len(inputFiles):
					self.setStatus("failed")
					raise BackupError("No input files for %s found" % self.name)
				compressionCommand = runner.config.gzip
				compressionCommand = "%s > " % runner.config.gzip
				uncompressionCommand = [ "%s" % runner.config.gzip, "-dc" ] 
				recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFile, compressionCommand, uncompressionCommand )
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
		self._chunks = chunks

	def detail(self):
		return "This contains the log of actions performed on pages."

	def listFiles(self, runner):
		return ["pages-logging.xml.gz"]

	def cleanupOldFiles(self, runner):
		logging = self.buildOutputFilename(runner)
		if exists(logging):
			runner.remove(logging)

	def buildOutputFilename(self, runner):
		logging = runner.dumpDir.publicPath("pages-logging.xml.gz")
		return logging

	def run(self, runner):
		self.cleanupOldFiles(runner)
		logging = self.buildOutputFilename(runner)
		command = [ "%s" % runner.config.php,
			    "-q",  "%s/maintenance/dumpBackup.php" % runner.config.wikiDir,
			    "--wiki=%s" % runner.dbName,
			    "--logs", "--report=10000",
			    "%s" % runner.forceNormalOption(),
			    "--output=gzip:%s" % logging ]
		pipeline = [ command ]
		series = [ pipeline ]
		error = runner.runCommand([ series ], callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if (error):
			raise BackupError("error dimping log files")

class XmlDump(Dump):
	"""Primary XML dumps, one section at a time."""
	def __init__(self, subset, name, desc, detail, prefetch, spawn, chunks = False):
		Dump.__init__(self, name, desc)
		self._subset = subset
		self._detail = detail
		self._prefetch = prefetch
		self._spawn = spawn
		self._chunks = chunks
		self._pageID = {}

	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return self._detail

	def _file(self, ext, chunk=0):
		if (chunk):
			return "pages-" + self._subset + ("%s.xml." % chunk) + ext
		else:
			return "pages-" + self._subset + ".xml." + ext

	def _path(self, runner, ext, chunk = 0):
		return runner.dumpDir.publicPath(self._file(ext, chunk))

	def run(self, runner):
		commands = []
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			series = self.buildCommand(runner)
			commands.append(series)
		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)

		checkforbz2footer = "%s" % runner.config.checkforbz2footer
		if exists(checkforbz2footer):
			# check to see if any of the output files are truncated
			files = []
			if (self._chunks):
				for i in range(1, len(self._chunks)+1):
					files.append( self._path(runner, 'bz2', i ) )
			files.append( self._path(runner, 'bz2', i ) )

			for f in files:
				pipeline = []
				pipeline.append([ checkforbz2footer, f ])
				p = CommandPipeline(pipeline, quiet=True)
				p.runPipelineAndGetOutput()
				if not p.exitedSuccessfully():
					runner.logAndPrint("file %s is truncated, moving out of the way" %f )
					os.rename( f,  f + ".truncated" )
					error = 1
		if (error):
			raise BackupError("error producing xml bz2 file(s) %s" % self._subset)

	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--current"

	def buildFilters(self, runner, chunk = 0):
		"""Construct the output filter options for dumpTextPass.php"""
		xmlbz2 = self._path(runner, "bz2", chunk)
		if runner.config.bzip2[-6:] == "dbzip2":
			bz2mode = "dbzip2"
		else:
			bz2mode = "bzip2"
		return "--output=%s:%s" % (bz2mode, xmlbz2)

	def buildCommand(self, runner, chunk=0):
		"""Build the command line for the dump, minus output and filter options"""

		if (chunk):
			chunkinfo = "%s" % chunk
		else:
			chunkinfo =""

		# Page and revision data pulled from this skeleton dump...
		stub = "stub-%s" % self._subset
		stub = stub + "%s.xml.gz" % chunkinfo
		stub = runner.dumpDir.publicPath(stub),
		stubOption = "--stub=gzip:%s" % stub

		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		sources = []
		possibleSources = None
		if self._prefetch:
			possibleSources = self._findPreviousDump(runner, chunk)
			# if we have a list of more than one then we need to check existence for each and put them together in a string
			if possibleSources:
				for sourceFile in possibleSources:
					if exists(sourceFile):
						sources.append(sourceFile)
		if (len(sources) > 0):
			source = "bzip2:%s" % (";".join(sources) )
			runner.showRunnerState("... building %s %s XML dump, with text prefetch from %s..." % (self._subset, chunkinfo, source))
			prefetch = "--prefetch=%s" % (source)
		else:
			runner.showRunnerState("... building %s %s XML dump, no text prefetch..." % (self._subset, chunkinfo))
			prefetch = None

		if self._spawn:
			spawn = "--spawn=%s" % (runner.config.php)
		else:
			spawn = None

		dumpCommand = [ "%s" % runner.config.php,
				"-q", "%s/maintenance/dumpTextPass.php" % runner.config.wikiDir,
				"--wiki=%s" % runner.dbName,
				"%s" % stubOption,
				"%s" % prefetch,
				"%s" % runner.forceNormalOption(),
				"--report=1000",
				"%s" % spawn ]
		command = dumpCommand
		filters = self.buildFilters(runner, chunk)
		eta = self.buildEta(runner)
		command.extend([ filters, eta ])
		pipeline = [ command ]
		series = [ pipeline ]
		return series

	# given filename, (assume bz2 compression) dig out the first page id in that file
	def findFirstPageIDInFile(self, runner, fileName):
		if (fileName in self._pageID):
			return self._pageID[fileName]
		pageID = None
		pipeline = []
		uncompressionCommand = [ "%s" % runner.config.bzip2, "-dc", fileName ]
		pipeline.append(uncompressionCommand)
		# warning: we figure any header (<siteinfo>...</siteinfo>) is going to be less than 2000 lines!
		head = runner.config.head
		headEsc = MiscUtils.shellEscape(head)
		pipeline.append([ head, "-2000"])
		# without shell
		p = CommandPipeline(pipeline, quiet=True)
		p.runPipelineAndGetOutput()
		if (p.output()):
			pageData = p.output()
			titleAndIDPattern = re.compile('<title>(?P<title>.+?)</title>\s*' + '<id>(?P<pageid>\d+?)</id>')
			result = titleAndIDPattern.search(pageData)
			if (result):
				pageID = result.group('pageid')
		self._pageID[fileName] = pageID
		return(pageID)


	def filenameHasChunk(self, filename, ext):
		fileNamePattern = re.compile('.*pages-' + self._subset + '[0-9]+.xml.' + ext +'$')
		if (fileNamePattern.match(filename)):
			return True
		else:
			return False

	# taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
	def sort_nicely(self, l): 
		""" Sort the given list in the way that humans expect. 
		""" 
		convert = lambda text: int(text) if text.isdigit() else text 
		alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
		l.sort( key=alphanum_key ) 

		
	# this finds the content file or files from the first previous successful dump
	# to be used as input ("prefetch") for this run.
	def _findPreviousDump(self, runner, chunk = 0):
		"""The previously-linked previous successful dump."""
		bzfile = self._file("bz2")
		if (chunk):
			startPageID = sum([ self._chunks[i] for i in range(0,chunk-1)]) + 1
			if (len(self._chunks) > chunk):
				endPageID = sum([ self._chunks[i] for i in range(0,chunk)])
			else:
				endPageID = None
			# we will look for the first chunk file, if it's there and the
			# status of the job is ok then we will get the rest of the info
			bzfileChunk = self._file("bz2", 1)
			bzfileGlob = self._file("bz2", '[1-9]*')
			currentChunk = realpath(runner.dumpDir.publicPath(bzfile))
		current = realpath(runner.dumpDir.publicPath(bzfile))
		dumps = runner.wiki.dumpDirs()
		dumps.sort()
		dumps.reverse()
		for date in dumps:
			base = runner.wiki.publicDir()
			# first see if a "chunk" file is there, if not we will accept 
			# using the the single file dump although it will be slower
			possibles = []
			oldChunk = None
			# scan all the existing chunk files and get the first page ID from each
			if (chunk):
				oldChunk = runner.dumpDir.buildPath(base, date, bzfileChunk)
				oldGlob = runner.dumpDir.buildPath(base, date, bzfileGlob)
				pageIDs = []
				bzfileChunks = glob.glob(oldGlob)
				self.sort_nicely(bzfileChunks)
				if (bzfileChunks):
					for fileName in bzfileChunks:
						pageID = self.findFirstPageIDInFile(runner, fileName )
						if (pageID):
							pageIDs.append(pageID)

			old = runner.dumpDir.buildPath(base, date, bzfile)
			if (oldChunk):
				if exists(oldChunk):
					possibles.append(oldChunk)
			if (old):
				if exists(old):
					possibles.append(old)

			for possible in possibles:
				if exists(possible):
					size = getsize(possible)
					if size < 70000:
						runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, possible))
						continue
					if realpath(old) == current:
						runner.debug("skipping current dump for prefetch %s" % possible)
						continue
					if not self.statusOfOldDumpIsDone(runner, date):
						runner.debug("skipping incomplete or failed dump for prefetch %s" % possible)
						continue
					if (chunk) and (self.filenameHasChunk(possible, "bz2")):
						runner.debug("Prefetchable %s etc." % possible)
					else:
						runner.debug("Prefetchable %s" % possible)
					# found something workable, now check the chunk situation
					if (chunk):
						if (self.filenameHasChunk(possible, "bz2")):
							if len(pageIDs) > 0:
								possibleStartNum = None
								for i in range(len(pageIDs)):
									if int(pageIDs[i]) <= int(startPageID):
										# chunk number of file starts at 1.
										possibleStartNum = i+1
									else:
										break;
								if possibleStartNum:
									possibleEndNum = possibleStartNum
									for j in range(i,len(pageIDs)):
										if (not endPageID) or (int(pageIDs[j]) <= int(endPageID)):
											# chunk number of file starts at 1.
											possibleEndNum = j + 1
										else:
											break
									# now we have the range of the relevant files, put together the list.
									possible = [ runner.dumpDir.buildPath(base, date, self._file("bz2", k)) for k in range(possibleStartNum,possibleEndNum+1) ]
									return possible
						else:
							continue
							
					return [ possible ]
		runner.debug("Could not locate a prefetchable dump.")
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
			return None

	# format: name:%; updated:%; status:%
	def _getStatusForJobFromRunInfoFileLine(self, line, jobName):
		# get rid of leading/trailing/embedded blanks
		line = line.replace(" ","")
		line = line.replace("\n","")
		fields = line.split(';',3)
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
			return None

	def statusOfOldDumpIsDone(self, runner, date):
		oldDumpRunInfoFilename=runner.dumpItemList._getDumpRunInfoFileName(date)
		jobName = self.name()
		status = self._getStatusForJobFromRunInfoFile(oldDumpRunInfoFilename, jobName)
		if (status == "done"):
			return 1
		elif (not status == None):
			# failure, in progress, some other useless thing
			return 0

		# ok, there was no info there to be had, try the index file. yuck.
		indexFilename = os.path.join(runner.wiki.publicDir(), date, runner.config.perDumpIndex)
		desc = self._desc
		status = self._getStatusForJobFromIndexFile(indexFilename, desc)
		if (status == "done"):
			return 1
		else:
			return 0

	def listFiles(self, runner, unnumbered = False):
		if (self._chunks) and not unnumbered:
			files = []
			for i in range(1, len(self._chunks)+1):
				files.append(self._file("bz2",i))
			return files
		else:
			return [ self._file("bz2",0) ]

class RecombineXmlDump(XmlDump):
	def __init__(self, subset, name, desc, detail, chunks = False):
		# no prefetch, no spawn
		XmlDump.__init__(self, subset, name, desc, detail, None, None, chunks)
		# this is here only so that a callback can capture output from some commands
		# related to recombining files if we did parallel runs of the recompression
		self._output = None

	def listFiles(self, runner):
		return(XmlDump.listFiles(self, runner, unnumbered=True))

	def run(self, runner):
		error=0
		if (self._chunks):
			files = XmlDump.listFiles(self,runner)
			outputFileList = self.listFiles(runner)
			for outputFile in outputFileList:
				inputFiles = []
				for inFile in files:
					(base, rest) = inFile.split('.',1)
					base = re.sub("\d+$", "", base)
					if base + "." + rest == outputFile:
						inputFiles.append(inFile)
				if not len(inputFiles):
					self.setStatus("failed")
					raise BackupError("No input files for %s found" % self.name)
				compressionCommand = runner.config.bzip2
				compressionCommand = "%s > " % runner.config.bzip2
				uncompressionCommand = [ "%s" % runner.config.bzip2, "-dc" ] 
				recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFile, compressionCommand, uncompressionCommand )
				recombineCommand = [ recombineCommandString ]
				recombinePipeline = [ recombineCommand ]
				series = [ recombinePipeline ]
				result = runner.runCommand([ series ], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
				if result:
					error = result
		if (error):
			raise BackupError("error recombining xml bz2 files")

class BigXmlDump(XmlDump):
	"""XML page dump for something larger, where a 7-Zip compressed copy
	could save 75% of download time for some users."""

	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--full"

class XmlRecompressDump(Dump):
	"""Take a .bz2 and recompress it as 7-Zip."""

	def __init__(self, subset, name, desc, detail, chunks = False):
		Dump.__init__(self, name, desc)
		self._subset = subset
		self._detail = detail
		self._chunks = chunks

	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return self._detail

	def _file(self, ext, chunk = 0):
		if (chunk):
			return "pages-" + self._subset + ("%s.xml." % chunk) + ext
		else:
			return "pages-" + self._subset + ".xml." + ext

	def _path(self, runner, ext, chunk=0):
		return runner.dumpDir.publicPath(self._file(ext,chunk))

	def buildOutputFilename(self, runner, chunk=0):
		if (chunk):
			xml7z = self._path(runner, "7z", chunk)
		else:
			xml7z = self._path(runner, "7z")
		return(xml7z)

	def getInputFilename(self, runner, chunk):
		if (chunk):
			xmlbz2 = self._path(runner, "bz2", chunk)
		else:
			xmlbz2 = self._path(runner, "bz2")
		return(xmlbz2)

	def buildCommand(self, runner, chunk = 0):
		xmlbz2 = self.getInputFilename(runner, chunk)
		xml7z = self.buildOutputFilename(runner, chunk)

		# FIXME need shell escape
		commandPipe = [ [ "%s -dc %s | %s a -si %s"  % (runner.config.bzip2, xmlbz2, runner.config.sevenzip, xml7z) ] ]
		commandSeries = [ commandPipe ]
		return(commandSeries)

	def cleanupOldFiles(self, runner, chunk = 0):
		xml7z = self.buildOutputFilename(runner, chunk)
		if exists(xml7z):
			runner.remove(xml7z)

	def run(self, runner):
		if runner.lastFailed:
			raise BackupError("bz2 dump incomplete, not recompressing")
		commands = []
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				# Clear prior 7zip attempts; 7zip will try to append an existing archive
				self.cleanupOldFiles(runner, i)
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			# Clear prior 7zip attempts; 7zip will try to append an existing archive
			self.cleanupOldFiles(runner)
			series = self.buildCommand(runner)
			commands.append(series)
		error = runner.runCommand(commands, callbackTimed=self.progressCallback, callbackTimedArg=runner, shell = True)
		# temp hack force 644 permissions until ubuntu bug # 370618 is fixed - tomasz 5/1/2009
		# some hacks aren't so temporary - atg 3 sept 2010
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				xml7z = self.buildOutputFilename(runner,i)
				if exists(xml7z):
					os.chmod(xml7z, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH )
		else:
				xml7z = self.buildOutputFilename(runner)
				if exists(xml7z):
					os.chmod(xml7z, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH )
		if (error):
			raise BackupError("error recompressing bz2 file(s)")
	
	def listFiles(self, runner, unnumbered = False):
		if (self._chunks) and not unnumbered:
			files = []
			for i in range(1, len(self._chunks)+1):
				files.append(self._file("7z",i))
			return files
		else:
			return [ self._file("7z",0) ]

	def getCommandOutputCallback(self, line):
		self._output = line

class RecombineXmlRecompressDump(XmlRecompressDump):
	def __init__(self, subset, name, desc, detail, chunks):
		XmlRecompressDump.__init__(self, subset, name, desc, detail, chunks)
		# this is here only so that a callback can capture output from some commands
		# related to recombining files if we did parallel runs of the recompression
		self._output = None

	def listFiles(self, runner):
		return(XmlRecompressDump.listFiles(self, runner, unnumbered=True))

	def cleanupOldFiles(self, runner):
		files = self.listFiles(runner)
		print "here is cleanup"
		for filename in files:
			filename = runner.dumpDir.publicPath(filename)
			if exists(filename):
				runner.remove(filename)

	def run(self, runner):
		error = 0
		if (self._chunks):
			self.cleanupOldFiles(runner)
			files = XmlRecompressDump.listFiles(self,runner)
			outputFileList = self.listFiles(runner)
			for outputFile in outputFileList:
				inputFiles = []
				for inFile in files:
					(base, rest) = inFile.split('.',1)
					base = re.sub("\d+$", "", base)
					if base + "." + rest == outputFile:
						inputFiles.append(inFile)
				if not len(inputFiles):
					self.setStatus("failed")
					raise BackupError("No input files for %s found" % self.name)
				compressionCommand = "%s a -si" % runner.config.sevenzip
				uncompressionCommand = [ "%s" % runner.config.sevenzip, "e", "-so" ] 

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

        def __init__(self, name, desc, chunks = False):
		Dump.__init__(self, name, desc)
		self._chunks = chunks

        def buildCommand(self, runner, chunk = 0):
		command = [ "%s" % runner.config.php,
			    "-q", "%s/maintenance/dumpBackup.php" % runner.config.wikiDir,
			    "--wiki=%s" % runner.dbName,
			    "--plugin=AbstractFilter:%s/extensions/ActiveAbstract/AbstractFilter.php" % runner.config.wikiDir,
			    "--current", "--report=1000", "%s" % runner.forceNormalOption(),
			    ]
		for variant in self._variants(runner):
			command.extend( [ "--output=file:%s" % runner.dumpDir.publicPath(self._variantFile(variant, chunk)),
					  "--filter=namespace:NS_MAIN", "--filter=noredirect", 
					  "--filter=abstract%s" % self._variantOption(variant) ] )
			if (chunk):
				# set up start end end pageids for this piece
				# note there is no page id 0 I guess. so we start with 1
				# start = runner.pagesPerChunk()*(chunk-1) + 1
				start = sum([ self._chunks[i] for i in range(0,chunk-1)]) + 1
				startopt = "--start=%s" % start
				# if we are on the last chunk, we should get up to the last pageid, 
				# whatever that is. 
				command.append(startopt)
				if chunk < len(self._chunks):
					# end = start + runner.pagesPerChunk()
					end = sum([ self._chunks[i] for i in range(0,chunk)]) +1
					endopt = "--end=%s" % end
					command.append(endopt)
		pipeline = [ command ]
		series = [ pipeline ]
		return(series)

	def run(self, runner):
		commands = []
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			series = self.buildCommand(runner)
		        commands.append(series)
		error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
		if (error):
			raise BackupError("error producing abstract dump")


	def _variants(self, runner):
		# If the database name looks like it's marked as Chinese language,
		# return a list including Simplified and Traditional versions, so
		# we can build separate files normalized to each orthography.
		if runner.dbName[0:2] == "zh" and runner.dbName[2:3] != "_":
			return ("", "zh-cn", "zh-tw")
		else:
			return ("",)

	def _variantOption(self, variant):
		if variant == "":
			return ""
		else:
			return ":variant=%s" % variant

	def _variantFile(self, variant, chunk = 0):
		if chunk:
			chunkInfo = "%s" % chunk
		else:
			chunkInfo = ""
		if variant == "":
			return( "abstract"+chunkInfo + ".xml")
		else:
			return( "abstract-%s%s.xml" % (variant, chunkInfo) )

	def listFiles(self, runner, unnumbered = False):
		files = []
		for x in self._variants(runner):
			if (self._chunks) and not unnumbered:
				for i in range(1, len(self._chunks)+1):
					files.append(self._variantFile(x, i))
			else:
				files.append(self._variantFile(x))
		return files 

class RecombineAbstractDump(AbstractDump):
	def __init__(self, name, desc, chunks):
		AbstractDump.__init__(self, name, desc, chunks)
		# this is here only so that a callback can capture output from some commands
		# related to recombining files if we did parallel runs of the recompression
		self._output = None

	def listFiles(self, runner):
		return(AbstractDump.listFiles(self,runner, unnumbered = True))

	def run(self, runner):
		error = 0
		if (self._chunks):
			files = AbstractDump.listFiles(self,runner)
			outputFileList = self.listFiles(runner)
			for outputFile in outputFileList:
				inputFiles = []
				for inFile in files:
					(base, rest) = inFile.split('.',1)
					base = re.sub("\d+$", "", base)
					if base + "." + rest == outputFile:
						inputFiles.append(inFile)
				if not len(inputFiles):
					self.setStatus("failed")
					raise BackupError("No input files for %s found" % self.name)
				compressionCommand = "%s > " % runner.config.cat
				uncompressionCommand = [ "%s" % runner.config.cat ] 
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
	def run(self, runner):
		retries = 0
		# try this initially and see how it goes
		maxretries = 3 
		query="select page_title from page where page_namespace=0;"
		error = runner.saveSql(query, runner.dumpDir.publicPath("all-titles-in-ns0.gz"))
		while (error and retries < maxretries):
			retries = retries + 1
			time.sleep(5)
			error = runner.saveSql(query, runner.dumpDir.publicPath("all-titles-in-ns0.gz"))
		if (error):
			raise BackupError("error dumping titles list")

	def listFiles(self, runner):
		return ["all-titles-in-ns0.gz"]


def findAndLockNextWiki(config):
	if config.halt:
		print "Dump process halted by config."
		return None

	next = config.dbListByAge()
	next.reverse()

	print "Finding oldest unlocked wiki..."

	for db in next:
		wiki = WikiDump.Wiki(config, db)
		try:
			wiki.lock()
			return wiki
		except:
			print "Couldn't lock %s, someone else must have got it..." % db
			continue
	return None

def usage(message = None):
	if message:
		print message
	print "Usage: python worker.py [options] [wikidbname]"
	print "Options: --configfile, --date, --job, --addnotice, --delnotice, --force, --noprefetch, --nospawn, --restartfrom, --log"
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

		try:
			(options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
								 ['date=', 'job=', 'configfile=', 'addnotice=', 'delnotice', 'force', 'dryrun', 'noprefetch', 'nospawn', 'restartfrom', 'log'])
		except:
			usage("Unknown option specified")

		for (opt, val) in options:
			if opt == "--date":
				date = val
			elif opt == "--configfile":
				configFile = val
			elif opt == "--force":
				forceLock = True
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

		if dryrun and (len(remainder) == 0):
			usage("--dryrun requires the name of a wikidb to be specified")
		if jobRequested and (len(remainder) == 0):
			usage("--job option requires the name of a wikidb to be specified")
		if (jobRequested and forceLock):
	       		usage("--force cannot be used with --job option")
		if (restart and not jobRequested):
			usage("--restartfrom requires --job and the job from which to restart")

		# allow alternate config file
		if (configFile):
			config = WikiDump.Config(configFile)
		else:
			config = WikiDump.Config()

		if dryrun:
			print "***"
			print "Dry run only, no files will be updated."
			print "***"

		if len(remainder) > 0:
			wiki = WikiDump.Wiki(config, remainder[0])
			if not dryrun:
				# if we are doing one piece only of the dump, we don't try to grab a lock
				# unless told to. 
				if forceLock and wiki.isLocked():
					wiki.unlock()
				if restart or not jobRequested:
					wiki.lock()

		else:
			wiki = findAndLockNextWiki(config)

		if wiki:
			runner = Runner(wiki, date, prefetch, spawn, jobRequested, restart, htmlNotice, dryrun, enableLogging)
			if (restart):
				print "Running %s, restarting from job %s..." % (wiki.dbName, jobRequested)
			elif (jobRequested):
				print "Running %s, job %s..." % (wiki.dbName, jobRequested)
			else:
				print "Running %s..." % wiki.dbName
			runner.run()
			# if we are doing one piece only of the dump, we don't unlock either
			if not dryrun:
				if restart or not jobRequested:
					wiki.unlock()
		else:
			print "No wikis available to run."
	finally:
		WikiDump.cleanup()
