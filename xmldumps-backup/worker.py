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

from os.path import dirname, exists, getsize, join, realpath
from subprocess import Popen, PIPE
from WikiDump import prettyTime, prettySize, shellEscape
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel

def splitPath(path):
	# For some reason, os.path.split only does one level.
	parts = []
	(path, file) = os.path.split(path)
	if not file:
		# Probably a final slash
		(path, file) = os.path.split(path)
	while file:
		parts.insert(0, file)
		(path, file) = os.path.split(path)
	return parts

def relativePath(path, base):
	"""Return a relative path to 'path' from the directory 'base'."""
	path = splitPath(path)
	base = splitPath(base)
	while base and path[0] == base[0]:
		path.pop(0)
		base.pop(0)
	for prefix in base:
		path.insert(0, "..")
	return os.path.join(*path)

def xmlEscape(text):
	return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# so if the pages/revsPerChunkAbstract/History are just one number it means
# use that number for all the chunks, figure out yourself how many.
# otherwise we get passed alist that says "here's now many for each chunk and it's this many chunks. 
# extra pages/revs go in the last chunk, stuck on the end. too bad. :-P
class Chunk(object, ):
	def __init__(self, wiki, dbName):

		self._dbName = dbName
		self._chunksEnabled = wiki.config.chunksEnabled
		self._pagesPerChunkHistory = self.convertCommaSepLineToNumbers(wiki.config.pagesPerChunkHistory)
		self._revsPerChunkHistory = self.convertCommaSepLineToNumbers(wiki.config.revsPerChunkHistory)
		self._pagesPerChunkAbstract = self.convertCommaSepLineToNumbers(wiki.config.pagesPerChunkAbstract)

		if (self._chunksEnabled):
			self.Stats = PageAndEditStats(wiki,dbName)

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

class PageAndEditStats(object):
	def __init__(self, wiki, dbName):
		self.totalPages = None
		self.totalEdits = None
		self.config = wiki.config
		self.dbName = dbName
		(self.totalPages, totalEdits) = self.getStatistics(config,dbName)

	def getStatistics(self, config,dbName):
		"""Get (cached) statistics for the wiki"""
		totalPages = None
		totalEdits = None
		statsCommand = """%s -q %s/maintenance/showStats.php --wiki=%s """ % shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		# FIXME runAndReturn?  defined somewhere else
		results = self.runAndReturn(statsCommand)
		lines = results.splitlines()
		if (lines):
			for line in lines:
				(name,value) = line.split(':')
				name = name.replace(' ','')
				value = value.replace(' ','')
				if (name == "Totalpages"):
					totalPages = int(value)
				elif (name == "Totaledits"):
					totalEdits = int(value)
		return(totalPages, totalEdits)

	def getTotalPages(self):
		return self.totalPages

	def getTotalEdits(self):
		return self.totalEdits

	# FIXME should rewrite this I guess and also move it elsewhere, phooey
	def runAndReturn(self, command):
		"""Run a command and return the output as a string.
		Raises BackupError on non-zero return code."""
		# FIXME convert all these calls so they just use runCommand now
		proc = popen2.Popen4(command, 64)
		output = proc.fromchild.read()
		retval = proc.wait()
		if retval:
			raise BackupError("Non-zero return code from '%s'" % command)
		else:
			return output

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
			if (self.chunkInfo.chunksEnabled()):
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
			if (self.chunkInfo.chunksEnabled()):
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
		# get rid of leading/trailing/embedded blanks
		line = line.replace(" ","")
		line = line.replace("\n","")
		fields = line.split(';',3)
		dumpRunInfo = RunInfo()
		for field in fields:
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
		dumpRunInfoFilename = self._getDumpRunInfoFileName()
		WikiDump.dumpFile(dumpRunInfoFilename, text)

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
		print "No job of the name specified exists. Choose one of the following:"
		print "tables (includes all items below that end in 'table'"
		for item in self.dumpItems:
			print "%s " % item.name()
	        return False

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

	def getChecksumFileNameBasename(self):
		return ("md5sums.txt")

	def getChecksumFileName(self):
		return (self.dumpDir.publicPath(self.getChecksumFileNameBasename()))

	def prepareChecksums(self):
		"""Create the md5 checksum file at the start of the run.
		This will overwrite a previous run's output, if any."""
		checksumFileName = self.getChecksumFileName()
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
		checksumFileName = self.getChecksumFileName()
		output = file(checksumFileName, "a")
		self.saveChecksum(filename, output, runner)
		output.close()
				      
class DumpDir(object):
	def __init__(self, wiki, dbName, date):
		self._wiki = wiki
		self._dbName = dbName
		self._date = date

	def buildDir(self, base, version):
		return join(base, self._dbName, version)

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

	def latestPath(self, filename):
		return self.buildPath(self._wiki.publicDir(), "latest", filename)

	def webPath(self, filename):
		return self.buildPath(self._wiki.webDir(), self._date, filename)
				      
class Runner(object):

	def __init__(self, wiki, date=None, checkpoint=None, prefetch=True, spawn=True, job=None):
		self.wiki = wiki
		self.config = wiki.config
		self.dbName = wiki.dbName
		self.prefetch = prefetch
		self.spawn = spawn
		self.chunkInfo = Chunk(wiki, self.dbName)

		if date:
			# Override, continuing a past dump?
			self.date = date
		else:
			self.date = WikiDump.today()
		wiki.setDate(self.date)

		self.failCount = 0
		self.lastFailed = False

		self.checkpoint = checkpoint

		self.jobRequested = job
		self.dumpDir = DumpDir(self.wiki, self.dbName, self.date)
		self.checksums = Checksummer(self.wiki, self.dumpDir)
		# some or all of these dumpItems will be marked to run
		self.dumpItemList = DumpItemList(self.wiki, self.prefetch, self.spawn, self.date, self.chunkInfo);

	def passwordOption(self):
		"""If you pass '-pfoo' mysql uses the password 'foo',
		but if you pass '-p' it prompts. Sigh."""
		if self.config.dbPassword == "":
			return None
		else:
			return "-p" + self.config.dbPassword

	def forceNormalOption(self):
		if self.config.forceNormal:
			return "--force-normal"
		else:
			return ""

	def getDBTablePrefix(self):
		"""Get the prefix for all tables for the specific wiki ($wgDBprefix)"""
		# FIXME later full path
		command = "echo 'print $wgDBprefix; ' | %s -q %s/maintenance/eval.php --wiki=%s" % shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		return self.runAndReturn(command).strip()
				      
	def saveTable(self, table, outfile):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		commands = [ [ "%s" % self.config.mysqldump, "-h", 
			       "%s" % self.dbServer, "-u", 
			       "%s" % self.config.dbUser, 
			       "%s" % self.passwordOption(), "--opt", "--quick", 
			       "--skip-add-locks", "--skip-lock-tables", 
			       "%s" % self.dbName, 
			       "%s" % self.getDBTablePrefix() + table ], 
			     [ "%s" % self.config.gzip ] ]

		return self.saveCommand(commands, outfile)

	def saveSql(self, query, outfile):
		"""Pass some SQL commands to the server for this DB and save output to a file."""
		command = [ [ "/bin/echo", "%s" % query ], 
			    [ "%s" % self.config.mysql, "-h", 
			      "%s" % self.dbServer,
			      "-u", "%s" % self.config.dbUser,
			      "%s" % self.passwordOption(),
			      "%s" % self.dbName, 
			      "-r" ],
			    [ "%s" % self.config.gzip ] ]
		return self.saveCommand(command, outfile)

	def saveCommand(self, commands, outfile):
		"""For one pipeline of commands, redirect output to a given file."""
		commands[-1].extend( [ ">" , outfile ] )
		series = [ commands ]
		return self.runCommand([ series ])

	def getStatistics(self, dbName):
		"""Get (cached) statistics for the wiki"""
		totalPages = None
		totalEdits = None
		statsCommand = """%s -q %s/maintenance/showStats.php --wiki=%s """ % shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		results = self.runAndReturn(statsCommand)
		lines = results.splitlines()
		if (lines):
			for line in lines:
				(name,value) = line.split(':')
				name = name.replace(' ','')
				value = value.replace(' ','')
				if (name == "Totalpages"):
					totalPages = int(value)
				elif (name == "Totaledits"):
					totalEdits = int(value)
		return(totalPages, totalEdits)

	# command series list: list of (commands plus args) is one pipeline. list of pipelines = 1 series. 
	# this function wants a list of series.
	# be a list (the command name and the various args)
	# If the shell option is true, all pipelines will be run under the shell.
	def runCommand(self, commandSeriesList, callback=None, arg=None, shell = False):
		"""Nonzero return code from the shell from any command in any pipeline will raise a BackupError.
		If a callback function is passed, it will receive lines of
		output from the call.  If the callback function takes another argument (which will
		be passed before the line of output) must be specified by the arg paraemeter.
		If no callback is provided, and no output file is specified for a given 
		pipe, the output will be written to stderr. (Do we want that?)
		This function spawns multiple series of pipelines  in parallel.

		"""
		commands = CommandsInParallel(commandSeriesList, callback=callback, arg=arg, shell=shell)
		commands.runCommands()
		if commands.exitedSuccessfully():
			return 0
		else:
			#print "***** BINGBING retval is '%s' ********" % retval
			problemCommands = commands.commandsWithErrors()
			errorString = "Error from command(s): "
			for cmd in problemCommands: 
				errorString = errorString + "%s " % cmd
			raise BackupError(errorString)
		return 1

	def runAndReport(self, command, callback):
		"""Shell out to a command, and feed output lines to the callback function.
		Returns the exit code from the program once complete.
		stdout and stderr will be combined into a single stream.
		"""
		# FIXME convert all these calls so they just use runCommand now
		proc = popen2.Popen4(command, 64)
		#for line in proc.fromchild:
		#	callback(self, line)
		line = proc.fromchild.readline()
		while line:
			callback(self, line)
			line = proc.fromchild.readline()
		return proc.wait()

	def runAndReturn(self, command):
		"""Run a command and return the output as a string.
		Raises BackupError on non-zero return code."""
		# FIXME convert all these calls so they just use runCommand now
		proc = popen2.Popen4(command, 64)
		output = proc.fromchild.read()
		retval = proc.wait()
		if retval:
			raise BackupError("Non-zero return code from '%s'" % command)
		else:
			return output

	def debug(self, stuff):
		print "%s: %s %s" % (prettyTime(), self.dbName, stuff)

	def makeDir(self, dir):
		if exists(dir):
			self.debug("Checkdir dir %s ..." % dir)
		else:
			self.debug("Creating %s ..." % dir)
			os.makedirs(dir)

	def selectDatabaseServer(self):
		self.dbServer = self.defaultServer()

	def defaultServer(self):
		command = "%s -q %s/maintenance/getSlaveServer.php --wiki=%s --group=dump" % shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		return self.runAndReturn(command).strip()
				      
	def runHandleFailure(self):
		if self.failCount < 1:
			# Email the site administrator just once per database
			self.reportFailure()
			self.failCount += 1
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

		self.makeDir(join(self.wiki.publicDir(), self.date))
	       	self.makeDir(join(self.wiki.privateDir(), self.date))

		if (self.jobRequested):
			print "Preparing for job %s of %s" % (self.jobRequested, self.dbName)
		else:
			self.showRunnerState("Cleaning up old dumps for %s" % self.dbName)
			self.cleanOldDumps()
			self.showRunnerState("Starting backup of %s" % self.dbName)

		self.selectDatabaseServer()

		files = self.listFilesFor(self.dumpItemList.dumpItems)

		if (self.jobRequested):
			self.checksums.prepareChecksums()

			for item in self.dumpItemList.dumpItems:
				if (item.toBeRun()):
					item.start(self)
					self.updateStatusFiles()
					self.dumpItemList.saveDumpRunInfoFile()
					try:
						item.dump(self)
					except Exception, ex:
						self.debug("*** exception! " + str(ex))
						item.setStatus("failed")
					if item.status() == "failed":
						self.runHandleFailure()
					else:
						self.lastFailed = False
				# this ensures that, previous run or new one, the old or new md5sums go to the file
				if item.status() == "done":
					self.runUpdateItemFileInfo(item)

			if (self.dumpItemList.allPossibleJobsDone()):
				self.updateStatusFiles("done")
			else:
				self.updateStatusFiles("partialdone")
			self.dumpItemList.saveDumpRunInfoFile()

			# if any job succeeds we might as well make the sym link
			if (self.failCount < 1):
				self.completeDump(files)
											
			self.showRunnerState("Completed job %s for %s" % (self.jobRequested, self.dbName))
		else:
			self.checksums.prepareChecksums()

			for item in self.dumpItemList.dumpItems:
				item.start(self)
				self.updateStatusFiles()
				self.dumpItemList.saveDumpRunInfoFile()
				# FIXME is this checkpoint stuff useful to us now?
				if self.checkpoint and not item.matchCheckpoint(self.checkpoint):
					self.debug("*** Skipping until we reach checkpoint...")
					item.setStatus("done")
					pass
				else:
					if self.checkpoint and item.matchCheckpoint(self.checkpoint):
						self.debug("*** Reached checkpoint!")
						self.checkpoint = None
				try:
					item.dump(self)
				except Exception, ex:
					self.debug("*** exception! " + str(ex))
					item.setStatus("failed")
				if item.status() == "failed":
					self.runHandleFailure()
				else:
					self.runUpdateItemFileInfo(item)
					self.lastFailed = False

			self.updateStatusFiles("done")
			self.dumpItemList.saveDumpRunInfoFile()
										
			if self.failCount < 1:
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
				base = os.path.join(self.wiki.publicDir(), dump)
				shutil.rmtree("%s" % base)
		else:
			self.showRunnerState("No old dumps to purge.")

	def reportFailure(self):
		if self.config.adminMail:
			subject = "Dump failure for " + self.dbName
			message = self.config.readTemplate("errormail.txt") % {
				"db": self.dbName,
				"date": self.date,
				"time": prettyTime(),
				"url": "/".join((self.config.webRoot, self.dbName, self.date, ''))}
			config.mail(subject, message)

	def listFilesFor(self, items):
		files = []
		for item in items:
			for file in item.listFiles(self):
				files.append(file)
		return files

	def saveStatusSummaryAndDetail(self, items, done=False):
		"""Write out an HTML file with the status for this wiki's dump and links to completed files, as well as a summary status in a separate file."""
		try:
			# Comprehensive report goes here
			self.wiki.writePerDumpIndex(self.reportDatabaseStatusDetailed(items, done))

			# Short line for report extraction goes here
			self.wiki.writeStatus(self.reportDatabaseStatusSummary(items, done))
		except:
			print "Couldn't update status files. Continuing anyways"

	def updateStatusFiles(self, done=False):
		self.saveStatusSummaryAndDetail(self.dumpItemList.dumpItems, done)

	def reportDatabaseStatusSummary(self, items, done=False):
		"""Put together a brief status summary and link for the current database."""
		status = self.reportStatusSummaryLine(done)
		html = self.wiki.reportStatusLine(status)

		activeItems = [x for x in items if x.status() == "in-progress"]
		if activeItems:
			return html + "<ul>" + "\n".join([self.reportItem(x) for x in activeItems]) + "</ul>"
		else:
			return html

	def reportDatabaseStatusDetailed(self, items, done=False):
		"""Put together a status page for this database, with all its component dumps."""
		statusItems = [self.reportItem(item) for item in items]
		statusItems.reverse()
		html = "\n".join(statusItems)
		return self.config.readTemplate("report.html") % {
			"db": self.dbName,
			"date": self.date,
			"status": self.reportStatusSummaryLine(done),
			"previous": self.reportPreviousDump(done),
			"items": html,
			"checksum": self.dumpDir.webPath(self.checksums.getChecksumFileNameBasename()),
			"index": self.config.index}

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
		prettyDate = WikiDump.prettyDate(rawDate)
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
	# Report on the file size & status of the current output and output a link if were done
	def reportFile(self, file, status):
		filepath = self.dumpDir.publicPath(file)
		if status == "in-progress" and exists (filepath):
			size = prettySize(getsize(filepath))
			return "<li class='file'>%s %s (written) </li>" % (file, size)
		elif status == "done" and exists(filepath):
			size = prettySize(getsize(filepath))
			webpath = self.dumpDir.webPath(file)
			return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpath, file, size)
		else:
			return "<li class='missing'>%s</li>" % file

	def lockFile(self):
		return self.dumpDir.publicPath("lock")

	def doneFile(self):
		return self.dumpDir.publicPath("done")

	def lock(self):
		self.showRunnerState("Creating lock file.")
		lockfile = self.lockFile()
		donefile = self.doneFile()
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
		#####date -u > $StatusLockFile
				      
	def unlock(self):
		self.showRunnerState("Marking complete.")
		######date -u > $StatusDoneFile

	def dateStamp(self):
		#date -u --iso-8601=seconds
		pass

	def showRunnerState(self, message):
		#echo $DatabaseName `dateStamp` OK: "$1" | tee -a $StatusLog | tee -a $GlobalLog
		self.debug(message)

	def showRunnerStateComplete(self):
		#  echo $DatabaseName `dateStamp` SUCCESS: "done." | tee -a $StatusLog | tee -a $GlobalLog
		self.debug("SUCCESS: done.")

	def completeDump(self, files):
		# FIXME: md5sums.txt won't be consistent with mixed data.
		# later comment: which mixed data was meant here?
		self.saveSymlink(self.checksums.getChecksumFileNameBasename())

	def saveSymlink(self, file):
		self.makeDir(join(self.wiki.publicDir(), 'latest'))
		real = self.dumpDir.publicPath(file)
		link = self.dumpDir.latestPath(file)
		if exists(link) or os.path.islink(link):
			if os.path.islink(link):
				self.debug("Removing old symlink %s" % link)
				os.remove(link)
			else:
				raise BackupError("What the hell dude, %s is not a symlink" % link)
		relative = relativePath(real, dirname(link))
		if exists(real):
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
		rssPath = self.dumpDir.latestPath(file + "-rss.xml")
		WikiDump.dumpFile(rssPath, rssText)

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
			self.runInfo.setUpdated(prettyTime())

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
			sys.stderr.write(line)
		self.progress = line.strip()
		runner.updateStatusFiles()
		runner.dumpItemList.saveDumpRunInfoFile()

	def timeToWait(self):
		# we use wait this many secs for a command to complete that
		# doesn't produce output
		return 5

	def waitAlarmHandler(self, signum, frame):
		pass

	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__
				      
	def buildRecombineCommandString(self, runner, files, outputFileBasename, compressionCommand, uncompressionCommand, endHeaderMarker="</siteinfo>"):
		outputFilename = runner.dumpDir.publicPath(outputFileBasename)
		chunkNum = 0
		recombines = []
		head = runner.config.head
		tail = runner.config.tail
		grep = runner.config.grep

		# we assume the result is always going to be run in a subshell. 
		# much quicker than this script trying to read output
		# and pass it to a subprocess
		outputFilenameEsc = shellEscape(outputFilename)
		headEsc = shellEscape(head)
		tailEsc = shellEscape(tail)
		grepEsc = shellEscape(grep)

		uncompressionCommandEsc = uncompressionCommand[:]
		for u in uncompressionCommandEsc:
			u = shellEscape(u)
		for u in compressionCommand:
			u = shellEscape(u)
		for f in files:
			f = shellEscape(f)

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
			# without sheell
			p = CommandPipeline(pipeline, quiet=True)
			p.runPipelineAndGetOutput()
			if (p.output()):
				(headerEndNum, junk) = p.output().split(":",1)
				# get headerEndNum
			if exists(outputFilename):
				os.remove(outputFilename)
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
		return runner.saveTable(self._table, self._path(runner))

	def listFiles(self, runner):
		return [self._file()]

	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__ + "." + self._table

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
		if (chunk):
			chunkinfo = "%s" % chunk
		else:
			 chunkinfo = ""
		history = runner.dumpDir.publicPath("stub-meta-history" + chunkinfo + ".xml.gz")
		current = runner.dumpDir.publicPath("stub-meta-current" + chunkinfo + ".xml.gz")
		articles = runner.dumpDir.publicPath("stub-articles" + chunkinfo + ".xml.gz")
		for filename in (history, current, articles):
			 if exists(filename):
				os.remove(filename)
			 command = [ "%s" % runner.config.php,
				    "-q", "%s/maintenance/dumpBackup.php" % runner.config.wikiDir,
				    "--wiki=%s" % runner.dbName,
				    "--full", "--stub", "--report=10000",
				    "%s" % runner.forceNormalOption(),
				    "--server=%s" % runner.dbServer,
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
	
	def run(self, runner):
		commands = []
		if self._chunks:
			for i in range(1, len(self._chunks)+1):
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			series = self.buildCommand(runner)
			commands.append(series)
		runner.runCommand(commands, callback=self.progressCallback, arg=runner)

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
				recombinePipeline = CommandPipeline([ recombineCommand ], shell = True)
				recombinePipeline.startCommands()
				while True:
					# these commands don't produce any progress bar... so we can at least
					# update the size and last update time of the file once a minute
					signal.signal(signal.SIGALRM, self.waitAlarmHandler)
					signal.alarm(self.timeToWait())
					try:
						recombinePipeline._lastProcessInPipe.wait()
						break
					except Exception, e:
						if e.errno == errno.EINTR:
							pass
						else:
							raise
					self.progressCallback(runner)
					signal.alarm(0)

class XmlLogging(Dump):
	""" Create a logging dump of all page activity """

	def __init__(self, desc, chunks = False):
		Dump.__init__(self, "xmlpagelogsdump", desc)
		self._chunks = chunks

	def detail(self):
		return "This contains the log of actions performed on pages."

	def listFiles(self, runner):
		return ["pages-logging.xml.gz"]

	def run(self, runner):
		logging = runner.dumpDir.publicPath("pages-logging.xml.gz")
		if exists(logging):
			os.remove(logging)
		command = [ "%s" % runner.config.php,
			    "-q",  "%s/maintenance/dumpBackup.php" % runner.config.wikiDir,
			    "--wiki=%s" % runner.dbName,
			    "--logs", "--report=10000",
			    "%s" % runner.forceNormalOption(),
			    "--server=%s" % runner.dbServer,
			    "--output=gzip:%s" % logging ]
		pipeline = [ command ]
		series = [ pipeline ]
		runner.runCommand([ series ], callback=self.progressCallback, arg=runner)

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
		return runner.runCommand(commands, callback=self.progressCallback, arg=runner)

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
		if self._prefetch:
			possibleSources = self._findPreviousDump(runner, chunk)
			# if we have a list of more than one then we need to check existence for each and put them together in a string
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
				"--report=1000", "--server=%s" % runner.dbServer,
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
		headEsc = shellEscape(head)
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
					runner.debug("Prefetchable %s" % possible)
					# found something workable, now check the chunk situation
					if (chunk):
						if (self.filenameHasChunk(possible, "bz2")):
							if len(pageIDs) > 0:
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

	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__ + "." + self._subset

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
				recombinePipeline = CommandPipeline([ recombineCommand ], shell = True)
				recombinePipeline.startCommands()
				while True:
					# these commands don't produce any progress bar... so we can at least
					# update the size and last update time of the file once a minute
					signal.signal(signal.SIGALRM, self.waitAlarmHandler)
					signal.alarm(self.timeToWait())
					try:
						recombinePipeline._lastProcessInPipe.wait()
						break
					except Exception, e:
						if e.errno == errno.EINTR:
							pass
						else:
							raise
					self.progressCallback(runner)
					signal.alarm(0)

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

	def getOutputFilename(self, runner, chunk=0):
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
		xml7z = self.getOutputFilename(runner, chunk)

		# Clear prior 7zip attempts; 7zip will try to append an existing archive
		if exists(xml7z):
			os.remove(xml7z)
		# FIXME need shell escape
		commandPipe = [ [ "%s -dc %s | %s a -si %s"  % (runner.config.bzip2, xmlbz2, runner.config.sevenzip, xml7z) ] ]
		commandSeries = [ commandPipe ]
		return(commandSeries)

	def run(self, runner):
		if runner.lastFailed:
			raise BackupError("bz2 dump incomplete, not recompressing")
		commands = []
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				series = self.buildCommand(runner, i)
				commands.append(series)
		else:
			series = self.buildCommand(runner)
			commands.append(series)
		# FIXME don't we want callback? yes we do. *sigh* on each one of these, right? bleah
		# this means we have the alarm loop in here (while we do what, poll a lot?) and um
		# write out a progress bar regardless after 60 secs by looking at all the files etc. bleah
		result = runner.runCommand(commands, callback=self.progressCallback, arg=runner, shell = True)
		# temp hack force 644 permissions until ubuntu bug # 370618 is fixed - tomasz 5/1/2009
		# some hacks aren't so temporary - atg 3 sept 2010
		if (self._chunks):
			for i in range(1, len(self._chunks)+1):
				xml7z = self.getOutputFilename(runner,i)
				if exists(xml7z):
					os.chmod(xml7z, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH )
		else:
				xml7z = self.getOutputFilename(runner)
				if exists(xml7z):
					os.chmod(xml7z, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH )
		return(result)
	
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

	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__ + "." + self._subset

class RecombineXmlRecompressDump(XmlRecompressDump):
	def __init__(self, subset, name, desc, detail, chunks):
		XmlRecompressDump.__init__(self, subset, name, desc, detail, chunks)
		# this is here only so that a callback can capture output from some commands
		# related to recombining files if we did parallel runs of the recompression
		self._output = None

	def listFiles(self, runner):
		return(XmlRecompressDump.listFiles(self, runner, unnumbered=True))

	def run(self, runner):
		if (self._chunks):
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
				recombinePipeline = CommandPipeline([ recombineCommand ], shell = True)
				recombinePipeline.startCommands()
				while True:
					# these commands don't produce any progress bar... so we can at least
					# update the size and last update time of the file once a minute
					signal.signal(signal.SIGALRM, self.waitAlarmHandler)
					signal.alarm(self.timeToWait())
					try:
						recombinePipeline._lastProcessInPipe.wait()
						break
					except Exception, e:
						if e.errno == errno.EINTR:
							pass
						else:
							raise
					self.progressCallback(runner)
					signal.alarm(0)

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
			    "--server=%s" % runner.dbServer ]
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
		runner.runCommand(commands, callback=self.progressCallback, arg=runner)

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
				recombinePipeline = CommandPipeline([ recombineCommand ], shell = True)
				recombinePipeline.startCommands()
				while True:
					# these commands don't produce any progress bar... so we can at least
					# update the size and last update time of the file once a minute
					signal.signal(signal.SIGALRM, self.waitAlarmHandler)
					signal.alarm(self.timeToWait())
					try:
						recombinePipeline._lastProcessInPipe.wait()
						break
					except Exception, e:
						if e.errno == errno.EINTR:
							pass
						else:
							raise
					self.progressCallback(runner)
					signal.alarm(0)

class TitleDump(Dump):
	"""This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""
	def run(self, runner):
		return runner.saveSql("select page_title from page where page_namespace=0;",
			runner.dumpDir.publicPath("all-titles-in-ns0.gz"))

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
	print "Options: --configfile, --date, --checkpoint, --job, --force, --noprefetch, --nospawn"
	print "--configfile:  Specify an alternative configuration file to read."
	print "              Default config file name: wikidump.conf"
	print "--date:       Rerun dump of a given date (probably unwise)"
	print "--checkpoint: Run just the specified step (deprecated)"
	print "--job:        Run just the specified step or set of steps; for the list,"
	print "              give the option --job help"
	print "              This option requires specifiying a wikidbname on which to run."
	print "              This option cannot be specified with --force."
	print "--force:      remove a lock file for the specified wiki (dangerous, if there is"
	print "              another process running, useful if you want to start a second later"
	print "              run while the first dump from a previous date is still going)"
	print "              This option cannot be specified with --job."
	print "--noprefetch: Do not use a previous file's contents for speeding up the dumps"
	print "              (helpful if the previous files may have corrupt contents)"
	print "--nospawn:    Do not spawn a separate process in order to retrieve revision texts"
	sys.exit(1)


if __name__ == "__main__":
	try:
		date = None
		checkpoint = None
		configFile = False
		forceLock = False
		prefetch = True
		spawn = True
		jobRequested = None

		try:
			(options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
								 ['date=', 'checkpoint=', 'job=', 'configfile=', 'force', 'noprefetch', 'nospawn'])
		except:
			usage("Unknown option specified")

		for (opt, val) in options:
			if opt == "--date":
				date = val
			elif opt == "--checkpoint":
				checkpoint = val
			elif opt == "--configfile":
				configFile = val
			elif opt == "--force":
				forceLock = True
			elif opt == "--noprefetch":
				prefetch = False
			elif opt == "--nospawn":
				spawn = False
			elif opt == "--job":
				jobRequested = val

		if jobRequested and (len(remainder) == 0):
			usage("--job option requires the name of a wikidb to be specified")
		if (jobRequested and forceLock):
	       		usage("--force cannot be used with --job option")

		# allow alternate config file
		if (configFile):
			config = WikiDump.Config(configFile)
		else:
			config = WikiDump.Config()

		if len(remainder) > 0:
			wiki = WikiDump.Wiki(config, remainder[0])
			# if we are doing one piece only of the dump, we don't try to grab a lock. 
			if forceLock:
				if wiki.isLocked():
					wiki.unlock()
			if not jobRequested:
				wiki.lock()
		else:
			wiki = findAndLockNextWiki(config)

		if wiki:
			runner = Runner(wiki, date, checkpoint, prefetch, spawn, jobRequested)
			if (jobRequested):
				print "Running %s, job %s..." % (wiki.dbName, jobRequested)
			else:
				print "Running %s..." % wiki.dbName
			runner.run()
			# if we are doing one piece only of the dump, we don't unlock either
			if not jobRequested:
				wiki.unlock()
		else:
			print "No wikis available to run."
	finally:
		WikiDump.cleanup()
