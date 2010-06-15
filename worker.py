# Worker process, does the actual dumping

import getopt
import md5
import os
import popen2
import re
import sys
import time
import WikiDump

from os.path import dirname, exists, getsize, join, realpath
from WikiDump import prettyTime, prettySize, shellEscape

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

def md5File(filename):
	summer = md5.new()
	infile = file(filename, "rb")
	bufsize = 4192 * 32
	buffer = infile.read(bufsize)
	while buffer:
		summer.update(buffer)
		buffer = infile.read(bufsize)
	infile.close()
	return summer.hexdigest()

def md5FileLine(filename):
	return "%s  %s\n" % (md5File(filename), os.path.basename(filename))

def xmlEscape(text):
	return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

class BackupError(Exception):
	pass

class Runner(object):
	
	def __init__(self, wiki, date=None, checkpoint=None, prefetch=True, spawn=True):
		self.wiki = wiki
		self.config = wiki.config
		self.dbName = wiki.dbName
		
		if date:
			# Override, continuing a past dump?
			self.date = date
		else:
			self.date = WikiDump.today()
		wiki.setDate(self.date)
		
		self.failCount = 0
		self.lastFailed = False
		
		self.checkpoint = checkpoint
	
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
		command = "echo 'print $wgDBprefix; ' | %s -q %s/maintenance/eval.php --wiki=%s" % shellEscape((
			self.config.php, self.config.wikiDir, self.dbName))
		return self.runAndReturn(command).strip()

	def saveTable(self, table, outfile):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		command = "mysqldump -h %s -u %s %s --extended-insert --skip-opt --quick --create-options --add-drop-table --extended-insert --set-charset  --quote-names  %s %s | gzip" % shellEscape((
			self.dbServer,
			self.config.dbUser,
			self.passwordOption(),
			self.dbName,
			self.getDBTablePrefix() + table))
		return self.saveCommand(command, outfile, pipe=True)
	
	def saveSql(self, query, outfile):
		"""Pass some SQL commands to the server for this DB and save output to a file."""
		command = "echo %s | mysql -h %s -u %s %s %s -r | gzip" % shellEscape((
			query,
			self.dbServer,
			self.config.dbUser,
			self.passwordOption(),
			self.dbName))
		return self.saveCommand(command, outfile, pipe=True)
	
	def saveCommand(self, command, outfile, pipe=False):
		"""Shell out and redirect output to a given file."""
		return self.runCommand(command + " > " + shellEscape(outfile), pipe)
	
	def runCommand(self, command, pipe=False, callback=None):
		"""Shell out; output is assumed to be saved usefully somehow.
		Nonzero return code from the shell will raise a BackupError.
		If a callback function is passed, it will receive lines of
		output from the call.
		"""
		if pipe:
			command += "; exit $PIPESTATUS"
		self.debug("runCommand: " + command)
		if callback:
			retval = self.runAndReport(command, callback)
		else:
			retval = os.system(command)
		#print "***** BINGBING retval is '%s' ********" % retval
		if retval:
			raise BackupError("nonzero return code from '%s'" % command)
		return retval
	
	def runAndReport(self, command, callback):
		"""Shell out to a command, and feed output lines to the callback function.
		Returns the exit code from the program once complete.
		stdout and stderr will be combined into a single stream.
		"""
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
		proc = popen2.Popen4(command, 64)
		output = proc.fromchild.read()
		retval = proc.wait()
		if retval:
			raise BackupError("Non-zero return code from '%s'" % command)
		else:
			return output
	
	def debug(self, stuff):
		print "%s: %s %s" % (prettyTime(), self.dbName, stuff)
	
	def buildDir(self, base, version):
		return join(base, self.dbName, version)
	
	def buildPath(self, base, version, filename):
		return join(base, version, "%s-%s-%s" % (self.dbName, version, filename))
	
	def privatePath(self, filename):
		"""Take a given filename in the private dump dir for the selected database."""
		return self.buildPath(self.wiki.privateDir(), self.date, filename)

	def publicPath(self, filename):
		"""Take a given filename in the public dump dir for the selected database.
		If this database is marked as private, will use the private dir instead.
		"""
		return self.buildPath(self.wiki.publicDir(), self.date, filename)
	
	def latestPath(self, filename):
		return self.buildPath(self.wiki.publicDir(), "latest", filename)
	
	def webPath(self, filename):
		return self.buildPath(self.wiki.webDir(), self.date, filename)
	
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
	
	def run(self):
		self.makeDir(join(self.wiki.publicDir(), self.date))
		self.makeDir(join(self.wiki.privateDir(), self.date))
		
		self.status("Cleaning up old dumps for %s" % self.dbName)
		self.cleanOldDumps()
		
		self.status("Starting backup of %s" % self.dbName)
		self.selectDatabaseServer()
		
		self.items = [PrivateTable("user", "User account data."),
			PrivateTable("watchlist", "Users' watchlist settings."),
			PrivateTable("ipblocks", "Data for blocks of IP addresses, ranges, and users."),
			PrivateTable("archive", "Deleted page and revision data."),
			PrivateTable("updates", "Update dataset for OAI updater system."),
			PrivateTable("logging", "Data for various events (deletions, uploads, etc)."),
			#PrivateTable("oldimage", "Metadata on prior versions of uploaded images."),
			#PrivateTable("filearchive", "Deleted image data"),
			
			PublicTable("site_stats", "A few statistics such as the page count."),
			PublicTable("image", "Metadata on current versions of uploaded images."),
			PublicTable("oldimage", "Metadata on prior versions of uploaded images."),
			PublicTable("pagelinks", "Wiki page-to-page link records."),
			PublicTable("categorylinks", "Wiki category membership link records."),
			PublicTable("imagelinks", "Wiki image usage records."),
			PublicTable("templatelinks", "Wiki template inclusion link records."),
			PublicTable("externallinks", "Wiki external URL link records."),
			PublicTable("langlinks", "Wiki interlanguage link records."),
			PublicTable("interwiki", "Set of defined interwiki prefixes and links for this wiki."),
			PublicTable("user_groups", "User group assignments."),
			PublicTable("category", "Category information."),
			
			PublicTable("page", "Base per-page data (id, title, old restrictions, etc)."),
			PublicTable("page_restrictions", "Newer per-page restrictions table."),
			PublicTable("page_props", "Name/value pairs for pages."),
			PublicTable("protected_titles", "Nonexistent pages that have been protected."),
			#PublicTable("revision", "Base per-revision data (does not include text)."), // safe?
			#PrivateTable("text", "Text blob storage. May be compressed, etc."), // ?
			PublicTable("redirect", "Redirect list"),
			
			TitleDump("List of page titles"),
			
			AbstractDump("Extracted page abstracts for Yahoo"),
			
			XmlStub("First-pass for page XML data dumps"),
			XmlDump("articles",
				"<big><b>Articles, templates, image descriptions, and primary meta-pages.</b></big>",
				"This contains current versions of article content, and is the archive most mirror sites will probably want.", prefetch, spawn),
			XmlDump("meta-current",
				"All pages, current versions only.",
				"Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", prefetch, spawn),
			XmlLogging("Pull out all logging data")]
		if self.wiki.hasFlaggedRevs():
			self.items.append(
				PublicTable( "flaggedpages", "This contains a row for each flagged article, containing the stable revision ID, if the lastest edit was flagged, and how long edits have been pending." ))
			self.items.append(
				PublicTable( "flaggedrevs", "This contains a row for each flagged revision, containing who flagged it, when it was flagged, reviewer comments, the flag values, and the quality tier those flags fall under." ))			

		if not self.wiki.isBig():
			self.items.append(
				BigXmlDump("meta-history",
					"All pages with complete page edit history (.bz2)",
					"These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
					"Suitable for archival and statistical use, most mirror sites won't want or need this.", prefetch, spawn))
			self.items.append(
				XmlRecompressDump("meta-history",
					"All pages with complete edit history (.7z)",
					"These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
					"Suitable for archival and statistical use, most mirror sites won't want or need this."))
		
		files = self.listFilesFor(self.items)
		self.prepareChecksums()
		
		for item in self.items:
			item.start(self)
			self.updateStatusFiles()
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
			if item.status == "failed":
				if self.failCount < 1:
					# Email the site administrator just once per database
					self.reportFailure()
				self.failCount += 1
				self.lastFailed = True
			else:
				for f in item.listFiles(self):
					self.saveSymlink(f)
					self.saveFeed(f)
					self.checksum(f)
				self.lastFailed = False

		self.updateStatusFiles(done=True)

		if self.failCount < 1:
			self.completeDump(files)
		
		self.statusComplete()
	
	def cleanOldDumps(self):
		old = self.wiki.dumpDirs()
		if old:
			if old[-1] == self.date:
				# If we're re-running today's dump, don't count it as one
				# of the old dumps to keep... or delete it halfway through!
				old = old[:-1]
			if self.config.keep > 0:
				# Keep the last few
				old = old[:-(self.config.keep)]
		if old:
			for dump in old:
				self.status("Purging old dump %s for %s" % (dump, self.dbName))
				base = os.path.join(self.wiki.publicDir(), dump)
				command = "rm -rf %s" % shellEscape(base)
				self.runCommand(command)
		else:
			self.status("No old dumps to purge.")
	
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
	
	def updateStatusFiles(self, done=False):
		self.saveStatus(self.items, done)
	
	def saveStatus(self, items, done=False):
		"""Write out an HTML file with the status for this wiki's dump and links to completed files."""
		try: 
			self.wiki.writeIndex(self.reportStatus(items, done))
		
			# Short line for report extraction
			self.wiki.writeStatus(self.reportDatabase(items, done))
		except:
			print "Couldn't update status files. Continuing anyways"
	
	def progressReports(self):
		status = {}
		for db in self.dblist:
			item = self.readProgress(db)
			if item:
				status[db] = item
		# sorted by name...
		return [status[db] for db in self.dblist if db in status]
	
	def readProgress(self, db):
		dir = self.latestDump(db)
		if dir:
			status = join(self.publicBase(db), db, dir, "status.html")
			try:
				return readFile(status)
			except:
				return "<li>%s missing status record</li>" % db
		else:
			self.debug("No dump dir for %s?" % db)
			return None
	
	def reportDatabase(self, items, done=False):
		"""Put together a brief status summary and link for the current database."""
		status = self.reportStatusLine(done)
		html = self.wiki.reportStatusLine(status)
		
		activeItems = [x for x in items if x.status == "in-progress"]
		if activeItems:
			return html + "<ul>" + "\n".join([self.reportItem(x) for x in activeItems]) + "</ul>"
		else:
			return html
	
	def reportStatus(self, items, done=False):
		"""Put together a status page for this database, with all its component dumps."""
		statusItems = [self.reportItem(item) for item in items]
		statusItems.reverse()
		html = "\n".join(statusItems)
		return self.config.readTemplate("report.html") % {
			"db": self.dbName,
			"date": self.date,
			"status": self.reportStatusLine(done),
			"previous": self.reportPreviousDump(done),
			"items": html,
			"checksum": self.webPath("md5sums.txt"),
			"index": self.config.index}
	
	def reportPreviousDump(self, done):
		"""Produce a link to the previous dump, if any"""
		try:
			raw = self.wiki.latestDump(-2)
		except:
			return "No prior dumps of this database stored."
		date = WikiDump.prettyDate(raw)
		if done:
			prefix = ""
			message = "Last dumped on"
		else:
			prefix = "This dump is in progress; see also the "
			message = "previous dump from"
		return "%s<a href=\"../%s/\">%s %s</a>" % (prefix, raw, message, date)
	
	def reportStatusLine(self, done=False):
		if done:
			classes = "done"
			text = "Dump complete"
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
		html = "<li class='%s'><span class='updates'>%s</span> <span class='status'>%s</span> <span class='title'>%s</span>" % (item.status, item.updated, item.status, item.description())
		if item.progress:
			html += "<div class='progress'>%s</div>\n" % item.progress
		files = item.listFiles(self)
		if files:
			listItems = [self.reportFile(file, item.status) for file in files]
			html += "<ul>"
			detail = item.detail()
			if detail:
				html += "<li class='detail'>%s</li>\n" % detail
			html += "\n".join(listItems)
			html += "</ul>"
		html += "</li>"
		return html
	
	# Report on the file size & status of the current output and output a link if were done 
	def reportFile(self, file, status):
		filepath = self.publicPath(file)
		if status == "in-progress" and exists (filepath):
			size = prettySize(getsize(filepath))
			return "<li class='file'>%s %s (written) </li>" % (file, size)
		elif status == "done" and exists(filepath):
			size = prettySize(getsize(filepath))
			webpath = self.webPath(file)
			return "<li class='file'><a href=\"%s\">%s</a> %s</li>" % (webpath, file, size)
		else:
			return "<li class='missing'>%s</li>" % file
	
	def lockFile(self):
		return self.publicPath("lock")
	
	def doneFile(self):
		return self.publicPath("done")
	
	def lock(self):
		self.status("Creating lock file.")
		lockfile = self.lockFile()
		donefile = self.doneFile()
		if exists(lockfile):
			raise BackupError("Lock file %s already exists" % lockfile)
		if exists(donefile):
			self.status("Removing completion marker %s" % donefile)
			os.remove(donefile)
		try:
			os.remove(lockfile)
		except:
			# failure? let it die
			pass
		#####date -u > $StatusLockFile
	
	def unlock(self):
		self.status("Marking complete.")
		######date -u > $StatusDoneFile
	
	def dateStamp(self):
		#date -u --iso-8601=seconds
		pass
	
	def status(self, message):
		#echo $DatabaseName `dateStamp` OK: "$1" | tee -a $StatusLog | tee -a $GlobalLog
		self.debug(message)
	
	def statusComplete(self):
		#  echo $DatabaseName `dateStamp` SUCCESS: "done." | tee -a $StatusLog | tee -a $GlobalLog
		self.debug("SUCCESS: done.")
	
	def prepareChecksums(self):
		"""Create the md5 checksum file at the start of the run.
		This will overwrite a previous run's output, if any."""
		output = file(self.publicPath("md5sums.txt"), "w")
	
	def checksum(self, filename):
		"""Run checksum for an output file, and append to the list."""
		output = file(self.publicPath("md5sums.txt"), "a")
		self.saveChecksum(filename, output)
		output.close()
	
	def saveChecksum(self, file, output):
		self.debug("Checksumming %s" % file)
		path = self.publicPath(file)
		if os.path.exists(path):
			checksum = md5FileLine(path)
			output.write(checksum)
	
	def completeDump(self, files):
		# FIXME: md5sums.txt won't be consistent with mixed data.
		# Buuuuut life sucks, huh?
		self.saveSymlink("md5sums.txt")
	
	def saveSymlink(self, file):
		self.makeDir(join(self.wiki.publicDir(), 'latest'))
		real = self.publicPath(file)
		link = self.latestPath(file)
		if exists(link) or os.path.islink(link):
			if os.path.islink(link):
				self.debug("Removing old symlink %s" % link)
				os.remove(link)
			else:
				raise BackupError("What the hell dude, %s is not a symlink" % link)
		relative = relativePath(real, dirname(link))
		self.debug("Adding symlink %s -> %s" % (link, relative))
		os.symlink(relative, link)
	
	def saveFeed(self, file):
		self.makeDir(join(self.wiki.publicDir(), 'latest'))
		filePath = self.webPath(file)
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
		rssPath = self.latestPath(file + "-rss.xml")
		WikiDump.dumpFile(rssPath, rssText)

class Dump(object):
	def __init__(self, desc):
		self._desc = desc
		self.updated = ""
		self.status = "waiting"
		self.progress = ""
	
	def description(self):
		return self._desc
	
	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return None
	
	def setStatus(self, status):
		self.status = status
		self.updated = prettyTime()
	
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
	
	def progressCallback(self, runner, line):
		"""Receive a status line from a shellout and update the status files."""
		# pass through...
		sys.stderr.write(line)
		self.progress = line.strip()
		runner.updateStatusFiles()
	
	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__

class PublicTable(Dump):
	"""Dump of a table using MySQL's mysqldump utility."""
	
	def __init__(self, table, desc):
		Dump.__init__(self, desc)
		self._table = table
	
	def _file(self):
		return self._table + ".sql.gz"
	
	def _path(self, runner):
		return runner.publicPath(self._file())
	
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
		return runner.privatePath(self._file())
	
	def listFiles(self, runner):
		"""Private table won't have public files to list."""
		return []


class XmlStub(Dump):
	"""Create lightweight skeleton dumps, minus bulk text.
	A second pass will import text from prior dumps or the database to make
	full files for the public."""
	
	def description(self):
		return "Creating split stub dumps..."
	
	def detail(self):
		return "These files contain no page text, only revision metadata."
	
	def listFiles(self, runner):
		return ["stub-meta-history.xml.gz",
			"stub-meta-current.xml.gz",
			"stub-articles.xml.gz",]
	
	def run(self, runner):
		history = runner.publicPath("stub-meta-history.xml.gz")
		current = runner.publicPath("stub-meta-current.xml.gz")
		articles = runner.publicPath("stub-articles.xml.gz")
		for filename in (history, current, articles):
			if exists(filename):
				os.remove(filename)
		command = """
%s -q %s/maintenance/dumpBackup.php \
  --wiki=%s \
  --full \
  --stub \
  --report=10000 \
  %s \
  --server=%s \
  --output=gzip:%s \
  --output=gzip:%s \
	--filter=latest \
  --output=gzip:%s \
	--filter=latest \
	--filter=notalk \
	--filter=namespace:\!NS_USER \
""" % shellEscape((
			runner.config.php,
			runner.config.wikiDir,
			runner.dbName,
			runner.forceNormalOption(),
			runner.dbServer,
			history,
			current,
			articles))
		runner.runCommand(command, callback=self.progressCallback) 

class XmlLogging(Dump):
	""" Create a logging dump of all page activity """
	
	def description(self):
		return "<big><b>Log events to all pages.</big></b>"
	
	def detail(self):
		return "This contains the log of actions performed on pages."
	
	def listFiles(self, runner):
		return ["pages-logging.xml.gz"]
	
	def run(self, runner):
		logging = runner.publicPath("pages-logging.xml.gz")
		if exists(logging):
			os.remove(logging)
		command = """
%s -q %s/maintenance/dumpBackup.php \
  --wiki=%s \
  --logs \
  --report=10000 \
  %s \
  --server=%s \
  --output=gzip:%s \
""" % shellEscape((
			runner.config.php,
			runner.config.wikiDir,
			runner.dbName,
			runner.forceNormalOption(),
			runner.dbServer,
			logging))
		runner.runCommand(command, callback=self.progressCallback)

class XmlDump(Dump):
	"""Primary XML dumps, one section at a time."""
	def __init__(self, subset, desc, detail, prefetch, spawn):
		Dump.__init__(self, desc)
		self._subset = subset
		self._detail = detail
		self._prefetch = prefetch
		self._spawn = spawn

	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return self._detail
	
	def _file(self, ext):
		return "pages-" + self._subset + ".xml." + ext
	
	def _path(self, runner, ext):
		return runner.publicPath(self._file(ext))
	
	def run(self, runner):
		filters = self.buildFilters(runner)
		command = self.buildCommand(runner)
		eta = self.buildEta(runner)
		return runner.runCommand(command + " " + filters + " " + eta,
			callback=self.progressCallback)
	
	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--current"
	
	def buildFilters(self, runner):
		"""Construct the output filter options for dumpTextPass.php"""
		xmlbz2 = self._path(runner, "bz2")
		if runner.config.bzip2[-6:] == "dbzip2":
			bz2mode = "dbzip2"
		else:
			bz2mode = "bzip2"
		return "--output=%s:%s" % shellEscape((bz2mode, xmlbz2))
	
	def buildCommand(self, runner):
		"""Build the command line for the dump, minus output and filter options"""
		
		# Page and revision data pulled from this skeleton dump...
		stub = runner.publicPath("stub-%s.xml.gz" % self._subset),
		stubOption = "--stub=gzip:%s" % stub
		
		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		if self._prefetch:
			source = self._findPreviousDump(runner)
		else:
			source = None
		if source and exists(source):
			runner.status("... building %s XML dump, with text prefetch from %s..." % (self._subset, source))
			prefetch = "--prefetch=bzip2:%s" % (source)
		else:
			runner.status("... building %s XML dump, no text prefetch..." % self._subset)
			prefetch = None

		if self._spawn:
			spawn = "--spawn=%s" % (runner.config.php)
		else:
			spawn = None

		dumpCommand = """
%s -q %s/maintenance/dumpTextPass.php \
  --wiki=%s \
  %s \
  %s \
  %s \
  --report=1000 \
  --server=%s \
  %s""" % shellEscape((
			runner.config.php,
			runner.config.wikiDir,
			runner.dbName,
			stubOption,
			prefetch,
			runner.forceNormalOption(),
			runner.dbServer,
			spawn))
		command = dumpCommand
		return command
	
	def _findPreviousDump(self, runner):
		"""The previously-linked previous successful dump."""
		bzfile = self._file("bz2")
		current = realpath(runner.publicPath(bzfile))
		dumps = runner.wiki.dumpDirs()
		dumps.sort()
		dumps.reverse()
		for date in dumps:
			base = runner.wiki.publicDir()
			old = runner.buildPath(base, date, bzfile)
			print old
			if exists(old):
				size = getsize(old)
				if size < 70000:
					runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, old))
					continue
				if realpath(old) == current:
					runner.debug("skipping current dump for prefetch %s" % old)
					continue
				runner.debug("Prefetchable %s" % old)
				return old
		runner.debug("Could not locate a prefetchable dump.")
		return None
	
	def listFiles(self, runner):
		return [self._file("bz2")]
	
	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__ + "." + self._subset

class BigXmlDump(XmlDump):
	"""XML page dump for something larger, where a 7-Zip compressed copy
	could save 75% of download time for some users."""
	
	def buildEta(self, runner):
		"""Tell the dumper script whether to make ETA estimate on page or revision count."""
		return "--full"

class XmlRecompressDump(Dump):
	"""Take a .bz2 and recompress it as 7-Zip."""
	
	def __init__(self, subset, desc, detail):
		Dump.__init__(self, desc)
		self._subset = subset
		self._detail = detail
	
	def detail(self):
		"""Optionally return additional text to appear under the heading."""
		return self._detail
	
	def _file(self, ext):
		return "pages-" + self._subset + ".xml." + ext
	
	def _path(self, runner, ext):
		return runner.publicPath(self._file(ext))
	
	def run(self, runner):
		if runner.lastFailed:
			raise BackupError("bz2 dump incomplete, not recompressing")
		
		xmlbz2 = self._path(runner, "bz2")
		xml7z = self._path(runner, "7z")
		
		# Clear prior 7zip attempts; 7zip will try to append an existing archive
		if exists(xml7z):
			os.remove(xml7z)
		
		# temp hack force 644 permissions until ubuntu bug # 370618 is fixed - tomasz 5/1/2009
		command = "%s -dc < %s | %s a -si %s ; chmod 644 %s" % shellEscape(( 
			runner.config.bzip2,
			xmlbz2,
			runner.config.sevenzip,
			xml7z,
			xml7z));

		return runner.runCommand(command, callback=self.progressCallback)
		
	def listFiles(self, runner):
		return [self._file("7z")]
	
	def matchCheckpoint(self, checkpoint):
		return checkpoint == self.__class__.__name__ + "." + self._subset

class AbstractDump(Dump):
	"""XML dump for Yahoo!'s Active Abstracts thingy"""
	
	def run(self, runner):
		command = """
%s -q %s/maintenance/dumpBackup.php \
  --wiki=%s \
  --plugin=AbstractFilter:%s/extensions/ActiveAbstract/AbstractFilter.php \
  --current \
  --report=1000 \
  %s \
  --server=%s \
""" % shellEscape((
				runner.config.php,
				runner.config.wikiDir,
				runner.dbName,
				runner.config.wikiDir,
				runner.forceNormalOption(),
				runner.dbServer))
		for variant in self._variants(runner):
			command = command + """  --output=file:%s \
    --filter=namespace:NS_MAIN \
    --filter=noredirect \
    --filter=abstract%s \
""" % shellEscape((
				runner.publicPath(self._variantFile(variant)),
				self._variantOption(variant)))
		command = command + "\n"
		runner.runCommand(command, callback=self.progressCallback)
	
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
	
	def _variantFile(self, variant):
		if variant == "":
			return "abstract.xml"
		else:
			return "abstract-%s.xml" % variant
	
	def listFiles(self, runner):
		return [self._variantFile(x) for x in self._variants(runner)]
	
class TitleDump(Dump):
	"""This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""
	def run(self, runner):
		return runner.saveSql("select page_title from page where page_namespace=0;",
			runner.publicPath("all-titles-in-ns0.gz"))
	
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
			
if __name__ == "__main__":
	try:
		config = WikiDump.Config()
		
		date = None
		checkpoint = None
		forceLock = False
		prefetch = True
		spawn = True

		(options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
			['date=', 'checkpoint=', 'force', 'noprefetch', 'nospawn'])
		for (opt, val) in options:
			if opt == "--date":
				date = val
			elif opt == "--checkpoint":
				checkpoint = val
			elif opt == "--force":
				forceLock = True
			elif opt == "--noprefetch":
				prefetch = False
			elif opt == "--nospawn":
				spawn = False

		if len(remainder) > 0:
			wiki = WikiDump.Wiki(config, remainder[0])
			if forceLock:
				if wiki.isLocked():
					wiki.unlock()
			wiki.lock()
		else:
			wiki = findAndLockNextWiki(config)
	
		if wiki:
			runner = Runner(wiki, date, checkpoint, prefetch, spawn)
			print "Running %s..." % wiki.dbName
			runner.run()
			wiki.unlock()
		else:
			print "No wikis available to run."
	finally:
		WikiDump.cleanup()
