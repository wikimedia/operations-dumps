#!/usr/bin/python

"""Backup/public data dump runner for Wikimedia's MediaWiki-based sites.

This replaces the old set of hacky bash scripts we used to use.

Current state:
* Seems to dump basic files correctly on my test system.

TODO:
* lock files / looping
* make upload tarballs?
* detect low disk space and either call for help or automatically clear old files


To run, make a wrapper script something like this:
runner = WikiBackup.Runner(
	public="/dumps/public",
	private="/dumps/private",
	dblist=["onesix"],
	privatelist=(),
	dbserver="localhost",
	dbuser="root",
	dbpassword="",
	wikidir="/opt/web/pages/head",
	php="/opt/php51/bin/php",
	bzip2="/usr/local/bin/dbzip",
	sevenzip="/sw/bin/7za",
	webroot="http://dumps.example.com/dumps",
	adminmail="root@localhost",
	mailfrom="root@localhost")
runner.run()
"""

import email.MIMEText
import md5
import os
import popen2
import re
import smtplib
import sys
import time

from os.path import dirname, exists, getsize, join, realpath

def dbList(filename):
	infile = open(filename)
	dbs = []
	for line in infile:
		line = line.strip()
		if line != "":
			dbs.append(line)
	infile.close()
	return dbs

def rotateList(stuff, start):
	"""If the given item is in the list, return a rotated copy of the list
	which starts at that item and wraps around. If not in the list, returns
	a copy of the original list."""
	out = []
	split = 0
	for i in range(0, len(stuff)):
		if start == stuff[i]:
			split = i
			break
	for i in range(split, len(stuff)):
		out.append(stuff[i])
	for i in range(0, split):
		out.append(stuff[i])
	return out

def shellEscape(param):
	"""Escape a string parameter, or set of strings, for the shell."""
	if isinstance(param, basestring):
		return "'" + param.replace("'", "'\\''") + "'"
	elif param is None:
		# A blank string might actually be needed; None means we can leave it out
		return ""
	else:
		return tuple([shellEscape(x) for x in param])

def prettySize(size):
	"""Return a string with an attractively formatted file size."""
	quanta = ("%d bytes", "%d KB", "%0.1f MB", "%0.1f GB", "%0.1f TB")
	return _prettySize(size, quanta)

def _prettySize(size, quanta):
	if size < 1024 or len(quanta) == 1:
		return quanta[0] % size
	else:
		return _prettySize(size / 1024.0, quanta[1:])

def today():
	return time.strftime("%Y%m%d", time.gmtime())

def prettyTime():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def prettyDate(key):
	"Prettify a MediaWiki date key"
	return "-".join((key[0:4], key[4:6], key[6:8]))

def dumpFile(filename, text):
	"""Dump a string to a file."""
	file = open(filename, "wt")
	file.write(text)
	file.close()

def readFile(filename):
	file = open(filename, "r")
	text = file.read()
	file.close()
	return text

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

def quickMail(mailserver, fromaddr, toaddr, subject, body):
	"""Send out a quickie email."""
	message = email.MIMEText.MIMEText(body)
	message["Subject"] = subject
	message["From"] = fromaddr
	message["To"] = toaddr
	
	try:
		server = smtplib.SMTP(mailserver)
		server.sendmail(fromaddr, toaddr, message.as_string())
		server.close()
	except:
		print "MAIL SEND FAILED! GODDAMIT! Was sending this mail:"
		print message

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

class BackupError(Exception):
	pass

class Runner(object):
	
	def __init__(self, public, private, dblist, privatelist, dbserver=None,
			dbuser="", dbpassword="", wikidir="", php="php", webroot="",
			template=dirname(realpath(sys.modules[__module__].__file__)),
			tmp="/tmp", adminmail=None, mailfrom="root@localhost",
			mailserver="localhost", bzip2="bzip2", sevenzip="7za",
			normalization=None):
		self.public = public
		self.private = private
		self.dblist = dblist
		self.privatelist = privatelist
		self.dbserverOverride = dbserver
		self.dbserver = None
		self.dbuser = dbuser
		self.dbpassword = dbpassword
		self.wikidir = wikidir
		self.php = php
		self.webroot = webroot
		self.template = template
		self.tmp = tmp
		self.adminmail = adminmail
		self.mailfrom = mailfrom
		self.mailserver = mailserver
		self.bzip2 = bzip2
		self.sevenzip = sevenzip
		if normalization:
			self.normalization = "--force-normal"
		else:
			self.normalization = ""
		
		self.db = None
		self.date = None
		self.failcount = 0
		self.lastFailed = False
	
	"""Public methods for the manager script..."""
	
	def run(self, subset=[], skip=[], start=""):
		"""Iterate through the list of wikis and dump them!"""
		self.debug("Starting dump...")
		if subset:
			runset = subset
		else:
			runset = self.dblist
		if skip:
			runset = [x for x in runset if x not in skip]
		if start:
			runset = rotateList(runset, start)
		for db in runset:
			self.db = db
			self.date = today()
			self.failcount = 0
			self.doBackup()
		self.saveIndex(done=True)
		self.debug("Done!")
	
	"""Public methods for dumps to use..."""
	
	def publicBase(self, db=None):
		"""Return the base directory tree to put public files into.
		If a private wiki is selected, all files will go into the private dir.
		"""
		if db is None:
			db = self.db # use the db being processed currently
		if db in self.privatelist:
			return self.private
		else:
			return self.public
	
	def privateDir(self):
		return self.buildDir(self.private, self.date)
	
	def publicDir(self):
		return self.buildDir(self.publicBase(), self.date)
	
	def latestDir(self):
		return self.buildDir(self.publicBase(), "latest")
	
	
	def privatePath(self, filename):
		"""Take a given filename in the private dump dir for the selected database."""
		return self.buildPath(self.privateDir(), self.date, filename)
	
	def publicPath(self, filename):
		"""Take a given filename in the public dump dir for the selected database.
		If this database is marked as private, will use the private dir instead.
		"""
		return self.buildPath(self.publicDir(), self.date, filename)
	
	def tmpPath(self, filename):
		"""Return a filename in the temporary directory based on the given name."""
		return join(self.tmp, self.db + "-tmp-" + filename)
	
	def latestPath(self, filename):
		return self.buildPath(self.latestDir(), "latest", filename)
	
	def webPath(self, filename):
		return self.buildPath(".", self.date, filename)
	
	
	def passwordOption(self):
		"""If you pass '-pfoo' mysql uses the password 'foo', but if you pass '-p' it prompts. Sigh."""
		if self.dbpassword == "":
			return None
		else:
			return "-p" + self.dbpassword
	
	def saveTable(self, table, outfile):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		command = "mysqldump -h %s -u %s %s --opt --quote-names %s %s | gzip" % shellEscape((
			self.dbserver,
			self.dbuser,
			self.passwordOption(),
			self.db,
			table))
		return self.saveCommand(command, outfile, pipe=True)
	
	def saveSql(self, query, outfile):
		"""Pass some SQL commands to the server for this DB and save output to a file."""
		command = "echo %s | mysql -h %s -u %s %s %s | gzip" % shellEscape((
			query,
			self.dbserver,
			self.dbuser,
			self.passwordOption(),
			self.db))
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
		print "%s: %s %s" % (prettyTime(), self.db, stuff)
	
	def buildDir(self, base, version):
		return join(base, self.db, version)
	
	def buildPath(self, base, version, filename):
		return join(base, "%s-%s-%s" % (self.db, version, filename))
	
	def makeDir(self, dir):
		if exists(dir):
			self.debug("Checkdir dir %s ..." % dir)
		else:
			self.debug("Creating %s ..." % dir)
			os.makedirs(dir)
	
	def selectDatabaseServer(self):
		if self.dbserverOverride:
			self.dbserver = self.dbserverOverride
		else:
			self.dbserver = self.defaultServer()
	
	def defaultServer(self):
		command = "%s -q %s/maintenance/getSlaveServer.php %s" % shellEscape((
			self.php, self.wikidir, self.db))
		return self.runAndReturn(command).strip()
	
	def doBackup(self):
		self.makeDir(self.publicDir())
		self.makeDir(self.privateDir())
		
		self.status("Starting backup of %s" % self.db)
		self.lock()
		self.selectDatabaseServer()
		
		self.items = [PrivateTable("user", "User account data."),
			PrivateTable("watchlist", "Users' watchlist settings."),
			PrivateTable("ipblocks", "Data for blocks of IP addresses, ranges, and users."),
			PrivateTable("archive", "Deleted page and revision data."),
			PrivateTable("updates", "Update dataset for OAI updater system."),
			
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
			PublicTable("logging", "Data for various events (deletions, uploads, etc)."),
			PublicTable("user_groups", "User group assignments."),
			
			PublicTable("page", "Base per-page data (id, title, old restrictions, etc)."),
			PublicTable("page_restrictions", "Newer per-page restrictions table."),
			#PublicTable("revision", "Base per-revision data (does not include text)."), // safe?
			#PrivateTable("text", "Text blob storage. May be compressed, etc."), // ?
			PublicTable("redirect", "Redirect list"),
			
			TitleDump("List of page titles"),
			
			AbstractDump("Extracted page abstracts for Yahoo"),
			
			XmlStub("First-pass for page XML data dumps"),
			XmlDump("articles",
				"<big><b>Articles, templates, image descriptions, and primary meta-pages.</b></big>",
				"This contains current versions of article content, and is the archive most mirror sites will probably want."),
			XmlDump("meta-current",
				"All pages, current versions only.",
				"Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material."),
			#SearchIndex("Updating search index"),
			BigXmlDump("meta-history",
				"All pages with complete page edit history (.bz2)",
				"These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
				"Suitable for archival and statistical use, most mirror sites won't want or need this."),
			XmlRecompressDump("meta-history",
				"All pages with complete edit history (.7z)",
				"These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
				"Suitable for archival and statistical use, most mirror sites won't want or need this.")]
		
		files = self.listFilesFor(self.items)
		self.prepareChecksums()
		
		for item in self.items:
			item.start(self)
			self.updateStatusFiles()
			try:
				item.dump(self)
			except Exception, ex:
				self.debug("*** exception! " + str(ex))
			if item.status == "failed":
				if self.failcount < 1:
					# Email the site administrator just once per database
					self.reportFailure()
				self.failcount += 1
				self.lastFailed = True
			else:
				self.checksums(item.listFiles(self))
				self.lastFailed = False

		self.updateStatusFiles(done=True)

		if self.failcount < 1:
			self.completeDump(files)
		
		self.unlock()
		self.statusComplete()
	
	def reportFailure(self):
		if self.adminmail:
			subject = "Dump failure for " + self.db
			message = self.readTemplate("errormail.txt") % {
				"db": self.db,
				"date": self.date,
				"time": prettyTime(),
				"url": self.webroot + "/" + self.db + "/" + self.date + "/"}
			quickMail(self.mailserver, self.mailfrom, self.adminmail, subject, message)
	
	def listFilesFor(self, items):
		files = []
		for item in items:
			for file in item.listFiles(self):
				files.append(file)
		return files
	
	def updateStatusFiles(self, done=False):
		self.saveStatus(self.items, done)
		self.saveIndex()
	
	def saveStatus(self, items, done=False):
		"""Write out an HTML file with the status for this wiki's dump and links to completed files."""
		html = self.reportStatus(items, done)
		index = join(self.publicDir(), "index.html")
		dumpFile(index, html)
		
		# Short line for report extraction
		html = self.reportDatabase(items, done)
		index = join(self.publicDir(), "status.html")
		dumpFile(index, html)
	
	def saveIndex(self, done=False):
		html = self.reportIndex(done)
		index = join(self.public, "backup-index.html")
		dumpFile(index, html)
	
	def reportIndex(self, done=False):
		"""Put together the list of dumped databases as it goes..."""
		if done:
			status = "Dump process is idle."
		else:
			status = "Dumps are in progress..."
		html = "\n".join(self.progressReports())
		return self.readTemplate("progress.html") % {
			"status": status,
			"items": html}
	
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
	
	def latestDump(self, db, index=-1):
		"""Find the last (or slightly less than last) dump for a db."""
		dirs = self.dumpDirs(db)
		if dirs:
			return dirs[index]
		else:
			return None
	
	def dumpDirs(self, db):
		"""List all dump directories for the given database."""
		base = join(self.publicBase(db), db)
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
	
	def reportDatabase(self, items, done=False):
		"""Put together a brief status summary and link for the current database."""
		status = self.reportStatusLine(done)
		if self.db in self.privatelist:
			link = "%s (private data)" % self.db
		else:
			link = "<a href=\"%s/%s\">%s</a>" % (self.db, self.date, self.db)
		html = "<li>%s %s: %s</li>\n" % (prettyTime(), link, status)
		
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
		return self.readTemplate("report.html") % {
			"db": self.db,
			"date": self.date,
			"status": self.reportStatusLine(done),
			"previous": self.reportPreviousDump(done),
			"items": html,
			"checksum": self.webPath("md5sums.txt")}
	
	def reportPreviousDump(self, done):
		"""Produce a link to the previous dump, if any"""
		try:
			raw = self.latestDump(self.db, -2)
		except:
			return "No prior dumps of this database stored.";
		date = prettyDate(raw)
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
		if self.failcount > 0:
			classes += " failed"
			if self.failcount == 1:
				ess = ""
			else:
				ess = "s"
			text += ", %d item%s failed" % (self.failcount, ess)
		return "<span class='%s'>%s</span>" % (classes, text)
	
	def readTemplate(self, name):
		template = join(self.template, name)
		return readFile(template)
	
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
	
	def reportFile(self, file, status):
		filepath = self.publicPath(file)
		if status == "done" and exists(filepath):
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
	
	def checksums(self, files):
		"""Run checksums for a set of output files, and append to the list."""
		output = file(self.publicPath("md5sums.txt"), "a")
		for filename in files:
			self.saveChecksum(filename, output)
		output.close()
	
	def saveChecksum(self, file, output):
		self.debug("Checksumming %s" % file)
		path = self.publicPath(file)
		if os.path.exists(path):
			checksum = md5FileLine(path)
			output.write(checksum)
	
	def completeDump(self, files):
		self.makeDir(self.latestDir())
		for file in files:
			self.saveSymlink(file)
		self.saveSymlink("md5sums.txt")
	
	def saveSymlink(self, file):
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
			"stub-articles.xml.gz"]
	
	def run(self, runner):
		history = runner.publicPath("stub-meta-history.xml.gz")
		current = runner.publicPath("stub-meta-current.xml.gz")
		articles = runner.publicPath("stub-articles.xml.gz")
		for filename in (history, current, articles):
			if exists(filename):
				os.remove(filename)
		command = """
%s -q %s/maintenance/dumpBackup.php %s \
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
			runner.php,
			runner.wikidir,
			runner.db,
			runner.normalization,
			runner.dbserver,
			history,
			current,
			articles))
		runner.runCommand(command, callback=self.progressCallback)

class XmlDump(Dump):
	"""Primary XML dumps, one section at a time."""
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
		return "--output=dbzip2:%s" % shellEscape(xmlbz2)
	
	def buildCommand(self, runner):
		"""Build the command line for the dump, minus output and filter options"""
		
		# Page and revision data pulled from this skeleton dump...
		stub = runner.publicPath("stub-%s.xml.gz" % self._subset),
		stubOption = "--stub=gzip:%s" % stub
		
		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		source = self._findPreviousDump(runner)
		if source and exists(source):
			runner.status("... building %s XML dump, with text prefetch from %s..." % (self._subset, source))
			prefetch = "--prefetch=bzip2:%s" % (source)
		else:
			runner.status("... building %s XML dump, no text prefetch..." % self._subset)
			prefetch = None
		
		dumpCommand = """
%s -q %s/maintenance/dumpTextPass.php %s \
  %s \
  %s \
  %s \
  --report=1000 \
  --server=%s""" % shellEscape((
			runner.php,
			runner.wikidir,
			runner.db,
			stubOption,
			prefetch,
			runner.normalization,
			runner.dbserver))
		command = dumpCommand
		return command
	
	def _findPreviousDump(self, runner):
		"""The previously-linked previous successful dump."""
		bzfile = self._file("bz2")
		current = realpath(runner.publicPath(bzfile))
		dumps = runner.dumpDirs(runner.db)
		dumps.sort()
		dumps.reverse()
		for date in dumps:
			base = join(runner.publicBase(runner.db), runner.db, date)
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
		
		command = "%s -dc < %s | %s a -si %s" % shellEscape((
			runner.bzip2,
			xmlbz2,
			runner.sevenzip,
			xml7z));
		return runner.runCommand(command, callback=self.progressCallback)
		
	def listFiles(self, runner):
		return [self._file("7z")]

class AbstractDump(Dump):
	"""XML dump for Yahoo!'s Active Abstracts thingy"""
	
	def run(self, runner):
		command = """
%s -q %s/maintenance/dumpBackup.php %s \
  --plugin=AbstractFilter:%s/extensions/ActiveAbstract/AbstractFilter.php \
  --current \
  --report=1000 \
  %s \
  --server=%s \
""" % shellEscape((
				runner.php,
				runner.wikidir,
				runner.db,
				runner.wikidir,
				runner.normalization,
				runner.dbserver))
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
		if runner.db[0:2] == "zh" and runner.db[2:3] != "_":
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


class Checksums(Dump):
	def description(self):
		return "calculating MD5 hashes"
	
	def run(self, runner):
		# FIXME: run checksums only on the master server?
		command = "md5sum " + \
			runner.publicPath("*.xml.*") + " " + \
			runner.publicPath("*.sql.gz") + " " + \
			runner.publicPath("all-titles-in-ns0.gz")
		return runner.saveCommand(command, runner.publicPath("md5sums.txt"))

class SearchIndex(Dump):
	def run(self, runner):
		lockfile = "/tmp/search-build-" + runner.db
		command = "touch %s && MWSearchTool --import=%s %s && rm -f %s" % \
			shellEscape((
				lockfile,
				runner.publicPath("pages-meta-current.xml.bz2"),
				runner.db,
				lockfile))
		return runner.runCommand(command, callback=self.progressCallback)
