#!/usr/bin/python

"""Backup/public data dump runner for Wikimedia's MediaWiki-based sites.

This replaces the old set of hacky bash scripts we used to use.

Current state:
* Seems to dump basic files correctly on my test system.

TODO:
* detect handle error conditions ;)
* lock files / looping
* generate HTML pages with status and navigable links (part-done)
* generate file checksums
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
	php="/opt/php51/bin/php"
	webroot="/dumps")
runner.run()
"""

import os
import time

def dbList(filename):
	infile = open(filename)
	dbs = []
	for line in infile:
		line = line.strip()
		if line != "":
			dbs.append(line)
	infile.close()
	return dbs

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

class BackupError(Exception):
	pass

class Runner(object):
	def __init__(self, public, private, dblist, privatelist, dbserver, dbuser, dbpassword, wikidir, php="php", webroot=""):
		self.public = public
		self.private = private
		self.dblist = dblist
		self.privatelist = privatelist
		self.dbserver = dbserver
		self.dbuser = dbuser
		self.dbpassword = dbpassword
		self.wikidir = wikidir
		self.php = php
		self.webroot = webroot
		self.db = None
		self.date = None
	
	"""Public methods for the manager script..."""
	
	def run(self):
		"""Iterate through the list of wikis and dump them!"""
		self.debug("Starting dump...")
		for db in self.dblist:
			self.db = db
			self.date = today()
			self.doBackup()
		self.debug("Done!")
	
	"""Public methods for dumps to use..."""
	
	def publicBase(self):
		"""Return the base directory tree to put public files into.
		If a private wiki is selected, all files will go into the private dir.
		"""
		if self.db in self.privatelist:
			return self.private
		else:
			return self.public
	
	def privateDir(self):
		return self.buildDir(self.private, self.date)
	
	def publicDir(self):
		return self.buildDir(self.publicBase(), self.date)
	
	def latestDir(self):
		return self.buildDir(self.publicBase(), "latest")
	
	def webDir(self):
		"""Get the relative URL path for thingies
		FIXME: may fail on non-Unix systems. HAHAHAHA
		"""
		return self.buildDir(self.webroot, self.date)
	
	
	def privatePath(self, filename):
		"""Take a given filename in the private dump dir for the selected database."""
		return self.buildPath(self.privateDir(), self.date, filename)
	
	def publicPath(self, filename):
		"""Take a given filename in the public dump dir for the selected database.
		If this database is marked as private, will use the private dir instead.
		"""
		return self.buildPath(self.publicDir(), self.date, filename)
	
	def latestPath(self, filename):
		return self.buildPath(self.latestDir(), "latest", filename)
	
	def webPath(self, filename):
		return self.buildPath(self.webDir(), self.date, filename)
	
	
	def passwordOption(self):
		"""If you pass '-pfoo' mysql uses the password 'foo', but if you pass '-p' it prompts. Sigh."""
		if self.dbpassword == "":
			return None
		else:
			return "-p" + shellEscape(self.dbpassword)
	
	def saveTable(self, table, outfile):
		"""Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
		command = "mysqldump -h %s -u %s %s --opt --quote-names %s %s | gzip" % shellEscape((
			self.dbserver,
			self.dbuser,
			self.passwordOption(),
			self.db,
			table))
		return self.saveCommand(command, outfile)
	
	def saveSql(self, query, outfile):
		"""Pass some SQL commands to the server for this DB and save output to a file."""
		command = "echo %s | mysql -h %s -u %s %s %s | gzip" % shellEscape((
			query,
			self.dbserver,
			self.dbuser,
			self.passwordOption(),
			self.db))
		return self.saveCommand(command, outfile)
	
	def saveCommand(self, command, outfile):
		"""Shell out and redirect output to a given file."""
		return self.runCommand(command + " > " + shellEscape(outfile))
	
	def runCommand(self, command):
		"""Shell out; output is assumed to be saved usefully somehow.
		Nonzero return code from the shell will raise a BackupError.
		"""
		self.debug("runCommand: " + command)
		retval = os.system(command)
		#print "***** BINGBING retval is '%s' ********" % retval
		if retval:
			raise BackupError("nonzero return code from '%s'" % command)
		return retval
	
	def debug(self, stuff):
		print "%s: %s %s" % (prettyTime(), self.db, stuff)
	
	def buildDir(self, base, version):
		return os.path.join(base, self.db, version)
	
	def buildPath(self, base, version, filename):
		return os.path.join(base, "%s-%s-%s" % (self.db, version, filename))
	
	def makeDir(self, dir):
		if os.path.exists(dir):
			self.debug("Checkdir dir %s ..." % dir)
		else:
			self.debug("Creating %s ..." % dir)
			os.makedirs(dir)

	def doBackup(self):
		self.makeDir(self.publicDir())
		self.makeDir(self.privateDir())
		
		self.status("Starting backup of %s" % self.db)
		self.lock()
		
		items = [PrivateTable("user", "User account data."),
			PrivateTable("user_groups", "User group assignments."),
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
			PublicTable("interwiki", "Set of defined interwiki prefixes and links for this wiki."),
			PublicTable("logging", "Data for various events (deletions, uploads, etc)."),
			
			PublicTable("page", "Base per-page data (id, title, restrictions, etc)."),
			#PublicTable("revision", "Base per-revision data (does not include text)."), // safe?
			#PrivateTable("text", "Text blob storage. May be compressed, etc."), // ?
			
			TitleDump("List of page titles"),
			
			XmlStub("First-pass for page XML data dumps"),
			XmlDump("full", "All pages with complete page edit history (very large!)"),
			XmlDump("current", "All pages, current versions only"),
			XmlDump("articles", "Articles, templates, image descriptions, and main meta-pages (recommended)"),
			
			AbstractDump("Extracted page abstracts for Yahoo")]
		
		files = self.listFilesFor(items)
		
		for item in items:
			item.start(self)
			self.saveStatus(items)
			try:
				item.dump(self)
			except Exception, ex:
				self.debug("*** exception! " + str(ex))
			self.saveStatus(items)

		self.checksums(files)
		self.completeDump(files)

		self.unlock()
		self.statusComplete()
	
	def listFilesFor(self, items):
		files = []
		for item in items:
			for file in item.listFiles(self):
				files.append(file)
		return files
	
	def saveStatus(self, items):
		"""Write out an HTML file with the status for this wiki's dump and links to completed files."""
		html = self.reportStatus(items)
		index = os.path.join(self.publicDir(), "index.html")
		file = open(index, "wt")
		file.write(html)
		file.close()
	
	def reportStatus(self, items):
		html = "\n".join([self.reportItem(item) for item in items])
		return html
	
	def reportItem(self, item):
		html = "<li>%s %s %s:" % (item.updated, item.status, item.description())
		files = item.listFiles(self)
		if files:
			html += "<ul>"
			html += "\n".join([self.reportFile(file) for file in files])
			html += "</ul>"
		html += "</li>"
		return html
	
	def reportFile(self, file):
		filepath = self.publicPath(file)
		if os.path.exists(filepath):
			size = prettySize(os.path.getsize(filepath))
			webpath = self.webPath(file)
			return "<li><a href=\"%s\">%s</a> %s</li>" % (webpath, file, size)
		else:
			return "<li>%s</li>" % file
	
	def lockFile(self):
		return self.publicPath("lock")
	
	def doneFile(self):
		return self.publicPath("done")

	def lock(self):
		self.status("Creating lock file.")
		lockfile = self.lockFile()
		donefile = self.doneFile()
		if os.path.exists(lockfile):
			raise BackupError("Lock file %s already exists" % lockfile)
		if os.path.exists(donefile):
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

	def statusError(self, message):
		#  echo $DatabaseName `dateStamp` ABORT: "$1" | tee -a $StatusLog | tee -a $GlobalLog
		#  echo "Backup of $DatabaseName failed at: $1" | \
		#	mail -s "Wikimedia backup error on $DatabaseName" $AbortEmail
		#  exit -1
		self.debug(message)

	def statusComplete(self):
		#  echo $DatabaseName `dateStamp` SUCCESS: "done." | tee -a $StatusLog | tee -a $GlobalLog
		self.debug("SUCCESS: done.")
	
	def checksums(self, files):
		self.debug("If this script were finished, it would be checksumming files here")
	
	def completeDump(self, files):
		self.debug("If this script were finished, it would be adding symlinks or something")
		self.makeDir(self.latestDir())
		for file in files:
			self.saveSymlink(file)
	
	def saveSymlink(self, file):
		real = self.publicPath(file)
		link = self.latestPath(file)
		if os.path.exists(link):
			if os.path.islink(link):
				self.debug("Removing old symlink %s" % link)
				os.remove(link)
			else:
				raise BackupError("What the hell dude, %s is not a symlink" % link)
		self.debug("Adding symlink %s -> %s" % (link, real))
		os.symlink(real, link)

class Dump(object):
	def __init__(self, desc):
		self._desc = desc
		self.updated = ""
		self.status = "waiting"
	
	def description(self):
		return self._desc
	
	def setStatus(self, status):
		self.status = status
		self.updated = prettyTime()
	
	def listFiles(self, runner):
		"""Return a list of filenames which should be exported and checksummed"""
		return []
	
	def start(self, runner):
		"""Set the 'in progress' flag so we can output status."""
		self.setStatus("in progress")
	
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
		return "creating split stub dumps..."
	
	def run(self, runner):
		command = """
%s -q %s/maintenance/dumpBackup.php %s \
  --full \
  --stub \
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
			runner.privatePath("stub-full.xml.gz"),
			runner.privatePath("stub-current.xml.gz"),
			runner.privatePath("stub-articles.xml.gz")))
		runner.runCommand(command)

class XmlDump(Dump):
	"""Primary XML dumps, one section at a time."""
	def __init__(self, subset, desc):
		Dump.__init__(self, desc)
		self._subset = subset
	
	def _file(self, ext):
		return "pages-" + self._subset + ".xml." + ext
	
	def _path(self, runner, ext):
		return runner.publicPath(self._file(ext))
	
	def run(self, runner):
		xmlbz2 = self._path(runner, "bz2")
		xml7z = self._path(runner, "7z")
		
		# Clear prior 7zip attempts; 7zip will try to append an existing archive
		if os.path.exists(xml7z):
			os.remove(xml7z)
		
		filters = "--output=bzip2:%s --output=7zip:%s" % shellEscape((
			xmlbz2,
			xml7z))
		command = self._buildCommand(runner)
		
		return runner.runCommand(command + " " + filters)
	
	def _buildCommand(self, runner):
		"""Build the command line for the dump, minus output and filter options"""
		
		# Page and revision data pulled from this skeleton dump...
		stub = runner.privatePath("stub-%s.xml.gz" % self._subset),
		stubOption = "--stub=gzip:%s" % stub

		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		source = self._findPreviousDump(runner)
		if os.path.exists(source):
			runner.status("... building %s XML dump, with text prefetch from %s..." % (self._subset, source))
			prefetch = "--prefetch=bzip2:%s" % (source)
		else:
			runner.status("... building %s XML dump, no text prefetch..." % self._subset)
			prefetch = None
		
		dumpCommand = "%s -q %s/maintenance/dumpTextPass.php %s %s %s" % shellEscape((
			runner.php,
			runner.wikidir,
			runner.db,
			stubOption,
			prefetch))
		command = dumpCommand
		return command
	
	def _findPreviousDump(self, runner):
		return "/tmp/fake/foo"
	
	def listFiles(self, runner):
		return [self._file("bz2"), self._file("7z")]


class AbstractDump(Dump):
	"""XML dump for Yahoo!'s Active Abstracts thingy"""
	
	def run(self, runner):
		command = """
%s -q %s/maintenance/dumpBackup.php %s \
  --plugin=AbstractFilter:%s/extensions/ActiveAbstract/AbstractFilter.php \
  --current \
  --output=gzip:%s \
    --filter=namespace:NS_MAIN \
    --filter=noredirect \
    --filter=abstract
""" % shellEscape((
			runner.php,
			runner.wikidir,
			runner.db,
			runner.wikidir,
			runner.publicPath("abstract.xml.gz")))
		runner.runCommand(command)
	
	def listFiles(self, runner):
		return ["abstract.xml.gz"]
	
	
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
