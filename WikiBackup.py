#!/usr/bin/python

"""Backup/public data dump runner for Wikimedia's MediaWiki-based sites.

This replaces the old set of hacky bash scripts we used to use.

Current state:
* Seems to dump basic files correctly on my test system.

TODO:
* detect handle error conditions ;)
* lock files / looping
* use date-based subdirectories
* generate HTML pages with status and navigable links
* generate file checksums
* symlink files to a stable directory on completion
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
	php="/opt/php51/bin/php")
runner.run()
"""

import os

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

class Runner(object):
	def __init__(self, public, private, dblist, privatelist, dbserver, dbuser, dbpassword, wikidir, php="php"):
		self.public = public
		self.private = private
		self.dblist = dblist
		self.privatelist = privatelist
		self.dbserver = dbserver
		self.dbuser = dbuser
		self.dbpassword = dbpassword
		self.wikidir = wikidir
		self.php = php
	
	"""Public methods for the manager script..."""
	
	def run(self):
		"""Iterate through the list of wikis and dump them!"""
		self.debug("Starting dump...")
		for db in self.dblist:
			self.db = db
			self.doBackup()
		self.debug("Done!")
	
	"""Public methods for dumps to use..."""
	
	def privatePath(self, filename=""):
		"""Take a given filename in the private dump dir for the selected database."""
		return os.path.join(self.private, self.db, filename)
	
	def publicPath(self, filename=""):
		"""Take a given filename in the public dump dir for the selected database.
		If this database is marked as private, will use the private dir instead.
		"""
		if self.db in self.privatelist:
			return self.privatePath(filename)
		else:
			return os.path.join(self.public, self.db, filename)
	
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
		"""Shell out; output is assumed to be saved usefully somehow."""
		self.debug("runCommand: " + command)
		return os.system(command)
	
	def debug(self, stuff):
		print stuff
	
	# auto-set
	#OutputDir=$PublicDir/$DirLang
	#StatusLog=$OutputDir/backup.log
	#StatusLockFile=$OutputDir/backup.lock
	#StatusDoneFile=$OutputDir/backup.done
	
	#GlobalLog=/var/backup/public/backup.log
	
	def makeDir(self, dir):
		if os.path.exists(dir):
			self.debug("Checkdir dir %s ..." % dir)
		else:
			self.debug("Creating %s ..." % dir)
			os.mkdir(dir)

	def doBackup(self):
		self.makeDir(self.public)
		self.makeDir(self.publicPath())
		
		self.makeDir(self.private)
		self.makeDir(self.privatePath())
		
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
			
			XmlStub("First-pass for page XML data dumps"),
			XmlDump("full", "All pages with complete page edit history (very large!)"),
			XmlDump("current", "All pages, current versions only"),
			XmlDump("articles", "Articles, templates, image descriptions, and main meta-pages (recommended)"),
			
			TitleDump("List of page titles"),
			
			# YahooDump(),
			];
		
		for item in items:
			item.run(self)

		self.checksums()
		self.completeDump()

		self.unlock()
		self.statusComplete()
	
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
	
	def checksums(self):
		self.debug("If this script were finished, it would be checksumming files here")
	
	def completeDump(self):
		self.debug("If this script were finished, it would be adding symlinks or something")

class Dump(object):
	def __init__(self, desc):
		self._desc = desc
	
	def description(self):
		return self._desc

class PublicTable(Dump):
	def __init__(self, table, descr):
		self._table = table
		self._descr = descr
	
	def _path(self, runner, filename):
		return runner.publicPath(filename)
	
	def run(self, runner):
		path = self._path(runner, self._table + ".sql.gz")
		return runner.saveTable(self._table, path)

class PrivateTable(PublicTable):
	def __init__(self, table, descr):
		self._table = table
		self._descr = descr
	
	def description(self):
		return self._desc + " (private)"
	
	def _path(self, runner, filename):
		return runner.privatePath(filename)


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
		self._subset = subset
		self._desc = desc
	
	def run(self, runner):
		xmlbz2 = runner.publicPath("pages_" + self._subset + ".xml.bz2")
		xml7z = runner.publicPath("pages_" + self._subset + ".xml.7z")
		
		# Clear prior 7zip attempts; 7zip will try to append an existing archive
		if os.path.exists(xml7z):
			os.remove(xml7z)
		
		# Page and revision data pulled from this skeleton dump...
		stub = runner.privatePath("stub-%s.xml.gz" % self._subset),
		stubCommand = "gzip -dc %s" % stub

		# Try to pull text from the previous run; most stuff hasn't changed
		#Source=$OutputDir/pages_$section.xml.bz2
		source = self._findPreviousDump(runner)
		if os.path.exists(source):
			runner.status("... building %s XML dump, with text prefetch from %s..." % (self._subset, source))
			prefetch = "--prefetch=bzip2:%s" % (source)
		else:
			runner.status("... building %s XML dump, no text prefetch..." % self._subset)
			prefetch = ""
		
		dumpCommand = "%s -q %s/maintenance/dumpTextPass.php %s %s --output=bzip2:%s --output=7zip:%s" % shellEscape((
			runner.php,
			runner.wikidir,
			runner.db,
			prefetch,
			xmlbz2,
			xml7z))
		command = stubCommand + " | " + dumpCommand
		
		return runner.runCommand(command)
	
	def _findPreviousDump(self, runner):
		return "/tmp/fake/foo"

class TitleDump(Dump):
	"""This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""
	def run(self, runner):
		return runner.saveSql("select page_title from page where page_namespace=0;",
			runner.publicPath("all_titles_in_ns0.gz"))


class Checksums(Dump):
	def description(self):
		return "calculating MD5 hashes"
	
	def run(self, runner):
		# FIXME: run checksums only on the master server?
		command = "md5sum " + \
			runner.publicPath("*.xml.*") + " " + \
			runner.publicPath("*.sql.gz") + " " + \
			runner.publicPath("all_titles_in_ns0.gz")
		return runner.saveCommand(command, runner.publicPath("md5sums.txt"))

