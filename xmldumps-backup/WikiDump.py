import ConfigParser
import email.MIMEText
import os
import re
import smtplib
import socket
import sys
import threading
import time
import tempfile

class FileUtils(object):

	def fileAge(filename):
		return time.time() - os.stat(filename).st_mtime

	def atomicCreate(filename, mode='w'):
		"""Create a file, aborting if it already exists..."""
		fd = os.open(filename, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
		return os.fdopen(fd, mode)

	def writeFile(dirname, filename, text, perms = 0):
		"""Write text to a file, as atomically as possible, via a temporary file in the same directory."""
		
		(fd, tempFilename ) = tempfile.mkstemp("_txt","wikidump_",dirname);
		os.write(fd,text)
		os.close(fd)
		if (perms):
			os.chmod(tempFilename,perms)
		# This may fail across filesystems or on Windows.
		# Of course nothing else will work on Windows. ;)
		os.rename(tempFilename, filename)

	def readFile(filename):
		"""Read text from a file in one fell swoop."""
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
		path = FileUtils.splitPath(path)
		base = FileUtils.splitPath(base)
		while base and path[0] == base[0]:
			path.pop(0)
			base.pop(0)
		for prefix in base:
			path.insert(0, "..")
		return os.path.join(*path)

	def prettySize(size):
		"""Return a string with an attractively formatted file size."""
		quanta = ("%d bytes", "%d KB", "%0.1f MB", "%0.1f GB", "%0.1f TB")
		return FileUtils._prettySize(size, quanta)

	def _prettySize(size, quanta):
		if size < 1024 or len(quanta) == 1:
			return quanta[0] % size
		else:
			return FileUtils._prettySize(size / 1024.0, quanta[1:])

	fileAge = staticmethod(fileAge)
	atomicCreate = staticmethod(atomicCreate)
	writeFile = staticmethod(writeFile)
	readFile = staticmethod(readFile)
	splitPath = staticmethod(splitPath)
	relativePath = staticmethod(relativePath)
	prettySize = staticmethod(prettySize)
	_prettySize = staticmethod(_prettySize)

class TimeUtils(object):

	def today():
		return time.strftime("%Y%m%d", time.gmtime())

	def prettyTime():
		return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

	def prettyDate(key):
		"Prettify a MediaWiki date key"
		return "-".join((key[0:4], key[4:6], key[6:8]))

	today = staticmethod(today)
	prettyTime = staticmethod(prettyTime)
	prettyDate = staticmethod(prettyDate)

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

	dbList = staticmethod(dbList)
	shellEscape = staticmethod(shellEscape)

class Config(object):
	def __init__(self, configFile=False):
		home = os.path.dirname(sys.argv[0])
		if (not configFile):
			configFile = "wikidump.conf"
		files = [
			os.path.join(home,configFile),
			"/etc/wikidump.conf",
			os.path.join(os.getenv("HOME"), ".wikidump.conf")]
		defaults = {
			#"wiki": {
			"dblist": "",
			"privatelist": "",
			"biglist": "",
			"flaggedrevslist": "",
#			"dir": "",
			"forcenormal": "0",
			"halt": "0",
			"skipdblist" : "",
			#"output": {
			"public": "/dumps/public",
			"private": "/dumps/private",
			"webroot": "http://localhost/dumps",
			"index": "index.html",
			"templatedir": home,
			"perdumpindex": "index.html",
			"logfile": "dumplog.txt",
			"fileperms": "0640",
			#"reporting": {
			"adminmail": "root@localhost",
			"mailfrom": "root@localhost",
			"smtpserver": "localhost",
			"staleage": "3600",
			#"database": {
			"user": "root",
			"password": "",
			#"tools": {
			"php": "/bin/php",
			"gzip": "/usr/bin/gzip",
			"bzip2": "/usr/bin/bzip2",
			"sevenzip": "/bin/7za",
			"mysql": "/usr/bin/mysql",
			"mysqldump": "/usr/bin/mysqldump",
			"head": "/usr/bin/head",
			"tail": "/usr/bin/tail",
			"cat": "/bin/cat",
			"grep": "/bin/grep",
			#"cleanup": {
			"keep": "3",
			#"chunks": {
			# set this to 1 to enable runing the various xml dump stages as chunks in parallel
			"chunksEnabled" : "0",
			# for page history runs, number of pages for each chunk, specified separately 
			# e.g. "1000,10000,100000,2000000,2000000,2000000,2000000,2000000,2000000,2000000"
			# would define 10 chunks with the specified number of pages in each and any extra in
			# a final 11th chunk
			"pagesPerChunkHistory" : False,
			# revs per chunk (roughly, it will be split along page lines) for history and current dumps
			# values: positive integer, "compute", 
			# this field is overriden by pagesPerChunkHistory
			# CURRENTLY NOT COMPLETE so please don't use this.
			"revsPerChunkHistory" : False,
			# pages per chunk for abstract runs
			"pagesPerChunkAbstract" : False,
			# whether or not to recombine the history pieces
			"recombineHistory" : "1",
			}
		conf = ConfigParser.SafeConfigParser(defaults)
		conf.read(files)
		
		if not conf.has_section("wiki"):
			print "The mandatory configuration section 'wiki' was not defined."
			raise ConfigParser.NoSectionError('wiki')

		if not conf.has_option("wiki","dir"):
			print "The mandatory setting 'dir' in the section 'wiki' was not defined."
			raise ConfigParser.NoOptionError('wiki','dir')

		self.dbList = MiscUtils.dbList(conf.get("wiki", "dblist"))
		self.skipDbList = MiscUtils.dbList(conf.get("wiki", "skipdblist"))
		self.privateList = MiscUtils.dbList(conf.get("wiki", "privatelist"))
		self.bigList = MiscUtils.dbList(conf.get("wiki", "biglist"))
		self.flaggedRevsList = MiscUtils.dbList(conf.get("wiki", "flaggedrevslist"))
		self.wikiDir = conf.get("wiki", "dir")
		self.forceNormal = conf.getint("wiki", "forceNormal")
		self.halt = conf.getint("wiki", "halt")

		self.dbList = list(set(self.dbList) - set(self.skipDbList))

		if not conf.has_section('output'):
			conf.add_section('output')
		self.publicDir = conf.get("output", "public")
		self.privateDir = conf.get("output", "private")
		self.webRoot = conf.get("output", "webroot")
		self.index = conf.get("output", "index")
		self.templateDir = conf.get("output", "templateDir")
		self.perDumpIndex = conf.get("output", "perdumpindex")
		self.logFile = conf.get("output", "logfile")
		self.fileperms = conf.get("output", "fileperms")
		self.fileperms = int(self.fileperms,0)
		if not conf.has_section('reporting'):
			conf.add_section('reporting')
		self.adminMail = conf.get("reporting", "adminmail")
		self.mailFrom = conf.get("reporting", "mailfrom")
		self.smtpServer = conf.get("reporting", "smtpserver")
		self.staleAge = conf.getint("reporting", "staleAge")
		
		if not conf.has_section('database'):
			conf.add_section('database')
		self.dbUser = conf.get("database", "user")
		self.dbPassword = conf.get("database", "password")
		
		if not conf.has_section('tools'):
			conf.add_section('tools')
		self.php = conf.get("tools", "php")
		self.gzip = conf.get("tools", "gzip")
		self.bzip2 = conf.get("tools", "bzip2")
		self.sevenzip = conf.get("tools", "sevenzip")
		self.mysql = conf.get("tools", "mysql")
		self.mysqldump = conf.get("tools", "mysqldump")
		self.head = conf.get("tools", "head")
		self.tail = conf.get("tools", "tail")
		self.cat = conf.get("tools", "cat")
		self.grep = conf.get("tools", "grep")

		if not conf.has_section('chunks'):
			conf.add_section('chunks')
		self.chunksEnabled = conf.getint("chunks","chunksEnabled")
		self.pagesPerChunkHistory = conf.get("chunks","pagesPerChunkHistory")
		self.revsPerChunkHistory = conf.get("chunks","revsPerChunkHistory")
		self.pagesPerChunkAbstract = conf.get("chunks","pagesPerChunkAbstract")
		self.recombineHistory = conf.getint("chunks","recombineHistory")

		if not conf.has_section('cleanup'):
			conf.add_section('cleanup')
		self.keep = conf.getint("cleanup", "keep")

	def dbListByAge(self):
		"""
			Sort wikis in reverse order of last successful dump :

			Order is (DumpFailed, Age), and False < True :
			First, wikis whose latest dump was successful, most recent dump first
			Then, wikis whose latest dump failed, most recent dump first.
			Finally, wikis which have never been dumped.

			According to that sort, the last item of this list is, when applicable,
			the oldest failed dump attempt.

			If some error occurs checking a dump status, that dump is put last in the
			list (sort value is (True, maxint) )

			Note that we now sort this list by the date of the dump directory, not the
			last date that a dump file in that directory may have been touched. This
			allows us to rerun jobs to completion from older runs, for example
			an en pedia history urn that failed in the middle, without borking the
			index page links.
		"""
		available = []
		for db in self.dbList:
			wiki = Wiki(self, db)

			age = sys.maxint
			date = sys.maxint
			last = wiki.latestDump()
			status = ''
			if last:
				dumpStatus = os.path.join(wiki.publicDir(), last, "status.html")
				try:
					# tack on the file mtime so that if we have multiple wikis
					# dumped on the same day, they get ordered properly
					date = int(today()) - int(last)
					age = FileUtils.fileAge(dumpStatus)
					status = FileUtils.readFile(dumpStatus)
				except:
					print "dump dir %s corrupt?" % dumpStatus
			dumpFailed = (status == '') or ('dump aborted' in status)
			available.append((dumpFailed, date, age, db))
		available.sort()
		return [db for (failed, date, age, db) in available]
	
	def readTemplate(self, name):
		template = os.path.join(self.templateDir, name)
		return FileUtils.readFile(template)
	
	def mail(self, subject, body):
		"""Send out a quickie email."""
		message = email.MIMEText.MIMEText(body)
		message["Subject"] = subject
		message["From"] = self.mailFrom
		message["To"] = self.adminMail

		try:
			server = smtplib.SMTP(self.smtpServer)
			server.sendmail(self.mailFrom, self.adminMail, message.as_string())
			server.close()
		except:
			print "MAIL SEND FAILED! GODDAMIT! Was sending this mail:"
			print message


class Wiki(object):
	def __init__(self, config, dbName):
		self.config = config
		self.dbName = dbName
		self.date = None
		self.watchdog = None
	
	def isPrivate(self):
		return self.dbName in self.config.privateList
	
	def isBig(self):
		return self.dbName in self.config.bigList

	def hasFlaggedRevs(self):
		return self.dbName in self.config.flaggedRevsList
	
	def isLocked(self):
		return os.path.exists(self.lockFile())
	
	def isStale(self):
		if not self.isLocked():
			return False
		try:
			age = self.lockAge()
			return age > self.config.staleAge
		except:
			# Lock file vanished while we were looking
			return False

	# Paths and directories...
	
	def publicDir(self):
		if self.isPrivate():
			return self.privateDir()
		else:
			return os.path.join(self.config.publicDir, self.dbName)
	
	def privateDir(self):
		return os.path.join(self.config.privateDir, self.dbName)
	
	def webDir(self):
		return "/".join((self.config.webRoot, self.dbName))
	
	# Actions!
	
	def lock(self):
		if not os.path.isdir(self.privateDir()):
			try:
				os.makedirs(self.privateDir())
			except:
				# Maybe it was just created (race condition)?
				if not os.path.isdir(self.privateDir()):
					raise
		f = FileUtils.atomicCreate(self.lockFile(), "w")
		f.write("%s %d" % (socket.getfqdn(), os.getpid()))
		f.close()
		
		self.watchdog = LockWatchdog(self.lockFile())
		self.watchdog.start()
		return True
	
	def unlock(self):
		if self.watchdog:
			self.watchdog.stopWatching()
			self.watchdog = None
		os.remove(self.lockFile())
	
	def setDate(self, date):
		self.date = date
	
	def cleanupStaleLock(self):
		date = self.latestDump()
		if date:
			self.setDate(date)
			self.writeStatus(self.reportStatusLine(
				"<span class=\"failed\">dump aborted</span>"))
		self.unlock()
	
	def writePerDumpIndex(self, html):
		directory = os.path.join(self.publicDir(), self.date)
		index = os.path.join(self.publicDir(), self.date, self.config.perDumpIndex)
		FileUtils.writeFile(directory, index, html, self.config.fileperms)
	
	def existsPerDumpIndex(self):
		index = os.path.join(self.publicDir(), self.date, self.config.perDumpIndex)
		return os.path.exists(index)
	
	def writeStatus(self, message):
		directory = os.path.join(self.publicDir(), self.date)
		index = os.path.join(self.publicDir(), self.date, "status.html")
		FileUtils.writeFile(directory, index, message, self.config.fileperms)
	
	def statusLine(self):
		date = self.latestDump()
		if date:
			status = os.path.join(self.publicDir(), date, "status.html")
			try:
				return FileUtils.readFile(status)
			except:
				return self.reportStatusLine("missing status record")
		else:
			return self.reportStatusLine("has not yet been dumped")
	
	def reportStatusLine(self, status, error=False):
		if error:
			# No state information, hide the timestamp
			stamp = "<span style=\"visible: none\">" + TimeUtils.prettyTime() + "</span>"
		else:
			stamp = TimeUtils.prettyTime()
		if self.isPrivate():
			link = "%s (private data)" % self.dbName
		else:
			if self.date:
				link = "<a href=\"%s/%s\">%s</a>" % (self.dbName, self.date, self.dbName)
			else:
				link = "%s (new)" % self.dbName
		return "<li>%s %s: %s</li>\n" % (stamp, link, status)

	def latestDump(self, index=-1, all=False):
		"""Find the last (or slightly less than last) dump for a db."""
		dirs = self.dumpDirs()
		if dirs:
			if all:
				return dirs
			else:
				return dirs[index]
		else:
			return None

	def dumpDirs(self):
		"""List all dump directories for the given database."""
		base = self.publicDir()
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
	
	# private....
	
	def lockFile(self):
		return os.path.join(self.privateDir(), "lock")
	
	def lockAge(self):
		return FileUtils.fileAge(self.lockFile())

class LockWatchdog(threading.Thread):
	"""Touch the given file every 10 seconds until asked to stop."""
	
	# For emergency aborts
	threads = []
	
	def __init__(self, lockfile):
		threading.Thread.__init__(self)
		self.lockfile = lockfile
		self.trigger = threading.Event()
		self.finished = threading.Event()
	
	def stopWatching(self):
		"""Run me outside..."""
		# Ask the thread to stop...
		self.trigger.set()
		
		# Then wait for it, to ensure that the lock file
		# doesn't get touched again after we delete it on
		# the main thread.
		self.finished.wait(10)
		self.finished.clear()
	
	def run(self):
		LockWatchdog.threads.append(self)
		while not self.trigger.isSet():
			self.touchLock()
			self.trigger.wait(10)
		self.trigger.clear()
		self.finished.set()
		LockWatchdog.threads.remove(self)
	
	def touchLock(self):
		"""Run me inside..."""
		os.utime(self.lockfile, None)

def cleanup():
	"""Call cleanup handlers for any background threads..."""
	for watchdog in LockWatchdog.threads:
		watchdog.stopWatching()

if __name__ == "__main__":
	config = Config()
	print "Config load ok!"
