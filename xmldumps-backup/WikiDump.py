import ConfigParser
import email.MIMEText
import os
import re
import smtplib
import socket
import sys
import threading
import time

def fileAge(filename):
	return time.time() - os.stat(filename).st_mtime

def atomicCreate(filename, mode='w'):
	"""Create a file, aborting if it already exists..."""
	fd = os.open(filename, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
	return os.fdopen(fd, mode)

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
	"""Dump a string to a file, as atomically as possible, via a temporary file in the same directory."""
	
	# I'd use os.tempnam() here but it whines about symlinks attacks.
	tempFilename = filename + ".tmp"
	
	file = open(tempFilename, "wt")
	file.write(text)
	file.close()
	
	# This may fail across filesystems or on Windows.
	# Of course nothing else will work on Windows. ;)
	os.rename(tempFilename, filename)

def readFile(filename):
	file = open(filename, "r")
	text = file.read()
	file.close()
	return text

def dbList(filename):
	"""Read database list from a file"""
	infile = open(filename)
	dbs = []
	for line in infile:
		line = line.strip()
		if line != "":
			dbs.append(line)
	infile.close()
	dbs.sort()
	return dbs

class Config(object):
	def __init__(self):
		home = os.path.dirname(sys.argv[0])
		files = [
			os.path.join(home, "wikidump.conf"),
			"/etc/wikidump.conf",
			os.path.join(os.getenv("HOME"), ".wikidump.conf")]
		defaults = {
			#"wiki": {
			"dblist": "",
			"privatelist": "",
			"biglist": "",
			"dir": "",
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
			"gzip2": "/usr/bin/gzip",
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
			# set this to True to enable runing the various xml dump stages as chunks in parallel
			"chunksEnabled" : False,
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
			}
		conf = ConfigParser.SafeConfigParser(defaults)
		conf.read(files)
		
		self.dbList = dbList(conf.get("wiki", "dblist"))
		self.skipDbList = dbList(conf.get("wiki", "skipdblist"))
		self.dbList = list(set(self.dbList) - set(self.skipDbList))

		self.privateList = dbList(conf.get("wiki", "privatelist"))
		biglistFile = conf.get("wiki", "biglist")
		if biglistFile:
			self.bigList = dbList(biglistFile)
		else:
			self.bigList = []
		flaggedRevsFile = conf.get("wiki", "flaggedrevslist")
		if flaggedRevsFile:
			self.flaggedRevsList = dbList(flaggedRevsFile)
		else:
			self.flaggedRevsList = []
		
		self.wikiDir = conf.get("wiki", "dir")
		self.forceNormal = conf.getint("wiki", "forceNormal")
		self.halt = conf.getint("wiki", "halt")
		
		self.publicDir = conf.get("output", "public")
		self.privateDir = conf.get("output", "private")
		self.webRoot = conf.get("output", "webroot")
		self.index = conf.get("output", "index")
		self.templateDir = conf.get("output", "templateDir")
		self.perDumpIndex = conf.get("output", "perdumpindex")
		
		self.adminMail = conf.get("reporting", "adminmail")
		self.mailFrom = conf.get("reporting", "mailfrom")
		self.smtpServer = conf.get("reporting", "smtpserver")
		self.staleAge = conf.getint("reporting", "staleAge")
		
		self.dbUser = conf.get("database", "user")
		self.dbPassword = conf.get("database", "password")
		
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

		self.chunksEnabled = conf.get("chunks","chunksEnabled")
		self.pagesPerChunkHistory = conf.get("chunks","pagesPerChunkHistory")
		self.revsPerChunkHistory = conf.get("chunks","revsPerChunkHistory")
		self.pagesPerChunkAbstract = conf.get("chunks","pagesPerChunkAbstract")

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
			last = wiki.latestDump()
			status = ''
			if last:
				dumpStatus = os.path.join(wiki.publicDir(), last, "status.html")
				try:
					# tack on the file age so that if we have multiple wikis
					# dumped on the same day, they get ordered properly
					age = last . fileAge(dumpStatus)
					status = readFile(dumpStatus)
				except:
					print "dump dir %s corrupt?" % dumpStatus
			dumpFailed = (status == '') or ('dump aborted' in status)
			available.append((dumpFailed, age, db))
		available.sort()
		return [db for (failed, age, db) in available]
	
	def readTemplate(self, name):
		template = os.path.join(self.templateDir, name)
		return readFile(template)
	
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
		f = atomicCreate(self.lockFile(), "w")
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
		index = os.path.join(self.publicDir(), self.date, self.config.perDumpIndex)
		dumpFile(index, html)
	
	def existsPerDumpIndex(self):
		index = os.path.join(self.publicDir(), self.date, self.config.perDumpIndex)
		return os.path.exists(index)
	
	def writeStatus(self, message):
		index = os.path.join(self.publicDir(), self.date, "status.html")
		dumpFile(index, message)
	
	def statusLine(self):
		date = self.latestDump()
		if date:
			status = os.path.join(self.publicDir(), date, "status.html")
			try:
				return readFile(status)
			except:
				return self.reportStatusLine("missing status record")
		else:
			return self.reportStatusLine("has not yet been dumped")
	
	def reportStatusLine(self, status, error=False):
		if error:
			# No state information, hide the timestamp
			stamp = "<span style=\"visible: none\">" + prettyTime() + "</span>"
		else:
			stamp = prettyTime()
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
		return fileAge(self.lockFile())

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
