import ConfigParser
import email.MIMEText
import os
import re
import smtplib
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
			"dir": "",
			"forcenormal": "0",
			#"output": {
			"public": "/dumps/public",
			"private": "/dumps/private",
			"webroot": "http://localhost/dumps",
			"index": "index.html",
			"templatedir": home,
			#"reporting": {
			"adminmail": "root@localhost",
			"mailfrom": "root@localhost",
			"smtpserver": "localhost",
			"staleage": "3600",
			#"database": {
			"user": "root",
			"password": "",
			#"tools": {
			"php": "php",
			"bzip2": "bzip2",
			"sevenzip": "7za",
			"mysql": "mysql",
			#"cleanup": {
			"keep": "3",
			}
		conf = ConfigParser.SafeConfigParser(defaults)
		conf.read(files)
		
		self.dbList = dbList(conf.get("wiki", "dblist"))
		self.privateList = dbList(conf.get("wiki", "privatelist"))
		self.wikiDir = conf.get("wiki", "dir")
		self.forceNormal = conf.getint("wiki", "forceNormal")
		
		self.publicDir = conf.get("output", "public")
		self.privateDir = conf.get("output", "private")
		self.webRoot = conf.get("output", "webroot")
		self.index = conf.get("output", "index")
		self.templateDir = conf.get("output", "templateDir")
		
		self.adminMail = conf.get("reporting", "adminmail")
		self.mailFrom = conf.get("reporting", "mailfrom")
		self.smtpServer = conf.get("reporting", "smtpserver")
		self.staleAge = conf.getint("reporting", "staleAge")
		
		self.dbUser = conf.get("database", "user")
		self.dbPassword = conf.get("database", "password")
		
		self.php = conf.get("tools", "php")
		self.bzip2 = conf.get("tools", "bzip2")
		self.sevenzip = conf.get("tools", "sevenzip")
		self.mysql = conf.get("tools", "mysql")
		
		self.keep = conf.getint("cleanup", "keep")
	
	def dbListByAge(self):
		"""Sort available wikis in reverse order of last dump."""
		available = []
		for db in self.dbList:
			wiki = Wiki(self, db)
			age = sys.maxint
			last = wiki.latestDump()
			if last:
				dumpStatus = os.path.join(wiki.publicDir(), last, "status.html")
				try:
					age = fileAge(dumpStatus)
				except:
					print "dump dir %s corrupt?" % dumpStatus
			available.append((age, db))
		available.sort()
		return [db for (age, db) in available]
	
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
				os.mkdir(self.privateDir())
			except:
				# Maybe it was just created (race condition)?
				if not os.path.isdir(self.privateDir()):
					raise
		f = atomicCreate(self.lockFile(), "w")
		f.write("%s.%d" % (os.getenv("HOSTNAME"), os.getpid()))
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
	
	def writeIndex(self, html):
		index = os.path.join(self.publicDir(), self.date, "index.html")
		dumpFile(index, html)
	
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

	def latestDump(self, index=-1):
		"""Find the last (or slightly less than last) dump for a db."""
		dirs = self.dumpDirs()
		if dirs:
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
