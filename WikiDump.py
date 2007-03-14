import ConfigParser
import os
import re
import sys
import time

def atomicOpen(filename, mode='w'):
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
	"""Dump a string to a file."""
	file = open(filename, "wt")
	file.write(text)
	file.close()

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
			#"output": {
			"public": "/dumps/public",
			"private": "/dumps/private",
			"webroot": "http://localhost/dumps",
			"index": "index.html",
			"templatedir": home,
			#"reporting": {
			"adminmail": "root@localhost",
			"mailfrom": "root@localhost",
			"staleage": "3600",
			#"database": {
			"user": "root",
			"password": "",
			#"tools": {
			"php": "php",
			"bzip2": "bzip2",
			"sevenzip": "7za",
			"mysql": "mysql"
			}
		conf = ConfigParser.SafeConfigParser(defaults)
		conf.read(files)
		
		self.dbList = dbList(conf.get("wiki", "dblist"))
		self.privateList = dbList(conf.get("wiki", "privatelist"))
		self.wikiDir = conf.get("wiki", "dir")
		
		self.publicDir = conf.get("output", "public")
		self.privateDir = conf.get("output", "private")
		self.webRoot = conf.get("reporting", "webroot")
		self.index = conf.get("reporting", "index")
		self.templateDir = conf.get("reporting", "templateDir")
		
		self.adminMail = conf.get("reporting", "adminmail")
		self.mailFrom = conf.get("reporting", "mailfrom")
		self.staleAge = conf.getint("reporting", "staleAge")
		
		self.dbUser = conf.get("database", "user")
		self.dbPassword = conf.get("database", "password")
		
		self.php = conf.get("tools", "php")
		self.bzip2 = conf.get("tools", "bzip2")
		self.sevenzip = conf.get("tools", "sevenzip")
		self.mysql = conf.get("tools", "mysql")
	
	def readTemplate(self, name):
		template = os.path.join(self.templateDir, name)
		return readFile(template)

class Wiki(object):
	def __init__(self, config, dbName):
		self.config = config
		self.dbName = dbName
	
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
	
	# Actions!
	
	def lock(self):
		try:
			f = atomicCreate(self.lockFile(), "wt")
			f.write("%s.%d" % (os.getenv("HOSTNAME"), os.getpid()))
			f.close()
			return True
		except:
			return False
	
	def unlock(self):
		os.remove(self.lockFile())
	
	def cleanupStaleLock(self):
		date = self.latestDump()
		if date:
			self.date = date
			self.writeStatus(self.reportStatusLine(
				"<span class=\"failed\">dump aborted</span>"))
		self.unlock()
	
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
		return time.time() - os.stat(self.lockFile()).st_mtime



if __name__ == "__main__":
	config = Config()
	print "Config load ok!"
