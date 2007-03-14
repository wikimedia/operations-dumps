import ConfigParser
import os
import re
import sys

def atomicOpen(filename, mode='w'):
	"""Create a file, aborting if it already exists..."""
	fd = os.open(filename, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
	return os.fdopen(fd, mode)

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
			#"dump": {
			"public": "/dumps/public",
			"private": "/dumps/private",
			#"reporting": {
			"adminmail": "root@localhost",
			"mailfrom": "root@localhost",
			"webroot": "http://localhost/dumps",
			"templatedir": home,
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
		
		self.publicDir = conf.get("dump", "public")
		self.privateDir = conf.get("dump", "private")
		
		self.templateDir = conf.get("reporting", "templateDir")
		self.adminMail = conf.get("reporting", "adminmail")
		self.mailFrom = conf.get("reporting", "mailfrom")
		self.webRoot = conf.get("reporting", "webroot")
		
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
	
	def isRunning(self):
		return self.isLocked()

	def isLocked(self):
		return os.path.exists(self.lockFile())

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
	
	def statusLine(self):
		date = self.latestDump()
		if date:
			status = os.path.join(self.publicDir(), date, "status.html")
			try:
				return readFile(status)
			except:
				return "<li>%s missing status record</li>" % self.dbName
		else:
			return "<li>%s has not yet been dumped</li>" % self.dbName

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


if __name__ == "__main__":
	config = Config()
	print "Config load ok!"
