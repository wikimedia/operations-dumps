import ConfigParser
import email.MIMEText
import os
import shutil
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
		"""Write text to a file, as atomically as possible, via a temporary file in a specified directory.
		Arguments: dirname = where temp file is created, filename = full path to actual file, text = contents
		to write to file, perms = permissions that the file will have after creation"""
		
		if not os.path.isdir(dirname):
			try:
				os.makedirs(dirname)
			except:
				raise IOError("The given directory '%s' is neither a directory nor can it be created" % dirname)
				
		(fd, tempFilename ) = tempfile.mkstemp("_txt","wikidump_",dirname);
		os.write(fd,text)
		os.close(fd)
		if (perms):
			os.chmod(tempFilename,perms)
		# This may fail across filesystems or on Windows.
		# Of course nothing else will work on Windows. ;)
		shutil.move(tempFilename, filename)

	def writeFileInPlace(filename, text, perms = 0):
		"""Write text to a file, after opening it for write with truncation.
		This assumes that only one process or thread accesses the given file at a time.
		Arguments: filename = full path to actual file, text = contents
		to write to file, perms = permissions that the file will have after creation,
		if it did not exist already"""
		
		file = open(filename, "wt")
		file.write(text)
		file.close()
		if (perms):
			os.chmod(filename,perms)

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

	def fileInfo(path):
		"""Return a tuple of date/time and size of a file, or None, None"""
		try:
			timestamp = time.gmtime(os.stat(path).st_mtime)
			timestamp = time.strftime("%Y-%m-%d %H:%M:%S",timestamp)
			size = os.path.getsize(path)
			return (timestamp, size)
		except:
			return(None, None)

	fileAge = staticmethod(fileAge)
	atomicCreate = staticmethod(atomicCreate)
	writeFile = staticmethod(writeFile)
	writeFileInPlace = staticmethod(writeFileInPlace)
	readFile = staticmethod(readFile)
	splitPath = staticmethod(splitPath)
	relativePath = staticmethod(relativePath)
	prettySize = staticmethod(prettySize)
	_prettySize = staticmethod(_prettySize)
	fileInfo = staticmethod(fileInfo)

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
		self.projectName = False

		home = os.path.dirname(sys.argv[0])
		if (not configFile):
			configFile = "wikidump.conf"
		self.files = [
			os.path.join(home,configFile),
			"/etc/wikidump.conf",
			os.path.join(os.getenv("HOME"), ".wikidump.conf")]
		defaults = {
			#"wiki": {
			"dblist": "",
			"privatelist": "",
			"flaggedrevslist": "",
			"wikidatalist": "",
			"wikidataclientlist": "",
#			"dir": "",
			"forcenormal": "0",
			"halt": "0",
			"skipdblist" : "",
			#"output": {
			"public": "/dumps/public",
			"private": "/dumps/private",
			"temp":"/dumps/temp",
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
			# these are now set in getDbUserAndPassword() if needed
			"user": "",
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
			"checkforbz2footer": "/usr/local/bin/checkforbz2footer",
			"writeuptopageid": "/usr/local/bin/writeuptopageid",
			"recompressxml": "/usr/local/bin/recompressxml",
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
                        # number of chunks for abstract dumps, overrides pagesPerChunkAbstract
                        "chunksForAbstract" : 0,
			# whether or not to recombine the history pieces
			"recombineHistory" : "1",
			# do we write out checkpoint files at regular intervals? (article/metacurrent/metahistory
			# dumps only.)
			"checkpointTime" : "0",
			#"otherformats": {
			"multistream" : "0",
			}
		self.conf = ConfigParser.SafeConfigParser(defaults)
		self.conf.read(self.files)
		
		if not self.conf.has_section("wiki"):
			print "The mandatory configuration section 'wiki' was not defined."
			raise ConfigParser.NoSectionError('wiki')

		if not self.conf.has_option("wiki","dir"):
			print "The mandatory setting 'dir' in the section 'wiki' was not defined."
			raise ConfigParser.NoOptionError('wiki','dir')

		self.dbUser = None
		self.dbPassword = None
		self.parseConfFileGlobally()
		self.parseConfFilePerProject()
		self.getDbUserAndPassword() # get from MW adminsettings file if not set in conf file

	def parsePHPAssignment(self, line):
		# not so much parse as grab a string to the right of the equals sign,
		# we expect a line that has  ... = "somestring" ;
		# with single or double quotes, spaes or not.  but nothing more complicated.
		equalspattern ="=\s*(\"|')(.+)(\"|')\s*;"
		result = re.search(equalspattern, line)
		if result:
			return result.group(2)
		else:
			return ""

	def getDbUserAndPassword(self):
		# check MW adminsettings file for these if we didn't have values for
		# them in the conf file; failing that we fall back on defaults specified
		# here

		if self.dbUser: # already set via conf file, don't override
			return

		defaultDbUser = "root"
		defaultDbPassword = ""

		if not self.conf.has_option("wiki", "adminsettings"):
			self.dbUser = defaultDbUser
			self.dbPassword = defaultDbPassword
			return

		fd = open(os.path.join(self.wikiDir,self.conf.get("wiki","adminsettings")), "r")
		lines = fd.readlines()
		fd.close()

		# we are digging through a php file and expecting to find
		# lines more or less like the below.. anything more complicated we're not going to handle.
		# $wgDBadminuser = 'something';
		# $wgDBuser = $wgDBadminuser = "something" ;

		for l in lines:
			if "$wgDBadminuser" in l:
				self.dbUser = self.parsePHPAssignment(l)
			elif "$wgDBuser" in l:
				defaultDbUser = self.parsePHPAssignment(l)
			elif "$wgDBadminpassword" in l:
				self.dbPassword = self.parsePHPAssignment(l)
			elif "$wgDBpassword" in l:
				defaultDbPassword = self.parsePHPAssignment(l)

		if not self.dbUser:
			self.dbUser = defaultDbUser
		if not self.dbPassword:
			self.dbPassword = defaultDbPassword
		return

	def parseConfFileGlobally(self):
		self.dbList = MiscUtils.dbList(self.conf.get("wiki", "dblist"))
		self.skipDbList = MiscUtils.dbList(self.conf.get("wiki", "skipdblist"))
		self.privateList = MiscUtils.dbList(self.conf.get("wiki", "privatelist"))
		self.flaggedRevsList = MiscUtils.dbList(self.conf.get("wiki", "flaggedrevslist"))
		self.wikidataList = MiscUtils.dbList(self.conf.get("wiki", "wikidatalist"))
		self.wikidataClientList = MiscUtils.dbList(self.conf.get("wiki", "wikidataclientlist"))
		self.forceNormal = self.conf.getint("wiki", "forcenormal")
		self.halt = self.conf.getint("wiki", "halt")

		self.dbList = list(set(self.dbList) - set(self.skipDbList))

		if not self.conf.has_section('output'):
			self.conf.add_section('output')
		self.publicDir = self.conf.get("output", "public")
		self.privateDir = self.conf.get("output", "private")
		self.tempDir = self.conf.get("output", "temp")
		self.webRoot = self.conf.get("output", "webroot")
		self.index = self.conf.get("output", "index")
		self.templateDir = self.conf.get("output", "templatedir")
		self.perDumpIndex = self.conf.get("output", "perdumpindex")
		self.logFile = self.conf.get("output", "logfile")
		self.fileperms = self.conf.get("output", "fileperms")
		self.fileperms = int(self.fileperms,0)
		if not self.conf.has_section('reporting'):
			self.conf.add_section('reporting')
		self.adminMail = self.conf.get("reporting", "adminmail")
		self.mailFrom = self.conf.get("reporting", "mailfrom")
		self.smtpServer = self.conf.get("reporting", "smtpserver")
		self.staleAge = self.conf.getint("reporting", "staleage")
		
		if not self.conf.has_section('tools'):
			self.conf.add_section('tools')
		self.php = self.conf.get("tools", "php")
		self.gzip = self.conf.get("tools", "gzip")
		self.bzip2 = self.conf.get("tools", "bzip2")
		self.sevenzip = self.conf.get("tools", "sevenzip")
		self.mysql = self.conf.get("tools", "mysql")
		self.mysqldump = self.conf.get("tools", "mysqldump")
		self.head = self.conf.get("tools", "head")
		self.tail = self.conf.get("tools", "tail")
		self.cat = self.conf.get("tools", "cat")
		self.grep = self.conf.get("tools", "grep")
		self.checkforbz2footer = self.conf.get("tools","checkforbz2footer")
		self.writeuptopageid = self.conf.get("tools","writeuptopageid")
		self.recompressxml = self.conf.get("tools","recompressxml")

		if not self.conf.has_section('cleanup'):
			self.conf.add_section('cleanup')
		self.keep = self.conf.getint("cleanup", "keep")

	def parseConfFilePerProject(self, projectName = False):
		# we need to read from the project section without falling back
		# to the defaults, which has_option() normally does, ugh.  so set
		# up a local conf instance without the defaults
		conf = ConfigParser.SafeConfigParser()
		conf.read(self.files)

		if (projectName):
			self.projectName = projectName

		if not self.conf.has_section('database'):
			self.conf.add_section('database')

		dbUser = self.getOptionForProjectOrDefault(conf, "database", "user",0)
		if dbUser:
			self.dbUser = dbUser
		dbPassword = self.getOptionForProjectOrDefault(conf, "database", "password",0)
		if dbPassword:
			self.dbPassword = dbPassword

		if not self.conf.has_section('chunks'):
			self.conf.add_section('chunks')
		self.chunksEnabled = self.getOptionForProjectOrDefault(conf, "chunks","chunksEnabled",1)
		self.pagesPerChunkHistory = self.getOptionForProjectOrDefault(conf, "chunks","pagesPerChunkHistory",0)
		self.revsPerChunkHistory = self.getOptionForProjectOrDefault(conf, "chunks","revsPerChunkHistory",0)
		self.chunksForAbstract = self.getOptionForProjectOrDefault(conf, "chunks","chunksForAbstract",0)
		self.pagesPerChunkAbstract = self.getOptionForProjectOrDefault(conf, "chunks","pagesPerChunkAbstract",0)
		self.recombineHistory = self.getOptionForProjectOrDefault(conf, "chunks","recombineHistory",1)
		self.checkpointTime = self.getOptionForProjectOrDefault(conf, "chunks","checkpointTime",1)
	
		if not self.conf.has_section('otherformats'):
			self.conf.add_section('otherformats')
		self.multistreamEnabled = self.getOptionForProjectOrDefault(conf, 'otherformats', 'multistream', 1)

                if not self.conf.has_section('wiki'):
                        self.conf.add_section('wiki')
		self.wikiDir = self.getOptionForProjectOrDefault(conf, "wiki", "dir", 0)

	def getOptionForProjectOrDefault(self, conf, sectionName, itemName, isInt):
		if (conf.has_section(self.projectName)):
			if (conf.has_option(self.projectName, itemName)):
				if (isInt):
					return(conf.getint(self.projectName,itemName))
				else:
					return(conf.get(self.projectName,itemName))
		if (isInt):
			return(self.conf.getint(sectionName,itemName))
		else:
			return(self.conf.get(sectionName,itemName))
				
	def dbListByAge(self, use_status_time=False):
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
                today = int(TimeUtils.today())
		for db in self.dbList:
			wiki = Wiki(self, db)

			age = sys.maxint
			date = sys.maxint
			last = wiki.latestDump()
			status = ''
			if last:
				dumpStatus = os.path.join(wiki.publicDir(), last, "status.html")
				try:
                                        if use_status_time:
                                                # only use the status file time, not the dir date
                                                date = today
                                        else:
                                                date = today - int(last)
					# tack on the file mtime so that if we have multiple wikis
					# dumped on the same day, they get ordered properly
                                        age = FileUtils.fileAge(dumpStatus)
					status = FileUtils.readFile(dumpStatus)
				except:
					print "dump dir missing status file %s?" % dumpStatus
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
	
	def hasFlaggedRevs(self):
		return self.dbName in self.config.flaggedRevsList

	def hasWikidata(self):
		return self.dbName in self.config.wikidataList
	
	def isWikidataClient(self):
		return self.dbName in self.config.wikidataClientList

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
		webRoot = self.config.webRoot
		if webRoot[-1] == '/':
			webRoot = webRoot[:-1]
		return "/".join((webRoot, self.dbName))

	def webDirRelative(self):
		webRootRelative = self.webDir()
		i = webRootRelative.find("://")
		if i >= 0:
			webRootRelative = webRootRelative[i+3:]
		i = webRootRelative.find("/")
		if i >= 0:
			webRootRelative = webRootRelative[i:]
		return webRootRelative
	
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
		FileUtils.writeFileInPlace(index, html, self.config.fileperms)
	
	def existsPerDumpIndex(self):
		index = os.path.join(self.publicDir(), self.date, self.config.perDumpIndex)
		return os.path.exists(index)
	
	def writeStatus(self, message):
		directory = os.path.join(self.publicDir(), self.date)
		index = os.path.join(self.publicDir(), self.date, "status.html")
		FileUtils.writeFileInPlace(index, message, self.config.fileperms)
	
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

        def dateTouchedLatestDump(self):
                mtime = 0
                last = self.latestDump()
                if last:
                        dumpStatus = os.path.join(self.publicDir(), last, "status.html")
                        try:
                                mtime = os.stat(dumpStatus).st_mtime
                        except:
                                pass
                return time.strftime("%Y%m%d", time.gmtime(mtime))

	def dumpDirs(self, private=False):
		"""List all dump directories for the given database."""
                if private:
                        base = self.privateDir()
                else:
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
