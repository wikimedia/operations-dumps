import os
import re
import sys
import WikiBackup

from os.path import dirname, isdir, islink

# Keep just the last two

# Migrating public data...
def migrate(dblist, workDir, archDir):
	"""dblist: list of database subdirectories
	workDir: path to the primary work area
	archiveDir: path to the archival directory.
	
	Relative symbolic links will be generated according to the full paths
	you provide for workDir and archiveDir."""
	
	for db in dblist:
		migrateDatabase(workDir + "/" + db, archDir + "/" + db)

def migrateDatabase(workDir, archDir):
	if not os.path.exists(workDir):
		print "%s missing, skipped" % workDir
		return
	
	dates = migratableSubdirs(workDir)
	if not dates:
		print "%s has nothing ready to migrate, skipped"
		return
	
	for date in dates:
		migrateSubdir(workDir + "/" + date, archDir + "/" + date)

def migratableSubdirs(workDir):
	digits = re.compile(r"^\d{4}\d{2}\d{2}$")
	dates = []
	try:
		for dir in os.listdir(workDir):
			subDir = workDir + "/" + dir
			if digits.match(dir) and isdir(subDir) and not islink(subDir):
				dates.append(dir)
	except OSError:
		return []
	dates.sort()
	return dates[:-2]

def migrateSubdir(workDir, archDir):
	relDir = WikiBackup.relativePath(archDir, dirname(workDir))
	print "%s -> %s" % (workDir, relDir)
	command = commandSeries(
		("mkdir -p %s", (archDir)),
		("cp -pr %s/* %s/", (workDir, archDir)),
		("mv %s %s", (workDir, workDir + ".temp")),
		("ln -s %s %s", (relDir, workDir)),
		("rm -rf %s", (workDir + ".temp")))
	ok = os.system(command)
	if ok != 0:
		raise Exception("failed: " + command)

def commandSeries(*list):
	commands = [command % WikiBackup.shellEscape(args)
		for (command, args) in list]
	return " && ".join(commands)

if __name__ == "__main__":
	migrate(WikiBackup.dbList(sys.argv[1]), sys.argv[2], sys.argv[3])
