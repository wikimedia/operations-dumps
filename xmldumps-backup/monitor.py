# Wiki dump-generation monitor

import os
import sys
import WikiDump
from os.path import exists
from WikiDump import FileUtils

def addToFilename(filename, infix):
    main, suffix = filename.split('.',1)
    return main + "-" + infix + ("." + suffix if suffix else "")

def generateIndex(otherIndexhtml = None, sortedByDb=False):
	running = False
	states = []
	
        if sortedByDb:
                dbs = sorted(config.dbList)
        else:
                dbs = config.dbListByAge()

	for dbName in dbs:
		wiki = WikiDump.Wiki(config, dbName)
		if wiki.isStale():
			print dbName + " is stale"
			wiki.cleanupStaleLock()
		if wiki.isLocked():
                    try:
                            f = open( wiki.lockFile(), 'r' )
                            (host, pid) = f.readline().split(" ") 
                            f.close()
                            print dbName, "is locked by pid", pid, "on", host 
                    except: 
                            print dbName, "is locked" 
		running = running or wiki.isLocked()
		states.append(wiki.statusLine())
	
	if running:
		status = "Dumps are in progress..."
	elif exists("maintenance.txt"):
		status = FileUtils.readFile("maintenance.txt")
	else:
		status = "Dump process is idle."
	
        if otherIndexhtml is None:
            otherIndexLink = ""
        else:
            if sortedByDb:
                otherSortedBy = "dump date"
            else:
                otherSortedBy = "wiki name"

            otherIndexLink = ('Also view sorted by <a href="%s">%s</a>'
                              % (os.path.basename(otherIndexhtml), otherSortedBy))

	return config.readTemplate("progress.html") % {
                "otherIndexLink": otherIndexLink,
		"status": status,
		"items": "\n".join(states)}
	
def updateIndex():
	outputFileName = os.path.join(config.publicDir, config.index)
	outputFileNameSortedByDb = addToFilename(os.path.join(config.publicDir, config.index), "bydb")

	tempFilename = outputFileName + ".tmp"
	file = open(tempFilename, "wt")
	file.write(generateIndex(otherIndexhtml=outputFileNameSortedByDb))
	file.close()
	os.rename(tempFilename, outputFileName)

	tempFilename = outputFileNameSortedByDb + ".tmp"
	file = open(tempFilename, "wt")
	file.write(generateIndex(otherIndexhtml=outputFileName, sortedByDb=True))
	file.close()
	os.rename(tempFilename, outputFileNameSortedByDb)

if __name__ == "__main__":
	# can specify name of alternate config file
	if (len(sys.argv) >= 2):
		config = WikiDump.Config(sys.argv[1])
	else:
		config = WikiDump.Config()

	updateIndex()
