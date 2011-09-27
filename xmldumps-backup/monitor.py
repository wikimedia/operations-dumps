# Wiki dump-generation monitor

import os
import sys
import WikiDump
from os.path import exists
from WikiDump import FileUtils

# can specify name of alternate config file
if (sys.argv[1]):
	config = WikiDump.Config(sys.argv[1])
else:
	config = WikiDump.Config()

def generateIndex():
	running = False
	states = []
	
	for dbName in config.dbListByAge():
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
	
	return config.readTemplate("progress.html") % {
		"status": status,
		"items": "\n".join(states)}
	
def updateIndex():
	outputFileName = os.path.join(config.publicDir, config.index)
	tempFilename = outputFileName + ".tmp"
	file = open(tempFilename, "wt")
	file.write(generateIndex())
	file.close()
	os.rename(tempFilename, outputFileName)

if __name__ == "__main__":
	updateIndex()
