# Wiki dump-generation monitor

import os
import WikiDump

config = WikiDump.Config()

def generateIndex():
	running = False
	states = []
	
	for dbName in config.dbList:
		wiki = WikiDump.Wiki(config, dbName)
		if wiki.isStale():
			print dbName + " is stale"
			wiki.cleanupStaleLock()
		if wiki.isLocked():
			print dbName + " is locked"
		running = running or wiki.isLocked()
		states.append(wiki.statusLine())
	
	if running:
		status = "Dumps are in progress..."
	else:
		status = "Dump process is idle."
	
	return config.readTemplate("progress.html") % {
		"status": status,
		"items": "\n".join(states)}
	
def updateIndex():
	outputFileName = os.path.join(config.publicDir, config.index)
	outputFile = open(outputFileName, "w")
	outputFile.write(generateIndex())
	outputFile.close()

if __name__ == "__main__":
	updateIndex()
