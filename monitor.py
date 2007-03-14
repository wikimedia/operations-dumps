# Wiki dump-generation monitor

import WikiDump

config = WikiDump.Config()

def updateIndex():
	running = False
	states = []
	
	for dbName in config.dbList:
		wiki = WikiDump.Wiki(config, dbName)
		running = running or wiki.isLocked()
		states.append(wiki.statusLine())
	
	if running:
		status = "Dumps are in progress..."
	else:
		status = "Dump process is idle."
	
	output = config.readTemplate("progress.html") % {
		"status": status,
		"items": "\n".join(states)}
	
	print output

if __name__ == "__main__":
	updateIndex()
