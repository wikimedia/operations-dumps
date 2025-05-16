#!/bin/bash

# function used by wikibase rdf dumps, customized for Commons

setProjectName() {
    projectName="commons"
}

setEntityType() {
	entityTypes="--entity-type mediainfo --ignore-missing"
}

setDumpFlavor() {
	dumpFlavor="full-dump"
}

setFilename() {
    filename=commons-$today-$dumpName
}

setDcatConfig() {
	# TODO: add DCAT info
    dcatConfig=""
}
