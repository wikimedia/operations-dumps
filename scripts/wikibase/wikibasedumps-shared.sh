#!/bin/bash

# Shared variable and function declarations for creating Wikibase dumps
# of any sort

today=`date +'%Y%m%d'`
daysToKeep=70

args="wiki:multiversion;output:temp;tools:php,lbzip2;${projectName}:shards,fileSizes,pagesPerBatch"
results=`python3 "${repodir}/getconfigvals.py" --configfile "$configfile" --args "$args"`

multiversion=`getsetting "$results" "wiki" "multiversion"` || exit 1
tempDir=`getsetting "$results" "output" "temp"` || exit 1
php=`getsetting "$results" "tools" "php"` || exit 1
lbzip2=`getsetting "$results" "tools" "lbzip2"` || exit 1
shards=`getsetting "$results" "$projectName" "shards"` || exit 1
fileSizes=`getsetting "$results" "$projectName" "fileSizes"` || exit 1
pagesPerBatch=`getsetting "$results" "$projectName" "pagesPerBatch"` || exit 1

for settingname in "multiversion" "tempDir" "shards" "fileSizes" "pagesPerBatch"; do
    checkval "$settingname" "${!settingname}"
done

targetDirBase=${systemdjobsdir}/wikibase/${projectName}wiki
targetDirDefault=$targetDirBase/$today

multiversionscript="${multiversion}/MWScript.php"

function makeTargetDir {
	mkdir -p "$1"
}

function runDcat {
	if [[ -n "$dcatConfig" ]]; then
		$php /usr/local/share/dcat/DCAT.php --config=$dcatConfig --dumpDir=$targetDirBase --outputDir=$targetDirBase
	fi
}

# Add the checksums for $1 to today's checksum files
function putDumpChecksums {
	local targetPath=$1
	local targetDir=`dirname "$targetPath"`

	md5=`md5sum "$targetPath" | awk '{print $1}'`
	echo "$md5  `basename $targetPath`" >> $targetDir/${projectName}-$today-md5sums.txt

	sha1=`sha1sum "$targetPath" | awk '{print $1}'`
	echo "$sha1  `basename $targetPath`" >> $targetDir/${projectName}-$today-sha1sums.txt
}

# Get the number of batches needed to dump all of the particular project, stored in $numberOfBatchesNeeded.
function getNumberOfBatchesNeeded {
	maxPageId="$($php $multiversionscript mysql.php --wiki ${projectName}wiki --group=dump -- --silent --skip-column-names -e 'SELECT MAX(page_id) AS max_page_id FROM page')"
	# Fail if mysql exited with a non-zero exit code, we got a non-numerical value or our value is less than 1.
	if [ "$?" -ne 0 ] || ! echo "$maxPageId" | grep -qPv '[^\d]' || [ "$maxPageId" -lt 1 ]; then
		echo "Couldn't get MAX(page_id) from db."
		exit 1
	fi

	# This should be roughly enough to dump all pages. The last batch is run without specifying a last page id, so it's ok if this is slightly off.
	numberOfBatchesNeeded=$(($maxPageId / $pagesPerBatch))
}

# Set batch-dependent variables needed for a call to the PHP dump scripts
function setPerBatchVars {
	firstPageIdParam="--first-page-id "$(( $batch * $pagesPerBatch * $shards + 1))
	lastPageIdParam="--last-page-id "$(( ( $batch + 1 ) * $pagesPerBatch * $shards))

	lastRun=0
	if [ $(($batch + 1)) -eq $numberOfBatchesNeeded ]; then
		# Do not limit the last run
		lastPageIdParam=""
		lastRun=1
	fi
}

# Get temporary files selected by the given pattern $1, sorted.
function getTempFiles {
	# Need to use sort -V here as batches need to be concated in order
	tempFiles=`ls -1 $1 2>/dev/null | sort -V | paste -s -d ' '`
}

# Get the total file size of all files in $1
function getFileSize {
	fileSize=`du -b -c $1 | awk '/total$/ { print $1 }'`
}

# Handle the failure of a batch run.
function handleBatchFailure {
	echo -e "\n\n(`date --iso-8601=minutes`) Process for batch $batch of shard $i failed with exit code $exitCode"

	let retries++

	if [ $retries -gt 5 ]; then
		# Give up with this shard.
		echo -e "\n\n(`date --iso-8601=minutes`) Giving up after $(($retries - 1)) retries."
		echo 1 > $failureFile
		return 1
	fi

	# Increase the sleep time for every retry
	sleep $((900 * $retries))
}

# Set the last batch number into $batch, based on the given temporary files $1.
function getContinueBatchNumber {
	getTempFiles "$1"
	if [ -n "$tempFiles" ]; then
		batch=`echo $tempFiles | awk '{ print $(NF) }' | sed -r 's/.*batch([0-9]+).gz/\1/'`
	fi
}

# Move the dump file from sourcePath to targetPath, create a symlink at latestPath,
# and put the checksums for the dump file.
function moveLinkFile {
	local sourcePath=$1
	local targetPath=$2
	local latestPath=$3
	mv "$sourcePath" "$targetPath"
	ln -frs "$targetPath" "$latestPath"
	putDumpChecksums "$targetPath"
}

setDumpNameToMinSize() {
    # format: dumpName:size,dumpName:size...
    declare -g -A dumpNameToMinSize
    IFS=',' read -r -a namesSizesArray <<< "$fileSizes"
    for nameValue in "${namesSizesArray[@]}"; do
        IFS=':' read -r key value <<<"$nameValue"
        dumpNameToMinSize[$key]=$(( $value / $shards ))
    done
}

reportProgress() {
	local shard=$1
	local batch=$2
	local totalBatches=$3
	local progressFile=$4
	local completed percent

	echo "shard $shard batch $batch done" >> "$progressFile"
	completed=$(wc -l < "$progressFile")
	percent=$(( completed * 100 / totalBatches ))
	echo "Progress: $completed/$totalBatches batches done (${percent}%)"
}

reportMetrics() {
	local dump_report=$1
	local prometheus_url=$2
	local type=$3
	local format=$4
	local skipped_entities curl_output

	# count the number of skipped entities from Exception log tag
	skipped_entities=$( grep "failed-to-dump" $dump_report | wc -l || true)
	echo "Number of skipped entities: $skipped_entities"
	if ! curl_output=$(
		echo "wikidata_dumps_skipped_entities{type=\"$type\",format=\"$format\"} ${skipped_entities}" \
		| curl -sS --data-binary @- ${prometheus_url} 2>&1
	); then
		echo "Warning: Failed to push metrics to Prometheus: ${curl_output}" >&2
	fi
}
