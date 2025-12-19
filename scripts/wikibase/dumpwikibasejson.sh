#!/bin/bash

# Generate a json dump for wikibase (wikidata or commons) and remove old ones.

# Setting a stricter mode for this script, to enable better error
# handling and avoid subtle bugs. This will exit the script on
# any error, on use of undefined variables, and will propagate
# errors in pipelines.
set -euo pipefail

PROJECTS=("wikidata" "commons")
DUMP_REPORT="/tmp/wikibasejson-reports.log"
echo "" > $DUMP_REPORT
PROMETHEUS_PUSH_URL="http://prometheus-pushgateway.discovery.wmnet/metrics/job/wikidata_dumps"

source /usr/local/etc/dump_functions.sh

usage() {
    echo "Usage: $0 --project <name> --dump <name> [--entities <name>[|name...]] [--extra <option>]" >& 2
    echo "[--config <path>] [--continue] [--dryrun] [--help]" >& 2
    echo >& 2
    echo "Args: " >& 2
    echo "  --config   (-c) path to configuration file for dump generation" >& 2
    echo "                  (default value: ${confsdir}/wikidump.conf.other" >& 2
    echo "  --project  (-p) 'wikidata' or 'commons'" >& 2
    echo "                  (default value: wikidata)" >& 2
    echo "  --dump     (-d) 'all' or 'lexemes' (for wikidata)" >& 2
    echo "                  'mediainfo' (for commons)" >&2
    echo "                  (default value: all)" >& 2
    echo "  --entities (-e) one of 'item|property' or 'lexemes' (for wikidata)" >& 2
    echo "                  'mediainfo' (for commons)" >& 2
    echo "  --extra    (-E) extra args, e.g. '--ignore-missing' for commons" >& 2
    echo "                  (default value: empty)" >& 2
    echo >& 2
    echo "Flags: " >& 2
    echo "  --continue (-C) resume the specified dump from where it left off" >& 2
    echo "                  (default value: false)" >& 2
    echo "  --dryrun   (-D) don't run dump, show what would have been done" >& 2
    echo "                  (default value: false)" >& 2
    echo "  --help     (-h) show this help message" >& 2
    exit 1
}

progressFile="/tmp/wikibasejson-batches-completed.log"
echo -n "" > "$progressFile"

configfile="${confsdir}/wikidump.conf.other"
projectName="wikidata"
dumpName="all"
entities="item|property"
dryrun="false"
extra=""
continue=0

while [ $# -gt 0 ]; do
    case "$1" in
	"--config"|"-c")
            configfile="$2"
            shift; shift
	    ;;
	"--project"|"-p")
            projectName="$2"
            shift; shift
	    ;;
	"--dump"|"-d")
            dumpName="$2"
            shift; shift
	    ;;
	"--entities"|"-e")
            entities="$2"
            shift; shift
	    ;;
	"--extra"|"-E")
            extra="$2"
            shift; shift
	    ;;
	"--dryrun"|"-D")
            dryrun="true"
            shift
	    ;;
	"--continue"|"-C")
            continue=1
            shift
	    ;;
	"--help"|"-h")
	    usage && exit 1
	    ;;
	*)
            echo "$0: Unknown option $1" >& 2
            usage && exit 1
	    ;;
    esac
done

if [ -z "$projectName" ]; then
    echo -e "Mandatory arg --project not specified."
    usage
    exit 1
fi
projectOK=""
for value in "${PROJECTS[@]}"; do
  if [ "$value" == "$projectName" ]; then
      projectOK="true"
      break;
  fi
done
if [ -z "$projectOK" ]; then
    echo -e "Unknown project name."
    usage
    exit 1
fi
IFS='|' read -r -a entityArray <<< "$entities"
entityTypes=()
for value in "${entityArray[@]}"; do
  entityTypes+=("--entity-type")
  entityTypes+=("$value")
done

. /usr/local/bin/wikibasedumps-shared.sh
. /usr/local/bin/${projectName}json_functions.sh

targetDir=${targetDirDefault}

makeTargetDir "$targetDir"

if [ $continue -eq 0 ]; then
	# Remove old leftovers, as we start from scratch.
	rm -f "${tempDir}/${projectName}-${dumpName}."*-batch*.json.gz
fi

filename=${projectName}-$today-$dumpName
failureFile=/tmp/dump${projectName}json-$dumpName-failure

setDumpNameToMinSize

i=0
rm -f $failureFile

getNumberOfBatchesNeeded
numberOfBatchesNeeded=$(($numberOfBatchesNeeded / $shards))
if [[ $numberOfBatchesNeeded -lt 1 ]]; then
    # wiki is too small for default settings, change settings to something sane
    # this assumes wiki has at least four entities, which sounds plausible
	shards=4
	numberOfBatchesNeeded=1
	pagesPerBatch=$(( $maxPageId / $shards ))
fi

totalBatches=$(( shards * numberOfBatchesNeeded ))

function returnWithCode { return $1; }

extraArgs="--dbgroupdefault dump"
if  [ -n "$extra" ]; then
    extraArgs="$extra $extraArgs"
fi
while [ $i -lt $shards ]; do
	(
		set -o pipefail

		batch=0

		if [ $continue -gt 0 ]; then
			getContinueBatchNumber "${tempDir}/${projectName}-${dumpName}.$i-batch*.json.gz"
		fi

		retries=0
		while [ $batch -lt $numberOfBatchesNeeded ] && [ ! -f $failureFile ]; do
			setPerBatchVars

			echo "Starting batch $batch"
			$php $multiversionscript extensions/Wikibase/repo/maintenance/dumpJson.php \
				--wiki ${projectName}wiki \
				--shard $i \
				--sharding-factor $shards \
				--batch-size $(($shards * 250)) \
				--snippet 2 \
				--page-metadata \
				--log ${DUMP_REPORT} \
				"${entityTypes[@]}" \
				$extraArgs \
				$firstPageIdParam \
				$lastPageIdParam \
				| gzip -9 > "${tempDir}/${projectName}-${dumpName}.${i}-batch${batch}.json.gz"

			exitCode=$?
			if [ $exitCode -gt 0 ]; then
				handleBatchFailure
				continue
			fi

			retries=0
			batch=$((batch+1))
			reportProgress $i $batch $totalBatches $progressFile
		done
	) &
	i=$((i+1))
done

wait

if [ -f $failureFile ]; then
	echo -e "\n\n(`date --iso-8601=minutes`) Giving up after a shard failed."
	rm -f $failureFile

	exit 1
fi

# Open the json list
echo '[' | gzip -f > "$tempDir/${projectName}-${dumpName}.json.gz"

sawOutput=0
i=0
while [ $i -lt $shards ]; do

	getTempFiles "${tempDir}/${projectName}-${dumpName}.${i}-batch*.gz"
	if [ -z "$tempFiles" ]; then
		echo "No files for shard $i!"
		exit 1
	fi
	getFileSize "$tempFiles"
	if [ $fileSize -lt ${dumpNameToMinSize[$dumpName]} ]; then
		echo "File size for shard $i is only $fileSize. Aborting."
		exit 1
	fi
	for tempFile in $tempFiles; do
		# If this file is non-empty, append it to the output
		if [ "$(zcat "$tempFile" | head -c 5 | wc -c)" -lt 4 ]; then
			continue
		fi
		if [ $sawOutput -gt 0 ]; then
			# If we had output before, make sure to separate the data with ",\n"
			echo ',' | gzip >> "${tempDir}/${projectName}-${dumpName}.json.gz"
		fi
		sawOutput=1
		cat "$tempFile" >> "${tempDir}/${projectName}-${dumpName}.json.gz"
	done
	i=$((i+1))
done

# Close the json list
echo -e '\n]' | gzip -f >> "$tempDir/${projectName}-${dumpName}.json.gz"

# count the number of skipped entities from Exception log tag
skipped_entities=$( grep "failed-to-dump" $DUMP_REPORT | wc -l || true)
echo "Number of skipped entities: $skipped_entities"
if ! curl_output=$(
	echo "wikidata_dumps_skipped_entities_json ${skipped_entities}" \
	| curl -sS --data-binary @- ${PROMETHEUS_PUSH_URL} 2>&1
); then
    echo "Warning: Failed to push metrics to Prometheus: ${curl_output}" >&2
fi

i=0
while [ $i -lt $shards ]; do
	getTempFiles "${tempDir}/${projectName}-${dumpName}.${i}-batch*.json.gz"
	rm -f $tempFiles
	i=$((i+1))
done

moveLinkFile "${tempDir}/${projectName}-${dumpName}.json.gz" \
	"${targetDir}/${filename}.json.gz" \
	"${targetDirBase}/latest-${dumpName}.json.gz"

nthreads=$(( $shards / 2))
if [ $nthreads -lt 1 ]; then
    nthreads=1
fi

gzip -dc $targetDir/$filename.json.gz | "$lbzip2" -n $nthreads -c > "${tempDir}/${projectName}-${dumpName}.json.bz2"

moveLinkFile "${tempDir}/${projectName}-${dumpName}.json.bz2" \
	"${targetDir}/${filename}.json.bz2" \
	"${targetDirBase}/latest-${dumpName}.json.bz2"

setDcatConfig
runDcat
