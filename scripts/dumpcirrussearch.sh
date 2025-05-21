#!/bin/bash

# Generate a json dump of cirrussearch indices for all enabled
# wikis and remove old ones.

source /usr/local/etc/dump_functions.sh

usage() {
	echo "Usage: $0 [--config <pathtofile>] [--dryrun] [--dblist <pathtofile>]"
	echo
	echo "  --config  path to configuration file for dump generation"
	echo "            (default value: ${confsdir}/wikidump.conf.other"
	echo "  --dryrun  don't run dump, show what would have been done"
	echo "  --dblist  run dump against specified dblist instead of the all wikis dblist"
	exit 1
}

configFile="${confsdir}/wikidump.conf.other"
dryrun="false"

while [ $# -gt 0 ]; do
	if [ "$1" = "--config" ]; then
		configFile="$2"
		shift; shift;
	elif [ "$1" = "--dryrun" ]; then
		dryrun="true"
		shift
	elif [ "$1" = "--dblist" ]; then
		dbList="$2"
		shift; shift;
	else
		echo "$0: Unknown option $1"
		usage
	fi
done

if [ ! -f "$configFile" ]; then
	echo "Could not find config file: $configFile"
	echo "Exiting..."
	exit 1
fi

args="wiki:dblist,privatelist,multiversion;output:temp;tools:gzip,php"
results=$(python3 "${repodir}/getconfigvals.py" --configfile "$configFile" --args "$args")

allList=$(getsetting "$results" "wiki" "dblist") || exit 1
privateList=$(getsetting "$results" "wiki" "privatelist") || exit 1
multiversion=$(getsetting "$results" "wiki" "multiversion") || exit 1
tempDir=$(getsetting "$results" "output" "temp") || exit 1
gzip=$(getsetting "$results" "tools" "gzip") || exit 1
php=$(getsetting "$results" "tools" "php") || exit 1

for settingname in "allList" "privateList" "multiversion" "tempDir" "gzip" "php"; do
	checkval "$settingname" "${!settingname}"
done

if [ -z "$dbList" ]; then
	dbList="$allList"
fi
if [ ! -f "$dbList" ]; then
	echo "Could not find dblist: $dbList"
	echo "Exiting..."
	exit 1
fi

today=$(date +'%Y%m%d')
targetDirBase="${systemdjobsdir}/cirrussearch"
targetDir="$targetDirBase/$today"
multiVersionScript="${multiversion}/MWScript.php"
hasErrors=0

# create todays folder
if [ "$dryrun" = "true" ]; then
	echo "mkdir -p '$targetDir'"
else
	if ! mkdir -p "$targetDir"; then
		echo "Can't make output directory: $targetDir"
		echo "Exiting..."
		exit 1
	fi
fi

# iterate over all known wikis
while read wiki; do
	# exclude all private wikis
	if ! grep -E -q "^$wiki$" "$privateList"; then
		# most wikis only have two indices
		suffixes="content general"
		# commonswiki is special, it also has a file index
		if [ "$wiki" = "commonswiki" ]; then
			suffixes="$suffixes file"
		fi
		# run the dump for each index type
		for indexSuffix in $suffixes; do
			filename="$wiki-$today-cirrussearch-$indexSuffix"
			targetFile="$targetDir/$filename.json.gz"
			tempFile="$tempDir/$filename.json.gz"
			if [ -e "$tempFile" ] || [ -e "$targetFile" ]; then
				echo "$targetFile or $tempFile already exists, skipping..."
				hasErrors=1
			else
				if [ "$dryrun" = "true" ]; then
					echo "$php '$multiVersionScript' extensions/CirrusSearch/maintenance/DumpIndex.php --wiki='$wiki' --indexSuffix='$indexSuffix' | $gzip > '$tempFile'"
					echo "mv '$tempFile' '$targetFile'"
				else
					$php "$multiVersionScript" \
						extensions/CirrusSearch/maintenance/DumpIndex.php \
						--wiki="$wiki" \
						--indexSuffix="$indexSuffix" \
						| $gzip > "$tempFile"
					PSTATUS_COPY=( "${PIPESTATUS[@]}" )
					if [ "${PSTATUS_COPY[0]}" = "0" ] && [ "${PSTATUS_COPY[1]}" = "0" ]; then
						mv "$tempFile" "$targetFile"
					else
						echo "extensions/CirrusSearch/maintenance/DumpIndex.php failed for $targetFile"
						rm "$tempFile"
						hasErrors=1
					fi
				fi
			fi
		done
	fi
done < "$dbList"

# Maintain a 'current' symlink always pointing at the most recently completed dump
# Note that this could be somewhat out of sync when the script is invoked multiple times
# in parallel with separate dblists, but hopefully close enough. The symlink will swap when
# the first task finishes, rather than when the full dump is complete.
if [ "$dryrun" = "false" ]; then
	cd "$targetDirBase"
	rm -f "current"
	ln -s "$today" "current"
fi

exit $hasErrors
