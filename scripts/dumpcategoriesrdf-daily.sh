#!/bin/bash

# Generate a daily list of changes for all wikis in
# categories-rdf list and remove old ones.


. /usr/local/bin/dumpcategoriesrdf-shared.sh

targetDirBase="${categoriesDirBase}/daily"
timestampsDir="${targetDirBase}/lastdump"
fullTimestampsDir="${categoriesDirBase}/lastdump"
targetDir="${targetDirBase}/${today}"
fileSuffix="sparql.gz"

createFolder

# iterate over configured wikis
cat "$dbList" | while read wiki; do
	# exclude all private wikis
	if ! egrep -q "^${wiki}$" "$privateList"; then
		filename="${wiki}-${today}-daily"
		targetFile="${targetDir}/${filename}.${fileSuffix}"
		tsFile="${timestampsDir}/${wiki}-daily.last"
		fullTsFile="${fullTimestampsDir}/${wiki}-categories.last"
		# get latest timestamps
		if [ -f "$fullTsFile" ]; then
			fullTs=`cat $fullTsFile`
		fi
		if [ -z "$fullTs" ]; then
			echo "Can not find full dump timestamp at $fullTsFile!"
			continue
		fi
		if [ -f "$tsFile" ]; then
			lastTs=`cat $tsFile`
		fi
		if [ -z "$lastTs" ]; then
			lastTs=$fullTs
		fi
		# if dump is more recent than last daily, we have to generate diff between dump and now
		if [ "$fullTs" -gt "$lastTs" ]; then
			dumpTs=${fullTs:0:8}
			fromTargetFile="${targetDir}/fromDump${dumpTs}-${filename}.${fileSuffix}"
			if [ "$dryrun" == "true" ]; then
				# get only day TS
				echo "$php $multiVersionScript maintenance/categoryChangesAsRdf.php --wiki=$wiki -s $fullTs -e $ts 2> /var/log/categoriesrdf/${filename}-daily.log | $gzip > $fromTargetFile"
			else
				$php "$multiVersionScript" maintenance/categoryChangesAsRdf.php --wiki="$wiki" -s $fullTs -e $ts 2> "/var/log/categoriesrdf/${filename}-daily.log" | "$gzip" > "$fromTargetFile"
			fi
		fi
		# create daily diff
		if [ "$dryrun" == "true" ]; then
			echo "$php $multiVersionScript maintenance/categoryChangesAsRdf.php --wiki=$wiki -s $lastTs -e $ts 2>> /var/log/categoriesrdf/${filename}-daily.log | $gzip > $targetFile"
			echo "Timestamp: $ts > $tsFile"
		else
			$php "$multiVersionScript" maintenance/categoryChangesAsRdf.php --wiki="$wiki" -s $lastTs -e $ts 2>> "/var/log/categoriesrdf/${filename}-daily.log" | "$gzip" > "$targetFile"
			echo "$ts" > "$tsFile"
		fi

	fi
done

makeLatestLink
