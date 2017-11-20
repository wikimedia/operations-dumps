#!/bin/bash
# no error checking, we don't care. if file fails we'll
# rerun it by hand later

# locks wiki for date, generates a bunch of page ranges based on
# user input, runs a bunch of jobs in batches to create the
# specified page-meta-history bz2 output files, unlocks wiki.
# does NOT: update md5s, status, dumprininfo, symlinks, etc.
# does NOT: clean up old dumps, remove old files from run

usage() {
	echo "Usage: $0 --config <pathtofile> --wiki <dbname>"
	echo "  --date <YYYYMMDD> --jobinfo num:num:num,..."
	echo "[--skiplock] [--dryrun] [--verbose]"
	echo
	echo "  --config   path to configuration file for dump generation"
	echo "  --wiki     dbname of wiki"
	echo "  --jobinfo  partnum:start:end,partnum2:start:end,..."
	echo "  --date     date of run"
        echo "  --numjobs  number of jobs to run simultaneously"
        echo "  --skiplock don't lock the wiki (use with care!)"
	echo "  --dryrun   don't run commands, show what would have been done"
	echo "  --verbose print commands as they are run, etc"
	exit 1
}

set_defaults() {
    CONFIGFILE=""
    WIKI=""
    JOBINFO=""
    DATE=""
    NUMJOBS=""
    SKIPLOCK=""
    DRYRUN=""
    VERBOSE=""
}

process_opts () {
    while [ $# -gt 0 ]; do
	if [ $1 == "--config" ]; then
		CONFIGFILE="$2"
		shift; shift;
	elif [ $1 == "--wiki" ]; then
		WIKI="$2"
		shift; shift
	elif [ $1 == "--jobinfo" ]; then
		JOBINFO="$2"
		shift; shift
	elif [ $1 == "--date" ]; then
		DATE="$2"
		shift; shift
	elif [ $1 == "--numjobs" ]; then
		NUMJOBS="$2"
		shift; shift
	elif [ $1 == "--skiplock" ]; then
		SKIPLOCK="true"
		shift
	elif [ $1 == "--dryrun" ]; then
		DRYRUN="true"
		shift
	elif [ $1 == "--verbose" ]; then
		VERBOSE="true"
		shift
	else
		echo "$0: Unknown option $1"
		usage
	fi
    done
}

check_opts() {
    if [ -z "$WIKI" -o -z "$JOBINFO" -o -z "$DATE" -o -z "$CONFIGFILE" -o -z "$NUMJOBS" ]; then
        echo "$0: Mandatory options 'wiki', 'jobinfo', 'date', 'numjobs' and 'config' must be specified"
        usage
    fi
}

setup_pagerange_args() {
    # set up the command
    pagerangeargs=( "$WIKIDUMP_BASE/get_pagerange.py" )
    pagerangeargs=( "${pagerangeargs[@]}" "--configfile" "$CONFIGFILE" )
    if [ -n "$START" ]; then
	pagerangeargs=( "${pagerangeargs[@]}" "--start" "$START" )
    fi
    if [ -n "$END" ]; then
	pagerangeargs=( "${pagerangeargs[@]}" "--end" "$END" )
    fi
    pagerangeargs=( "${pagerangeargs[@]}" "--wiki" "$WIKI" )

    # pipeline args go here
    grepargs=( "grep" "-v" "DEBUG" )
    jqargs=( "/usr/bin/jq" "-r"  '.[]|.pstart+":"+.pend' )
}

get_ranges() {
    if [ -n "$VERBOSE" ]; then
	echo "/usr/bin/python ${pagerangeargs[@]} | ${grepargs[@]} | ${jqargs[@]}"
    fi
    ranges=( $(/usr/bin/python ${pagerangeargs[@]} | ${grepargs[@]} | ${jqargs[@]}) )
    result=$?
    if [ $result -ne 0 ]; then
	echo "Failed to get page ranges, dumping them here"
	echo "${ranges[@]}"
	exit 1
    fi
}

setup_worker_args() {
    FILE="$1"
    # set up the command
    workerargs=( "$WIKIDUMP_BASE/worker.py" )
    workerargs=( "${workerargs[@]}" "--configfile" "$CONFIGFILE" )
    # workerargs=( "${workerargs[@]}" "--log" )
    workerargs=( "${workerargs[@]}" "--job" "metahistorybz2dump" )
    # sanity check of date
    result=`date -d "$DATE"`
    if [ -z "$result" ]; then
	echo "bad date given for 'date' arg"
	exit 1
    fi
    workerargs=( "${workerargs[@]}" "--date" "$DATE" )
    workerargs=( "${workerargs[@]}" "--checkpoint" "$FILE" )
    workerargs=( "${workerargs[@]}" "$WIKI" )
}

run_workers() {
    # run this many workers at once
    LIMIT="$1"
    while :
    do
	if [ ${#ranges[*]} -eq 0 ]; then
            break
	elif [ ${#ranges[*]} -lt $LIMIT ]; then
            end=${#ranges[*]} 
	else
            end=$LIMIT
	fi

	tenpairs=(${ranges[@]:0:$end})

        wait_pids=()
	files=()
	for pagerange in ${tenpairs[@]}; do
	    IFS=: read startpage endpage <<< "$pagerange"
	    outputfile="${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml-p${startpage}p${endpage}.bz2"
	    setup_worker_args "$outputfile"
	    if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		echo "${workerargs[@]}"
	    fi
	    if [ -z "$DRYRUN" ]; then
	        /usr/bin/python ${workerargs[@]} &
	        wait_pids+=($!)
		files+=("$outputfile")
            fi
	done
	i=0
	for pid in ${wait_pids[*]}; do
	    wait $pid
	    if [ $? -ne 0 ]; then
		echo "failed to generate" ${files[$i]} "with nonzero exit code"
            fi
	    ((i++))
	done
	ranges=(${ranges[@]:$end})
    done
}

lockerup() {
    if [ -z "$DRYRUN" ]; then
        /usr/bin/python "$WIKIDUMP_BASE/dump_lock.py" --wiki $WIKI --date $DATE --configfile $CONFIGFILE &
        lockerpid=$!
	sleep 2  #  wait a bit, give the process time to finish up if it failed
	# see if it's still running (which means it got the lock)
	kill -0 "$lockerpid" >/dev/null 2>&1
	if [ $? -ne 0 ]; then
	    echo "failed to get lock, exiting"
	    exit 1
	elif [ -n "$VERBOSE" ]; then
	    echo "got lock"
	fi
    fi
}

cleanup_lock() {
    if [ -z "$DRYRUN" ]; then
	if [ -n "$lockerpid" ]; then
           kill -HUP $lockerpid
	fi
	if [ -n "$VERBOSE" ]; then
	    echo "removed lock"
	fi
    fi
}

WIKIDUMP_BASE=`dirname "$0"`
#DUMPFILESBASE="/mnt/data/xmldatadumps/public"
DUMPFILESBASE="/home/ariel/dumptesting/dumpruns/public"
set_defaults
process_opts "$@"
check_opts
IFS=',' read -a JOBARRAY <<< "$JOBINFO"
if [ -z "$SKIPLOCK" ]; then
    lockerup
fi
for JOB in ${JOBARRAY[*]}; do
    IFS=: read PARTNUM START END <<< "$JOB"
    setup_pagerange_args
    get_ranges
    run_workers $NUMJOBS
done
if [ -z "$SKIPLOCK" ]; then
    cleanup_lock
fi
