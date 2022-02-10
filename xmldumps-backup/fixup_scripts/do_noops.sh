#!/bin/bash
# no error checking needed; this script runs the noop job which will just
# generate hash files if any are missing, and update the various status files.
# this script locks each wiki when running the noop job.

# to generate a list of wikis, for example those starting with "b" and "c" but excluding
# "big" wikis, you can...
# mywikis=$( cd /mnt/dumpsdata/xmldatadumps/public/; ls -d b*wik* c*wik* | grep -v commonswiki | paste -s -d ',' )
# and pass that in to the script:
# bash ./fixup_scripts/do_noops.sh --date 20220201 --config /etc/dumps/confs/wikidump.conf.dumps  --numwikis 20 --wikis $mywikis

# NOTE that you need to check the wikis in /etc/dumpsdblists/bigwikis.dblist yourself
# and grep them out of the list; otherwise the same config file will be used for any
# of those included with the rest of the wikis, which at least in our setup is not
# what we want.

usage() {
	echo "Usage: $0 --config <pathtofile> --wikis <dbname>[,<dbname>...]"
	echo "  --numwikis <int> --date <YYYYMMDD> [--dryrun] [--verbose]"
	echo
	echo "  --config   path to configuration file for dump generation"
	echo "  --wikis     dbnames of wikis"
	echo "  --date     date of run"
        echo "  --numwikis  number of wikis for which to run the job simultaneously"
	echo "  --dryrun   don't run commands, show what would have been done"
	echo "  --verbose print commands as they are run, etc"
	exit 1
}

set_defaults() {
    CONFIGFILE=""
    WIKIS=""
    DATE=""
    NUMWIKIS=""
    DRYRUN=""
    VERBOSE=""
}

process_opts () {
    while [ $# -gt 0 ]; do
	if [ $1 == "--config" ]; then
		CONFIGFILE="$2"
		shift; shift;
	elif [ $1 == "--wikis" ]; then
		WIKIS="$2"
		shift; shift
	elif [ $1 == "--date" ]; then
		DATE="$2"
		shift; shift
	elif [ $1 == "--numwikis" ]; then
		NUMWIKIS="$2"
		shift; shift
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
    if [ -z "$WIKIS" -o -z "$DATE" -o -z "$CONFIGFILE" -o -z "$NUMWIKIS" ]; then
        echo "$0: Mandatory options 'wikis', 'date', 'numwikis' and 'config' must be specified"
        usage
    fi
}

setup_worker_args() {
    WIKI_TO_RUN=$1
    # set up the command
    workerargs=( "$WIKIDUMP_BASE/worker.py" )
    workerargs=( "${workerargs[@]}" "--configfile" "$CONFIGFILE" )
    workerargs=( "${workerargs[@]}" "--log" )
    workerargs=( "${workerargs[@]}" "--job" "noop" )
    workerargs=( "${workerargs[@]}" "--exclusive" )
    # sanity check of date
    result=`date -d "$DATE"`
    if [ -z "$result" ]; then
	echo "bad date given for 'date' arg"
	exit 1
    fi
    workerargs=( "${workerargs[@]}" "--date" "$DATE" )
    workerargs=( "${workerargs[@]}" "$WIKI_TO_RUN" )
}

run_workers() {
    # run this many workers at once
    LIMIT="$1"
    wikis_todo=("${WIKIARRAY[@]}")
    while :
    do
        if [ ${#wikis_todo[*]} -eq 0 ]; then
            break
	elif [ ${#wikis_todo[*]} -lt $LIMIT ]; then
            end=${#wikis_todo[*]}
	else
            end=$LIMIT
	fi

	wikis_batch=(${wikis_todo[@]:0:$end})

        wait_pids=()
	files_doing=()
        if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
            echo "new batch"
        fi
	for wikiname in ${wikis_batch[@]}; do
	    # skip wikis that might be in some list but deleted so we didn't run them,
	    # wikis where you have a typo, "wikis" that are just junk files in the top
	    # level dir that you got from an ls, etc.
	    if [ -e $DUMPFILESBASE/$wikiname/$DATE ]; then
  		setup_worker_args $wikiname
		if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		    echo "/usr/bin/python3 ${workerargs[@]}"
		fi
		if [ -z "$DRYRUN" ]; then
		    /usr/bin/python3 ${workerargs[@]} &
	            wait_pids+=($!)
		    wikis_doing+=("$wikiname")
		fi
            fi
	done
	i=0
	for pid in ${wait_pids[*]}; do
	    wait $pid
	    if [ $? -ne 0 ]; then
		echo "failed to do noop for " ${wikis_doing[$i]} "with nonzero exit code"
            fi
	    ((i++))
	done
	wikis_todo=(${wikis_todo[@]:$end})
    done
}

WIKIDUMP_BASE=$( dirname "$0" )
WIKIDUMP_BASE="${WIKIDUMP_BASE}/.."
DUMPFILESBASE="/mnt/dumpsdata/xmldatadumps/public"
set_defaults
process_opts "$@"
check_opts
IFS=',' read -a WIKIARRAY <<< "$WIKIS"

run_workers $NUMWIKIS
