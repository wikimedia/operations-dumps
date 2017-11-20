#!/bin/bash
# no error checking, we don't care. if file fails we'll
# rerun it by hand later

# locks wiki for date, recompresses revision history content bz2
# files to 7z files, doing recompression in batches.
# does NOT: update md5s, status, dumprininfo, symlinks, etc.
# does NOT: clean up old dumps, remove old files from run

usage() {
	cat<<EOF
Usage: $0 --config <pathtofile> --wiki <dbname>
  --date <YYYYMMDD> --jobinfo num:num:num,...
 [--skiplock] [--dryrun] [--verbose]

  --config   path to configuration file for dump generation
  --wiki     dbname of wiki
  --jobinfo  partnum,partnum2,...
  --date     date of run
  --numjobs  number of jobs to run simultaneously
  --skiplock don't lock the wiki (use with care!)
  --dryrun   don't run commands, show what would have been done
  --verbose print commands as they are run, etc
EOF
	exit 1
}

set_defaults() {
    vars="CONFIGFILE WIKI JOBINFO DATE NUMJOBS SKIPLOCK DRYRUN VERBOSE"
    for varname in $vars; do
        declare $varname="";
    done
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
		usage && exit 1
	fi
    done
}

check_opts() {
    if [ -z "$WIKI" -o -z "$JOBINFO" -o -z "$DATE" -o -z "$CONFIGFILE" -o -z "$NUMJOBS" ]; then
        echo "$0: Mandatory options 'wiki', 'jobinfo', 'date', 'numjobs' and 'config' must be specified"
        usage && exit 1
    fi
    # sanity check of date
    result=`date -d "$DATE"`
    if [ -z "$result" ]; then
	echo "bad date given for 'date' arg"
        usage && exit 1
    fi
}

get_dumps_output_dir() {
    DUMPS_OUTPUT_ROOT=$( /usr/bin/python $WIKIDUMP_BASE/getconfigvals.py --configfile "$CONFIGFILE" --args 'output:public' --format values )
    if [ -z "$DUMPS_OUTPUT_ROOT" ]; then
	echo "Failed to get dumps output root dir from config file, giving up"
	exit 1
    fi
    DUMPS_OUTPUT_DIR="${DUMPS_OUTPUT_ROOT}/${WIKI}/${DATE}"
}

get_bz2files_completed() {
    # get list of bz2files we would compress, remove from the
    # list all those that are not yet complete (they are still
    # being written, etc), we will not recompress those.

    bz2files_completed=()
    bz2files=$( ls "${DUMPS_OUTPUT_DIR}/${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml"*.bz2 )
    for bz2file in $bz2files; do
        /usr/local/bin/checkforbz2footer "$bz2file";
        if [ $? -eq 0 ]; then
            bz2files_completed=( "${bz2files_completed[@]}" "$bz2file" )
        fi
    done
}

setup_recompression_command() {
    inputfile="$1"
    outputfile=$( echo $inputfile | sed -e 's/.bz2/.7z/g;' )
    ZCAT_COMMAND=("/bin/bzcat" "$inputfile")
    SEVENZ_COMMAND=("/usr/bin/7za" "a" "-mx=4" "-si" "$outputfile")
}

do_recompression() {
    # this many processes at once
    LIMIT="$1"
    while :
    do
        if [ ${#bz2files_completed[*]} -eq 0 ]; then
            break
	elif [ ${#bz2files_completed[*]} -lt $LIMIT ]; then
            end=${#bz2files_completed[*]}
	else
            end=$LIMIT
	fi

	files_in_batch=(${bz2files_completed[@]:0:$end})

        wait_pids=()
	files_doing=()
        if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
            echo "new batch"
        fi
	for filename in ${files_in_batch[@]}; do
            setup_recompression_command "$filename"
            if [ -e $outputfile ]; then
                continue;
            fi
	    if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		echo  "${ZCAT_COMMAND[@]} | ${SEVENZ_COMMAND[@]}"
	    fi
	    if [ -z "$DRYRUN" ]; then
                ( ${ZCAT_COMMAND[@]} | ${SEVENZ_COMMAND[@]} ) &
	        wait_pids+=($!)
		files_doing+=("$outputfile")
            fi
	done
	i=0
	for pid in ${wait_pids[*]}; do
	    wait $pid
	    if [ $? -ne 0 ]; then
		echo "failed to generate" ${files_doing[$i]} "with nonzero exit code"
            fi
	    ((i++))
	done
	bz2files_completed=(${bz2files_completed[@]:$end})
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
set_defaults || exit 1
process_opts "$@" || exit 1
check_opts || exit 1
IFS=',' read -a JOBARRAY <<< "$JOBINFO"
if [ -z "$SKIPLOCK" ]; then
    lockerup || exit 1
fi
get_dumps_output_dir || exit 1
for PARTNUM in ${JOBARRAY[*]}; do
    get_bz2files_completed
    do_recompression $NUMJOBS
done
if [ -z "$SKIPLOCK" ]; then
    cleanup_lock
fi
