#!/bin/bash

# optionally locks wiki for date, writes md5 and
# sha1 hashes into files for page-meta-history
# files of the specified part number and type (bz2 or
# 7z) that do not already have them
# does NOT: update status, dumprininfo, symlinks, etc.
# does NOT: clean up old dumps, remove old files from run

usage() {
	cat<<EOF
Usage: $0 --config <pathtofile> --wiki <dbname>
  --date <YYYYMMDD> --jobinfo num,num,... --type bz2|7z
 [--skiplock] [--dryrun] [--verbose]

  --config   path to configuration file for dump generation
  --wiki     dbname of wiki
  --jobinfo  partnum,partnum2,...
  --type     bz2 or 7z
  --date     date of run
  --numjobs  number of jobs to run simultaneously
  --skiplock don't lock the wiki (use with care!)
  --dryrun   don't run commands, show what would have been done
  --verbose print commands as they are run, etc
EOF
	exit 1
}

set_defaults() {
    vars="CONFIGFILE WIKI JOBINFO DATE NUMJOBS SKIPLOCK TYPE DRYRUN VERBOSE"
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
	elif [ $1 == "--type" ]; then
		TYPE="$2"
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
    if [ -z "$WIKI" -o -z "$JOBINFO" -o -z "$DATE" -o -z "$CONFIGFILE" -o -z "$NUMJOBS" -o -z "$TYPE" ]; then
        echo "$0: Mandatory options 'wiki', 'jobinfo', 'date', 'numjobs', 'type' and 'config' must be specified"
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
    DUMPS_OUTPUT_ROOT=$( /usr/bin/python3 $WIKIDUMP_BASE/getconfigvals.py --configfile "$CONFIGFILE" --args 'output:public' --format values )
    if [ -z "$DUMPS_OUTPUT_ROOT" ]; then
	echo "Failed to get dumps output root dir from config file, giving up"
	exit 1
    fi
    DUMPS_OUTPUT_DIR="${DUMPS_OUTPUT_ROOT}/${WIKI}/${DATE}"
}

get_bz2files_good() {
    # get list of bz2files we would hash, remove from the
    # list all those that are incomplete (bad), we will not
    # hash those.

    TOHASH=()
    bz2files=$( ls "${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml"*.bz2 )
    for bz2file in $bz2files; do
        /usr/local/bin/checkforbz2footer "$bz2file"
        if [ $? -eq 0 ]; then
            TOHASH=( "${TOHASH[@]}" "$bz2file" )
        fi
    done
}

get_7zfiles() {
    # get list of 7zfiles to hash
    TOHASH=( $( ls "${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml"*.7z ) )
}

setup_hash_commands() {
    inputfile="$1"
    MD5_outputfile="md5sums-${inputfile}.txt"
    MD5_COMMAND=("/usr/bin/md5sum" "$inputfile")
    SHA1_outputfile="sha1sums-${inputfile}.txt"
    SHA1_COMMAND=("/usr/bin/sha1sum" "$inputfile")
}

do_hashes() {
    # this many processes at once
    LIMIT="$1"
    while :
    do
        if [ ${#TOHASH[*]} -eq 0 ]; then
            break
	elif [ ${#TOHASH[*]} -lt $LIMIT ]; then
            end=${#TOHASH[*]}
	else
            end=$LIMIT
	fi
	files_in_batch=(${TOHASH[@]:0:$end})
        wait_pids=()
	files_doing=()
        if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
            echo "new batch"
        fi
	for filename in ${files_in_batch[@]}; do
            setup_hash_commands "$filename"
            if [ ! -e ${MD5_outputfile} ]; then
		if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		    echo  "${MD5_COMMAND[@]} > ${MD5_outputfile}"
		fi
		if [ -z "$DRYRUN" ]; then
                    ( ${MD5_COMMAND[@]} > "$MD5_outputfile" ) &
	            wait_pids+=($!)
		    files_doing+=("$MD5_outputfile")
		fi
	    fi

            if [ ! -e ${SHA1_outputfile} ]; then
		if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		    echo  "${SHA1_COMMAND[@]} > ${SHA1_outputfile}"
		fi
		if [ -z "$DRYRUN" ]; then
                    ( ${SHA1_COMMAND[@]} > "$SHA1_outputfile" ) &
	            wait_pids+=($!)
		    files_doing+=("$SHA1_outputfile")
		fi
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
	TOHASH=(${TOHASH[@]:$end})
    done
}

lockerup() {
    if [ -z "$DRYRUN" ]; then
        /usr/bin/python3 "$WIKIDUMP_BASE/dump_lock.py" --wiki $WIKI --date $DATE --configfile $CONFIGFILE &
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

WIKIDUMP_BASE=$( dirname "$0" )
WIKIDUMP_BASE="${WIKIDUMP_BASE}/.."
set_defaults || exit 1
process_opts "$@" || exit 1
check_opts || exit 1
IFS=',' read -a JOBARRAY <<< "$JOBINFO"
if [ -z "$SKIPLOCK" ]; then
    lockerup || exit 1
fi
get_dumps_output_dir || exit 1
# NUMJOBS needs to be divided in 2 because
# we will run md5 and sha1 for a given file at the same time
NUMJOBS=$(( $NUMJOBS/2 ))
if [[ $NUMJOBS -eq 0 ]]; then
    NUMJOBS=1
fi
if [ -n "$VERBOSE" ]; then
    echo "Doing ${NUMJOBS} file(s) at once"
fi

cd "$DUMPS_OUTPUT_DIR"
for PARTNUM in ${JOBARRAY[*]}; do
    if [ "$TYPE" == "bz2" ]; then
	get_bz2files_good
    else
	get_7zfiles
    fi
    do_hashes $NUMJOBS
done
cd "$WIKIDUMP_BASE"

if [ -z "$SKIPLOCK" ]; then
    cleanup_lock
fi
