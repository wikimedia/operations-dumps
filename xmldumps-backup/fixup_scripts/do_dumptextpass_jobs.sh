#!/bin/bash
# no error checking, we don't care. if file fails we'll
# rerun it by hand later

# based on user args, checks for existence of stub files covering
# specified page range, determines which 7z input files from
# previous dump run to use for prefetch for each of these, and
# uses the stubs and prefetch files as args to dumpTextPass.php
# running the script in batches to create the corresponding
# page-meta-history bz2 output files
# optionally locks wiki for date during the run
#
# output files are written to the wiki's temp dir and only
# moved into place once they are verified
#
# does NOT: update md5s, status, dumpruninfo, symlinks, etc.
# does NOT: clean up old dumps, remove old files from run
# this should be done by running a noop via the regular dumps system
# or some similar means

usage() {
    cat<<EOF
Usage: $0 --wiki <dbname> --config <pathtofile>
          --date <YYYYMMDD> --jobinfo num:num:num,...
	 [--outdir <path>]
         [--skiplock] [--dryrun] [--verbose]

Arguments:
  --config   path to configuration file for dump generation
  --wiki     dbname of wiki
  --jobinfo  partnum:start:end,partnum2:start:end,...
  --date     date of run
  --numjobs  number of jobs to run simultaneously
  --outdir   directory into which to write output files
             (overrides values derived from config file)
             prefetchs and locks will be read/written to
	     the configured location however
  --skiplock don't lock the wiki (use with care!)
  --dryrun   don't run commands, show what would have been done
  --verbose  print commands as they are run, etc

Example run:
bash do_dumptextpass_jobs.sh --wiki wikidatawiki --jobinfo 3:301:600,4:2971:4342  --date 20190916 --numjobs 3
        --skiplock --config ~/dumptesting/confs/wikidump.conf.current:wikidatawiki --verbose

This expects stub files covering the specified ranges to have been
created already! If you don't have them, you should be using
 do_pagerange_content_jobs.sh instead.

Note that it's fine to specify a page range that covers several
stubs together, as long as the stubs have no gaps; one job will
be run per stub.
EOF
	exit 1
}

set_defaults() {
    CONFIGFILE=""
    WIKI=""
    RANGEINFO=""
    DATE=""
    OUTDIR=""
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
		RANGEINFO="$2"
		shift; shift
	elif [ $1 == "--date" ]; then
		DATE="$2"
		shift; shift
	elif [ $1 == "--numjobs" ]; then
		NUMJOBS="$2"
		shift; shift
	elif [ $1 == "--outdir" ]; then
		OUTDIR="$2"
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
    if [ -z "$WIKI" -o -z "$RANGEINFO" -o -z "$DATE" -o -z "$NUMJOBS" -o -z "$CONFIGFILE" ]; then
        echo "$0: Mandatory options 'wiki', 'jobinfo', 'date', 'numjobs' and 'config' must be specified"
        usage
    fi
}

getsetting() {
    results=$1
    section=$2
    setting=$3
    echo "$results" | /usr/bin/jq -M -r ".$section.$setting"
}

get_config_settings() {
    args='output:public,temp;tools:php;wiki:multiversion,dir'
    results=$( python3 "${WIKIDUMP_BASE}/getconfigvals.py" --configfile "$CONFIGFILE" --args "$args" )

    MULTIVERSION=$( getsetting "$results" "wiki" "multiversion" ) || exit 1
    # it's ok for this value not to be in the config; this means we aren't
    # running a wikifarm
    if [ "$MULTIVERSION" == "null" ]; then
	MULTIVERSION=""
    fi

    PHP=$( getsetting "$results" "tools" "php" ) || exit 1
    MWDIR=$( getsetting "$results" "wiki" "dir" ) || exit 1
    DUMPFILESBASE=$( getsetting "$results" "output" "public" ) || exit 1
    TEMPFILESBASE=$( getsetting "$results" "output" "temp" ) || exit 1

    if [ "$DUMPFILESBASE" == "null" ]; then
	echo "Failed to get dumps base dir from config file, giving up"
	exit 1
    fi
    if [ "$TEMPFILESBASE" == "null" ]; then
	echo "Failed to get dumps temp dir from config file, giving up"
	exit 1
    fi
    if [ "$PHP" == "null" ]; then
	echo "Failed to get php path from config file, giving up"
	exit 1
    fi
    if [ "$MWDIR" == "null" ]; then
	echo "Failed to get base MediaWiki dir from config file, giving up"
	exit 1
    fi

    # now that we have the root of the dumps output tree, we can set the
    # output dir if not given by the user
    if [ -z "$OUTDIR" ]; then
	OUTDIR="${DUMPFILESBASE}/${WIKI}/${DATE}"
    fi
}

get_stub_range() {
    file="$1"
    # format: ${WIKI}-${DATE}-stub-meta-history${PARTNUM}.xml-p[0-9]*p[0-9].gz
    # are there wiki names with - in them? yes. so can't do that
    # split at the period: field 1 (xml-pnnnpnnn)
    # split again at the p and we have 1 and 2
    IFS='.' read -ra STUBFIELDS <<< "$file"
    IFS='p' read -ra RANGEFIELDS <<< ${STUBFIELDS[1]}
    STUBSTART=${RANGEFIELDS[1]}
    STUBEND=${RANGEFIELDS[2]}
}

get_stub_files() {
    stubs_wanted=()
    stubs=$( ls "${TEMPFILESDIR}/${WIKI}-${DATE}-stub-meta-history${PARTNUM}".xml-p*.gz )
    result=$?
    if [ $result -ne 0 ]; then
	echo "Failed to find stubs files"
	exit 1
    fi
    # now we must make sure each stub file is in the range we wanted ($START $END)
    for stubfile in $stubs; do
	get_stub_range $stubfile
	if [ $STUBSTART -ge $START -a $STUBEND -le $END ]; then
	    stubs_wanted=( "${stubs_wanted[@]}" "$stubfile" )
	fi
    done
}

setup_textpass_args() {
    #/usr/bin/php7.2 /srv/mediawiki/multiversion/MWScript.php dumpTextPass.php --wiki=wikidatawiki --stub=gzip:/mnt/dumpsdata/xmldatadumps/temp/w/wikidatawiki/wikidatawiki-20190901-stub-meta-history1.xml-p82872p98330.gz --prefetch=7zip:/mnt/dumpsdata/xmldatadumps/public/wikidatawiki/20190801/wikidatawiki-20190801-pages-meta-history1.xml-p74429p85729.7z;/mnt/dumpsdata/xmldatadumps/public/wikidatawiki/20190801/wikidatawiki-20190801-pages-meta-history1.xml-p85730p103181.7z --report=1000 --spawn=/usr/bin/php7.2 --output=bzip2:/mnt/dumpsdata/xmldatadumps/temp/w/wikidatawiki/wikidatawiki-20190901-pages-meta-history1.xml-p82872p98330.bz2.inprog --full

    # sanity check of date
    result=`date -d "$DATE"`
    if [ -z "$result" ]; then
	echo "bad date given for 'date' arg"
	exit 1
    fi

    STUB="$1"
    OFILE="$2"
    PREFETCHES="$3"
    # set up the command
    if [ -n "$MULTIVERSION" ]; then
	dumptextargs=( "${MULTIVERSION}/MWScript.php" "dumpTextPass.php" )
    else
	dumptextargs=( "${MWDIR}/maintenance/dumpTextPass.php" )
    fi
    dumptextargs=( "${dumptextargs[@]}" "--wiki=${WIKI}" "--report=1000" "--spawn=$PHP" )
    dumptextargs=( "${dumptextargs[@]}" "--full" )
    dumptextargs=( "${dumptextargs[@]}" "--stub=gzip:${STUB}" )
    dumptextargs=( "${dumptextargs[@]}" "--output=bzip2:${TEMPFILESDIR}/${OFILE}.inprog" )
    if [ -n "$PREFETCHES" ]; then
	dumptextargs=( "${dumptextargs[@]}" "--prefetch=7zip:${PREFETCHES}" )
    fi
}

get_prev_good_full_run() {
    WIKIDATES=()
    SUBDIRS=$( ls -r "${DUMPFILESBASE}/${WIKI}/" )
    for possible_date in $SUBDIRS; do
	# for each entry make sure it's a date
	result=`date -d "${possible_date}" 2>/dev/null`
	if [ -z "$result" ]; then
	    continue
	fi
	# keep the ones with status.html contains 'Dump complete' and dumpruninfo indicates the job was run
	is_done=$( grep 'Dump complete' "${DUMPFILESBASE}/${WIKI}/${possible_date}/status.html" 2>/dev/null | wc -l )
	if [ $is_done -ne 1 ]; then
	    continue
	fi
	job_done=$( grep 'metahistorybz2dump' "${DUMPFILESBASE}/${WIKI}/${possible_date}/dumpruninfo.txt" | grep 'status:done' | wc -l )
	if [ $job_done -eq 1 ]; then
	    WIKIDATES=( "${WIKIDATES[@]}" "$possible_date" )
	fi
    done
    if [ ${#WIKIDATES[@]} -eq 0 ]; then
	echo "No prefetch files available for ${WIKI}, giving up"
	exit 1
    fi
    # get the most recent
    RUNDATE="${WIKIDATES[0]}"
    if [ -n "$VERBOSE" ]; then
	echo "found rundate $RUNDATE"
    fi
}

get_prefetch_range() {
    file="$1"
    # format: ${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml-p[0-9]*p[0-9].7z
    # are there wiki names with - in them? yes. so can't do that
    # split at the period: field 1 (xml-pnnnpnnn)
    # split again at the p and we have 1 and 2
    IFS='.' read -ra PREFETCHFIELDS <<< "$file"
    IFS='p' read -ra RANGEFIELDS <<< "${PREFETCHFIELDS[1]}"
    PREFETCHSTART="${RANGEFIELDS[1]}"
    PREFETCHEND="${RANGEFIELDS[2]}"
}

get_possible_prefetches() {
    PREFETCHDIR="${DUMPFILESBASE}/${WIKI}/${RUNDATE}"
    possible_prefetches=$( ls "${PREFETCHDIR}/${WIKI}-${RUNDATE}-pages-meta-history${PARTNUM}.xml-p"*.7z )
}

get_prefetches() {
    page_start="$1"
    page_end="$2"
    prefetches_wanted=()
    result=$?
    if [ $result -ne 0 ]; then
	echo "Failed to find prefetch files"
	exit 1
    fi
    # now keep only prefetch files covering our desired range
    for prefetchfile in $possible_prefetches; do
	get_prefetch_range $prefetchfile
	if [ $PREFETCHSTART -ge $page_start -a $PREFETCHSTART -le $page_end ]; then
	    prefetches_wanted=( "${prefetches_wanted[@]}" "$prefetchfile" )
	elif [ $PREFETCHSTART -le $page_start -a $PREFETCHEND -ge $page_start ]; then
	    prefetches_wanted=( "${prefetches_wanted[@]}" "$prefetchfile" )
	fi
    done
}

combine_prefetches() {
    prefetches=$(printf ";%s" "${prefetches_wanted[@]}")
    prefetches=${prefetches:1}
}

run_dumpers() {
    # run this many dumpers at once
    LIMIT="$1"
    while :
    do
	if [ ${#stubs_wanted[*]} -eq 0 ]; then
            break
	elif [ ${#stubs_wanted[*]} -lt $LIMIT ]; then
            end=${#stubs_wanted[*]} 
	else
            end=$LIMIT
	fi

	stubs_batch=(${stubs_wanted[@]:0:$end})

        wait_pids=()
	outfiles=()
	if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
	    echo "New batch..."
	fi
	for stub_doing in ${stubs_batch[@]}; do
	    EXISTS=""
	    get_stub_range $stub_doing
	    outputfile="${WIKI}-${DATE}-pages-meta-history${PARTNUM}.xml-p${STUBSTART}p${STUBEND}.bz2"
	    get_prefetches $STUBSTART $STUBEND || exit 1
	    combine_prefetches
	    setup_textpass_args "$stub_doing" "$outputfile" ${prefetches[@]}

	    # skip if there is a partial or complete output file already there
	    if [[ -f "${OUTDIR}/${outputfile}.inprog" ]] || [[ -f "${OUTDIR}/${outputfile}" ]]; then
		if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
	            echo "Output file ${OUTDIR}/${outputfile} already in progress, skipping"
		fi
		EXISTS="true"
	    fi

	    if [ -z "$EXISTS" ]; then
		if [ -n "$DRYRUN" -o -n "$VERBOSE" ]; then
		    echo "$PHP ${dumptextargs[@]}"
		fi
	    fi
	    if [ -z "$DRYRUN" -a -z "$EXISTS" ]; then
	        $PHP ${dumptextargs[@]} &
	        wait_pids+=($!)
		outfiles+=("$outputfile")
            fi
	done
	i=0
	for pid in ${wait_pids[*]}; do
	    wait $pid
	    if [ $? -ne 0 ]; then
		echo "failed to generate" ${outfiles[$i]} "with nonzero exit code"
	    elif $( /usr/local/bin/checkforbz2footer ${TEMPFILESDIR}/${outfiles[$i]}.inprog ); then
		# should we move over an existing file? no, we don't know what put it there, so manual
		# intervention will be required in that case. this may not indicate an error though
		if [ -f "${OUTDIR}/${outfiles[$i]}" ]; then
		    echo "File ${OUTDIR}/${outfiles[$i]} already exists, not writing over it"
		    mv ${TEMPFILESDIR}/${outfiles[$i]}.inprog ${TEMPFILESDIR}/${outfiles[$i]}
		else
		    mv ${TEMPFILESDIR}/${outfiles[$i]}.inprog ${OUTDIR}/${outfiles[$i]}
		fi
	    else
		echo "renaming truncated ${outfiles[$i]}"
		mv ${TEMPFILESDIR}/${outfiles[$i]}.inprog ${TEMPFILESDIR}/${outfiles[$i]}.truncated
            fi
	    ((i++))
	done
	stubs_wanted=(${stubs_wanted[@]:$end})
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
set_defaults
process_opts "$@"
check_opts
get_config_settings || exit 1
TEMPFILESDIR="${TEMPFILESBASE}/${WIKI:0:1}/${WIKI}"
IFS=',' read -a RANGEARRAY <<< "$RANGEINFO"
if [ -z "$SKIPLOCK" ]; then
    lockerup
fi

get_prev_good_full_run || exit 1

for RANGE in ${RANGEARRAY[*]}; do
    IFS=: read PARTNUM START END <<< "$RANGE"
    get_stub_files
    get_possible_prefetches
    run_dumpers $NUMJOBS
done
if [ -z "$SKIPLOCK" ]; then
    cleanup_lock
fi
