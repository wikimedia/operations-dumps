#!/bin/bash

# This script generates a list of the last n sets of XML dump files
# per project that were successful, adding failed dumps to the list if there
# are not n successful dumps available.  

# Options:
# dirsonly    -- list only the directories to include
# dumpsnumber -- number of dumps to list
# outputfile  -- path to file in which to write the list
# configfile  -- path to config file used to generate dumps

usage() {
    echo "Usage: $0 --dumpsnumber n --outputfile filename --configfile filename --rsyncprefix path"
    echo 
    echo "  dirsonly          list only directories to include"
    echo "  dumpsnumber       number of dumps to list"
    echo "  outputfile        name of file to which we will write iw action list"
    echo "  configfile        name of configuration file for dump generation"
    echo "                    (default value: wikidump.conf)"
    echo "  rsyncprefix       path to substitute in place of the public path supplied"
    echo "                    in the configuration file, if needed"
    echo 
    echo "For example:"
    echo "   $0 --dumpsnumber 5 --outputfile /data/dumps/public/dumpsfiles_for_rsync.txt --configfile wikidump.conf.testing"

    exit 1
}

check_args() {
    if [ -z "$dumpsnumber" ]; then
	echo "$0: dumpsnumber must be an integer greater than 0"
	usage
    fi
    if ! [[ "$dumpsnumber" =~ ^[0-9]+$ ]] ; then
	echo "$0: dumpsnumber must be an integer greater than 0"
	usage
    fi
    if [ "$dumpsnumber" -lt "1" ]; then
	echo "$0: dumpsnumber must be an integer greater than 0"
	usage
    fi
    if [ -z "$outputfile" ]; then
	echo "No value was given for outfile option."
	usage
    fi
    if [ -z "$configfile" ]; then
	echo "No value was given for configfile option."
	usage
    fi
    if [ ! -f "$configfile" ]; then
	echo "$0: can't open configuration file $configfile, exiting..."
	exit 1
    fi
}


listdumpsforproject() {
    # cannot rely on timestamp. sometimes we have rerun a phase in 
    # some earlier dump and have it completed later than a later dump,
    # or we may have two en pedia runs going at once in different 
    # phases.
    dirs=`ls -dr $publicdir/$p/20* 2>/dev/null`

    for day in $dirs; do
	# tools, mw, static...
	if [ -d "$day" ]; then
	    complete=`grep "Dump complete" "$day/status.html" 2>/dev/null | grep -v "failed" 2>/dev/null`
	    if [ ! -z "$complete" ]; then
		complete_dumps=("${complete_dumps[@]}" "$day")
	    fi
	    failed=`grep "Dump complete" "$day/status.html" 2>/dev/null | grep "failed" 2>/dev/null`
	    if [ ! -z "$failed" ]; then
		failed_dumps=("${failed_dumps[@]}" "$day")
	    fi
	fi
    done
}

list_dir_only() {
    if [ "$rsyncprefix" == "false" ]; then
	ls -d $d 2>/dev/null >> $outputfile.tmp
    else
	ls -d $d 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
    fi
}

list_files_in_dir() {
    if [ ! -f "$outputfile.tmp" ]; then
	touch $outputfile.tmp
    fi
    if [ "$dirsonly" == false ]; then
	if [ "$rsyncprefix" == "false" ]; then
	    ls $d/*.gz 2>/dev/null >> $outputfile.tmp
	    ls $d/*.bz2 2>/dev/null >> $outputfile.tmp
	    ls $d/*.7z 2>/dev/null >> $outputfile.tmp
	    ls $d/*.html 2>/dev/null >> $outputfile.tmp
	    ls $d/*.txt 2>/dev/null >> $outputfile.tmp
	    ls $d/.xml 2>/dev/null >> $outputfile.tmp
	else
	    ls $d/*.gz 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	    ls $d/*.bz2 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	    ls $d/*.7z 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	    ls $d/*.html 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	    ls $d/*.txt 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	    ls $d/*.xml 2>/dev/null | sed -e "s|^$publicdir|$rsyncprefix|" >> $outputfile.tmp
	fi
    else
	list_dir_only
    fi
}

get_list_of_files() {
    projectdirs=`ls -d $publicdir/$p/20* 2>/dev/null`
    declare -a complete_dumps
    declare -a failed_dumps
    listdumpsforproject
    if [ ${#complete_dumps[@]} -ge $dumpsnumber ]; then
	dumps_to_copy=${complete_dumps[@]:0:$dumpsnumber}
	for d in $dumps_to_copy; do
	    list_files_in_dir
	done
    else
	for d in ${complete_dumps[@]}; do
	    list_files_in_dir
	done
	left_to_get=$(( $dumpsnumber - ${#complete_dumps[@]} ))
	if [ ${#failed_dumps[@]} -ge $left_to_get ]; then
	    dumps_to_copy=${failed_dumps[@]:0:$left_to_get}
	    for d in $dumps_to_copy; do
		list_files_in_dir
	    done
	else
	    for d in ${failed_dumps[@]}; do
		list_files_in_dir
	    done
	fi
    fi
}
    
if [ "$#" -lt "4" -o "$#" -gt "9" ]; then
    usage
fi

dumpsnumber=""
outputfile=""
configfile="wikidump.conf"
rsyncprefix="false"
dirsonly="false"

while [ $# -gt 0 ]; do
    if [ $1 == "--dirsonly" ]; then
	dirsonly="true"
	shift
    elif [ $1 == "--dumpsnumber" ]; then
	dumpsnumber="$2"
	shift; shift
    elif [ $1 == "--outputfile" ]; then
	outputfile="$2"
	shift; shift
    elif [ $1 == "--configfile" ]; then
	configfile="$2"
	shift; shift
    elif [ $1 == "--rsyncprefix" ]; then
	rsyncprefix="$2"
	shift; shift
    else
	echo "$0: Unknown option $1"
	usage
    fi
done

check_args

tempdir=`egrep "^temp=" "$configfile" | awk -Ftemp= '{ print $2 }'`
if [ -z "$tempdir" ]; then
    tempdir="/tmp"
fi

dblist="${tempdir}/all.dblist"

wget -P "$tempdir" -N -q 'http://noc.wikimedia.org/conf/all.dblist'

if [ ! -f "$dblist" ]; then
    echo "$0: failed to retrieve list of valid projects that are dumped, exiting."
    exit 1
fi

publicdir=`egrep "^public=" "$configfile" | awk -Fpublic= '{ print $2 }'`
if [ -z "$publicdir" ]; then
    publicdir="/dumps/public"
fi

projects=`cat $dblist`

for p in $projects; do
    get_list_of_files
done

# do this last so that if someone is using the file in the meantime, they  aren't 
# interrupted
if [ -f "$outputfile" ]; then
    mv "$outputfile" "$outputfile.old"
fi
if [ -f "$outputfile.tmp" ]; then
    mv "$outputfile.tmp" "$outputfile"
else
    echo "$0: no output file created. Something is wrong."
    exit 1
fi

/usr/bin/rsync --list-only --files-from="$outputfile" "$publicdir" dummy  > "$outputfile".rsync
