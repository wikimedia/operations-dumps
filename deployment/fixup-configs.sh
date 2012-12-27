#!/bin/bash

# from a bastion host, copy to the specified host(s),
# the specified directory of code, config files, etc for xml dumps
# the directory should have a name in the format mon-dd-yyyy
# where mon is the first three letters of the month name in lower case

basedir="/home/wikipedia/downloadserver/snapshothosts/dumps"

usage() {
    echo "update config files in deployment dir to reference dblists and other files in same dir"
    echo
    echo "Usage: $0 [--deploydir mon-dd-yyy]"
    echo
    echo "--deploydir:   name of deployment directory; format is mon-dd-yyyy"
    echo "                where mon is the first three letters of the month name"
    echo "                in lower case; if no date is provided then today's date"
    echo "                will be used to generate the corresponding dir name"
    echo
    echo "Example use: $0 --deploydir mar-12-2012"
    exit 1
}
username=`whoami`
if [ "$username" != "root" ]; then
   echo "This script must be run as root."
   exit 1
fi

while [ $# -gt 0 ]; do
    if [ $1 == "--deploydir" ]; then
    deploydir="$2"
    shift; shift
    else
    echo "$0: Unknown option $1"
    usage
    fi
done

if [ -z "$deploydir" ]; then
    echo "No dir specified, using today's date"
    # use today's date
    deploydir=`date -u +%m-%d-%Y | sed -e 's/^01/jan/; s/^02/feb/; s/^03/mar/; s/^04/apr/; s/^05/may/; s/^06/jun/; s/^07/jul/; s/^08/aug/; s/^09/sep/; s/^10/oct/; s/^11/nov/; s/^12/dec/'` 
else
    # check the user's date for sanity
    good=`echo "$deploydir" | /bin/egrep '^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-[0-9][0-9]-20[0-9][0-9]$'`
    if [ -z "$good" ]; then
    echo "Bad format for datestring; expecting mon-dd-yyyy, example: mar-12-2012"
    usage
    fi
fi

# make sure the directory exists before we fix up the config files using it
if [ ! -d "$basedir/deploy/$deploydir" ]; then
    echo "Directory $basedir/deploy/$deploydir does not exist or it's not a directory, exiting"
    exit 1
fi

echo "Updating config files..."

configs=`find "$basedir/deploy/$deploydir/confs/" -name \*conf\* -type f -print`
for f in $configs; do
    # any old directory name that's a date gets replaced by the current deploydir
    cat "$f" | sed -e "s:/\(jan\|feb\|mar\|apr\|may\|jun\|jul\|aug\|sep\|oct\|nov\|dec\)\-[0-9]\{2\}\-20[0-9]\{2\}/:/$deploydir/:g;" > "$f.new"
    cp "$f.new" "$f"; rm "$f.new" # keep permissions on old file intact
done

echo "Done."
