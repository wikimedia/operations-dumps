#!/bin/bash

# list all media on commons not used on any project, 
# uploaded within a certain date range

# part 2; there's two parts so folks can run each part separately

# requires:

# directory /a/imageinfo with at least 3gb of space

# in the same directory as this script, the following:
#
# wikiqueries.py
# wikiqueries.conf
# listFileNames.py
#
# wikiqueries.conf

# The current host should have mysql access to the dbs of the various wiki projects.
set -x

basedir='/a/imageinfo'
cwd=`pwd`

usage() {
  echo "This script generates a list of media on Commons that are not in use on any Wikimedia project."
  echo "Optionally the list may be restricted to a date range for when the media were uploaded."
  echo
  echo "Usage: $0 [startdate [enddate]]"
  echo
  echo "startdate  -- The starting date of uploaded media to check; format is YYYYMMDD"
  echo "              If omitted, start from the earliest uploads in the database"
  echo "enddate    -- The ending date of uploaded media to check; format is YYYYMMDD"
  echo "              If omitted, check media files uploaded through 30 days before today"
  echo 
  echo "For example:"
  echo "$0 20111001 20111101"
  echo "will check all media uploaded from Oct 1 00:00 UTC up to but not including Nov 1 2001 00:00 UTC."
  echo 
  echo "$0"
  echo "will check all media uploaded up through but not including 30 days before today."
  exit 1
}

if [ $# -gt 2 ]; then
    echo "Too many arguments specified."
    usage
fi

if [ $# -ge 1  ]; then
    startdate="${1}"
    if ! [[ "$startdate" =~ ^20[0-9]{6} ]]; then
	echo "Bad format for specified date $startdate."
	usage
    fi
    startdate="${startdate}000000"
else
    startdate=""
fi
if [ $# -eq 2 ]; then
    enddate="$2"
    if ! [[ "$enddate" =~ ^20[0-9]{6} ]]; then
	echo "Bad format for specified date $enddate."
	usage
    fi
    enddate="${enddate}000000"
else
    # default: enddate is today - 30 days
    today=`date -u +"%Y-%m-%d %H:%M:%S +0000"`
    secs=`date +%s -d "$today"`
    incr=$(( 86400 * 30 ))
    secs=$(( $secs-$incr ))
    enddate=`date +%Y%m%d000000 -d @$secs`
fi
if [[ "$enddate" < "$startdate" ]]; then
    echo "Start date specified is later than end date."
    usage
fi

echo "Part 2: Checking images in date range... start date: $startdate, end date: $enddate"

cd $basedir

# the file of *commons* files supposedly used locally but stored remotely (there is no such thing) is probably useless but let's keep it anyways
# move any other commons related files out of the general pool too
mkdir commonsfiles
mv files/commonswiki* commonsfiles/

# list of all files that are used on local projects -- other than commons -- and stored remotely, i.e. on commons
zcat files/*-used-locally-stored-remotely.gz | sort -T $basedir/tmp | uniq | gzip >  commonsfiles/commonswiki-all-linked-on-other-projects.gz
# list of all files used *nowhere* but stored on commons
zcat commonsfiles/commonswiki-unused-locally-stored-locally.gz commonsfiles/commonswiki-all-linked-on-other-projects.gz commonsfiles/commonswiki-all-linked-on-other-projects.gz | sort -T $basedir/tmp | uniq -u | gzip > commonsfiles/commonswiki-unused-anywhere-stored-locally.gz

# generate the list of images on commons we want to check for use
if [ ! -z "$enddate" -a  ! -z "$startdate" ]; then
    query="select img_name from image where img_timestamp < $enddate and img_timestamp >= $startdate"
elif [ ! -z "$enddate" ]; then
    query="select img_name from image where img_timestamp < $enddate"
elif [ ! -z "$startdate" ]; then
    query="select img_name from image where img_timestamp img_timestamp >= $startdate"
else
    query="select img_name from image"
fi
python $cwd/wikiqueries.py --verbose --configfile $cwd/wikiqueries.conf --outdir files --filenameformat '{w}-titles-in-daterange.gz' --query "$query" commonswiki

mv files/commonswiki* commonsfiles/

# generate the almost-final list of images on commons not used anywhere
zcat commonsfiles/commonswiki-titles-in-daterange.gz commonsfiles/commonswiki-unused-anywhere-stored-locally.gz | sort -T $basedir/tmp | uniq -d | gzip > commonsfiles/commonswiki-unused-anywhere-in-daterange.gz

# toss any names with forward slashes in them just for convenience...
zcat commonsfiles/commonswiki-unused-anywhere-in-daterange.gz | grep -v '/' | gzip > commonsfiles/commonswiki-unused-anywhere-in-daterange-no-slashes.gz

# turn the dir names into full paths for removal
zcat commonsfiles/commonswiki-unused-anywhere-in-daterange-no-slashes.gz | python $cwd/listFileNames.py | gzip > commonsfiles/commonswiki-dirs-to-remove.gz

# move everything into a directory with today's date
today=`date -u +"%Y%m%d"`
mkdir "$today"
mv commonsfiles files "$today"

echo "You can now copy the file $today/commonsfiles/commonswiki-dirs-to-remove.gz to ms5:/root/ and from there run in screen the command"
echo "zcat commonswiki-dirs-to-remove.gz | python ./removeThumbDirs.py"

