#!/bin/bash

# list all media on commons not used on any project, 
# uploaded within a certain date range

# part 1; there's two parts so folks can run each part separately

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
basedir='/a/imageinfo'
cwd=`pwd`

echo "Part 1: Generating image lists for all projects"

cd $basedir

today=`date -u +"%Y%m%d"`

# don't clobber files form older runs
if [ -e "files/" -o -e "commonsfiles/" -o -e "$today" ]; then
    if [ -d old/ ]; then
	echo "you have a directory of old files around as well as some files from"
	echo "a recent run.  Please clean up the following: either the directory old/"
	echo "or any of the directories files commonsfiles"
	echo "that currently exist."
	exit 1
    fi
    mkdir old
    mv "$titlesdir" "$imagelinksdir" files commonsfiles "$today"  old/
fi

mkdir -p tmp

# list of all images in use on a project
python $cwd/wikiqueries.py --verbose --configfile $cwd/wikiqueries.conf --query 'select il_to from imagelinks' --outdir files --filenameformat '{w}-{d}-imagelinks.gz' 

# all images locally stored on a project
python $cwd/wikiqueries.py --verbose --configfile $cwd/wikiqueries.conf --query 'select img_name from image' --outdir files --filenameformat '{w}-{d}-imagenames.gz'

# format simplewikidb-20111205-wikiquery.gz
dblist=`(cd files; ls *imagelinks* | sed -e 's/-20[0-9]\{6\}-imagelinks\.gz//g;')`
for db in $dblist; do
  echo -n "Doing db $db..."
  linksfilename="${db}-${today}-imagelinks.gz"
  namesfilename="${db}-${today}-imagenames.gz"

  # all images locally stored but *not* used on a project
  zcat files/$namesfilename files/$linksfilename files/$linksfilename | sort -T $basedir/tmp | uniq -u | gzip > "files/${db}-unused-locally-stored-locally.gz"
  # all images used on a project and remotely stored (or they just don't exist; links to nonexistent images still go into the links table)
  zcat files/$linksfilename files/$namesfilename files/$namesfilename | sort -T $basedir/tmp | uniq -u | gzip >  "files/${db}-used-locally-stored-remotely.gz"
  echo "done."
done

echo "end part 1"
