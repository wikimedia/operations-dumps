#!/bin/bash

# creates a list of files by month, each file has a list of
# date, number of thumbs created for each of a fixed number
# of sizes (sizes list: 320px, 640px, 1024px, 1280px)

python thumbPxSize.py 0-00-files.txt.nobad  > pixelsizes.txt

domonth() {
    outfile=${outfileprefix}-pixelsizes.txt
    rm "$outfile"
    for d in $dates; do
	ymdstring=${ymstring}-$d
	echo -n "$ymdstring   " >> "$outfile"
	for size in $sizes; do
	    printf "%d " "$size" >> "$outfile"
	    count=`grep " $ymdstring " pixelsizes.txt | grep " ${size}:" | wc -l`
	    printf "%3d   " "$count" >> "$outfile"
	done
	echo >> "$outfile"
    done
}

dates30="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30"
dates31="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"
datesdec="01 02 03 04 05 06 07"
sizes="320 640 1024 1280"

dates="$dates31" ; outfileprefix=aug ; ymstring="2011-08"
domonth

dates="$dates30" ; outfileprefix=sept ; ymstring="2011-09"
domonth

dates="$dates31" ;  outfileprefix=oct ; ymstring="2011-10"
domonth

dates="$dates30" ; outfileprefix=nov ; ymstring="2011-11"
domonth

dates="$datesdec" ; outfileprefix=dec ; ymstring="2011-12"
domonth

