#!/bin/bash

# creates output files each of which has a list by date of 
# the number of thumb dirs with 1 thumb file created on that date, 
# with 2 thumb files created on that date, with 3, etc. 

python thumbDateAnalysis.py 0-00-files.txt.nobad  > dateanalysis.txt

domonth() {
    outfile=${outfileprefix}-dateanalysis.txt
    rm "$outfile"
    for d in $dates; do
	ymdstring=${ymstring}-$d
	echo -n "$ymdstring " >> "$outfile"
	grep " $ymdstring " dateanalysis.txt | awk '{ print $4 }' | sort | uniq -c | sort -n -k2,2 | sed -e ':a;N;$!ba;s/ \+/ /g; s/\n/,/g' >> "$outfile"
    done
}

dates30="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30"
dates31="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"
datesdec="01 02 03 04 05 06 07"

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

