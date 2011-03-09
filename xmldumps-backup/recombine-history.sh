#!/bin/bash

if [ -z "$4" ]; then
  echo "This script recombines one piece of a history dump for a given piece number, date and project."
  echo Usage: $0 date part subpieces projectname '[configfile]'
  echo Example: $0 20110223 1 pre,a,b,c,d,e,f,g enwiki
  echo
  echo "Expects names to have the form blah-pages-meta-history-n.xml.bz2"
  exit 1
fi

date=$1
number=$2
subpiecenums=$3
project=$4

if [ ! -z "$5" ]; then
    configfile="$5"
else
    configfile="wikidump.conf"
fi

if [ ! -f "$configfile" ]; then
    echo "Can't open configuration file $configfile, exiting..."
    exit 1
fi

subpieces=`echo $subpiecenums | sed -e 's/,/ /g;'`

phase3=`egrep "^dir=" "$configfile" | awk -Fdir= '{ print $2 }'`
publicdumps=`egrep "^public=" "$configfile" | awk -Fpublic= '{ print $2 }'`
head=`egrep "^head=" "$configfile" | awk -Fhead= '{ print $2 }'`
if [ -z "$head" ]; then
    head="/usr/bin/head"
fi
tail=`egrep "^tail=" "$configfile" | awk -Ftail= '{ print $2 }'`
if [ -z "$tail" ]; then
    tail="/usr/bin/tail"
fi
bzip=`egrep "^bzip2=" "$configfile" | awk -Fbzip2= '{ print $2 }'`
basefilename="${publicdumps}/${project}/${date}/${project}-${date}-pages-meta-history${number}"
outfile="${basefilename}.xml.bz2"
if [ -f "$outfile" ]; then
  echo output file "$outfile" 
  echo already exists, please remove before running this script
  errors=TRUE
fi

for i in $subpieces; do
    histfilepiece="${basefilename}-${i}.xml.bz2"
    if [ ! -f "$histfilepiece" ]; then
	echo no history file found: "$histfilepiece"
	errors=TRUE
    fi
done

# string the command together now
command=""
last=`echo $subpieces | awk '{ print $NF }'`
first=`echo $subpieces | awk '{ print $1 }'`
for i in $subpieces; do
    histfilepiece="${basefilename}-${i}.xml.bz2"
    if [ "$i" ==  "$first" ]; then
	# first part of command, put header but not footer
	command="$bzip -dc $histfilepiece | $head -n -1;"
    elif [ "$i" == "$last" ]; then
	# last part of command, put footer but not header
	headerEndNum=`$bzip -dc $histfilepiece | $head -n 2000 | grep -n '</siteinfo>' | awk -F: '{ print $1 }'`
	headerEndNum=$(( $headerEndNum+1 ))
	headerEndNum="+$headerEndNum"
	command="$command $bzip -dc $histfilepiece | $tail -n  $headerEndNum"
    else
	# the rest, we strip both header and footer from all the intermediate files
	headerEndNum=`$bzip -dc $histfilepiece | $head -n 2000 | grep -n '</siteinfo>' | awk -F: '{ print $1 }'`
	headerEndNum=$(( $headerEndNum+1 ))
	headerEndNum="+$headerEndNum"
	command="$command $bzip -dc $histfilepiece | $tail -n  $headerEndNum | $head -n -1;"
    fi
done

if [ -z "$errors" ]; then
    echo about to run:
    echo "( $command ) | $bzip > $outfile"
    read -p "Is this OK? (N/y) " yesorno
    case "$yesorno" in
[Yy]* ) 
    eval "( $command ) | $bzip > $outfile"
    ;;
[Nn]*|* )
    echo "OK, exiting at user request";
    exit 1;
    esac
else
    echo "exiting..."
    exit 1
fi
