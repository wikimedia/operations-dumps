#!/bin/bash
if [ -z "$5" ]; then
  echo "This script runs the history dump for a given stub number, date and project."
  echo Usage: $0 date part prefetchdate prefetchpart projectname '[configfile]'
  echo Example: $0 20110223 1-a 20100904 1 enwiki
  echo If more than one prefetch file is needed, specify the numbers separated
  echo by commas, thus:
  echo Example: $0 20110223 8-a 20100904 7,8,9 enwiki
  exit 1
fi

date=$1
number=$2
prefetchdate=$3
prefetchnumber=$4
project=$5

if [ ! -z "$6" ]; then
    configfile="$6"
else
    configfile="wikidump.conf"
fi

if [ ! -f "$configfile" ]; then
    echo "Can't open configuration file $configfile, exiting..."
    exit 1
fi

phase3=`egrep "^dir=" "$configfile" | awk -Fdir= '{ print $2 }'`
publicdumps=`egrep "^public=" "$configfile" | awk -Fpublic= '{ print $2 }'`

# example: 
#
#/usr/bin/php -q /apache/common/php-1.5/maintenance/dumpTextPass.php --wiki=enwiki \
#  --stub=gzip:/mnt/data/xmldatadumps/public/enwiki/20110115/enwiki-20110115-stub-meta-history1-e.xml.gz \
#  --prefetch=bzip2:/mnt/data/xmldatadumps/public/enwiki/20100904/enwiki-20100904-pages-meta-history1.xml.bz2 \
#  --force-normal --report=1000 --server=10.0.6.22 --spawn=/usr/bin/php \
#  --output=bzip2:/mnt/data/xmldatadumps/public/enwiki/20110115/enwiki-20110115-pages-meta-history1-e.xml.bz2 --full

stubfile="$publicdumps/${project}/${date}/${project}-${date}-stub-meta-history${number}.xml.gz"
if [ ! -f "$stubfile" ]; then
  echo no such stub file: "$stubfile"
  errors=TRUE
fi

# deal with the possibility of multiple prefetch files
prefetches=`echo $prefetchnumber | sed -e 's/,/ /g;'`

prefetchfiles=""
for p in $prefetches; do
    pfile="${publicdumps}/${project}/${prefetchdate}/${project}-${prefetchdate}-pages-meta-history${p}.xml.bz2"
    if [ ! -f "$pfile" ]; then
	echo no such prefetch file: "$pfile"
	errors=TRUE
    fi
    if [ -z "$prefetchfiles" ]; then
	prefetchfiles="bzip2:$pfile"
    else
	prefetchfiles="$prefetchfiles;$pfile"
    fi
done

outfile="${publicdumps}/${project}/${date}/${project}-${date}-pages-meta-history${number}.xml.bz2"
if [ -f "$outfile" ]; then
  echo output file "$outfile" 
  echo already exists, please remove before running this script
  errors=TRUE
fi

if [ -z "$errors" ]; then
    echo about to run:
    echo    /usr/bin/php -q $phase3/maintenance/dumpTextPass.php --wiki="$project" \
	--stub="gzip:$stubfile" \
	--prefetch="$prefetchfiles" \
	--force-normal --report=1000 --spawn=/usr/bin/php \
	--output="bzip2:$outfile" --full
    read -p "Is this OK? (N/y) " yesorno
    case "$yesorno" in
	[Yy]* ) 
	    /usr/bin/php -q $phase3/maintenance/dumpTextPass.php --wiki="$project" \
		--stub="gzip:$stubfile" \
		--prefetch="$prefetchfiles" \
		--force-normal --report=1000 --spawn=/usr/bin/php \
		fi	--output="bzip2:$outfile" --full;
	    ;;
	[Nn]*|* )
	    echo "OK, exiting at user request";
	    exit 1;
    esac
else
    echo "exiting..."
    exit 1
fi


