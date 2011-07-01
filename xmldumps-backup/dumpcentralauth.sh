#!/bin/bash

if [ ! -z "$1" ]; then
	configfile="$1"
else
	configfile="/backups-atg/wikidump.conf"
fi
wikiadmin=`egrep "^user=" "$configfile" | awk -Fuser= '{ print $2 }'`
wikipass=`egrep "^password=" "$configfile" | awk -Fpassword= '{ print $2 }'`
private=`egrep "^private=" "$configfile" | awk -Fprivate= '{ print $2 }'`
mysqldump=`egrep "^mysqldump=" "$configfile" | awk -Fmysqldump= '{ print $2 }'`
gzip=`egrep "^gzip=" "$configfile" | awk -Fgzip= '{ print $2 }'`
if [ -z "$wikiadmin" -o -z "$wikipass" -o -z "$private" -o -z "$mysqldump" -o -z "$gzip" ]; then
	echo "failed to find value of one of the following from config file $configfile:"
	echo "wikiadmin, wikipass, private, mysqldump, gzip"
	echo "exiting..."
	exit 1
fi
dbcluster=`grep centralauth /apache/common/php/wmf-config/db.php | awk -F"'" ' { print $4 }'`
wiki=`grep $dbcluster /apache/common/php/wmf-config/db.php | grep wiki | head -1 | awk -F"'" ' { print $2 }'`
host=`echo 'echo wfGetLB()->getServerName(0);' | php /apache/common/php/maintenance/eval.php $wiki`
if [ -z "$dbcluster" -o -z "$wiki" -o -z "$host" ]; then
	echo "can't locate db server for centralauth, exiting."
	exit 1
fi
tables="global_group_permissions global_group_restrictions global_user_groups globalblocks globalnames globaluser globaluser_medium globaluser_old localnames localuser localuser_medium localuser_old migrateuser_medium wikiset"
today=`date +%Y%m%d`
dir="$private/centralauth/$today"
mkdir -p "$dir"
for t in $tables; do
	outputfile="$dir/centralauth-$today-$t.gz"
	echo "dumping $t into $outputfile"
	"$mysqldump" -u "$wikiadmin" -p"$wikipass" -h "$host" --opt --quick --skip-add-locks --skip-lock-tables centralauth "$t" | "$gzip" > "$outputfile"
done



