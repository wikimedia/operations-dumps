#!/bin/bash

# from a bastion host, copy to the specified host(s),
# the specified directory of code, config files, etc for xml dumps
# the directory should have a name in the format mon-dd-yyyy
# where mon is the first three letters of the month name in lower case

nodefile="/usr/local/dsh/node_groups/snapshots"
deploymentbase="/backups/dumps/deploy"

usage() {
    echo "copy deployment dir for xml dump code to specified hosts"
    echo
    echo "Usage: $0 [--deploydir mon-dd-yyy] [--hosts host1,host2,host3...]"
    echo
    echo "--deploydir:   name of deployment directory; format is mon-dd-yyyy"
    echo "                where mon is the first three letters of the month name"
    echo "                in lower case; if no date is provided then today's date"
    echo "                will be used to generate the corresponding dir name"
    echo "--hosts:        comma-separated list of one or more hosts for production"
    echo "                update; if no list is provided, the file $nodefile"
    echo "                will be read instead for the list"
    echo 
    echo "Example use: $0 --deploydir mar-12-2012 --hosts snapshot1,snapshot4"
    exit 1
}

while [ $# -gt 0 ]; do
    if [ $1 == "--deploydir" ]; then
	deploydir="$2"
	shift; shift
    elif [ $1 == "--hosts" ]; then
	hostnames="$2"
	shift; shift
    else
	echo "$0: Unknown option $1"
	usage
    fi
done

if [ -z "$hostnames" ]; then
    echo "No hosts specified, using nodefile $nodefile"
    if [ ! -f "$nodefile" ]; then 
	echo "$nodefile does not exist or is not a regular tile, exiting."
	exit 1
    fi
    hostnames=`cat $nodefile`
else
    hostnames=`echo $hostnames | sed -e 's/,/ /g;'`
fi



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

echo "Checking target hosts..."

if [ ! -z "$hostnames" ]; then
    # check hosts to see which ones are not responsive
    downhosts=""
    uphosts=""
    for h in $hostnames; do
	ssh -q -o SetupTimeOut=20 root@$h exit
	if [ $? -ne 0 ]; then
	    #add this host to list of hosts that aren't responsive
	    downhosts="$downhosts $host"
	else
	    uphosts="$uphosts $host"
	fi
    done
    if [ -z "$uphosts" ]; then
	echo "No hosts responsive, exiting."
	exit 1
    fi
    if [ ! -z "$downhosts" ]; then
	echo "The following hosts are not responsive:"
	echo "$downhosts"
	echo "Continue anyways? "
	read yn
	if [ -z "$yn" ]; then
	    yn="N"
	fi
	case $yn in
            [Nn]* ) echo "Exiting at user request"; exit 1;;
            [Yy]* ) break;;
            * ) echo "Unknown response, treating as no and exiting."; exit 1;;
	esac
    fi
fi

# make sure the directory exists before we try to copy it to the remote hosts
if [ ! -d "$deploymentbase/$deploydir" ]; then
    echo "Directory $deploymentbase/$deploydir does not exist or it's not a directory, exiting"
    exit 1
fi

exitcode=0

echo "Copying..."

for h in $hostnames; do
    scp -Rp -q -o SetupTimeOut=20 deploy/$deploydir  root@$h:$deploymentbase/$deploydir
    if [ $? -ne 0 ]; then
	# serious whine
	echo "Failed to copy deployment dir deploy/$deploydir to root@$h:$deploymentbase/$deploydir"
	exitcode=1
    fi
done

# why don't we do this on the source host? because all kinds of people have access there, and we don't want them to 
# see passwords or whatever that might be in config files.
echo "Setting dir/file perms..."

for h in $hostnames; do
    ssh -q -o SetupTimeOut=20 root@$h "find $deploymentbase/$deploydir -type d -exec chmod 755 {} \; ; find $deploymentbase/$deploydir -type f -exec chmod 644 {} \;"
    if [ $? -ne 0 ]; then
	echo "Failed to change file / dir permissions on root@$h:$deploymentbase/$deploydir"
	exitcode=1
    fi
done

echo "Done!"

