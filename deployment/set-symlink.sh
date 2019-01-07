#!/bin/bash

# on the specified host(s),
# remove symlink for current xml dump code production dir and
# point it to some earlier/later/other one as specified by the user
# or to the one for "today" if none is specified

nodefile="/usr/local/dsh/node_groups/snapshot"
productionlink="/backups/dumps/production"
deploymentdir="/backups/dumps/deploy"

usage() {
    echo "Point production dir for xml dump code to a specific deployment dir"
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
    echo "This script will check to make sure the named deployment dir actually"
    echo "exists on all reachable hosts before doing the update"
    echo
    echo "Example use: $0 --deploydir mar-12-2012 --hosts snapshot1,snapshot4"
    exit 1
}

while [ $# -gt 0 ]; do
    if [ $1 == "--deploydir" ]; then
	deploydir="$2"
	shift; shift
    elif [ $1 == "--hosts" ]; then
	hosts="$2"
	shift; shift
    else
	echo "$0: Unknown option $1"
	usage
    fi
done

if [ -z "$hosts" ]; then
    echo "No hosts specified, using nodefile $nodefile"
    if [ ! -f "$nodefile" ]; then 
	echo "$nodefile does not exist or is not a regular tile, exiting."
	exit 1
    fi
    hosts=`cat $nodefile`
else
    hosts=`echo $hosts | sed -e 's/,/ /g;'`
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


dshargs=("--concurrent-shell" "--show-machine-names" "--forklimit" "5" "--remoteshellopt" "-oConnectTimeout=30" "--" "rm $productionlink; ln -s -s deploy/$deploydir/ $productionlink")

if [ ! -z "$hosts" ]; then
    # check hosts to see which ones are not responsive
    downhosts=""
    uphosts=""
    for h in $hosts; do
	ssh -q -oConnectTimeout=20 root@$h exit
	if [ $? -ne 0 ]; then
	    #add this host to list of hosts that aren't responsive
	    downhosts="$downhosts $h"
	else
	    uphosts="$uphosts $h"
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

    # check hosts to see which ones have the specified deployment dir ready
    goodhosts=""
    for h in $uphosts; do
	result=`ssh -oConnectTimeout=30 "root@$h" "ls -lL $deploymentdir/$deploydir"`
	if [ -z "$result" ]; then
	    badhosts="$badhosts $h"
	else
	    goodhosts="$goodhosts $h"
	fi
    done
    if [ -z "$goodhosts" ]; then
	echo "No hosts good for deployment, exiting."
	exit 1
    fi
    if [ ! -z "$badhosts" ]; then
	echo "The following hosts either do not have the specified deploy directory or have"
	echo -n "some related problem.  Continue anyways (N/y)? "
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

# actually make specified directory the production dir on the hosts
echo "Doing deployment"

for h in $goodhosts; do
    ssh -q -o ConnectTimeout=20 root@$h "rm $productionlink; ln -s -s deploy/$deploydir/ $productionlink"
    if [ $? -ne 0 ]; then
	# serious whine
	echo "Failed to update production symlink root@$h:deploy/$deploydir/"
	exitcode=1
    fi
done

