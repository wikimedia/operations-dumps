#!/bin/bash

# functions used by "other" dump periodic jobs.

source /usr/local/etc/set_dump_dirs.sh

checkval() {
    setting=$1
    value=$2
    if [ -z "$value" -o "$value" == "null" ]; then
        echo "failed to retrieve value of $setting from $configfile" >& 2
        exit 1
    fi
}

getsetting() {
    results=$1
    section=$2
    setting=$3
    echo "$results" | /usr/bin/jq -M -r ".$section.$setting"
}
