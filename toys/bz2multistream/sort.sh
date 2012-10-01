#!/bin/bash

# this is a tiny little helper script to sort the index file
# for use with the reader script.

usage() {
    echo "Sort index file on third field with ':' separator, by C locale ordering"
    echo "Usage: $0 --input filename --output filename"
    echo 
    echo "  input      name of bz2 compressed index file for your wiki project,"
    echo "             downloaded from dumps.wikimedia.org"
    echo "  output     name of sorted index file; this will be a plain text file."
    echo
    echo "For example:"
    echo "   $0 --input elwiki-pages-multistream-index.bz2 --output elwiki-pages-multistream-index-sorted.txt"
    exit 1
}

inputfile=""
outputfile=""

while [ $# -gt 0 ]; do
    if [ "$1" == "--input" ]; then
	inputfile="$2"
	shift; shift
    elif [ "$1" == "--output" ]; then
	outputfile="$2"
	shift; shift
    else
	echo "$0: Unknown option $1"
	usage
    fi
done

if [ -z "$inputfile" ]; then
    echo "No value was given for 'input'."
    usage
fi
if [ -z "$outputfile" ]; then
    echo "No value was given for 'output'."
    usage
fi

LC_ALL_save=`echo $LC_ALL`; LC_ALL=C; export LC_ALL; \
bzcat "$inputfile" | \
sort -k 3 -t ':' > "$outputfile"; \
LC_ALL=${LC_ALL_save}; export LC_ALL
