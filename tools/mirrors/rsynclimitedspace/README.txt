What is this?

This tool is used to rsync current complete good WMF XML dumps from a
remote host onto locally-accessible space-restricted volumes.

Small dumps go on one volume, cleaning up old directories per project
after the current dump for the project is copied over.

Big dumps (specified in a file "bigwikis.txt", one per line) go on
one volume, cleaning up all old files of a given type (for example
all stub-meta-current gz files) after the current ones are copied.

Huge dumps (specified in a file "hugewikis.txt", one per line) go
on one volume, cleaning up old files of a given type (for example
all stub-meta-current-*-1gz files) before the current ones are
copied.

Installation:

Make sure wget is somewhere in your path.

This script also depends on bash, rsync, sed, mawk, grep, rm.

Put the script rsync-last-good.sh into some directory, create bigwikis.txt
and hugewikis.txt in the same directory if you need/want them; you can see
the sample files in this directory for those. Create a configuruation file
if you want it; it's optional.  See the file wmfrsync-config.txt.sample
in this directory for an example.

RUnning:

an example run might look like this, to rsync all big wikis with no cleanup
of old dirs, and with display of extra progress messages:

/bin/bash ./rsync-last-good.sh --type big --destdir /mnt/wmf/dumps/bigwikis --verbose

For an explanation of all the options, run

./rsync-last-good.sh  --help

This script might be used to sync XML dumps to an ec2 amazon instance.
