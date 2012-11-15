#!/bin/bash

runcommand() {
    if [ ! -z "$ECHO" ]; then
	"$ECHO" "${command[@]}"
    else
	"${command[@]}"
    fi
}

showmessage() {
  if [ "$VERBOSE" == "true" ]; then
      echo "$message"
  fi
}

cleanupfiletype() {
    canskip=""  # did we already do cleanup on this filetype? if so skip
    for ((i=0; i < ${#cleanedupthese[$@]}; i++)) {
	if [ ${cleanedupthese[$i]} == "$filetype" ]; then
	    canskip="true"
	    break;
	fi
    }
    if [ -z "$canskip" ]; then
	for d in $olddates; do
	    message="cleaning up file ${DESTDIR}/${wikiname}/${d}/${filetype}" && showmessage
	    command=( "rm" "-f" "${DESTDIR}/${wikiname}/${d}/${filetype}" )
	    runcommand
	done
    fi
    cleanedup[${#cleanedupthese[$@]}]="$filetype"
}

usage() {
    echo "rsync wikis one dir at a time from wmf public dump mirror host"
    echo "this script rsyncs all wikis not in a list of big or huge wikis"
    echo "if those lists don't exist it will retrieve them all"
    echo
    echo "Usage: $0 [--basedir dirpath] [--wikilist filename]"
    echo
    echo "options:"
    echo ""
    echo "--basedir:     path of dirctory where big and huge wiki lists are stored"
    echo "               and where list of all wikis to rsync is stashed"
    echo "               default: /home/ubuntu/rsyncscripts"
    echo "--configfile:  path to config file with variables set in it, if desired"
    echo "               default: $HOME/wmfrsync-config.txt"
    echo "--destdir:     path of dirctory where wiki dirs are rsynced; this directory"
    echo "               must exist already, it will not be created for you"
    echo "               default: /mnt/wmf/dumps/smallwikis"
    echo "--remote:      remote url base (remotehost::modulename) from which to rsync"
    echo "               default: dataset1001.wikimedia.org::dumpmirrorseverything"
    echo "--type:        small big or huge (whichwikis to rsync)"
    echo "               default: small"
    echo "--wikilist:    list of (small) wikis for rsync, in format provided by rsync --list-only"
    echo "               (wikiname/datedir as last of five space-separated fields in line)"
    echo "               default: the list will be downloaded via wget from"
    echo "               http://download.wikimedia.org/rsync-dirlist-last-1-good.txt.rsync"
    echo ""
    echo "flags:"
    echo ""
    echo "--cleanup:     delete contents of old directories for each wiki synced"
    echo "               depending on the wiki sync type (see --type), this is handled"
    echo "               differently.  For 'small' wikis, old directories are removed"
    echo "               after the new directory has successfully rsynced."
    echo "               For 'big' and 'huge' wikis, files in the old directories are removed"
    echo "               after the corresponding files are rsynced over, one at a time,"
    echo "               to the current directory."
    echo "--dryrun:      don't do the rsyncs and deletes, echo the commands that would be run"
    echo "--verbose:     print lots of extra messages about what's being done, do verbose rsyncs"
    echo ""
    echo "Example use: $0 --basedir /home/wikirsync --wikilist listfortesting.txt --type big --remote dataset1001.wikimedia.org::dumpmirrorseverything --cleanup"
    exit 1
}

BASEDIR="/home/ubuntu/rsyncscripts"
DESTDIR="/mnt/wmf/dumps/smallwikis"
TYPE="small"
CLEANUP="false"
CONFIGFILE="$HOME/wmfrsync-config.txt"
DRYRUN="false"
REMOTE="dataset1001.wikimedia.org::dumpmirrorseverything"
VERBOSE="false"
rsyncfile=""
declare -a skipthese
declare -a cleanedupthese

# first look for configfile and read those values in
if [ $# -gt 0 ]; then
    args=("$@")
    for ((i=0; i < $#; i++)) {
	if [ ${args[$i]} == "--configfile" ]; then
	    CONFIGFILE="${args[$((i+1))]}"
	    break
	fi
    }
fi

if [ -e "$CONFIGFILE" ]; then
    source "$CONFIGFILE"
fi

while [ $# -gt 0 ]; do
    if [ "$1" == "--basedir" ]; then
	BASEDIR="$2"
	shift; shift
    elif [ "$1" == "--cleanup" ]; then
	CLEANUP="true"
	shift
    elif [ "$1" == "--configfile" ]; then
	# already processed above
	shift; shift
    elif [ "$1" == "--dryrun" ]; then
	DRYRUN="true"
	shift
    elif [ "$1" == "--destdir" ]; then
	DESTDIR="$2"
	shift; shift
    elif [ "$1" == "--remote" ]; then
	REMOTE="$2"
	shift; shift
    elif [ "$1" == "--type" ]; then
	TYPE="$2"
	shift; shift
    elif [ "$1" == "--verbose" ]; then
	VERBOSE="true"
	shift
    elif [ "$1" == "--wikilist" ]; then
	rsyncfile="$2"
	shift; shift
    else
	echo "$0: Unknown option $1"
	usage
    fi
done

if [ ! -d "$BASEDIR" ]; then
    echo "No such directory $BASEDIR, exiting."
    exit 1
fi

if [ ! -d "$DESTDIR" ]; then
    echo "No such directory $DESTDIR, exiting."
    exit 1
fi

cd $BASEDIR
if [ "$?" -ne 0 ]; then
    echo "Failed to cd to $BASEDIR, exiting."
    exit 1
fi

if [ "$DRYRUN" == "true" ]; then
    ECHO="echo"
else
    ECHO=""
fi

if [ -z "$rsyncfile" ]; then
    rsyncfile="rsync-dirlist-last-1-good.txt.rsync"
    wget -q -N "http://download.wikimedia.org/$rsyncfile"
    if [ $? -ne 0 ]; then
	echo "Failed to retrieve wiki directory listing for rsync, exiting."
	exit 1
    fi
fi

if [ "$TYPE" == "small" ]; then
  message="doing a small wiki" && showmessage

  index=0
  bigwikisfile="$BASEDIR/bigwikis.txt"
  if [ -e "$bigwikisfile" ]; then
      while read line
      do
	  skipthese[$index]="$line/"
	  let "index = $index + 1"
      done < "$bigwikisfile"
  fi

  hugewikisfile="$BASEDIR/hugewikis.txt"
  if [ -e "$hugewikisfile" ]; then
      while read line
      do
	  skipthese[$index]="$line/"
	  let "index = $index + 1"
      done < "$hugewikisfile"
  fi

  while read line
  do
      dirname=`echo $line | mawk '{ print $5 }'`
      if [[ ! "$dirname" =~ "/" ]]; then
	  continue
      fi
      wikiname=`echo $dirname | mawk -F'/' '{ print $1 }'`
      if [[ ${skipthese[*]} =~ "^$wikiname/" ]]; then
	  continue
      fi
      message="dirname $dirname" && showmessage
      command=( "/usr/bin/rsync" "-avR" "--bwlimit=50000" "${REMOTE}/${dirname}" "${DESTDIR}" )
      runcommand
      if [ "$CLEANUP" == 'true' ]; then
	  # find old dates, remove those dirs
	  currentdate=`echo $dirname | mawk -F'/' '{ print $2 }'`
	  olddates=`( cd ${DESTDIR}/${wikiname}; ls -d 20* | grep -v $currentdate )`
	  if [ ! -z "$olddates" ]; then
	      message="cleaning up olddates $olddates" && showmessage
	  else
	      message="no old dates to clean up" && showmessage
	  fi
	  for d in $olddates; do
	      command=( "rm -rf" "${DESTDIR}/${wikiname}/${d}" )
	      runcommand
	  done
      else
	  message="no cleanup requested" && showmessage
      fi
  done < "$BASEDIR/$rsyncfile"

elif [ "$TYPE" == "big" -o "$TYPE" == "huge" ]; then

  message="doing $TYPE wikis" && showmessage

  wikisfile="$BASEDIR/${TYPE}wikis.txt"
  if [ ! -e "$wikisfile" ]; then
      echo "No list of $TYPE wikis ${TYPE}wikisfile found, exiting."
      exit 1
  fi

  while read line
  do
      rsyncline=`grep " $line/" "$BASEDIR/$rsyncfile"`
      dirname=`echo $rsyncline | mawk '{ print $5 }'`
      if [[ ! "$dirname" =~ "/" ]]; then
	  continue
      fi
      wikiname=`echo $dirname | mawk -F'/' '{ print $1 }'`
      currentdate=`echo $dirname | mawk -F'/' '{ print $2 }'`

      message="wikiname $wikiname and date $currentdate" && showmessage
      message="retrieving list of files for rsync" && showmessage
      list=`/usr/bin/rsync -a --bwlimit=50000 --list-only ${REMOTE}/${dirname} ${DESTDIR} | mawk '{ print $5 }'`

      if [ "$CLEANUP" == 'true' ]; then
	  # find old dates so we can remove files from those dirs
	  olddates=`(cd ${DESTDIR}/${wikiname}; ls -d 20* | grep -v $currentdate)`
	  if [ ! -z "$olddates" ]; then
	      message="cleaning up olddates $olddates" && showmessage
	  else
	      message="no old dates to clean up" && showmessage
	  fi
      else
	  olddates=""
	  message="no cleanup requested" && showmessage
      fi

      for f in $list; do
	  filename=`echo "$f" | mawk -F'/' '{ print $2 }'`
	  if [ -z "$filename" ]; then
	      continue
	  fi

	  # file type consists of: filename, extension, but the date is irrelevant and we also 
	  # don't care about the checkpoint piece of the filename, which for us is a string
	  # like -p000000010p000002017
	  filetype=`echo "$filename" | sed -e 's/[-]p[0-9]\{9\}p[0-9]\{9\}/*/g; s/20[0-9]\{6\}/*/g;'`

	  if [ "$TYPE" == "huge" ]; then
	      # do cleanup before rsync so we don't run out of room
	      cleanupfiletype
	  fi

	  message="doing rsync of file type $filetype" && showmessage
	  # hope this 'protect args' thing works
	  command=( "/usr/bin/rsync" "-avRs" --bwlimit=50000 "${REMOTE}/${dirname}/${filetype}" "${DESTDIR}" )
	  runcommand

	  if [ "$TYPE" == "big" ]; then
	      # do cleanup after rsync but still per file type
	      cleanupfiletype
	  fi
      done

      # now remove the directories and any remaining cruft for olddates
      for d in $olddates; do
	  message="removing dir ${DESTDIR}/${wikiname}/${d} and remaining contents" && showmessage
	  command=( "rm" "-rf" "${DESTDIR}/${wikiname}/${d}" )
	  runcommand
      done

  done < "$wikisfile"

else
    echo "Unknown type for wiki rsync, must be one of 'small', 'big', 'huge'. Exiting."
fi
