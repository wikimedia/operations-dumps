# -*- coding: utf-8  -*-

import os
import re
import sys
import time
import getopt

def usage(message=None):
    print "Usage: %s [--sdate=date --edate=date --created [filename]" % sys.argv[0]
    print "sdate: start date for which to print stats, default: earliest date in file "
    print "edate: end date for which to print stats, default: latest date in file"
    print "created: show only the number of files and sizes on the date the first thumb"
    print "was created (presumably the date the image itself was first uploaded)"
    print ""
    print "Date format for sdate and edate: yyyy-mm-dd"
    print ""
    print "If no filename is specified, input is read from stdin"
    print
    print "Format of input file: (sample line)"
    print "2011-10-29  01:57:51   100311   Festiwal_Słowian_i_Wikingów_2009_121.jpg/640px-Festiwal_Słowian_i_Wikingów_2009_121.jpg"
    print "date in yyyy-mm-dd format, time in hh:mm::ss format, size in bytes, thumb directory/thumb filename"
    sys.exit(1)

if __name__ == "__main__":
    sdate = None
    edate = None
    created = False
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 [ 'sdate=', 'edate=', 'created' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--sdate":
            sdate = val
        elif opt == "--edate":
            edate = val
        elif opt == "--created":
            created = True

    dateexp = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for d in filter(None, [ sdate, edate ]):
        if not dateexp.match(d):
            usage("Bad date format.")

    if len(remainder) == 1:
        inputFile = remainder[0]
        fHandle = open(inputFile,"r")
    elif len(remainder) == 0:
        fHandle = sys.stdin
    else:
        usage("Too many arguments.")

    lastDirName = None
    numFilesSameDate = 0
    byteCountSameDate = 0
    fileCounts = {}
    byteCounts = {}
    for line in fHandle:
        try:
            ( fDate, fTime, fSize, path ) = line.rstrip().split()
        except:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        ( dirName, fName ) = path.split('/',2)
        if not lastDirName:
            lastDirName = dirName
        if dirName != lastDirName:
            # should just print the number of files for every date sorted by date order, plus the dir name of course"
            if (sdate and (fDate >= sdate)) or not sdate:
                if (edate and (fDate <= edate)) or not edate:
                    # print the stats
                    dateStrings = fileCounts.keys()
                    dateStrings.sort()
                    if created:
                        printDates = [ dateStrings[0] ]
                    else:
                        printDates = dateStrings
                    for d in printDates:
                        print "Date:", d, "FilesThisDate:", fileCounts[d], "ByteCountThisDate:", byteCounts[d], "Dir: ", lastDirName
            lastDirName = dirName
            # reinitialize stats
            numFilesSameDate = 0
            byteCountSameDate = 0
            fileCounts = {}
            byteCounts = {}
        # add to the stats.
        if (sdate and (fDate >= sdate)) or not sdate:
            if (edate and (fDate <= edate)) or not edate:
                if fDate not in fileCounts:
                    fileCounts[fDate] = 0
                fileCounts[fDate] = fileCounts[fDate] + 1
                if fDate not in byteCounts:
                    byteCounts[fDate] = 0
                byteCounts[fDate] = byteCounts[fDate] + int(fSize)

    # print stats for final dir
    if (sdate and (fDate >= sdate)) or not sdate:
        if (edate and (fDate <= edate)) or not edate:
            dateStrings = fileCounts.keys()
            dateStrings.sort()
            if created:
                printDates = [ dateStrings[0] ]
            else:
                printDates = dateStrings
            for d in printDates:
                print "Date:", d, "FilesThisDate:", fileCounts[d], "ByteCountThisDate:", byteCounts[d], "Dir: ", dirName
    sys.exit(0)

