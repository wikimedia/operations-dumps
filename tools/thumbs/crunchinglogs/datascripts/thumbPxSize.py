# -*- coding: utf-8  -*-

import os
import re
import sys
import time
import getopt


def usage(message=None):
    print "Usage: %s [--sdate=date --edate=date [filename]" % sys.argv[0]
    print "sdate: start date for which to print stats, default: earliest date in file "
    print "edate: end date for which to print stats, default: latest date in file"
    print "Date format: yyyy-mm-dd"
    print "If filename is not specified, reads from stdin"
    print ""
    print "Format of input file: (sample line)"
    print "2011-10-29  01:57:51   100311   Festiwal_Słowian_i_Wikingów_2009_121.jpg/640px-Festiwal_Słowian_i_Wikingów_2009_121.jpg"
    print "date in yyyy-mm-dd format, time in hh:mm::ss format, size in bytes, thumb directory/thumb filename"
    sys.exit(1)

if __name__ == "__main__":
    sdate = None
    edate = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 [ 'sdate=', 'edate=', ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--sdate":
            sdate = val
        elif opt == "--edate":
            edate = val

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
    fileCounts = {}
    sizes = {}
    for line in fHandle:
        try:
            ( fDate, fTime, fSize, path ) = line.rstrip().split()
        except:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        try:
            ( dirName, fName ) = path.split('/',2)
        except:
            continue
        if not lastDirName:
            lastDirName = dirName
        if dirName != lastDirName:
            if (sdate and (fDate >= sdate)) or not sdate:
                if (edate and (fDate <= edate)) or not edate:
                    # print the stats
                    dateStrings = fileCounts.keys()
                    dateStrings.sort()
                    for d in dateStrings:
                        print "Date:", d, "ThumbsForFileThisDate:", fileCounts[d], "PixelSizes:",
                        for k in sizes[d].keys():
                            print "%s:%s" % (k, sizes[d][k]),
                        print "Dir:", lastDirName
            lastDirName = dirName
            # reinitialize stats
            fileCounts = {}
            sizes = {}
        # add to the stats.
        if (sdate and (fDate >= sdate)) or not sdate:
            if (edate and (fDate <= edate)) or not edate:
                try:
                    ( pixelSize, junk ) = fName.split('px-',1)
                except:
                    continue
                if not fDate in sizes:
                    sizes[fDate] = {}
                if not pixelSize in sizes[fDate]:
                    sizes[fDate][pixelSize] = 0
                sizes[fDate][pixelSize] = sizes[fDate][pixelSize] + 1
                if not fDate in fileCounts:
                    fileCounts[fDate] = 0
                fileCounts[fDate] = fileCounts[fDate] + 1
    # print stats for final dir
    if (sdate and (fDate >= sdate)) or not sdate:
        if (edate and (fDate <= edate)) or not edate:
            dateStrings = fileCounts.keys()
            dateStrings.sort()
            for d in dateStrings:
                print "Date:", d, "ThumbsForFileThisDate:", fileCounts[d], "PixelSizes:",
                for k in sizes[d].keys():
                    print "%s:%s" % (k, sizes[d][k]),
                print "Dir:", dirName
    sys.exit(0)

