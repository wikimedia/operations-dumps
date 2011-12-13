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

    sizes = {}
    counts = {}
    totalSize = 0
    totalNum = 0
    for line in fHandle:
        try:
            ( fDate, fTime, fSize, fName ) = line.rstrip().split()
        except:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        if (sdate and (fDate >= sdate)) or not sdate:
            if (edate and (fDate <= edate)) or not edate:
                if not fDate in sizes:
                    sizes[fDate] = 0
                    counts[fDate] = 0
                sizes[fDate] = sizes[fDate] + int(fSize)
                counts[fDate] = counts[fDate] + 1

    dates = sizes.keys()
    dates.sort()
    for d in dates:
        print "Date:", d, "Bytes:", sizes[d], "Files:", counts[d]
    sys.exit(0)

