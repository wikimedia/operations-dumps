# -*- coding: utf-8  -*-

import re
import sys
import getopt


def usage(message=None):
    if message is not None:
        sys.stderr.write(message + "\n")
    usage_message = """
Usage: thumbFilesSizesCount [--sdate=date --edate=date [filename]

  --sdate: start date for which to print stats, default: earliest date in file
  --edate: end date for which to print stats, default: latest date in file

Date format: yyyy-mm-dd

If filename is not specified, reads from stdin

Format of input file: (sample line)

2011-10-29  01:57:51   100311   \
    Festiwal_Słowian_i_Wikingów_2009_121.jpg/640px-Festiwal_Słowian_i_Wikingów_2009_121.jpg
date in yyyy-mm-dd format, time in hh:mm::ss format, size in bytes, \
    thumb directory/thumb filename
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    sdate = None
    edate = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 ['sdate=', 'edate='])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--sdate":
            sdate = val
        elif opt == "--edate":
            edate = val

    dateexp = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for date in filter(None, [sdate, edate]):
        if not dateexp.match(date):
            usage("Bad date format.")

    if len(remainder) == 1:
        input_file = remainder[0]
        f_handle = open(input_file, "r")
    elif len(remainder) == 0:
        f_handle = sys.stdin
    else:
        usage("Too many arguments.")

    sizes = {}
    counts = {}
    for line in f_handle:
        try:
            (f_date, ftime_unused, f_size, fname_unused) = line.rstrip().split()
        except:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        if (sdate and (f_date >= sdate)) or not sdate:
            if (edate and (f_date <= edate)) or not edate:
                if f_date not in sizes:
                    sizes[f_date] = 0
                    counts[f_date] = 0
                sizes[f_date] = sizes[f_date] + int(f_size)
                counts[f_date] = counts[f_date] + 1

    dates = sizes.keys()
    dates.sort()
    for date in dates:
        print "Date:", date, "Bytes:", sizes[date], "Files:", counts[date]
    sys.exit(0)


if __name__ == "__main__":
    do_main()
