# -*- coding: utf-8  -*-

import re
import sys
import getopt


def usage(message=None):
    if message is not None:
        sys.stderr.write(message + "\n")
    usage_message = """
Usage: thumbPxSize [--sdate=date] [--edate=date] [filename]

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

    last_dir_name = None
    file_counts = {}
    sizes = {}
    for line in f_handle:
        try:
            (f_date, ftime_unused, fsize_unused, path) = line.rstrip().split()
        except:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        try:
            (dir_name, f_name) = path.split('/', 2)
        except:
            continue
        if not last_dir_name:
            last_dir_name = dir_name
        if dir_name != last_dir_name:
            if (sdate and (f_date >= sdate)) or not sdate:
                if (edate and (f_date <= edate)) or not edate:
                    # print the stats
                    date_strings = file_counts.keys()
                    date_strings.sort()
                    for date in date_strings:
                        print("Date:", date, "ThumbsForFileThisDate:",
                              file_counts[date], "PixelSizes:"),
                        for key in sizes[date].keys():
                            print "%s:%s" % (key, sizes[date][key]),
                        print "Dir:", last_dir_name
            last_dir_name = dir_name
            # reinitialize stats
            file_counts = {}
            sizes = {}
        # add to the stats.
        if (sdate and (f_date >= sdate)) or not sdate:
            if (edate and (f_date <= edate)) or not edate:
                try:
                    (pixel_size, junk_unused) = f_name.split('px-', 1)
                except:
                    continue
                if f_date not in sizes:
                    sizes[f_date] = {}
                if pixel_size not in sizes[f_date]:
                    sizes[f_date][pixel_size] = 0
                sizes[f_date][pixel_size] = sizes[f_date][pixel_size] + 1
                if f_date not in file_counts:
                    file_counts[f_date] = 0
                file_counts[f_date] = file_counts[f_date] + 1

    # print stats for final dir
    if (sdate and (f_date >= sdate)) or not sdate:
        if (edate and (f_date <= edate)) or not edate:
            date_strings = file_counts.keys()
            date_strings.sort()
            for date in date_strings:
                print "Date:", date, "ThumbsForFileThisDate:", file_counts[date], "PixelSizes:",
                for key in sizes[date].keys():
                    print "%s:%s" % (key, sizes[date][key]),
                print "Dir:", dir_name
    sys.exit(0)

if __name__ == "__main__":
    do_main()
