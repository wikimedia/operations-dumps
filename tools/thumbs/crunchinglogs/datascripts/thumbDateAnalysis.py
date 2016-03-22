# -*- coding: utf-8  -*-

import re
import sys
import getopt


def usage(message=None):
    if message is not None:
        sys.stderr.write(message + "\n")
    usage_message = """
Usage: thumbDateAnalysis.py [--sdate=date --edate=date --created [filename]

  --sdate:   start date for which to print stats, default: earliest date in file
  --edate:   end date for which to print stats, default: latest date in file
  --created: show only the number of files and sizes on the date the first thumb
             was created (presumably the date the image itself was first uploaded)

Date format for sdate and edate: yyyy-mm-dd

If no filename is specified, input is read from stdin.

Format of input file: (sample line)

2011-10-29  01:57:51   100311   Festiwal_Słowian_i_Wikingów_2009_121.jpg/640px-Festiwal_Słowian_i_Wikingów_2009_121.jpg
date in yyyy-mm-dd format, time in hh:mm::ss format, size in bytes, thumb directory/thumb filename
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    sdate = None
    edate = None
    created = False
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ['sdate=', 'edate=', 'created'])
    except Exception:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--sdate":
            sdate = val
        elif opt == "--edate":
            edate = val
        elif opt == "--created":
            created = True

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
    byte_counts = {}
    for line in f_handle:
        try:
            (f_date, ftime_unused, f_size, path) = line.rstrip().split()
        except Exception:
            print >> sys.stderr, "skipping badly formatted line: ", line.rstrip()
            continue
        (dir_name, fname_unused) = path.split('/', 2)
        if not last_dir_name:
            last_dir_name = dir_name
        if dir_name != last_dir_name:
            # should just print the number of files
            # for every date sorted by date order, plus the dir name of course
            if (sdate and (f_date >= sdate)) or not sdate:
                if (edate and (f_date <= edate)) or not edate:
                    # print the stats
                    date_strings = file_counts.keys()
                    date_strings.sort()
                    if created:
                        print_dates = [date_strings[0]]
                    else:
                        print_dates = date_strings
                    for date in print_dates:
                        print ("Date:", date, "FilesThisDate:", file_counts[date],
                               "ByteCountThisDate:", byte_counts[date], "Dir: ", last_dir_name)
            last_dir_name = dir_name
            # reinitialize stats
            file_counts = {}
            byte_counts = {}
        # add to the stats.
        if (sdate and (f_date >= sdate)) or not sdate:
            if (edate and (f_date <= edate)) or not edate:
                if f_date not in file_counts:
                    file_counts[f_date] = 0
                file_counts[f_date] = file_counts[f_date] + 1
                if f_date not in byte_counts:
                    byte_counts[f_date] = 0
                byte_counts[f_date] = byte_counts[f_date] + int(f_size)

    # print stats for final dir
    if (sdate and (f_date >= sdate)) or not sdate:
        if (edate and (f_date <= edate)) or not edate:
            date_strings = file_counts.keys()
            date_strings.sort()
            if created:
                print_dates = [date_strings[0]]
            else:
                print_dates = date_strings
            for date in print_dates:
                print("Date:", date, "FilesThisDate:", file_counts[date],
                      "ByteCountThisDate:", byte_counts[date], "Dir: ", dir_name)
    sys.exit(0)


if __name__ == "__main__":
    do_main()
