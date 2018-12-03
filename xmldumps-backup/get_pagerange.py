#!/usr/bin/python3
"""
quickie standalone test of page range job generation, given
an arbitrary start and end point
"""
import sys
import getopt
import json
from dumps.wikidump import Config
from dumps.pagerange import jsonify
from dumps.pagerange import PageRange
from dumps.pagerange import QueryRunner


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: python3 get_pagerange.py --wiki <wikiname>
        --start <int> --end <int>
        [--configfile <path>] [--verbose] [--help]

--wiki       (-w):  name of db of wiki for which to run
--start      (-s):  page id start
--end        (-e):  page id end
--configfile (-c):  path to config file
--verbose    (-v):  display messages about what the script is doing
--help       (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    """
    main entry point
    """
    start = None
    end = None
    configpath = "wikidump.conf"
    wikiname = None
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "s:e:c:w:vh",
                                                 ["start=", "end=",
                                                  "configfile=", "wiki=",
                                                  "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configpath = val
        elif opt in ["-w", "--wiki"]:
            wikiname = val
        elif opt in ["-s", "--start"]:
            if not val.isdigit():
                usage("start must be a number")
            start = int(val)
        elif opt in ["-e", "--end"]:
            if not val.isdigit():
                usage("end must be a number")
            end = int(val)
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if not start or not end or not wikiname:
        usage("one of the mandatory arguments 'start', 'end' or 'wiki' was not specified")

    if remainder:
        usage("Unknown option(s) specified: %s" % remainder[0])

    wiki_config = Config(configpath)
    # pick up the per-wiki settings here
    wiki_config.parse_conffile_per_project(wikiname)

    prange = PageRange(QueryRunner(wikiname, wiki_config, verbose), verbose)
    ranges = prange.get_pageranges_for_revs(start, end, wiki_config.revs_per_job)

    print(json.dumps(jsonify(ranges, 0)))  # skip zero-padding, who cares


if __name__ == "__main__":
    do_main()
