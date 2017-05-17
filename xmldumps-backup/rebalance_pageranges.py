"""
generate new set of values for PagesPerChunkHistory based
on current state of a given wiki
"""
import sys
import getopt
from dumps.WikiDump import Config
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
Usage: rebalance_pagerange.py --wiki <wikiname>
        --start <int> --end <int>
        [--configfile <path>] [--verbose] [--help]

--wiki       (-w):  name of db of wiki for which to run
--jobs       (-j):  generate page ranges for this number of jobs
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
    jobs = None
    configpath = "wikidump.conf"
    wikiname = None
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:j:w:vh",
                                                 ["jobs=", "configfile=", "wiki=",
                                                  "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configpath = val
        elif opt in ["-w", "--wiki"]:
            wikiname = val
        elif opt in ["-j", "--jobs"]:
            if not val.isdigit():
                usage("jobs must be a number")
            jobs = int(val)
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if not jobs or not wikiname:
        usage("one of the mandatory arguments 'jobs' or 'wiki' was not specified")

    if len(remainder) > 0:
        usage("Unknown option(s) specified: %s" % remainder[0])

    wiki_config = Config(configpath)
    # pick up the per-wiki settings here
    wiki_config.parse_conffile_per_project(wikiname)

    prange = PageRange(QueryRunner(wikiname, wiki_config, verbose), verbose)
    ranges = prange.get_pageranges_for_jobs(jobs)
    # convert ranges into the output we need for the pagesperchunkhistory config
    pages_per_job = [page_end - page_start for (page_start, page_end) in ranges]
    print "for {jobs} jobs, have ranges:".format(jobs=jobs)
    print ranges
    print "for {jobs} jobs, have config setting:".format(jobs=jobs)
    print pages_per_job


if __name__ == "__main__":
    do_main()
