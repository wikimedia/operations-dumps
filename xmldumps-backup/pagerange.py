import getopt
import sys
import time

from dumps.WikiDump import Config, Wiki
from xmlstreams import get_max_id
from dumps.utils import DbServerInfo


def get_count_from_output(sqloutput):
    lines = sqloutput.splitlines()
    if lines and lines[1]:
        if not lines[1].isdigit():
            return None   # probably NULL or missing table
        return int(lines[1])
    return None


def get_estimate_from_output(sqloutput):
    lines = sqloutput.splitlines()
    if lines and lines[1]:
        fields = lines[1].split()
        # id | select_type | table | type | possible_keys | key | key_len | ref | rows
        if not fields[8].isdigit():
            return None   # probably NULL or missing table
        return int(fields[8])
    return None


class PageRange(object):
    '''
    Methods for getting number of revisions for a page range.
    We use this for splitting up history runs into small chunks to be run in
    parallel, with each job taking roughly the same length of time.
    '''

    def __init__(self, dbname, config, maxrev=None, verbose=False):
        '''
        Arguments:
        dbname  -- the name of the database we are dumping
        config  -- this is the general config context used by the runner class
        maxrev  -- if specified, this is the number of total revisions in a dump
                   if not specified, the max(rev_id) value from the db will be used,
                   which will not account for revisions which should be skipped
                   because pages have been deleted
        '''
        self.dbname = dbname
        self.config = config
        self.verbose = verbose

        self.total_pages = get_max_id(self.config, self.dbname, 'page_id', 'page')
        if maxrev is not None:
            self.total_revs = maxrev
        else:
            self.total_revs = get_max_id(self.config, self.dbname, 'rev_id', 'revision')
        self.wiki = Wiki(self.config, self.dbname)
        self.db_info = DbServerInfo(self.wiki, self.dbname)

    def get_page_ranges(self, numjobs):
        '''
        get and return list of tuples consisting of page id start and end to be passed to
        self.numjobs jobs which should run in approximately the same length of time
        for full history dumps, which is all we care about, really
        numjobs -- number of jobs to be run in parallel, and so number of page ranges
                   we want to produce for these parallel runs
        '''

        ranges = []
        page_start = 1
        numrevs = self.total_revs/numjobs + 1
        prevguess = 0
        for jobnum in range(1, int(numjobs) + 1):
            if jobnum == numjobs:
                # last job, don't bother searching. just append up to max page id
                ranges.append((page_start, self.total_pages))
                break
            numjobs_left = numjobs - jobnum + 1
            interval = ((self.total_pages - page_start)/numjobs_left) + 1
            (start, end) = self.get_page_range(page_start, numrevs,
                                               page_start + interval, prevguess)
            page_start = end + 1
            prevguess = end
            if end > self.total_pages:
                end = self.total_pages
            ranges.append((start, end))
            if page_start > self.total_pages:
                break
        return ranges

    def get_estimate(self, page_start, badguess):
        query = ("explain select count(rev_id) from revision where "
                 "rev_page >= %d and rev_page < %d" % (page_start, badguess))
        queryout = self.run_sql_query(query)
        if queryout is None:
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)
        return get_estimate_from_output(queryout)

    def get_count(self, page_start, badguess):
        query = ("select count(rev_id) from revision where "
                 "rev_page >= %d and rev_page < %d" % (page_start, badguess))
        queryout = self.run_sql_query(query)
        if queryout is None:
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)

        revcount = get_count_from_output(queryout)
        if revcount is None:
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)
        return revcount

    def get_revcount(self, page_start, guess, estimate):
        total = 0
        maxtodo = 1000000

        runstodo = estimate/maxtodo + 1
        step = (guess - page_start)/runstodo
        ends = range(page_start, guess, step)

        if ends[-1] != guess:
            ends.append(guess)
        interval_start = ends.pop(0)

        for interval_end in ends:
            count = self.get_count(interval_start, interval_end)
            interval_start = interval_end + 1
            total += count
        return total

    def get_page_range(self, page_start, numrevs, badguess, prevguess):
        if self.verbose:
            print ("get_page_range called with page_start", page_start,
                   "numrevs", numrevs, "badguess", badguess,
                   "prevguess", prevguess)
        interval_start = page_start
        interval_end = badguess
        revcount = 0
        while True:
            if self.verbose:
                print ("page range loop, start page_id", interval_start,
                       "end page_id:", interval_end)

            estimate = self.get_estimate(interval_start, interval_end)
            revcount_adj = self.get_revcount(interval_start, interval_end, estimate)
            if badguess < prevguess:
                revcount -= revcount_adj
            else:
                revcount += revcount_adj

            if self.verbose:
                print "estimate is", estimate, "revcount is", revcount, "and numrevs is", numrevs

            interval = abs(prevguess - badguess) / 2
            if not interval:
                return (page_start, badguess)
            prevguess = badguess

            if revcount - numrevs > 100:
                # too many
                if self.verbose:
                    print "too many, adjusting to", badguess, " minus", interval
                badguess = badguess - interval
            elif numrevs - revcount > 100:
                # too few
                badguess = badguess + interval
            else:
                return (page_start, badguess)
            if badguess < prevguess:
                interval_start = badguess
                interval_end = prevguess
            else:
                interval_start = prevguess
                interval_end = badguess

    def run_sql_query(self, query, maxretries=3):
        results = None
        retries = 0
        while results is None and retries < maxretries:
            retries = retries + 1
            results = self.db_info.run_sql_and_get_output(query)
            if results is None:
                time.sleep(5)
                # get db config again in case something's changed
                self.db_info = DbServerInfo(self.wiki, self.dbname)
                continue
            return results
        return results


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: pagerange.py --wiki <wikiname> --jobs <int>
        [--maxrev <int>] [--configfile <path>] [--verbose] [--help]

This script generates a list of page intervals suitable for use
by pagesPerChunkHistory in a wiki dumps config file.  Provide
the name of the wiki and the number of parallel jobs you wish to
run for dumping page revision content, and it will generate a list
which should ensure that each job will contain about the same
number of revisions and hence take about the same length of time
to complete.

Note that this script does not account for revisions that should not
be counted because they belong to deleted pages and so will not be
dumped. when the script decides how many revisions should be processed
by each job, it references the total number of revisions instead.
So you may end up with not very many pages in the last page range.

If this happens for you, you may specify the total number of revisions in the
dump if you have a recent number handy (e.g. by grep '<revision>'| wc -l of the
stubs dumps).  You can pass that number with the --maxrev option
as described below.

--wiki       (-w):  name of db of wiki for which to run
--jobs       (-j):  generate page ranges for this number of jobs
--maxrev     (-m):  use this number for the total number of revisions
                    rather than using maxrevid from the database
--configfile (-c):  path to config file
--verbose    (-v):  display messages about what the script is doing
--help       (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    wiki = None
    configpath = "wikidump.conf"
    jobs = None
    maxrev = None
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:w:j:m:vh",
                                                 ["configfile=", "wiki=", "jobs=",
                                                  "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configpath = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-j", "--jobs"]:
            if not val.isdigit():
                usage("--jobs argument requires a number")
            jobs = int(val)
        elif opt in ["-m", "--maxrev"]:
            if not val.isdigit():
                usage("--maxrev argument requires a number")
            maxrev = int(val)
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if len(remainder) > 0:
        usage("Unknown option specified")

    if wiki is None or jobs is None:
        usage("One of mandatory options 'wiki' or 'jobs' not set")

    config = Config(configpath)
    prange = PageRange(wiki, config, maxrev, verbose)
    ranges = prange.get_page_ranges(jobs)
    print "for %d jobs, have ranges:" % jobs
    print ranges
    # convert ranges into the output we need for the pagesperchunkhistory config
    pages_per_job = [page_end - page_start for (page_start, page_end) in ranges]
    print "for %d jobs, have config setting:" % jobs
    print pages_per_job

if __name__ == "__main__":
    main()
