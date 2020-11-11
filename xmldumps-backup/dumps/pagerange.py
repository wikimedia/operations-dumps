#!/usr/bin/python3
"""
given either number of jobs desired, or number
of revs (approx) per job desired, generate a list
of sequential page ranges that could be dumped to
produce the required number of jobs all taking
about the same length of time, or to produce
the requested number of revisions in each output,
likewise taking (roughly) the same amount of
time.
this lets us break up full content dumps into
a number of smaller jobs and run them in batches,
rerunning specific files as needed.
"""


import getopt
import gzip
import os
import sys
import json

from dumps.wikidump import Config, Wiki
from dumps.utils import DbServerInfo
import xmlstreams


def get_count_from_output(sqloutput):
    """
    given sql query output,
    return the single integer value we expect from it
    """
    lines = sqloutput.splitlines()
    if lines and lines[1]:
        if not lines[1].isdigit():
            return None   # probably NULL or missing table
        return int(lines[1])
    return None


def get_length_from_output(sqloutput):
    """
    given sql query output,
    return the single integer value we expect from it
    """
    lines = sqloutput.splitlines()
    if lines and lines[1]:
        if lines[1] == b'NULL':
            return 0
        if not lines[1].isdigit():
            return None   # probably NULL or missing table
        return int(lines[1])
    return None


def get_estimate_from_output(sqloutput):
    """
    dig out the estimated number or rows form sql output
    and return it
    """
    lines = sqloutput.splitlines()
    if lines and lines[1]:
        fields = lines[1].split()
        # id | select_type | table | type | possible_keys | key | key_len | ref | rows
        if not fields[8].isdigit():
            print("unexpected output from sql query, giving up:")
            print(sqloutput)
            sys.exit(1)
        return int(fields[8])
    return None


def adjust(count, adj, sign):
    """
    adjust the count by the adjustment amount in
    the direction of the sign
    """
    if sign < 0:
        count -= adj
    else:
        count += adj
    return count


class QueryRunner():
    """
    runs various db queries related to page, revision count, etc.
    """
    def __init__(self, dbname, config, verbose=False):
        self.dbname = dbname
        self.config = config
        self.verbose = verbose
        self.wiki = Wiki(self.config, self.dbname)
        self.db_info = DbServerInfo(self.wiki, self.dbname)

    def get_max_id(self, idtype):
        """
        get and return the max (rev or page) id
        """
        if idtype == 'page':
            return xmlstreams.get_max_id(self.config, self.dbname, 'page_id', 'page')
        if idtype == 'rev':
            return xmlstreams.get_max_id(self.config, self.dbname, 'rev_id', 'revision')
        return None

    def get_count(self, page_start, page_end):
        """
        get the number of revisions for the pages starting
        from page_start and ending with page_end
        and return it
        """
        query = ("select count(rev_id) from revision where "
                 "rev_page >= {start} and rev_page < {end}".format(
                     start=page_start, end=page_end))
        queryout = self.db_info.run_sql_query_with_retries(query)
        if queryout is None:
            print("unexpected output from sql query, giving up:")
            print(query, queryout)
            sys.exit(1)

        revcount = get_count_from_output(queryout)
        if revcount is None:
            print("unexpected output from sql query, giving up:")
            print(query, queryout)
            sys.exit(1)
        return revcount

    def get_length(self, page_start, page_end):
        """
        get cumulative byte count of all revisions for the pages
        starting from page_start and ending with page_end,
        and return it
        """
        query = ("select sum(rev_len) from revision where "
                 "rev_page >= {start} and rev_page < {end}".format(
                     start=page_start, end=page_end))
        queryout = self.db_info.run_sql_query_with_retries(query)
        if queryout is None:
            print("unexpected output from sql query, giving up:")
            print(query, queryout)
            sys.exit(1)

        revlength = get_length_from_output(queryout)
        if revlength is None:
            print("unexpected output from sql query, giving up:")
            print(query, queryout)
            sys.exit(1)
        return revlength

    def get_estimate(self, page_start, page_end):
        """
        get estimate of number of revisions (via explain)
        for page range from page_start to page_end
        and return it
        """
        query = ("explain select count(rev_id) from revision where "
                 "rev_page >= {start} and rev_page <= {end}".format(
                     start=page_start, end=page_end))
        queryout = self.db_info.run_sql_query_with_retries(query)
        if queryout is None:
            print("unexpected output from sql query, giving up:")
            print(query, queryout)
            sys.exit(1)
        return get_estimate_from_output(queryout)


class PageRange():
    '''
    Methods for getting number of revisions for a page range.
    We use this for splitting up history runs into small chunks to be run in
    parallel, with each job taking roughly the same length of time.
    '''

    def __init__(self, qrunner, verbose=False):
        '''
        args:
           QueryRunner
           bool
        '''
        self.qrunner = qrunner
        self.verbose = verbose

        # these only get loaded if we need them
        self.total_pages = None
        self.total_revs = None

    def get_pageranges_for_jobs(self, numjobs):
        '''
        get and return list of tuples consisting of page id start and end to be passed to
        self.numjobs jobs which should run in approximately the same length of time
        for full history dumps, which is all we care about, really
        numjobs -- number of jobs to be run in parallel, and so number of page ranges
                   we want to produce for these parallel runs
        '''

        if self.total_revs is None:
            self.total_revs = self.qrunner.get_max_id('rev')
        if self.total_pages is None:
            self.total_pages = self.qrunner.get_max_id('page')
        ranges = []
        page_start = 1
        numrevs = int(self.total_revs / numjobs) + 1
        # actually this is ok for the start but it varies right afterwards
        # interval = ((self.total_pages - page_start)/numjobs_left) + 1
        prevguess = 0
        for jobnum in range(1, numjobs + 1):
            if jobnum == numjobs:
                # last job, don't bother searching. just append up to max page id
                ranges.append((page_start, self.total_pages))
                break
            numjobs_left = numjobs - jobnum + 1
            interval = int((self.total_pages - page_start) / numjobs_left) + 1
            (start, end) = self.get_pagerange(page_start, numrevs, None,
                                              page_start + interval, prevguess)
            page_start = end + 1
            prevguess = end
            if end > self.total_pages:
                end = self.total_pages
            ranges.append((start, end))
            if page_start > self.total_pages:
                break
        return ranges

    @staticmethod
    def get_ranges_via_revinfo(revinfo_path, maxbytes, maxrevs,
                               minpageid, maxpageid, minpagecount=1):
        '''
        given a path to a file with triplets

        pageid:bytecount:revcount

        where the bytecount is the length of the revs up to the next
        page id entry (or until maxpageid, if the entry is the last
        one in the file), the revcount is the number of revs up to
        the next page id entry or until maxpageid if the entry is the
        last in the file,

        return a list of ordered tuples (startpageid, endpageid) covering
        minpageid to maxpageid,
        where one or the other or possibly both bytecount or rev count
        for the range will be over the max specified, but as close as
        possible given the batched revinfo,
        OR the number of pages in the range is minpagecount and that
        puts both revcount and bytecount over the max by an arbitrary
        amount.
        '''
        if not os.path.exists(revinfo_path):
            return []

        ranges = []
        done = False
        range_start = minpageid
        bytecount_sum = 0
        revcount_sum = 0
        with gzip.open(revinfo_path, "r") as page_info:
            while not done:
                entry = page_info.readline().rstrip()
                if not entry:
                    # eof, stash last range
                    if range_start < maxpageid:
                        ranges.append((range_start, maxpageid))
                    break

                fields = entry.split(b':')
                pageid = int(fields[0])
                bytecount = int(fields[1])
                revcount = int(fields[2])

                if pageid < minpageid:
                    bytecount_sum = bytecount
                    revcount_sum = revcount
                elif pageid >= maxpageid:
                    # past the range of pages we want, stash last range
                    ranges.append((range_start, maxpageid))
                    break
                elif ((bytecount_sum + bytecount > maxbytes or
                       revcount_sum + revcount > maxrevs) and
                      pageid - range_start >= minpagecount):
                    # we're over maxbytes or max revcount
                    # and we're over min page count for a range,
                    # stash the range
                    ranges.append((range_start, pageid - 1))
                    range_start = pageid
                    # we expect that the bulk of the byte and rev count will be in the rest of the
                    # interval, not the last page. this can be wrong but it won't be a disaster.
                    bytecount_sum = 0
                    revcount_sum = 0
                else:
                    bytecount_sum += bytecount
                    revcount_sum += revcount
            return ranges

    def get_ranges_via_db(self, page_start, page_end, numrevs, maxbytes):
        '''
        get page ranges for small page content jobs by repeated
        queries to the db about the size and length of revisions
        per range of pages. truly despicable.
        '''
        # do it the hard way. wow this is horrible.
        if self.total_revs is None:
            self.total_revs = self.qrunner.get_max_id('rev')
        if self.total_pages is None:
            self.total_pages = self.qrunner.get_max_id('page')
        ranges = []
        prevguess = page_start
        if page_start == 1 and page_end == self.total_pages:
            numjobs = int(self.total_revs / numrevs) + 1
        else:
            estimate = self.qrunner.get_estimate(page_start, page_end)
            revs_for_range = self.get_revcount(int(page_start), int(page_end), estimate)
            numjobs = int(revs_for_range / numrevs) + 1
            if self.verbose:
                print("DEBUG***: page_start, page_end, estimate, revs_for_range, numjobs:",
                      page_start, page_end, estimate, revs_for_range, numjobs)
        jobnum = 1
        while True:
            jobnum += 1
            numjobs_left = numjobs - jobnum + 1
            if numjobs_left <= 0:
                # our initial count was a bit off, and we'll have more jobs
                # than we thought. just keep passing the same endpoint
                # and getting ranges until we've gotten up through
                # the endpoint returned
                numjobs_left = 1
            interval = int((page_end - page_start) / numjobs_left) + 1
            (start, end) = self.get_pagerange(page_start, numrevs, maxbytes,
                                              page_start + interval, prevguess)
            if self.verbose:
                print("DEBUG!! page range decided is", start, "to", end)
            page_start = end + 1
            prevguess = end
            if end > page_end:
                end = page_end
            ranges.append((start, end))
            if page_start > page_end:
                break
        return ranges

    def get_pageranges_for_revs(self, page_start, page_end, numrevs, maxbytes,
                                revinfo_path=None, minpagecount=5):
        '''
        get and return list of tuples consisting of page id start and end
        which should each, if dumped (full history content dumps) contain about
        the specified number of revisions (with maxbytes as a hard cutoff to the
        total byte count of the revisions), and thus run in something
        close to the same time
        numrevs    -- number of revisions (approx) for each page range to contain
        page_start -- don't start at page 1, start at this page instead
        page_end   -- don't end with last page, end at this page instead

        all args are ints
        returns: list of (pagestart, pageend)
        '''

        ranges = []
        if not page_start:
            page_start = 1
        if not page_end:
            if self.total_pages is None:
                self.total_pages = self.qrunner.get_max_id('page')
            page_end = self.total_pages

        if revinfo_path:
            ranges = self.get_ranges_via_revinfo(revinfo_path, maxbytes, numrevs,
                                                 page_start, page_end, minpagecount)
            if ranges:
                if self.verbose:
                    print("page ranges retrieved via revinfo for", page_start, page_end)
                return ranges

        ranges = self.get_ranges_via_db(page_start, page_end, numrevs, maxbytes)
        return ranges

    def get_revcount(self, page_start, page_end, estimate):
        """
        for the given page range, get the number of revisions,
        running multiple queries so we don't make the servers sad
        the number of queries is based on the estimated number
        of revs passed in ("estimate"), where we try not to run
        a query that will result in more than 50k revs being
        counted. Key word being "try".  IN some cases a page
        all by itself may have hundreds of thousands of revs,
        whaddya gonna do

        args:
           page_start, page_end: numbers
           estimate: number
        """
        total = 0
        maxtodo = 50000

        runstodo = int(estimate / maxtodo) + 1
        # let's say minimum pages per job is 1, that's
        # quite reasonable (in the case where some pages
        # have many many revisions
        step = int((page_end - page_start) / runstodo) + 1
        ends = list(range(page_start, page_end, step))

        if ends[-1] != page_end:
            ends.append(page_end)
        interval_start = ends.pop(0)

        for interval_end in ends:
            count = self.qrunner.get_count(interval_start, interval_end)
            interval_start = interval_end + 1
            total += count
        return total

    def get_revbytes(self, page_start, page_end, estimate):
        """
        for the given page range, get the cumulative revision byte count,
        running multiple queries so we don't make the servers sad
        the number of queries is based on the estimated number
        of revs passed in ("estimate"), where we try not to run
        a query that will result in more than 50k revs being
        summed up. Key word being "try".  In some cases a page
        all by itself may have hundreds of thousands of revs,
        that's the breaks

        args:
           page_start, page_end: numbers
           estimate: number
        """
        total = 0
        maxtodo = 50000

        if estimate is None:
            runstodo = 2
        else:
            runstodo = int(estimate / maxtodo) + 1
        # let's say minimum pages per job is 1, that's
        # quite reasonable (in the case where some pages
        # have many many revisions
        step = int((page_end - page_start) / runstodo) + 1
        ends = list(range(page_start, page_end, step))

        if ends[-1] != page_end:
            ends.append(page_end)
        interval_start = ends.pop(0)

        for interval_end in ends:
            revbytes = self.qrunner.get_length(interval_start, interval_end)
            interval_start = interval_end + 1
            total += revbytes
        return total

    def adjust_pagerange_for_revbytes(self, page_start, page_end, total_revs, maxbytes):
        """
        given a page range that might be a candidate, get a good estimate of the
        cumulative number of bytes in the revisions and adjust the end of the page
        range accordingly, returning the new end
        """
        # don't do a check, return what we got
        if maxbytes is None:
            return page_end

        if total_revs <= 50000:
            estimate = None
        else:
            estimate = total_revs
        if self.verbose:
            print("adjust pagerange for revbytes, start page_id",
                  page_start, "end page_id:", page_end, "estimate:", estimate)
        revbytes = self.get_revbytes(page_start, page_end, estimate)

        interval_end = page_end
        incr = int((page_end - page_start) / 2)

        while True:
            # someday the pages will be so huge that 16 pages and all their revisions
            # in one file will be too many. but not today.
            if page_end - page_start <= 16:
                # things are too large. set the minimum interval of 16 pages and return.
                return page_start + 16

            if incr <= 16:
                # can't do better than this, give up
                return interval_end

            if revbytes <= maxbytes:
                # we're in the byte limit and so done
                return interval_end

            if self.verbose:
                print("adjust pagerange for revbytes loop, start page_id",
                      page_start, "interval_end:", interval_end, "revbytes:",
                      revbytes, "incr:", incr)

            interval_end = interval_end - incr
            incr = int(incr / 2)
            # estimate may be too large, we don't care, that just means more jobs
            # will run than needed
            revbytes = self.get_revbytes(page_start, interval_end, estimate)
            if revbytes > maxbytes:
                continue

            # we are less than maxbytes, but by how much? is it worth it
            # to try to fine tune? I'll say that if we are within 10% it's good
            # enough
            if (maxbytes - revbytes) / maxbytes >= 0.9:
                # no fine tuning needed, let the loop kick us out on the
                # next pass through
                continue

            while (maxbytes > revbytes and incr > 16 and interval_end - page_start > 16 and
                   (maxbytes - revbytes) / maxbytes < 0.9):
                # estimate may be too large, we don't care, that just means more jobs
                # will run than needed
                if self.verbose:
                    print("under maxrevbytes, fine tune: maxbytes:", maxbytes,
                          "revbytes:", revbytes, "incr:", incr)
                interval_end = interval_end + incr
                incr = int(incr / 2)
                revbytes = self.get_revbytes(page_start, interval_end, estimate)
                if maxbytes < revbytes:
                    interval_end = interval_end - incr
                    incr = int(incr / 2)
                    # let the main loop take care of it
                    break

    def get_pagerange(self, page_start, numrevs, maxbytes, badguess, prevguess):
        """
        given starting page, number of revisions desired for the page
        range, the current end of range guess and the previous guess,
        progressively narrow down the endpoint til we get a value
        that is "close enough" and return the page range
        as tuple (start, end)
        """
        if self.verbose:
            print("get_pagerange called with page_start", page_start,
                  "numrevs", numrevs, "badguess", badguess, "prevguess", prevguess)
        interval_start = page_start
        interval_end = badguess
        revcount = 0
        while True:
            if self.verbose:
                print("page range loop, start page_id",
                      interval_start, "end page_id:", interval_end)

            estimate = self.qrunner.get_estimate(interval_start, interval_end)
            revcount_adj = self.get_revcount(interval_start, interval_end, estimate)
            revcount = adjust(revcount, revcount_adj, badguess - prevguess)

            if self.verbose:
                print("estimate is", estimate, "revcount is", revcount,
                      "and numrevs is", numrevs)

            margin = abs(revcount - numrevs)
            if margin <= self.qrunner.wiki.config.revs_margin or abs(prevguess - badguess) <= 2:
                badguess = self.adjust_pagerange_for_revbytes(page_start, badguess,
                                                              numrevs, maxbytes)
                return (page_start, badguess)

            interval = int(abs(prevguess - badguess) / 2)

            prevguess = badguess

            # set 1 page as an absolute minimum in a query, even if revcount is too large
            if badguess - page_start <= 1:
                if self.verbose:
                    print("badguess:", badguess, "page_start:", page_start,
                          "stopping here with guess")
                return (page_start, badguess)

            if self.verbose:
                print("revcount is greater than allowed margin from numrevs")

            badguess = adjust(badguess, interval, numrevs - revcount)

            if badguess < prevguess:
                interval_start = badguess
                interval_end = prevguess
            else:
                interval_start = prevguess
                interval_end = badguess
            if self.verbose:
                print("new interval:", interval_start, interval_end)


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: pagerange.py --wiki <wikiname> --jobs|--revs <int>
        [--revinfopath <path>] [--maxbytes <int>]
        [--pagestart <int>] [--pageend <int>]
        [--pad <int>] [--json]
        [--configfile <path>] [--verbose] [--help]

--wiki        (-w):  name of db of wiki for which to run
--jobs        (-j):  generate page ranges for this number of jobs
--revs        (-r):  generate page ranges for this number of
                     revisions per interval
--revinfopath (-R):  path to revinfo file, default None
                     this option requires --revs
--maxbytes    (-m):  max bytes per pagerange (uncompressed)
--pagestart   (-s):  page id start for --revs option, default 1
--pageend     (-e):  page id end for --revs option, default last
--configfile  (-c):  path to config file
--pad         (-p):  pad numbers out to specified length with
                     leading zeros for json format
--json        (-J):  write results as json formatted output
--verbose     (-v):  display messages about what the script is doing
--help        (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def check_args(remainder, wiki, revs, jobs):
    """
    whine if there are command line arg problems,
    and exit
    """
    if remainder:
        usage("Unknown option specified")

    if wiki is None:
        usage("Mandatory option 'wiki' is not specified")
    if revs is None and jobs is None:
        usage("One of 'revs' or 'jobs' must be specified")
    elif revs and jobs:
        usage("Only one of 'revs' or 'jobs' must be specified.")


def check_int_range_opts(range_opts, names):
    """
    make sure they're all nice integers and convert them,
    or whine and exit
    """

    for varname in names:
        if range_opts[varname] is not None:
            if not range_opts[varname].isdigit:
                usage("--" + names[varname] + "argument requires a number")
            else:
                range_opts[varname] = int(range_opts[varname])


def jsonify(pagerange, pad):
    """
    given a list of tuples [(startid, endid)...] turn it into
    a list of hashes  [{'pstart': blah, 'pend': blah}...]
    which can be fed to any json processor
    optionally (if pad is greater than zero) the
    ids will be padded out to the specified length
    with leading zeros
    """
    jsonified = []
    for pair in pagerange:
        start = str(pair[0])
        end = str(pair[1])
        if pad > 0:
            start = start.zfill(pad)
            end = end.zfill(pad)
        jsonified.append({'pstart': start, 'pend': end})
    return jsonified


def do_pageranges(prange, range_opts, pad, jsonfmt):
    """
    get and display page ranges for revs or jobs as specified, adding
    optionally padding to 'pad' places by leading zeros, if 0
    is passed no padding will be done
    """
    if range_opts['jobs']:
        ranges = prange.get_pageranges_for_jobs(range_opts['jobs'])
        # convert ranges into the output we need for the pagesperchunkhistory config
        pages_per_job = [page_end - page_start for (page_start, page_end) in ranges]
        if jsonfmt:
            print(json.dumps(jsonify(ranges, pad)))
            print(json.dumps(jsonify(pages_per_job, pad)))
        else:
            print("for {jobs} jobs, have ranges:".format(jobs=range_opts['jobs']))
            print(ranges)
            print("for {jobs} jobs, have config setting:".format(jobs=range_opts['jobs']))
            print(pages_per_job)
    else:
        ranges = prange.get_pageranges_for_revs(range_opts['start'], range_opts['end'],
                                                range_opts['revs'], range_opts['maxbytes'],
                                                range_opts['revinfopath'])
        if jsonfmt:
            print(json.dumps(jsonify(ranges, pad)))
        else:
            print(("for start", range_opts['start'], "and end",
                   range_opts['end'] if range_opts['end'] is not None else "last page"))
            print("for {revs} revs, have ranges:".format(revs=range_opts['revs']))
            print(ranges)


def do_main():
    """
    main entry point
    """
    range_opts = {'jobs': None, 'revs': None, 'start': None, 'end': None,
                  'maxbytes': None, 'revinfopath': None}
    wiki = None
    configpath = "wikidump.conf"
    jsonfmt = False
    verbose = False
    pad = 0
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:w:j:s:e:m:r:R:p:vh",
                                                 ["configfile=", "wiki=", "jobs=", "maxbytes=",
                                                  "pagestart=", "pageend=", "revs=",
                                                  "revinfopath=", "pad=", "json",
                                                  "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configpath = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-j", "--jobs"]:
            range_opts['jobs'] = val
        elif opt in ["-m", "--maxbytes"]:
            range_opts['maxbytes'] = val
        elif opt in ["-r", "--revs"]:
            range_opts['revs'] = val
        elif opt in ["-R", "--revinfopath"]:
            range_opts['revinfopath'] = val
        elif opt in ["-e", "--pageend"]:
            range_opts['end'] = val
        elif opt in ["-s", "--pagestart"]:
            range_opts['start'] = val
        elif opt in ["-p", "--pad"]:
            try:
                pad = int(val)
            except ValueError:
                usage("'pad' argument must be a number")
        elif opt in ["-J", "--json"]:
            jsonfmt = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    check_int_range_opts(range_opts, {'jobs': 'jobs', 'revs': 'revs',
                                      'end': 'pageend', 'start': 'pagestart',
                                      'maxbytes': 'maxbytes'})
    check_args(remainder, wiki, range_opts['revs'], range_opts['jobs'])

    prange = PageRange(QueryRunner(wiki, Config(configpath), verbose), verbose)
    do_pageranges(prange, range_opts, pad, jsonfmt)


if __name__ == "__main__":
    do_main()
