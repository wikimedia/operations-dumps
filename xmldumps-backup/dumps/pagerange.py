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
import sys
import json

from dumps.WikiDump import Config, Wiki
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
            print "unexpected output from sql query, giving up:"
            print sqloutput
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


class QueryRunner(object):
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
        elif idtype == 'rev':
            return xmlstreams.get_max_id(self.config, self.dbname, 'rev_id', 'revision')
        else:
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
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)

        revcount = get_count_from_output(queryout)
        if revcount is None:
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)
        return revcount

    def get_estimate(self, page_start, page_end):
        """
        get estimate of number of revisions (via explain)
        for page range from page_start to page_end
        and return it
        """
        query = ("explain select count(rev_id) from revision where "
                 "rev_page >= {start} and rev_page < {end}".format(
                     start=page_start, end=page_end))
        queryout = self.db_info.run_sql_query_with_retries(query)
        if queryout is None:
            print "unexpected output from sql query, giving up:"
            print query, queryout
            sys.exit(1)
        return get_estimate_from_output(queryout)


class PageRange(object):
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

        self.total_pages = qrunner.get_max_id('page')
        self.total_revs = qrunner.get_max_id('rev')

    def get_pageranges_for_jobs(self, numjobs):
        '''
        get and return list of tuples consisting of page id start and end to be passed to
        self.numjobs jobs which should run in approximately the same length of time
        for full history dumps, which is all we care about, really
        numjobs -- number of jobs to be run in parallel, and so number of page ranges
                   we want to produce for these parallel runs
        '''

        ranges = []
        page_start = 1
        numrevs = self.total_revs / numjobs + 1
        # actually this is ok for the start but it varies right afterwards
        # interval = ((self.total_pages - page_start)/numjobs_left) + 1
        prevguess = 0
        for jobnum in range(1, numjobs + 1):
            if jobnum == numjobs:
                # last job, don't bother searching. just append up to max page id
                ranges.append((page_start, self.total_pages))
                break
            # this is wrong too, we need to get it passed or something
#            prevguess = min(interval*(jobnum+1), self.total_pages)
            # fixme here the interval*jobnum can't be right
#            (start, end) = self.get_pagerange(page_start, numrevs,
#                                               interval*jobnum, prevguess)
            numjobs_left = numjobs - jobnum + 1
            interval = ((self.total_pages - page_start) / numjobs_left) + 1
            (start, end) = self.get_pagerange(page_start, numrevs,
                                              page_start + interval, prevguess)
            page_start = end + 1
            prevguess = end
            if end > self.total_pages:
                end = self.total_pages
            ranges.append((start, end))
            if page_start > self.total_pages:
                break
        return ranges

    def get_pageranges_for_revs(self, page_start, page_end, numrevs):
        '''
        get and return list of tuples consisting of page id start and end
        which should each, if dumped (full history content dumps) contain about
        the specified number of revisions, and thus run in something close
        to the same time
        numrevs    -- number of revisions (approx) for each page range to contain
        page_start -- don't start at page 1, start at this page instead
        page_end   -- don't end with last page, end at this page instead

        all args are ints
        returns: list of (pagestart<str>, pageend<str>)
        '''

        ranges = []
        if not page_start:
            page_start = 1
        if not page_end:
            page_end = self.total_pages
        # actually this is ok for the start but it varies right afterwards
        # interval = ((self.total_pages - page_start)/numjobs_left) + 1
        prevguess = 0
        if page_start == 1 and page_end == self.total_pages:
            numjobs = self.total_revs / numrevs + 1
        else:
            estimate = self.qrunner.get_estimate(page_start, page_end)
            revs_for_range = self.get_revcount(int(page_start), int(page_end), estimate)
            numjobs = revs_for_range / numrevs + 1
        for jobnum in range(1, numjobs + 1):
            if jobnum == numjobs:
                # last job, don't bother searching. just append up to max page id
                ranges.append((str(page_start), str(page_end)))
                break
            # this is wrong too, we need to get it passed or something
#            prevguess = min(interval*(jobnum+1), self.total_pages)
            # fixme here the interval*jobnum can't be right
#            (start, end) = self.get_pagerange(page_start, numrevs,
#                                               interval*jobnum, prevguess)
            numjobs_left = numjobs - jobnum + 1
            interval = (page_end - page_start) / numjobs_left + 1
            (start, end) = self.get_pagerange(page_start, numrevs,
                                              page_start + interval, prevguess)
            page_start = end + 1
            prevguess = end
            if end > page_end:
                end = page_end
            ranges.append((str(start), str(end)))
            if page_start > page_end:
                break
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

        runstodo = estimate / maxtodo + 1
        step = (page_end - page_start) / runstodo
        ends = range(page_start, page_end, step)

        if ends[-1] != page_end:
            ends.append(page_end)
        interval_start = ends.pop(0)

        for interval_end in ends:
            count = self.qrunner.get_count(interval_start, interval_end)
            interval_start = interval_end + 1
            total += count
        return total

    def get_pagerange(self, page_start, numrevs, badguess, prevguess):
        """
        given starting page, number of revisions desired for the page
        range, the current end of range guess and the previous guess,
        progressively narrow down the endpoint til we get a value
        that is "close enough" and return the page range
        as tuple (start, end)
        """
        if self.verbose:
            print ("get_pagerange called with page_start", page_start,
                   "numrevs", numrevs, "badguess", badguess, "prevguess", prevguess)
        interval_start = page_start
        interval_end = badguess
        revcount = 0
        while True:
            if self.verbose:
                print ("page range loop, start page_id",
                       interval_start, "end page_id:", interval_end)

            estimate = self.qrunner.get_estimate(interval_start, interval_end)
            revcount_adj = self.get_revcount(interval_start, interval_end, estimate)
            revcount = adjust(revcount, revcount_adj, badguess - prevguess)

            if self.verbose:
                print ("estimate is", estimate, "revcount is", revcount,
                       "and numrevs is", numrevs)

            interval = abs(prevguess - badguess) / 2
            if not interval:
                return (page_start, badguess)
            prevguess = badguess

            margin = abs(revcount - numrevs)
            # FIXME configurable?
            if margin <= 100:
                return (page_start, badguess)
            if self.verbose:
                print "revcount is greater than allowed margin from numrevs"

            badguess = adjust(badguess, interval, numrevs - revcount)

            if badguess < prevguess:
                interval_start = badguess
                interval_end = prevguess
            else:
                interval_start = prevguess
                interval_end = badguess
            if self.verbose:
                print "new interval:", interval_start, interval_end


def check_range_overlap(first_a, last_a, first_b, last_b):
    """
    return True if there is overlap between the two ranges,
    False otherwise
    for purposes of checking, if last in either range is None,
    consider it to be after first in both ranges
    """
    if (first_a <= first_b and
            (last_a is None or
             last_a >= first_b)):
        return True
    if (first_a >= first_b and
            (last_b is None or
             first_a <= last_b)):
        return True
    return False


def check_file_covers_range(candidate_dfname, pagerange, maxparts, all_files, runner):
    """
    see if passed DumpFilename covers at least some of the page range specified
    returns True if so, False if not and False on error

    args: DumpFilename, {'start': num, 'end': num}, ???, list of DumpFilename (all
    output files that currently exist for a given run), Runner
    """
    # If some of the dumpfilenames in file_list could not be properly be parsed, some of
    # the (int) conversions below will fail. However, it is of little use to us,
    # which conversion failed. /If any/ conversion fails, it means, that that we do
    # not understand how to make sense of the current dumpfilename. Hence we cannot use
    # it as prefetch object and we have to drop it, to avoid passing a useless file
    # to the text pass. (This could days as of a comment below, but by not passing
    # a likely useless file, we have to fetch more texts from the database)
    #
    # Therefore try...except-ing the whole block is sufficient: If whatever error
    # occurs, we do not abort, but skip the file for prefetch.
    try:
        # If we could properly parse
        first_page_id_in_file = int(candidate_dfname.first_page_id)

        # fixme what do we do here? this could be very expensive. is that worth it??
        if not candidate_dfname.last_page_id:
            # (b) nasty hack, see (a)
            # it's not a checkpoint fle or we'd have the pageid in the filename
            # so... temporary hack which will give expensive results
            # if file part, and it's the last one, put none
            # if it's not the last part, get the first pageid in the next
            #  part and subtract 1
            # if not file part, put none.
            if candidate_dfname.is_file_part and candidate_dfname.partnum_int < maxparts:
                for dfname in all_files:
                    if dfname.partnum_int == candidate_dfname.partnum_int + 1:
                        # not true!  this could be a few past where it really is
                        # (because of deleted pages that aren't included at all)
                        candidate_dfname.last_page_id = str(int(dfname.first_page_id) - 1)
        if candidate_dfname.last_page_id:
            last_page_id_in_file = int(candidate_dfname.last_page_id)
        else:
            last_page_id_in_file = None

        # FIXME there is no point in including files that have just a
        # few rev ids in them that we need, and having to read through
        # the whole file... could take hours or days (later it won't matter,
        # right? but until a rewrite, this is important)
        # also be sure that if a critical page is deleted by the time we
        # try to figure out ranges, that we don't get hosed
        if check_range_overlap(first_page_id_in_file, last_page_id_in_file,
                               pagerange['start'], pagerange['end']):
            return True
    except Exception as ex:
        runner.debug(
            "Couldn't process %s for prefetch. Format update? Corrupt file?"
            % candidate_dfname.filename)
    return False


def compare_partial_ranges(first_a, last_a, first_b, last_b):
    """
    given two ranges of numbers where first or second
    pair has the endpoint missing, compare and return
    True if overlap, False otherwise,
    the missing endpoint is presumed to be beyond
    all values
    """
    # one or both end values are missing:
    if not last_a and not last_b:
        return True
    elif not last_a and int(last_b) < int(first_a):
        return True
    elif not last_b and int(last_a) < int(first_b):
        return True
    else:
        return False


def compare_full_ranges(first_a, last_a, first_b, last_b):
    """
    given two ranges of numbers, compare and return
    True if overlap, False otherwise
    """
    # no values are 'None', can compare them all
    if (int(first_a) <= int(first_b) and
            int(first_b) <= int(last_a)):
        return True
    elif (int(first_b) <= int(first_a) and
          int(first_a) <= int(last_b)):
        return True
    else:
        return False


def chkptfile_in_pagerange(dfname, chkpt_dfname):
    """
    return False if both files are checkpoint files (with page ranges)
    and the second file page range does not overlap with the first one

    args: DumpFilename, checkpoint file DumpFilename
    """
    # one or both are not both checkpoint files, default to 'true'
    if not dfname.is_checkpoint_file or not chkpt_dfname.is_checkpoint_file:
        return True

    if not dfname.last_page_id or not chkpt_dfname.last_page_id:
        # one or both end values are missing:
        return compare_partial_ranges(dfname.first_page_id, dfname.last_page_id,
                                      chkpt_dfname.first_page_id, chkpt_dfname.last_page_id)
    else:
        # have end values for both files:
        return compare_full_ranges(dfname.first_page_id, dfname.last_page_id,
                                   chkpt_dfname.first_page_id, chkpt_dfname.last_page_id)


def get_pagerange_missing_before(needed_range, have_range):
    """
    given range of numbers needed and range of numbers we have,
    return range of numbers needed before first number we have,
    or None if none
    args:
        tuple (startpage, endpage, partnum) needed,
        tuple (startpage, endpage, partnum) already have,
    """
    if have_range is None:
        return needed_range
    elif needed_range is None or int(have_range[0]) <= int(needed_range[0]):
        return None
    else:
        return (needed_range[0], str(int(have_range[0]) - 1), needed_range[2])


def find_missing_pageranges(needed, have):
    """
    given list tuples of ranges of numbers needed, and ranges of numbers we have,
    determine the ranges of numbers missing and return list of said tuples
    args:
        sorted asc list of tuples (startpage<str>, endpage<str>, partnum<str>) needed,
        sorted asc list of tuples (startpage<str>, endpage<str>, partnum<str>) already have,
    returns: list of (startpage<str>, endpage<str>, partnum<str>)

    """
    needed_index = 0
    have_index = 0
    missing = []

    if not needed:
        return missing
    if not have:
        return needed

    needed_range = needed[needed_index]
    have_range = have[have_index]

    while True:
        # if we're out of haves, append everything we need
        if have_range is None:
            missing.append(needed_range)
            needed_index += 1
            if needed_index < len(needed):
                needed_range = needed[needed_index]
            else:
                # end of needed. done
                return missing

        before_have = get_pagerange_missing_before(needed_range, have_range)

        # write anything we don't have
        if before_have is not None:
            missing.append(before_have)

        # if we haven't already exhausted all the ranges we have...
        if have_range is not None:
            # skip over the current range of what we have
            skip_up_to = str(int(have_range[1]) + 1)
            while int(needed_range[1]) < int(skip_up_to):
                needed_index += 1
                if needed_index < len(needed):
                    needed_range = needed[needed_index]
                else:
                    # end of needed. done
                    return missing

            if int(needed_range[0]) < int(skip_up_to):
                needed_range = (skip_up_to, needed_range[1], needed_range[2])

            # get the next range we have
            have_index += 1
            if have_index < len(have):
                have_range = have[have_index]
            else:
                have_range = None

    return missing


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
        [--pagestart <int>] [--pageend <int>]
        [--pad <int>] [--json]
        [--configfile <path>] [--verbose] [--help]

--wiki       (-w):  name of db of wiki for which to run
--jobs       (-j):  generate page ranges for this number of jobs
--revs       (-r):  generate page ranges for this number of
                    revisions per interval
--pagestart  (-s):  page id start for --revs option, default 1
--pageend    (-e):  page id end for --revs option, default last
--configfile (-c):  path to config file
--pad        (-p):  pad numbers out to specified length with
                    leading zeros for json format
--json       (-J):  write results as json formatted output
--verbose    (-v):  display messages about what the script is doing
--help       (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def check_args(remainder, wiki, revs, jobs):
    """
    whine if there are command line arg problems,
    and exit
    """
    if len(remainder) > 0:
        usage("Unknown option specified")

    if wiki is None:
        usage("Mandatory option 'wiki' is not specified")
    if (revs is None and jobs is None) or (revs and jobs):
        usage("Exactly one of 'revs' or 'jobs' must be specified")


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
            print json.dumps(jsonify(ranges, pad))
            print json.dumps(pages_per_job, pad)
        else:
            print "for {jobs} jobs, have ranges:".format(jobs=range_opts['jobs'])
            print ranges
            print "for {jobs} jobs, have config setting:".format(jobs=range_opts['jobs'])
            print pages_per_job
    else:
        ranges = prange.get_pageranges_for_revs(range_opts['start'], range_opts['end'],
                                                range_opts['revs'])
        if jsonfmt:
            print json.dumps(jsonify(ranges, pad))
        else:
            print ("for start", range_opts['start'], "and end",
                   range_opts['end'] if range_opts['end'] is not None else "last page")
            print "for {revs} revs, have ranges:".format(revs=range_opts['revs'])
            print ranges


def do_main():
    """
    main entry point
    """
    range_opts = {'jobs': None, 'revs': None, 'start': None, 'end': None}
    wiki = None
    configpath = "wikidump.conf"
    jsonfmt = False
    verbose = False
    pad = 0
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:w:j:s:e:r:p:vh",
                                                 ["configfile=", "wiki=", "jobs=",
                                                  "pagestart=", "pageend=", "revs=",
                                                  "pad=", "json", "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configpath = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-j", "--jobs"]:
            range_opts['jobs'] = val
        elif opt in ["-r", "--revs"]:
            range_opts['revs'] = val
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
                                      'end': 'pageend', 'start': 'pagestart'})
    check_args(remainder, wiki, range_opts['revs'], range_opts['jobs'])

    prange = PageRange(QueryRunner(wiki, Config(configpath), verbose), verbose)
    do_pageranges(prange, range_opts, pad, jsonfmt)


if __name__ == "__main__":
    do_main()
