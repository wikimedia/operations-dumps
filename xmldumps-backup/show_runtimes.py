import os
import sys
import getopt
import operator
import time


class RuntimeRetriever(object):
    '''
    manage retrieval of runtimes for dumps of wikis
    '''
    def __init__(self, dumpsdir, jobs, rundate):
        self.dumpsdir = dumpsdir
        self.rundate = rundate
        jobs_known = RuntimeRetriever.get_jobs_todo()
        if jobs is None:
            self.jobs_todo = jobs_known
        else:
            self.jobs_todo = RuntimeRetriever.get_desired_jobs(jobs, jobs_known)

    @staticmethod
    def get_desired_jobs(jobs_wanted, jobs_known):
        types_known = list(set([job['type'] for job in jobs_known if 'type' in job]))
        to_return = []
        for job in jobs_wanted:
            wanted_basename = job
            wanted_type = None
            for jobtype in types_known:
                # convert wanted job arg to our known job format
                if job.endswith(jobtype):
                    wanted_basename = job.rsplit('-', 1)[0]
                    wanted_type = jobtype
                    break
            for entry in jobs_known:
                # see if the requested job is this one from our known list
                if (entry['name'] == wanted_basename and
                        ((wanted_type is None and 'type' not in entry) or
                         ('type' in entry and wanted_type == entry['type']))):
                    to_return.append(entry)
                    break
        return to_return

    @staticmethod
    def get_jobs_todo():
        jobs = []
        jobs.append({'name': 'meta-history', 'type': '7z'})
        jobs.append({'name': 'meta-history', 'type': 'bz2'})
        jobs.append({'name': 'meta-current'})
        jobs.append({'name': 'multistream'})
        jobs.append({'name': 'articles', 'type': 'bz2', 'exclude': 'multistream'})
        jobs.append({'name': 'abstract'})
        jobs.append({'name': 'logging'})
        jobs.append({'name': 'stub'})
        return jobs

    @staticmethod
    def file_is_from_job(filename, job):
        return (job['name'] in filename and
                ('type' not in job or job['type'] in filename) and
                ('exclude' not in job or job['exclude'] not in filename))

    @staticmethod
    def get_job_runtime(sorted_dumpfiles, job, dumpfile_info):
        runtime = {}
        filenames = [entry[0] for entry in sorted_dumpfiles
                     if RuntimeRetriever.file_is_from_job(entry[0], job)]
        if not filenames:
            return runtime
        runtime['end'] = dumpfile_info[filenames[0]]
        earliest_file = filenames[-1]
        sorted_dumpfiles_filenames = [dumpfile[0] for dumpfile in sorted_dumpfiles]
        index = sorted_dumpfiles_filenames.index(earliest_file)
        if index == len(sorted_dumpfiles_filenames) - 1:
            runtime['start'] = dumpfile_info[earliest_file]
        else:
            runtime['start'] = dumpfile_info[sorted_dumpfiles_filenames[index + 1]]
        return runtime

    @staticmethod
    def make_job_string(job):
        jobstring = job['name']
        if 'type' in job:
            jobstring = jobstring + '-' + job['type']
        return jobstring

    def get_latest_rundate(self, wikiname):
        path = os.path.join(self.dumpsdir, wikiname)
        dirs = os.listdir(path)
        dirdates = sorted([dirname for dirname in dirs if dirname.isdigit() and len(dirname) == 8])
        if dirdates:
            return dirdates[-1]
        else:
            return None

    def get_runtimes(self, wikiname):
        '''
        for a given dump directory tree and a specified wiki,
        get and return a dict of times it took each dump job to run
        (well, certain ones, we don't really care about the tables)
        '''
        runtimes = {}

        if not os.path.exists(os.path.join(self.dumpsdir, wikiname)):
            return {}

        if self.rundate is None:
            rundate = self.get_latest_rundate(wikiname)
            if rundate is None:
                return {}
        else:
            rundate = self.rundate

        rundir = os.path.join(self.dumpsdir, wikiname, rundate)
        dumpfiles = os.listdir(rundir)
        dumpfile_info = {}
        for dumpfile in dumpfiles:
            dumpfile_info[dumpfile] = os.stat(os.path.join(rundir, dumpfile)).st_mtime
        sorted_dumpfiles = sorted(dumpfile_info.items(), key=operator.itemgetter(1), reverse=True)
        for job in self.jobs_todo:
            runtimes[RuntimeRetriever.make_job_string(job)] = RuntimeRetriever.get_job_runtime(
                sorted_dumpfiles, job, dumpfile_info)
        return runtimes


def read_wikilist(wikilist):
    '''
    read entries from a file, returning them as a list, skipping
    blank lines and comments
    '''
    contents = open(wikilist).read().splitlines()
    return [line for line in contents if line and not line.startswith('#')]


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: show-runtimes.py --dumpsdir path --wikilist path|--wikinames string
                        [--slowjobs num] [--jobs name,name...] [--rundate YYYYMMDD]
       show-runtimes.py --help

This script shows the runtimes for each dump job for the most current run
of the specified wiki or list of wikis, by checking timestamps on filenames
associated with each dump step.

Note that if a job was interrupted during a run, and other jobs ran before
a retry picked up and completed the particular step, these times will not
be accurate.

Arguments:
  --dumpsdir  (-d):  path to dumps directory tree
  --wikilist  (-w):  path to file listing all wikinames as they appear in dumps
                     dir tree; if this is specified, wikiname arg must not be
                     provided
  --wikinames (-W):  comma-separated list of names of wiki to check; if this is specified,
                     wikilist must not be provided
  --jobs      (-j):  comma-separated list of known jobnames to check; if none are specified
                     the full list will be checked. The full list is:
                         meta-history-7z, meta-history-bz2, meta-current, multistream,
                         articles-bz2, abstract, logging, stub
  --slowjobs  (-s):  show only this many slowest entries for each job
  --rundate   (-r):  date in YYYYMMDD format of dump run to check
  --help      (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def check_args(dumpsdir, wikilist, wikinames, remainder):
    if dumpsdir is None:
        usage("Mandatory argument 'dumpsdir' is missing")
    if wikilist is None and wikinames is None:
        usage("One of the options 'wikilist' or 'wikinames' must be specified")

    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])


def get_args():
    """
    read, parse and return command line options
    """
    dumpsdir = None
    jobs = None
    slowjobs = None
    rundate = None
    wikilist = None
    wikinames = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "d:j:r:s:w:W:h", [
                "dumpsdir=", "jobs=", "rundate=", "slowjobs=", "wikilist=", "wikinames=",
                "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-d", "--dumpsdir"]:
            dumpsdir = val
        elif opt in ["-j", "--jobs"]:
            jobs = val
        elif opt in ["-r", "--rundate"]:
            rundate = val
        elif opt in ["-w", "--wikilist"]:
            wikilist = val
        elif opt in ["-W", "--wikinames"]:
            wikinames = val
        elif opt in ["-s", "--slowjobs"]:
            if not val.isdigit:
                usage("The slowjobs option requires a number")
            slowjobs = int(val)
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    check_args(dumpsdir, wikilist, wikinames, remainder)

    return dumpsdir, wikilist, wikinames, slowjobs, jobs, rundate


def get_nicetime(timestamp):
    try:
        return time.strftime("%Y-%m-%d %H:%M", time.gmtime(timestamp))
    except Exception:
        return "Unknown"


def display_job_runtimes(runtimes, slowjobs):
    if not runtimes:
        print "No job runtimes available."
        return

    # all the wikis have the same jobs listed, so we look at any entry to get the list
    some_entry = runtimes.itervalues().next()
    jobs = sorted(some_entry.keys())
    jobinfo = []
    for job in jobs:
        for wikiname in runtimes:
            if job in runtimes[wikiname]:
                jobinfo.append((wikiname, get_job_info_printable(runtimes[wikiname][job], wikiname),
                                get_job_duration(runtimes[wikiname][job])))
        jobinfo_sorted = sorted(jobinfo, key=lambda entry: entry[2], reverse=True)
        print "Job:", job
        for entry in jobinfo_sorted[0:slowjobs]:
            print "  ", entry[1]


def get_job_duration(runtime):
    if 'start' in runtime and 'end' in runtime:
        interval = runtime['end'] - runtime['start']
    else:
        interval = -1
    return interval


def get_job_info_printable(runtime, wikiname):
    start = runtime['start'] if 'start' in runtime else 'Unknown'
    end = runtime['end'] if 'end' in runtime else 'Unknown'
    if 'start' in runtime and 'end' in runtime:
        interval = end - start
        hours = int(interval / 3600)
        mins = int(interval - (hours * 3600)) / 60
        return ("   Wiki: {wiki:<20} Duration: {hours}h, {mins}m,"
                " Start: {s_mtime:.0f} ({s_nicetime}),"
                " End: {e_mtime:.0f} ({e_nicetime})".format(
                    wiki=wikiname,
                    hours=hours, mins=mins,
                    s_mtime=start, s_nicetime=get_nicetime(start),
                    e_mtime=end, e_nicetime=get_nicetime(end)))
    else:
        return ("   Wiki: {wiki:<20} Duration: Unknown, Start: Unknown, End: Unknown".format(
            wiki=wikiname))


def display_runtimes(runtimes, wikiname):
    print "Wiki:", wikiname
    for job in sorted(runtimes.keys()):
        runtime = runtimes[job]
        start = runtime['start'] if 'start' in runtime else 'Unknown'
        end = runtime['end'] if 'end' in runtime else 'Unknown'
        if 'start' in runtime and 'end' in runtime:
            interval = end - start
            hours = int(interval / 3600)
            mins = int(interval - (hours * 3600)) / 60
            print ("   Job: {job:<20} Start: {s_mtime:.0f} ({s_nicetime}),"
                   " End: {e_mtime:.0f} ({e_nicetime}), Duration: {hours}h, {mins}m".format(
                       job=job,
                       s_mtime=start, s_nicetime=get_nicetime(start),
                       e_mtime=end, e_nicetime=get_nicetime(end),
                       hours=hours, mins=mins))
        else:
            print "   Job: {job:<20} Start: Unknown, End: Unknown, Duration: Unknown".format(
                job=job)


def do_main():
    dumpsdir, wikilist, wikinames, slowjobs, jobs, rundate = get_args()
    if wikinames is not None:
        wikis_todo = wikinames.split(',')
    else:
        wikis_todo = read_wikilist(wikilist)
    jobs_todo = None
    if jobs is not None:
        jobs_todo = jobs.split(',')

    retriever = RuntimeRetriever(dumpsdir, jobs_todo, rundate)
    runtimes = {}
    for wikiname in wikis_todo:
        runtimes[wikiname] = retriever.get_runtimes(wikiname)

    if slowjobs:
        display_job_runtimes(runtimes, slowjobs)
    else:
        for wikiname in wikis_todo:
            display_runtimes(runtimes[wikiname], wikiname)


if __name__ == '__main__':
    do_main()
