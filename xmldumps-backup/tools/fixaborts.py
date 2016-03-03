'''
In case new directorys are not created for
a dump run and the run tries to update the previous
(complete) run but breaks because bugs, all status files
will be left with status aborted run and a wrong date.

This script fixes all those up en masse.
'''
import os
import sys
import getopt

def usage(message=None):
    '''
    display a helpful usage message
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: fixaborts.py --dumpdir <path> --wikilist <path>
                    [--dryrun] [--verbose] [--help]

    dumpdir  (-d)  path to the directory under which all wiki dumps are
                   generated and stored by wikiname/date
    wikilist (-w)  path to file of wikis to check, one wiki per line

    dryrun      (-D) don't do it but show what would be done
    verbose     (-v) print progress messages
    help        (-h) show this message

Example: python fixaborts.py --dumpdir /mnt/data/xmldatadumps/private
              --wikilist /srv/mediawiki/dblists/all.dblist  --verbose
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_options(arglist):
    '''
    process, check and return arguments for this script
    '''
    dumpdir = None
    wikilist = None
    dryrun = False
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(
            arglist, "d:w:Dvh",
            ["dumpdir=", "wikilist=", "dryrun", "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-d", "--dumpdir"]:
            dumpdir = val
        elif opt in ["-w", "--wikilist"]:
            wikilist = val
        elif opt in ["-D", "--dryrun"]:
            dryrun = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    if dumpdir is None:
        usage("The mandatory argument 'dumpdir' was not specified.")
    if wikilist is None:
        usage("The mandatory argument 'wikilist' was not specified.")
    return(dumpdir, wikilist, dryrun, verbose)


def get_status_filepath(wiki, dumpdir, date):
    'return full path to status html file for dump run'
    return os.path.join(dumpdir, wiki, date, "status.html")


def get_runinfo_filepath(wiki, dumpdir, date):
    'return full path to dump run info file for dump run'
    return os.path.join(dumpdir, wiki, date, "dumpruninfo.txt")


def get_date_from_entry(entry):
    'given line from dump run info file, get the date out of it'
    # name:articlesmultistreamdump; status:done; updated:2016-02-07 12:50:11
    fields = entry.split(';')
    if not fields[2].startswith(' updated:'):
        return None
    date = fields[2][9:].strip()
    if not date:
        return None

    return date


def get_file_contents(path):
    'get file contents as one blob, from file'
    try:
        # fixme does this close automagically later?
        contents = open(path, "r").read()
        return contents
    except:
        return None


def get_entries_from_file(path):
    '''
    get contents from a file and split into lines,
    returning those lines
    '''
    contents = get_file_contents(path)
    if contents is None:
        return None

    return contents.splitlines()


def check_wiki_run_completed(wiki, dumpdir, date):
    '''
    for the given wiki and the dump run on the given date,
    see if according to the dump run info file all jobs
    were completed
    '''
    runinfofile = get_runinfo_filepath(wiki, dumpdir, date)
    entries = get_entries_from_file(runinfofile)
    if entries is None:
        return False

    for entry in entries:
        if 'status:done' not in entry:
            return False

    return True


def check_wiki_status_aborted(wiki, dumpdir, date):
    '''
    for the given wiki and dump run date, see if
    according to the dump run status html file,
    the dump was 'aborted'
    '''
    statusfile = get_status_filepath(wiki, dumpdir, date)
    if not os.path.exists(statusfile):
        return False
    contents = get_file_contents(statusfile)
    if contents is None:
        return False
    if 'aborted' not in contents:
        return False

    return True


def update_status(wiki, dumpdir, date, new_status_date):
    '''
    given new date and time info to go into dump run status html
    file, create a new status file with that date and time and
    a status of 'done'
    '''
    # <li>2016-02-13 02:55:36 <a href="enwiktionary/20160203">enwiktionary</a>: <span class='done'>Dump complete</span></li>
    # <li>2016-03-03 03:12:33 <a href="avwiktionary/20160203">avwiktionary</a>: <span class="failed">dump aborted</span></li>
    statusfile = get_status_filepath(wiki, dumpdir, date)
    new_content_format = ('<li>{0} <a href="{1}/{2}">{1}</a>: '
                          '<span class="done">Dump complete</span></li>\n')
    filedesc = open(statusfile, "w")
    filedesc.write(new_content_format.format(new_status_date, wiki, date))
    filedesc.close()


def fix_wiki_status(wiki, dumpdir, date, dryrun, verbose):
    '''
    find the most recent job run for the specified wiki on the
    given dump run date, grab the date and time of that job run,
    write out a new status html file for that run with the date
    and time just grabbed and 'done' for the status
    '''
    if dryrun:
        print("would fix status line for %s for date %s"
              % (wiki, date))
    elif verbose:
        print("fixing status line for %s for date %s"
              % (wiki, date))
    runinfofile = get_runinfo_filepath(wiki, dumpdir, date)
    if not os.path.exists(runinfofile):
        if verbose:
            print "no runinfo file, skipping that"
        return False
    entries = get_entries_from_file(runinfofile)
    if entries is None:
        if verbose:
            print "no entries found in runinfo file, skipping that"
        return False

    most_recent_job = None
    for entry in entries:
        entry_date = get_date_from_entry(entry)
        if entry_date is None:
            continue
        if (most_recent_job is None or
                entry_date > most_recent_job):
            most_recent_job = entry_date

    if dryrun:
        print("would fix status line for %s for date %s with %s"
              % (wiki, date, most_recent_job))
        return True

    if verbose:
        print("fixing status line for %s for date %s with %s"
              % (wiki, date, most_recent_job))

    update_status(wiki, dumpdir, date, most_recent_job)
    return True


def get_latest_date(wiki, dumpdir):
    '''
    return date of the most recent dump run,
    based on name of the dir (date), not on
    access times or any of that
    '''
    try:
        dates = os.listdir(os.path.join(dumpdir, wiki))
    except:
        return None
    if not dates:
        return None
    dates = [date for date in dates if date.isdigit()]
    return sorted(dates)[-1]


def fix_wiki(wiki, dumpdir, dryrun, verbose):
    date = get_latest_date(wiki, dumpdir)
    if date is None:
        return
    if (check_wiki_status_aborted(wiki, dumpdir, date) and
            check_wiki_run_completed(wiki, dumpdir, date)):
        fix_wiki_status(wiki, dumpdir, date, dryrun, verbose)
    elif verbose:
        print "skipping this wiki/date: %s/%s" % (wiki, date)
    return


def do_main():
    'main entry point, does all the work'

    dumpdir, wikilist, dryrun, verbose = get_options(sys.argv[1:])

    wikis = get_entries_from_file(wikilist)
    for wiki in wikis:
        fix_wiki(wiki, dumpdir, dryrun, verbose)


if __name__ == '__main__':
    do_main()
