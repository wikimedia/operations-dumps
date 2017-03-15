"""
get a lock on the specified wiki and date,
touch it every so often so it doesn't get stale,
listen for a hup and if there is one, remove
the lock and go home.
this is designed for use be shell scripts
that run standalone sets of jobs and get the
lock at the beginning, releaseing it for the end.
"""


import sys
import signal
import getopt
import time
from dumps.WikiDump import Locker
from dumps.WikiDump import Config
from dumps.WikiDump import Wiki


class StandaloneLocker(object):
    """
    lock a wiki run for a given date
    return True if lock acquired, False otherwise
    on HUP, removes lock and exits with 0
    """
    def __init__(self, wikiname, configfile, date):
        self.wiki = Wiki(Config(configfile), wikiname)
        self.wiki.set_date(date)
        self.locker = None

    def get_lock(self):
        """
        get the lock on the specified wiki
        or return False;
        this also automatically starts up
        a watchdog that touches the lock file
        every so often.
        """
        try:
            locker = Locker(self.wiki, self.wiki.date)
            locker.lock()
            self.locker = locker
            return True
        except Exception as ex:
            return False

    def handle_hup(self, signo, dummy_frame):
        """
        ignore any more hups
        stop the lock refresher and remove the lock
        go bye bye
        """
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        lockfiles = self.locker.is_locked()
        self.locker.unlock(lockfiles, owner=True)
        sys.exit(0)


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: dump_lock.py --wiki <wikiname> --date <YYYYMMDD>
        --configfile <path> [--verbose] [--help]

--wiki       (-w):  name of db of wiki for which to get lock
--date       (-j):  date of run to lock
--configfile (-c):  path to config file
--verbose    (-v):  display messages about what the script is doing
--help       (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_args():
    """
    get and validate args, and return them
    """
    wiki = None
    date = None
    configfile = None
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:w:d:vh", ["configfile=", "wiki=", "date=",
                                       "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-j", "--date"]:
            date = val
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if not date.isdigit or len(date) != 8:
        usage("'date' must be in format YYYYMMDD")

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not wiki or not date or not configfile:
        usage("Arguments 'wiki', 'date' and 'configfile' must be set")
    return(wiki, date, configfile, verbose)


def do_main():
    """entry point:
    get args, try to lock, nonzero exit code on failure
    """
    wiki, date, configfile, verbose = get_args()

    stlocker = StandaloneLocker(wiki, configfile, date)
    signal.signal(signal.SIGHUP, stlocker.handle_hup)
    if not stlocker.get_lock():
        if verbose:
            sys.stderr.write("Failed to get lock for {wiki}, {date}\n".format(
                wiki=wiki, date=date))
        sys.exit(1)

    if verbose:
        sys.stderr.write("Lock acquired for {wiki}, {date}\n".format(
            wiki=wiki, date=date))

    # sit here mostly lazing around until HUP time
    while True:
        time.sleep(60)


if __name__ == '__main__':
    do_main()
