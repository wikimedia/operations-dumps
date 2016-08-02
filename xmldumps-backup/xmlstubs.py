'''
generate an xml dump via multiple runs of a php script instead of one
long run.

avoids memory leak issues, permits retries when a single run fails,
recovery if db servers go away in the middle of a run by retrying
the run.
'''

import os
import sys
import time
from dumps.WikiDump import Config
from dumps.utils import MultiVersion
from dumps.utils import DbServerInfo
import getopt
from xmlstreams import gzippit, do_xml_stream


def get_revs_per_page_interval(page_id_start, interval, wiki, db_info):
    '''
    given page id start and the number of pages, get
    and return total number of revisions these pages have

    wiki is a Wiki object for the specific wiki
    db_info is a DbServerInfo object for the specific wiki
    '''

    query = ("select COUNT(rev_id) from revision where "
             "rev_page >= %s and rev_page < %s;" % (
                 page_id_start, page_id_start + interval))
    results = None
    retries = 0
    maxretries = 5
    end = 0
    results = db_info.run_sql_and_get_output(query)
    if results:
        lines = results.splitlines()
        if lines and lines[1]:
            if not lines[1].isdigit():
                return 0   # probably NULL or missing table
            end = int(lines[1])
            return end

    while results is None and retries < maxretries:
        retries = retries + 1
        time.sleep(5)
        # maybe the server was depooled. if so we will get another one
        db_info = DbServerInfo(wiki, wiki.db_name)
        results = db_info.run_sql_and_get_output(query)
        if not results:
            continue
        lines = results.splitlines()
        if lines and lines[1]:
            end = int(lines[1])
            break

    if not end:
        sys.stderr.write("failed to get revision count for page range from db, exiting\n")
        sys.exit(1)
    else:
        return end


def get_page_interval(page_id_start, interval_guess, wiki, db_info):
    '''
    given a starting page id, estimate page range ('interval') such that
    the following query will not take a ridiculously long time:

      SELECT * FROM revision JOIN page ON rev_page=page_id WHERE
      rev_page >= page_id_start and rev_page < page_id_start + interval
      ORDER BY rev_page, rev_id

    then return this interval.

    see phabricator bug T29112 for more on this horrible thing
    '''
    current_interval = interval_guess
    min_interval = wiki.config.stubs_minpages
    max_revs = wiki.config.stubs_maxrevs

    while current_interval > min_interval:
        now = time.time()
        num_revs_for_interval = get_revs_per_page_interval(
            page_id_start, current_interval, wiki, db_info)
        now2 = time.time()
        # if getting the rev count takes too long, cut back
        if now2 - now > 60:
            current_interval = current_interval / 2
        # if we get more than some abs number of revs, scale back accordingly
        elif num_revs_for_interval > max_revs:
            current_interval = current_interval / ((num_revs_for_interval / max_revs) + 1)
        else:
            break
    if current_interval < min_interval:
        current_interval = min_interval
    return current_interval


def dostubsbackup(wikidb, history_file, current_file, articles_file,
                  wikiconf, start, end, dryrun):
    '''
    do a stubs xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into gzip's stdin for the
    concatenated compressed output
    '''
    outfiles = {'history': {'name': history_file},
                'current': {'name': current_file},
                'articles': {'name': articles_file}}
    for filetype in outfiles:
        outfiles[filetype]['temp'] = os.path.join(
            wikiconf.temp_dir, os.path.basename(outfiles[filetype]['name']) + "_tmp")
        if dryrun:
            outfiles[filetype]['compr'] = None
        else:
            outfiles[filetype]['compr'] = gzippit(outfiles[filetype]['name'])

    script_command = MultiVersion.mw_script_as_array(wikiconf, "dumpBackup.php")
    command = [wikiconf.php] + script_command

    command.extend(["--wiki=%s" % wikidb,
                    "--full", "--stub", "--report=1000",
                    "--output=file:%s" % outfiles['history']['temp'],
                    "--output=file:%s" % outfiles['current']['temp'],
                    "--filter=latest",
                    "--output=file:%s" % outfiles['articles']['temp'],
                    "--filter=latest", "--filter=notalk",
                    "--filter=namespace:!NS_USER"])

    if wikiconf.stubs_orderrevs:
        command.append("--orderrevs")
        callback = get_page_interval
    else:
        callback = None

    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  5000, 100000, '</page>\n', callback)


def usage(message=None):
    """
    display a helpful usage message with
    an optional introductory message first
    """
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: xmlstubs.py --wiki wikidbname --articles path --current path
    --history path [--start number] [--end number]
    [--config path]

Options:

  --wiki (-w):         wiki db name, e.g. enwiki
  --articles (-a):     full path of articles xml stub dump that will be created
  --current (-c):      full path of current pages xml stub dump that will be created
  --history (-h):      full path of xml stub dump with full history that will be created

  --start (-s):        starting page to dump (default: 1)
  --end (-e):          ending page to dump, exclusive of this page (default: dump all)

  --config (-C):       path to wikidump configfile (default: "wikidump.conf" in current dir)
  --dryrun (-d):       display the commands that would be run to produce the output but
                       don't actually run them
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    'main entry point, does all the work'
    wiki = None
    articles_file = None
    current_file = None
    history_file = None
    start = None
    end = None
    dryrun = False
    configfile = "wikidump.conf"

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:a:c:h:s:e:C:fhd",
            ["wiki=", "articles=", "current=", "history=",
             "start=", "end=", "config=",
             "help", "dryrun"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))
    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-a", "--articles"]:
            articles_file = val
        elif opt in ["-c", "--current"]:
            current_file = val
        elif opt in ["-h", "--history"]:
            history_file = val
        elif opt in ["-s", "--start"]:
            start = val
        elif opt in ["-e", "--end"]:
            end = val
        elif opt in ["-C", "--config"]:
            configfile = val
        elif opt in ["-d", "--dryrun"]:
            dryrun = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if wiki is None:
        usage("mandatory argument argument missing: --wiki")
    if articles_file is None:
        usage("mandatory argument argument missing: --articles")
    if current_file is None:
        usage("mandatory argument argument missing: --current")
    if history_file is None:
        usage("mandatory argument argument missing: --history")

    if start is not None:
        if not start.isdigit():
            usage("value for --start must be a number")
        else:
            start = int(start)

    if end is not None:
        if not end.isdigit():
            usage("value for --end must be a number")
        else:
            end = int(end) - 1

    if not os.path.exists(configfile):
        usage("no such file found: " + configfile)

    wikiconf = Config(configfile)
    wikiconf.parse_conffile_per_project(wiki)
    dostubsbackup(wiki, history_file, current_file, articles_file, wikiconf, start, end, dryrun)

if __name__ == '__main__':
    main()
