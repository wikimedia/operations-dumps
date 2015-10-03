'''
generate an xml dump via multiple runs of a php script instead of one
long run.

avoids memory leak issues, permits retries when a single run fails,
recovery if db servers go away in the middle of a run by retrying
the run.
'''

import os
import sys
from dumps.WikiDump import Config
from dumps.utils import MultiVersion
import getopt
from xmlstreams import gzippit, do_xml_stream


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
            wikiconf.tempDir, os.path.basename(outfiles[filetype]['name']) + "_tmp")
        if dryrun:
            outfiles[filetype]['compr'] = None
        else:
            outfiles[filetype]['compr'] = gzippit(outfiles[filetype]['name'])

    script_command = MultiVersion.mw_script_as_array(wikiconf, "dumpBackup.php")
    command = [wikiconf.php, "-q"] + script_command

    command.extend(["--wiki=%s" % wikidb,
                    "--full", "--stub", "--report=10000",
                    "--output=file:%s" % outfiles['history']['temp'],
                    "--output=file:%s" % outfiles['current']['temp'],
                    "--filter=latest",
                    "--output=file:%s" % outfiles['articles']['temp'],
                    "--filter=latest", "--filter=notalk",
                    "--filter=namespace:!NS_USER"
                    ])

    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  100000, 500000, '</page>\n')


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
    wikiconf.parseConfFilePerProject(wiki)
    dostubsbackup(wiki, history_file, current_file, articles_file, wikiconf, start, end, dryrun)

if __name__ == '__main__':
    main()
