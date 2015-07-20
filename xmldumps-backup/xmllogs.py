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
import worker
import WikiDump
import getopt
from xmlstreams import run_script, catfile, gzippit, get_max_id, do_xml_piece, do_xml_stream


def dologsbackup(wikidb, outfile,
                 wikiconf, force_normal, start, end, dryrun):
    '''
    do a logs xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into gzip's stdin for the
    concatenated compressed output
    '''
    outfiles = {'logs': {'name': outfile}}
    for filetype in outfiles:
        outfiles[filetype]['temp'] = os.path.join(wikiconf.tempDir, os.path.basename(outfiles[filetype]['name']) + "_tmp")
        if dryrun:
            outfiles[filetype]['compr'] = None
        else:
            outfiles[filetype]['compr'] = gzippit(outfiles[filetype]['name'])

    script_command = worker.MultiVersion.MWScriptAsArray(wikiconf, "dumpBackup.php")
    command = [wikiconf.php, "-q"] + script_command

    command.extend(["--wiki=%s" % wikidb,
                    "--logs", "--report=1000",
                    "--output=file:%s" % outfiles['logs']['temp']
                    ])
    if force_normal is not None:
        command.append("--force-normal")

    do_xml_stream(wikidb, outfiles, command, wikiconf, force_normal,
                  start, end, dryrun, 'log_id', 'logging',
                  50000, 100000, '</logitem>\n')


def usage(message=None):
    """
    display a helpful usage message with
    an optional introductory message first
    """
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: xmllogs.py --wiki wikidbname --outfile path
    [--start number] [--end number]
    [--force-normal bool] [--config path]

Options:

  --wiki (-w):         wiki db name, e.g. enwiki
  --outfile (-o):      full path to xml logs dump that will be created

  --start (-s):        starting log id to dump (default: 1)
  --end (-e):          ending log id to dump, exclusive of this entry (default: dump all)

  --force-normal (-f): if set, this argument will be passed through to dumpBackup.php
                       (default: unset)
  --config (-C):       path to wikidump configfile (default: "wikidump.conf" in current dir)
  --dryrun (-d):       display the commands that would be run to produce the output but
                       don't actually run them
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    'main entry point, does all the work'
    wiki = None
    output_file = None
    start = None
    end = None
    force_normal = False
    configfile = "wikidump.conf"
    dryrun = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:o:s:e:C:fhv",
            ["wiki=", "outfile=",
             "start=", "end=", "config=", "force-normal",
             "help", "dryrun"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))
    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-o", "--outfile"]:
            output_file = val
        elif opt in ["-s", "--start"]:
            start = val
        elif opt in ["-e", "--end"]:
            end = val
        elif opt in ["-f", "--force-normal"]:
            force_normal = True
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
    if output_file is None:
        usage("mandatory argument argument missing: --output")

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

    wikiconf = WikiDump.Config(configfile)
    wikiconf.parseConfFilePerProject(wiki)
    dologsbackup(wiki, output_file, wikiconf, force_normal, start, end, dryrun)

if __name__ == '__main__':
    main()
