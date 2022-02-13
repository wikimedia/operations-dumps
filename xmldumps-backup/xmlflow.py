#!/usr/bin/python3
'''
generate an xml dump via multiple runs of a php script instead of one
long run.

avoids memory leak issues, permits retries when a single run fails,
recovery if db servers go away in the middle of a run by retrying
the run.
'''

import os
import sys
import getopt
from dumps.wikidump import Config
from dumps.fileutils import FileUtils
from dumps.utils import MultiVersion
from xmlstreams import do_xml_stream, bzip2it_append


def do_flowbackup(wikidb, outfile, history,
                  wikiconf, start, end, dryrun, verbose):
    '''
    do an flow xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into bzip2's stdin for the
    concatenated compressed output
    '''
    outfiles = {'flow': {'name': outfile}}

    for filetype in outfiles:
        outfiles[filetype]['temp'] = os.path.join(
            FileUtils.wiki_tempdir(wikidb, wikiconf.temp_dir),
            os.path.basename(outfiles[filetype]['name']) + "_tmp")
        if dryrun:
            outfiles[filetype]['compr'] = [None, outfiles[filetype]['name']]
        else:
            outfiles[filetype]['compr'] = [bzip2it_append, outfiles[filetype]['name']]

    script_command = MultiVersion.mw_script_as_array(wikiconf,
                                                     "extensions/Flow/maintenance/dumpBackup.php")
    command = [wikiconf.php] + script_command

    command.extend(["--wiki=%s" % wikidb,
                    "--output=file:%s" % outfiles['flow']['temp'],
                    "--current", "--report=1000"])

    if history:
        command.append("--full")

    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  5000, 10000, '</board>\n', verbose=verbose, header=True)
    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  5000, 10000, '</board>\n', verbose=verbose)
    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  5000, 10000, '</board>\n', verbose=verbose, footer=True)


# fixme must take a list of ouput files and a list of
# variants so we can put together the correct command

def usage(message=None):
    """
    display a helpful usage message with
    an optional introductory message first
    """
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: xmlflow.py --wiki wikidbname --outfile path
    [--start number] [--end number]
    [--config path[:overrides_section]] [--dryrun] [--verbose]

Options:

  --wiki     (-w):     wiki db name, e.g. enwiki
  --outfile  (-o):     full path to xml flow dumps that will be created
  --start    (-s):     starting page id to dump (default: 1)
  --end      (-e):     ending page id to dump, exclusive of this page (default: dump all)

  --config   (-C):     path to wikidump configfile (default: "wikidump.conf" in current dir)
                       if followed by : and a name, this section name in the config file
                       will be used to override config settings in default sections
  --dryrun   (-d):     display the commands that would be run to produce the output but
                       don't actually run them
  --verbose  (-v):     write various progress messages
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    'main entry point, does all the work'
    wiki = None
    output_file = None
    history = False
    start = None
    end = None
    configfile = "wikidump.conf"
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:o:s:e:C:Hfhv",
            ["wiki=", "outfile=",
             "start=", "end=", "config=",
             "history", "help", "dryrun", "verbose"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))
    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-o", "--outfile"]:
            output_file = val
        elif opt in ["-H", "--history"]:
            history = True
        elif opt in ["-s", "--start"]:
            start = val
        elif opt in ["-e", "--end"]:
            end = val
        elif opt in ["-C", "--config"]:
            configfile = val
        elif opt in ["-d", "--dryrun"]:
            dryrun = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if wiki is None:
        usage("mandatory argument argument missing: --wiki")
    if output_file is None:
        usage("mandatory argument argument missing: --outfile")

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

    wikiconf = Config(configfile)
    wikiconf.parse_conffile_per_project(wiki)
    do_flowbackup(wiki, output_file, history, wikiconf,
                  start, end, dryrun, verbose)


if __name__ == '__main__':
    main()
