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
from xmlstreams import do_xml_stream, gzippit


def do_abstractsbackup(wikidb, output_files, variants,
                       wikiconf, start, end, dryrun):
    '''
    do an abstracts xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into gzip's stdin for the
    concatenated compressed output
    '''
    outfiles = {}
    index = 0
    for variant in variants:
        outfiles[variant] = {'name': output_files[index]}
        index += 1

    for filetype in outfiles:
        outfiles[filetype]['temp'] = os.path.join(
            FileUtils.wiki_tempdir(wikidb, wikiconf.temp_dir),
            os.path.basename(outfiles[filetype]['name']) + "_tmp")
        if dryrun:
            outfiles[filetype]['compr'] = None
        else:
            outfiles[filetype]['compr'] = gzippit(outfiles[filetype]['name'])

    script_command = MultiVersion.mw_script_as_array(wikiconf,
                                                     "dumpBackup.php")
    command = [wikiconf.php] + script_command
    version = MultiVersion.mw_version(wikiconf, wikidb)
    abstract_cmd_dir = wikiconf.wiki_dir
    if version:
        abstract_cmd_dir = abstract_cmd_dir + "/" + version
    abstract_filter = ("--plugin=AbstractFilter:"
                       "%s/extensions/ActiveAbstract/AbstractFilter.php"
                       % abstract_cmd_dir)
    command.extend(["--wiki=%s" % wikidb, abstract_cmd_dir,
                    abstract_filter,
                    "--current", "--report=1000"])

    for filetype in outfiles:
        command.extend(["--output=file:%s" % outfiles[filetype]['temp'],
                        "--filter=namespace:NS_MAIN",
                        "--filter=noredirect",
                        "--filter=abstract%s" % filetype])

    do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, 'page_id', 'page',
                  5000, 10000, '</doc>\n')


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
Usage: xmlabstracts.py --wiki wikidbname --outfile path
    [--start number] [--end number]
    [--config path[:overrides_section]]

Options:

  --wiki (-w):         wiki db name, e.g. enwiki
  --outfiles (-o):     comma separated list of full paths to xml abstracts dumps that
                       will be created, one per language variant
  --variants (-V):     comma separated list of language variants for which abstracts
                       dumps will be produced, in the same order as the list of
                       output files, each variant corresponding to one file

  --start (-s):        starting page id to dump (default: 1)
  --end (-e):          ending page id to dump, exclusive of this page (default: dump all)

  --config (-C):       path to wikidump configfile (default: "wikidump.conf" in current dir)
                       if followed by : and a name, this section name in the config file
                       will be used to override config settings in default sections
  --dryrun (-d):       display the commands that would be run to produce the output but
                       don't actually run them
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    'main entry point, does all the work'
    wiki = None
    output_files = None
    variants = None
    start = None
    end = None
    configfile = "wikidump.conf"
    dryrun = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "w:o:V:s:e:C:fhv",
            ["wiki=", "outfiles=", "variants=",
             "start=", "end=", "config=",
             "help", "dryrun"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))
    for (opt, val) in options:
        if opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-o", "--outfiles"]:
            output_files = val
        elif opt in ["-V", "--variants"]:
            variants = val
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

    if remainder:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if wiki is None:
        usage("mandatory argument argument missing: --wiki")
    if output_files is None:
        usage("mandatory argument argument missing: --outputs")
    if variants is None:
        variants = ''

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

    output_files = output_files.split(",")
    variants = variants.split(",")
    if len(output_files) != len(variants):
        usage("each variant must correspond to outfile, "
              "different number supplied")

    wikiconf = Config(configfile)
    wikiconf.parse_conffile_per_project(wiki)
    do_abstractsbackup(wiki, output_files, variants, wikiconf,
                       start, end, dryrun)


if __name__ == '__main__':
    main()
