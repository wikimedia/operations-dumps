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
from dumps.utils import DbServerInfo
from dumps.WikiDump import Wiki

from subprocess import Popen, PIPE

# fix all the error returns and make subroutines out of stuff
# current code puts together a command with a bunch of crap in it


def do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, id_field, table,
                  small_interval, max_interval, ends_with,
                  callback=None):
    '''
    do an xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into gzip's stdin for the
    concatenated compressed output
    '''
    if start is None:
        start = 1

    interval = None
    if end is None:
        end = get_max_id(wikiconf, wikidb, id_field, table)
        # if the whole wiki is small enough, take
        # arbitrary hopefully reasonable slices
        if start == 1 and end < 1000000:
            interval = small_interval

    if interval is None:
        # hope this is not too awful a guess
        interval = (int(end) - int(start)) / 12
        if interval > max_interval:
            interval = max_interval

    interval_save = interval
    # get just the header
    piece_command = [field for field in command]
    piece_command.extend(["--skip-footer", "--start=1", "--end=1"])
    do_xml_piece(piece_command, outfiles, dryrun=dryrun)

    if callback is not None:
        wiki = Wiki(wikiconf, wikidb)
        db_info = DbServerInfo(wiki, wikidb)

    upto = start
    while upto <= end:
        if callback is not None:
            interval = callback(upto, interval_save, wiki, db_info)
        piece_command = [field for field in command]
        piece_command.append("--skip-header")
        piece_command.extend(["--start=%s" % str(upto)])
        piece_command.append("--skip-footer")
        if upto + interval <= end:
            piece_command.extend(["--end", str(upto + interval)])
        else:
            piece_command.extend(["--end", str(end + 1)])
        upto = upto + interval
        do_xml_piece(piece_command, outfiles, ends_with, dryrun)

    # get just the footer
    piece_command = [field for field in command]
    piece_command.extend(["--skip-header", "--start=1", "--end=1"])
    do_xml_piece(piece_command, outfiles, dryrun=dryrun)

    if dryrun:
        return

    for filetype in outfiles:
        outfiles[filetype]['compr'].stdin.close()

    for filetype in outfiles:
        outfiles[filetype]['compr'].wait()


def run_script(command, outfiles, shouldendwith=None):
    '''
    given a command
    returns True on success, None on failure
    '''
    failed = False
    process = Popen(command)
    # would be best for there to be a timeout for this eh?
    process.wait()
    retval = process.returncode
    if not retval:
        for filetype in outfiles:
            outfile = outfiles[filetype]['temp']
            if os.path.exists(outfile):
                # file could be empty (all pages in the range deleted)
                if os.path.getsize(outfile) > 0:
                    if shouldendwith is not None:
                        with open(outfile, 'r') as outfd:
                            outfd.seek(len(shouldendwith) * -1, os.SEEK_END)
                            remainder = outfd.read()
                            outfd.close()
                            if remainder != shouldendwith:
                                os.unlink(outfile)
                                failed = True
    else:
        failed = True

    if failed:
        return False
    else:
        return True


def catfile(inputfile, process):
    '''
    read a file, cat it as fast as possible to the
    stdin of the process passed, then go away
    '''
    with open(inputfile, "r") as filed:
        while True:
            content = filed.read(1048576)
            if not content:
                filed.close()
                break
            process.stdin.write(content)


def gzippit(outfile):
    '''
    start a gzip process that reads from stdin
    and writes to the specified file
    '''
    process = Popen("gzip > %s" % outfile, stdin=PIPE, shell=True, bufsize=-1)
    return process


def catit(outfile):
    '''
    start a cat process that reads from stdin
    and writes to the specified file
    '''
    process = Popen("cat > %s" % outfile, stdin=PIPE, shell=True, bufsize=-1)
    return process


def get_max_id(wikiconf, wikidb, id_field, table):
    '''
    retrieve the largest id for this wiki from the db for specific table
    pass in name of id field, name of table
    '''
    wiki = Wiki(wikiconf, wikidb)

    db_info = DbServerInfo(wiki, wikidb)
    query = "select MAX(%s) from %s%s;" % (
        id_field, db_info.db_table_prefix, table)
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
        results = db_info.run_sql_and_get_output(query)
        if not results:
            continue
        lines = results.splitlines()
        if lines and lines[1]:
            end = int(lines[1])
            break

    if not end:
        sys.stderr.write("failed to get max page id from db, exiting\n")
        sys.exit(1)
    else:
        return end


def do_xml_piece(command, outfiles, ends_with=None, dryrun=False):
    '''
    do one piece of a logs dump, output going uncompressed
    to a temporary file and the that file being shovelled
    into the compressor's stdin

    we do three retries with plenty of delay, in case
    the db server has issues or some other problem
    crops up
    '''

    if dryrun:
        sys.stderr.write("would run command: %s\n" % " ".join(command))
        return

    retries = 0
    maxretries = 3
    timeout = 60
    while retries < maxretries:
        try:
            result = run_script(command, outfiles, ends_with)
        except:
            result = False
        if result:
            break
        time.sleep(timeout)
        timeout = timeout * 2
        retries += 1
    if not result:
        sys.stderr.write("failed job after max retries\n")
        for filetype in outfiles:
            try:
                # these partial output files can be used later with a
                # run that dumps the rest of the pages, and a recombine
                # so we don't remove them
                outfiles[filetype]['compr'].stdin.close()

                # don't remove the temp files either, might be useful
                # for checking the problem later
                # os.unlink(outfiles[filetype]['temp'])
            except:
                pass
        sys.exit(1)

    for filetype in outfiles:
        # any exception here means we don't unlink the temp files;
        # this is intentional, might examine them or re-use them
        catfile(outfiles[filetype]['temp'], outfiles[filetype]['compr'])

    for filetype in outfiles:
        try:
            os.unlink(outfiles[filetype]['temp'])
        except:
            pass
