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
import time
import traceback
from subprocess import Popen, PIPE
from dumps.utils import DbServerInfo
from dumps.wikidump import Wiki


# fix all the error returns and make subroutines out of stuff
# current code puts together a command with a bunch of crap in it


def do_xml_stream(wikidb, outfiles, command, wikiconf,
                  start, end, dryrun, id_field, table,
                  small_interval, max_interval, ends_with,
                  verbose=False, callback=None, header=False, footer=False):
    '''
    do an xml dump one piece at a time, writing into uncompressed
    temporary files and shovelling those into gzip's stdin for the
    concatenated compressed output

    if header is True, write only the header
    if footer is True, write only the footer
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
        interval = int((int(end) - int(start)) / 50)
        if interval == 0:
            interval = 1
        elif interval > max_interval:
            interval = max_interval

    interval_save = interval
    if header:
        # get just the header
        piece_command = [field for field in command]
        piece_command.extend(["--skip-footer", "--start=1", "--end=1"])
        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'] = outfiles[filetype]['compr'][0](
                    outfiles[filetype]['compr'][1])

        do_xml_piece(piece_command, outfiles, wikiconf, dryrun=dryrun, verbose=verbose)
        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'].stdin.close()
            for filetype in outfiles:
                outfiles[filetype]['process'].wait()
    elif footer:
        # get just the footer
        piece_command = [field for field in command]
        piece_command.extend(["--skip-header", "--start=1", "--end=1"])
        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'] = outfiles[filetype]['compr'][0](
                    outfiles[filetype]['compr'][1])

        do_xml_piece(piece_command, outfiles, wikiconf, dryrun=dryrun, verbose=verbose)
        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'].stdin.close()
            for filetype in outfiles:
                outfiles[filetype]['process'].wait()
    else:
        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'] = outfiles[filetype]['compr'][0](
                    outfiles[filetype]['compr'][1])

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
            do_xml_piece(piece_command, outfiles, wikiconf, ends_with,
                         dryrun=dryrun, verbose=verbose)

        if not dryrun:
            for filetype in outfiles:
                outfiles[filetype]['process'].stdin.close()
            for filetype in outfiles:
                outfiles[filetype]['process'].wait()

    if dryrun:
        return


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
                        with open(outfile, 'rb') as outfd:
                            outfd.seek(len(shouldendwith) * -1, os.SEEK_END)
                            remainder = outfd.read().decode('utf-8')
                            outfd.close()
                            if remainder != shouldendwith:
                                os.unlink(outfile)
                                sys.stderr.write(
                                    "bad output saved to {ofile} from '{command}'\n".format(
                                        ofile=outfile, command=" ".join(command)))
                                failed = True
    else:
        sys.stderr.write("nonzero return {retval} from command '{command}'\n".format(
            retval=retval, command=" ".join(command)))
        failed = True

    if failed:
        return False
    return True


def catfile(inputfile, process):
    '''
    read a file, cat it as fast as possible to the
    stdin of the process passed, then go away
    '''
    with open(inputfile, "r") as fhandle:
        while True:
            content = fhandle.read(1048576)
            if not content:
                fhandle.close()
                break
            process.stdin.write(content.encode('utf-8'))


def gzippit_append(outfile):
    '''
    start a gzip process that reads from stdin
    and appends to the specified file
    '''
    process = Popen("gzip >> %s" % outfile, stdin=PIPE, shell=True, bufsize=-1)
    return process


def bzip2it_append(outfile):
    '''
    start a bzip2 process that reads from stdin
    and appends to the specified file
    '''
    process = Popen("bzip2 >> %s" % outfile, stdin=PIPE, shell=True, bufsize=-1)
    return process


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
        id_field, db_info.get_attr('db_table_prefix'), table)
    results = None
    retries = 0
    maxretries = wiki.config.max_retries
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


def do_xml_piece(command, outfiles, wikiconf, ends_with=None, dryrun=False, verbose=False):
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

    if verbose:
        sys.stderr.write("running command: %s\n" % " ".join(command))

    retries = 0
    maxretries = wikiconf.max_retries
    timeout = 60
    while retries < maxretries:
        try:
            result = run_script(command, outfiles, ends_with)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
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
                # don't bother to save partial files, cleanup everything
                outfiles[filetype]['compr'].stdin.close()
                os.unlink(outfiles[filetype]['temp'])
            except Exception:
                # files might not be there, we don't care
                pass
        sys.exit(1)

    errors = False
    for filetype in outfiles:
        try:
            catfile(outfiles[filetype]['temp'], outfiles[filetype]['process'])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            errors = True
            try:
                # get rid of the final output file, it's crap now
                os.unlink(outfiles[filetype]['compr'][1])
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))

    # get rid of all temp files, regardless
    for filetype in outfiles:
        try:
            os.unlink(outfiles[filetype]['temp'])
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

    if errors:
        # consider ourselves screwed. the parent process can start over
        sys.exit(1)
