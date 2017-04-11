"""
grab CREATE TABLE statement from e.g. a mysql dump
and write it to a separate file
"""
import getopt
import sys
import gzip


def usage(message=None):
    """
    show usage information for this script with an optional
    message preceding it
    """
    if message is not None:
        sys.stderr.write(message + "\n")
    usage_message = """extract_tablecreate.py --sqlfile path
               [--help]

Tis script will read the sql contained in the specified sql file until
it finds a CREATE TABLE statement.  It will write that statement to
an output file of a similar name but with 'create' tacked on at
the end.

Gzipped files will be zcatted silently as input;
the output file will be uncompressed regardless.

Options:

--sqlfile (-s):  path to possibly gzipped sql file with the
                 CREATE TABLE statement and perhaps a bunch of
                 INSERTS and such afterwards
--help    (-h):  show this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_output_file(sqlfile):
    """
    generate suitable output filename
    """
    newfile = sqlfile
    if newfile.endswith(".gz"):
        newfile = newfile[:-3]
    return newfile + ".create"


def get_fhandle(path, mode="r"):
    """
    get an appropriate filehandle for
    plaintext or gzipped file
    """
    if path.endswith(".gz"):
        return gzip.open(path, mode)
    else:
        return open(path, mode)


def write_create_table(sqlfile):
    """
    read the first part of the sql file,
    fine the create table statement,
    write it out to a file of a similar name but with
    no compression file extension (as the file will
    be written out uncompressed), and the string
    'create' tacked on at the end.
    """
    out_fhandle = get_fhandle(get_output_file(sqlfile), "w+")
    in_fhandle = get_fhandle(sqlfile, "r")
    writing = False
    for line in in_fhandle:
        if line.startswith("CREATE"):
            writing = True
            out_fhandle.write(line)
        elif line.startswith(")") and writing:
            writing = False
            out_fhandle.write(line)
            out_fhandle.close()
            return
        elif writing:
            out_fhandle.write(line)


def do_main():
    'main entry point, does all the work'
    sqlfile = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "s:h", ["sqlfile=", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-s", "--sqlfile"]:
            sqlfile = val
        elif opt in ["-h", "--help"]:
            usage("Help for this script")
        else:
            usage("Unknown option specified: <%s>" % opt)

    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])
    if sqlfile is None:
        print "Mandatory 'sqlfile' argument not specified"
        sys.exit(1)

    write_create_table(sqlfile)


if __name__ == '__main__':
    do_main()

