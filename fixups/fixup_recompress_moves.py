import os
import sys
from subprocess import Popen


def read_wikis(filepath):
    "read list of wikis, one per line, from file and return the list"
    fhandle = open(filepath, "r")
    text = fhandle.read()
    fhandle.close()
    return text.splitlines()


def compress(input_path, output_path, dryrun):
    """
    returns True on success, False on failure
    """
    command = "/bin/bzip2 -zc {inp} > {out}".format(
        inp=input_path, out=output_path)
    if dryrun:
        print "would run", command
        return True
    try:
        proc = Popen(command, shell=True)
        _output, error = proc.communicate()
    except Exception:
        # fixme display the issue too
        return False

    if error is not None:
        print error
        return False
    else:
        return True


def is_compressed(path):
    """
    check if the file is bz2 compressed
    return True if so, False otherwise
    """
    with open(path) as fhandle:
        header = fhandle.read(7)
        return bool(header.startswith("BZh91AY"))


def cleanup_multistreams(wiki, dumpstree, date, dryrun):
    """
    for the specified wiki, if there is a multistream
    content file with temp filename, move it into the
    permanent location; if there is a multistream index
    file with temp filename, bzip2 compress it into the
    permanent location
    """
    dumpsdir = os.path.join(dumpstree, wiki, date)
    if not os.path.exists(dumpsdir):
        # skip dirs where the file doesn't exist,
        # the run hasn't happened, or it's a private
        # wiki with files elsewhere
        return
    multistream_name = '-'.join([wiki, date, 'pages-articles-multistream.xml.bz2'])
    index_name = '-'.join([wiki, date, 'pages-articles-multistream-index.txt.bz2'])
    extension = '.inprog'
    multistream_path = os.path.join(dumpsdir, multistream_name)
    index_path = os.path.join(dumpsdir, index_name)
    if os.path.exists(multistream_path + extension):
        if dryrun:
            print "would rename", multistream_path + extension, "to", multistream_path
        else:
            os.rename(multistream_path + extension, multistream_path)
    if os.path.exists(index_path + extension):
        if os.path.exists(index_path):
            print "target file ", index_path, "already exists, skipping"
        else:
            if is_compressed(index_path + extension):
                # don't compress, just move into place
                if dryrun:
                    print "would rename", index_path + extension, "to", index_path
                else:
                    os.rename(index_path + extension, index_path)
            elif compress(index_path + extension, index_path, dryrun):
                if dryrun:
                    print "would remove", index_path + extension
                else:
                    os.unlink(index_path + extension)


def do_main(alldbs, dumpstree, date, dryrun):
    """
    entry point. for all wikis in the list, for the dump date specified
    by date (YYYYMMDD), fix up the articles multistream content and
    index file in the subdir wiki/date under the specified dumpstree.
    """
    wikis = read_wikis(alldbs)
    for wiki in wikis:
        cleanup_multistreams(wiki, dumpstree, date, dryrun)


def usage(message=None):
    "display a usage message and exit."
    if message is not None:
        print message

    usage_message = """Usage: {script} YYYYMMDD [dryrun]
Moves multistream content file from temp to permanent location;"
Bzip2 compresses index file into permanent location and removes"
temp file.
""".format(script=sys.argv[0])
    print usage_message
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        usage()
    if sys.argv[1] in ['-h', '--help']:
        usage("Help for this script")

    dblist = '/home/datasets/all.dblist.edited'
    publicdir = '/mnt/data/xmldatadumps/public'

    # dblist = '/home/ariel/dumptesting/dblists/all.dblist'
    # publicdir = '/home/ariel/dumptesting/dumpruns/public'
    do_main(dblist,
            publicdir,
            date=sys.argv[1], dryrun=True if len(sys.argv) == 3 else False)
