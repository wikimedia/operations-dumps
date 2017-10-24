import os
import sys
import hashlib
import json


def read_wikis(filepath):
    "read list of wikis, one per line, from file, return the list"
    fhandle = open(filepath, "r")
    text = fhandle.read()
    fhandle.close()
    return text.splitlines()


def checksum(filename, htype):
    "return hash of specified file in string format, using specified hash type"
    if htype == 'md5':
        summer = hashlib.md5()
    else:
        summer = hashlib.sha1()
    infhandle = file(filename, "rb")
    bufsize = 4192 * 32
    fbuffer = infhandle.read(bufsize)
    while fbuffer:
        summer.update(fbuffer)
        fbuffer = infhandle.read(bufsize)
    infhandle.close()
    return summer.hexdigest()


def update_hashes_text(hashed_paths, output_file, hash_strings, dryrun):
    """
    we expect the file to contain all the existing hashes,
    we will append to it
    """
    if not os.path.exists(output_file):
        # no file with old hashes. something's wrong, skip.
        return

    with open(output_file, "r") as fhandle:
        content = fhandle.read()
    new_file = output_file + ".new"

    if not dryrun:
        output_handle = file(new_file, "wt")
        output_handle.write(content)

    for idx in range(0, len(hashed_paths)):
        if hashed_paths[idx] in content:
            # info already present in hash file. skip.
            continue

        if dryrun:
            print "would append: '{hsum}  {path}' to".format(
                hsum=hash_strings[idx], path=hashed_paths[idx]), new_file
        else:
            output_handle.write("{hsum}  {path}\n".format(hsum=hash_strings[idx],
                                                          path=hashed_paths[idx]))
    if not dryrun:
        output_handle.close()


def update_hashes_json(hashed_paths, output_file, hash_strings, htype, dryrun):
    """
    we expect the file to contain all the existing hashes,
    we read it, load the json, add our entry to the dict, convert it
    back to json and write it back out as new file
    """
    if not os.path.exists(output_file):
        # no file with old hashes. something's wrong, skip.
        return

    with open(output_file, "r") as fhandle:
        contents = fhandle.read()
        output = json.loads(contents)

    new_file = output_file + ".new"
    if not dryrun:
        output_handle = file(new_file, "wt")

    for idx in range(0, len(hashed_paths)):
        output[htype]["files"][hashed_paths[idx]] = hash_strings[idx]

    if dryrun:
        print "would write: '{outp}' to".format(outp=json.dumps(output)), new_file
    else:
        output_handle.write(json.dumps(output))
        output_handle.close()


def update_hashes(file_paths, hashes_path, hash_strings, htype, ftype, dryrun):
    filenames = [os.path.basename(path) for path in file_paths]
    if ftype == 'txt':
        update_hashes_text(filenames, hashes_path, hash_strings, dryrun)
    else:
        update_hashes_json(filenames, hashes_path, hash_strings, htype, dryrun)


def get_hashfile_path(dumpstree, wiki, date, hashtype, filetype):
    dumpsdir = os.path.join(dumpstree, wiki, date)
    filename = '-'.join([wiki, date, '{htype}sums.{ftype}'.format(htype=hashtype, ftype=filetype)])
    return os.path.join(dumpsdir, filename)


def cleanup_hashfiles(wiki, dumpstree, date, filename_bases, dryrun):
    """
    For the specified wiki and date, given the base part of the filename,
    get the md5 and sha1 sums of the corresponding wiki dump file for
    that date, append these to the plaintext files of hashes and write
    out new files.

    Also write new json files of hashes to include this information;
    these values will overwrite old values if present.
    """
    dumpsdir = os.path.join(dumpstree, wiki, date)
    if not os.path.exists(dumpsdir):
        # skip dirs where the file doesn't exist,
        # the run hasn't happened, or it's a private
        # wiki with files elsewhere
        print "skipping this wiki", dumpsdir
        return

    filenames = ['-'.join([wiki, date, base]) for base in filename_bases]
    file_paths = [os.path.join(dumpsdir, filename) for filename in filenames]
    file_paths = [path for path in file_paths if os.path.exists(path)]
    for htype in ['md5', 'sha1']:
        for ftype in ['txt', 'json']:
            hashes_path = get_hashfile_path(dumpstree, wiki, date, htype, ftype)
            hash_strings = [checksum(filename, htype) for filename in file_paths]
            update_hashes(file_paths, hashes_path, hash_strings, htype, ftype, dryrun)


def usage(message=None):
    "display a usage message and exit."
    if message is not None:
        print message

    usage_message = """Usage: {script} YYYYMMDD [dryrun]
Adds md5sum and sha1sum of multistream content and index files
to the plaintext files and the json files with hash lists.

The new files are created with the extension '.new' at the end.
""".format(script=sys.argv[0])
    print usage_message
    sys.exit(1)


def do_main(alldbs, dumpstree, date, filename_bases, dryrun):
    "main entry point"
    wikis = read_wikis(alldbs)
    for wiki in wikis:
        cleanup_hashfiles(wiki, dumpstree, date, filename_bases, dryrun)


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        usage()
    if sys.argv[1] in ['-h', '--help']:
        usage("Help for this script")

    dblist = '/home/datasets/all.dblist.edited'
    publicdir = '/mnt/data/xmldatadumps/public'

    # dblist = '/home/ariel/dumptesting/dblists/all.dblist'
    # publicdir = '/home/ariel/dumptesting/dumpruns/public'

    basenames = ['pages-articles-multistream-index.txt.bz2',
                 'pages-articles-multistream.xml.bz2']
    do_main(dblist, publicdir, sys.argv[1], basenames,
            dryrun=True if len(sys.argv) == 3 else False)
