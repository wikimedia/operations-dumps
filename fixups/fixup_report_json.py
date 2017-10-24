import os
import sys
import json


def read_wikis(filepath):
    "read list of wkis from file, one per line, and return the list"
    fhandle = open(filepath, "r")
    text = fhandle.read()
    fhandle.close()
    return text.splitlines()


def cleanup_report_json(wiki, dumpstree, date, dryrun):
    """add size and relative url for multistream content and index files
    to contents of report.json, and write out a new file."""
    dumpsdir = os.path.join(dumpstree, wiki, date)
    if not os.path.exists(dumpsdir):
        # skip dirs where the file doesn't exist,
        # the run hasn't happened, or it's a private
        # wiki with files elsewhere
        print "skipping this wiki:", wiki
        return
    multistream_name = '-'.join([wiki, date, 'pages-articles-multistream.xml.bz2'])
    index_name = '-'.join([wiki, date, 'pages-articles-multistream-index.txt.bz2'])

    multistream_path = os.path.join(dumpsdir, multistream_name)
    index_path = os.path.join(dumpsdir, index_name)

    report_json_path = os.path.join(dumpstree, wiki, date, 'report.json')
    with open(report_json_path, "r") as fhandle:
        contents = fhandle.read()
        output = json.loads(contents)

    if os.path.exists(multistream_path):
        output['jobs']['articlesmultistreamdump']['files'][multistream_name] = {
            'size': os.path.getsize(multistream_path),
            'url': os.path.join('/', wiki, date, multistream_name)}
    if os.path.exists(index_path):
        output['jobs']['articlesmultistreamdump']['files'][index_name] = {
            'size': os.path.getsize(index_path),
            'url': os.path.join('/', wiki, date, index_name)}

    new_file = report_json_path + '.new'
    if dryrun:
        print "would write '{inp}' to".format(inp=json.dumps(output)), new_file
    else:
        output_handle = file(new_file, "w")
        output_handle.write(json.dumps(output))
        output_handle.close()


def usage(message=None):
    "display a usage message and exit."
    if message is not None:
        print message

    usage_message = """Usage: {script} YYYYMMDD [dryrun]
Adds information about the multistream content file and the
index file to report.json, writing a new temp file.
""".format(script=sys.argv[0])
    print usage_message
    sys.exit(1)


def do_main(alldbs, dumpstree, date, dryrun):
    "main entry point"
    wikis = read_wikis(alldbs)
    for wiki in wikis:
        cleanup_report_json(wiki, dumpstree, date, dryrun)


if __name__ == '__main__':
    dblist = '/home/datasets/all.dblist.edited'
    publicdir = '/mnt/data/xmldatadumps/public'

    # dblist = '/home/ariel/dumptesting/dblists/all.dblist'
    # publicdir = '/home/ariel/dumptesting/dumpruns/public'

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        usage()
    if sys.argv[1] in ['-h', '--help']:
        usage("Help for this script")

    do_main(dblist, publicdir, date=sys.argv[1],
            dryrun=True if len(sys.argv) == 3 else False)
