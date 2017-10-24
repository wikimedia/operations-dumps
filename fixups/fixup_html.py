import os
import sys


def read_wikis(filepath):
    "read list of wikis from file, one per line, and return list"
    fhandle = open(filepath, "r")
    text = fhandle.read()
    fhandle.close()
    return text.splitlines()


def pretty_size(size, quanta):
    "return size of file scaled down as much as possible."
    if size < 1024 or len(quanta) == 1:
        return quanta[0] % size
    else:
        return pretty_size(size / 1024.0, quanta[1:])


def get_printable_size(filepath):
    "return size of file with nice human readable format"
    quanta = ("%d bytes", "%d KB", "%0.1f MB", "%0.1f GB", "%0.1f TB")
    size = os.path.getsize(filepath)
    return pretty_size(size, quanta)


def get_new_html(multistream_name, multistr_index_name,
                 multistream_path, multistr_index_path,
                 html_path):
    """
    read old html content, fix up the lines that are missing info
    for the multistream content and index files, return the new
    content
    """
    with open(html_path, "r") as fhandle:
        contents = fhandle.read()
        lines = contents.splitlines()

    new_lines = []
    for line in lines:
        if 'pages-articles-multistream.xml' in line:
            line = line.replace(
                "<li class='missing'>",
                "<li class='file'>" + '<a href="{path}">'.format(path=multistream_name))
            line = line.replace(
                "stream.xml.bz2</li>",
                "stream.xml.bz2</a> {size} </li>".format(size=get_printable_size(multistream_path)))
        elif 'pages-articles-multistream-index.txt' in line:
            line = line.replace(
                "<li class='missing'>",
                "<li class='file'>" + '<a href="{path}">'.format(path=multistr_index_name))
            line = line.replace(
                "index.txt.bz2</li>",
                "index.txt.bz2</a> {size} </li>".format(
                    size=get_printable_size(multistr_index_path)))
        new_lines.append(line)
    return new_lines


def cleanup_html(wiki, dumpstree, date, dryrun):
    """
    add size and link for content and index multistream files
    to index.html file for the dump of the given wiki and date,
    writing out a new file.
    """
    dumpsdir = os.path.join(dumpstree, wiki, date)
    if not os.path.exists(dumpsdir):
        # skip dirs where the file doesn't exist,
        # the run hasn't happened, or it's a private
        # wiki with files elsewhere
        return
    multistream_name = '-'.join([wiki, date, 'pages-articles-multistream.xml.bz2'])
    multistr_index_name = '-'.join([wiki, date, 'pages-articles-multistream-index.txt.bz2'])

    multistream_path = os.path.join(dumpsdir, multistream_name)
    multistr_index_path = os.path.join(dumpsdir, multistr_index_name)

    html_path = os.path.join(dumpsdir, 'index.html')
    lines = get_new_html(multistream_name, multistr_index_name,
                         multistream_path, multistr_index_path,
                         html_path)

    new_file = html_path + '.new'
    if dryrun:
        print "would write lines to {out}:".format(out=new_file)
        for line in lines:
            if 'pages-articles-multistream' in line:
                print line
    else:
        output = '\n'.join(lines) + '\n'
        output_handle = file(new_file, "wt")
        output_handle.write(output)
        output_handle.close()


def usage(message=None):
    "display a usage message and exit."
    if message is not None:
        print message

    usage_message = """Usage: {script} YYYYMMDD [dryrun]
Add link and size of multistream content and index files to index.html
for all wikis for the given date.
Writes new html files into a temporary location 'index.html.new'.
""".format(script=sys.argv[0])
    print usage_message
    sys.exit(1)


def do_main(alldbs, dumpstree, date, dryrun):
    "entry point"
    wikis = read_wikis(alldbs)
    for wiki in wikis:
        cleanup_html(wiki, dumpstree, date, dryrun)


if __name__ == '__main__':
    dblist = '/home/datasets/all.dblist.edited'
    publicdir = '/mnt/data/xmldatadumps/public'

    # dblist = '/home/ariel/dumptesting/dblists/all.dblist'
    # publicdir = '/home/ariel/dumptesting/dumpruns/public'

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        usage()
    if sys.argv[1] in ['-h', '--help']:
        usage("Help for this script")

    do_main(dblist,
            publicdir,
            date=sys.argv[1], dryrun=True if len(sys.argv) == 3 else False)
