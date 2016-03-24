import getopt
import sys
import codecs


class IndexTOC(object):
    '''
    Generate a table of contents for an index file,
    where the TOC will consist of lines containing
    offset:char
    where char is a unique starting character of
    the text field in the index file, and offset is
    the offset into the index file of the first
    line with an article starting with the specific
    character.

    The index file should have lines of the format:
    xxx:xxx:...:text field:xxx:...
    or instead of ':' you can use the field separator
    of your choice.
    For the purposes of this script we only care about
    the contents of the text field.
    The index file must have been sorted by the text field
    so that all entries starting with the same first
    character are consecutive in the file.
    '''

    def __init__(self, input_fd, field_num, separator, verbose):
        '''
        constructor
        arguments:
        input_fd  -- open file descriptor from which index
                     lines will be read; it better have been
                     opened with utf8 codec if there are any
                     unicode characters in the text fields
        field_num -- number of field containing text, numbering
                     starts at 1
        sep       -- field separator. for wmf index files this
                     is ':'
        verbose   -- whether or not to display info about
                     processing of the index lines
        '''
        self.input_fd = input_fd
        self.field_num = field_num
        self.sep = separator
        self.verbose = verbose
        self.current_char = None
        self.offset = 0

    def do_toc(self, out_fd):
        '''
        read all input from the input file descriptor
        and write a TOC file for that input to the
        specified output file descriptor, which should
        already have been set up for writing by the
        caller
        '''
        out_fd_utf8 = codecs.getwriter("utf-8")(out_fd)
        self.current_char = None
        self.offset = 0
        for line in self.input_fd:
            self.process_line(line, out_fd_utf8)

    def process_line(self, line, out_fd):
        '''
        for a given line of input, see if the
        text field in the line starts with a new
        unique first character, and if so, write
        a TOC entry for that character to the
        specified output file descriptor
        '''
        first_char = self.get_first_char_from_field(line)
        if not first_char:
            if self.verbose:
                sys.stderr.write("no first char retrieved for line: %s, skipping\n" % line)
        elif not self.current_char or first_char != self.current_char:
            if self.verbose:
                sys.stderr.write("new first char for line: %s, recording\n" % line)
            self.current_char = first_char
            out_fd.write("%s:%s\n" % (self.offset, first_char))
        self.offset += len(line.encode('utf-8'))

    def get_first_char_from_field(self, line):
        '''
        find the text field in the given line
        and return the first character (not byte) in the field
        or None if there is none
        '''
        stripped = line.rstrip('\n')
        fields = stripped.split(self.sep, self.field_num-1)
        if  len(fields) < self.field_num:
            return None
        if not len(fields[self.field_num -1]):
            return None
        return fields[self.field_num -1][0]


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """
Usage: python writetoc.py --field=num --separator=char
                --tocfile=filename [--verbose]

Given plain text input consisting lines with several fields with a given
separator, which have been sorted by a specified field from each line, write
a TOC  (table of contents) which contains a list of the unique first
characters of the sort field and the offset to the first line of the file in
which the sort field starts with that character.  In other words, if the text
fields of the input file all happen to start only with a,b,c, and q, there
will be exactly four lines in the created TOC, with offsets to the first
line from the input with the sort field starting with a, the first line
from the input the sort field starting with b, and so on.

This is used to create a TOC into an article XML multistream index
(after it has been uncompressed and sorted by article title), so that
retrieval of article text from the article XML multistream content file
can be done quickly without a database or other server-client model.

--field:     the number of the field with which the input file was
             alphabetically sorted, starting with 1
             default: 1
--tocfile:   path to the TOC file which will be created
--separator: the string used to separate fields in the input file
             default: space
--verbose:   display extra messages about what is being done

Example: LC_ALL_save=`echo $LC_ALL`; LC_ALL=C; export LC_ALL; \\
         bzcat enwiki-20120902-pages-articles-multistream-index.txt.bz2 | \\
         sort -k 3 -t ':' > \\
         enwiki-20120902-pages-articles-multistream-index-sorted.txt; \\
         LC_ALL=${LC_ALL_save}; export LC_ALL

         cat enwiki-20120902-pages-articles-multistream-index-sorted.txt | \\
         python writetoc.py --field 3 --separator ':' \\
         --tocfile enwiki-20120902-pages-articles-multistream-index-sorted-idx.txt
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    toc_file_name = None
    field_num = 1
    separator = ' '
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ['field=', 'tocfile=', 'separator=', 'verbose'])
    except Exception:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--field":
            if not val.isdigit():
                usage("Bad value specified for 'field' option")
            field_num = int(val)
        elif opt == "--tocfile":
            toc_file_name = val
        elif opt == "--separator":
            if len(separator) != 1:
                usage("Bad value specified for 'separator' option")
            separator = val
        elif opt == "--verbose":
            verbose = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not toc_file_name:
        usage("Missing required option 'tocfile'")

    try:
        out_file = open(toc_file_name, "w")
    except:
        sys.stderr.write("failed to open file %s for writing\n", toc_file_name)
        raise

    in_file = codecs.getreader("utf-8")(sys.stdin)

    toc = IndexTOC(in_file, field_num, separator, verbose)
    toc.do_toc(out_file)

    out_file.close()

    exit(0)


if __name__ == "__main__":
    do_main()
