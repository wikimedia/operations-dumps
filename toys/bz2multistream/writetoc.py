import getopt, os, sys, re, codecs

class indexTOC(object):
    # Generate a table of contents for an index file,
    # where the TOC will consist of lines containing
    # offset:char
    # where char is a unique starting character of
    # the text field in the index file, and offset is
    # the offset into the index file of the first
    # line with an article starting with the specific
    # character.

    # The index file should have lines of the format:
    # xxx:xxx:...:text field:xxx:...
    # or instead of ':' you can use the field separator
    # of your choice.
    # For the purposes of this script we only care about
    # the contents of the text field.
    # The index file must have been sorted by the text field
    # so that all entries starting with the same first
    # character are consecutive in the file.

    def __init__(self, inputFd, fieldNum, separator, verbose):
        # constructor
        # arguments:
        # inputFd   -- open file descriptor from which index lines will be read
        #              it better have been opened with utf8 codec if there are
        #              any unicode characters in the text fields
        # fieldNum  -- number of field containing text, numbering starts at 1
        # sep       -- field separator. for wmf index files this is ':'
        # verbose   -- whether or not to display info about processing of the index lines
        self.inputFd = inputFd
        self.fieldNum = fieldNum
        self.sep = separator
        self.verbose = verbose
        self.currentChar = None
        self.offset = 0

    def doTOC(self, outFd):
        # read all input from the input file descriptor
        # and write a TOC file for that input to the
        # specified output file descriptor, which should
        # already have been set up for writing by the
        # caller
        outFdUTF8 = codecs.getwriter("utf-8")(outFd)
        self.currentChar = None
        self.offset = 0
        for line in self.inputFd:
            self.processLine(line, outFdUTF8)
        
    def processLine(self, line, outFd):
        # for a given line of input, see if the
        # text field in the line starts with a new
        # unique first character, and if so, write
        # a TOC entry for that character to the
        # specified output file descriptor
        firstChar = self.getFirstCharFromField(line)
        if not firstChar:
            if self.verbose:
                sys.stderr.write("no first char retrieved for line: %s, skipping\n" % line)
            self.offset += len(line.encode('utf-8'))
            next
        if not self.currentChar or firstChar != self.currentChar:
            if self.verbose:
                sys.stderr.write("new first char for line: %s, recording\n" % line)
            self.currentChar = firstChar
            outFd.write("%s:%s\n" % (self.offset, firstChar))
        self.offset += len(line.encode('utf-8'))

    def getFirstCharFromField(self, line):
        # find the text field in the given line
        # and return the first character (not byte) in the field
        # or None if there is none
        stripped = line.rstrip('\n')
        fields = stripped.split(self.sep, self.fieldNum-1)
        if  len(fields) < fieldNum:
            return None
        if not len(fields[fieldNum -1]):
            return None
        return fields[fieldNum -1][0]

def usage(message = None):
    if message:
        sys.stderr.write("%s\n" % message)
    sys.stderr.write("Usage: python %s --field=num --separator=char --tocfile=filename [--verbose]\n" % sys.argv[0])
    sys.stderr.write("\n")
    sys.stderr.write("Given plain text input consisting lines with several fields with a given\n")
    sys.stderr.write("separator, which have been sorted by a specified field from each line, write\n")
    sys.stderr.write("a TOC  (table of contents) which contains a list of the unique first\n")
    sys.stderr.write("characters of the sort field and the offset to the first line of the file in\n")
    sys.stderr.write("which the sort field starts with that character.  In other words, if the text\n")
    sys.stderr.write("fields of the input file all happen to start only with a,b,c, and q, there\n")
    sys.stderr.write("will be exactly four lines in the created TOC, with offsets to the first\n")
    sys.stderr.write("line from the input with the sort field starting with a, the first line\n")
    sys.stderr.write("from the input the sort field starting with b, and so on.\n")
    sys.stderr.write("\n")
    sys.stderr.write("This is used to create a TOC into an article XML multistream index\n")
    sys.stderr.write("(after it has been uncompressed and sorted by article title), so that\n")
    sys.stderr.write("retrieval of article text from the article XML multistream content file\n")
    sys.stderr.write("can be done quickly without a database or other server-client model.\n")
    sys.stderr.write("\n")
    sys.stderr.write("--field:     the number of the field with which the input file was\n")
    sys.stderr.write("             alphabetically sorted, starting with 1\n")
    sys.stderr.write("             default: 1\n")
    sys.stderr.write("--tocfile:   path to the TOC file which will be created\n")
    sys.stderr.write("--separator: the string used to separate fields in the input file\n")
    sys.stderr.write("             default: space\n")
    sys.stderr.write("--verbose:   display extra messages about what is being done\n")
    sys.stderr.write("\n")
    sys.stderr.write("Example: LC_ALL_save=`echo $LC_ALL`; LC_ALL=C; export LC_ALL; \\\n")
    sys.stderr.write("         bzcat enwiki-20120902-pages-articles-multistream-index.txt.bz2 | \\\n")
    sys.stderr.write("         sort -k 3 -t ':' > enwiki-20120902-pages-articles-multistream-index-sorted.txt; \\\n")
    sys.stderr.write("         LC_ALL=${LC_ALL_save}; export LC_ALL\n")
    sys.stderr.write("\n")
    sys.stderr.write("         cat enwiki-20120902-pages-articles-multistream-index-sorted.txt | \\\n")
    sys.stderr.write("         python %s --field 3 --separator ':' --tocfile enwiki-20120902-pages-articles-multistream-index-sorted-idx.txt\n" % sys.argv[0])
    sys.exit(1)

if __name__ == "__main__":
    tocFileName = None
    fieldNum = 1
    separator = ' '
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", ['field=', 'tocfile=', 'separator=', 'verbose' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--field":
            if not val.isdigit():
                usage("Bad value specified for 'field' option")
            fieldNum = int(val)
        elif opt == "--tocfile":
            tocFileName = val
        elif opt == "--separator":
            if len(separator) != 1:
                usage("Bad value specified for 'separator' option")
            separator = val
        elif opt == "--verbose":
            verbose = True

    if (len(remainder) > 0):
        usage("Unknown option specified")

    if (not tocFileName):
        usage("Missing required option 'tocfile'")

    try:
        outFile = open(tocFileName, "w")
    except:
        sys.stderr.write("failed to open file %s for writing\n", tocFileName)
        raise

    inFile = codecs.getreader("utf-8")(sys.stdin)
    
    toc = indexTOC(inFile, fieldNum, separator, verbose)
    toc.doTOC(outFile)

    outFile.close()

    exit(0);
