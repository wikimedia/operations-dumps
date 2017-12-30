import getopt
import os
import sys
import re
import bz2
import ConfigParser


def get_choice_from_batch(titles, start, batch_size):
    '''
    display titles from start to start + batchsize, with count in front
    ask the caller for a title number or an action
    actions may be Q (quit), N (next batch), B (previous batch), R (redisplay)
    if caller enters nothing, treat that as default (R)
    if caller enters something else, whine and treat that as default (R) too
    arguments:
    titles     -- full list of titles
    start      -- display from this point in the list
    batch_size -- how many titles to display
    returns a tuple of (action, title number) where one or the other of these
    may be None
    '''

    # yay python, it will silently ignore the fact that you requested
    # more things in the list than exist. (no this is not sarcasm)
    count = start
    for line in titles[start:start+batch_size]:
        print "%s) %s" % (count+1, WATitleMunger.un_normalize_title(line))
        count += 1
    print
    print("Enter number of choice, or Q/N/P/R to "
          "quit/next page/prev page/redisplay page (default R): "),
    choice = sys.stdin.readline()
    choice = choice.strip()
    if not choice:
        choice = 'R'
    if choice.isdigit():
        num = int(choice)
        if num < 1 or num > len(titles):
            print "Bad number given."
            return "R", None
        return None, num
    else:
        choice = choice.capitalize()
        if choice in ['N', 'P', 'Q', 'R']:
            return choice, None
        else:
            print "Bad choice given."
            return "R", None


def process_action(action, start, batch_size, total):
    '''
    given a caller action,
    update title list display pointer to the appropriate position
    arguments:
    action     -- Q (quit), N (next batch), P (prev batch), or anything else
    start      -- title list display pointer, a batch of titles from the list
                  will be displayed starting from this number
    batch_size -- how many titles are dispayed in a batch
    total      -- total titles in the list
    returns: updated title list display pointer, or exits at user request
    (action Q)
    note that any action other than Q/N/P will result in the default R (redisplay
    current batch of titles) which means no change, return existing value.  This
    includes the None action.
    '''
    if action == 'N' or action == 'n':
        if start + batch_size < total:
            start += batch_size
        else:
            print "End of list reached."
    elif action == 'P' or action == 'p':
        if start > batch_size:
            start = start - batch_size
        else:
            print "Beginning of list reached."
    elif action == 'Q' or action == 'q':
        print "Exiting at user's request."
        sys.exit(0)
    return start


def get_user_title_choice(title_hash):
    '''
    show a numbered list of page titles on stdout and read the
    caller's choice on stdin
    I guess this is a poor person's pager
    arguments:
    titleHash -- hash of page titles and their offsets into the xml file
    returns: the offset into the xml file for the title selected
    '''
    titles = title_hash.keys()
    titles.sort()
    total = len(titles)

    choice = None
    start = 0
    batch_size = 30
    print "Multiple titles found, please choose from the following."
    while not choice:
        (action, choice) = get_choice_from_batch(titles, start, batch_size)
        if choice:
            return titles[choice-1]
        else:
            start = process_action(action, start, batch_size, total)


def get_text(xml):
    '''
    get the contents of the <text>...</text> tags if needed,
    returns the contents found or None if none found
    '''
    text = None
    if xml is not None:
        match = re.search("<text[^>]*>(.*?)</text>", xml, flags=re.DOTALL)
        if match:
            text = match.group(1)
    return text


def get_redirect(text, localized_redir_string, verbose):
    '''
    look for and set page title of a redirect link in the text, if any
    returns the redirction link or None if none was found
    format: <text ...>#REDIRECT [[link|show to reader]] ...
    fixme the redirect keyword really should be case insensitive but
    have we done any unicode stuff here? nope, so that ain't happening

    arguments:
    text                   -- the raw text dug out from the <text> tags of page content
    localized_redir_string -- the string 'REDIR' in the wiki content language
    verbose
    '''
    redirect = None
    if text is not None:
        redir_regex = (re.compile(r"<text [^>]+>\s*#(REDIRECT|%s)\s*\[\[([^\|\]]+)"
                                  % localized_redir_string))
        match = redir_regex.search(text)
        if match:
            if verbose:
                sys.stderr.write("Found a redirect in the page "
                                 "text: %s\n" % match.group(2))
            redirect = match.group(2)
    return redirect


def display(text, file_string, category_string, clean=False, text_only=False):
    '''
    display the text, optionally extracting just the content
    from the <text> tags and optionally doing some cleanup
    on the text before display
    arguments:
    text -- the page text from the xml file, everything between
    <page>...</page>

    arguments:
    fileString      --  the string 'File' in the wiki's content language
    category_string --  the string 'Category' in the wiki's content language
    clean           --  whether or not to clean up various tags etc or leave the raw text
                        as retrieved from the xml file
    text_only       --  whether or not to include the metadata and other stuff for the page
                        as retrieved from the xml file or just the content from the <text> tags
    '''
    if text is None:
        print "No page text for that title found."
        return

    if text_only:
        text = get_text(text)

    if clean:
        formatter = WATextFormatter(text, file_string, category_string)
        text = formatter.do_formatting()

    print text


def handle_redirects(text, retriever, max_redirs, redir_text, verbose):
    '''
    follow redirect link in page text, retrieve the target page text, and
    check that til we reach a page that's not a redirect or we hit the
    maxRedirs limit or we hit a redirect to a nonexistent page

    arguments:
    text      -- initial page text as retrieved from the xml file,
                 without any cleanup etc.
    retriever -- WAPageRetriever object (used to follow the redirects)
    returns the page text, either of the first non-redirect or the
    last redirect before going over the redir follow limit, or
    the last page text before following a link to a nonexistent page
    max_redirs -- follow this many redirect links until giving up, if 0 then
                  don't follow any (need this so we avoid redirection loops)
    redir_text -- the string 'REDIRECT' in the wiki's content language
    verbose    -- whether or not to display messages about processing being done
    '''
    if not text:
        return text
    redirs_done = 0
    redir_link = None
    while redirs_done < max_redirs:
        if verbose:
            sys.stderr.write("Checking for redirects in text, "
                             "redirs done already:%s\n" % redirs_done)
        redir_link = get_redirect(text, redir_text, verbose)
        if not redir_link:
            break
        old_text = text
        text = retriever.retrieve(WATitleMunger.normalize_title(redir_link), True)
        if text is None:
            # redirect to nonexistent article
            text = old_text
            break
        redirs_done += 1
    if redir_link and redirs_done >= max_redirs:
        sys.stderr.write("Too many redirects encountered.\n")
    return text


class WAPageRetriever(object):
    '''
    retrive the xml page content from a bz2 miltistream xml file
    given an index of the offsets of the streams in the file and
    the page titles in those streams, along with a table of
    contents into the index file by first letter of page titles
    '''
    def __init__(self, xml_file, idx_file, toc_file, verbose):
        '''
        constructor; besides setting instance attributes,
        initializes a few things so we know they need to be
        properly set later
        arguments:
        xml_file - name of the bz2 multistream xml file of pages
        idx_file - name of the index into the bz2 multistream file
        toc_file - name of the toc into the index file
        verbose  - whether or not to display extra messages about processing
        '''
        self.xml_file = xml_file
        self.idx_file = idx_file
        self.toc_file = toc_file
        self.verbose = verbose
        self.xml_fd = None
        self.toc_fd = None
        self.idx_fd = None
        self.title_matches = None  # hash of titles and offsets that partially match, if any
        self.setup_done = False

    def setup(self):
        '''
        call just after initalizing the instance, opens files. note that the bz2
        compressed file is not opened with a decompressor; since
        we seek around in the file we can't do that.
        '''
        if self.setup_done:
            return

        if self.verbose:
            sys.stderr.write("Opening files\n")
        self.xml_fd = open(self.xml_file, "r")
        self.idx_fd = open(self.idx_file, "r")
        self.toc_fd = open(self.toc_file, "r")
        self.setup_done = True

    def teardown(self):
        '''
        call when the instance is no longer needed
        closes all file descriptors and readies
        the instance for a new setup() call if desired
        '''
        if self.verbose:
            sys.stderr.write("Closing files\n")
        if self.xml_fd:
            self.xml_fd.close()
        if self.idx_fd:
            self.idx_fd.close()
        if self.toc_fd:
            self.toc_fd.close()
        self.xml_fd = None
        self.toc_fd = None
        self.idx_fd = None
        self.title_matches = None
        self.setup_done = False

    def retrieve(self, title, exact):
        '''
        retrieve the contents of the xml file with the
        specified page title.
        arguments:
        title -- the page title, case sensitive, with spaces, not underscores
        exact -- true if the title must match exactly, otherwise a list of
                 page titles starting with the specified string is displayed
                 on stdout with a prompt for selection from stdin.
        returns None if no corresponding page title can be found
        '''
        title_unicode = title.decode("utf-8")
        first_char = title_unicode[0]
        idx_offset = self.find_char_in_toc(first_char)
        if idx_offset is None:
            sys.stderr.write("No such title found in toc.\n")
            return None
        if exact:
            if self.verbose:
                sys.stderr.write("Found index offset %s\n" % idx_offset)
            result = self.retrieve_exact(title, idx_offset)
        else:
            titles_hash = self.find_title_in_index(title, idx_offset)
            # if there is only one entry in the hash don't ask the user
            if len(titles_hash.keys()) == 1:
                title = titles_hash.keys()[0]
            else:
                title = get_user_title_choice(titles_hash)
            xml_offset = titles_hash[title]
            result = self.retrieve_exact(title, None, xml_offset)
        return result

    def retrieve_exact(self, title, idx_offset, xml_offset=None):
        '''
        retrieve the contents of the xml file with the
        specified page title, first seeking to the appropriate place
        in the xml file and possibly the index file
        arguments:
        title      -- the page title, case sensitive, with spaces, not underscores
        idx_offset -- the offset in bytes into the index file with the title
                      if this is None, the xmlOffset must be provided
        xml_offset -- the offset into the xml file in bytes of the bz2
                      stream containing the page with the specified title
                      if this is None, the idxOffset must be provided
                      if this is provided it will be used and the idxOffset
                      will be ignored, as it would only be used to look up
                      this value
        returns text if found, None otherwise
        '''
        if xml_offset is None:
            xml_offset = self.find_title_in_index_exact_match(title, idx_offset)
        if xml_offset is None:
            sys.stderr.write("No such title found in index.\n")
            return None
        if self.verbose:
            sys.stderr.write("Found xml offset %s\n" % xml_offset)
        text = self.retrieve_text(title, xml_offset)
        return text

    def find_char_in_toc(self, char):
        '''
        given a (possibly multibyte) character, find its entry
        in the toc file, read the index file offset listed there
        and return it
        arguments:
        char -- character for which to find the toc entry
        returns the index file offset of the character, or None if not found
        '''
        self.toc_fd.seek(0)
        # format of these lines is
        # 14815067:A
        # this is offset, first (unicode) character
        for line in self.toc_fd:
            stripped = line.rstrip('\n')
            fields = stripped.split(':', 1)
            if len(fields) < 2:
                continue
            indexed_char_unicode = fields[1].decode("utf-8")
            if indexed_char_unicode == char:
                return int(fields[0])
            if indexed_char_unicode > char:
                break
        return None

    def find_title_in_index(self, title, offset):
        '''
        find entries beginning with the specified in the index file,
        first seeking to the specified offset
        arguments:
        title  -- page title to be found in index
        offset -- offset into the index file in bytes of
                  the line with the specified title
        returns a hash of matching page titles and their offsets into
        the xml file, or None if no matches were found
        '''
        title_matches = {}
        self.idx_fd.seek(offset)
        title_len = len(title)
        # format of these lines is
        # 9186323419:33202778:A Girl like Me (film)
        # this is offset, page id, page title
        for line in self.idx_fd:
            stripped = line.rstrip("\n")
            fields = stripped.split(':', 2)
            if len(fields) < 2:
                continue
            if fields[2].startswith(title):
                title_matches[fields[2]] = int(fields[0])  # offset into xml file
            if fields[2][:title_len] > title:  # we are past all the matches (if there were any)
                break
        if not len(title_matches.keys()):
            title_matches = None
        return title_matches

    def find_title_in_index_exact_match(self, title, offset):
        '''
        find entry in the index file that matches exactly the specified title,
        first seeking to the specified offset
        arguments:
        title  -- page title to be found in index
        offset -- offset into the index file in bytes of
                  the line with the specified title
        returns the offset of the title in the xml file,
        as listed in the index file, or None if no exact match was found
        '''
        self.idx_fd.seek(offset)
        # format of these lines is
        # 9186323419:33202778:A Girl like Me (film)
        # this is offset, page id, page title
        for line in self.idx_fd:
            stripped = line.rstrip("\n")
            fields = stripped.split(':', 2)
            if len(fields) < 2:
                continue
            if len(fields) < 3:
                sys.stderr.write("Fewer splits than we expected: line %s\n" % line)
            if fields[2] == title:
                return int(fields[0])  # xml offset into file
            if fields[2] > title:
                break
        return None

    def retrieve_text(self, title, offset):
        '''
        retrieve the page text for a given title from the xml file
        this does decompression of a bz2 stream so it's more expsive than
        other parts of this class
        arguments:
        title  -- the page title, with spaces and not underscores, case sensitive
        offset -- the offset in bytes to the bz2 stream in the xml file which contains
                  the page text
        returns the page text or None if no such page was found
        '''
        self.xml_fd.seek(offset)
        unzipper = bz2.BZ2Decompressor()
        out = None
        found = False
        try:
            block = self.xml_fd.read(262144)
            out = unzipper.decompress(block)
        # hope we got enough back to have the page text
        except:
            raise
        # format of the contents (and there are multiple pages per stream):
        #   <page>
        #   <title>AccessibleComputing</title>
        #   <ns>0</ns>
        #   <id>10</id>
        # ...
        #   </page>
        title_regex = re.compile(r"<page>(\s*)<title>%s(\s*)</title>" % re.escape(title))
        while not found:
            match = title_regex.search(out)
            if match:
                found = True
                text = out[match.start():]
                if self.verbose:
                    sys.stderr.write("Found page title, first 600 characters: %s\n" % text[:600])
                break
            # we could have a part of the regex at the end of the string, so...
            if len(out) > 40 + len(title):  # length of the above plus extra whitespace
                out = out[-1 * (40 + len(title)):]
            try:
                block = self.xml_fd.read(262144)
            except:
                # reached end of file (normal case) or
                # something really broken (other cases)
                break
            try:
                out = out + unzipper.decompress(block)
            except EOFError:
                # reached end of bz2 stream
                # EOFError  means we have some data after end of stream, don't care
                pass

        if not found:
            return None

        out = text
        found = False
        text = ""
        while not found:
            ind = out.find("</page>")
            if ind != -1:
                found = True
                if self.verbose:
                    sys.stderr.write("Found end page tag\n")
                text = text + out[:ind + len("</page>")]
                break
            # we could have part of the end page tag at the end of the string
            text = text + out[:-1 * len("</page>") - 1]
            out = out[-1 * len("</page>"):]
            try:
                block = self.xml_fd.read(262144)
            except:
                # reached end of file (normal case) or
                # something really broken (other cases)
                break
            try:
                out = out + unzipper.decompress(block)
            except EOFError:
                # reached end of bz2 stream
                # EOFError  means we have some data after end of stream, don't care
                pass

        # if not found this can be partial text. should we return it? no
        if not found:
            if self.verbose:
                sys.stderr.write("Found partial text but no end page tag. Text follows:\n")
                sys.stderr.write(text)
                sys.stderr.write("\n")
            text = None
        return text


class WATextFormatter(object):
    '''
    format page text for a given title as desired by the caller
    we do this since we don't have a real renderer of wikitext
    with template expansion and all that crapola
    '''
    def __init__(self, text, localized_file_string, localized_category_string):
        '''
        constructor
        arguments:
        text                      -- page text, could also include xml tags and page metadata
        localized_file_string     -- the string 'File' in the local wiki language
        localized_category_string -- the string 'Category' in the local wiki language
        '''
        self.text = text
        self.localized_file_string = localized_file_string
        self.localized_category_string = localized_category_string
        self.formatting_done = False

    def cleanup_links(self):
        '''
        for all links (has [[ ]] and maybe | in them  -- no special treatment for interwiki links
        or categories, sorrry but this is a rough cut), toss the [[ ]] and the pipe arg if any.
        except file and category
        '''
        if self.text is not None:
            nopipes = re.sub(r"\[\[(?!(File|Category|%s|%s))([^\|\]]+)\|([^\]]+)\]\]"
                             % (self.localized_file_string, self.localized_category_string),
                             "\\3", self.text)
            nowikilinks = re.sub(r"\[\[(?!(File|Category|%s|%s))([^\]]+)\]\]"
                                 % (self.localized_file_string, self.localized_category_string),
                                 "\\2", nopipes)
            self.text = nowikilinks

    def cleanup_text(self):
        '''
        convert html entities back into <>"&,
        remove wiki markup for bold/italics, remove <span> tags
        '''
        if self.text is not None:
            noampersands = self.text.replace(
                "&lt;", '<').replace("&gt;", '>').replace(
                    "&quot;", '"').replace("&amp;", '&').replace("&nbsp;", ' ')
            nofontstyling = noampersands.replace("'''", "").replace("''", "")
            nospans = re.sub("</?span[^>]*>", "", nofontstyling)
            self.text = nospans

    def cleanup_refs(self):
        '''
        toss the refs, this should really be overridable by the user. we want this so it's
        easier to read the plaintext of the article, there will already be a ton
        of templates and crap in there
        '''
        if self.text is not None:
            norefs = re.sub("<ref[^>]*>.*?</ref>", "", self.text, flags=re.DOTALL)
            # <ref name="mises.org"/>
            nosimplerefs = re.sub("<ref.*?/>", "", norefs)
            self.text = nosimplerefs

    def cleanup_html_comments(self):
        '''
        toss html (<!-- -->) comments, <nowiki>, <code> and <sup> tags, and <br> tags
        '''
        if self.text is not None:
            nocomments = re.sub(r"<!--.*?-->", "", self.text, flags=re.DOTALL)
            nonowikis = re.sub(r"</?nowiki>", "", nocomments)
            nocodes = re.sub(r"</?code>", "", nonowikis)
            nobrs = re.sub(r"<br\s*/>", "", nocodes)
            nosups = re.sub(r"</?sup>", "", nobrs)
            self.text = nosups

    def do_formatting(self):
        '''
        do all the text formatting in some reasonable order
        and return the formatted text
        '''
        if not self.formatting_done:
            self.cleanup_links()
            self.cleanup_text()
            self.cleanup_refs()
            self.cleanup_html_comments()
            self.formatting_done = True
        return self.text


class WATitleMunger(object):
    '''
    transform page title to the format in the xml file
    or to ordinary plaintext
    '''
    @staticmethod
    def normalize_title(title):
        '''
        # doesn't do much right now. remember how this is only a proof of concept??
        '''
        return title.replace('_', ' ').replace('&', '&amp;').replace('"', "&quot;")

    @staticmethod
    def un_normalize_title(title):
        '''
        # not an exact opposite kids cause of the underscore, that's the breaks
        '''
        return title.replace('&amp;', '&').replace("&quot;", '"')


def usage(message=None):
    '''
    display usage information about the script, after optionally
    displaying a specified message
    arguments:
    message -- message to be displayed before usage information
               if omitted, only the usage information will be shown
    '''
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """
Usage: python wikiarticles.py --title titlestring --xmlfile filename
                 --idxfile filename --tocfile filename [--configfile filename]
                 [--maxredirs num] [--redirtext string] [--cleanup] [--exact]
                 [--textonly] [--verbose]

Given a bz2-compressed multistream xml file of articles, a sorted plain text
index file into the article file, and a plain text toc file for the index,
find and display the xml including article text of any article specified
by title.

The user may specify the first so many characters of the title, in which
case all matching titles will be displayed as a list so that the user
may select the one desired.

If no such title is found, an error message will be displayed.

Titles are case-sensitive for now.

A reasonable front end would parse the xml, strip or expand templates,
do something interesting with citations, references and links, etc.
This script does none of that; it is a proof of concept only.

Arguments:

  --title:         first so many characters of the article title
  --xmlfile:       path to the bz2 compressed xml format article file
  --idxfile:       plain text file which is the index into the bz2 xml file
  --tocfile:       plain text file which is the toc of the index file
  --configfile:    plain text file which contains config options
  --maxredirs:     maximum number of redirects to follow
                   default: 3
  --categorytext:  text of the 'category' string in the wiki's content language
                   default: Category
  --filetext:      text of the 'file' string in the wiki's content language
                   default: File
  --redirtext:     text in capital letters of the 'redirect' string in the
                   wiki's content language
                   default: REDIRECT

Flags:
  --cleanup:   cleanup text (remove refs, font stylings, etc) for ease of reading
               default: false
  --exact:     require exact match of specified title
               default: false
  --textonly:  print only the contents of the xml<text> tag, not the rest of the
               page info
               default: false
  --verbose:   print extra message about what is being done
               default: false

Example:

python %s --exact --xmlfile enwiki-articles-current.xml.bz2 \\\n" % self.whoami)
          --idxfile articles-index-sorted.txt --tocfile index-toc.txt
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def read_config(config_file=None):
    '''
    set up configuration defaults and read overriding values from files in
    the current directory, /etc, and the user's home directory, if they exist
    arguments:
    configFile -- name of the configuration file in the current dir, if any
    returns a ConfigParser object with the configuration values in it
    '''
    home = os.path.dirname(sys.argv[0])
    if not config_file:
        config_file = "wikiarticles.conf"

    # fixme I should really check what order these get read in
    # and which files override which
    files = [
        os.path.join(home, config_file),
        "/etc/wikiarticles.conf",
        os.path.join(os.getenv("HOME"), ".wikiarticles.conf")]

    defaults = {
        # "files": {
        "xmlfile": "",
        "idxfile": "",
        "tocfile": "",
        # "format": {,
        "cleanup": "0",
        "textonly": "0",
        "maxredirs": "3",
        "filetext": "File",
        "categorytext": "Category",
        "redirtext": "REDIRECT"
        }

    conf = ConfigParser.SafeConfigParser(defaults)
    conf.read(files)

    if not conf.has_section("files"):
        conf.add_section("files")
    if not conf.has_section("format"):
        conf.add_section("format")

    return conf


def do_main():
    config_file_name = None
    xml_file_name = None
    index_file_name = None
    toc_file_name = None
    page_title = None
    exact_match = None
    verbose = None
    max_redirs = None
    file_text = None
    category_text = None
    redir_text = None
    cleanup = None
    text_only = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ['xmlfile=', 'idxfile=', 'tocfile=',
                               'title=', 'configfile=', 'maxredirs=',
                               "filetext=", "categorytext=", "redirtext=",
                               'cleanup', 'exact', 'textonly', 'verbose'])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--xmlfile":
            xml_file_name = val
        elif opt == "--idxfile":
            index_file_name = val
        elif opt == "--tocfile":
            toc_file_name = val
        elif opt == "--title":
            page_title = val
        elif opt == "--configfile":
            config_file_name = val
        elif opt == "--maxredirs":
            if not val.isdigit() or int(val) < 0:
                usage("maxredirs must be a non-negative integer.")
            max_redirs = int(val)
        elif opt == "--redirtext":
            redir_text = val
        elif opt == "--cleanup":
            cleanup = True
        elif opt == "--exact":
            exact_match = True
        elif opt == "--textonly":
            text_only = True
        elif opt == "--verbose":
            verbose = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    conf = read_config(config_file_name)

    # check config file for fallbacks.
    if xml_file_name is None:
        xml_file_name = conf.get("files", "xmlfile")
    if index_file_name is None:
        index_file_name = conf.get("files", "idxfile")
    if toc_file_name is None:
        toc_file_name = conf.get("files", "tocfile")
    if max_redirs is None:
        max_redirs = conf.getint("format", "maxredirs")
    if redir_text is None:
        redir_text = conf.get("format", "redirtext")
    if file_text is None:
        file_text = conf.get("format", "filetext")
    if category_text is None:
        category_text = conf.get("format", "categorytext")
    if cleanup is None:
        cleanup = conf.getboolean("format", "cleanup")
    if text_only is None:
        text_only = conf.getboolean("format", "textonly")

    mandatory = [("xmlfile", xml_file_name), ("idxfile", index_file_name),
                 ("tocfile", toc_file_name), ("title", page_title)]
    for (opt_name, val) in mandatory:
        if not val:
            usage("Missing required option '%s'" % opt_name)

    retriever = WAPageRetriever(xml_file_name, index_file_name, toc_file_name, verbose)
    retriever.setup()
    text = retriever.retrieve(WATitleMunger.normalize_title(page_title), exact_match)
    if text:
        text = handle_redirects(text, retriever, max_redirs, redir_text, verbose)

    display(text, file_text, category_text, cleanup, text_only)

    retriever.teardown()


if __name__ == "__main__":
    do_main()
