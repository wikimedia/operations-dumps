import getopt, os, sys, re, codecs, bz2, ConfigParser

class WAPageRetriever(object):
    # retrive the xml page content from a bz2 miltistream xml file
    # given an index of the offsets of the streams in the file and
    # the page titles in those streams, along with a table of
    # contents into the index file by first letter of page titles
    def __init__(self, xmlFile, idxFile, tocFile, verbose):
        # constructor; besides setting instance attributes,
        # initializes a few things so we know they need to be
        # properly set later
        # arguments:
        # xmlFile - name of the bz2 multistream xml file of pages
        # idxFile - name of the index into the bz2 multistream file
        # tocFile - name of the toc into the index file
        # verbose - whether or not to display extra messages about processing
        self.xmlFile = xmlFile
        self.idxFile = idxFile
        self.tocFile = tocFile
        self.verbose = verbose
        self.xmlFd = None
        self.tocFd = None
        self.idxFd = None
        self.titleMatches = None # hash of titles and offsets that partially match, if any
        self.setupDone = False

    def setup(self):
        # call just after initalizing the instance, opens files. note that the bz2
        # compressed file is not opened with a decompressor; since
        # we seek around in the file we can't do that.
        if self.setupDone:
            return

        if verbose:
            sys.stderr.write("Opening files\n")
        self.xmlFd = open(self.xmlFile, "r")
        self.idxFd = open(self.idxFile, "r")
        self.tocFd = open(self.tocFile, "r")
        self.setupDone

    def teardown(self):
        # call when the instance is no longer needed
        # closes all file descriptors and readies
        # the instance for a new setup() call if desired
        if verbose:
            sys.stderr.write("Closing files\n")
        if self.xmlFd:
            self.xmlFd.close()
        if self.idxFd:
            self.idxFd.close()
        if self.tocFd:
            self.tocFd.close()
        self.xmlFd = None
        self.tocFd = None
        self.idxFd = None
        self.titleMatches = None
        self.setupDone = False

    def retrieve(self, title, exact):
        # retrieve the contents of the xml file with the
        # specified page title.
        # arguments:
        # title -- the page title, case sensitive, with spaces, not underscores
        # exact -- true if the title must match exactly, otherwise a list of
        #          page titles starting with the specified string is displayed
        #          on stdout with a prompt for selection from stdin.
        # returns None if no corresponding page title can be found
        titleUnicode = title.decode("utf-8")
        firstChar = titleUnicode[0]
        idxOffset = self.findCharInToc(firstChar)
        if idxOffset is None:
            sys.stderr.write("No such title found in toc.\n")
            return None
        if exact:
            if self.verbose:
                sys.stderr.write("Found index offset %s\n" % idxOffset)
            result = self.retrieveExact(title, idxOffset)
        else:
            titlesHash = self.findTitleInIndex(title, idxOffset)
            # if there is only one entry in the hash don't ask the user
            if len(titlesHash.keys()) == 1:
                title = titlesHash.keys()[0]
            else:
                title = self.getUserTitleChoice(titlesHash)
            xmlOffset = titlesHash[title]
            result = self.retrieveExact(title, None, xmlOffset)
        return result

    def retrieveExact(self, title, idxOffset, xmlOffset=None):
        # retrieve the contents of the xml file with the
        # specified page title, first seeking to the appropriate place
        # in the xml file and possibly the index file
        # arguments:
        # title     -- the page title, case sensitive, with spaces, not underscores
        # idxOffset -- the offset in bytes into the index file with the title
        #              if this is None, the xmlOffset must be provided
        # xmlOffset -- the offset into the xml file in bytes of the bz2
        #              stream containing the page with the specified title
        #              if this is None, the idxOffset must be provided
        #              if this is provided it will be used and the idxOffset
        #              will be ignored, as it would only be used to look up
        #              this value
        # returns text if found, None otherwise
        if xmlOffset is None:
            xmlOffset = self.findTitleInIndexExactMatch(title, idxOffset)
        if xmlOffset is None:
            sys.stderr.write("No such title found in index.\n")
            return None
        if self.verbose:
                sys.stderr.write("Found xml offset %s\n" % xmlOffset)
        text = self.retrieveText(title, xmlOffset)
        return text

    def findCharInToc(self, char):
        # given a (possibly multibyte) character, find its entry
        # in the toc file, read the index file offset listed there
        # and return it
        # arguments:
        # char -- character for which to find the toc entry
        # returns the index file offset of the character, or None if not found
        self.tocFd.seek(0)
        # format of these lines is
        # 14815067:A
        # this is offset, first (unicode) character
        for line in self.tocFd:
            stripped = line.rstrip('\n')
            fields = stripped.split(':', 1)
            if  len(fields) < 2:
                next
            indexedCharUnicode = fields[1].decode("utf-8")
            if indexedCharUnicode == char:
                return int(fields[0])
            if indexedCharUnicode > char:
                break
        return None

    def findTitleInIndex(self, title, offset):
        # find entries beginning with the specified in the index file,
        # first seeking to the specified offset
        # arguments:
        # title  -- page title to be found in index
        # offset -- offset into the index file in bytes of
        #           the line with the specified title
        # returns a hash of matching page titles and their offsets into
        # the xml file, or None if no matches were found
        titleMatches = {}
        self.idxFd.seek(offset)
        titleLen = len(title)
        # format of these lines is
        # 9186323419:33202778:A Girl like Me (film)
        # this is offset, page id, page title
        for line in self.idxFd:
            stripped = line.rstrip("\n")
            fields = stripped.split(':', 2)
            if  len(fields) < 2:
                next
            if fields[2].startswith(title):
                titleMatches[fields[2]] = int(fields[0]) # offset into xml file
            if fields[2][:titleLen] > title: # we are past all the matches (if there were any)
                break
        if not len(titleMatches.keys()):
            titleMatches = None
        return titleMatches

    def findTitleInIndexExactMatch(self, title, offset):
        # find entry in the index file that matches exactly the specified title,
        # first seeking to the specified offset
        # arguments:
        # title  -- page title to be found in index
        # offset -- offset into the index file in bytes of
        #           the line with the specified title
        # returns the offset of the title in the xml file, 
        # as listed in the index file, or None if no exact match was found
        self.idxFd.seek(offset)
        # format of these lines is
        # 9186323419:33202778:A Girl like Me (film)
        # this is offset, page id, page title
        for line in self.idxFd:
            stripped = line.rstrip("\n")
            fields = stripped.split(':', 2)
            if  len(fields) < 2:
                next
            if len(fields) < 3:
                sys.stderr.write("Fewer splits than we expected: line %s\n" % line)
            if fields[2] == title:
                return int(fields[0]) # xml offset into file
            if fields[2] > title:
                break
        return None
        
    def retrieveText(self, title, offset):
        # retrieve the page text for a given title from the xml file
        # this does decompression of a bz2 stream so it's more expsive than
        # other parts of this class
        # arguments:
        # title  -- the page title, with spaces and not underscores, case sensitive
        # offset -- the offset in bytes to the bz2 stream in the xml file which contains
        #           the page text
        # returns the page text or None if no such page was found
        self.xmlFd.seek(offset)
        bz = bz2.BZ2Decompressor()
        out = None
        found = False
        try:
            block = self.xmlFd.read(262144)
            out = bz.decompress(block)
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
        titleRegex = re.compile("<page>(\s*)<title>%s(\s*)</title>" % re.escape(title))
        while not found:
            match = titleRegex.search(out)
            if match:
                found = True
                text = out[match.start():]
                if self.verbose:
                    sys.stderr.write("Found page title, first 600 characters: %s\n" % text[:600])
                break
            # we could have a part of the regex at the end of the string, so...
            if len(out) > 40 + len(title): # length of the above plus extra whitespace
                out = out[-1 *(40 + len(title)):]
            try:
                block = self.xmlFd.read(262144)
            except:
                # reached end of file (normal case) or
                # something really broken (other cases)
                break
            try:
                out = out + bz.decompress(block)
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
            text = text + out[:-1 * len("</page>") -1]
            out = out[-1 * len("</page>"):]
            try:
                block = self.xmlFd.read(262144)
            except:
                # reached end of file (normal case) or
                # something really broken (other cases)
                break
            try:
                out = out + bz.decompress(block)
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

    def getUserTitleChoice(self, titleHash):
        # show a numbered list of page titles on stdout and read the
        # caller's choice on stdin
        # I guess this is a poor person's pager
        # arguments:
        # titleHash -- hash of page titles and their offsets into the xml file
        # returns: the offset into the xml file for the title selected
        titles = titleHash.keys()
        titles.sort()
        total = len(titles)

        choice = None
        start = 0
        batchSize = 30
        print "Multiple titles found, please choose from the following."
        while not choice:
            (action, choice) = self.getChoiceFromBatch(titles, start, batchSize)
            if choice:
                return titles[choice-1]
            else:
                start = self.processAction(action, start, batchSize, total)

    def getChoiceFromBatch(self, titles, start, batchSize):
        # display titles from start to start + batchsize, with count in front
        # ask the caller for a title number or an action
        # actions may be Q (quit), N (next batch), B (previous batch), R (redisplay)
        # if caller enters nothing, treat that as default (R)
        # if caller enters something else, whine and treat that as default (R) too
        # arguments:
        # titles     -- full list of titles
        # start      -- display from this point in the list
        # batchSize  -- how many titles to display
        # returns a tuple of (action, title number) where one or the other of these
        # may be None

        # yay python, it will silently ignore the fact that you requested
        # more things in the list than exist. (no this is not sarcasm)
        count = start
        for line in titles[start:start+batchSize]:
            print "%s) %s" % (count+1, WATitleMunger.unNormalizeTitle(line))
            count += 1
        print
        print "Enter number of choice, or Q/N/P/R to quit/next page/prev page/redisplay page (default R): ",
        choice = sys.stdin.readline()
        choice = choice.strip()
        if not choice:
            choice = 'R'
        if choice.isdigit():
            num = int(choice)
            if num < 1 or num > len(titles):
                print "Bad number given."
                return("R", None)
            return(None, num)
        else:
            choice = choice.capitalize()
            if choice in [ 'N', 'P', 'Q', 'R' ]:
                return(choice, None)
            else:
                print "Bad choice given."
                return("R", None)

    def processAction(self, action, start, batchSize, total):
        # given a caller action,
        # update title list display pointer to the appropriate position
        # arguments:
        # action    --  Q (quit), N (next batch), P (prev batch), or anything else
        # start     --  title list display pointer, a batch of titles from the list
        #               will be displayed starting from this number
        # batchSize -- how many titles are dispayed in a batch
        # total     -- total titles in the list
        # returns: updated title list display pointer, or exits at user request
        # (action Q)
        # note that any action other than Q/N/P will result in the default R (redisplay
        # current batch of titles) which means no change, return existing value.  This
        # includes the None action.
        if action == 'N' or action == 'n':
            if start + batchSize < total:
                start += batchSize
            else:
                print "End of list reached."
        elif action == 'P' or action == 'p':
            if start > batchSize:
                start = start - batchSize
            else:
                print "Beginning of list reached."
        elif action == 'Q' or action == 'q':
            print "Exiting at user's request."
            sys.exit(0)
        return(start)

class WATextFormatter(object):
    # format page text for a given title as desired by the caller
    # we do this since we don't have a real renderer of wikitext
    # with template expansion and all that crapola
    def __init__(self, text, localizedFileString, localizedCategoryString):
        # constructor
        # arguments:
        # text                    -- page text, could also include xml tags and page metadata
        # localizedFileString     -- the string 'File' in the local wiki language
        # localizedCategoryString -- the string 'Category' in the local wiki language
        self.text = text
        self.localizedFileString = localizedFileString
        self.localizedCategoryString = localizedCategoryString
        self.formattingDone = False

    def cleanupLinks(self):
        # for all links (has [[ ]] and maybe | in them  -- no special treatment for interwiki links
        # or categories, sorrry but this is a rough cut), toss the [[ ]] and the pipe arg if any.
        # except file and category
        if self.text is not None:
            nopipes = re.sub("\[\[(?!(File|Category|%s|%s))([^\|\]]+)\|([^\]]+)\]\]" %( self.localizedFileString, self.localizedCategoryString ), "\\3", self.text)
            nowikilinks = re.sub("\[\[(?!(File|Category|%s|%s))([^\]]+)\]\]" % ( self.localizedFileString, self.localizedCategoryString ),"\\2", nopipes)
            self.text = nowikilinks

    def cleanupText(self):
        # convert html entities back into <>"&, remove wiki markup for bold/italics, remove <span> tags
        if self.text is not None:
            noampersands = self.text.replace("&lt;", '<').replace("&gt;",'>').replace("&quot;",'"').replace("&amp;",'&').replace("&nbsp;",' ')
            nofontstyling = noampersands.replace("'''","").replace("''","")
            nospans = re.sub("</?span[^>]*>","", nofontstyling)
            self.text = nospans

    def cleanupRefs(self):
        # toss the refs, this should really be overridable by the user. we want this so it's
        # easier to read the plaintext of the article, there will already be a ton
        # of templates and crap in there
        if self.text is not None:
            norefs = re.sub("<ref[^>]*>.*?</ref>","", self.text, flags = re.DOTALL)
            # <ref name="mises.org"/>
            nosimplerefs = re.sub("<ref.*?/>", "", norefs)
            self.text = nosimplerefs

    def cleanupHtmlComments(self):
        # toss html (<!-- -->) comments, <nowiki>, <code> and <sup> tags, and <br> tags
        if self.text is not None:
            nocomments = re.sub("<!--.*?-->","", self.text, flags = re.DOTALL)
            nonowikis = re.sub("</?nowiki>","", nocomments)
            nocodes = re.sub("</?code>","", nonowikis)
            nobrs = re.sub("<br\s*/>","",nocodes)
            nosups = re.sub("</?sup>","",nobrs)
            self.text = nosups

    def doFormatting(self):
        # do all the text formatting in some reasonable order
        # and return the formatted text
        if not self.formattingDone:
            self.cleanupLinks()
            self.cleanupText()
            self.cleanupRefs()
            self.cleanupHtmlComments()
            self.formattingDone = True
        return(self.text)

class WAXMLExtractor(object):
    # get various things from the xml page text
    def __init__(self, XML):
        # constructor
        # arguments:
        # XML -- the xml text of the page, including the <page>...</page>
        #        tags and everything in between
        self.XML = XML
        self.text = None

    def getText(self):
        # get the contents of the <text>...</text> tags if needed,
        # returns the contents found or None if none found
        if self.text is None:
            if self.XML is not None:
                match = re.search("<text[^>]*>(.*?)</text>", self.XML, flags = re.DOTALL)
                if match:
                    self.text = match.group(1)
        return self.text

class WATextExtractor(object):
    # retrieve various things from page text
    # right now various = redirection info, but this could have more things later
    def __init__(self, text, localizedRedirString):
        # constructor
        # arguments:
        # text                 -- the raw text dug out from the <text> tags of page content
        # localizedRedirString -- the string 'REDIR' in the wiki content language
        self.text = text
        self.localizedRedirString = localizedRedirString
        self.redirect = None

    def getRedirect(self):
        # look for and set page title of a redirect link in the text, if any
        # returns the redirction link or None if none was found
        # format: <text ...>#REDIRECT [[link|show to reader]] ...
        # fixme the redirect keyword really should be case insensitive but
        # have we done any unicode stuff here? nope, so that ain't happening
        if self.redirect is None:
            if self.text is not None:
                redirRegex = re.compile("<text [^>]+>\s*#(REDIRECT|%s)\s*\[\[([^\|\]]+)" % self.localizedRedirString)
                match = redirRegex.search(self.text)
                if match:
                    if verbose:
                        sys.stderr.write("Found a redirect in the page text: %s\n" % match.group(2))
                    self.redirect = match.group(2)
        return self.redirect


class WATitleMunger(object):
    # transform page title to the format in the xml file
    # or to ordinary plaintext

    @staticmethod
    def normalizeTitle(title):
        # doesn't do much right now. remember how this is only a proof of concept??
        return title.replace('_', ' ').replace('&','&amp;').replace('"', "&quot;")

    @staticmethod
    def unNormalizeTitle(title):
        # not an exact opposite kids cause of the underscore, that's the breaks
        return title.replace('&amp;','&').replace("&quot;", '"')

class WATextDisplay(object):
    # process and display text of a page from the xml file of page content
    def __init__(self, fileString, categoryString, clean = False, textOnly = False):
        # constructor
        # arguments:
        # fileString     --  the string 'File' in the wiki's content language
        # categoryString --  the string 'Category' in the wiki's content language
        # clean          --  whether or not to clean up various tags etc or leave the raw text
        #                    as retrieved from the xml file
        # textOnly       --  whether or not to include the metadata and other stuff for the page
        #                    as retrieved from the xml file or just the content from the <text> tags
        self.fileString = fileString
        self.categoryString = categoryString
        self.clean = clean
        self.textOnly= textOnly
    
    def display(self, text):
        # display the text, optionally extracting just the content
        # from the <text> tags and optionally doing some cleanup
        # on the text before display
        # arguments:
        # text -- the page text from the xml file, everything between
        # <page>...</page>
        if text is None:
            print "No page text for that title found."
            return

        if self.textOnly:
            xe = WAXMLExtractor(text)
            text = xe.getText()
                          
        if self.clean:
            tf = WATextFormatter(text, self.fileString, self.categoryString)
            text = tf.doFormatting()

        print text

class WAErrorHandler(object):
    # display warning and error message
    def __init__(self, whoami):
        # constructor
        # arguments:
        # whoami -- the name of the script being executed
        self.whoami = whoami

    def usage(self, message = None):
        # display usage information about the script, after optionally
        # displaying a specified message
        # arguments:
        # message -- message to be displayed before usage information
        #            if omitted, only the usage information will be shown
        if message:
            sys.stderr.write("%s\n" % message)
        sys.stderr.write("Usage: python %s --title titlestring --xmlfile filename\n" % self.whoami)
        sys.stderr.write("           --idxfile filename --tocfile filename [--configfile filename]\n")
        sys.stderr.write("           [--maxredirs num] [--redirtext string] [--cleanup] [--exact]\n")
        sys.stderr.write("           [--textonly] [--verbose]\n")
        sys.stderr.write("\n")
        sys.stderr.write("Given a bz2-compressed multistream xml file of articles, a sorted plain text\n")
        sys.stderr.write("index file into the article file, and a plain text toc file for the index,\n")
        sys.stderr.write("find and display the xml including article text of any article specified\n")
        sys.stderr.write("by title.\n")
        sys.stderr.write("\n")
        sys.stderr.write("The user may specify the first so many characters of the title, in which\n")
        sys.stderr.write("case all matching titles will be displayed as a list so that the user\n")
        sys.stderr.write("may select the one desired.\n")
        sys.stderr.write("\n")
        sys.stderr.write("If no such title is found, an error message will be displayed.\n")
        sys.stderr.write("\n")
        sys.stderr.write("Titles are case-sensitive for now.\n")
        sys.stderr.write("\n")
        sys.stderr.write("A reasonable front end would parse the xml, strip or expand templates,\n")
        sys.stderr.write("do something interesting with citations, references and links, etc.\n")
        sys.stderr.write("This script does none of that; it is a proof of concept only.\n")
        sys.stderr.write("\n")
        sys.stderr.write("Arguments:\n")
        sys.stderr.write("--title:         first so many characters of the article title\n")
        sys.stderr.write("--xmlfile:       path to the bz2 compressed xml format article file\n")
        sys.stderr.write("--idxfile:       plain text file which is the index into the bz2 xml file\n")
        sys.stderr.write("--tocfile:       plain text file which is the toc of the index file\n")
        sys.stderr.write("--configfile:    plain text file which contains config options\n")
        sys.stderr.write("--maxredirs:     maximum number of redirects to follow\n")
        sys.stderr.write("                 default: 3\n")
        sys.stderr.write("--categorytext:  text of the 'category' string in the wiki's content language\n")
        sys.stderr.write("                 default: Category\n")
        sys.stderr.write("--filetext:      text of the 'file' string in the wiki's content language\n")
        sys.stderr.write("                 default: File\n")
        sys.stderr.write("--redirtext:     text in capital letters of the 'redirect' string in the\n")
        sys.stderr.write("                 wiki's content language\n")
        sys.stderr.write("                 default: REDIRECT\n")
        sys.stderr.write("\n")
        sys.stderr.write("Flags:\n")
        sys.stderr.write("--cleanup:   cleanup text (remove refs, font stylings, etc) for ease of reading\n")
        sys.stderr.write("             default: false\n")
        sys.stderr.write("--exact:     require exact match of specified title\n")
        sys.stderr.write("             default: false\n")
        sys.stderr.write("--textonly:  print only the contents of the xml<text> tag, not the rest of the\n")
        sys.stderr.write("             page info\n")
        sys.stderr.write("             default: false\n")
        sys.stderr.write("--verbose:   print extra message about what is being done\n")
        sys.stderr.write("             default: false\n")
        sys.stderr.write("\n")
        sys.stderr.write("Example:\n")
        sys.stderr.write("python %s --exact --xmlfile enwiki-articles-current.xml.bz2 \\\n" % self.whoami)
        sys.stderr.write("          --idxfile articles-index-sorted.txt --tocfile index-toc.txt\n")
        sys.exit(1)

class WARedirectHandler(object):
    # follow redirect links in the page text
    def __init__(self, maxRedirs, redirText, verbose):
        # constructor
        # arguments:
        # maxRedirs -- follow this many redirect links until giving up, if 0 then
        #              don't follow any (need this so we avoid redirection loops)
        # redirText -- the string 'REDIRECT' in the wiki's content language
        # verbose   -- whether or not to display messages about processing being done
        self.maxRedirs = maxRedirs
        self.redirText = redirText
        self.verbose = verbose

    def handleRedirects(self, text, retriever):
        # follow redirect link in page text, retrieve the target page text, and
        # check that til we reach a page that's not a redirect or we hit the
        # maxRedirs limit or we hit a redirect to a nonexistent page
        # arguments:
        # text      -- initial page text as retrieved from the xml file,
        #              without any cleanup etc.
        # retriever -- WAPageRetriever object (used to follow the redirects)
        # returns the page text, either of the first non-redirect or the
        # last redirect before going over the redir follow limit, or
        # the last page text before following a link to a nonexistent page
        if not text:
            return text
        redirsDone = 0
        redirLink = None
        while redirsDone < self.maxRedirs:
            if self.verbose:
                sys.stderr.write("Checking for redirects in text, redirs done already:%s\n" % redirsDone)
            te = WATextExtractor(text, self.redirText)
            redirLink = te.getRedirect()
            if not redirLink:
                break
            oldText = text
            text = retriever.retrieve(WATitleMunger.normalizeTitle(redirLink), True)
            if text is None:
                # redirect to nonexistent article
                text = oldText
                break
            redirsDone += 1
        if redirLink and redirsDone >= maxRedirs:
            sys.stderr.write("Too many redirects encountered.\n")
        return text

def readConfig(configFile=None):
    # set up configuration defaults and read overriding values from files in
    # the current directory, /etc, and the user's home directory, if they exist
    # arguments:
    # configFile -- name of the configuration file in the current dir, if any
    # returns a ConfigParser object with the configuration values in it
    home = os.path.dirname(sys.argv[0])
    if (not configFile):
        configFile = "wikiarticles.conf"

    # fixme I should really check what order these get read in
    # and which files override which
    files = [
        os.path.join(home,configFile),
        "/etc/wikiarticles.conf",
        os.path.join(os.getenv("HOME"), ".wikiarticles.conf")]

    defaults = {
        #"files": {
        "xmlfile": "",
        "idxfile": "",
        "tocfile": "",
        #"format": {,
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

if __name__ == "__main__":
    configFileName = None
    xmlFileName = None
    indexFileName = None
    tocFileName = None
    pageTitle = None
    exactMatch = None
    verbose = None
    maxRedirs = None
    fileText = None
    categoryText = None
    redirText = None
    cleanup = None
    textOnly = None

    errs = WAErrorHandler(sys.argv[0])

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", ['xmlfile=', 'idxfile=', 'tocfile=', 'title=', 'configfile=', 'maxredirs=', "filetext=", "categorytext=", "redirtext=", 'cleanup', 'exact', 'textonly', 'verbose' ])
    except:
        errs.usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--xmlfile":
            xmlFileName = val
        elif opt == "--idxfile":
            indexFileName = val
        elif opt == "--tocfile":
            tocFileName = val
        elif opt == "--title":
            pageTitle = val
        elif opt == "--configfile":
            configFileName = val
        elif opt == "--maxredirs":
            if not val.isdigit() or int(val) < 0:
                errs.usage("maxredirs must be a non-negative integer.")
            maxRedirs = int(val)
        elif opt == "--redirtext":
            redirText = val
        elif opt == "--cleanup":
            cleanup = True
        elif opt == "--exact":
            exactMatch = True
        elif opt == "--textonly":
            textOnly = True
        elif opt == "--verbose":
            verbose = True

    if (len(remainder) > 0):
        errs.usage("Unknown option specified")

    conf = readConfig(configFileName)
    
    # check config file for fallbacks.
    if xmlFileName is None:
        xmlFileName = conf.get("files", "xmlfile")
    if indexFileName is None:
        indexFileName = conf.get("files", "idxfile")
    if tocFileName is None:
        tocFileName = conf.get("files", "tocfile")
    if maxRedirs is None:
        maxRedirs = conf.getint("format", "maxredirs")
    if redirText is None:
        redirText = conf.get("format", "redirtext")
    if fileText is None:
        fileText = conf.get("format", "filetext")
    if categoryText is None:
        categoryText = conf.get("format", "categorytext")
    if cleanup is None:
        cleanup = conf.getboolean("format", "cleanup")
    if textOnly is None:
        textOnly = conf.getboolean("format", "textonly")

    mandatory = [ ("xmlfile", xmlFileName), ("idxfile", indexFileName), ("tocfile", tocFileName), ("title", pageTitle) ]
    for (optName, val) in mandatory:
        if not val:
            errs.usage("Missing required option '%s'" % optName)

    pr = WAPageRetriever(xmlFileName, indexFileName, tocFileName, verbose)
    pr.setup()
    text = pr.retrieve(WATitleMunger.normalizeTitle(pageTitle), exactMatch)
    if text:
        rh = WARedirectHandler(maxRedirs, redirText, verbose)
        text = rh.handleRedirects(text, pr)

    td = WATextDisplay(fileText, categoryText, cleanup, textOnly)
    td.display(text)

    pr.teardown()
