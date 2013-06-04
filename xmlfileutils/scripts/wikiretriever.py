# -*- coding: utf-8 -*-
import os, re, sys, getopt, httplib, urllib, time, calendar, htmlentitydefs, gzip, bz2, getpass
from xml.etree import ElementTree as ElementTree
from wikifile import File

class WikiRetrieveErr(Exception):
    pass

class WikiConnection(object):
    """Base class for a connection to a MediaWiki wiki, holding authentication
    credentials, wiki name, type of api request, etc.
    This class is responsible for performing the actual GET request and for checking
    the response, for logging in, and for checking maxlag.
    All connections are https but with no certificate checks."""

    def __init__(self, wikiName, username, password, verbose):
        """Constructor. Arguments:
        wikiName        -- host name of the wiki, e.g. en.wikipedia.org
        username        -- username with which to authenticate to the wiki, if any;
                           if not supplied, requests are made anonymously (via the user IP)
        password        -- password for auth to the wiki, if any; if username is
                           supplied and password is not, the user will be
                           prompted to supply one
        verbose         -- if set, display various progress messages on stderr"""

        self.wikiName = wikiName
        self.username = username
        self.password = password
        self.verbose = verbose
        self.loggedIn = False
        self.userAgent = "wikicontentretriever.py/0.1"
        self.queryApiUrlBase = "/w/api.php?action=query&format=xml&maxlag=5"
        self.errorPattern = re.compile("<error code=\"([^\"]+)\"")
        self.lagged = False
        self.cookies = []

    def getUrl(self, url, method = "GET", params = None):
        """Request a specific url and return the contents. On error
        writes an error message to stderr and returns None. Arguments:
        url      -- everything that follows the hostname in a normal url, eg.
                    /w/api.php?action=query&list=allpages&ns=0
        methd    -- GET, PUT, POST etc.
        params   -- dict of name/value query pairs for POST requests"""

        self.lagged = False
        if params:
            params = urllib.urlencode(params)
        try:
            httpConn = httplib.HTTPSConnection(self.wikiName)
            httpConn.putrequest(method, url, skip_accept_encoding = True)
            httpConn.putheader("Accept", "text/html")
            httpConn.putheader("Accept", "text/plain")
            httpConn.putheader("Cookie", "; ".join(self.cookies))
            httpConn.putheader("User-Agent", self.userAgent)
            if params:
                httpConn.putheader("Content-Length", len(params))
                httpConn.putheader("Content-Type", "application/x-www-form-urlencoded")

            httpConn.endheaders()
            if params:
                httpConn.send(params)
            httpResult = httpConn.getresponse()
            if httpResult.status != 200:
                if httpResult.status == 503:
                    contents = httpResult.read()
                    httpConn.close()
                    if contents.find("seconds lagged"):
                        if verbose:
                            sys.stderr.write(contents)
                        self.lagged = True
                        return contents
                sys.stderr.write( "status %s, reason %s\n" %( httpResult.status, httpResult.reason) )
                raise httplib.HTTPException
        except:
            sys.stderr.write("failed to retrieve output from %s\n" % url)
            return None

        contents = httpResult.read()
        httpConn.close()

        # format <error code="maxlag"
        result = self.errorPattern.search(contents)
        if result:
            if result.group(1) == "maxlag":
                self.lagged = True
            else:
                sys.stderr.write("Error '%s' encountered\n" % result.group(1))
                return None
        else:
            self.lagged = False
        return contents

    def login(self):
        """Log in to the wiki with the username given to the class as argument.
        If no such argument was supplied, this method does nothing.
        On success, stores a cookie for use with future requests, on
        error raises an exception"""

        if self.username and not self.loggedIn:
            url = "/w/api.php?action=login"
            params = {"lgname": self.username, "lgpassword": self.password, "format": "xml"}
            contents = self.getUrl(url, "POST", params)
            if not contents:
                sys.stderr.write("Login failed for unknown reason\n")
                raise httplib.HTTPException

            tree = ElementTree.fromstring(contents)
            # format <?xml version="1.0"?><api>
            # <login result="NeedToken" 
            # token="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" 
            # cookieprefix="enwiktionary" 
            # sessionid="yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" />
            # </api>
            p = tree.find("login")
            if p is None:
                sys.stderr.write("Login failed, no login element found in <%s>\n" %  contents)
                raise httplib.HTTPException

            if p.get("result") == "NeedToken":
                wikiprefix = p.get("cookieprefix")
                token = p.get("token")
                url = url + "&lgtoken=%s" % token
                self.cookies = [ "%s_session=%s" % (wikiprefix, p.get("sessionid")) ]
                contents = self.getUrl(url, "POST", params)
                if not contents:
                    sys.stderr.write("Login failed for unknown reason\n")
                    raise httplib.HTTPException
                # format <?xml version="1.0"?><api>
                # <login result="Success" lguserid="518" lgusername="AtouBot"
                # lgtoken="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                # cookieprefix="elwiktionary"
                # sessionid="yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" />
                # </api>
                tree = ElementTree.fromstring(contents)
                p = tree.find("login")
                if p is None:
                    sys.stderr.write("login failed, <%s>\n" %  contents)
                    raise httplib.HTTPException

            if p.get("presult") == "NeedToken":
                sys.stderr.write("Login failed, claiming token needed after second attempt, giving up\n")
                raise httplib.HTTPException
                
            if p.get("result") != "Success":
                sys.stderr.write("Login failed, <%s>\n" %  contents)
                raise httplib.HTTPException

            wikiprefix = p.get("cookieprefix")
            lgtoken = p.get("lgtoken")
            lgusername = p.get("lgusername").encode("utf8")
            lguserid = p.get("lguserid")
            sessionid = p.get("sessionid")

            self.loggedIn = True
            self.cookies = [ "%s_session=%s" %(wikiprefix, sessionid), "%sUserName=%s" % (wikiprefix, lgusername), 
                             "%sUserID=%s" % (wikiprefix, lguserid), "%sToken=%s" % (wikiprefix, lgtoken) ]

class Content(object):
    """Download page content from a wiki, given a WikiConnection object for it.
    This class also provides methods for converting titles into various
    formats (linked, removing sql escaping, etc.)"""

    def __init__(self, wikiConn, titlesFile, outDirName, outFileName, batchSize, maxRetries, verbose):
        """Constructor.  Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        titlesFile  -- path to list of titles for which to retrieve page content
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        batchSize   -- number of pages to download at once (default 500)
        maxRetries  -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        self.wikiConn = wikiConn
        self.titlesFile = titlesFile
        self.outDirName = outDirName
        if not os.path.isdir(self.outDirName):
            os.makedirs(self.outDirName)
        self.batchSize = batchSize
        self.timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        if outFileName:
            self.outFileName = os.path.join(self.outDirName, outFileName)
        else:
            self.outFileName = os.path.join(self.outDirName, "content-%s-%s.gz" % (self.wikiConn.wikiName, self.timestamp))
        self.exportUrl = "/w/index.php?title=Special:Export&action=submit&maxlag=5"
        self.maxRetries = maxRetries
        self.verbose = verbose
        
    def unSqlEscape(self, title):
        """Remove sql escaping from a page title.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        so we unescape:  '  "   \   only, by removing leading \
        Note that in the database all titles are stored with underscores instead of
        spaces so convert those; remove enclosing single quotes too, if the title has them.
        Returns cleaned up title.
        Arguments:
        title   -- page title"""

        # expect: \\\\\" -> \\"
        #         \\\\a -> \\a
        #         \\\a and \\" : impossible
        if title[0] == "'" and title[-1] == "'":
            title = title[1:-1]
        title = title.replace("\\\\", '\\')
        title = title.replace("\\'", "\'")
        title = title.replace('\\"', '"')
        title = title.replace('_', ' ')
        return title
    
    def stripLink(self, title):
        """Remove wikilink markup from title if it exists.
        Returns cleaned up title.
        Arguments:
        title   -- page title"""

        if title.startswith("[[") and title.endswith("]]"):
            return title[2:-2]
        else:
            return title

    def titlesFormat(self,titles):
        """Format titles for content retrieval via the MediaWiki api.
        Returns formatted title list.
        Arguments:
        titles   -- list of page titles"""

        return [ self.unSqlEscape(self.stripLink(t)) for t in titles ]

    def getBatchOfPageContent(self,titles):
        """Get content for one batchsize (for example 500) pages via the MediaWiki api.
        Returns content.  If the pages are large and the batchsize is huge, this
        could consume a lot of memory.
        If the servers are overloaded it will retry up to maxRetries, waiting a few
        seconds between retries.
        Arguments:
        titles   -- list of page titles"""

        titlesFormatted = self.titlesFormat(titles)
        params = {"wpDownload" : "1", "curonly" : "1", "pages" : "\n".join(titlesFormatted) + "\n"}
        self.retries = 0
        while self.retries < self.maxRetries:
            if self.wikiConn.lagged:
                self.retries = self.retries + 1;
                if self.verbose:
                    sys.stderr.write("server lagged, sleeping 5 seconds\n")
                time.sleep(5)
            if self.verbose:
                sys.stderr.write("getting batch of page content via %s\n" % self.exportUrl)
            contents = self.wikiConn.getUrl(self.exportUrl,"POST", params)
            if not self.wikiConn.lagged:
                break
        if self.retries == self.maxRetries:
            raise WikiRetrieveErr("Server databases lagged, max retries %s reached" %self.maxRetries)

        return contents

    def stripSiteFooter(self, content):
        """Remove </mediawiki> footer from complete XML text for page content
        If no such tag is found, this indicates damaged input.
        On error, raises WikiRetrieveErr exception
        Arguments:
        content   -- complete XML text for page content"""

        if not content.endswith("</mediawiki>\n"):
            raise WikiRetrieveErr("no mediawiki end tag found, uh oh.")
        return(content[:-13])

    def stripSiteHeaderAndFooter(self, content):
        """Remove <mediawiki> and <siteinfo>...</siteinfo> header from
        complete XML text for page content, also remove the footer
        </mediawiki> from the end
        If no such tag is found, this indicates damaged input.
        On error, raises WikiRetrieveErr exception
        Arguments:
        content   -- complete XML text for page content"""

        # don't parse, just find </siteinfo>\n in the string and toss everything before that
        start = content.find("</siteinfo>\n")
        if not start:
            raise WikiRetrieveErr("no siteinfo header found, uh oh.")
        if not content.endswith("</mediawiki>\n"):
            raise WikiRetrieveErr("no mediawiki end tag found, uh oh.")
        return(content[start+12:-13])

    def getAllEntries(self):
        """Retrieve page content for all titles in accordance with arguments
        given to constructor, in batches, writing it out to a file.
        On error (failure to retrieve some content), raises WikiRetrieveErr exception"""
        
        self.outputFd = File.openOutput(self.outFileName)
        self.inputFd = File.openInput(self.titlesFile)
        first = True
        count = 0

        eof = False
        while not eof:
            linecount = 0
            titles = []
            while not eof:
                line = self.inputFd.readline()
                if line == "":
                    eof = True
                line = line.strip()
                if line:
                    titles.append(line)
                    linecount = linecount + 1;
                if linecount >= self.batchSize:
                    break
                    
            if (not titles):
                break

            count = count + self.batchSize
            content = self.getBatchOfPageContent(titles)

            if not len(content):
                raise WikiRetrieveErr("content of zero length returned, uh oh.")

            if first:
                first = False
                content = self.stripSiteFooter(content)
            else:
                content = self.stripSiteHeaderAndFooter(content)

            self.outputFd.write(content)

        # cheap hack
        self.outputFd.write("</mediawiki>\n")
        self.outputFd.close()
        self.inputFd.close()

class Entries(object):
    """Base class for downloading page titles from a wiki, given a
    WikiConnection object for it. This class also provides methods for
    converting titles into various formats (linked, sql escaped, etc.)."""

    def __init__(self, wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, maxRetries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of pages to download at once (default 500)
        maxRetries  -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        self.wikiConn = wikiConn
        self.outDirName = outDirName
        if not os.path.isdir(self.outDirName):
            os.makedirs(self.outDirName)
        self.linked = linked
        self.sqlEscaped = sqlEscaped
        self.batchSize = batchSize
        self.maxRetries = maxRetries
        self.timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        if outFileName:
            self.outFileName = os.path.join(self.outDirName, outFileName)
        else:
            self.outFileName = os.path.join(self.outDirName, "titles-%s-%s.gz" % (self.wikiConn.wikiName, self.timestamp))
        self.continueFrom = None
        self.more = None
        self.verbose = verbose

        self.dateFormatter = None
        self.startDateString = None
        self.endDateString = None
        self.startDateSecs = None
        self.endDateSecs = None


        # subclasses should set these up as appropriate
        self.url = None
        self.entryTagName = None
        self.startDate = None
        self.endDate = None
        self.startDateParam = None
        self.endDateParam = None

    def sqlEscape(self, title):
        """Escape title in preparation for it to be written
        to an sql file for import.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        Escapes these characters:  ' "  \   by adding leading \
        Note that in the database all titles are stored with underscores instead of spaces
        so replace those; also enclose the title in single quotes
        Arguments:
        title  -- page title to escape"""

        title = title.replace('\\', "\\\\")  # must insert new backslashs after this step
        title = title.replace("\'", "\\'")
        title = title.replace('"', '\\"')
        title = title.replace(' ', '_')
        return "'" + title + "'"

    def deSanitize(self, title):
        """Convert XML sanitized title to its regular format.
        This expects no newlines, \r or \t in titles and unescapes
        these characters: & " ' < >
        Arguments:
        title   -- title to be desantized"""

        title = title.replace("&quot;", '"')
        title = title.replace("&lt;", '<')
        title = title.replace("&gt;", '>')
        title = title.replace("&#039;", "'")
        title = title.replace("&amp;", '&') # this one must be last
        return title

    def writeTitles(self, titles):
        """Write list of titles to an open file,
        optionally formatting them for sql use
        Arguments:
        titles   -- list of titles to write"""

        for t in titles:
            if sqlEscaped:
                t = self.sqlEscape(t)
            if linked:
                self.outputFd.write("[[%s]]\n" % t)
            else:
                self.outputFd.write("%s\n" % t)

    def getAllEntries(self):
        """Retrieve entries such as page titles from wiki in accordance with arguments
        given to constructor, in batches, writing them out to a file.
        On error (failure to rerieve some titles), raises WikiRetrieveErr exception."""

        self.more = True
        
        if self.startDate:
            self.dateFormatter = Date()
            self.startDateString = self.dateFormatter.formatDate(self.startDate)
            self.endDateString = self.dateFormatter.formatDate(self.endDate)
            self.startDateSecs = self.dateFormatter.getSecs(self.startDateString)
            self.endDateSecs = self.dateFormatter.getSecs(self.endDateString)

        self.outputFd = File.openOutput(self.outFileName)

        count = 0
        while True:
            count = count + self.batchSize
            titles = self.getBatchOfEntries()
            self.writeTitles(titles)
            if not len(titles):
                # not always an error
                break
            # FIXME is there a possibility that there will be a continue elt and
            # we'll be served the same titles again?
            if not self.more:
                break
        self.outputFd.close()

    def extractItemsFromXml(self, tree):
        return [ self.deSanitize(entry.get("title").encode("utf8")) for entry in tree.iter(self.entryTagName) ]

    def getBatchOfEntries(self):
        """Retrieve one batch of entries such as page titles via the MediaWiki api
        If the servers are overloaded it will retry up to maxRetries, waiting a few
        seconds between retries.
        NOTE:
        If getting user contribs worked the way it should, we would get a unique
        continue param which would guarantee that the new batch of titles has no
        overlap with the old batch. However, since the continue param is a timestamp,
        and it's possible that there are multiple entries for that timestamp, and
        it's possible that the previous batch ended in the middle of that timestamp,
        we can't rule out the possibility of dups.
        The caller should therefore deal with potential dup titless from this method.
        At least the defaut batchsize of 500 is large enough that we should never wind
        up in a loop getting the same batch every time.
        See bugs https://bugzilla.wikimedia.org/show_bug.cgi?id=35786 and
        https://bugzilla.wikimedia.org/show_bug.cgi?id=24782 for more info.
        """

        entries = []
        contents = None
        url = self.url

        # start off with an empty param, because the api requires it, see
        # http://www.mediawiki.org/wiki/API:Query#Continuing_queries
        if self.more:
            if (self.continueFrom):
                for key in self.continueFrom.keys():
                    url = url + "&%s=%s" % (key, urllib.pathname2url(self.continueFrom[key]))
            else:
                    url = url + "&%s=%s" % ("continue", "")
        # usercontribs use ucstart (start date param) as its continuation param too,
        # don't want it in the url twice
        if self.startDateString and (not self.continueFrom or not self.startDateParam in self.continueFrom):
            url = url + "&%s=%s" % (self.startDateParam, urllib.pathname2url(self.startDateString))
        if self.endDateString:
            url = url + "&%s=%s" % (self.endDateParam, urllib.pathname2url(self.endDateString))

        self.retries = 0
        while self.retries < self.maxRetries:
            if self.wikiConn.lagged:
                self.retries = self.retries + 1;
                if self.verbose:
                    sys.stderr.write("server lagged, sleeping 5 seconds\n")
                    time.sleep(5)

            if self.verbose:
                sys.stderr.write("getting batch of titles via %s\n" % url)
            contents = self.wikiConn.getUrl(url)
            if not self.wikiConn.lagged:
                break
            if self.retries == self.maxRetries:
                raise WikiRetrieveErr("Server databases lagged, max retries %s reached" %self.maxRetries)

        if contents:
            tree = ElementTree.fromstring(contents)
            # format: 
            #  <continue continue="-||" cmcontinue="page|444f472042495343554954|4020758" />
            #  <continue continue="-||" eicontinue="10|!|600" />
            #  <continue continue="-||" apcontinue="B&amp;ALR" />
            #  <continue continue="-||" ucstart="2011-02-24T22:47:06Z" />
            # etc.
            p = tree.find("continue")
            if p is None:
                self.more = False
            else:
                self.more = True
                self.continueFrom = p.attrib
                for k in self.continueFrom.keys():
                    self.continueFrom[k] = self.continueFrom[k].encode("utf8")

            # format:
            #  <cm ns="10" title="Πρότυπο:-ακρ-" />
            #  <ei pageid="230229" ns="0" title="μερικοί" />
            #  <p pageid="34635826" ns="0" title="B" />
            #  <item userid="271058" user="YurikBot" ns="0" title="Achmet II" />
            # etc.
            entries = self.extractItemsFromXml(tree)

        return entries

class CatTitles(Entries):
    """Retrieves titles of pages in a given category.  Does not include
    subcategories but that might be nice for the future."""

    def __init__(self, wikiConn, catName, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        catName     -- name of category from which to retrieve page titles
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super( CatTitles, self ).__init__(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose)
        self.catName = catName
        self.url = "%s&list=categorymembers&cmtitle=Category:%s&cmprop=title&cmlimit=%d" % ( self.wikiConn.queryApiUrlBase, self.catName, self.batchSize )
        # format <cm ns="10" title="Πρότυπο:-ακρ-" />
        self.entryTagName = "cm"

class EmbeddedTitles(Entries):
    """Retrieves titles of pages that have a specific page embedded in them
    (link, used as template, etc.)"""

    def __init__(self, wikiConn, pageTitle, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        pageTitle   -- title of page for which to find all pages with it embedded
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super( EmbeddedTitles, self ).__init__(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose)
        self.pageTitle = pageTitle
        self.url = "%s&list=embeddedin&eititle=%s&eilimit=%d" % ( self.wikiConn.queryApiUrlBase, self.pageTitle, self.batchSize )
        # format <ei pageid="230229" ns="0" title="μερικοί" />
        self.entryTagName = "ei"

class NamespaceTitles(Entries):
    """Retrieves titles of pages in a given namespace."""

    def __init__(self, wikiConn, namespace, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        namespace   -- number of namespace for which to get page titles
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super( NamespaceTitles, self ).__init__(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose)
        self.namespace = namespace
        self.url = "%s&list=allpages&apnamespace=%s&aplimit=%d" % ( self.wikiConn.queryApiUrlBase, self.namespace, self.batchSize )
        # format <p pageid="34635826" ns="0" title="B" />
        self.entryTagName = "p"

class Users(Entries):
    """Retrieves all user names, ids, editcounts and registration info."""

    def __init__(self, wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the user names as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the user names in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of users to request info for at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super( Users, self ).__init__(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose)
        self.url = "%s&list=allusers&auprop=editcount|registration&aulimit=%d" % ( self.wikiConn.queryApiUrlBase, self.batchSize )
        # format <u userid="146308" name="!" editcount="93" registration="2004-12-04T19:39:42Z" />
        self.entryTagName = "u"

    def writeUserInfo(self, users):
        """Write userinfo to an open file,
        optionally formatting for sql use
        Arguments:
        userinfo  -- list of tuples (username, userid, editcount, registration) to write"""

        for (name, id, editcount, registration) in users:
            if sqlEscaped:
                name = self.sqlEscape(name)
            if linked:
                self.outputFd.write("[[%s]] %s %s '%s'\n" % (name, id, editcount, registration))
            else:
                self.outputFd.write("%s %s %s '%s'\n" % (name, id, editcount, registration))

    def extractItemsFromXml(self, tree):
        return [ ( self.deSanitize(entry.get("name").encode("utf8")), entry.get("userid"), entry.get("editcount"), entry.get("registration") )  for entry in tree.iter(self.entryTagName) ]

    def getAllEntries(self):
        """Retrieve user info from wiki in accordance with arguments
        given to constructor, in batches, writing them out to a file.
        On error (failure to rerieve some batch of users), raises
        WikiRetrieveErr exception."""

        self.more = True

        self.outputFd = File.openOutput(self.outFileName)

        count = 0
        while True:
            count = count + self.batchSize
            users = self.getBatchOfEntries()
            self.writeUserInfo(users)
            if not len(users):
                # not always an error
                break
            # FIXME is there a possibility that there will be a continue elt and
            # we'll be served the same users again?
            if not self.more:
                break
        self.outputFd.close()


class UserContribsTitles(Entries):
    """Retrieves pages edited by a given user, within a specified date range"""

    def __init__(self, wikiConn, userName, startDate, endDate, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose):
        """Constructor. Arguments:
        wikiConn    -- initialized WikiConnection object for a wiki
        startDate   -- starting timestamp for edits, 
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        endDate     -- ending timestamp  for edits, 
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        outDirName  -- directory in which to write any output files
        outFileName -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sqlEscaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batchSize   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super( UserContribsTitles, self ).__init__(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, retries, verbose)
        self.userName = userName
        self.url = "%s&list=usercontribs&ucuser=%s&uclimit=%d&ucprop=title" % ( self.wikiConn.queryApiUrlBase, self.userName, self.batchSize )
        # format: <item userid="271058" user="YurikBot" ns="0" title="Achmet II" />
        self.entryTagName = "item"
        # need these for "&ucstart=$rcstartdate&ucend=$rcenddate"
        self.startDateParam = "ucstart"
        self.endDateParam = "ucend"
        self.startDate = startDate
        self.endDate = endDate

# parse user-supplied dates, compute 'now - d/m/s' expressions,
# format date strings for use in retrieving user contribs (or other lists
# which can be limited by time interval)
class Date(object):
    """Manipulate date and time strings."""

    def __init__(self):
        """Constructor. Duh."""

        self.timePattern = re.compile("\s+([0-9]+):([0-9])+(:[0-9]+)?$")
        self.datePattern = re.compile("^([0-9]{4})-([0-9][0-9]?)-([0-9][0-9]?)$")
        self.incrPattern = re.compile("^(now|today)\s*-\s*([0-9]+)([dhms])$")

    def getDateFormatString(self):
        """Return format string we use with strftime for converting all
        user entered date and time strings to a canonical format"""

        return "%Y-%m-%dT%H:%M:%SZ"

    def getNowMinusIncr(self, dateString):
        """Convert date string in format "now|today [- Xd/h/m/s (default seconds)]
        to YYYY-MM-DDThh:mm:ssZ
        Arguments:
        dateString  -- date string to convert"""

        if dateString == "now" or self.dateString == "today":
            return time.strftime(self.getDateFormatString(),time.gmtime(time.time()))
        result = self.incrPattern.search(dateString)
        if result:
            increment = int(result.group(2))
            incrType = result.group(3)
            if incrType == 'd':
                increment = increment * 60* 60* 24
            elif incrType == 'h':
                increment = increment * 60* 60
            elif incrType == 'm':
                increment = increment * 60
            else:
                # incrType == 's' or omitted
                pass
            return time.strftime(self.getDateFormatString(),time.gmtime(time.time() - increment))
        return None

    def getYMDHMS(self, dateString):
        """Convert date string in form yyyy-MM-dd [hh:mm:ss]
        to form YYY-MM-DDThh:mm:ssZ
        Arguments:
        datestring   -- string to convert"""

        # yyyy-mm-dd [hh:mm:ss]
        years = months = days = hours = mins = secs = 0

        date = dateString
        result = self.timePattern.search(dateString)
        if result:
            date = dateString[:result.start()]
            hours, mins = int(result.group(1)), int(result.group(2))
            if len(result.group(3)):
                secs = int(result.group(3))

        result = self.datePattern.search(date)
        if result:
            years, months, days = int(result.group(1)), int(result.group(2)), int(result.group(3))
        if not years:
            return False
        else:
            return time.strftime(self.getDateFormatString(),(years, months, days, hours,mins, secs, 0, 0, 0))

    def formatDate(self, dateString):
        """Convert user-supplied date argument into canonical format
        YYYY-MM-DDThh:mm:ssZ
        Allowable input formats: 
          now/today [- Xh/m/d/s (default seconds)]
          yyyy-mm-dd [hh:mm:ss]
        Arguments:
        dateString --  string to convert"""

        dateString = dateString.strip()
        if dateString.startswith("now") or dateString.startswith("today"):
            return(self.getNowMinusIncr(dateString))
        return(self.getYMDHMS(dateString))

    def getSecs(self, dateStringFormatted):
        """Given a date string in X format, return the number of seconds since Jan 1 1970
        represented by that date
        Arguments:
        dateStringFormatted  -- date string in the specified format"""

        return calendar.timegm(time.strptime(dateStringFormatted, self.getDateFormatString()))

def getAuthFromFile(authfile, username, password):
    """Get username and password from file, overriding
    them with the values that were passed as args, if any
    returns a tuple of the new username and password
    on error, raises exception
    Arguments:
    username -- username that will override value in file, if not None
    password -- password that will override value in file, if not None"""

    if username and password:
        return(username, password)

    fd = open(authfile,"r")
    for line in fd:
        if line[0] == '#' or line.isspace():
            continue

        (keyword, value) = line.split(None,1)
        value.strip()

        if keyword == "username":
            if not username:
                username = value
        elif keyword == "password":
            if not password:
                password = value
        else:
            raise WikiRetrieveErr("Unknown keyword in auth file <%s>" % keyword)
    fd.close()
    return(username, password)

def usage(message):
    """Display help on all options to stderr and exit.
    Arguments:
    message   -- display this message, with newline added, before
    the standard help output."""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    sys.stderr.write("Usage: python %s --query querytype [--param value] [--wiki wikiname]\n" % sys.argv[0])
    sys.stderr.write("                 [--outputdir dirname] [--outputfile filename]\n")
    sys.stderr.write("                 [--startdate datestring] [--enddate datestring]\n")
    sys.stderr.write("                 [--linked] [--sqlEscaped] [--batchsize batchsize]\n")
    sys.stderr.write("                 [--auth username:password] [--authfile filename] [--verbose]\n")
    sys.stderr.write("\n")
    sys.stderr.write("This script uses the MediaWiki api to download titles of pages in a\n")
    sys.stderr.write("specific category, or that include a specific template, or that were\n")
    sys.stderr.write("edited by a specific user on a specified wiki.\n")
    sys.stderr.write("Alternatively it can retrieve content for a list of titles.\n")
    sys.stderr.write("The script may be run as an an anonymous user on that wiki, or with\n")
    sys.stderr.write("authentication.\n")  
    sys.stderr.write("The path to the output file will be written to stdout just before the program\n")
    sys.stderr.write("exits, if the run was successful.\n")
    sys.stderr.write("Warning: if there happens to be a schema change or namespace change while this\n")
    sys.stderr.write("script is running, the results will be inconsistent and maybe broken. These changes\n")
    sys.stderr.write("are rare but do happen.\n")
    sys.stderr.write("\n")
    sys.stderr.write("--query (-q):      one of 'category', 'embeddedin', 'namespace', 'usercontribs', 'users' or 'content'\n")
    sys.stderr.write("--param (-p):      namndatory for all queries but 'users'\n")
    sys.stderr.write("                   for titles: name of the category for which to get titles or name of the\n")
    sys.stderr.write("                   article for which to get links, or the number of the namespace from which\n")
    sys.stderr.write("                   to get all titles, or the user for which to get changes; for the 'users'\n")
    sys.stderr.write("                   query this option should not be specified\n")
    sys.stderr.write("                   for content: name of the file containing titles for download\n")
    sys.stderr.write("                   for the namespace query, standard namespaces (with their unlocalized names) are:\n")
    sys.stderr.write("                   0    Main (content)   1    Talk\n")
    sys.stderr.write("                   2    User             3    User talk\n")
    sys.stderr.write("                   4    Project          5    Project talk\n")
    sys.stderr.write("                   6    File             7    File talk\n")
    sys.stderr.write("                   8    MediaWiki        9    MediaWiki talk\n")
    sys.stderr.write("                   10   Template         11   Template talk\n")
    sys.stderr.write("                   12   Help             13   Help talk\n")
    sys.stderr.write("                   14   Category         15   Category talk\n")
    sys.stderr.write("                   828  Module           829  Module talk\n")
    sys.stderr.write("--wiki (-w):       name of the wiki from which to get the category titles\n")
    sys.stderr.write("                   default: en.wikipedia.org\n")
    sys.stderr.write("--outputdir (-o):  relative or full path to the directory where all files will\n")
    sys.stderr.write("                   be created; directory will be created if it does not exist\n")
    sys.stderr.write("--outputfile (-O): filename for titles or content output, if it ends in gz or bz2,\n")
    sys.stderr.write("                   the file will be compressed appropriately\n")
    sys.stderr.write("                   default: for title listings, titles-wikiname-yyyy-mm-dd-hhmmss.gz\n")
    sys.stderr.write("                   and for content retrieval, content-wikiname--yyyy-mm-dd-hhmmss.gz\n")
    sys.stderr.write("--startdate (-S):  start date of titles, for usercontribs queries\n")
    sys.stderr.write("--enddate (-E):    end date of titles, for usercontribs queries\n")
    sys.stderr.write("--linked (-l):     write titles as wikilinks with [[ ]] around the text\n")
    sys.stderr.write("--sqlescaped (-s): write titles with character escaping as for sql INSERT statements\n")
    sys.stderr.write("--batchsize (-b):  number of titles to get at once (for bots and sysadmins this\n")
    sys.stderr.write("                   can be 5000, but for other users 500, which is the default)\n")
    sys.stderr.write("--retries (-r):    number of times a given http request will be retried if the\n")
    sys.stderr.write("                   wiki databases are lagged, before giving up\n")
    sys.stderr.write("                   default: 20\n")
    sys.stderr.write("--auth (-a):       username:password if you need to authenticate for the\n")
    sys.stderr.write("                   action or to use a large batchsize; if password is not provided\n")
    sys.stderr.write("                   the user will be prompted to enter one\n")
    sys.stderr.write("--authfile (-A):   name of file containing authentication information; values that\n")
    sys.stderr.write("                   are specified via the auth option will override this\n")
    sys.stderr.write("                   file format: each line contains keyword<spaces>value\n")
    sys.stderr.write("                   lines with blanks or starting with # will be skipped,\n")
    sys.stderr.write("                   keywords are username and password\n")
    sys.stderr.write("--verbose (-v):    display messages about what the program is doing\n")
    sys.stderr.write("--help:            display this usage message\n")
    sys.stderr.write("\n")
    sys.stderr.write("Date format can be one of the following:\n")
    sys.stderr.write("   now|today [- num[d|h|m|s]]    (days, hours, minutes, seconds, default s)\n")
    sys.stderr.write("   yyyy-MM-dd [hh:mm:ss]         (UTC time)\n")
    sys.stderr.write("Examples:\n")
    sys.stderr.write("   today\n")
    sys.stderr.write("   now-30d\n")
    sys.stderr.write("   now-3600 (seconds implied)\n")
    sys.stderr.write("   2013-02-01\n")
    sys.stderr.write("   2013-03-12 14:01:59\n")
    sys.stderr.write("\n")
    sys.stderr.write("Example usage:\n")
    sys.stderr.write("   python %s --query category --param 'Πρότυπα για τα μέρη του λόγου' \\\n" % sys.argv[0])
    sys.stderr.write("             --wiki el.wiktionary.org\n")
    sys.stderr.write("   python %s --query usercontribs --param ArielGlenn --startdate now \\\n" % sys.argv[0])
    sys.stderr.write("             --enddate 2012-05-01 --outputdir junk\n")
    sys.stderr.write("   python %s --query embeddedin --param 'Template:WikiProject Cats' \\\n" % sys.argv[0])
    sys.stderr.write("             -o junk -v\n")
    sys.stderr.write("   python %s -q namespace --param 10 -w as.wikisource.org \\\n" % sys.argv[0])
    sys.stderr.write("             -o junk -v\n")
    sys.stderr.write("   python %s -q users -w el.wikisource.org -o wikisourceusers --sqlescape -v\n" % sys.argv[0])
    sys.stderr.write("   python %s --query content --param page_titles/titles-2013-03-28-064814.gz \\\n" % sys.argv[0])
    sys.stderr.write("             --outputdir junk_content\n")
    sys.exit(1)

if __name__ == "__main__":
    param = None
    query = None
    batchSize = 500
    wikiName = "en.wikipedia.org"
    linked = False  # whether to write the page titles with [[ ]] around them
    sqlEscaped = False # whether to sql-escape the title before writing it
    verbose = False
    outDirName = os.path.join(os.getcwd(), "page_titles")
    outFileName = None
    maxRetries = 20
    username = None
    password = None
    authFile = None
    startDate = None
    endDate = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "q:p:S:E:w:o:O:lsb:r:a:A:vh", ["query=", "param=", "startdate=", "enddate=", "wiki=", "outputdir=", "outputfile=", "linked", "sqlescaped", "batchsize=", "retries=", "auth=", "authfile=", "verbose", "help" ])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in [ "-a", "--auth"]:
            if ':' in val:
                username, password = val.split(':')
            else:
                username = val
        elif opt in ["-A", "--authfile"]:
            authFile = val
        elif opt in ["-p", "--param"]:
            param = val
        elif opt in ["-S", "--startdate"]:
            startDate = val
        elif opt in ["-E", "--enddate"]:
            endDate = val
        elif opt in ["-b", "--batchsize"]:
            if not val.isdigit():
                usage("batch size must be a number")
            batchSize = int(val)
        elif opt in ["-r", "--retries"]:
            if not val.isdigit():
                usage("retries must be a number")
            retries = int(val)
        elif opt in ["-q", "--query"]:
            query = val
        elif opt in ["-w", "--wiki"]:
            wikiName = val
        elif opt in ["-o", "--outputdir"]:
            outDirName = val
        elif opt in ["-O", "--outputfile"]:
            outFileName = val
        elif opt in ["-l", "--linked"]:
            linked = True
        elif opt in ["-s", "--sqlescaped"]:
            sqlEscaped = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Options help:\n")
        else:
            usage("Unknown option specified: %s" % opt )

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    if not query or (query != 'users' and not param):
        usage("Missing mandatory option query or param")

    if authFile:
        (username, password) = getAuthFromFile(authFile, username, password)

    if username and not password:
        password = getpass.getpass("Password: ")
        
    if not query == "usercontribs" and (startDate or endDate):
        usage("startdate or enddate specified for wrong query type")
        
    wikiConn = WikiConnection(wikiName, username, password, verbose)
    wikiConn.login()

    if query != "content":
        if param:
            param =  urllib.pathname2url(param)
    if query == "category":
        retriever = CatTitles(wikiConn, param, outDirName, outFileName, linked, sqlEscaped, batchSize, maxRetries, verbose)
    elif query == "embeddedin":
        retriever = EmbeddedTitles(wikiConn, param, outDirName, outFileName, linked, sqlEscaped, batchSize, maxRetries, verbose)
    elif query == "namespace":
        retriever = NamespaceTitles(wikiConn, param, outDirName, outFileName,  linked, sqlEscaped, batchSize, maxRetries, verbose)
    elif query == "usercontribs":
        retriever = UserContribsTitles(wikiConn, param, startDate, endDate, outDirName, outFileName, linked, sqlEscaped, batchSize, maxRetries, verbose)
    elif query == "content":
        retriever = Content(wikiConn, param, outDirName, outFileName, batchSize, maxRetries, verbose)
    elif query == 'users':
        retriever = Users(wikiConn, outDirName, outFileName, linked, sqlEscaped, batchSize, maxRetries, verbose)
    else:
        usage("Unknown query type specified")

    retriever.getAllEntries()

    # this is the only thing we display to the user, unless verbose is set.
    # wrapper scripts that call this program can grab this in order to do
    # further processing of the titles.
    print retriever.outFileName

    if verbose:
        sys.stderr.write("Done!\n")
