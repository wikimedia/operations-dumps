# -*- coding: utf-8 -*-
import os, re, sys, getopt, urllib, gzip, bz2, subprocess, json, time, select, shutil
from subprocess import Popen, PIPE
from wikifile import File

class WikiContentErr(Exception):
    pass

class Path(object):
    """Make files or paths which contain the lang code,
    project name and date in the filename."""

    def __init__(self, dirName, project = None, lang = None, date = None):
        """Constructor.  Arguments:
        dirName   --  directory name in which files will be located (abs or rel path)
        project   --  name of wiki project type, i.e. 'wikipedia', 'wiktionary', etc.
        lang      --  language code of wiki, i.e. 'en', 'el', etc.
        date      --  datestring in some nice format"""

        self.dir = dirName
        self.project = project
        self.lang = lang
        self.date = date

    def makePath(self, filename):
        """Create a pathname with the filename in the format
        "langcode-project-date-restofname" in the directory
        given to the object when instantiated
        Returns the pathname"""

        return(os.path.join(self.dir, "-".join(filter(None, [ self.lang, self.project, self.date, filename ]))))

    def makeFile(self, filename):
        """Create a filename in the format "langcode-project-date-restofname
        Returns the filename"""

        return("-".join(filter(None, [ self.lang, self.project, self.date, filename ])))

class Command(object):
    """Run a command capturing stdout and optionally displaying stderr
    as it runs"""

    def __init__(self, verbose = False, dryrun = False):
        """Constructor.  Arguments:
        verbose   -- print messages received on stderr from command,
                     also display messages about the command being run
        dryrun    -- don't run the command, show what would have been run"""

        self.dryrun = dryrun
        self.verbose = verbose

    def runCommand(self, command):
        """Run a command, capturing output to stdout and stderr,
        optionally displaying stderr output as it is received
        On nonzero return code from the command, displays an error on stderr.
        Returns:  return code from the command, any output produced to stdout
        the output.
        """

        if type(command).__name__=="list":
            commandString = " ".join(command)
        else:
            commandString = command
        if (self.dryrun or self.verbose):
            if self.dryrun:
                sys.stderr.write("would run %s\n" % commandString)
                return (None, None)
            if self.verbose:
                sys.stderr.write("about to run %s\n" % commandString)

        self._proc = Popen(command, shell = False, stdout = PIPE, stderr = PIPE)

        self._poller = select.poll()
        self._poller.register(self._proc.stdout,select.POLLIN|select.POLLPRI)
        self._poller.register(self._proc.stderr,select.POLLIN|select.POLLPRI)
        self.polledfds = 2  # keep track of active fds
        self.pollAndWait()

        if self._proc.returncode:
            sys.stderr.write("command '%s failed with return code %s\n"
                              % ( commandString, self._proc.returncode ) )

        # let the caller decide whether to bail or not
        return (self._proc.returncode, self.output)

    def pollAndWait(self):
        """Collect output to stdout from a process and optionally
        display messages to stderr from the process, until it
       exits or an error is encountered or its stdout and stderr are closed"""

        self.output = ""
        while self.polledfds:  # if there are active fds
            self.pollOnce()
        self._proc.wait()

    def pollOnce(self):
        """poll process, collect stdout, optionally display stderr,
        waiting up to a second for an event"""

        fds = self._poller.poll(1000)  # once a second is plenty to poll
        if (fds):
            for (fd,event) in fds:
                if event & (select.POLLIN|select.POLLPRI):
                    out = os.read(fd, 1024)
                    if fd == self._proc.stderr.fileno():
                        if (self.verbose):
                            sys.stderr.write(out)
                    else:
                        self.output = self.output + out
                elif (event & (select.POLLHUP |select.POLLNVAL|select.POLLERR)):
                    self._poller.unregister(fd)
                    self.polledfds = self.polledfds - 1  # lower number of active fds

class Converter(object):
    """Convert MediaWiki stub and content XML to page, revision
    and sql tables"""

    def __init__(self, mwxml2sql, outputDir, verbose):
        """Constructor.  Arguments:
        mwxml2sql   -- path to mwxml2sql program which does the conversion
        outputDir   -- output directory into which to place the sql files
        verbose     -- display progress messages about what is being done"""

        self.mwxml2sql = mwxml2sql
        self.outputDir = outputDir
        self.verbose = verbose
        self.runner = Command(verbose = self.verbose)

    def convertContent(self, contentPath, stubsPath, mwVersion):
        """Run the command to convert XML to sql. Raises excption
        on error from the command.  Arguments:
        contentPath  -- path to XML content file (containing full text of pages)
        stubsPath    -- path to XML stubs file corresponding to content file
        mwVersion    -- string eg 1.20 representing the version of MediaWiki for
                        which sql tables will be produced
                        Note that for 1.21 and on, the fields page_content_model,
                        rev_content_format, rev_content_model will always be written,
                        even if the user wishes to install into a wiki with
                        $wgContentHandlerUseDB set to false"""
        
        command = [ self.mwxml2sql, '-s', stubsPath, '-t', contentPath, '-f', os.path.join(self.outputDir, "filteredsql.gz"), "-m", mwVersion ]
        if self.verbose:
            command.append('--verbose')
        (result, junk) = self.runner.runCommand(command)
        if (self.verbose):
            sys.stderr.write(junk)
        if result:
            raise WikiContentErr("Error trying to convert page content to sql tables\n")

class Stubber(object):
    """Produce MediaWiki XML stub file and a separate file with a list
    of page ids, from a XML page content file
    Note that the XML page content file must hae the bytes attribute
    in the text tag (as dumps produced by Special:Export do)
    and the sha1 tag."""

    def __init__(self, outputDir, verbose):
        """Constructor. Arguments:
        outputDir  --  directory where files will be written
        verbose    --  display progress messages"""

        self.outputDir = outputDir
        self.verbose = verbose
        self.runner = Command(verbose = self.verbose)

    def writeStubAndPageIds(self, contentPath, stubsPath, pageIdsPath): 
        """Write an XML stub file (omitting text content) and a
        list of page ids, from a MediaWiki XML page content file.
        Arguments:
        contentPath  -- path to the XML page content file to read
        stubsPath    -- path to the stubs file to write
        pageIdsPath  -- path to the page ids file to write"""
        
        pagePattern = "^\s*<page>"
        compiledPagePattern = re.compile(pagePattern)
        revisionPattern = "^\s*<revision>"
        compiledRevisionPattern = re.compile(revisionPattern)
        idPattern = "^\s*<id>(?P<i>.+)</id>\s*\n$"
        compiledIdPattern = re.compile(idPattern)
        textPattern = '^(?P<s>\s*)<text\s+[^<>/]*bytes="(?P<b>[0-9]+)"'
        compiledTextPattern = re.compile(textPattern)

        inFd = File.openInput(contentPath)
        outFd = File.openOutput(stubsPath)
        outPageIdFd = File.openOutput(pageIdsPath)
        currentTitle = None
        currentTextId = None
        pageId = None

        expectRevId = False
        expectPageId = False

        for line in inFd:
            # FIXME we could jus calculate text len  if the output is missing
            # the bytes attr. (as in dumps not from Special:Export)
            # format in content file:
            #   <text <text xml:space="preserve" bytes="78">
            # format wanted for stubs file:
            #   <text id="11248" bytes="9" />
            if '<' in line:
                result = compiledTextPattern.match(line)
                if result:
                    line = result.group("s") + '<text id="%s" bytes="%s" />\n' % (currentTextId, result.group("b"))
                    outFd.write(line)
                    continue
                elif '</text' in line:
                    continue

                result = compiledPagePattern.match(line)
                if result:
                    expectPageId = True
                    outFd.write(line)
                    continue
                result = compiledRevisionPattern.match(line)
                if result:
                    expectRevId = True
                    outFd.write(line)
                    continue
                if expectPageId:
                    result = compiledIdPattern.match(line)
                    if result:
                        outPageIdFd.write("1:%s\n" % result.group("i"))
                        expectPageId = False
                    outFd.write(line)
                    continue
                if expectRevId:
                    result = compiledIdPattern.match(line)
                    if result:
                        currentTextId = result.group("i")
                        expectRevId = False
                    outFd.write(line)
                    continue
                outFd.write(line)
            else:
                continue  # these are lines of text, we can skip them
        inFd.close()
        outFd.close()
        outPageIdFd.close()

class Retriever(object):
    """Retrieve page titles, page content, or namespace information from a wiki using
    the MediaWiki api"""

    def __init__(self, wcr, outputDir, langCode, project, verbose):
        """Constructor. Arguments:
        outputDir  --  directory where files will be written
        verbose    --  display progress messages"""
        self.wcr = wcr
        self.outputDir = outputDir
        self.langCode = langCode
        self.project = project
        self.verbose = verbose
        self.runner = Command(verbose = self.verbose)

    def getTitlesEmbeddedIn(self, template, outputFile, escaped = False):
        """Run command to retrieve all page titles using a given template.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        template    -- name of the template, includes the 'Template:' string or
                       its localized equivalent on the wiki
        outputFile  -- name of file (not full path) for the list of titles
        escaped     -- whether to sqlescape these titles"""

        command = [ 'python', self.wcr, '-q', 'embeddedin', '-p', template, '-o', self.outputDir, '-O', outputFile, '-w', "%s.%s.org" % (self.langCode, self.project) ]

        if escaped:
            command.append('--sqlescaped')
        if self.verbose:
            command.append('--verbose')
        (result, titlesPath) = self.runner.runCommand(command)
        if result:
            raise WikiContentErr("Error trying to retrieve page titles with embedding\n")
        else:
            titlesPath = titlesPath.strip()
            return titlesPath

    def getTitlesInNamespace(self, ns, outputFile, escaped = False):
        """Run command to retrieve all page titles in a given namespace.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        ns          -- number of the namespace.
        outputFile  -- name of file (not full path) for the list of titles
        escaped     -- whether to sqlescape these titles"""

        command = [ 'python', self.wcr, '-q', 'namespace', '-p', ns, '-o', self.outputDir, '-O', outputFile, '-w', "%s.%s.org" % (self.langCode, self.project) ]
        if escaped:
            command.append('--sqlescaped')
        if self.verbose:
            command.append('--verbose')
        (result, titlesPath) = self.runner.runCommand(command)
        if result:
            raise WikiContentErr("Error trying to retrieve page titles in namespace\n")
        else:
            titlesPath = titlesPath.strip()
            return titlesPath

    def getContent(self, titlesPath, outputFile):
        """Run command to retrieve all page content for a list of page titles.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        titlesPath   -- full path to the list of page titles
        outputFile   -- name of file (not full path) for the page content"""

        command = [ 'python', self.wcr, '-q', 'content', '-p', titlesPath, '-o', self.outputDir, "-O", outputFile, '-w', "%s.%s.org" % (self.langCode, self.project) ]
        if self.verbose:
            command.append('--verbose')
        (result, contentPath) = self.runner.runCommand(command)
        if result:
            raise WikiContentErr("Error trying to retrieve content\n")
        else:
            contentPath = contentPath.strip()
            return contentPath

    def getNsDict(self):
        """Retrieve namespace informtion for a wiki via the MediaWiki api
        and store in in dict form.
        On error raises an exception."""

        # http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json 
        apiUrl = "http://" + self.langCode + "." + self.project + "." + "org/w/api.php" + "?action=query&meta=siteinfo&siprop=namespaces&format=json"
        nsDict = {}
        ufd = urllib.urlopen(apiUrl)
        if str(ufd.getcode()).startswith("2"):
            output = ufd.read()
            ufd.close()
            siteInfo = json.loads(output)
            if 'query' not in siteInfo or 'namespaces' not in siteInfo['query']:
                raise WikiContentErr("Error trying to get namespace information from api\n")
            for k in siteInfo['query']['namespaces'].keys():
                if '*' in siteInfo['query']['namespaces'][k]:
                    nsDict[k] = siteInfo['query']['namespaces'][k]['*'].encode('utf8')
                else:
                    raise WikiContentErr("Error trying to get parse namespace information\n")
            return nsDict
        else:
            code = ufd.getcode()
            ufd.close()
            raise WikiContentErr("Error trying to retrieve namespace info: %s\n" % code);

        return nsDict

class Titles(object):
    """Manipulate lists and dicts of wiki page titles"""

    def __init__(self, nsDict, nsDictByString):
        """Constructor.  Arguments:
        nsDict          -- dictionary of namespace entries, { num1 : name1, num2 : name2... }
        nsDictByString  -- dictionary of namespace entries, { name1 : num1, name2 : num2... }
        Note that the namespace numbers are strings of digits, not ints"""

        self.nsDict = nsDict
        self.nsDictByString = nsDictByString

        self.list = [] # list of all titles but templates, with namespace prefix
        self.listTemplates = [] # list of all template titles, with namespace prefix
        self.dict = {} # dict without namespace prefix but values are { ns1 : True, ns2 : True } etc

    def addRelatedTitlesFromFile(self, filename, relatedNsList, nsList):
        """Read list of titles from file, for those in one of the
        specified namespaces, convert the title to one from its related
        namespace (i.e. if it was in Category talk, convert to Category,
        if it was in File talk, convert to File, etc.) and add to title
        list and dict. Arguments:
        filename       -- full path to list of titles
        relatedNsList  -- list of namespaces wanted, e.g. [ "4", "6", "12" ]
        nsList         -- list of namespaces to convert from, in the same order as the
                          related NsList, e.g. [ "5", "7", "13" ]"""

        # don't pass templates in here, we do those separately
        # because it could be a huge list and we want the user
        # to be able to save and reuse it 
        fd = File.openInput(filename)
        for line in fd:
            line = line.strip()
            sep = line.find(":")
            if sep != -1:
                prefix = line[:sep]
                if prefix in self.nsDictByString:
                    # main, file, category, project talk namespaces
                    if self.nsDictByString[prefix] in relatedNsList:
                        noPrefixTitle = line[sep+1:]
                        # convert to file, category, project namespace
                        relatedNs = str(int(self.nsDictByString[prefix]) - 1)
                        if (self.nsDict[relatedNs]):
                            newTitle = self.nsDict[relatedNs] + ":" + noPrefixTitle 
                        else:
                            newTitle = noPrefixTitle  # main namespace titles
                        self.list.append(newTitle)
                        if noPrefixTitle in self.dict:
                            self.dict[noPrefixTitle][relatedNs] = True
                        else:
                            self.dict[noPrefixTitle] = { relatedNs : True }
                    # file, category, project talk namespaces
                    elif self.nsDictByString[prefix] in nsList:
                        ns = self.nsDictByString[prefix]
                        noPrefixTitle = line[sep+1:]
                        self.list.append(noPrefixTitle)
                        if noPrefixTitle in self.dict:
                            self.dict[noPrefixTitle][ns] = True
                        else:
                            self.dict[noPrefixTitle] = { ns : True }
            elif "0" in nsList:
                # main namespace, won't be caught above
                self.list.append(line)
                if line in self.dict:
                    self.dict[line]["0"] = True
                else:
                    self.dict[line] = { "0" : True }
        fd.close()

    def addTitlesFromFile(self, filename, ns):
        """add titles from a file to the title list and dict.
        Note that template titles get added to a different title list
        than the rest, for separate processing
        Arguments:
        filename   -- full path to file containing page titles
        ns         -- number (string of digits) of namespace of page titles to
                      grab from file"""

        fd = File.openInput(filename)
        prefix = self.nsDict[ns] + ":"
        prefixLen = len(prefix)
        for line in fd:
            if line.startswith(prefix):
                if ns == "10": # special case bleah
                    self.listTemplates.append(line[:-1]) # lose newline
                else:
                    self.list.append(line[:-1]) # lose newline
                noPrefixTitle = line[prefixLen:-1]
                if noPrefixTitle in self.dict:
                    self.dict[noPrefixTitle][ns] = True
                else:
                    self.dict[noPrefixTitle] = { ns : True }

    def uniq(self):
        """Remove duplicates from the lists of titles"""

        self.list = list(set(self.list))
        self.listTemplates = list(set(self.listTemplates))

class Filter(object):
    """Filter dumps of MediaWiki sql tables against a list f pageids, keeping
    only the rows for pageids in the list"""

    def __init__(self, sqlFilter, outputDir, verbose):
        """Constructor. Arguments:
        outputDir  --  directory where files will be written
        verbose    --  display progress messages"""
        self.sqlFilter = sqlFilter
        self.outputDir = outputDir
        self.verbose = verbose
        self.runner = Command(verbose = self.verbose)

    def filter(self, input, output, filterPath):
        """Run command to filter an sql table dump against certain values,
        optinally writing out only certain columns from each row.
        Arguments:
        input           -- full path to sql file for input
        output          -- filename (not full path) to write filtered sql output
        filterPath      -- full path to file containing filter values in form column:value
                           (starting with column 1)"""

        command = [ self.sqlFilter, '-s', input, '-o', os.path.join(self.outputDir,output) ]
        if (filterPath):
            command.extend(['-f', filterPath ])
        if self.verbose:
            command.append('--verbose')
            (result, junk) = self.runner.runCommand(command)
        if result:
            raise WikiContentErr("Error trying to filter sql tables\n")
        return

def extendedUsage():
    """Show extended usage information, explaining how to
    run just certain steps of this program"""

    sys.stderr.write("This script has several steps:\n")
    sys.stderr.write("retrievetitles   -- retrieve titles and content for pages from the wiki\n")
    sys.stderr.write("converttitles    -- convert titles to non-talk page titles, discard titles not in the\n")
    sys.stderr.write("                    main, file, category, project talk namespaces\n")
    sys.stderr.write("retrievecontent  -- retrieve titles and content for pages from the wiki\n")
    sys.stderr.write("makestubs        -- write a stub xml file and a pageids file from downloaded content\n")
    sys.stderr.write("convertxml       -- convert retrieved content to page, revision and text sql tables\n")
    sys.stderr.write("filtersql        -- filter previously downloaded sql table dumps against page ids\n")
    sys.stderr.write("             of the page content for import\n")
    sys.stderr.write("By default each of these will be done in order; to skip one pass the corresponding\n")
    sys.stderr.write("no<stepname> e.g. --nofiltersql, --noconvertxml\n")
    sys.stderr.write("\n")
    sys.stderr.write("By providing some or all of the output files to a step you can skip part or all of it.\n")
    sys.stderr.write("All output files from the last skipped step must be provided for the program to run.\n")
    sys.stderr.write("Retrievetitles outputfiles:\n")
    sys.stderr.write("--titles        path of file containing main content (possibly talk page) titles with the template\n")
    sys.stderr.write("--mwtitles      path of file containing all mediawiki namespace titles for the wiki\n")
    sys.stderr.write("--mdltitles     path of file containing all module namespace titles for the wiki\n")
    sys.stderr.write("--tmpltitles    path of file containing all template namespace titles for the wiki\n")
    sys.stderr.write("\n")
    sys.stderr.write("Converttitles outputfiles:\n")
    sys.stderr.write("--titleswithprefix       path of file containing all titles except for templates for import\n")
    sys.stderr.write("--tmpltitleswithprefix   path of file containing all template namespace titles for this wiki\n")
    sys.stderr.write("                if already retrieved e.g. during a previous run\n")
    sys.stderr.write("\n")
    sys.stderr.write("Retrievecontent outputfiles:\n")
    sys.stderr.write("--maincontent   path of file containing all content except templates for import\n")
    sys.stderr.write("--tmplcontent   path of file containing all template namespace content for import\n")
    sys.stderr.write("--content       path of file containing all content for import\n")
    sys.stderr.write("\n")
    sys.stderr.write("Makestub outputfles:\n")
    sys.stderr.write("--stubs         path to file containing stub XML of all content to be imported\n")
    sys.stderr.write("--pageids       path to file containing pageids of all content to be imported\n")
    sys.stderr.write("\n")
    return

def usage(message = None, extended = None):
    """Show usage and help information. Arguments:
    message   -- message to be shown (e.g. error message) before the help
    extended  -- show exended help as well"""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    sys.stderr.write("Usage: python %s --template name --sqlfiles pathformat\n" % sys.argv[0])
    sys.stderr.write("          [--lang langcode] [--project name] [--batchsize]\n")
    sys.stderr.write("          [--output directory] [--auth username:password]\n")
    sys.stderr.write("          [--sqlfilter path] [--mwxml2sql] [--wcr path]\n")
    sys.stderr.write("          [--verbose] [--help] [--extendedhelp] \n")
    sys.stderr.write("\n")

    if (extended):
        sys.stderr.write("Additional options for skipping various steps of the processing\n")
        sys.stderr.write("are listed below.\n")
        sys.stderr.write("\n")

    sys.stderr.write("This script uses the MediaWiki api and Special:Export to download pages\n")
    sys.stderr.write("with a specific template, including category and Wikipedia (or other wiki)\n")
    sys.stderr.write("pages, all templates and all system messages, js and css.\n")
    sys.stderr.write("For example, pages in a specific wikiproject on some Wikipedias can be\n")
    sys.stderr.write("retrieved by specifying the name of the template included on the articles'\n")
    sys.stderr.write("Talk pages; though the articles themselves do not include the template,\n")
    sys.stderr.write("this script will find the titles via the Talk pages which do have the template.\n")
    sys.stderr.write("This content is converted into the appropriate sql tables for import.\n")
    sys.stderr.write("It is also used to generate a list of page ids against which sql table\n")
    sys.stderr.write("dumps of the wiki are filtered for import.\n")
    sys.stderr.write("At the end of the process the user should have a directory of sql files for\n")
    sys.stderr.write("import into a new wiki, which will contain all content needed except for media.\n")
    sys.stderr.write("The script may be run as an an anonymous user on that wiki, or with\n")
    sys.stderr.write("authentication.\n")  
    sys.stderr.write("\n")
    sys.stderr.write("Options:\n")
    sys.stderr.write("\n")
    sys.stderr.write("--template      name of template for which to download content; this should be\n")
    sys.stderr.write("                the name of a template which is included on all articles or their\n")
    sys.stderr.write("                talk pages, e.g. 'Template:WikiProject Lepidoptera'\n")
    sys.stderr.write("--sqlfiles      path including file format string, to sql files from the most recent dump\n")
    sys.stderr.write("                of the wiki from which you are retrieving content\n")
    sys.stderr.write("                the format string must contain a '{t}' which will be replaced with the\n")
    sys.stderr.write("                appropriate sql table name\n")
    sys.stderr.write("                example: dump/enwiki-20130304-{t}.sql.gz would be expanded to the table files\n")
    sys.stderr.write("                dump/enwiki-20130304-category.sql.gz, dump/enwiki-20130304-pagelinks.sql.gz, etc.\n")
    sys.stderr.write("--mwversion     version of MediaWiki such as '1.20' for which sql files should be produced from\n")
    sys.stderr.write("                the content; these should match the version from which the downloaded sql table\n")
    sys.stderr.write("                dumps were produced\n")
    sys.stderr.write("--lang          language code of wiki to download from (en, fr, el etc.), default: en\n")
    sys.stderr.write("--project       name project to download from (wikipedia, wiktionary, etc), default: wikipedia\n")
    sys.stderr.write("--batchsize     number of pages to download at once, if you don't have a lot of memory\n")
    sys.stderr.write("                consider specifying 100 or 50 here, default: 500 (the maximum)\n")
    sys.stderr.write("--output        directory into which to put all resulting files, default: './new_wiki'\n")
    sys.stderr.write("--auth          username, optionally a colon and the password, for connecting to\n")
    sys.stderr.write("                the wiki; if no password is specified the user will be prompted for one\n")
    sys.stderr.write("\n")
    sys.stderr.write("--sqlfilter     path to sqlfilter program, default: ./sqlfilter\n")
    sys.stderr.write("--mwxml2sql     path to mwxml2sql program, default: ./mwxml2sql\n")
    sys.stderr.write("--wcr           path to wikicontentretriever script, default: ./wcr\n")
    sys.stderr.write("\n")
    sys.stderr.write("--verbose       print progress messages to stderr\n")
    sys.stderr.write("--help          show this usage message\n")
    sys.stderr.write("--extendedhelp  show this usage message plus extended help\n")
    sys.stderr.write("\n")

    if (extended):
        extendedUsage()

    sys.stderr.write("Example usage:")
    sys.stderr.write("\n")
    sys.stderr.write("python %s --template 'Template:Wikiproject Lepidoptera' \\\n" % sys.argv[0])
    sys.stderr.write("             --sqlfiles '/home/ariel/dumps/en-mar-2013/enwiki-20130304-{t}.sql.gz' --verbose\n")
    sys.exit(1)
    
def initSteps(optDict):
    """Initialize vars for running each step, by default we will run them"""

def processStepOption(stepToSkip, odict):
    """Process options that specify skipping a step.
    Arguments:
    stepToSkip -- name of the option without the leading '--'
                  and without the 'no'"""

    if stepToSkip == "retrievetitles":
        odict['retrieveTitles'] = False
    elif stepToSkip == "converttitles":
        odict['convertTitles'] = False
    elif stepToSkip == "retrievecontent":
        odict['retrieveContent'] = False
    elif stepToSkip == "makestubs":
        odict['makeStubs'] = False
    elif stepToSkip == "convertxml":
        odict['convertXML'] = False
    elif stepToSkip == "filtersql":
        odict['filterSql'] = False

def processFileOption(fileOpt, value, odict):
    """Process options specifying output files to reuse.
    Raiss exception if the file doesn't exist.
    Arguments:
    fileOpt  -- the name of the file option without the leading '--'
    value    -- the file path"""

    if not os.path.exists(value):
        usage("specified file %s for %s does not exist or is not a file" % (value, fileOpt))

    if fileOpt == "titles":
        odict['titlesPath'] = value
    elif fileOpt == "mwtitles":
        odict['mediawikiTitlesPath'] = value
    elif fileOpt == "mdltitles":
        odict['moduleTitlesPath'] = value
    elif fileOpt == "tmpltitles":
        odict['templateTitlesPath'] = value
    elif fileOpt == "titleswithprefix":
        odict['mainTitlesWithPrefixPath'] = value
    elif fileOpt == "tmpltitleswithprefix":
        odict['tmplTitlesWithPrefixPath'] = value
    elif fileOpt == "maincontent":
        odict['mainContentPath'] = value
    elif fileOpt == "tmplcontent":
        odict['templateContentPath'] = value
    elif fileOpt == "content":
        odict['contentPath'] = value
    elif fileOpt == "stubs":
        odict['stubsPath'] = value
    elif fileOpt == "pageids":
        odict['pageIdsPath'] = value

if __name__ == "__main__":

    o = {} # stash all opt vars in here

    # init main opt vars
    for opt in [ 'template', 'sqlFiles', 'mwVersion', 'outputDir', 'username', 'password' ]:
        o[opt] = None

    o['project'] = "wikipedia"
    o['langCode'] = "en"
    o['batchSize'] = 500

    cwd = Path(os.getcwd())
    o['sqlfilter'] = cwd.makePath("sqlfilter")
    o['wcr'] = cwd.makePath("wikiretriever.py")
    o['mwxml2sql'] = cwd.makePath("mwxml2sql")

    # init step opt vars
    for opt in [ 'retrieveTitles', 'convertTitles', 'retrieveContent', 'makeStubs', 'convertXML', 'filterSql' ]:
        o[opt] = True

    # init file opt vars
    for opt in [ 'titlesPath', 'mediawikiTitlesPath', 'moduleTitlesPath', 'templateTitlesPath',
               'mainTitlesWithPrefixPath', 'tmplTitlesWithPrefixPath', 'mainContentPath',
               'templateContentPath', 'contentPath', 'stubsPath', 'pageIdsPath']:
        o[opt] = None

    verbose = False

    # option handling
    mainOptions = ["template=", "sqlfiles=", "mwversion=", "lang=", "project=", "batchsize=", "output=", "auth=" ]
    cmdOptions = [ "sqlfilter=", "mwxml2sql=", "wcr=" ]

    steps = [ "retrievetitles", "converttitles", "retrievecontent", "makestubs", "convertxml", "filtersql" ]
    skipStepFlags = [ "no" + s  for s in steps ]

    convertTitlesOptions = [ "titles=", "mwtitles=", "mdltitles=", "tmpltitles=" ]
    retrieveContentOptions = [ "titleswithprefix=", "tmpltitleswithprefix=" ]
    makeStubsOptions = [ "maincontent=", "tmplcontent=", "content=" ]
    convertXMLfilterSqlOptions = [ "stubs=", "pageids=" ]

    files = [ fopt[:-1] for fopt in convertTitlesOptions + retrieveContentOptions + makeStubsOptions + convertXMLfilterSqlOptions ]

    miscFlags = [ "verbose", "help", "extendedhelp" ]

    allOptions = mainOptions + cmdOptions + skipStepFlags + convertTitlesOptions + retrieveContentOptions + makeStubsOptions + convertXMLfilterSqlOptions + miscFlags
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", allOptions)
    except:
        usage("Unknown option specified")

    for (opt, val) in options:

        # main opts
        if opt == "--template":
            o['template'] = val;
        elif opt == "--sqlfiles":
            o['sqlFiles'] = val
        elif opt == "--mwversion":
            o['mwVersion'] = val
        elif opt == "--lang":
            o['langCode'] = val
        elif opt == "--project":
            o['project'] = val
        elif opt == "--batchsize":
            if not val.isdigit():
                usage("batch size must be a number")
            o['batchSize'] = int(val)
        elif opt == "--output":
            o['outputDir'] = val
        elif opt == "--auth":
            if ':' in val:
                o['username'], o['password'] = val.split(':')
            else:
                o['username'] = val

        # command opts
        elif opt == "--sqlfilter":
            o['sqlfilter'] = val
        elif opt == "--mwxml2sql":
            o['mwxml2sql'] = val
        elif opt == "--wcr":
            o['wcr'] = val

        # step options
        elif opt.startswith("--no"):
            processStepOption(opt[4:], o)
            
        # file options
        elif opt[2:] in files:
            processFileOption(opt[2:],val, o)
            
        # misc flags
        elif opt == "--verbose":
            verbose = True
        elif opt == "--help":
            usage("Options help:\n")
        elif opt == "--extendedhelp":
            usage("Options help:\n", True)
        else:
            usage("Unknown option specified: %s" % opt )

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    # output files will have this date in their names
    date = time.strftime("%Y-%m-%d-%H%M%S",time.gmtime(time.time()))
    out = Path(o['outputDir'], o['langCode'], o['project'], date)

    # processing begins
    if o['retrieveTitles']:
        if not o['wcr']:
            usage("in retrieveTitles: Missing mandatory option wcr.")
        if not o['template']:
            usage("in retrieveTitles: Missing mandatory option template.")
        if not ':' in o['template']:
            usage("in retrieveTitles: template option should start with 'Template:' or the equivalent in the wiki's language")
        if not o['mwVersion']:
            usage("in retrieveTitles: Missing mandatory option mwversion.")

        if (verbose):
           sys.stderr.write("Retrieving page titles from wiki\n")

        r = Retriever(o['wcr'], o['outputDir'], o['langCode'], o['project'], verbose)
        if not o['titlesPath']:
            # get titles corresponding to the template
            o['titlesPath'] = r.getTitlesEmbeddedIn(o['template'], out.makeFile("main-titles.gz"))
            if verbose:
                sys.stderr.write("main content titles file produced: <%s>\n" % o['titlesPath'])

        if not o['mediawikiTitlesPath']:
            # get the mediawiki page titles
            o['mediawikiTitlesPath'] = r.getTitlesInNamespace("8", out.makeFile("mw-titles.gz"))
            if verbose:
                sys.stderr.write("mediawiki titles file produced: <%s>\n" % o['mediawikiTitlesPath'])

        if not o['moduleTitlesPath']:
            # get the module (lua) page titles
            o['moduleTitlesPath'] = r.getTitlesInNamespace("828", out.makeFile("mod-titles.gz"))
            if verbose:
                sys.stderr.write("modules (lua) titles file produced: <%s>\n" % o['moduleTitlesPath'])

        if not o['templateTitlesPath']:
            # get the template page titles
            o['templateTitlesPath'] = r.getTitlesInNamespace("10", out.makeFile("tmpl-titles.gz"))
            if verbose:
                sys.stderr.write("templates titles file produced: <%s>\n" % o['templateTitlesPath'])

        if (verbose):
           sys.stderr.write("Done retrieving page titles from wiki, have %s, %s, %s and %s\n" % (o['titlesPath'], o['mediawikiTitlesPath'], o['moduleTitlesPath'], o['templateTitlesPath']))

    if o['convertTitles']:
        if not o['titlesPath'] or not o['mediawikiTitlesPath'] or not o['moduleTitlesPath'] or not o['templateTitlesPath']:
            usage("Missing mandatory option for skipping previous step.", True)
        if not o['wcr']:
            usage("Missing mandatory option wcr.")

        if (verbose):
           sys.stderr.write("Converting retrieved titles \n")
        
        r = Retriever(o['wcr'], o['outputDir'], o['langCode'], o['project'], verbose)

        # get namespaces from the api
        nsDict = r.getNsDict()

        nsDictByString = {}
        for nsnum in nsDict.keys():
            nsDictByString[nsDict[nsnum]] = nsnum

        if verbose:
            sys.stderr.write("namespace dicts assembled\n")

        # get list of titles with prefix, not the talk pages but the actual ones, (for use for download) - without dups
        # also create a hash with title, list of ns for this title (it will have at least one entry in the list)
        t = Titles(nsDict, nsDictByString)

        # check main, file, category, project talk namespaces and convert to
        # main, file, category, project talk namespaces        
        t.addRelatedTitlesFromFile(o['titlesPath'], [ "1", "5", "7", "15" ], [ "0", "4", "6", "14" ])

        if verbose:
            sys.stderr.write("page title hash assembled\n")

        t.addTitlesFromFile(o['mediawikiTitlesPath'], "8")
        if verbose:
            sys.stderr.write("mediawiki titles added to page title hash\n")

        t.addTitlesFromFile(o['moduleTitlesPath'], "828")
        if verbose:
            sys.stderr.write("module titles added to page title hash\n")

        t.addTitlesFromFile(o['templateTitlesPath'], "10")
        if verbose:
            sys.stderr.write("template titles added to page title hash\n")

        t.uniq()

        o['mainTitlesWithPrefixPath'] = out.makePath("main-titles-with-nsprefix.gz")
        outFd = File.openOutput(o['mainTitlesWithPrefixPath'])
        for line in t.list:
            outFd.write(line + "\n")
        outFd.close()

        o['tmplTitlesWithPrefixPath'] = out.makePath("tmpl-titles-with-nsprefix.gz")
        outFd = File.openOutput(o['tmplTitlesWithPrefixPath'])
        for line in t.listTemplates:
            outFd.write(line + "\n")
        outFd.close()

        if (verbose):
           sys.stderr.write("Done converting retrieved titles, have %s and %s\n" % (o['mainTitlesWithPrefixPath'], o['tmplTitlesWithPrefixPath']))

    if o['retrieveContent']:
        if not o['mainTitlesWithPrefixPath'] or not o['tmplTitlesWithPrefixPath']:
            usage("in RetrieveContent: Missing mandatory option for skipping previous step.", True)

        if (verbose):
           sys.stderr.write("Retrieving page content from wiki\n")

        if not o['templateContentPath']:
            # filter out the template titles from the mainTitlesWithPrefixPath file and just download the rest
            o['templateContentPath'] = r.getContent(o['tmplTitlesWithPrefixPath'],out.makeFile("template-content.gz"))
            if verbose:
                sys.stderr.write("content retrieved from template page titles\n")
                
        if not o['mainContentPath']:
            o['mainContentPath'] = r.getContent(o['mainTitlesWithPrefixPath'], out.makeFile("rest-content.gz"))
            if verbose:
                sys.stderr.write("content retrieved from page titles\n")

        o['contentPath'] = out.makePath("content.gz")
        File.combineXML([ o['templateContentPath'], o['mainContentPath'] ], o['contentPath'])

        if (verbose):
           sys.stderr.write("Done retrieving page content from wiki, have %s, %s and %s\n" % ( o['templateContentPath'], o['mainContentPath'], o['contentPath']))

    if o['makeStubs']:
        if not o['contentPath']:
            usage("in MakeStubs: Missing mandatory option for skipping previous step.", True)
        
        if (verbose):
           sys.stderr.write("Generating stub XML file and pageids file from downloaded content\n")
        s = Stubber(o['outputDir'], verbose)
        # generate stub XML file for converting sql and list of page ids for filtering sql
        o['stubsPath'] = out.makePath("stubs.gz")
        o['pageIdsPath'] = out.makePath("pageids.gz")
        s.writeStubAndPageIds(o['contentPath'], o['stubsPath'], o['pageIdsPath'])
        if (verbose):
           sys.stderr.write("Done generating stub XML file and pageids file from downloaded content, have %s and %s\n" % (o['stubsPath'], o['pageIdsPath']))

    if o['convertXML']:
        if not o['contentPath']:
            usage("in ConvertXML: Missing mandatory option for skipping previous step.", True)
        if not o['mwxml2sql']:
            usage("in ConvertXML: Missing mandatory option mwxml2sql.")

        if (verbose):
            sys.stderr.write("Converting content to page, revision, text tables\n")
        c = Converter(o['mwxml2sql'], o['outputDir'], verbose)
        # convert the content file to page, revision and text tables
        c.convertContent(o['contentPath'], o['stubsPath'], o['mwVersion'])
        if verbose:
            sys.stderr.write("Done converting content to page, revision, text tables\n")

    if o['filterSql']:
        if not o['pageIdsPath']:
            usage("in FilterSql: Missing mandatory option for skipping previous step.", True)
        if not o['sqlFiles']:
            usage("in FilterSql: Missing mandatory option sqlfiles.")
        if not o['sqlfilter']:
            usage("in FilterSql: Missing mandatory option sqlfilter.")

        if verbose:
           sys.stderr.write("Filtering sql tables against page ids for import\n")

        f = Filter(o['sqlfilter'], o['outputDir'], verbose)
        # filter all the sql tables (which should be in some nice directory)
        # against the pageids in pageidsPath file
        for table in [ "categorylinks", "externallinks", "imagelinks", "interwiki", 
            "iwlinks", "langlinks", "page_props", "page_restrictions",
            "pagelinks", "protected_titles", "redirect", "templatelinks" ]:
            sqlFileName = o['sqlFiles'].format(t=table)
            filteredFileName = os.path.basename(sqlFileName)
            f.filter(sqlFileName,
                     filteredFileName, 
                     o['pageIdsPath'])
        if (verbose):
           sys.stderr.write("Done filtering sql tables against page ids for import\n")

        # the one file we can't filter, it's not by pageid as categories might not have pages
        # so we'll have to import it wholesale... (or you can ignore them completely)
        sqlFileName = o['sqlFiles'].format(t='category')
        newFileName = os.path.join(o['outputDir'],os.path.basename(sqlFileName))
        if verbose:
            sys.stderr.write("about to copy %s to %s\n" % (sqlFileName, newFileName))
        shutil.copyfile(sqlFileName, newFileName)

    if (verbose):
        sys.stderr.write("Done!\n")
    sys.exit(0)
