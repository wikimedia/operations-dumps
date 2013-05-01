# -*- coding: utf-8 -*-
import os, re, sys, getopt, urllib, gzip, bz2, subprocess, json, time, select, shutil, string
from wikifile import File

class WikiContentErr(Exception):
    pass

class NsDict(object):

    def __init__(self, langCode, project, verbose = False):
        """Constructor. Arguments:
        langCode   -- language code of project, like en el etc.
        project    -- type of project, like wiktionary, wikipedia, etc.
        verbose    --  display progress messages"""
        self.langCode = langCode
        self.project = project
        self.verbose = verbose

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

class TitlesDict(object):
    def __init__(self, nsDictByString):
        """Constructor. Arguments:
        nsDictByString  -- hash of nstitle => nsnum"""
        self.nsDictByString = nsDictByString

    def getTitlesDict(self,sqlFile):
        """Arguments:
        sqlFile         -- file containing pageid whitespace nsnum whitespace pagetitle where the title
                           is expected to be sql escaped and can be enclosed with single quotes"""
        fd = File.openInput(sqlFile)
        t = {}
        for line in fd:
            (pageid, ns, title) = line.split(' ',3)
            ns = int(ns)
            if title in t:
                t[title][ns] = pageid
            else:
                t[title] = { ns: pageid }
        return t

class LoggingXml(object):
    def __init__(self, nsDictByString, titlesDict, xmlFile, outputFile, userOutFile):
        """Constructor. Arguments:
        nsDictByString  -- hash of nstitle => nsnum
        titlesDict      -- hash of pagetitle => [ pageid, nsnum ]
        xmlFile         -- path to filename with logging.xml
        logOutFile      -- path to logging output filename"""

        self.nsDictByString = nsDictByString
        self.titlesDict = titlesDict
        self.xmlFile = xmlFile
        self.logOutFile = logOutFile
        self.userOutFile = userOutFile

        self.logitemPattern = "^\s*<logitem>\s*\n$"
        self.compiledLogitemPattern = re.compile(self.logitemPattern)
        self.idPattern = "^\s*<id>(?P<i>.+)</id>\s*\n$"
        self.compiledIdPattern = re.compile(self.idPattern)
        self.timestampPattern = "^\s*<timestamp>(?P<t>.+)</timestamp>\s*\n$"
        self.compiledTimestampPattern = re.compile(self.timestampPattern)
        self.contributorPattern = "^\s*<contributor>\n$"
        self.compiledContributorPattern = re.compile(self.contributorPattern)
        self.usernamePattern = "^\s*<username>(?P<u>.+)</username>\s*\n$"
        self.compiledUsernamePattern = re.compile(self.usernamePattern)
        self.endContributorPattern = "^\s*</contributor>\n$"
        self.compiledEndContributorPattern = re.compile(self.endContributorPattern)
        self.commentPattern = "^\s*<comment>(?P<c>.+)</comment>\s*\n$"
        self.compiledCommentPattern = re.compile(self.commentPattern, re.DOTALL)
        self.typePattern = "^\s*<type>(?P<t>.+)</type>\s*\n$"
        self.compiledTypePattern = re.compile(self.typePattern)
        self.actionPattern = "^\s*<action>(?P<a>.+)</action>\s*\n$"
        self.compiledActionPattern = re.compile(self.actionPattern)
        self.logtitlePattern = "^\s*<logtitle>(?P<l>.+)</logtitle>\s*\n$"
        self.compiledLogtitlePattern = re.compile(self.logtitlePattern)
        self.paramsPattern = '^\s*<params\s+xml:space="preserve">(?P<p>.+)</params>\s*\n$'
        self.compiledParamsPattern = re.compile(self.paramsPattern, re.DOTALL)
        self.noParamsPattern = '^\s*<params\s+xml:space="preserve" />\s*\n$'
        self.compiledNoParamsPattern = re.compile(self.noParamsPattern)
        self.endLogitemPattern = "^\s*</logitem>\s*\n$"
        self.compiledEndLogitemPattern = re.compile(self.endLogitemPattern)
        self.all=string.maketrans('','')
        self.nodigs=self.all.translate(self.all, string.digits)

    def skipHeader(self, fd):
        """skip over mediawiki site header etc"""
        endHeaderPattern = "^\s*</siteinfo>"
        compiledEndHeaderPattern = re.compile(endHeaderPattern)
        for line in fd:
            if compiledEndHeaderPattern.match(line):
                return True
        return False # never found it

    def unXMLEscape(self, title):
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

    def sqlEscape(self, string, underscores = True):
        """Escape string in preparation for it to be written
        to an sql file for import.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        Escapes these characters:  ' "  \   by adding leading \
        Note that in the database all titles are stored with underscores instead of spaces
        so replace those; also enclose the title in single quotes
        Arguments:
        string  -- string of to escape"""

        string = string.replace('\\', "\\\\")  # must insert new backslashs after this step
        string = string.replace("\'", "\\'")
        string = string.replace('"', '\\"')
        if underscores:
            string = string.replace(' ', '_')
        return "'" + string + "'"

    # format:
    #  <logitem>
    #    <id>1</id>
    #    <timestamp>2005-02-26T19:37:52Z</timestamp>
    #    <contributor>
    #      <username>Leonariso</username>
    #      <id>3</id>
    #    </contributor>
    #    <comment>content was: ''''Έντονης γραφής κείμενο'''''Πλάγιας γραφής κείμενο''[[Τίτλος σύνδεσης]]== Headline text ==[[...'</comment>
    #    <type>delete</type>
    #    <action>delete</action>
    #    <logtitle>Βικιλεξικό:By topic</logtitle>
    #    <params xml:space="preserve" />
    #  </logitem>
    def doLogItem(self, fd, logOutFd, userOutFd):
        # note that it's possible for a comment or the params to have an embedded newline in them
        # the rest of the fields, no

        line = fd.readline()
        result = self.compiledLogitemPattern.match(line)
        if not result:
            if "</mediawiki" in line:
                return True # eof
            else:
                raise WikiContentErr("bad line in logging file, expected <logitem>, found <%s>\n" % line)

        line = fd.readline()
        result = self.compiledIdPattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <id>, found <%s>\n" % line)
        logid = result.group("i")

        line = fd.readline()
        result = self.compiledTimestampPattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <timestamp>, found <%s>\n" % line)
        timestamp = result.group("t")

        line = fd.readline()
        result = self.compiledContributorPattern.match(line)
        if not result:
            if not "<contributor deleted" in line:
                raise WikiContentErr("bad line in logging file, expected <contributor>, found <%s>\n" % line)
            else:
                username = ''
                userid = '0'
        else:
            line = fd.readline()
            result = self.compiledUsernamePattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <username>, found <%s>\n" % line)
            username = result.group("u")

            line = fd.readline()
            result = self.compiledIdPattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <id>, found <%s>\n" % line)
            userid = result.group("i")

            line = fd.readline()
            result = self.compiledEndContributorPattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected </contributor>, found <%s>\n" % line)

        line = fd.readline()
        if "<comment>" not in line:
            # apparently comment is optional. OR it can be 'deleted'. wonderful.
            if "<comment deleted" in line:
                line = fd.readline()
            comment = ''
        else:
            while "</comment>" not in line:
                line = line + fd.readline()
            result = self.compiledCommentPattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <comment>, found <%s>\n" % line)
            comment = result.group("c")
            line = fd.readline()

        result = self.compiledTypePattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <type>, found <%s>\n" % line)
        type = result.group("t")

        line = fd.readline()
        result = self.compiledActionPattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <action>, found <%s>\n" % line)
        action = result.group("a")

        line = fd.readline()
        result = self.compiledLogtitlePattern.match(line)
        if not result:
            if "<text deleted" in line:
                logtitle = ''
            else:
                raise WikiContentErr("bad line in logging file, expected <logtitle>, found <%s>\n" % line)
        else:
            logtitle = result.group("l")

        line = fd.readline()
        # do the no params case first
        result = self.compiledNoParamsPattern.match(line)
        if result:
            params = ''
            line = fd.readline()
        else:
            if "<params" in line:
                # ok it has some params, possibly over more than one line
                while "</params>" not in line:
                    line = line + fd.readline()
                result = self.compiledParamsPattern.match(line)
                if not result:
                    raise WikiContentErr("bad line in logging file, expected <params  xml:space=\"preserve\" />, found <%s> for %s\n" % (line, logtitle))
                else:
                    params = result.group("p")
                    line = fd.readline()
            else: # it's some other tag, this elt was missing altogether
                params = ''
        
        result = self.compiledEndLogitemPattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected </logitem>, found <%s>\n" % line)

        # turn logtitle into pageid, namespace, title-with-no-namespace-prefix
        sep = logtitle.find(":")
        if sep != -1:
            prefix = logtitle[:sep]
            if prefix in self.nsDictByString:
                pagetitle = self.sqlEscape(self.unXMLEscape(logtitle[sep+1:]))
                nsnum = self.nsDictByString[prefix]
                if pagetitle in self.titlesDict:
                    pageid = self.titlesDict[pagetitle][nsnum]
                else:
                    pageid = "NULL"
            else:
                pagetitle = self.sqlEscape(self.unXMLEscape(logtitle))
                nsnum = 0
                if pagetitle in self.titlesDict:
                    pageid = self.titlesDict[pagetitle][0]
                else:
                    pageid = "NULL"
        else:
            pagetitle = self.sqlEscape(self.unXMLEscape(logtitle))
            nsnum = 0
            if pagetitle in self.titlesDict:
                pageid = self.titlesDict[pagetitle][0]
            else:
                pageid = "NULL"

        comment = self.sqlEscape(self.unXMLEscape(comment), False)
        username = self.sqlEscape(self.unXMLEscape(username), False)
        params = self.sqlEscape(self.unXMLEscape(params), False)

        line = "INSERT INTO logging ( log_id, log_type, log_action, log_timestamp, log_user, log_user_text, log_namespace, log_title, log_page, log_comment, log_params, log_deleted ) VALUES "
        logOutFd.write(unicode(line).encode('utf-8'))
        username = username.decode('utf-8')
        pagetitle = pagetitle.decode('utf-8')
        comment = comment.decode('utf-8')
        params = params.decode('utf-8')
        nsnum = str(nsnum)
        # need 20130425122902, have 2005-07-23T16:43:37Z
        timestamp = timestamp.translate(self.all, self.nodigs)
        
        line = "( %s );\n" % ", ".join([ logid, "'" + type+ "'", "'" + action + "'" , "'" + timestamp+ "'", userid, username, nsnum, pagetitle, pageid, comment, params, '0' ])
        logOutFd.write(unicode(line).encode('utf-8'))

        if self.userOutFile and userid not in self.userDict:
            line = "INSERT INTO user ( user_id, user_name, user_real_name, user_password, user_newpassword, user_newpass_time, user_email, user_touched, user_token, user_email_authenticated, user_email_token, user_email_token_expires, user_registration, user_editcount ) VALUES "
            userOutFd.write(unicode(line).encode('utf-8'))
            line = "( %s );\n" % ", ".join([ userid, username, "''", "''", "''", "NULL", "''", "'20010101000000'", "'6f9b27b447a7fd49bc525e51cc82320b'", "NULL", "NULL", "NULL", "NULL", "0" ])
            userOutFd.write(unicode(line).encode('utf-8'))
            
            self.userDict[userid] = True

        return False

    def writeSql(self):
        self.userDict = { 1: True }
        fd = File.openInput(self.xmlFile)
        logOutFd = File.openOutput(self.logOutFile)
        if self.userOutFile:
            userOutFd = File.openOutput(self.userOutFile)
        else:
            userOutFd = None
        if not self.skipHeader(fd):
            raise WikiContentErr("failed to find end of mediawiki/siteinfo header in xml file\n")
        eof = False
        while not eof:
            eof = self.doLogItem(fd, logOutFd, userOutFd)
        fd.close()
        logOutFd.close()
        if self.userOutFile:
            userOutFd.close()
        return
            
def usage(message = None):
    """Show usage and help information. Arguments:
    message   -- message to be shown (e.g. error message) before the help"""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    sys.stderr.write("Usage: python %s --lang langcode --project filename --sqlfile filename\n" % sys.argv[0])
    sys.stderr.write("                 --logfile filename --logout filename [--userout filename]\n")
    sys.stderr.write("\n")
    sys.stderr.write("This script converts a pages-logging.xml file to an sql file suitable for\n")
    sys.stderr.write("import into the logging table of a MediaWiki installation.\n")
    sys.stderr.write("It may get some things wrong because page ids of page titles can change\n")
    sys.stderr.write("over time and this program isn't clever about how it looks that up.\n")
    sys.stderr.write("We needed this script for testing logging dumps.  If you need it for production\n")
    sys.stderr.write("purposes, better test it carefully.\n")
    sys.stderr.write("\n")
    sys.stderr.write("Options:\n")
    sys.stderr.write("\n")
    sys.stderr.write("--lang         the language code of the project from which the logging table was dumped,\n")
    sys.stderr.write("               i.e. en, fr, el etc.\n")
    sys.stderr.write("--project      the type of wiki from which the logging table was dumped, i.e. wikipedia,\n")
    sys.stderr.write("               wiktionary, wikisource, etc.\n")
    sys.stderr.write("--sqlfile      path to an sql fle containing fields pageid namespacenum pagetitle space-\n")
    sys.stderr.write("               separated and one triple per line, pagetitle should be sql escaped as it\n")
    sys.stderr.write("               would be if written out by mysqldump, and it should not contain the namespace\n")
    sys.stderr.write("               prefix.\n")
    sys.stderr.write("--loggingfile  path to the xml pages-logging file to be converted\n")
    sys.stderr.write("--logout       path to the file where the converted sql will be written\n")
    sys.stderr.write("--userout      path to file where fake user table sql will be written, if specified\n")
    sys.stderr.write("               the user table is used when generating xml dumps of the log table;\n")
    sys.stderr.write("               any user id found in the logging sql file with non null username will\n")
    sys.stderr.write("               be added except for the user with uid 1, yes this is a hack\n")
    sys.stderr.write("               Make sure that there are no other users except uid 1 already in the table\n")
    sys.stderr.write("               and that the username is not in the produced sql BEFORE using it for import\n")
    sys.exit(1)

if __name__ == "__main__":
    langCode = None
    project = None
    sqlFile = None
    loggingFile = None
    logOutFile = None
    userOutFile = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "lang=", "project=", "sqlfile=", "loggingfile=", "logout=", "userout=" ] )
    except:
        usage("Unknown option specified")

    for (opt, val) in options:

        # main opts
        if opt == "--lang":
            langCode = val;
        elif opt == "--project":
            project = val
        elif opt == "--sqlfile":
            sqlFile = val
        elif opt == "--loggingfile":
            loggingFile = val
        elif opt == "--logout":
            logOutFile = val
        elif opt == "--userout":
            userOutFile = val
        else:
            usage("Unknown option specified: %s" % opt )

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    if not langCode:
        usage("Missing mandatory option <%s>" % "lang")
    if not sqlFile:
        usage("Missing mandatory option <%s>" % "sqlfile")
    if not project:
        usage("Missing mandatory option <%s>" % "project")
    if not loggingFile:
        usage("Missing mandatory option <%s>" % "loggingfile")
    if not logOutFile:
        usage("Missing mandatory option <%s>" % "logout")

    ns = NsDict(langCode, project)
    nsDict = ns.getNsDict()

    nsDictByString = {}
    for nsnum in nsDict.keys():
        nsDictByString[nsDict[nsnum]] = nsnum

    td = TitlesDict(nsDictByString)
    titlesDict = td.getTitlesDict(sqlFile)
    lx = LoggingXml(nsDictByString, titlesDict, loggingFile, logOutFile, userOutFile)
    lx.writeSql()

    
