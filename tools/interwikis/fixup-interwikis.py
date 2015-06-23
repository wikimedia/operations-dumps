import os, re, sys, time, getopt, cdb, urllib
from os.path import exists

class IWCdbUpdater(object):
    def __init__(self, wikiDbName, wikiTablePrefix, cdbFile, siteType, wikiLangCode, dryrun, verbose):
        self.wikiDbName = wikiDbName
        self.wikiTablePrefix = wikiTablePrefix
        self.cdbFile = cdbFile
        self.newCdbFile = cdbFile + ".new"
        self.siteType = siteType
        self.wikiLangCode = wikiLangCode
        self.dryrun = dryrun
        self.verbose = verbose

        self.wikiName = self.getWikiName()

        # if we can't find it, try to download it
        if not exists(self.cdbFile):
            if self.dryrun:
                sys.stderr.write("No such file %s, would download Wikimedia interwiki cdb file\n" % self.cdbFile)
            elif self.verbose:
                sys.stderr.write("No such file %s, downloading Wikimedia interwiki cdb file\n" % self.cdbFile)
            url = "https://noc.wikimedia.org/conf/interwiki.cdb"
            urllib.urlretrieve(url, self.cdbFile)

        self.oldcdbfd = cdb.init(self.cdbFile)
        self.newcdbfd = None

    def getWikiName(self):
        if self.wikiTablePrefix:
            return("%s-%s" % (self.wikiDbName, self.wikiTablePrefix))
        else:
            return self.wikiDbName

    @staticmethod
    def getKnownSiteTypesDict():
        return { "wikibooks": "b", "wikimedia": "chapter", "wikidata": "d", "wikinews": 'n', "wikiquote": 'q', "wikisource": "s", "wikiversity": 'v', "wikivoyage": "voy", "wiki": 'w', "wiktionary": "wikt" }

    @staticmethod
    def getKnownSiteTypes():
        return IWCdbUpdater.getKnownSiteTypesDict().keys()

    @staticmethod
    def getAbbrevs():
        return IWCdbUpdater.getKnownSiteTypesDict().values()

    @staticmethod
    def getAbbrevFromSiteType(siteType):
        return IWCdbUpdater.getKnownSiteTypesDict()[siteType]

    @staticmethod
    def getSiteUrl(langCode, siteType):
        if siteType == 'wiki':
            # special case
            siteType = 'wikipedia'
        return "%s.%s.org" % (langCode, siteType)

    def checkUpdateNeeded(self):
        # check keys in existing cdb file to see if we actually need to do the update
        # returns True and sets self.updateThese to a list of key/value pairs
        # to be added/replaced if update is needed
        # returns False if no update is needed
        self.updateThese = {}

        # key   enwiki-mw_:w
        # value  1 http://en.wikipedia.org/wiki/$1
        st = IWCdbUpdater.getKnownSiteTypes()
        for t in st:
            key = "%s:%s" % (self.wikiName, IWCdbUpdater.getAbbrevFromSiteType(t))
            oldValue = self.oldcdbfd.get(key)
            newValue = "1 //%s/wiki/$1" % IWCdbUpdater.getSiteUrl(self.wikiLangCode, t)
            if oldValue != newValue:
                self.updateThese[key] = newValue

        # key    __sites:enwiki-mw
        # value  wiki
        oldValue = self.oldcdbfd.get("__sites:%s" % self.wikiName)
        if oldValue != self.siteType:
            self.updateThese["__sites:%s" % self.wikiName] = self.siteType

        # key    __list:enwiki-mw
        # value  b chapter d n q s v voy w wikt
        try:
            oldValueList = self.oldcdbfd.get("__list:%s" % self.wikiName).split()
        except:
            oldValueList = []

        oldValueList.sort()
        oldValueString = " ".join(oldValueList)

        knownAbbrevs = self.getAbbrevs()
        knownAbbrevs.sort()
        knownAbbrevsString = " ".join(knownAbbrevs)
        if oldValueString != knownAbbrevsString:
            self.updateThese["__list:%s" % self.wikiName] = knownAbbrevsString

        # key    __list:__sites
        # value  aawiki aawikibooks ... enwiki-mw ...
        try:
            oldValueList = self.oldcdbfd.get("__list:__sites").split()
        except:
            oldValueList = []
        if not self.wikiName in oldValueList:
            oldValueList.append(self.wikiName)
            self.updateThese["__list:__sites"] = " ".join(oldValueList)

        if len(self.updateThese.keys()):
            return True
        else:
            return False

    def addOldKeys(self):
        # read all entries from old db and add them to new db, skipping those
        # for which values must be updated
        for k in self.oldcdbfd.keys():
            if not k in self.updateThese.keys():
                if self.dryrun:
                    sys.stderr.write("Would copy existing key %s to new cdb db\n" % k)
                elif verbose:
                    sys.stderr.write("Copying existing key %s to new cdb db\n" % k)
                if not dryrun:
                    self.newcdbfd.add(k,self.oldcdbfd.get(k))

    def addNewKeys(self):
        # add all the new/changed entries to the db
        for k in self.updateThese.keys():
            if self.dryrun:
                sys.stderr.write("Would add key %s to new cdb db\n" % k)
            elif verbose:
                sys.stderr.write("Adding key %s to new cdb db\n" % k)
            if self.newcdbfd:
                self.newcdbfd.add(k,self.updateThese[k])

    def doUpdate(self):
        if not self.checkUpdateNeeded():
            sys.stderr.write("No updates to cdb file needed, exiting.\n")
            return

        if self.dryrun:
            sys.stderr.write("Dry run, no new cdb file will be created\n")
        else:
            if self.verbose:
                sys.stderr.write("Creating new empty cdb file\n")
            self.newcdbfd = cdb.cdbmake(self.newCdbFile, self.newCdbFile + ".tmp")
        self.addOldKeys()
        self.addNewKeys()

    def done(self):
        # fixme is this going to rename some file from blah.tmp??
        if self.newcdbfd:
            if verbose:
                sys.stderr.write("closing new cdb file.\n")
            self.newcdbfd.finish()

def getLocalSettingInfo(localSettingsFile, wikiDbName, wikiTablePrefix, siteType, wikiLangCode, verbose):
    if not localSettingsFile:
        return(wikiDbName, wikiTablePrefix, siteType, wikiLangCode)

    if verbose:
        sys.stderr.write("before config file check, wikidbname %s, tableprefix %s, sitetype %s, langcode %s\n" % (wikiDbName, wikiTablePrefix, siteType, wikiLangCode))
    fd = open(localSettingsFile, "r")
    for line in fd:
        # expect: var = 'blah' ;  # some stuff
        found = re.match("^\s*(?P<name>[^\s=]+)\s*=\s*(?P<val>[^\s;#]+)\s*;", line)
        if not found:
            if verbose:
                sys.stderr.write("in config file skipping line %s" % line)
            continue
        varName = found.group('name')
        value = found.group('val')
        if (value[0] == '"' and value[-1] == '"') or value[0] == "'" and value[-1] == "'":
            value = value[1:-1]
        if varName == "$wgDBname":
            if not wikiDbName:
                wikiDbName = value
        elif varName == "$wgDBprefix":
            if not wikiTablePrefix:
                wikiTablePrefix = value
        elif varName == "$wgInterwikiFallbackSite":
            if not siteType:
                siteType = value
        elif varName == "$wgLanguageCode":
            if not wikiLangCode:
                wikiLangCode = value
    fd.close()
    if verbose:
        sys.stderr.write("after config file check, wikidbname %s, tableprefix %s, sitetype %s, langcode %s\n" % (wikiDbName, wikiTablePrefix, siteType, wikiLangCode))
    return(wikiDbName, wikiTablePrefix, siteType, wikiLangCode)

def usage(message = None):
    if message:
        sys.stderr.write("%s\n" % message)
    sys.stderr.write("Usage: python %s --wikidbname name --localsettings filename\n" % sys.argv[0])
    sys.stderr.write("Usage:        [--cdbfile filename] [--sitetype type] [--langcode langcode] \n")
    sys.stderr.write("              [--tableprefix prefix] [--dryrun] [--verbose]\n")
    sys.stderr.write("\n")
    sys.stderr.write("This script adds entries to an interwiki cdb file so that MediaWiki will treat\n")
    sys.stderr.write("the specified wiki as a wiki of the specified type and language for purposes of\n")
    sys.stderr.write("interwiki links. The new cdb file has the extension '.new' added to the end of the filename.\n")
    sys.stderr.write("\n")
    sys.stderr.write("--wikidbname:    the name of the wiki database, as specified in LocalSettings.php via\n")
    sys.stderr.write("                 the $wgDBname variable\n")
    sys.stderr.write("                 default: none, either this or localsettings must be specified\n")
    sys.stderr.write("--localsettings: the name of the LocalSettings.php or other wiki config file which contains\n")
    sys.stderr.write("                 configuration settings such as $wgDBname.  Values specified on the command\n")
    sys.stderr.write("                 line will override values read from this file, if there is a conflict.\n")
    sys.stderr.write("                 default: none, either this or wikidbname must be specified.\n")
    sys.stderr.write("--tableprefix    the db table prefix in the wiki's LocalSettings.php file, via the $wgDBprefix\n")
    sys.stderr.write("                 variable, if any.\n")
    sys.stderr.write("                 default: none\n")
    sys.stderr.write("--cdbfile:       the path to the cdb file you want to modify. If the file does not exist, an attempt\n")
    sys.stderr.write("                 will be made to download http://noc.wikimedia.org/interwiki/interwiki.cdb and save\n")
    sys.stderr.write("                 to the specified or default filename.\n")
    sys.stderr.write("                 default: interwiki.cdb in the current working directory\n")
    sys.stderr.write("--sitetype:      MediaWiki should treat your wiki as this projct type for purposes of\n")
    sys.stderr.write("                 interwiki links.  Links to other languages of the same site type will\n")
    sys.stderr.write("                 be treated differently than links to other projects.  If this isn't clear,\n")
    sys.stderr.write("                 see http://www.mediawiki.org/wiki/Help:Interwiki_linking#Interwiki_links\n")
    sys.stderr.write("                 known types: wiki (i.e. wikipedia), wiktionary, wikisource, wikiquote, wikinews,\n")
    sys.stderr.write("                 wikivoyage, wikimedia, wikiversity\n")
    sys.stderr.write("                 default: wiki (i.e. wikipedia)\n")
    sys.stderr.write("--langcode:      code (typically two or three letters) of your wiki's language for MediaWiki\n")
    sys.stderr.write("                 interlinks to other projects in the same language\n")
    sys.stderr.write("                 A full list of language codes is here:\n")
    sys.stderr.write("                 https://noc.wikimedia.org/conf/highlight.php?file=langlist\n")
    sys.stderr.write("                 If the use of this option isn't clear, see\n")
    sys.stderr.write("                 http://www.mediawiki.org/wiki/Help:Interwiki_linking#Interwiki_links\n")
    sys.stderr.write("                 default: en  (i.e. English)\n")
    sys.stderr.write("--dryrun:        don't write changes to the cdb file but report what would be done\n")
    sys.stderr.write("                 default: write changes to the cdb file\n")
    sys.stderr.write("--verbose:       write progress messages to stderr.\n")
    sys.stderr.write("                 default: process quietly\n")
    sys.stderr.write("\n")
    sys.stderr.write("Example usage:\n")
    sys.stderr.write("\n")
    sys.stderr.write("python %s --wikidbname enwiki --tableprefix mw_\n" % sys.argv[0])
    sys.stderr.write("\n")
    sys.stderr.write("This will download the interwiki.cdb file in use on Wikimedia sites and will add\n")
    sys.stderr.write("the appropriate entries for 'enwiki-mw_' to the new file which will be named\n")
    sys.stderr.write("'interwiki.cdb.new' and saved in the current directory.\n")
    sys.stderr.write("\n")
    sys.stderr.write("python %s --localsettings /var/www/html/mywiki/LocalSettings.php\n" % sys.argv[0])
    sys.stderr.write("\n")
    sys.stderr.write("This will download the interwiki.cdb file in use on Wikimedia sites and will add\n")
    sys.stderr.write("the appropriate entries, reading config vars from LocalSettings.php, to the new cdb\n")
    sys.stderr.write("file which will be named 'interwiki.cdb.new' and saved in the current directory.\n")
    sys.stderr.write("\n")
    sys.exit(1)


if __name__ == "__main__":
    wikiDbName = None
    wikiTablePrefix = None
    cdbFile = "interwiki.cdb"
    siteType = None
    wikiLangCode = None
    localSettingsFile = None
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [ "wikidbname=", "cdbfile=", "sitetype=", "langcode=", "tableprefix=", "localsettings=", "help", "dryrun", "verbose" ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--wikidbname":
            wikiDbName = val
        elif opt == "--cdbfile":
            cdbFile = val
        elif opt == "--sitetype":
            siteType = val
        elif opt == "--langcode":
            wikiLangCode = val
        elif opt == "--tableprefix":
            wikiTablePrefix = val
        elif opt == "--localsettings":
            localSettingsFile = val
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--help":
            usage()

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not wikiDbName and not localSettingsFile:
        usage("Missing value for --wikidbname and no localsettings specified, one of these arguments must be provided\n")

    (wikiDbName, wikiTablePrefix, siteType, wikiLangCode) = getLocalSettingInfo(localSettingsFile, wikiDbName, wikiTablePrefix, siteType, wikiLangCode, verbose)

    if siteType == None:
        siteType = "wiki"
    if wikiLangCode == None:
        wikiLangCode = "en"

    if siteType not in IWCdbUpdater.getKnownSiteTypes():
        usage("Unknown type specified for --sitetype\n")

    iu = IWCdbUpdater(wikiDbName, wikiTablePrefix, cdbFile, siteType, wikiLangCode, dryrun, verbose)
    iu.doUpdate()
    iu.done()
