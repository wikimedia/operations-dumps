import re
import sys
import getopt
import urllib
from os.path import exists
import cdb


class IWCdbUpdater(object):
    def __init__(self, wiki_db_name, wiki_table_prefix, cdb_file,
                 site_type, wiki_lang_code, dryrun, verbose):
        self.wiki_db_name = wiki_db_name
        self.wiki_table_prefix = wiki_table_prefix
        self.cdb_file = cdb_file
        self.new_cdb_file = cdb_file + ".new"
        self.site_type = site_type
        self.wiki_lang_code = wiki_lang_code
        self.dryrun = dryrun
        self.verbose = verbose

        self.wiki_name = self.get_wiki_name()

        # if we can't find it, try to download it
        if not exists(self.cdb_file):
            if self.dryrun:
                sys.stderr.write("No such file %s, would download "
                                 "Wikimedia interwiki cdb file\n" % self.cdb_file)
            elif self.verbose:
                sys.stderr.write("No such file %s, downloading "
                                 "Wikimedia interwiki cdb file\n" % self.cdb_file)
            url = "https://noc.wikimedia.org/conf/interwiki.cdb"
            urllib.urlretrieve(url, self.cdb_file)

        self.oldcdbfd = cdb.init(self.cdb_file)
        self.newcdbfd = None
        self.update_these = {}

    def get_wiki_name(self):
        '''
        return the wiki name including table prefix if needed
        '''
        if self.wiki_table_prefix:
            return "%s-%s" % (self.wiki_db_name, self.wiki_table_prefix)
        else:
            return self.wiki_db_name

    @staticmethod
    def get_known_site_types_dict():
        '''
        return wiki types along with their abbreviations
        '''
        return {"wikibooks": "b", "wikimedia": "chapter",
                "wikidata": "d", "wikinews": 'n', "wikiquote": 'q',
                "wikisource": "s", "wikiversity": 'v',
                "wikivoyage": "voy", "wiki": 'w', "wiktionary": "wikt"}

    @staticmethod
    def get_known_site_types():
        '''
        return the list of known wiki types
        '''
        return IWCdbUpdater.get_known_site_types_dict().keys()

    @staticmethod
    def get_abbrevs():
        '''
        return the list of abbreviations of known wiki types
        '''
        return IWCdbUpdater.get_known_site_types_dict().values()

    @staticmethod
    def get_abbrev_from_site_type(site_type):
        '''
        given a wiki type, return its abbreviation
        '''
        return IWCdbUpdater.get_known_site_types_dict()[site_type]

    @staticmethod
    def get_site_url(lang_code, site_type):
        '''
        given the language code and wiki type, return the hostname
        that would be used in a url
        '''
        if site_type == 'wiki':
            # special case
            site_type = 'wikipedia'
        return "%s.%s.org" % (lang_code, site_type)

    def check_update_needed(self):
        '''
        check keys in existing cdb file to see if we actually need to do the update
        returns True and sets self.updateThese to a list of key/value pairs
        to be added/replaced if update is needed
        returns False if no update is needed
        '''
        # key   enwiki-mw_:w
        # value  1 http://en.wikipedia.org/wiki/$1
        site_types = IWCdbUpdater.get_known_site_types()
        for stype in site_types:
            key = "%s:%s" % (self.wiki_name, IWCdbUpdater.get_abbrev_from_site_type(stype))
            old_value = self.oldcdbfd.get(key)
            new_value = "1 //%s/wiki/$1" % IWCdbUpdater.get_site_url(self.wiki_lang_code, stype)
            if old_value != new_value:
                self.update_these[key] = new_value

        # key    __sites:enwiki-mw
        # value  wiki
        old_value = self.oldcdbfd.get("__sites:%s" % self.wiki_name)
        if old_value != self.site_type:
            self.update_these["__sites:%s" % self.wiki_name] = self.site_type

        # key    __list:enwiki-mw
        # value  b chapter d n q s v voy w wikt
        try:
            old_value_list = self.oldcdbfd.get("__list:%s" % self.wiki_name).split()
        except Exception:
            old_value_list = []

        old_value_list.sort()
        old_value_string = " ".join(old_value_list)

        known_abbrevs = self.get_abbrevs()
        known_abbrevs.sort()
        known_abbrevs_string = " ".join(known_abbrevs)
        if old_value_string != known_abbrevs_string:
            self.update_these["__list:%s" % self.wiki_name] = known_abbrevs_string

        # key    __list:__sites
        # value  aawiki aawikibooks ... enwiki-mw ...
        try:
            old_value_list = self.oldcdbfd.get("__list:__sites").split()
        except Exception:
            old_value_list = []
        if self.wiki_name not in old_value_list:
            old_value_list.append(self.wiki_name)
            self.update_these["__list:__sites"] = " ".join(old_value_list)

        return bool(len(self.update_these.keys()))

    def add_old_keys(self):
        '''
        read all entries from old db and add them to new db, skipping those
        for which values must be updated
        '''
        for key in self.oldcdbfd.keys():
            if key not in self.update_these.keys():
                if self.dryrun:
                    sys.stderr.write("Would copy existing key %s to new cdb db\n" % key)
                elif self.verbose:
                    sys.stderr.write("Copying existing key %s to new cdb db\n" % key)
                if not self.dryrun:
                    self.newcdbfd.add(key, self.oldcdbfd.get(key))

    def add_new_keys(self):
        '''
        add all the new/changed entries to the db
        '''
        for key in self.update_these:
            if self.dryrun:
                sys.stderr.write("Would add key %s to new cdb db\n" % key)
            elif self.verbose:
                sys.stderr.write("Adding key %s to new cdb db\n" % key)
            if self.newcdbfd:
                self.newcdbfd.add(key, self.update_these[key])

    def do_update(self):
        if not self.check_update_needed():
            sys.stderr.write("No updates to cdb file needed, exiting.\n")
            return

        if self.dryrun:
            sys.stderr.write("Dry run, no new cdb file will be created\n")
        else:
            if self.verbose:
                sys.stderr.write("Creating new empty cdb file\n")
            self.newcdbfd = cdb.cdbmake(self.new_cdb_file, self.new_cdb_file + ".tmp")
        self.add_old_keys()
        self.add_new_keys()

    def done(self):
        # fixme is this going to rename some file from blah.tmp??
        if self.newcdbfd:
            if self.verbose:
                sys.stderr.write("closing new cdb file.\n")
            self.newcdbfd.finish()


def get_local_setting_info(local_settings_file, wiki_db_name, wiki_table_prefix,
                           site_type, wiki_lang_code, verbose):
    if not local_settings_file:
        return(wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code)

    if verbose:
        sys.stderr.write("before config file check, wikidbname %s, "
                         "tableprefix %s, sitetype %s, langcode %s\n"
                         % (wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code))
    fdesc = open(local_settings_file, "r")
    for line in fdesc:
        # expect: var = 'blah' ;  # some stuff
        found = re.match(r"^\s*(?P<name>[^\s=]+)\s*=\s*(?P<val>[^\s;#]+)\s*;", line)
        if not found:
            if verbose:
                sys.stderr.write("in config file skipping line %s" % line)
            continue
        var_name = found.group('name')
        value = found.group('val')
        if (value[0] == '"' and value[-1] == '"') or value[0] == "'" and value[-1] == "'":
            value = value[1:-1]
        if var_name == "$wgDBname":
            if not wiki_db_name:
                wiki_db_name = value
        elif var_name == "$wgDBprefix":
            if not wiki_table_prefix:
                wiki_table_prefix = value
        elif var_name == "$wgInterwikiFallbackSite":
            if not site_type:
                site_type = value
        elif var_name == "$wgLanguageCode":
            if not wiki_lang_code:
                wiki_lang_code = value
    fdesc.close()
    if verbose:
        sys.stderr.write("after config file check, wikidbname %s, "
                         "tableprefix %s, sitetype %s, langcode %s\n"
                         % (wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code))
    return(wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code)


def usage(message=None):
    '''
    display a help message describing how to use this script,
    with an optional preceding message
    '''
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = '''
Usage: python fixup-interwikis.py --wikidbname name --localsettings filename
              [--cdbfile filename] [--sitetype type] [--langcode langcode]
              [--tableprefix prefix] [--dryrun] [--verbose]

This script adds entries to an interwiki cdb file so that MediaWiki will treat
the specified wiki as a wiki of the specified type and language for purposes of
interwiki links. The new cdb file has the extension '.new' added to the end of
the filename.

--wikidbname:    the name of the wiki database, as specified in LocalSettings.php
                 via the $wgDBname variable
                 default: none, either this or localsettings must be specified
--localsettings: the name of the LocalSettings.php or other wiki config file
                 which contains configuration settings such as $wgDBname.  Values
                 specified on the command line will override values read from
                 this file, if there is a conflict.
                 default: none, either this or wikidbname must be specified.
--tableprefix    the db table prefix in the wiki's LocalSettings.php file, via the
                 $wgDBprefix variable, if any.
                 default: none
--cdbfile:       the path to the cdb file you want to modify. If the file does not
                 exist, an attempt will be made to download
                 http://noc.wikimedia.org/interwiki/interwiki.cdb and save to the
                 specified or default filename.
                 default: interwiki.cdb in the current working directory
--sitetype:      MediaWiki should treat your wiki as this projct type for purposes
                 of interwiki links.  Links to other languages of the same site type
                 will be treated differently than links to other projects.  If this
                 isn't clear, see
                 http://www.mediawiki.org/wiki/Help:Interwiki_linking#Interwiki_links
                 known types: wiki (i.e. wikipedia), wiktionary, wikisource,
                 wikiquote, wikinews, wikivoyage, wikimedia, wikiversity
                 default: wiki (i.e. wikipedia)
--langcode:      code (typically two or three letters) of your wiki's language for
                 MediaWiki interlinks to other projects in the same language
                 A full list of language codes is here:
                 https://noc.wikimedia.org/conf/highlight.php?file=langlist
                 If the use of this option isn't clear, see
                 http://www.mediawiki.org/wiki/Help:Interwiki_linking#Interwiki_links
                 default: en  (i.e. English)
--dryrun:        don't write changes to the cdb file but report what would be done
                 default: write changes to the cdb file
--verbose:       write progress messages to stderr.
                 default: process quietly

Example usage:

python fixup-interwikis.py --wikidbname enwiki --tableprefix mw_

This will download the interwiki.cdb file in use on Wikimedia sites and will add
the appropriate entries for 'enwiki-mw_' to the new file which will be named
'interwiki.cdb.new' and saved in the current directory.

python fixup-interwikis.py --localsettings /var/www/html/mywiki/LocalSettings.php

This will download the interwiki.cdb file in use on Wikimedia sites and will add
the appropriate entries, reading config vars from LocalSettings.php, to the new cdb
file which will be named 'interwiki.cdb.new' and saved in the current directory.
'''
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    '''
    main entry point, does all the work
    '''
    wiki_db_name = None
    wiki_table_prefix = None
    cdb_file = "interwiki.cdb"
    site_type = None
    wiki_lang_code = None
    local_settings_file = None
    dryrun = False
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ["wikidbname=", "cdbfile=", "sitetype=", "langcode=",
             "tableprefix=", "localsettings=", "help", "dryrun", "verbose"])
    except Exception:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--wikidbname":
            wiki_db_name = val
        elif opt == "--cdbfile":
            cdb_file = val
        elif opt == "--sitetype":
            site_type = val
        elif opt == "--langcode":
            wiki_lang_code = val
        elif opt == "--tableprefix":
            wiki_table_prefix = val
        elif opt == "--localsettings":
            local_settings_file = val
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--help":
            usage()

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not wiki_db_name and not local_settings_file:
        usage("Missing value for --wikidbname and no localsettings specified, "
              "one of these arguments must be provided\n")

    (wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code) = get_local_setting_info(
        local_settings_file, wiki_db_name, wiki_table_prefix, site_type, wiki_lang_code, verbose)

    if site_type is None:
        site_type = "wiki"
    if wiki_lang_code is None:
        wiki_lang_code = "en"

    if site_type not in IWCdbUpdater.get_known_site_types():
        usage("Unknown type specified for --sitetype\n")

    updater = IWCdbUpdater(wiki_db_name, wiki_table_prefix, cdb_file,
                           site_type, wiki_lang_code, dryrun, verbose)
    updater.do_update()
    updater.done()

if __name__ == "__main__":
    do_main()
