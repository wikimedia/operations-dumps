'''
for every wiki, dump revision content as HTML from RESTBase,
stored an an sqlite3 compressed database.
'''
import os
from miscdumplib import MiscDumpDir
from miscdumplib import MiscDumpDirs
from miscdumplib import log
from miscdumplib import ContentFile
from miscdumplib import MiscDumpConfig
from miscdumplib import MiscDumpBase
from miscdumplib import get_config_defaults
from dumps.utils import RunSimpleCommand, MultiVersion


# pylint: disable=broad-except


class HTMLFile(ContentFile):
    '''
    file containing revision content in html format
    from RESTBase, for given wiki and date
    '''
    def get_filename(self, ns):
        return "%s-%s-html.ns%s.sqlite3.xz" % (self.wikiname, self.date, ns)


# required for misc dump factory
class HTMLDumpConfig(MiscDumpConfig):
    '''
    additional config settings for html (RESTBase) dumps
    '''
    def __init__(self, config_file=None):
        defaults = get_config_defaults()
        defaults['nodejs'] = "/usr/bin/nodejs"
        # yeah whatever.  we'll override this with something sensible anyhow
        defaults['scriptpath'] = "/srv/htmldumper/bin/dump_wiki"
        super(HTMLDumpConfig, self).__init__(defaults, config_file)
        self.nodejs = self.conf.get("tools", "nodejs")
        self.scriptpath = self.conf.get("tools", "scriptpath")


class HTMLDump(MiscDumpBase):
    '''
    given a wiki object with date, config all set up,
    generate HTML (RESTBase) dump for this one wiki
    '''
    def __init__(self, wiki, dryrun=False, args=None):
        '''
        wiki:     WikiDump object with date set
        dryrun:   whether or not to run commands or display what would have been done
        args:     dict of additional args 'ns' and the namespace number (in string
                  format) to dump
        '''
        super(HTMLDump, self).__init__(wiki, dryrun, args)
        self.wiki = wiki
        self.dirs = MiscDumpDirs(self.wiki.config, self.wiki.db_name)
        self.dryrun = dryrun
        self.args = args

    def get_steps(self):
        html_filename = HTMLFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        steps = {'html': {'file': html_filename, 'run': True}}
        return steps

    def run(self):
        '''
        dump html from RESTBase of revision content, for given wiki and date
        '''
        try:
            log.info("dumping html for wiki %s", self.wiki.db_name)
            if not self.dump_html():
                return False
        except Exception as ex:
            log.info("Error encountered runing dump for %s ", self.wiki.db_name,
                     exc_info=ex)
            return False
        return True

    def get_domain_from_wikidbname(self):
        '''
        given the name of the wiki db, turn this into the
        fqdn of the wiki project (i.e. enwiki -> en.wikipedia.org)
        '''
        script_command = MultiVersion.mw_script_as_array(self.wiki.config,
                                                         "eval.php")
        # echo $wgCanonicalServer | php "$multiversionscript" eval.php $wiki
        command = ["echo", "'echo $wgCanonicalServer;'", "|", self.wiki.config.php]
        command.extend(script_command)
        command.append(self.wiki.db_name)
        command_text = " ".join(command)
        log.info("running with no output: " + command_text)
        output = RunSimpleCommand.run_with_output(command_text, shell=True)
        if not output:
            log.info("error retrieving domain for wiki %s", self.wiki.db_name)
            return None
        # rstrip gets rid of any trailing newlines from eval.php
        return output.split('//')[1].rstrip()

    def dump_html(self):
        '''
        dump HTML-formated revision content from RESTBase
        for the given wiki and date
        '''
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        htmlfile = HTMLFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        outputfile = htmlfile.get_filename(self.args['ns'])
        # /usr/bin/nodejs ./bin/dump_wiki --domain en.wikipedia.org --ns 0 \
        # --apiURL http://en.wikipedia.org/w/api.php \
        # --dataBase /srv/www/htmldumps/dumps/20160826/en.wikipedia.org.articles.ns0.sqlite3
        domain = self.get_domain_from_wikidbname()
        # FIXME: the nodejs wrapper which will do the compress etc stuff for one wiki is
        # not yet written
        command = [self.wiki.config.nodejs]
        command.append(self.wiki.config.scriptpath)
        command.extend(["--domain", domain, "--ns", self.args['ns'],
                        "--apiURL", "http://%s/w/api.php" % domain,
                        "--dataBase", os.path.join(outputdir, outputfile),
                        "--wiki=%s" % self.wiki.db_name,
                        "--output=gzip:%s" % os.path.join(outputdir, outputfile)])

        if self.dryrun:
            print "would run command for html dump:", command
        else:
            success = RunSimpleCommand.run_with_no_output(
                command, shell=False,
                timeout=self.get_lock_timeout_interval(),
                timeout_callback=self.periodic_callback)
            if not success:
                log.info("error producing html files for wiki %s", self.wiki.db_name)
                return False
        return True

    def get_output_files(self):
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        htmlfile = HTMLFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        filenames = [htmlfile.get_filename(self.args['ns'])]
        return [os.path.join(outputdir, filename) for filename in filenames]


# required for misc dump factory
def get_htmldump_usage():
    '''
    return usage message for args specific to the html (RESTBase) dumps
    (used for general usage message for misc dumps)
    '''
    return """Specific args for html (RESTBase) dumps:

ns    -- the number of the namespace to dump
"""
