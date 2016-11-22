'''
sample dump
'''
from dumps.WikiDump import FileUtils
from miscdumplib import ContentFile
from miscdumplib import MiscDumpConfig
from miscdumplib import MiscDumpBase
from miscdumplib import get_config_defaults
from miscdumplib import log


# pylint: disable=broad-except


# TODO: use the config testme var, the
# moretest var for something (with nice new names
# of course, duh)


# required for misc dump factory
class SampleDumpConfig(MiscDumpConfig):
    '''
    additional config settings for sample dumps
    '''
    def __init__(self, config_file=None):
        defaults = get_config_defaults()
        defaults['testme'] = "11111"  # same as on my luggage!
        super(SampleDumpConfig, self).__init__(defaults, config_file)
        testme = self.conf.get("output", "testme")
        self.testme = int(testme, 0)


# output file for a dump step
class NspacesFile(ContentFile):
    '''
    file containing namespace data
    '''
    def get_filename(self):
        return "%s-%s-namespaces.txt" % (self.wikiname, self.date)


# output file for a dump step
class AliasesFile(ContentFile):
    '''
    file containing namespace alias data
    '''
    def get_filename(self):
        return "%s-%s-namespace-aliases.txt" % (self.wikiname, self.date)


# required for misc dump factory
class SampleDump(MiscDumpBase):
    '''
    dump namespaces, aliases for one wiki
    '''
    # overrides base class
    def __init__(self, wiki, dryrun=False, args=None):
        '''
        wiki:     WikiDump object with date set
        dryrun:   whether or not to run commands or display what would have been done
        args:     dict of additional args 'revsonly' and/or 'stubsonly'
                  indicating whether or not to dump rev content and/or stubs
        '''
        super(SampleDump, self).__init__(wiki, dryrun, args)
        # self.moretest = FIXME(self.wiki.date, self.wiki.config)
        if 'nsonly' in args:
            self.steps['aliases']['run'] = False
        if 'aliasesonly' in args:
            self.steps['nspaces']['run'] = False

    # overrides base class
    def get_steps(self):
        nspacesfile = NspacesFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        nspaces_filename = nspacesfile.get_filename()

        aliasesfile = AliasesFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        aliases_filename = aliasesfile.get_filename()

        steps = {'nspaces': {'file': nspaces_filename, 'run': True},
                 'aliases': {'file': aliases_filename, 'run': True}}
        return steps

    # overrides base class
    def run(self):
        '''
        dump namespaces, namespace aliases for given wiki and date
        '''
        try:
            log.info("dumping namespaces for wiki %s", self.wiki.db_name)
            if not self.dump_namespaces():
                return False
            log.info("dumping aliases for wiki %s", self.wiki.db_name)
            if not self.dump_aliases():
                return False
        except Exception as ex:
            log.info("Error encountered runing dump for %s ", self.wiki.db_name,
                     exc_info=ex)
            return False
        return True

    # dump step
    def dump_namespaces(self):
        '''
        returns True on success
        False or exception on error are fine
        '''
        if not self.steps['nspaces']['run']:
            return True
        try:
            contents = "for wiki %s, here be namespaces! hahaha\n" % self.wiki.db_name
            nspacesfile = NspacesFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
            FileUtils.write_file_in_place(nspacesfile.get_path(),
                                          contents, self.wiki.config.fileperms)
            return True
        except Exception as ex:
            log.info("Error encountered dumping namespaces for %s ", self.wiki.db_name,
                     exc_info=ex)
            raise

    # dump step
    def dump_aliases(self):
        '''
        returns True on success
        False or exception on error are fine
        '''
        if not self.steps['aliases']['run']:
            return True
        try:
            contents = "for wiki %s: alias meow=more\n" % self.wiki.db_name
            aliasesfile = AliasesFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
            FileUtils.write_file_in_place(aliasesfile.get_path(),
                                          contents, self.wiki.config.fileperms)
            return True
        except Exception as ex:
            log.info("Error encountered dumping namespaces for %s ", self.wiki.db_name,
                     exc_info=ex)
            raise


# required for misc dump factory
def get_sampledump_usage():
    '''
    return usage message for args specific to the simple dumps
    (used for general usage message for misc dumps)
    '''
    return """Specific args for simple dumps:

nspacesonly        -- dump namespaces but not aliases
aliasesonly        -- dump aliases but not namespaces
"""
