from incr_dumps import IncrDump, IncrDumpConfig, get_incrdump_usage
from sample_dumps import SampleDump, SampleDumpConfig, get_sampledump_usage
from html_dumps import HTMLDump, HTMLDumpConfig, get_htmldump_usage


NAMES_CLASSES = {
    "htmldumps": [HTMLDump, HTMLDumpConfig, get_htmldump_usage],
    "sampledumps": [SampleDump, SampleDumpConfig, get_sampledump_usage],
    "incrdumps": [IncrDump, IncrDumpConfig, get_incrdump_usage],
}


class MiscDumpFactory(object):
    '''
    using the NAMES_CLASSES dict, retrieve the appropriate class
    or method for the specified dumptype
    '''
    @staticmethod
    def get_dumper(dumptype):
        '''
        return the class associated with the specific dumptype
        '''
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][0]

    @staticmethod
    def get_usage(dumptype):
        '''
        return the usage method associated with the specific dumptype
        '''
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][2]

    @staticmethod
    def get_secondary_usage_all():
        '''
        get usage messages about args specific to each dumptype,
        concat them together with newlines in between and return them
        '''
        text = ""
        for dumptype in NAMES_CLASSES.keys():
            text = text + MiscDumpFactory.get_usage(dumptype)() + "\n"
        return text

    @staticmethod
    def get_known_dumptypes():
        '''
        return list of known dumptypes
        '''
        return NAMES_CLASSES.keys()

    @staticmethod
    def get_configurator(dumptype):
        '''
        return the configuration method associated with the specific dumptype
        '''
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][1]
