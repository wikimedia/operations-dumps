from incr_dumps import IncrDump, IncrDumpConfig, get_incrdump_usage
# from sample_dumps import SampleDump, SampleDumpConfig, get_sampledump_usage


NAMES_CLASSES = {
    # "htmldumps": [HTMLDump, HTMLDumpConfig, get_htmldump_usage],
    # "sampledumps": [SampleDump, SampleDumpConfig, get_sampledump_usage],
    "incrdumps": [IncrDump, IncrDumpConfig, get_incrdump_usage],
}


class MiscDumpFactory(object):
    @staticmethod
    def get_dumper(dumptype):
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][0]

    @staticmethod
    def get_usage(dumptype):
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][2]

    @staticmethod
    def get_secondary_usage_all():
        text = ""
        for dumptype in NAMES_CLASSES.keys():
            text = text + MiscDumpFactory.get_usage(dumptype)() + "\n"
        return text

    @staticmethod
    def get_known_dumptypes():
        return NAMES_CLASSES.keys()

    @staticmethod
    def get_configurator(dumptype):
        if dumptype not in NAMES_CLASSES.keys():
            return None
        return NAMES_CLASSES[dumptype][1]
