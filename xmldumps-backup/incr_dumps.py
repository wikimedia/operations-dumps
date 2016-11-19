# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

from os.path import exists
from miscdumplib import ContentFile
from dumps.WikiDump import FileUtils
from dumps.utils import RunSimpleCommand
from dumps.utils import DbServerInfo


class MaxRevID(object):
    '''
    retrieve, read, write max revid from database/file
    '''
    def __init__(self, wiki, cutoff, dryrun):
        self.wiki = wiki
        self.cutoff = cutoff
        self.dryrun = dryrun
        self.max_id = None

    def get_max_revid(self):
        query = ("'select rev_id from revision where rev_timestamp < \"%s\" "
                 "order by rev_timestamp desc limit 1'" % self.cutoff)
        db_info = DbServerInfo(self.wiki, self.wiki.db_name)
        command = db_info.build_sql_command(query)
        # we get back: [[echo, some, args, and, stuff] [mysql, some, more, args]]
        # because it's formatted for the fancy command runner. we don't need that.
        # Turn into a flat list with pipe in between. Also we need the --silent
        # argument so we just get the value back and nothing else
        to_run = " ".join(command[0]) + " | " + " ".join(command[1]) + " --silent"
        self.max_id = RunSimpleCommand.run_with_output(to_run, shell=True)

    def record_max_revid(self):
        self.get_max_revid()
        if not self.dryrun:
            file_obj = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
            FileUtils.write_file_in_place(file_obj.get_path(), self.max_id,
                                          self.wiki.config.fileperms)

    def read_max_revid_from_file(self, date=None):
        if date is None:
            date = self.wiki.date
        try:
            file_obj = MaxRevIDFile(self.wiki.config, date, self.wiki.db_name)
            return FileUtils.read_file(file_obj.get_path().rstrip())
        except:
            return None

    def exists(self, date=None):
        if date is None:
            date = self.wiki.date
        return exists(MaxRevIDFile(self.wiki.config, date, self.wiki.db_name).get_path())


class MaxRevIDFile(ContentFile):
    def get_filename(self):
        return "maxrevid.txt"


class StubFile(ContentFile):
    def get_filename(self):
        return "%s-%s-stubs-meta-hist-incr.xml.gz" % (self.wikiname, self.date)


class RevsFile(ContentFile):
    def get_filename(self):
        return "%s-%s-pages-meta-hist-incr.xml.bz2" % (self.wikiname, self.date)
