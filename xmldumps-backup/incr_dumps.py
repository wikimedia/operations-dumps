# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

import os
from os.path import exists
import time
import calendar
from dumps.WikiDump import FileUtils
from dumps.utils import RunSimpleCommand
from dumps.utils import DbServerInfo
from dumps.utils import MultiVersion
from miscdumplib import ContentFile
from miscdumplib import StatusInfo
from miscdumplib import MiscDumpDir
from miscdumplib import Config
from miscdumplib import MiscDumpDirs
from miscdumplib import get_config_defaults
from miscdumplib import log
from miscdumplib import safe


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
        log.info("running with no output: " + to_run)
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
        except Exception as ex:
            log.info("Error encountered reading maxrevid from %s ", file_obj.get_path(),
                     exc_info=ex)
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


def cutoff_from_date(date, config):
    return time.strftime(
        "%Y%m%d%H%M%S", time.gmtime(calendar.timegm(
            time.strptime(date + "235900UTC", "%Y%m%d%H%M%S%Z")) - config.delay))


class IncrDumpConfig(Config):
    '''
    additional config settings for incremental dumps
    '''
    def __init__(self, config_file=None):
        defaults = get_config_defaults()
        defaults['delay'] = "43200"
        super(IncrDumpConfig, self).__init__(defaults, config_file)
        delay = self.conf.get("output", "delay")
        self.delay = int(delay, 0)


class IncrDump(object):
    '''
    given a wiki object with date, config all set up,
    provide some methods for adds changes dumps for this one wiki
    '''
    def __init__(self, wiki, dryrun=False, args=None):
        self.wiki = wiki
        self.dirs = MiscDumpDirs(self.wiki.config, self.wiki.db_name)
        self.dryrun = dryrun
        self.args = args
        self.cutoff = cutoff_from_date(self.wiki.date, self.wiki.config)

        if 'revsonly' in args:
            self.dostubs = False
        else:
            self.dostubs = True
        if 'stubsonly' in args:
            self.dorevs = False
        else:
            self.dorevs = True

    def get_prev_incrdate(self, date, dumpok=False, revidok=False):
        # find the most recent incr dump before the
        # specified date
        # if "dumpok" is True, find most recent dump that completed successfully
        # if "revidok" is True, find most recent dump that has a populated maxrevid.txt file
        previous = None
        old = self.dirs.get_misc_dumpdirs()
        if old:
            for dump in old:
                if dump == date:
                    return previous
                else:
                    if dumpok:
                        status_info = StatusInfo(self.wiki.config, dump, self.wiki.db_name)
                        if status_info.get_status(dump) == "done":
                            previous = dump
                    elif revidok:
                        max_revid_file = MaxRevIDFile(self.wiki.config, dump, self.wiki.db_name)
                        if exists(max_revid_file.get_path()):
                            revid = FileUtils.read_file(max_revid_file.get_path().rstrip())
                            if int(revid) > 0:
                                previous = dump
                    else:
                        previous = dump
        return previous

    def get_prev_revid(self, max_revid):
        # get the previous rundate, with or without maxrevid file
        # we can populate that file if need be
        prev_date = self.get_prev_incrdate(self.wiki.date)
        log.info("prev_date is %s", safe(prev_date))

        prev_revid = None

        if prev_date:
            cutoff = cutoff_from_date(prev_date, self.wiki.config)
            id_reader = MaxRevID(self.wiki, cutoff, self.dryrun)
            prev_revid = id_reader.read_max_revid_from_file(prev_date)

            if prev_revid is None:
                log.info("Wiki %s retrieving prevRevId from db.",
                         self.wiki.db_name)
                id_reader.record_max_revid()
                prev_revid = id_reader.max_id
        else:
            log.info("Wiki %s no previous runs, using %s - 10 ",
                     self.wiki.db_name, max_revid)
            prev_revid = str(int(max_revid) - 10)
            if int(prev_revid) < 1:
                prev_revid = str(1)

        # this incr will cover every revision from the last
        # incremental through the maxid we wrote out already.
        if prev_revid is not None:
            prev_revid = str(int(prev_revid) + 1)
        log.info("prev_revid is %s", safe(prev_revid))
        return prev_revid

    def dump_max_revid(self):
        max_id = None
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        revidfile = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        if not exists(revidfile.get_path()):
            log.info("Wiki %s retrieving max revid from db.",
                     self.wiki.db_name)
            query = ("select rev_id from revision where rev_timestamp < \"%s\" "
                     "order by rev_timestamp desc limit 1" % self.cutoff)
            db_info = DbServerInfo(self.wiki, self.wiki.db_name)
            results = db_info.run_sql_and_get_output(query)
            if results:
                lines = results.splitlines()
                if lines and lines[1] and lines[1].isdigit():
                    max_id = lines[1]
                    FileUtils.write_file_in_place(revidfile.get_path(),
                                                  max_id, self.wiki.config.fileperms)
        try:
            file_obj = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
            max_revid = FileUtils.read_file(file_obj.get_path().rstrip())
        except Exception as ex:
            log.info("Error encountered reading maxrevid from %s ", file_obj.get_path(),
                     exc_info=ex)
            max_revid = None

        # end rev id is not included in dump
        if max_revid is not None:
            max_revid = str(int(max_revid) + 1)

        log.info("max_revid is %s", safe(max_revid))
        return max_revid

    def dump_stub(self, start_revid, end_revid):
        if not self.dostubs:
            return True

        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        stubfile = StubFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        outputfile = stubfile.get_filename()
        script_command = MultiVersion.mw_script_as_array(self.wiki.config,
                                                         "dumpBackup.php")
        command = [self.wiki.config.php]
        command.extend(script_command)
        command.extend(["--wiki=%s" % self.wiki.db_name, "--stub", "--quiet",
                        "--output=gzip:%s" % os.path.join(outputdir, outputfile),
                        "--revrange", "--revstart=%s" % start_revid,
                        "--revend=%s" % end_revid])
        if self.dryrun:
            print "would run command for stubs dump:", command
        else:
            log.info("running with no output: " + " ".join(command))
            success = RunSimpleCommand.run_with_no_output(
                command, shell=False)
            if not success:
                log.info("error producing stub files for wiki %s", self.wiki.db_name)
                return False
        return True

    def dump_revs(self):
        if not self.dorevs:
            return True
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        revsfile = RevsFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        outputfile = revsfile.get_filename()
        script_command = MultiVersion.mw_script_as_array(self.wiki.config,
                                                         "dumpTextPass.php")
        command = [self.wiki.config.php]
        command.extend(script_command)
        stubfile = StubFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        stuboutputfile = stubfile.get_filename()
        command.extend(["--wiki=%s" % self.wiki.db_name,
                        "--stub=gzip:%s" % os.path.join(outputdir, stuboutputfile),
                        "--quiet",
                        "--spawn=%s" % self.wiki.config.php,
                        "--output=bzip2:%s" % os.path.join(outputdir, outputfile)])
        if self.dryrun:
            print "would run command for revs dump:", command
        else:
            log.info("running with no output: " + " ".join(command))
            success = RunSimpleCommand.run_with_no_output(command, shell=False)
            if not success:
                log.info("error producing revision text files"
                         " for wiki %s", self.wiki.db_name)
                return False
        return True

    def run(self):
        try:
            log.info("retrieving max rev id for wiki %s", self.wiki.db_name)
            max_revid = self.dump_max_revid()
            if not max_revid:
                return False

            log.info("retrieving prev max rev id for wiki %s", self.wiki.db_name)
            prev_revid = self.get_prev_revid(max_revid)
            if not prev_revid:
                return False

            log.info("producing stub file for wiki %s", self.wiki.db_name)
            if not self.dump_stub(prev_revid, max_revid):
                return False

            log.info("producing content file for wiki %s", self.wiki.db_name)
            if not self.dump_revs():
                return False
        except Exception as ex:
            log.info("Error encountered runing dump for %s ", self.wiki.db_name,
                     exc_info=ex)
            return False
        return True

    def get_stages_done(self):
        """
        return comma-sep list of stages that are complete, in case not all are.
        if all are complete, return 'all'
        """
        if 'stubsonly' in self.args:
            return 'stubs'
        else:
            return 'all'

    def get_output_files(self):
        dumpdir = MiscDumpDir(self.wiki.config, self.wiki.date)
        outputdir = dumpdir.get_dumpdir(self.wiki.db_name, self.wiki.date)
        revidfile = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        stubfile = StubFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        revsfile = RevsFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        filenames = [revidfile.get_filename(), stubfile.get_filename(), revsfile.get_filename()]
        expected = []
        if self.dorevs:
            expected.append(revsfile)
        if self.dostubs:
            expected.append(stubfile)
        return [os.path.join(outputdir, filename) for filename in filenames], expected


def get_incrdump_usage():
    return """Specific args for incremental dumps:

stubsonly        -- dump stubs but not revs
revsonly         -- dump revs but not stubs (requires that
                    stubs have already been dumped)
"""
