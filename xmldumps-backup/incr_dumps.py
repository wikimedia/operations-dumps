'''
for every wiki, generate the max rev id if it isn't already
present from a previous attempt at a run, read the max rev id
from the previous adds changes dump, dump stubs, dump history file
based on stubs.
'''
import os
from os.path import exists
import time
import calendar
from dumps.wikidump import FileUtils
from dumps.utils import RunSimpleCommand
from dumps.utils import DbServerInfo
from dumps.utils import MultiVersion
from miscdumplib import ContentFile
from miscdumplib import StatusInfo
from miscdumplib import MiscDumpDir
from miscdumplib import MiscDumpConfig
from miscdumplib import MiscDumpBase
from miscdumplib import get_config_defaults
from miscdumplib import safe, run_simple_query


# pylint: disable=broad-except


class MaxRevID():
    '''
    retrieve, read, write max revid from database/file
    '''
    def __init__(self, wiki, cutoff, dryrun, log):
        self.wiki = wiki
        self.cutoff = cutoff
        self.dryrun = dryrun
        self.log = log
        self.max_id = None

    def get_max_revid(self):
        '''
        get max rev id from wiki db
        '''
        query = ("'select rev_id from revision where rev_timestamp < \"%s\" "
                 "order by rev_timestamp desc limit 1'" % self.cutoff)
        self.max_id = run_simple_query(query, self.wiki, self.log).decode('utf-8')

    def record_max_revid(self, date=None):
        '''
        get max rev id for wiki from db, save it to file
        if 'date' is provided, the file saved to will be for the
        specified date, otherwise for the date of the run
        '''
        self.get_max_revid()
        if date is None:
            date = self.wiki.date
        file_obj = MaxRevIDFile(self.wiki.config, date, self.wiki.db_name)
        if self.dryrun:
            print("would write file {path} with contents {revid}".format(
                path=file_obj.get_path(), revid=self.max_id))
        else:
            FileUtils.write_file_in_place(file_obj.get_path(), self.max_id,
                                          self.wiki.config.fileperms)

    def read_max_revid_from_file(self, date=None):
        '''
        read and return max rev id for wiki from file
        '''
        if date is None:
            date = self.wiki.date
        try:
            file_obj = MaxRevIDFile(self.wiki.config, date, self.wiki.db_name)
            return FileUtils.read_file(file_obj.get_path().rstrip())
        except Exception as ex:
            self.log.info("Error encountered reading maxrevid from %s ", file_obj.get_path(),
                          exc_info=ex)
            return None

    def exists(self, date=None):
        '''
        check if the max rev id file exists for given wiki and date of dump run
        '''
        if date is None:
            date = self.wiki.date
        return exists(MaxRevIDFile(self.wiki.config, date, self.wiki.db_name).get_path())


class MaxRevIDFile(ContentFile):
    '''
    file containing max revision id for wiki at time of run
    '''
    def get_filename(self):
        return "maxrevid.txt"


class StubFile(ContentFile):
    '''
    file containing metadata for revisions greater than last
    run's maxrevid and less than current maxrevid
    '''
    def get_filename(self):
        return "%s-%s-stubs-meta-hist-incr.xml.gz" % (self.wikiname, self.date)


class RevsFile(ContentFile):
    '''
    file containing revision content corresponding to the revision
    metadata (stubs) dumped for this wiki and date
    '''
    def get_filename(self):
        return "%s-%s-pages-meta-hist-incr.xml.bz2" % (self.wikiname, self.date)


def cutoff_from_date(date, config):
    '''
    given the date of the run and how much older in seconds
    we expect the cutoff to be, generate and return the
    age cutoff in yymmddhhmmss format

    this format is used for revision timestamps in the db
    so we need it when selecting revisions older than
    the desired cutoff
    '''
    return time.strftime(
        "%Y%m%d%H%M%S", time.gmtime(calendar.timegm(
            time.strptime(date + "235900UTC", "%Y%m%d%H%M%S%Z")) - config.delay))


# required for misc dump factory
class IncrDumpConfig(MiscDumpConfig):
    '''
    additional config settings for incremental dumps
    '''
    def __init__(self, config_file=None):
        defaults = get_config_defaults()
        defaults['delay'] = "43200"
        super().__init__(defaults, config_file)
        delay = self.conf.get("output", "delay")
        self.delay = int(delay, 0)


# required for misc dump factory
class IncrDump(MiscDumpBase):
    '''
    given a wiki object with date, config all set up,
    provide some methods for adds changes dumps for this one wiki
    '''
    # overrides base class
    def __init__(self, wiki, log, dryrun=False, args=None):
        '''
        wiki:     wikidump.wiki object with date set
        log:      logger object
        dryrun:   whether or not to run commands or display what would have been done
        args:     dict of additional args 'revsonly' and/or 'stubsonly'
                  indicating whether or not to dump rev content and/or stubs
        '''
        super().__init__(wiki, log, dryrun, args)
        self.cutoff = cutoff_from_date(self.wiki.date, self.wiki.config)

        if 'revsonly' in args:
            self.steps['stubs']['run'] = False
        if 'stubsonly' in args:
            self.steps['revs']['run'] = False

    # overrides base class
    def get_steps(self):
        revidfile = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        revid_filename = revidfile.get_filename()

        stubfile = StubFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        stub_filename = stubfile.get_filename()

        revsfile = RevsFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        revs_filename = revsfile.get_filename()

        steps = {'maxrevid': {'file': revid_filename, 'run': True},
                 'stubs': {'file': stub_filename, 'run': True},
                 'revs': {'file': revs_filename, 'run': True}}
        return steps

    # overrides base class
    def run(self):
        '''
        dump maxrevid, stubs for revs from previous maxrevid to current one,
        revision content for these stubs, for given wiki and date
        '''
        try:
            self.log.info("retrieving max rev id for wiki %s", self.wiki.db_name)
            max_revid = self.dump_max_revid()
            if not max_revid:
                return False

            self.log.info("retrieving prev max rev id for wiki %s", self.wiki.db_name)
            prev_revid = self.get_prev_revid(max_revid)
            if not prev_revid:
                return False

            self.log.info("producing stub file for wiki %s", self.wiki.db_name)
            if not self.dump_stub(prev_revid, max_revid):
                return False

            self.log.info("producing content file for wiki %s", self.wiki.db_name)
            if not self.dump_revs():
                return False
        except Exception as ex:
            self.log.warning("Error encountered running dump for %s ", self.wiki.db_name,
                             exc_info=ex)
            return False
        return True

    def get_prev_incrdate(self, date, dumpok=False, revidok=False):
        '''
        find the most recent incr dump before the
        specified date
        if "dumpok" is True, find most recent dump that completed successfully
        if "revidok" is True, find most recent dump that has a populated maxrevid.txt file
        '''
        previous = None
        old = self.dirs.get_misc_dumpdirs()
        if old:
            for dump in old:
                if dump == date:
                    return previous
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
        '''
        get the previous rundate, with or without maxrevid file
        we can populate that file if need be
        '''
        prev_date = self.get_prev_incrdate(self.wiki.date)
        self.log.info("prev_date is %s", safe(prev_date))

        prev_revid = None

        if prev_date:
            cutoff = cutoff_from_date(prev_date, self.wiki.config)
            id_reader = MaxRevID(self.wiki, cutoff, self.dryrun, self.log)
            prev_revid = id_reader.read_max_revid_from_file(prev_date)

            if prev_revid is None:
                self.log.info("Wiki %s retrieving prevRevId from db.",
                              self.wiki.db_name)
                id_reader.record_max_revid(prev_date)
                prev_revid = id_reader.max_id
        else:
            self.log.info("Wiki %s no previous runs, using %s - 10 ",
                          self.wiki.db_name, max_revid)
            prev_revid = str(int(max_revid) - 10)
            if int(prev_revid) < 1:
                prev_revid = str(1)

        # this incr will cover every revision from the last
        # incremental through the maxid we wrote out already.
        if prev_revid is not None:
            prev_revid = str(int(prev_revid) + 1)
        self.log.info("prev_revid is %s", safe(prev_revid))
        return prev_revid

    def dump_max_revid(self):
        '''
        dump maximum rev id from wiki that's older than
        the configured number of seconds (cutoff)

        we have this cutoff so that content really new
        is not dumped; we want to give curators the chance to
        remove problematic entries first.

        a cutoff of some hours is reasonable.
        '''
        max_revid = None
        revidfile = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
        if exists(revidfile.get_path()):
            self.log.info("Wiki %s, max rev id file %s already exists",
                          self.wiki.db_name, revidfile.get_path())
        else:
            self.log.info("Wiki %s retrieving max revid from db.",
                          self.wiki.db_name)
            query = ("select rev_id from revision where rev_timestamp < \"%s\" "
                     "order by rev_timestamp desc limit 1" % self.cutoff)
            db_info = DbServerInfo(self.wiki, self.wiki.db_name)
            results = db_info.run_sql_and_get_output(query)
            if results:
                lines = results.splitlines()
                if lines and lines[1] and lines[1].isdigit():
                    max_revid = lines[1]
                    if self.dryrun:
                        print("would write file {path} with contents {revid}".format(
                            path=revidfile.get_path(), revid=max_revid))
                    else:
                        FileUtils.write_file_in_place(
                            revidfile.get_path(), max_revid.decode('utf-8'),
                            self.wiki.config.fileperms)
        if not max_revid:
            try:
                file_obj = MaxRevIDFile(self.wiki.config, self.wiki.date, self.wiki.db_name)
                max_revid = FileUtils.read_file(file_obj.get_path().rstrip())
            except Exception as ex:
                self.log.info("Error encountered reading maxrevid from %s ", file_obj.get_path(),
                              exc_info=ex)
                max_revid = None

        # end rev id is not included in dump
        if max_revid is not None:
            max_revid = str(int(max_revid) + 1)

        self.log.info("max_revid is %s", safe(max_revid))
        return max_revid

    def dump_stub(self, start_revid, end_revid):
        '''
        dump stubs (metadata) for revs from start_revid
        up to but not including end_revid
        '''
        if not self.steps['stubs']['run']:
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
            print("would run command for stubs dump:", command)
        else:
            self.log.info("running with no output: %s", " ".join(command))
            success = RunSimpleCommand.run_with_no_output(
                command, shell=False, timeout=self.get_lock_timeout_interval(),
                timeout_callback=self.periodic_callback)
            if not success:
                self.log.warning("error producing stub files for wiki %s", self.wiki.db_name)
                return False
        return True

    def dump_revs(self):
        '''
        dump revision content corresponding to previously-dumped
        stubs (revision metadata)
        '''
        if not self.steps['revs']['run']:
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
            print("would run command for revs dump:", command)
        else:
            self.log.info("running with no output: %s", " ".join(command))
            success = RunSimpleCommand.run_with_no_output(
                command, shell=False, timeout=self.get_lock_timeout_interval(),
                timeout_callback=self.periodic_callback)
            if not success:
                self.log.warning("error producing revision text files"
                                 " for wiki %s", self.wiki.db_name)
                return False
        return True


# required for misc dump factory
def get_incrdump_usage():
    '''
    return usage message for args specific to the incremental dumps
    (used for general usage message for misc dumps)
    '''
    return """Specific args for incremental dumps:

stubsonly        -- dump stubs but not revs
revsonly         -- dump revs but not stubs (requires that
                    stubs have already been dumped)
"""
