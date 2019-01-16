#!/usr/bin/python3
"""
manage a list of dump jobs for a dump run
for a particular wiki, choosing the right
ones to add to the list, given the wiki
configuration, marking the right ones
as ones we want to run, etc
"""
import sys
import traceback

from dumps.exceptions import BackupError

from dumps.apijobs import SiteInfoDump
from dumps.tablesjobs import PrivateTable, PublicTable, TitleDump, AllTitleDump
from dumps.recombinejobs import RecombineAbstractDump, RecombineXmlDump
from dumps.recombinejobs import RecombineXmlStub, RecombineXmlRecompressDump
from dumps.recombinejobs import RecombineXmlLoggingDump, RecombineXmlMultiStreamDump
from dumps.xmljobs import XmlLogging, XmlStub, AbstractDump
from dumps.xmlcontentjobs import XmlDump, BigXmlDump
from dumps.recompressjobs import XmlMultiStreamDump, XmlRecompressDump
from dumps.flowjob import FlowDump


def get_setting(settings, setting_name):
    '''
    given a string of settings like "xmlstubsdump=this,xmldump=that",
    return the value in the string for the specified setting name
    or None if not present
    '''
    if '=' not in settings:
        return None
    if ',' in settings:
        pairs = settings.split(',')
    else:
        pairs = [settings]
    for pair in pairs:
        if pair.startswith(setting_name + "="):
            return pair.split('=')[1]
    return None


def get_int_setting(settings, setting_name):
    '''
    given a string of settings like "xmlstubsdump=num,xmldump=num",
    return the int value in the string for the specified setting name
    or None if not present
    '''
    value = get_setting(settings, setting_name)
    if value is not None and value.isdigit():
        return int(value)
    return None


def normalize_tablejob_name(jobname):
    """
    make sure all jobs that dump tables have name converted
    to end in 'table' (in most places we pass just the name of the
    table around)
    """
    if jobname.endswith("table"):
        return jobname
    return jobname + "table"


class DumpItemList():
    """
    manage a list of dump items (jobs) to be possibly run
    """
    def __init__(self, wiki, prefetch, prefetchdate, spawn, partnum_todo, checkpoint_file,
                 singleJob, skip_jobs, filepart, page_id_range, dumpjobdata, dump_dir,
                 verbose):
        self.wiki = wiki
        self._has_flow = self.wiki.has_flow()
        self._prefetch = prefetch
        self._prefetchdate = prefetchdate
        self._spawn = spawn
        self.filepart = filepart
        self.checkpoint_file = checkpoint_file
        self._partnum_todo = partnum_todo
        self._single_job = singleJob
        self.skip_jobs = skip_jobs
        self.dumpjobdata = dumpjobdata
        self.dump_dir = dump_dir
        self.jobsperbatch = self.wiki.config.jobsperbatch
        self.page_id_range = page_id_range
        self.verbose = verbose

        checkpoints = bool(self.wiki.config.checkpoint_time)

        if self._single_job and self._partnum_todo is not None:
            if (self._single_job[-5:] == 'table' or
                    self._single_job[-9:] == 'recombine' or
                    self._single_job in ['createdirs', 'noop', 'latestlinks',
                                         'xmlpagelogsdump', 'pagetitlesdump',
                                         'alllpagetitlesdump'] or
                    self._single_job.endswith('recombine')):
                raise BackupError("You cannot specify a file part with the job %s, exiting.\n"
                                  % self._single_job)

        if self._single_job and self.checkpoint_file is not None:
            if (self._single_job[-5:] == 'table' or
                    self._single_job[-9:] == 'recombine' or
                    self._single_job in ['createdirs', 'noop', 'latestlinks',
                                         'xmlpagelogsdump', 'pagetitlesdump',
                                         'alllpagetitlesdump', 'abstractsdump',
                                         'xmlstubsdump'] or
                    self._single_job.endswith('recombine')):
                raise BackupError("You cannot specify a checkpoint file with the job %s, exiting.\n"
                                  % self._single_job)

        self.dump_items = []
        tables_known = self.wiki.get_known_tables()
        tables_configured = self.wiki.config.get_tablejobs_from_conf()['tables']
        for table in tables_configured:
            # account for wikis without the particular extension or feature enabled
            if table not in tables_known:
                continue

            try:
                # tables job names end in 'table' so stick that on
                if tables_configured[table]['type'] == 'private':
                    if not self.wiki.config.skip_privatetables:
                        self.dump_items.append(PrivateTable(
                            table,
                            normalize_tablejob_name(tables_configured[table]['job']),
                            tables_configured[table]['description']))
                elif tables_configured[table]['type'] == 'public':
                    self.dump_items.append(PublicTable(
                        table,
                        normalize_tablejob_name(tables_configured[table]['job']),
                        tables_configured[table]['description']))
                else:
                    raise BackupError("Unknown table type in table jobs config: " +
                                      tables_configured[table]['type'] +
                                      " for table " + table)
            except Exception:
                # whine about missing keys etc
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(
                    exc_type, exc_value, exc_traceback)))

        apijobs_configured = self.wiki.config.get_apijobs_from_conf()
        for apijob_type in apijobs_configured:
            if apijob_type == 'siteinfo':
                for apijob in apijobs_configured[apijob_type]:
                    self.dump_items.append(SiteInfoDump(
                        apijobs_configured[apijob_type][apijob]['properties'],
                        apijobs_configured[apijob_type][apijob]['job'],
                        apijobs_configured[apijob_type][apijob]['description']))
            else:
                raise BackupError("Unknown api job type in config: " + apijob_type)

        self.dump_items.extend([TitleDump("pagetitlesdump",
                                          "List of page titles in main namespace"),
                                AllTitleDump("allpagetitlesdump",
                                             "List of all page titles"),
                                AbstractDump("abstractsdump",
                                             "Extracted page abstracts for Yahoo",
                                             self._get_partnum_todo("abstractsdump"),
                                             self.wiki.db_name,
                                             get_int_setting(self.jobsperbatch, "abstractsdump"),
                                             self.filepart.get_pages_per_filepart_abstract())])

        self.append_job_if_needed(RecombineAbstractDump(
            "abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo",
            self.find_item_by_name('abstractsdump')))

        self.dump_items.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                                       self._get_partnum_todo("xmlstubsdump"),
                                       get_int_setting(self.jobsperbatch, "xmlstubsdump"),
                                       self.filepart.get_pages_per_filepart_history()))

        self.append_job_if_needed(RecombineXmlStub(
            "xmlstubsdumprecombine", "Recombine first-pass for page XML data dumps",
            self.find_item_by_name('xmlstubsdump')))

        # NOTE that the filepart thing passed here is irrelevant,
        # these get generated from the stubs which are all done in one pass
        self.dump_items.append(
            XmlDump("articles",
                    "articlesdump",
                    "<big><b>Articles, templates, media/file descriptions, " +
                    "and primary meta-pages.</b></big>",
                    "This contains current versions of article content, " +
                    "and is the archive most mirror sites will probably want.",
                    self.find_item_by_name('xmlstubsdump'), self._prefetch,
                    self._prefetchdate, self._spawn,
                    self.wiki, self._get_partnum_todo("articlesdump"),
                    self.filepart.get_pages_per_filepart_history(), checkpoints,
                    self.checkpoint_file, self.page_id_range, self.verbose))

        self.append_job_if_needed(
            RecombineXmlDump(
                "articlesdumprecombine",
                "<big><b>Recombine articles, templates, media/file descriptions, " +
                "and primary meta-pages.</b></big>",
                "This contains current versions of article content, and is " +
                "the archive most mirror sites will probably want.",
                self.find_item_by_name('articlesdump')))

        self.dump_items.append(
            XmlDump("meta-current",
                    "metacurrentdump",
                    "All pages, current versions only.",
                    "Discussion and user pages are included in this complete archive. " +
                    "Most mirrors won't want this extra material.",
                    self.find_item_by_name('xmlstubsdump'), self._prefetch,
                    self._prefetchdate,
                    self._spawn, self.wiki, self._get_partnum_todo("metacurrentdump"),
                    self.filepart.get_pages_per_filepart_history(), checkpoints,
                    self.checkpoint_file, self.page_id_range, self.verbose))

        self.append_job_if_needed(
            RecombineXmlDump(
                "metacurrentdumprecombine",
                "Recombine all pages, current versions only.",
                "Discussion and user pages are included in this complete archive. " +
                "Most mirrors won't want this extra material.",
                self.find_item_by_name('metacurrentdump')))

        self.dump_items.append(
            XmlLogging("Log events to all pages and users.",
                       self._get_partnum_todo("xmlpagelogsdump"),
                       get_int_setting(self.jobsperbatch, "xmlpagelogsdump"),
                       self.filepart.get_logitems_per_filepart_pagelogs()))

        self.append_job_if_needed(RecombineXmlLoggingDump(
            "xmlpagelogsdumprecombine", "Recombine Log events to all pages and users",
            self.find_item_by_name('xmlpagelogsdump')))

        self.append_job_if_needed(
            FlowDump("xmlflowdump", "content of flow pages in xml format"))
        self.append_job_if_needed(
            FlowDump("xmlflowhistorydump", "history content of flow pages in xml format", True))

        self.dump_items.append(
            BigXmlDump(
                "meta-history",
                "metahistorybz2dump",
                "All pages with complete page edit history (.bz2)",
                "These dumps can be *very* large, uncompressing up to " +
                "20 times the archive download size. " +
                "Suitable for archival and statistical use, " +
                "most mirror sites won't want or need this.",
                self.find_item_by_name('xmlstubsdump'), self._prefetch,
                self._prefetchdate, self._spawn,
                self.wiki, self._get_partnum_todo("metahistorybz2dump"),
                self.filepart.get_pages_per_filepart_history(),
                checkpoints, self.checkpoint_file, self.page_id_range, self.verbose))
        self.append_job_if_needed(
            RecombineXmlDump(
                "metahistorybz2dumprecombine",
                "Recombine all pages with complete edit history (.bz2)",
                "These dumps can be *very* large, uncompressing up to " +
                "100 times the archive download size. " +
                "Suitable for archival and statistical use, " +
                "most mirror sites won't want or need this.",
                self.find_item_by_name('metahistorybz2dump')))
        self.dump_items.append(
            XmlRecompressDump(
                "meta-history",
                "metahistory7zdump",
                "All pages with complete edit history (.7z)",
                "These dumps can be *very* large, uncompressing up to " +
                "100 times the archive download size. " +
                "Suitable for archival and statistical use, " +
                "most mirror sites won't want or need this.",
                self.find_item_by_name('metahistorybz2dump'),
                self.wiki, self._get_partnum_todo("metahistory7zdump"),
                self.filepart.get_pages_per_filepart_history(),
                checkpoints, self.checkpoint_file))
        self.append_job_if_needed(
            RecombineXmlRecompressDump(
                "metahistory7zdumprecombine",
                "Recombine all pages with complete edit history (.7z)",
                "These dumps can be *very* large, uncompressing " +
                "up to 100 times the archive download size. " +
                "Suitable for archival and statistical use, " +
                "most mirror sites won't want or need this.",
                self.find_item_by_name('metahistory7zdump'), self.wiki))
        # doing this only for recombined/full articles dump
        if self.wiki.config.multistream_enabled:
            input_for_multistream = "articlesdump"
            self.dump_items.append(
                XmlMultiStreamDump(
                    "articles",
                    "articlesmultistreamdump",
                    "Articles, templates, media/file descriptions, and " +
                    "primary meta-pages, in multiple bz2 streams, 100 pages per stream",
                    "This contains current versions of article content, " +
                    "in concatenated bz2 streams, 100 pages per stream, plus a separate" +
                    "index of page titles/ids and offsets into the file.  " +
                    "Useful for offline readers, or for parallel processing of pages.",
                    self.find_item_by_name(input_for_multistream), self.wiki, None))
            self.append_job_if_needed(RecombineXmlMultiStreamDump(
                "articlesmultistreamdumprecombine", "Recombine multiple bz2 streams",
                self.find_item_by_name('articlesmultistreamdump')))

        results = self.dumpjobdata.runinfo.get_old_runinfo_from_file()
        if results:
            for runinfo_entry in results:
                self._set_dump_item_runinfo(runinfo_entry)
            self.old_runinfo_retrieved = True
        else:
            self.old_runinfo_retrieved = False

    def append_job_if_needed(self, job):
        """
        if appropriate, append the specifed job to the list of
        jobs to poassibly be run; 'appropriate' means that
        according to the config settings for the wiki etc,
        this job can be run here
        """
        if job.name().endswith("recombine"):
            if self.filepart.parts_enabled():
                if (('metahistory' in job.name() and self.filepart.recombine_history()) or
                        ('metacurrent' in job.name() and self.filepart.recombine_metacurrent()) or
                        ('metahistory' not in job.name() and 'metacurrent' not in job.name())):
                    self.dump_items.append(job)
        elif 'flow' in job.name():
            if self._has_flow:
                self.dump_items.append(job)

    def all_possible_jobs_done(self):
        '''
        check to see if all jobs in the dump job list have been run
        if they are not meant to be skipped deliberately
        '''
        for item in self.dump_items:
            if (item.status() != "done" and item.status() != "failed" and
                    item.status() != "skipped"):
                return False
        return True

    def mark_dumps_to_run(self, job, skipgood=False):
        """
        determine list of dumps to run ("table" expands to all table dumps,
        the rest of the names expand to single items)
        and mark the items in the list as such
        return False if there is no such dump or set of dumps
        """
        if job == "tables":
            for item in self.dump_items:
                if item.name()[-5:] == "table":
                    if item.name in self.skip_jobs:
                        item.set_skipped()
                    elif not skipgood or item.status() != "done":
                        item.set_to_run(True)
            return True
        for item in self.dump_items:
            if item.name() == job:
                if item.name in self.skip_jobs:
                    item.set_skipped()
                elif not skipgood or item.status() != "done":
                    item.set_to_run(True)
                return True
        if job in ["noop", "latestlinks", "createdirs"]:
            return True
        sys.stderr.write("No job of the name specified exists. Choose one of the following:\n")
        sys.stderr.write("noop (runs no job but rewrites checksums files and"
                         "resets latest links)\n")
        sys.stderr.write("latestlinks (runs no job but resets latest links)\n")
        sys.stderr.write("createdirs (runs no job but creates dump dirs for the given date)\n")
        sys.stderr.write("tables (includes all items below that end in 'table')\n")
        for item in self.dump_items:
            sys.stderr.write("%s\n" % item.name())
        return False

    def mark_following_jobs_to_run(self, skipgood=False):
        """
        find the first job marked to run, mark the following ones
        this gets used when some wants to restart a dump run from
        job X (includes doing all jobs that follow X)
        """
        i = 0
        for item in self.dump_items:
            i = i + 1
            if item.to_run():
                for j in range(i, len(self.dump_items)):
                    if item.name in self.skip_jobs:
                        item.set_skipped()
                    elif not skipgood or item.status() != "done":
                        self.dump_items[j].set_to_run(True)
                break

    def mark_all_jobs_to_run(self, skipgood=False):
        """Marks each and every job to be run"""
        for item in self.dump_items:
            if item.name() in self.skip_jobs:
                item.set_skipped()
            elif not skipgood or item.status() != "done":
                item.set_to_run(True)

    def find_item_by_name(self, name):
        '''
        given the name of a job, find its entry in the job list
        and return it
        '''
        for item in self.dump_items:
            if item.name() == name:
                return item
        return None

    def _get_partnum_todo(self, job_name):
        if self._single_job:
            if self._single_job == job_name:
                return self._partnum_todo
        return False

    # read in contents from dump run info file and stuff into dump_items for later reference
    def _set_dump_item_runinfo(self, runinfo):
        if "name" not in runinfo:
            return False
        for item in self.dump_items:
            if item.name() == runinfo["name"]:
                if 'status' in runinfo:
                    item.set_status(runinfo["status"], False)
                if 'updated' in runinfo:
                    item.set_updated(runinfo["updated"])
                if "to_run" in runinfo:
                    item.set_to_run(runinfo["to_run"])
                return True
        return False
