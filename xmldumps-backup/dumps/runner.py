import os
import sys
import shutil
import thread
import traceback
import Queue

from os.path import exists
from dumps.CommandManagement import CommandsInParallel
from dumps.exceptions import BackupError
from dumps.fileutils import DumpDir, DumpFilename

from dumps.tablesjobs import PrivateTable, PublicTable, TitleDump, AllTitleDump
from dumps.recombinejobs import RecombineAbstractDump, RecombineXmlDump, RecombineXmlStub, RecombineXmlRecompressDump
from dumps.xmljobs import XmlDump, XmlLogging, XmlStub, BigXmlDump, AbstractDump
from dumps.recompressjobs import XmlMultiStreamDump, XmlRecompressDump

from dumps.runnerutils import RunSettings, SymLinks, Feeds, NoticeFile
from dumps.runnerutils import Checksummer, IndexHtml, StatusHtml, FailureHandler
from dumps.runnerutils import Maintenance, RunInfoFile, DumpRunJobData

from dumps.utils import DbServerInfo, FilePartInfo, TimeUtils


class Logger(object):
    def __init__(self, log_filename=None):
        if log_filename:
            self.log_file = open(log_filename, "a")
        else:
            self.log_file = None
        self.queue = Queue.Queue()
        self.jobs_done = "JOBSDONE"

    def log_write(self, line=None):
        if self.log_file:
            self.log_file.write(line)
            self.log_file.flush()

    def log_close(self):
        if self.log_file:
            self.log_file.close()
            # return 1 if logging terminated, 0 otherwise

    def do_job_on_log_queue(self):
        line = self.queue.get()
        if line == self.jobs_done:
            self.log_close()
            return 1
        else:
            self.log_write(line)
            return 0

    def add_to_log_queue(self, line=None):
        if line:
            self.queue.put_nowait(line)

    # set in order to have logging thread clean up and exit
    def indicate_jobs_done(self):
        self.queue.put_nowait(self.jobs_done)


class DumpItemList(object):
    def __init__(self, wiki, prefetch, spawn, partnum_todo, checkpoint_file,
                 singleJob, skip_jobs, filepart, page_id_range, dumpjobdata, dump_dir):
        self.wiki = wiki
        self._has_flagged_revs = self.wiki.has_flagged_revs()
        self._has_wikidata = self.wiki.has_wikidata()
        self._has_global_usage = self.wiki.has_global_usage()
        self._is_wikidata_client = self.wiki.is_wikidata_client()
        self._prefetch = prefetch
        self._spawn = spawn
        self.filepart = filepart
        self.checkpoint_file = checkpoint_file
        self._partnum_todo = partnum_todo
        self._single_job = singleJob
        self.skip_jobs = skip_jobs
        self.dumpjobdata = dumpjobdata
        self.dump_dir = dump_dir
        self.page_id_range = page_id_range

        if self.wiki.config.checkpoint_time:
            checkpoints = True
        else:
            checkpoints = False

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

        self.dump_items = [PrivateTable("user", "usertable", "User account data."),
                           PrivateTable("watchlist", "watchlisttable",
                                        "Users' watchlist settings."),
                           PrivateTable("ipblocks", "ipblockstable",
                                        "Data for blocks of IP addresses, ranges, and users."),
                           PrivateTable("archive", "archivetable",
                                        "Deleted page and revision data."),
                           PrivateTable("logging", "loggingtable",
                                        "Data for various events (deletions, uploads, etc)."),
                           PrivateTable("oldimage", "oldimagetable",
                                        "Metadata on prior versions of uploaded images."),

                           PublicTable("site_stats", "sitestatstable",
                                       "A few statistics such as the page count."),
                           PublicTable("image", "imagetable",
                                       "Metadata on current versions of uploaded media/files."),
                           PublicTable("pagelinks", "pagelinkstable",
                                       "Wiki page-to-page link records."),
                           PublicTable("categorylinks", "categorylinkstable",
                                       "Wiki category membership link records."),
                           PublicTable("imagelinks", "imagelinkstable",
                                       "Wiki media/files usage records."),
                           PublicTable("templatelinks", "templatelinkstable",
                                       "Wiki template inclusion link records."),
                           PublicTable("externallinks", "externallinkstable",
                                       "Wiki external URL link records."),
                           PublicTable("langlinks", "langlinkstable",
                                       "Wiki interlanguage link records."),
                           PublicTable("user_groups", "usergroupstable", "User group assignments."),
                           PublicTable("category", "categorytable", "Category information."),

                           PublicTable("page", "pagetable",
                                       "Base per-page data (id, title, old restrictions, etc)."),
                           PublicTable("page_restrictions", "pagerestrictionstable",
                                       "Newer per-page restrictions table."),
                           PublicTable("page_props", "pagepropstable",
                                       "Name/value pairs for pages."),
                           PublicTable("protected_titles", "protectedtitlestable",
                                       "Nonexistent pages that have been protected."),
                           PublicTable("redirect", "redirecttable", "Redirect list"),
                           PublicTable("iwlinks", "iwlinkstable",
                                       "Interwiki link tracking records"),
                           PublicTable("geo_tags", "geotagstable",
                                       "List of pages' geographical coordinates"),
                           PublicTable("change_tag", "changetagstable",
                                       "List of annotations (tags) for revisions and log entries"),

                           TitleDump("pagetitlesdump", "List of page titles in main namespace"),
                           AllTitleDump("allpagetitlesdump", "List of all page titles"),

                           AbstractDump("abstractsdump", "Extracted page abstracts for Yahoo",
                                        self._get_partnum_todo("abstractsdump"), self.wiki.db_name,
                                        self.filepart.get_pages_per_filepart_abstract())]

        self.append_job_if_needed(RecombineAbstractDump(
            "abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo",
            self.find_item_by_name('abstractsdump')))

        self.dump_items.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                                       self._get_partnum_todo("xmlstubsdump"),
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
                    self.find_item_by_name('xmlstubsdump'), self._prefetch, self._spawn,
                    self.wiki, self._get_partnum_todo("articlesdump"),
                    self.filepart.get_pages_per_filepart_history(), checkpoints,
                    self.checkpoint_file, self.page_id_range))

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
                    self._spawn, self.wiki, self._get_partnum_todo("metacurrentdump"),
                    self.filepart.get_pages_per_filepart_history(), checkpoints,
                    self.checkpoint_file, self.page_id_range))

        self.append_job_if_needed(
            RecombineXmlDump(
                "metacurrentdumprecombine",
                "Recombine all pages, current versions only.",
                "Discussion and user pages are included in this complete archive. " +
                "Most mirrors won't want this extra material.",
                self.find_item_by_name('metacurrentdump')))

        self.dump_items.append(
            XmlLogging("Log events to all pages and users."))

        self.append_job_if_needed(
            PublicTable("flaggedpages", "flaggedpagestable",
                        "This contains a row for each flagged article, " +
                        "containing the stable revision ID, if the lastest edit " +
                        "was flagged, and how long edits have been pending."))
        self.append_job_if_needed(
            PublicTable("flaggedrevs", "flaggedrevstable",
                        "This contains a row for each flagged revision, " +
                        "containing who flagged it, when it was flagged, " +
                        "reviewer comments, the flag values, and the " +
                        "quality tier those flags fall under."))

        self.append_job_if_needed(
            PublicTable("wb_items_per_site", "wbitemspersitetable",
                        "For each Wikidata item, this contains rows with the " +
                        "corresponding page name on a given wiki project."))
        self.append_job_if_needed(
            PublicTable("wb_terms", "wbtermstable",
                        "For each Wikidata item, this contains rows with a label, " +
                        "an alias and a description of the item in a given language."))
        self.append_job_if_needed(
            PublicTable("wb_entity_per_page", "wbentityperpagetable",
                        "Contains a mapping of page ids and entity ids, with " +
                        "an additional entity type column."))
        self.append_job_if_needed(
            PublicTable("wb_property_info", "wbpropertyinfotable",
                        "Contains a mapping of Wikidata property ids and data types."))
        self.append_job_if_needed(
            PublicTable("wb_changes_subscription", "wbchangessubscriptiontable",
                        "Tracks which Wikibase Client wikis are using which items."))
        self.append_job_if_needed(
            PublicTable("sites", "sitestable",
                        "This contains the SiteMatrix information from " +
                        "meta.wikimedia.org provided as a table."))

        self.append_job_if_needed(
            PublicTable("globalimagelinks", "globalimagelinkstable",
                        "Global wiki media/files usage records."))

        self.append_job_if_needed(
            PublicTable("wbc_entity_usage", "wbcentityusagetable",
                        "Tracks which pages use which Wikidata items or properties " +
                        "and what aspect (e.g. item label) is used."))

        self.dump_items.append(
            BigXmlDump(
                "meta-history",
                "metahistorybz2dump",
                "All pages with complete page edit history (.bz2)",
                "These dumps can be *very* large, uncompressing up to " +
                "20 times the archive download size. " +
                "Suitable for archival and statistical use, " +
                "most mirror sites won't want or need this.",
                self.find_item_by_name('xmlstubsdump'), self._prefetch, self._spawn,
                self.wiki, self._get_partnum_todo("metahistorybz2dump"),
                self.filepart.get_pages_per_filepart_history(),
                checkpoints, self.checkpoint_file, self.page_id_range))
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
            if self.filepart.parts_enabled():
                input_for_multistream = "articlesdumprecombine"
            else:
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

        results = self.dumpjobdata.runinfofile.get_old_runinfo_from_file()
        if results:
            for runinfo_entry in results:
                self._set_dump_item_runinfo(runinfo_entry)
            self.old_runinfo_retrieved = True
        else:
            self.old_runinfo_retrieved = False

    def append_job_if_needed(self, job):
        if job.name().endswith("recombine"):
            if self.filepart.parts_enabled():
                if 'metahistory' not in job.name() or self.filepart.recombine_history():
                    self.dump_items.append(job)
            elif job.name().startswith("wb_") or job.name() == "sitestable":
                if self._has_wikidata:
                    self.dump_items.append(job)
            elif job.name().startswith("flagged"):
                if self._has_flagged_revs:
                    self.dump_items.append(job)
            elif job.name().startswith("global"):
                if self._has_global_usage:
                    self.dump_items.append(job)
            elif job.name().startswith("wbc_"):
                if self._is_wikidata_client:
                    self.dump_items.append(job)
                
    def append_job(self, jobname, job):
        if jobname not in self.skip_jobs:
            self.dump_items.append(job)

    def all_possible_jobs_done(self):
        for item in self.dump_items:
            if (item.status() != "done" and item.status() != "failed"
                    and item.status() != "skipped"):
                return False
        return True

    # determine list of dumps to run ("table" expands to all table dumps,
    # the rest of the names expand to single items)
    # and mark the items in the list as such
    # return False if there is no such dump or set of dumps
    def mark_dumps_to_run(self, job, skipgood=False):
        if job == "tables":
            for item in self.dump_items:
                if item.name()[-5:] == "table":
                    if item.name in self.skip_jobs:
                        item.set_skipped()
                    elif not skipgood or item.status() != "done":
                        item.set_to_run(True)
            return True
        else:
            for item in self.dump_items:
                if item.name() == job:
                    if item.name in self.skip_jobs:
                        item.set_skipped()
                    elif not skipgood or item.status() != "done":
                        item.set_to_run(True)
                    return True
        if job == "noop" or job == "latestlinks" or job == "createdirs":
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
        # find the first one marked to run, mark the following ones
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
                item.set_status(runinfo["status"], False)
                item.set_updated(runinfo["updated"])
                if "to_run" in runinfo:
                    item.set_to_run(runinfo["to_run"])
                return True
        return False


class Runner(object):
    def __init__(self, wiki, prefetch=True, spawn=True, job=None, skip_jobs=None,
                 restart=False, notice="", dryrun=False, enabled=None,
                 partnum_todo=None, checkpoint_file=None, page_id_range=None,
                 skipdone=False, cleanup=False, verbose=False):
        self.wiki = wiki
        self.db_name = wiki.db_name
        self.prefetch = prefetch
        self.spawn = spawn
        self.filepart_info = FilePartInfo(wiki, self.db_name, self.log_and_print)
        self.restart = restart
        self.html_notice_file = None
        self.log = None
        self.dryrun = dryrun
        self._partnum_todo = partnum_todo
        self.checkpoint_file = checkpoint_file
        self.page_id_range = page_id_range
        self.skipdone = skipdone
        self.verbose = verbose
        self.enabled = enabled
        self.cleanup_old_files = cleanup

        if self.checkpoint_file is not None:
            fname = DumpFilename(self.wiki)
            fname.new_from_filename(checkpoint_file)
            # we should get file partnum if any
            if self._partnum_todo is None and fname.partnum_int:
                self._partnum_todo = fname.partnum_int
            elif self._partnum_todo is not None and fname.partnum_int and self._partnum_todo != fname.partnum_int:
                raise BackupError("specifed partnum to do does not match part number "
                                  "of checkpoint file %s to redo", self.checkpoint_file)
            self.checkpoint_file = fname

        if self.enabled is None:
            self.enabled = {}
        for setting in [StatusHtml.NAME, IndexHtml.NAME, Checksummer.NAME,
                        RunInfoFile.NAME, SymLinks.NAME, RunSettings.NAME,
                        Feeds.NAME, NoticeFile.NAME, "makedir", "clean_old_dumps",
                        "cleanup_old_files", "check_trunc_files"]:
            self.enabled[setting] = True

        if not self.cleanup_old_files:
            del self.enabled["cleanup_old_files"]

        if self.dryrun or self._partnum_todo is not None or self.checkpoint_file is not None:
            for setting in [StatusHtml.NAME, IndexHtml.NAME, Checksummer.NAME,
                            RunInfoFile.NAME, SymLinks.NAME, RunSettings.NAME,
                            Feeds.NAME, NoticeFile.NAME, "makedir", "clean_old_dumps"]:
                del self.enabled[setting]

        if self.dryrun:
            for setting in ["check_trunc_files"]:
                del self.enabled[setting]
            # may not always be present
            if "logging" in self.enabled:
                del self.enabled["logging"]

        self.job_requested = job

        if self.job_requested == "latestlinks":
            for setting in [StatusHtml.NAME, IndexHtml.NAME, RunInfoFile.NAME]:
                del self.enabled[setting]

        if self.job_requested == "createdirs":
            for setting in [SymLinks.NAME, Feeds.NAME]:
                del self.enabled[setting]

        if self.job_requested == "latestlinks" or self.job_requested == "createdirs":
            for setting in [Checksummer.NAME, NoticeFile.NAME, "makedir",
                            "clean_old_dumps", "check_trunc_files"]:
                del self.enabled[setting]

        if self.job_requested == "noop":
            for setting in ["clean_old_dumps", "check_trunc_files"]:
                del self.enabled[setting]

        self.skip_jobs = skip_jobs
        if skip_jobs is None:
            self.skip_jobs = []

        self.db_server_info = DbServerInfo(self.wiki, self.db_name, self.log_and_print)
        self.dump_dir = DumpDir(self.wiki, self.db_name)

        # these must come after the dumpdir setup so we know which directory we are in
        if "logging" in self.enabled and "makedir" in self.enabled:
            file_obj = DumpFilename(self.wiki)
            file_obj.new_from_filename(self.wiki.config.log_file)
            self.log_filename = self.dump_dir.filename_private_path(file_obj)
            self.make_dir(os.path.join(self.wiki.private_dir(), self.wiki.date))
            self.log = Logger(self.log_filename)
            thread.start_new_thread(self.log_queue_reader, (self.log,))
        self.dumpjobdata = DumpRunJobData(self.wiki, self.dump_dir, notice,
                                          self.log_and_print, self.debug, self.enabled,
                                          self.verbose)

        # some or all of these dump_items will be marked to run
        self.dump_item_list = DumpItemList(self.wiki, self.prefetch, self.spawn,
                                           self._partnum_todo, self.checkpoint_file,
                                           self.job_requested, self.skip_jobs,
                                           self.filepart_info, self.page_id_range,
                                           self.dumpjobdata, self.dump_dir)
        # only send email failure notices for full runs
        if self.job_requested:
            email = False
        else:
            email = True
        self.failurehandler = FailureHandler(self.wiki, email)
        self.statushtml = StatusHtml(self.wiki, self.dump_dir,
                                     self.dump_item_list.dump_items,
                                     self.dumpjobdata, self.enabled,
                                     self.failurehandler,
                                     self.log_and_print, self.verbose)
        self.indexhtml = IndexHtml(self.wiki, self.dump_dir,
                                   self.dump_item_list.dump_items,
                                   self.dumpjobdata, self.enabled,
                                   self.failurehandler,
                                   self.log_and_print, self.verbose)


    def log_queue_reader(self, log):
        if not log:
            return
        done = False
        while not done:
            done = log.do_job_on_log_queue()

    def log_and_print(self, message):
        if hasattr(self, 'log') and self.log and "logging" in self.enabled:
            self.log.add_to_log_queue("%s\n" % message)
        sys.stderr.write("%s\n" % message)

    def html_update_callback(self):
        self.indexhtml.update_index_html()
        self.statushtml.update_status_file()

    # returns 0 on success, 1 on error
    def save_command(self, commands, outfile):
        """For one pipeline of commands, redirect output to a given file."""
        commands[-1].extend([">", outfile])
        series = [commands]
        if self.dryrun:
            self.pretty_print_commands([series])
            return 0
        else:
            return self.run_command([series], callback_timed=self.html_update_callback)

    def pretty_print_commands(self, command_series_list):
        for series in command_series_list:
            for pipeline in series:
                command_strings = []
                for command in pipeline:
                    command_strings.append(" ".join(command))
                pipeline_string = " | ".join(command_strings)
                print "Command to run: ", pipeline_string

    # command series list: list of (commands plus args)
    # is one pipeline. list of pipelines = 1 series.
    # this function wants a list of series.
    # be a list (the command name and the various args)
    # If the shell option is true, all pipelines will be run under the shell.
    # callbackinterval: how often we will call callback_timed (in milliseconds),
    # defaults to every 5 secs
    def run_command(self, command_series_list, callback_stderr=None,
                    callback_stderr_arg=None, callback_timed=None,
                    callback_timed_arg=None, shell=False, callback_interval=5000):
        """Nonzero return code from the shell from any command in any pipeline will cause
        this function to print an error message and return 1, indicating error.
        Returns 0 on success.
        If a callback function is passed, it will receive lines of
        output from the call.  If the callback function takes another argument (which will
        be passed before the line of output) must be specified by the arg paraemeter.
        If no callback is provided, and no output file is specified for a given
        pipe, the output will be written to stderr. (Do we want that?)
        This function spawns multiple series of pipelines  in parallel.

        """
        if self.dryrun:
            self.pretty_print_commands(command_series_list)
            return 0

        else:
            commands = CommandsInParallel(command_series_list, callback_stderr=callback_stderr,
                                          callback_stderr_arg=callback_stderr_arg,
                                          callback_timed=callback_timed,
                                          callback_timed_arg=callback_timed_arg,
                                          shell=shell, callback_interval=callback_interval)
            commands.run_commands()
            if commands.exited_successfully():
                return 0
            else:
                problem_commands = commands.commands_with_errors()
                error_string = "Error from command(s): "
                for cmd in problem_commands:
                    error_string = error_string + "%s " % cmd
                self.log_and_print(error_string)
                return 1

    def debug(self, stuff):
        self.log_and_print("%s: %s %s" % (TimeUtils.pretty_time(), self.db_name, stuff))

    def run_handle_failure(self):
        if self.failurehandler.failure_count < 1:
            # Email the site administrator just once per database
            self.failurehandler.report_failure()
        self.failurehandler.failure_count += 1

    def run(self):
        if self.job_requested:
            if not self.dump_item_list.old_runinfo_retrieved and self.wiki.exists_perdump_index():

                # There was a previous run of all or part of this date, but...
                # There was no old RunInfo to be had (or an error was encountered getting it)
                # so we can't rerun a step and keep all the status information
                # about the old run around.
                # In this case ask the user if they reeeaaally want to go ahead
                print "No information about the previous run for this date could be retrieved."
                print "This means that the status information about the old run will be lost, and"
                print "only the information about the current (and future) runs will be kept."
                reply = raw_input("Continue anyways? [y/N]: ")
                if reply not in ["y", "Y"]:
                    raise RuntimeError("No run information available for previous dump, exiting")

            if not self.dump_item_list.mark_dumps_to_run(self.job_requested, self.skipdone):
                # probably no such job
                sys.stderr.write("No job marked to run, exiting")
                return None
            if self.restart:
                # mark all the following jobs to run as well
                self.dump_item_list.mark_following_jobs_to_run(self.skipdone)
        else:
            self.dump_item_list.mark_all_jobs_to_run(self.skipdone)

        Maintenance.exit_if_in_maintenance_mode(
            "In maintenance mode, exiting dump of %s" % self.db_name)

        self.make_dir(os.path.join(self.wiki.public_dir(), self.wiki.date))
        self.make_dir(os.path.join(self.wiki.private_dir(), self.wiki.date))

        self.show_runner_state("Cleaning up old dumps for %s" % self.db_name)
        self.clean_old_dumps()
        self.clean_old_dumps(private=True)

        # Informing what kind backup work we are about to do
        if self.job_requested:
            if self.restart:
                self.log_and_print("Preparing for restart from job %s of %s"
                                   % (self.job_requested, self.db_name))
            else:
                self.log_and_print("Preparing for job %s of %s" %
                                   (self.job_requested, self.db_name))
        else:
            self.show_runner_state("Starting backup of %s" % self.db_name)

        self.dumpjobdata.do_before_dump()

        for item in self.dump_item_list.dump_items:
            Maintenance.exit_if_in_maintenance_mode(
                "In maintenance mode, exiting dump of %s at step %s"
                % (self.db_name, item.name()))
            if item.to_run():
                item.start()
                self.indexhtml.update_index_html()
                self.statushtml.update_status_file()

                self.dumpjobdata.do_before_job(self.dump_item_list.dump_items)

                try:
                    item.dump(self)
                except Exception, ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    if self.verbose:
                        sys.stderr.write(repr(traceback.format_exception(
                            exc_type, exc_value, exc_traceback)))
                    else:
                        if exc_type.__name__ == 'BackupPrereqError':
                            self.debug(str(ex))
                        else:
                            self.debug("*** exception! " + str(ex))
                        if exc_type.__name__ != 'BackupPrereqError':
                            item.set_status("failed")

            if item.status() == "done":
                self.dumpjobdata.do_after_job(item)
            elif item.status() == "waiting" or item.status() == "skipped":
                # don't update the checksum files for this item.
                continue
            else:
                # Here for example status is "failed". But maybe also
                # "in-progress", if an item chooses to override dump(...) and
                # forgets to set the status. This is a failure as well.
                self.run_handle_failure()

        # special case
        if self.job_requested == "createdirs":
            if not os.path.exists(os.path.join(self.wiki.public_dir(), self.wiki.date)):
                os.makedirs(os.path.join(self.wiki.public_dir(), self.wiki.date))
            if not os.path.exists(os.path.join(self.wiki.private_dir(), self.wiki.date)):
                os.makedirs(os.path.join(self.wiki.private_dir(), self.wiki.date))

        if self.dump_item_list.all_possible_jobs_done():
            # All jobs are either in status "done", "waiting", "failed", "skipped"
            self.indexhtml.update_index_html("done")
            self.statushtml.update_status_file("done")
        else:
            # This may happen if we start a dump now and abort before all items are
            # done. Then some are left for example in state "waiting". When
            # afterwards running a specific job, all (but one) of the jobs
            # previously in "waiting" are still in status "waiting"
            self.indexhtml.update_index_html("partialdone")
            self.statushtml.update_status_file("partialdone")

        self.dumpjobdata.do_after_dump(self.dump_item_list.dump_items)

        # special case
        if (self.job_requested and self.job_requested == "latestlinks" and
                self.dump_item_list.all_possible_jobs_done()):
            self.dumpjobdata.do_latest_job()

        # Informing about completion
        if self.job_requested:
            if self.restart:
                self.show_runner_state("Completed run restarting from job %s for %s"
                                       % (self.job_requested, self.db_name))
            else:
                self.show_runner_state("Completed job %s for %s"
                                       % (self.job_requested, self.db_name))
        else:
            self.show_runner_state_complete()

        # let caller know if this was a successful run
        if self.failurehandler.failure_count > 0:
            return False
        else:
            return True

    def clean_old_dumps(self, private=False):
        """Removes all but the wiki.config.keep last dumps of this wiki.
        If there is already a directory for todays dump, this is omitted in counting
        and not removed."""
        if "clean_old_dumps" in self.enabled:
            if private:
                old = self.wiki.dump_dirs(private=True)
                dumptype = 'private'
            else:
                old = self.wiki.dump_dirs()
                dumptype = 'public'
            if old:
                if old[-1] == self.wiki.date:
                    # If we're re-running today's (or jobs from a given day's) dump, don't count
                    # it as one of the old dumps to keep... or delete it halfway through!
                    old = old[:-1]
                if self.wiki.config.keep > 0:
                    # Keep the last few
                    old = old[:-(self.wiki.config.keep)]
            if old:
                for dump in old:
                    self.show_runner_state("Purging old %s dump %s for %s" %
                                           (dumptype, dump, self.db_name))
                    if private:
                        base = os.path.join(self.wiki.private_dir(), dump)
                    else:
                        base = os.path.join(self.wiki.public_dir(), dump)
                    shutil.rmtree("%s" % base)
            else:
                self.show_runner_state("No old %s dumps to purge." % dumptype)

    def show_runner_state(self, message):
        self.debug(message)

    def show_runner_state_complete(self):
        self.debug("SUCCESS: done.")

    def make_dir(self, dirname):
        if "makedir" in self.enabled:
            if exists(dirname):
                self.debug("Checkdir dir %s ..." % dirname)
            else:
                self.debug("Creating %s ..." % dirname)
                os.makedirs(dirname)

