# Worker process, does the actual dumping

import getopt, os, sys
import shutil
import Queue, thread, traceback

from os.path import exists
from dumps.WikiDump import TimeUtils, Wiki, Config, cleanup
from dumps.CommandManagement import CommandsInParallel
from dumps.jobs import *
from dumps.runnerutils import *
from dumps.utils import DbServerInfo

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
    def __init__(self, wiki, prefetch, spawn, chunk_to_do, checkpoint_file, singleJob, skip_jobs, chunk_info, page_id_range, runinfo_file, dump_dir):
        self.wiki = wiki
        self._has_flagged_revs = self.wiki.hasFlaggedRevs()
        self._has_wikidata = self.wiki.hasWikidata()
        self._is_wikidata_client = self.wiki.isWikidataClient()
        self._prefetch = prefetch
        self._spawn = spawn
        self.chunk_info = chunk_info
        self.checkpoint_file = checkpoint_file
        self._chunk_todo = chunk_to_do
        self._single_job = singleJob
        self.skip_jobs = skip_jobs
        self._runinfo_file = runinfo_file
        self.dump_dir = dump_dir
        self.page_id_range = page_id_range

        if self.wiki.config.checkpointTime:
            checkpoints = True
        else:
            checkpoints = False

        if self._single_job and self._chunk_todo:
            if (self._single_job[-5:] == 'table' or
                    self._single_job[-9:] == 'recombine' or
                    self._single_job == 'createdirs' or
                    self._single_job == 'noop' or
                    self._single_job == 'latestlinks' or
                    self._single_job == 'xmlpagelogsdump' or
                    self._single_job == 'pagetitlesdump' or
                    self._single_job == 'allpagetitlesdump' or
                    self._single_job.endswith('recombine')):
                raise BackupError("You cannot specify a chunk with the job %s, exiting.\n" % self._single_job)

        if self._single_job and self.checkpoint_file:
            if (self._single_job[-5:] == 'table' or
                    self._single_job[-9:] == 'recombine' or
                    self._single_job == 'noop' or
                    self._single_job == 'createdirs' or
                    self._single_job == 'latestlinks' or
                    self._single_job == 'xmlpagelogsdump' or
                    self._single_job == 'pagetitlesdump' or
                    self._single_job == 'allpagetitlesdump' or
                    self._single_job == 'abstractsdump' or
                    self._single_job == 'xmlstubsdump' or
                    self._single_job.endswith('recombine')):
                raise BackupError("You cannot specify a checkpoint file with the job %s, exiting.\n" % self._single_job)

        self.dump_items = [PrivateTable("user", "usertable", "User account data."),
                           PrivateTable("watchlist", "watchlisttable", "Users' watchlist settings."),
                           PrivateTable("ipblocks", "ipblockstable", "Data for blocks of IP addresses, ranges, and users."),
                           PrivateTable("archive", "archivetable", "Deleted page and revision data."),
                           #PrivateTable("updates", "updatestable", "Update dataset for OAI updater system."),
                           PrivateTable("logging", "loggingtable", "Data for various events (deletions, uploads, etc)."),
                           PrivateTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded images."),
                           #PrivateTable("filearchive", "filearchivetable", "Deleted image data"),

                           PublicTable("site_stats", "sitestatstable", "A few statistics such as the page count."),
                           PublicTable("image", "imagetable", "Metadata on current versions of uploaded media/files."),
                           #PublicTable("oldimage", "oldimagetable", "Metadata on prior versions of uploaded media/files."),
                           PublicTable("pagelinks", "pagelinkstable", "Wiki page-to-page link records."),
                           PublicTable("categorylinks", "categorylinkstable", "Wiki category membership link records."),
                           PublicTable("imagelinks", "imagelinkstable", "Wiki media/files usage records."),
                           PublicTable("templatelinks", "templatelinkstable", "Wiki template inclusion link records."),
                           PublicTable("externallinks", "externallinkstable", "Wiki external URL link records."),
                           PublicTable("langlinks", "langlinkstable", "Wiki interlanguage link records."),
                           #PublicTable("interwiki", "interwikitable", "Set of defined interwiki prefixes and links for this wiki."),
                           PublicTable("user_groups", "usergroupstable", "User group assignments."),
                           PublicTable("category", "categorytable", "Category information."),

                           PublicTable("page", "pagetable", "Base per-page data (id, title, old restrictions, etc)."),
                           PublicTable("page_restrictions", "pagerestrictionstable", "Newer per-page restrictions table."),
                           PublicTable("page_props", "pagepropstable", "Name/value pairs for pages."),
                           PublicTable("protected_titles", "protectedtitlestable", "Nonexistent pages that have been protected."),
                           #PublicTable("revision", #revisiontable", "Base per-revision data (does not include text)."), // safe?
                           #PrivateTable("text", "texttable", "Text blob storage. May be compressed, etc."), // ?
                           PublicTable("redirect", "redirecttable", "Redirect list"),
                           PublicTable("iwlinks", "iwlinkstable", "Interwiki link tracking records"),
                           PublicTable("geo_tags", "geotagstable", "List of pages' geographical coordinates"),

                           TitleDump("pagetitlesdump", "List of page titles in main namespace"),
                           AllTitleDump("allpagetitlesdump", "List of all page titles"),

                           AbstractDump("abstractsdump", "Extracted page abstracts for Yahoo", self._get_chunk_to_do("abstractsdump"), self.wiki.dbName, self.chunk_info.get_pages_per_chunk_abstract())]

        if self.chunk_info.chunks_enabled():
            self.dump_items.append(RecombineAbstractDump("abstractsdumprecombine", "Recombine extracted page abstracts for Yahoo", self.find_item_by_name('abstractsdump')))

        self.dump_items.append(XmlStub("xmlstubsdump", "First-pass for page XML data dumps", self._get_chunk_to_do("xmlstubsdump"), self.chunk_info.get_pages_per_chunk_history()))
        if self.chunk_info.chunks_enabled():
            self.dump_items.append(RecombineXmlStub("xmlstubsdumprecombine", "Recombine first-pass for page XML data dumps", self.find_item_by_name('xmlstubsdump')))

        # NOTE that the chunk_info thing passed here is irrelevant, these get generated from the stubs which are all done in one pass
        self.dump_items.append(
            XmlDump("articles",
                    "articlesdump",
                    "<big><b>Articles, templates, media/file descriptions, and primary meta-pages.</b></big>",
                    "This contains current versions of article content, and is the archive most mirror sites will probably want.", self.find_item_by_name('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._get_chunk_to_do("articlesdump"), self.chunk_info.get_pages_per_chunk_history(), checkpoints, self.checkpoint_file, self.page_id_range))
        if self.chunk_info.chunks_enabled():
            self.dump_items.append(RecombineXmlDump("articlesdumprecombine", "<big><b>Recombine articles, templates, media/file descriptions, and primary meta-pages.</b></big>", "This contains current versions of article content, and is the archive most mirror sites will probably want.", self.find_item_by_name('articlesdump')))

        self.dump_items.append(
            XmlDump("meta-current",
                    "metacurrentdump",
                    "All pages, current versions only.",
                    "Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.find_item_by_name('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._get_chunk_to_do("metacurrentdump"), self.chunk_info.get_pages_per_chunk_history(), checkpoints, self.checkpoint_file, self.page_id_range))

        if self.chunk_info.chunks_enabled():
            self.dump_items.append(RecombineXmlDump("metacurrentdumprecombine", "Recombine all pages, current versions only.", "Discussion and user pages are included in this complete archive. Most mirrors won't want this extra material.", self.find_item_by_name('metacurrentdump')))

        self.dump_items.append(
            XmlLogging("Log events to all pages and users."))

        if self._has_flagged_revs:
            self.dump_items.append(
                PublicTable("flaggedpages", "flaggedpagestable", "This contains a row for each flagged article, containing the stable revision ID, if the lastest edit was flagged, and how long edits have been pending."))
            self.dump_items.append(
                PublicTable("flaggedrevs", "flaggedrevstable", "This contains a row for each flagged revision, containing who flagged it, when it was flagged, reviewer comments, the flag values, and the quality tier those flags fall under."))

        if self._has_wikidata:
            self.dump_items.append(
                PublicTable("wb_items_per_site", "wbitemspersitetable", "For each Wikidata item, this contains rows with the corresponding page name on a given wiki project."))
            self.dump_items.append(
                PublicTable("wb_terms", "wbtermstable", "For each Wikidata item, this contains rows with a label, an alias and a description of the item in a given language."))
            self.dump_items.append(
                PublicTable("wb_entity_per_page", "wbentityperpagetable", "Contains a mapping of page ids and entity ids, with an additional entity type column."))
            self.dump_items.append(
                PublicTable("wb_property_info", "wbpropertyinfotable", "Contains a mapping of Wikidata property ids and data types."))
            self.dump_items.append(
                PublicTable("wb_changes_subscription", "wbchangessubscriptiontable", "Tracks which Wikibase Client wikis are using which items."))
            self.dump_items.append(
                PublicTable("sites", "sitestable", "This contains the SiteMatrix information from meta.wikimedia.org provided as a table."))

        if self._is_wikidata_client:
            self.dump_items.append(
                PublicTable("wbc_entity_usage", "wbcentityusagetable", "Tracks which pages use which Wikidata items or properties and what aspect (e.g. item label) is used."))

        self.dump_items.append(
            BigXmlDump("meta-history",
                       "metahistorybz2dump",
                       "All pages with complete page edit history (.bz2)",
                       "These dumps can be *very* large, uncompressing up to 20 times the archive download size. " +
                       "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.find_item_by_name('xmlstubsdump'), self._prefetch, self._spawn, self.wiki, self._get_chunk_to_do("metahistorybz2dump"), self.chunk_info.get_pages_per_chunk_history(), checkpoints, self.checkpoint_file, self.page_id_range))
        if self.chunk_info.chunks_enabled() and self.chunk_info.recombine_history():
            self.dump_items.append(
                RecombineXmlDump("metahistorybz2dumprecombine",
                                 "Recombine all pages with complete edit history (.bz2)",
                                 "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                                 "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.find_item_by_name('metahistorybz2dump')))
        self.dump_items.append(
            XmlRecompressDump("meta-history",
                              "metahistory7zdump",
                              "All pages with complete edit history (.7z)",
                              "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                              "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.find_item_by_name('metahistorybz2dump'), self.wiki, self._get_chunk_to_do("metahistory7zdump"), self.chunk_info.get_pages_per_chunk_history(), checkpoints, self.checkpoint_file))
        if self.chunk_info.chunks_enabled() and self.chunk_info.recombine_history():
            self.dump_items.append(
                RecombineXmlRecompressDump("metahistory7zdumprecombine",
                                           "Recombine all pages with complete edit history (.7z)",
                                           "These dumps can be *very* large, uncompressing up to 100 times the archive download size. " +
                                           "Suitable for archival and statistical use, most mirror sites won't want or need this.", self.find_item_by_name('metahistory7zdump'), self.wiki))
        # doing this only for recombined/full articles dump
        if self.wiki.config.multistreamEnabled:
            if self.chunk_info.chunks_enabled():
                input_for_multistream = "articlesdumprecombine"
            else:
                input_for_multistream = "articlesdump"
            self.dump_items.append(
                XmlMultiStreamDump("articles",
                                   "articlesmultistreamdump",
                                   "Articles, templates, media/file descriptions, and primary meta-pages, in multiple bz2 streams, 100 pages per stream",
                                   "This contains current versions of article content, in concatenated bz2 streams, 100 pages per stream, plus a separate" +
                                   "index of page titles/ids and offsets into the file.  Useful for offline readers, or for parallel processing of pages.",
                                   self.find_item_by_name(input_for_multistream), self.wiki, None))

        results = self._runinfo_file.get_old_runinfo_from_file()
        if results:
            for runinfo_obj in results:
                self._set_dump_item_runinfo(runinfo_obj)
            self.old_runinfo_retrieved = True
        else:
            self.old_runinfo_retrieved = False

    def append_job(self, jobname, job):
        if jobname not in self.skip_jobs:
            self.dump_items.append(job)

    def report_dump_runinfo(self, done=False):
        """Put together a dump run info listing for this database, with all its component dumps."""
        runinfo_lines = [self._report_dump_runinfo_line(item) for item in self.dump_items]
        runinfo_lines.reverse()
        text = "\n".join(runinfo_lines)
        text = text + "\n"
        return text

    def all_possible_jobs_done(self, skip_jobs):
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
        sys.stderr.write("noop (runs no job but rewrites md5sums file and resets latest links)\n")
        sys.stderr.write("latestlinks (runs no job but resets latest links)\n")
        sys.stderr.write("createdirs (runs no job but creates dump dirs for the given date)\n")
        sys.stderr.write("tables (includes all items below that end in 'table')\n")
        for item in self.dump_items:
            sys.stderr.write("%s\n" % item.name())
            return False

    def mark_following_jobs_to_run(self, skipgood=False):
        # find the first one marked to run, mark the following ones
        i = 0;
        for item in self.dump_items:
            i = i + 1;
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

    def _get_chunk_to_do(self, job_name):
        if self._single_job:
            if self._single_job == job_name:
                return(self._chunk_todo)
        return(False)

    # read in contents from dump run info file and stuff into dump_items for later reference
    def _set_dump_item_runinfo(self, runinfo):
        if not runinfo.name():
            return False
        for item in self.dump_items:
            if item.name() == runinfo.name():
                item.set_status(runinfo.status(), False)
                item.set_updated(runinfo.updated())
                item.set_to_run(runinfo.to_run())
                return True
        return False

    # write dump run info file
    # (this file is rewritten with updates after each dumpItem completes)
    def _report_dump_runinfo_line(self, item):
        # even if the item has never been run we will at least have "waiting" in the status
        return "name:%s; status:%s; updated:%s" % (item.name(), item.status(), item.updated())


class Runner(object):
    def __init__(self, wiki, prefetch=True, spawn=True, job=None, skip_jobs=None, restart=False, notice="", dryrun=False, loggingEnabled=False, chunk_to_do=False, checkpoint_file=None, page_id_range=None, skipdone=False, verbose=False):
        self.wiki = wiki
        self.db_name = wiki.dbName
        self.prefetch = prefetch
        self.spawn = spawn
        self.chunk_info = Chunk(wiki, self.db_name, self.log_and_print)
        self.restart = restart
        self.html_notice_file = None
        self.log = None
        self.dryrun = dryrun
        self._chunk_todo = chunk_to_do
        self.checkpoint_file = checkpoint_file
        self.page_id_range = page_id_range
        self.skipdone = skipdone
        self.verbose = verbose

        if self.checkpoint_file:
            fname = DumpFilename(self.wiki)
            fname.new_from_filename(checkpoint_file)
            # we should get chunk if any
            if not self._chunk_todo and fname.chunk_int:
                self._chunk_todo = fname.chunk_int
            elif self._chunk_todo and fname.chunk_int and self._chunk_todo != fname.chunk_int:
                raise BackupError("specifed chunk to do does not match chunk of checkpoint file %s to redo", self.checkpoint_file)
            self.checkpoint_file = fname

        self._logging_enabled = loggingEnabled
        self._status_enabled = True
        self._checksummer_enabled = True
        self._runinfo_file_enabled = True
        self._symlinks_enabled = True
        self._feeds_enabled = True
        self._notice_file_enabled = True
        self._makedir_enabled = True
        self._clean_old_dumps_enabled = True
        self._cleanup_old_files_enabled = True
        self._check_for_trunc_files_enabled = True

        if self.dryrun or self._chunk_todo:
            self._status_enabled = False
            self._checksummer_enabled = False
            self._runinfo_file_enabled = False
            self._symlinks_enabled = False
            self._feeds_enabled = False
            self._notice_file_enabled = False
            self._makedir_enabled = False
            self._clean_old_dumps_enabled = False

        if self.dryrun:
            self._logging_enabled = False
            self._check_for_trunc_files_enabled = False
            self._cleanup_old_files_enabled = False

        if self.checkpoint_file:
            self._status_enabled = False
            self._checksummer_enabled = False
            self._runinfo_file_enabled = False
            self._symlinks_enabled = False
            self._feeds_enabled = False
            self._notice_file_enabled = False
            self._makedir_enabled = False
            self._clean_old_dumps_enabled = False

        if self.page_id_range:
            self._status_enabled = False
            self._checksummer_enabled = False
            self._runinfo_file_enabled = False
            self._symlinks_enabled = False
            self._feeds_enabled = False
            self._notice_file_enabled = False
            self._makedir_enabled = False
            self._cleanup_old_files_enabled = True

        self.job_requested = job

        self.skip_jobs = skip_jobs
        if skip_jobs is None:
            self.skip_jobs = []

        if self.job_requested == "latestlinks":
            self._status_enabled = False
            self._runinfo_file_enabled = False

        if self.job_requested == "createdirs":
            self._symlinks_enabled = False
            self._feeds_enabled = False

        if self.job_requested == "latestlinks" or self.job_requested == "createdirs":
            self._checksummer_enabled = False
            self._notice_file_enabled = False
            self._makedir_enabled = False
            self._clean_old_dumps_enabled = False
            self._cleanup_old_files_enabled = False
            self._check_for_trunc_files_enabled = False

        if self.job_requested == "noop":
            self._clean_old_dumps_enabled = False
            self._cleanup_old_files_enabled = False
            self._check_for_trunc_files_enabled = False

        self.db_server_info = DbServerInfo(self.wiki, self.db_name, self.log_and_print)
        self.dump_dir = DumpDir(self.wiki, self.db_name)

        # these must come after the dumpdir setup so we know which directory we are in
        if self._logging_enabled and self._makedir_enabled:
            file_obj = DumpFilename(self.wiki)
            file_obj.new_from_filename(self.wiki.config.log_file)
            self.log_filename = self.dump_dir.filename_private_path(file_obj)
            self.make_dir(os.path.join(self.wiki.privateDir(), self.wiki.date))
            self.log = Logger(self.log_filename)
            thread.start_new_thread(self.log_queue_reader, (self.log,))
        self.runinfo_file = RunInfoFile(wiki, self._runinfo_file_enabled, self.verbose)
        self.sym_links = SymLinks(self.wiki, self.dump_dir, self.log_and_print, self.debug, self._symlinks_enabled)
        self.feeds = Feeds(self.wiki, self.dump_dir, self.db_name, self.debug, self._feeds_enabled)
        self.html_notice_file = NoticeFile(self.wiki, notice, self._notice_file_enabled)
        self.checksums = Checksummer(self.wiki, self.dump_dir, self._checksummer_enabled, self.verbose)

        # some or all of these dump_items will be marked to run
        self.dump_item_list = DumpItemList(self.wiki, self.prefetch, self.spawn, self._chunk_todo, self.checkpoint_file, self.job_requested, self.skip_jobs, self.chunk_info, self.page_id_range, self.runinfo_file, self.dump_dir)
        # only send email failure notices for full runs
        if self.job_requested:
            email = False
        else:
            email = True
        self.status = Status(self.wiki, self.dump_dir, self.dump_item_list.dump_items, self.checksums, self._status_enabled, email, self.html_notice_file, self.log_and_print, self.verbose)

    def log_queue_reader(self, log):
        if not log:
            return
        done = False
        while not done:
            done = log.do_job_on_log_queue()

    def log_and_print(self, message):
        if hasattr(self, 'log') and self.log and self._logging_enabled:
            self.log.add_to_log_queue("%s\n" % message)
        sys.stderr.write("%s\n" % message)

    # returns 0 on success, 1 on error
    def save_command(self, commands, outfile):
        """For one pipeline of commands, redirect output to a given file."""
        commands[-1].extend([">", outfile])
        series = [commands]
        if self.dryrun:
            self.pretty_print_commands([series])
            return 0
        else:
            return self.run_command([series], callback_timed = self.status.update_status_files)

    def pretty_print_commands(self, command_series_list):
        for series in command_series_list:
            for pipeline in series:
                command_strings = []
                for command in pipeline:
                    command_strings.append(" ".join(command))
                pipeline_string = " | ".join(command_strings)
                print "Command to run: ", pipeline_string

    # command series list: list of (commands plus args) is one pipeline. list of pipelines = 1 series.
    # this function wants a list of series.
    # be a list (the command name and the various args)
    # If the shell option is true, all pipelines will be run under the shell.
    # callbackinterval: how often we will call callback_timed (in milliseconds), defaults to every 5 secs
    def run_command(self, command_series_list, callback_stderr=None, callback_stderr_arg=None, callback_timed=None, callback_timed_arg=None, shell=False, callback_interval=5000):
        """Nonzero return code from the shell from any command in any pipeline will cause this
        function to print an error message and return 1, indicating error.
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
            commands = CommandsInParallel(command_series_list, callback_stderr=callback_stderr, callback_stderr_arg=callback_stderr_arg, callback_timed=callback_timed, callback_timed_arg=callback_timed_arg, shell=shell, callback_interval=callback_interval)
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
        self.log_and_print("%s: %s %s" % (TimeUtils.prettyTime(), self.db_name, stuff))

    def run_handle_failure(self):
        if self.status.fail_count < 1:
            # Email the site administrator just once per database
            self.status.report_failure()
        self.status.fail_count += 1

    def run_update_item_fileinfo(self, item):
        # this will include checkpoint files if they are enabled.
        for file_obj in item.list_outfiles_to_publish(self.dump_dir):
            if exists(self.dump_dir.filename_public_path(file_obj)):
                # why would the file not exist? because we changed chunk numbers in the
                # middle of a run, and now we list more files for the next stage than there
                # were for earlier ones
                self.sym_links.save_symlink(file_obj)
                self.feeds.save_feed(file_obj)
                self.checksums.checksum(file_obj, self)
                self.sym_links.cleanup_symlinks()
                self.feeds.cleanup_feeds()

    def run(self):
        if self.job_requested:
            if not self.dump_item_list.old_runinfo_retrieved and self.wiki.existsPerDumpIndex():

                # There was a previous run of all or part of this date, but...
                # There was no old RunInfo to be had (or an error was encountered getting it)
                # so we can't rerun a step and keep all the status information about the old run around.
                # In this case ask the user if they reeeaaally want to go ahead
                print "No information about the previous run for this date could be retrieved."
                print "This means that the status information about the old run will be lost, and"
                print "only the information about the current (and future) runs will be kept."
                reply = raw_input("Continue anyways? [y/N]: ")
                if not reply in ["y", "Y"]:
                    raise RuntimeError("No run information available for previous dump, exiting")

            if not self.dump_item_list.mark_dumps_to_run(self.job_requested, self.skipdone):
                # probably no such job
                sys.stderr.write("No job marked to run, exiting")
                return None
            if self.restart:
                # mark all the following jobs to run as well
                self.dump_item_list.mark_following_jobs_to_run(self.skipdone)
        else:
            self.dump_item_list.mark_all_jobs_to_run(self.skipdone);

        Maintenance.exit_if_in_maintenance_mode("In maintenance mode, exiting dump of %s" % self.db_name)

        self.make_dir(os.path.join(self.wiki.publicDir(), self.wiki.date))
        self.make_dir(os.path.join(self.wiki.privateDir(), self.wiki.date))

        self.show_runner_state("Cleaning up old dumps for %s" % self.db_name)
        self.clean_old_dumps()
        self.clean_old_dumps(private=True)

        # Informing what kind backup work we are about to do
        if self.job_requested:
            if self.restart:
                self.log_and_print("Preparing for restart from job %s of %s" % (self.job_requested, self.db_name))
            else:
                self.log_and_print("Preparing for job %s of %s" % (self.job_requested, self.db_name))
        else:
            self.show_runner_state("Starting backup of %s" % self.db_name)

        self.checksums.prepare_checksums()

        for item in self.dump_item_list.dump_items:
            Maintenance.exit_if_in_maintenance_mode("In maintenance mode, exiting dump of %s at step %s" % (self.db_name, item.name()))
            if item.to_run():
                item.start(self)
                self.status.update_status_files()
                self.runinfo_file.save_dump_runinfo_file(self.dump_item_list.report_dump_runinfo())
                try:
                    item.dump(self)
                except Exception, ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    if self.verbose:
                        sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    else:
                        if exc_type.__name__ == 'BackupPrereqError':
                            self.debug(str(ex))
                        else:
                            self.debug("*** exception! " + str(ex))
                        if exc_type.__name__ != 'BackupPrereqError':
                            item.set_status("failed")

            if item.status() == "done":
                self.checksums.cp_md5_tmpfile_to_permfile()
                self.run_update_item_fileinfo(item)
            elif item.status() == "waiting" or item.status() == "skipped":
                # don't update the md5 file for this item.
                continue
            else:
                # Here for example status is "failed". But maybe also
                # "in-progress", if an item chooses to override dump(...) and
                # forgets to set the status. This is a failure as well.
                self.run_handle_failure()

                # special case
                if self.job_requested == "createdirs":
                    if not os.path.exists(os.path.join(self.wiki.publicDir(), self.wiki.date)):
                        os.makedirs(os.path.join(self.wiki.publicDir(), self.wiki.date))
                    if not os.path.exists(os.path.join(self.wiki.privateDir(), self.wiki.date)):
                        os.makedirs(os.path.join(self.wiki.privateDir(), self.wiki.date))

        if self.dump_item_list.all_possible_jobs_done(self.skip_jobs):
            # All jobs are either in status "done", "waiting", "failed", "skipped"
            self.status.update_status_files("done")
        else:
            # This may happen if we start a dump now and abort before all items are
            # done. Then some are left for example in state "waiting". When
            # afterwards running a specific job, all (but one) of the jobs
            # previously in "waiting" are still in status "waiting"
            self.status.update_status_files("partialdone")

        self.runinfo_file.save_dump_runinfo_file(self.dump_item_list.report_dump_runinfo())

        # if any job succeeds we might as well make the sym link
        if self.status.fail_count < 1:
            self.complete_dump()

        if self.job_requested:
            # special case...
            if self.job_requested == "latestlinks":
                if self.dump_item_list.all_possible_jobs_done(self.skip_jobs):
                    self.sym_links.remove_symlinks_from_old_runs(self.wiki.date)
                    self.feeds.cleanup_feeds()

        # Informing about completion
        if self.job_requested:
            if self.restart:
                self.show_runner_state("Completed run restarting from job %s for %s" % (self.job_requested, self.db_name))
            else:
                self.show_runner_state("Completed job %s for %s" % (self.job_requested, self.db_name))
        else:
            self.show_runner_state_complete()

        # let caller know if this was a successful run
        if self.status.fail_count > 0:
            return False
        else:
            return True

    def clean_old_dumps(self, private=False):
        """Removes all but the wiki.config.keep last dumps of this wiki.
        If there is already a directory for todays dump, this is omitted in counting and
        not removed."""
        if self._clean_old_dumps_enabled:
            if private:
                old = self.wiki.dumpDirs(private=True)
                dumptype='private'
            else:
                old = self.wiki.dumpDirs()
                dumptype='public'
            if old:
                if old[-1] == self.wiki.date:
                    # If we're re-running today's (or jobs from a given day's) dump, don't count it as one
                    # of the old dumps to keep... or delete it halfway through!
                    old = old[:-1]
                if self.wiki.config.keep > 0:
                    # Keep the last few
                    old = old[:-(self.wiki.config.keep)]
            if old:
                for dump in old:
                    self.show_runner_state("Purging old %s dump %s for %s" % (dumptype, dump, self.db_name))
                    if private:
                        base = os.path.join(self.wiki.privateDir(), dump)
                    else:
                        base = os.path.join(self.wiki.publicDir(), dump)
                    shutil.rmtree("%s" % base)
            else:
                self.show_runner_state("No old %s dumps to purge." % dumptype)

    def show_runner_state(self, message):
        self.debug(message)

    def show_runner_state_complete(self):
        self.debug("SUCCESS: done.")

    def complete_dump(self):
        # note that it's possible for links in "latest" to point to
        # files from different runs, in which case the md5sums file
        # will have accurate checksums for the run for which it was
        # produced, but not the other files. FIXME
        self.checksums.move_md5file_into_place()
        dumpfile = DumpFilename(self.wiki, None, self.checksums.get_checksum_filename_basename())
        self.sym_links.save_symlink(dumpfile)
        self.sym_links.cleanup_symlinks()

        for item in self.dump_item_list.dump_items:
            if item.to_run():
                dump_names = item.list_dumpnames()
                if type(dump_names).__name__!='list':
                    dump_names = [dump_names]

                if item._chunks_enabled:
                    # if there is a specific chunk, we want to only clear out
                    # old files for that piece, because new files for the other
                    # pieces may not have been generated yet.
                    chunk = item._chunk_todo
                else:
                    chunk = None

                checkpoint = None
                if item._checkpoints_enabled:
                    if item.checkpoint_file:
                        # if there's a specific checkpoint file we are
                        # rerunning, we would only clear out old copies
                        # of that very file. meh. how likely is it that we
                        # have one? these files are time based and the start/end pageids
                        # are going to fluctuate. whatever
                        checkpoint = item.checkpoint_file.checkpoint

                for dump in dump_names:
                    self.sym_links.remove_symlinks_from_old_runs(self.wiki.date, dump, chunk, checkpoint, onlychunks=item.onlychunks)

                self.feeds.cleanup_feeds()

    def make_dir(self, dir):
        if self._makedir_enabled:
            if exists(dir):
                self.debug("Checkdir dir %s ..." % dir)
            else:
                self.debug("Creating %s ..." % dir)
                os.makedirs(dir)

def check_jobs(wiki, date, job, skipjobs, page_id_range, chunk_to_do, checkpoint_file, prereqs=False, restart=False):
    '''
    if prereqs is False:
    see if dump run on specific date completed specific job(s)
    or if no job was specified, ran to completion

    if prereqs is True:
    see if dump run on specific date completed prereqs for specific job(s)
    or if no job was specified, return True

    '''
    if not date:
        return False

    if date == 'last':
        dumps = sorted(wiki.dumpDirs())
        if dumps:
            date = dumps[-1]
        else:
            # never dumped so that's the same as 'job didn't run'
            return False

    if not job and prereqs:
        return True

    wiki.setDate(date)

    runinfo_file = RunInfoFile(wiki, False)
    chunk_info = Chunk(wiki, wiki.dbName)
    dump_dir = DumpDir(wiki, wiki.dbName)
    dump_item_list = DumpItemList(wiki, False, False, chunk_to_do, checkpoint_file, job, skipjobs, chunk_info, page_id_range, runinfo_file, dump_dir)
    if not dump_item_list.old_runinfo_retrieved:
        # failed to get the run's info so let's call it 'didn't run'
        return False

    results = dump_item_list._runinfo_file.get_old_runinfo_from_file()
    if results:
        for runinfo_obj in results:
            dump_item_list._set_dump_item_runinfo(runinfo_obj)

    # mark the jobs we would run
    if job:
        dump_item_list.mark_dumps_to_run(job, True)
        if restart:
            dump_item_list.mark_following_jobs_to_run(True)
    else:
        dump_item_list.mark_all_jobs_to_run(True)

    if not prereqs:
        # see if there are any to run. no? then return True (all job(s) done)
        # otherwise return False (still some to do)
        for item in dump_item_list.dump_items:
            if item.to_run():
                return False
        return True
    else:
        # get the list of prereqs, see if they are all status done, if so
        # return True, otherwise False (still some to do)
        prereq_items = []
        for item in dump_item_list.dump_items:
            if item.name() == job:
                prereq_items = item._prerequisite_items
                break

        for item in prereq_items:
            if item.status() != "done":
                return False
        return True


def find_lock_next_wiki(config, locks_enabled, cutoff, bystatustime=False, check_job_status=False,
                        check_prereq_status=False, date=None, job=None, skipjobs=None, page_id_range=None,
                        chunk_to_do=None, checkpoint_file=None, restart=False):
    if config.halt:
        sys.stderr.write("Dump process halted by config.\n")
        return None

    next = config.dbListByAge(bystatustime)
    next.reverse()

    if verbose and not cutoff:
        sys.stderr.write("Finding oldest unlocked wiki...\n")

        # if we skip locked wikis which are missing the prereqs for this job,
        # there are still wikis where this job needs to run
        missing_prereqs = False
    for dbname in next:
        wiki = Wiki(config, dbname)
        if cutoff:
            last_updated = wiki.dateTouchedLatestDump()
            if last_updated >= cutoff:
                continue
        if check_job_status:
            if check_jobs(wiki, date, job, skipjobs, page_id_range, chunk_to_do, checkpoint_file, restart):
                continue
        try:
            if locks_enabled:
                wiki.lock()
            return wiki
        except:
            if check_prereq_status:
                # if we skip locked wikis which are missing the prereqs for this job,
                # there are still wikis where this job needs to run
                if not check_jobs(wiki, date, job, skipjobs, page_id_range, chunk_to_do,
                                  checkpoint_file, prereqs=True, restart=restart):
                    missing_prereqs = True
            sys.stderr.write("Couldn't lock %s, someone else must have got it...\n" % dbname)
            continue
    if missing_prereqs:
        return False
    else:
        return None


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    sys.stderr.write("Usage: python worker.py [options] [wikidbname]\n")
    sys.stderr.write("Options: --aftercheckpoint, --checkpoint, --chunk, --configfile, --date, --job, --skipjobs, --addnotice, --delnotice, --force, --noprefetch, --nospawn, --restartfrom, --log, --cutoff\n")
    sys.stderr.write("--aftercheckpoint: Restart thie job from the after specified checkpoint file, doing the\n")
    sys.stderr.write("               rest of the job for the appropriate chunk if chunks are configured\n")
    sys.stderr.write("               or for the all the rest of the revisions if no chunks are configured;\n")
    sys.stderr.write("               only for jobs articlesdump, metacurrentdump, metahistorybz2dump.\n")
    sys.stderr.write("--checkpoint:  Specify the name of the checkpoint file to rerun (requires --job,\n")
    sys.stderr.write("               depending on the file this may imply --chunk)\n")
    sys.stderr.write("--chunk:       Specify the number of the chunk to rerun (use with a specific job\n")
    sys.stderr.write("               to rerun, only if parallel jobs (chunks) are enabled).\n")
    sys.stderr.write("--configfile:  Specify an alternative configuration file to read.\n")
    sys.stderr.write("               Default config file name: wikidump.conf\n")
    sys.stderr.write("--date:        Rerun dump of a given date (probably unwise)\n")
    sys.stderr.write("               If 'last' is given as the value, will rerun dump from last run date if any,\n")
    sys.stderr.write("               or today if there has never been a previous run\n")
    sys.stderr.write("--addnotice:   Text message that will be inserted in the per-dump-run index.html\n")
    sys.stderr.write("               file; use this when rerunning some job and you want to notify the\n")
    sys.stderr.write("               potential downloaders of problems, for example.  This option\n")
    sys.stderr.write("               remains in effective for the specified wiki and date until\n")
    sys.stderr.write("               the delnotice option is given.\n")
    sys.stderr.write("--delnotice:   Remove any notice that has been specified by addnotice, for\n")
    sys.stderr.write("               the given wiki and date.\n")
    sys.stderr.write("--job:         Run just the specified step or set of steps; for the list,\n")
    sys.stderr.write("               give the option --job help\n")
    sys.stderr.write("               This option requires specifiying a wikidbname on which to run.\n")
    sys.stderr.write("               This option cannot be specified with --force.\n")
    sys.stderr.write("--skipjobs:    Comma separated list of jobs not to run on the wiki(s)\n")
    sys.stderr.write("               give the option --job help\n")
    sys.stderr.write("--dryrun:      Don't really run the job, just print what would be done (must be used\n")
    sys.stderr.write("               with a specified wikidbname on which to run\n")
    sys.stderr.write("--force:       remove a lock file for the specified wiki (dangerous, if there is\n")
    sys.stderr.write("               another process running, useful if you want to start a second later\n")
    sys.stderr.write("               run while the first dump from a previous date is still going)\n")
    sys.stderr.write("               This option cannot be specified with --job.\n")
    sys.stderr.write("--exclusive    Even if rerunning just one job of a wiki, get a lock to make sure no other\n")
    sys.stderr.write("               runners try to work on that wiki. Default: for single jobs, don't lock\n")
    sys.stderr.write("--noprefetch:  Do not use a previous file's contents for speeding up the dumps\n")
    sys.stderr.write("               (helpful if the previous files may have corrupt contents)\n")
    sys.stderr.write("--nospawn:     Do not spawn a separate process in order to retrieve revision texts\n")
    sys.stderr.write("--restartfrom: Do all jobs after the one specified via --job, including that one\n")
    sys.stderr.write("--skipdone:    Do only jobs that are not already succefully completed\n")
    sys.stderr.write("--log:         Log progress messages and other output to logfile in addition to\n")
    sys.stderr.write("               the usual console output\n")
    sys.stderr.write("--cutoff:      Given a cutoff date in yyyymmdd format, display the next wiki for which\n")
    sys.stderr.write("               dumps should be run, if its last dump was older than the cutoff date,\n")
    sys.stderr.write("               and exit, or if there are no such wikis, just exit\n")
    sys.stderr.write("--verbose:     Print lots of stuff (includes printing full backtraces for any exception)\n")
    sys.stderr.write("               This is used primarily for debugging\n")

    sys.exit(1)

def main():
    try:
        date = None
        config_file = False
        force_lock = False
        prefetch = True
        spawn = True
        restart = False
        job_requested = None
        skip_jobs = None
        enable_logging = False
        log = None
        html_notice = ""
        dryrun = False
        chunk_to_do = False
        after_checkpoint = False
        checkpoint_file = None
        page_id_range = None
        cutoff = None
        exitcode = 1
        skipdone = False
        do_locking = False
        verbose = False

        try:
            (options, remainder) = getopt.gnu_getopt(
                sys.argv[1:], "",
                ['date=', 'job=', 'skipjobs=', 'configfile=', 'addnotice=',
                 'delnotice', 'force', 'dryrun', 'noprefetch', 'nospawn',
                 'restartfrom', 'aftercheckpoint=', 'log', 'chunk=',
                 'checkpoint=', 'pageidrange=', 'cutoff=', "skipdone",
                 "exclusive", 'verbose'])
        except:
            usage("Unknown option specified")

        for (opt, val) in options:
            if opt == "--date":
                date = val
            elif opt == "--configfile":
                config_file = val
            elif opt == '--checkpoint':
                checkpoint_file = val
            elif opt == '--chunk':
                chunk_to_do = int(val)
            elif opt == "--force":
                force_lock = True
            elif opt == '--aftercheckpoint':
                after_checkpoint = True
                checkpoint_file = val
            elif opt == "--noprefetch":
                prefetch = False
            elif opt == "--nospawn":
                spawn = False
            elif opt == "--dryrun":
                dryrun = True
            elif opt == "--job":
                job_requested = val
            elif opt == "--skipjobs":
                skip_jobs = val
            elif opt == "--restartfrom":
                restart = True
            elif opt == "--log":
                enable_logging = True
            elif opt == "--addnotice":
                html_notice = val
            elif opt == "--delnotice":
                html_notice = False
            elif opt == "--pageidrange":
                page_id_range = val
            elif opt == "--cutoff":
                cutoff = val
                if not cutoff.isdigit() or not len(cutoff) == 8:
                    usage("--cutoff value must be in yyyymmdd format")
            elif opt == "--skipdone":
                skipdone = True
            elif opt == "--exclusive":
                do_locking = True
            elif opt == "--verbose":
                verbose = True

        if dryrun and (len(remainder) == 0):
            usage("--dryrun requires the name of a wikidb to be specified")
        if job_requested and force_lock:
            usage("--force cannot be used with --job option")
        if restart and not job_requested:
            usage("--restartfrom requires --job and the job from which to restart")
        if chunk_to_do and not job_requested:
            usage("--chunk option requires a specific job for which to rerun that chunk")
        if chunk_to_do and restart:
            usage("--chunk option can be specified only for one specific job")
        if checkpoint_file and (len(remainder) == 0):
            usage("--checkpoint option requires the name of a wikidb to be specified")
        if checkpoint_file and not job_requested:
            usage("--checkpoint option requires --job and the job from which to restart")
        if page_id_range and not job_requested:
            usage("--pageidrange option requires --job and the job from which to restart")
        if page_id_range and checkpoint_file:
            usage("--pageidrange option cannot be used with --checkpoint option")

        if skip_jobs is None:
            skip_jobs = []
        else:
            skip_jobs = skip_jobs.split(",")

        # allow alternate config file
        if config_file:
            config = Config(config_file)
        else:
            config = Config()
        externals = [
            'php', 'mysql', 'mysqldump', 'head', 'tail',
            'checkforbz2footer', 'grep', 'gzip', 'bzip2',
            'writeuptopageid', 'recompressxml', 'sevenzip', 'cat',]

        failed = False
        unknowns = []
        notfound = []
        for external in externals:
            try:
                ext = getattr(config, external)
            except AttributeError:
                unknowns.append(external)
                failed = True
            else:
                if not exists(ext):
                    notfound.append(ext)
                    failed = True
        if failed:
            if unknowns:
                sys.stderr.write("Unknown config param(s): %s\n" % ", ".join(unknowns))
            if notfound:
                sys.stderr.write("Command(s) not found: %s\n" % ", ".join(notfound))
            sys.stderr.write("Exiting.\n")
            sys.exit(1)

        if dryrun or chunk_to_do or (job_requested and not restart  and not do_locking):
            locks_enabled = False
        else:
            locks_enabled = True

        if dryrun:
            print "***"
            print "Dry run only, no files will be updated."
            print "***"

        if len(remainder) > 0:
            wiki = Wiki(config, remainder[0])
            if cutoff:
                # fixme if we asked for a specific job then check that job only
                # not the dir
                last_ran = wiki.latestDump()
                if last_ran >= cutoff:
                    wiki = None
            if wiki is not None and locks_enabled:
                if force_lock and wiki.isLocked():
                    wiki.unlock()
                if locks_enabled:
                    wiki.lock()

        else:
            # if the run is across all wikis and we are just doing one job,
            # we want the age of the wikis by the latest status update
            # and not the date the run started
            if job_requested:
                check_status_time = True
            else:
                check_status_time = False
            if skipdone:
                check_job_status = True
            else:
                check_job_status = False
            if job_requested and skipdone:
                check_prereq_status = True
            else:
                check_prereq_status = False
            wiki = find_lock_next_wiki(config, locks_enabled, cutoff, check_status_time,
                                       check_job_status, check_prereq_status,
                                       date, job_requested, skip_jobs, page_id_range, chunk_to_do,
                                       checkpoint_file, restart)

        if wiki is not None and wiki:
            # process any per-project configuration options
            config.parseConfFilePerProject(wiki.dbName)

            if date == 'last':
                dumps = sorted(wiki.dumpDirs())
                if dumps:
                    date = dumps[-1]
                else:
                    date = None

            if date is None or not date:
                date = TimeUtils.today()
            wiki.setDate(date)

            if after_checkpoint:
                fname = DumpFilename(wiki)
                fname.new_from_filename(checkpoint_file)
                if not fname.is_checkpoint_file:
                    usage("--aftercheckpoint option requires the name of a checkpoint file, bad filename provided")
                page_id_range = str(int(fname.last_page_id) + 1)
                chunk_to_do = fname.chunk_int
                # now we don't need this.
                checkpoint_file = None
                after_checkpoint_jobs = ['articlesdump', 'metacurrentdump', 'metahistorybz2dump']
                if not job_requested or not job_requested in ['articlesdump', 'metacurrentdump', 'metahistorybz2dump']:
                    usage("--aftercheckpoint option requires --job option with one of %s" % ", ".join(after_checkpoint_jobs))

            runner = Runner(wiki, prefetch, spawn, job_requested, skip_jobs, restart, html_notice, dryrun, enable_logging, chunk_to_do, checkpoint_file, page_id_range, skipdone, verbose)

            if restart:
                sys.stderr.write("Running %s, restarting from job %s...\n" % (wiki.dbName, job_requested))
            elif job_requested:
                sys.stderr.write("Running %s, job %s...\n" % (wiki.dbName, job_requested))
            else:
                sys.stderr.write("Running %s...\n" % wiki.dbName)
            result = runner.run()
            if result is not None and result:
                exitcode = 0
            # if we are doing one piece only of the dump, we don't unlock either
            if locks_enabled:
                wiki.unlock()
        elif wiki is not None:
            sys.stderr.write("Wikis available to run but prereqs not complete.\n")
            exitcode = 0
        else:
            sys.stderr.write("No wikis available to run.\n")
            exitcode = 255
    finally:
        cleanup()
    sys.exit(exitcode)

if __name__ == "__main__":
    main()
