#!/usr/bin/python3
'''
All xml content dump jobs are defined here
'''

import os
from os.path import exists
import time

from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename, DumpContents, FileUtils, PARTS_ANY
from dumps.utils import MultiVersion
from dumps.jobs import Dump
from dumps.wikidump import Locker
import dumps.pagerange
from dumps.pagerange import PageRange, QueryRunner
from dumps.pagerangeinfo import PageRangeInfo
from dumps.prefetch import PrefetchFinder
import dumps.intervals
from dumps.stubprovider import StubProvider
from dumps.outfilelister import OutputFileLister


class DFNamePageRangeConverter():
    '''
    make dfnames from page ranges, get page ranges from dfnames, etc.
    '''
    def __init__(self, wiki, dumpname, filetype, file_ext, verbose):
        self.wiki = wiki
        self.dumpname = dumpname
        self.filetype = filetype
        self.file_ext = file_ext
        self.verbose = verbose

    @staticmethod
    def get_pageranges_from_dfnames(dfnames):
        """
        given a list of dfnames, return a list of tuples of
        (startpageid, endpageid, partnum)
        if there are not start and end page ids in each filename,
        this will return None
        """
        pageranges = []
        for dfname in dfnames:
            if not dfname.first_page_id or not dfname.last_page_id:
                return None
            pageranges.append((dfname.first_page_id, dfname.last_page_id, dfname.partnum))
        return pageranges

    def get_pagerange_output_dfname(self, jobinfo):
        """
        given page range passed in,
        return a dumpfilename for the appropriate checkpoint file
        """
        if ',' in jobinfo['pageid_range']:
            first_page_id, last_page_id = jobinfo['pageid_range'].split(',', 1)
        else:
            first_page_id = jobinfo['pageid_range']
            # really? ewww gross
            last_page_id = "00000"  # indicates no last page id specified, go to end of stub
        if jobinfo['partnum_todo']:
            partnum = jobinfo['partnum_todo']
        else:
            partnum = None
        return self.make_dfname_from_pagerange([first_page_id, last_page_id], partnum)

    def make_dfname_from_pagerange(self, pagerange, partnum):
        """
        given pagerange, make output file for appropriate type
        of page content dumps
        args: (startpage, endpage), string
        """
        checkpoint_string = DumpFilename.make_checkpoint_string(
            str(pagerange[0]), str(pagerange[1]))
        output_dfname = DumpFilename(self.wiki, self.wiki.date, self.dumpname,
                                     self.filetype, self.file_ext,
                                     str(partnum), checkpoint=checkpoint_string,
                                     temp=False)
        return output_dfname

    def get_dfnames_from_pageranges(self, pageranges):
        """
        given a list of tuples of (startpageid, endpageid, partnum),
        return a list of corresponding dfnames
        """
        dfnames = []
        for startpage, endpage, partnum in pageranges:
            dfname = DumpFilename(
                self.wiki, self.wiki.date, self.dumpname,
                self.filetype, self.file_ext, partnum,
                DumpFilename.make_checkpoint_string(startpage, endpage),
                False)
            dfnames.append(dfname)
        return dfnames

    def get_pagerange_jobs_for_file(self, partnum, page_start, page_end, jobinfo,
                                    revinfo_path=None):
        """
        given an output filename, the start and end pages it should cover,
        split up into output filenames that will each contain roughly the same
        number of revisions, so that each dump to produce them doesn't
        take a ridiculous length of time

        args: DumpFilename, startpage<str>, endpage<str>
        returns: list of DumpFilename
        """
        output_dfnames = []
        if 'history' in jobinfo['subset']:
            prange = PageRange(QueryRunner(self.wiki.db_name, self.wiki.config,
                                           self.verbose), self.verbose)
            ranges = prange.get_pageranges_for_revs(page_start, page_end,
                                                    self.wiki.config.revs_per_job,
                                                    self.wiki.config.maxrevbytes,
                                                    revinfo_path, 5)
        else:
            # strictly speaking this splits up the pages-articles
            # dump more than is needed but who cares
            ranges = [(n, min(n + self.wiki.config.revs_per_job - 1, page_end))
                      for n in range(page_start, page_end,
                                     self.wiki.config.revs_per_job)]
        for pagerange in ranges:
            dfname = self.make_dfname_from_pagerange(pagerange, partnum)
            if dfname is not None:
                output_dfnames.append(dfname)
        return output_dfnames


class XmlDump(Dump):
    """Primary XML dumps, one section at a time."""
    def __init__(self, subset, name, desc, detail, item_for_stubs, item_for_stubs_recombine,
                 prefetch, prefetchdate, spawn,
                 wiki, partnum_todo, pages_per_part=None, checkpoints=False, checkpoint_file=None,
                 page_id_range=None, verbose=False):
        self.jobinfo = {'subset': subset, 'detail': detail, 'desc': desc,
                        'prefetch': prefetch, 'prefetchdate': prefetchdate,
                        'spawn': spawn, 'partnum_todo': partnum_todo,
                        'pageid_range': page_id_range, 'item_for_stubs': item_for_stubs}
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self._pages_per_part = pages_per_part
        if self._pages_per_part:
            self._parts_enabled = True
            self.onlyparts = True

        self.wiki = wiki
        self.verbose = verbose
        self._prerequisite_items = [self.jobinfo['item_for_stubs']]
        if item_for_stubs_recombine is not None:
            self._prerequisite_items.append(item_for_stubs_recombine)
        self.stubber = StubProvider(
            self.wiki, {'dumpname': self.get_dumpname(), 'pagesperpart': self._pages_per_part,
                        'dumpnamebase': self.get_dumpname_base(),
                        'item_for_stubs': item_for_stubs,
                        'partnum_todo': self.jobinfo['partnum_todo']},
            self.verbose)
        self.converter = DFNamePageRangeConverter(wiki, self.get_dumpname(), self.get_filetype(),
                                                  self.get_file_ext(), self.verbose)
        Dump.__init__(self, name, desc, self.verbose)
        self.oflister = XmlFileLister(self.dumpname, self.file_type, self.file_ext,
                                      self.get_fileparts_list(), self.checkpoint_file,
                                      self._checkpoints_enabled, self.list_dumpnames,
                                      self.jobinfo['pageid_range'])

    @classmethod
    def check_truncation(cls):
        return True

    @classmethod
    def get_dumpname_base(cls):
        """
        these dumps are all pages-{articles,meta-current,meta-history}
        return the common part of the name
        """
        return 'pages-'

    def get_dumpname(self):
        """
        these dumps are all pages-{articles,meta-current,meta-history}
        return the full dump name
        """
        return self.get_dumpname_base() + self.jobinfo['subset']

    @classmethod
    def get_filetype(cls):
        """
        text, sql, xml?
        """
        return "xml"

    @classmethod
    def get_file_ext(cls):
        """
        gz, bz2, 7z?
        """
        return "bz2"

    def cleanup_tmp_files(self, dump_dir, runner):
        """
        with checkpoint files turned on, this job writes output
        to <something>.xml<-maybemorestuff>.bz2-tmp
        and if those files are lying around after such a job dies,
        we should clean them up
        """
        if "cleanup_tmp_files" not in runner.enabled:
            return

        # if we don't have the lock it's possible some
        # other process is writing tmp files, don't touch
        locker = Locker(self.wiki, self.wiki.date)
        lockfiles = locker.is_locked()
        if not lockfiles:
            return
        if len(lockfiles) > 1:
            # more than one process with the lock? should not
            # be possible, but if it is... touch nothing!
            return
        if not locker.check_owner(lockfiles[0], str(os.getpid())):
            return

        to_delete = self.get_tmp_files(dump_dir)
        for finfo in to_delete:
            if exists(dump_dir.filename_public_path(finfo)):
                os.remove(dump_dir.filename_public_path(finfo))

    def get_done_pageranges(self, dump_dir, date):
        """
        get the current checkpoint files and from them get
        the page ranges that are covered by the files

        returns: sorted
        """
        chkpt_dfnames = self.oflister.list_checkpt_files(
            self.oflister.makeargs(dump_dir, [self.get_dumpname()],
                                   parts=PARTS_ANY, date=date))
        # get the page ranges covered by existing checkpoint files
        done_pageranges = [(dfname.first_page_id_int, dfname.last_page_id_int,
                            dfname.partnum_int)
                           for dfname in chkpt_dfnames]
        done_pageranges = sorted(done_pageranges, key=lambda x: int(x[0]))
        if self.verbose:
            print("done_pageranges:", done_pageranges)
        return done_pageranges

    def get_nochkpt_outputfiles(self, dump_dir):
        """
        get output files that should be produced by this step,
        if no checkpoints were specified.
        if only one part was specified to run, only that file will
        be listed
        returns:
            list of DumpFilename
        """
        return self.oflister.get_reg_files_for_filepart_possible(
            self.oflister.makeargs(dump_dir, self.list_dumpnames(), self.get_fileparts_list()))

    def get_ranges_covered_by_stubs(self, dump_dir):
        """
        get the page ranges covered by stubs
        returns a list of tuples: (startpage<str>, endpage<str>, partnum<str>)
        """
        output_dfnames = self.oflister.get_reg_files_for_filepart_possible(
            self.oflister.makeargs(dump_dir, self.list_dumpnames(), self.get_fileparts_list()))
        stub_dfnames = [self.stubber.get_stub_dfname(dfname.partnum, dump_dir)
                        for dfname in output_dfnames]
        stub_dfnames = sorted(stub_dfnames, key=lambda thing: thing.filename)

        stub_ranges = []
        for stub_dfname in stub_dfnames:
            # why do we do this instead of getting the theoretical page
            # ranges (which are used in page content files anyways, aren't they?)
            # via the wiki config? FIXME

            first_page, last_page = self.stubber.get_first_last_page_ids(
                stub_dfname, dump_dir, self._pages_per_part)
            stub_ranges.append((first_page, last_page, stub_dfname.partnum_int))

        return stub_ranges

    def get_dfnames_for_missing_pranges(self, dump_dir, date, stub_pageranges):
        """
        if there are some page ranges done already for this job,
        return a list of output files covering only the missing
        pages, otherwise return the usual (single output file
        or list of subjob output files)

        returns: list of DumpFilename
        """
        # get list of existing checkpoint files
        done_pageranges = self.get_done_pageranges(dump_dir, date)
        if not done_pageranges:
            # no pages already done, do them all
            return self.get_nochkpt_outputfiles(dump_dir)

        missing_ranges = dumps.intervals.find_missing_ranges(stub_pageranges, done_pageranges)
        todo = []
        parts = self.get_fileparts_list()
        output_dfnames = self.oflister.get_reg_files_for_filepart_possible(
            self.oflister.makeargs(dump_dir, self.list_dumpnames(), parts))
        for partnum in parts:
            if not [1 for chkpt_range in done_pageranges
                    if chkpt_range[2] == partnum]:
                # entire page range for a particular file part
                # is missing so generate the regular output file
                todo.extend([dfname for dfname in output_dfnames
                             if dfname.partnum_int == partnum])
            else:
                # at least some page ranges are covered, just do those that
                # are missing (maybe none are and list is empty)
                todo.extend([self.converter.make_dfname_from_pagerange((first, last), part)
                             for (first, last, part) in missing_ranges
                             if part == partnum])
        return todo

    def make_bitesize_jobs(self, output_dfnames, stub_pageranges):
        """
        for each file in the list, generate a list of page ranges
        such that we can dump page content files for those page ranges
        covering the output requested, all having around the same
        number of revisions in them, (example: split up
        dewiki page meta history part 1 into a bunch of
        1 million rev pieces for output)

        if we have been requested to produce a specific pagerange already,
        this routine should not be used.

        args: list of DumpFilename, list of (startpage, endpage, partnum)
        """
        revinfo_path = self.get_revinfofile_path()
        to_return = []
        for dfname in output_dfnames:
            if not dfname.is_checkpoint_file:
                pageranges = dumps.intervals.get_intervals_by_group(
                    dfname.partnum_int, stub_pageranges)
                # we get all the ranges for the whole part
                for prange in pageranges:
                    to_return.extend(self.converter.get_pagerange_jobs_for_file(
                        dfname.partnum_int, prange[0], prange[1], self.jobinfo, revinfo_path))
            else:
                # we get just the one range
                to_return.extend(self.converter.get_pagerange_jobs_for_file(
                    dfname.partnum_int, dfname.first_page_id_int, dfname.last_page_id_int,
                    self.jobinfo, revinfo_path))
        return to_return

    def setup_wanted(self, dfname, runner, prefetcher):
        """
        gather and return info about all comands we want to run,
        including input stubs, input prefetchs, output filenames, etc

        args: DumpFilename, Runner, PrefetchFinder
        """
        wanted = {}
        wanted['outfile'] = dfname
        wanted['pagerange'] = (dfname.first_page_id, dfname.last_page_id)
        wanted['partnum'] = dfname.partnum
        wanted['stub_input'] = self.stubber.get_stub_dfname(wanted['partnum'], runner.dump_dir)
        if wanted['pagerange'] and wanted['outfile'].first_page_id is not None:
            wanted['stub'] = self.stubber.get_pagerange_stub_dfname(wanted, runner.dump_dir)
            # generate a stub to cover the page range
            wanted['generate'] = True
        else:
            # use existing stub
            wanted['stub'] = wanted['stub_input']
            wanted['generate'] = False

        if self.jobinfo['prefetch']:
            wanted['prefetch'] = prefetcher.get_prefetch_arg(
                runner, wanted['outfile'], wanted['stub'])
        else:
            wanted['prefetch'] = ""
        return wanted

    def get_revinfo_dfname(self):
        '''
        get the dfname for the revinfo file for this wiki and date
        '''
        return DumpFilename(self.wiki, self.wiki.date, "revinfo", filetype=None,
                            ext="gz", partnum=None, checkpoint=None)

    def get_revinfofile_path(self):
        '''
        return the path to the revinfo file for this wiki and dump run
        the file should contain information about the number and length of revisions
        for each page or batch of pages
        '''
        dfname = self.get_revinfo_dfname()
        return os.path.join(
            FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir, True),
            dfname.filename)

    def get_temp_revinfofile_path(self):
        '''
        return the path to the temp revinfo file for this wiki and dump run
        the file as it is generated should be written here, and only moved into
        its proper name once output has been verified
        '''
        dfname = self.get_revinfo_dfname()
        dfname_tmp = DumpFilename(dfname.wiki, dfname.date, dfname.dumpname, dfname.file_type,
                                  dfname.file_ext, dfname.partnum, dfname.checkpoint, True)

        return os.path.join(
            FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir, True),
            dfname_tmp.filename)

    def stash_revinfo(self, runner, batchsize=10):
        '''
        get information from the latest stubs history file about the number and length
        of revisions for each batch of ten pages and save the output

        this is only generated when full page content history is being produced
        '''
        if not runner.wiki.config.revinfostash:
            return
        if 'history' not in self.jobinfo['subset']:
            return

        revinfo_path = self.get_revinfofile_path()
        if os.path.exists(revinfo_path):
            return
        revinfo_path_tmp = self.get_temp_revinfofile_path()
        stubs_filename = self.stubber.get_stub_dfname_no_parts(runner.dump_dir)

        stubs_path = os.path.join(self.wiki.public_dir(), self.wiki.date, stubs_filename.filename)
        if not os.path.exists(stubs_path):
            raise BackupError("no stub input available to generate rev info")

        commands = [[self.wiki.config.gzip, '-dc', stubs_path],
                    [self.wiki.config.revsperpage, '-B', str(batchsize), '-a', '-c', '-b'],
                    [self.wiki.config.gzip]]
        command_series = runner.get_save_command_series(commands, revinfo_path_tmp)
        retries = 0
        maxretries = 3
        error, _broken = runner.save_command(command_series)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(15)
            error, _broken = runner.save_command(command_series)
        if error:
            raise BackupError("error generating revision count info")
        # check that the file is ok
        dcontents = DumpContents(runner.wiki, revinfo_path_tmp)
        if (dcontents.check_if_empty() or dcontents.check_if_truncated() or
                dcontents.check_if_binary_crap()):
            os.unlink(revinfo_path_tmp)
            raise BackupError("error in writing revision count info file")
        os.rename(revinfo_path_tmp, revinfo_path)

    def get_dfnames_from_cached_pageranges(self, stub_pageranges, dfnames_todo, logger, runner):
        """
        if there is a pagerangeinfo file, get a list of page ranges suitable
        for small jobs (i.e. they don't take forever) from the file;
        if not, generate them the hard way by db queries and write them
        into a new pagerangeinfo file.
        from these page ranges, generate a list of corresponding output
        DumpFilenames for page content dumps and return them
        """
        # get previous page range tuples from file, if this is a rerun
        pr_info = PageRangeInfo(self.wiki, True, "json", logger,
                                self.verbose)
        bitesize_pageranges = None
        bitesize_pageranges_all = pr_info.get_pagerange_info(self.wiki)
        if (bitesize_pageranges_all and self.jobinfo['subset'] in bitesize_pageranges_all and
                bitesize_pageranges_all[self.jobinfo['subset']]):
            # get pagerangeinfo tuples that are covered by the stub ranges (i.e.
            # pages we are missing)
            bitesize_pageranges = dumps.intervals.get_covered_ranges(
                dumps.intervals.convert_intervals_to_ints(
                    bitesize_pageranges_all[self.jobinfo['subset']]),
                stub_pageranges)
        if bitesize_pageranges:
            dfnames_todo = self.converter.get_dfnames_from_pageranges(
                bitesize_pageranges)
        else:
            self.stash_revinfo(runner)
            dfnames_todo = self.make_bitesize_jobs(dfnames_todo, stub_pageranges)
            bitesize_pageranges = self.converter.get_pageranges_from_dfnames(dfnames_todo)
            pr_info.update_pagerangeinfo(self.wiki, self.jobinfo['subset'], bitesize_pageranges)
        return dfnames_todo

    def get_todos_for_checkpoints(self, dump_dir, date):
        """
        for a wiki with checkpoints (dump by page ranges) enabled,
        generate a list of output DumpFilenames for missing content
        from this job, and a list of page ranges for the corresponding
        stubs files we will want; return them both
        """
        stub_pageranges = self.get_ranges_covered_by_stubs(dump_dir)
        stub_pageranges = sorted(stub_pageranges, key=lambda x: x[0])
        dfnames_todo = self.get_dfnames_for_missing_pranges(dump_dir, date, stub_pageranges)
        # replace stub ranges for output files that cover smaller
        # ranges, with just those numbers
        new_stub_ranges = []
        for dfname in dfnames_todo:
            if dfname.is_checkpoint_file:
                new_stub_ranges.append((dfname.first_page_id_int,
                                        dfname.last_page_id_int, dfname.partnum_int))
            else:
                for srange in stub_pageranges:
                    if srange[2] == dfname.partnum_int:
                        new_stub_ranges.append(srange)
        return new_stub_ranges, dfnames_todo

    def get_todos_no_checkpoints(self, dump_dir):
        """
        get and return a list of output DumpFilenames corresponding
        to page content we want to dump, when checkpoint files
        (page ranges) are not enabled
        """
        output_dfnames = self.oflister.get_reg_files_for_filepart_possible(
            self.oflister.makeargs(dump_dir, self.list_dumpnames(), self.get_fileparts_list()))
        # at least some page ranges are covered, just do those that aren't
        dfnames_todo = [
            dfname for dfname in output_dfnames if not os.path.exists(
                dump_dir.filename_public_path(dfname))]
        return dfnames_todo

    def get_wanted(self, dfnames_todo, runner, prefetcher):
        """
        collect some info about the command for each output file we want to run,
        including input stubs, input prefetch files, output files, and so on;
        """
        return [self.setup_wanted(dfname, runner, prefetcher) for dfname in dfnames_todo]

    @staticmethod
    def get_to_generate_for_temp_stubs(wanted):
        """
        return info about input and output files we want to generate for temp stubs
        """
        to_generate = []
        for entry in wanted:
            if entry['generate']:
                to_generate.append((entry['stub_input'], entry['stub']))
        return to_generate

    def get_batchsize(self, stubs=False):
        """
        figure out how many commands we run at once for generating
        temp stubs
        """
        if self._pages_per_part:
            if stubs:
                # these jobs are more expensive than e.g. page content jobs,
                # do half as many
                batchsize = int(len(self._pages_per_part) / 2)
            else:
                batchsize = len(self._pages_per_part)
        else:
            batchsize = 1
        return batchsize

    def get_commands_for_pagecontent(self, wanted, runner):
        """
        get commands to generate page content files for the specific files wanted
        this also updates the 'commands_submitted' attribute which we will need
        later when checking command completion, moving around output files and
        so on.
        """
        commands = []

        for entry in wanted:
            output_dfname = DumpFilename(self.wiki, entry['stub'].date, self.get_dumpname(),
                                         self.get_filetype(), self.file_ext, entry['stub'].partnum,
                                         DumpFilename.make_checkpoint_string(
                                             entry['stub'].first_page_id,
                                             entry['stub'].last_page_id),
                                         False)
            entry['command'] = self.build_command(runner, entry['stub'],
                                                  entry['prefetch'], output_dfname)
            self.setup_command_info(runner, entry['command'], [output_dfname])
            commands.append(entry['command'])
        return commands

    def run_batch(self, command_batch, runner):
        """
        run one batch of commands, returning all command series that failed;
        this logs and/or displays error messages to the console on failure
        """
        error, broken = runner.run_command(
            command_batch, callback_stderr=self.progress_callback,
            callback_stderr_arg=runner,
            callback_on_completion=self.command_completion_callback)
        if error:
            for series in broken:
                for pipeline in series:
                    runner.log_and_print("error from commands: %s" % " ".join(pipeline))
        return broken

    def get_prefetcher(self, wiki):
        """
        return an appropriate object to manage prefetch args etc
        or None if we are not supposed to do prefetching
        """
        if self.jobinfo['prefetch']:
            if wiki.config.sevenzip_prefetch:
                file_exts = ['7z', self.file_ext]
            else:
                file_exts = [self.file_ext]
            prefetcher = PrefetchFinder(
                self.wiki,
                {'name': self.name(), 'desc': self.jobinfo['desc'],
                 'dumpname': self.get_dumpname(),
                 'ftype': self.file_type, 'fexts': file_exts,
                 'subset': self.jobinfo['subset']},
                {'date': self.jobinfo['prefetchdate'], 'pagesperpart': self._pages_per_part},
                self.verbose)
        else:
            prefetcher = None
        return prefetcher

    def get_content_dfnames_todo(self, runner):
        """
        depending on whether this wiki is configured to run jobs
        in parallel and possibly also generate output files
        for page ranges in each job, return a list of output
        files to produce, listing if applicable each paralell job
        output file by part number, with any page range included
        in the filename
        returns list of DumpFilenames
        """
        # in cases where we have request of specific file, do it as asked,
        # no splitting it up into smaller pieces
        do_bitesize = False

        dfnames_todo = []
        if self.jobinfo['pageid_range'] is not None:
            # convert to checkpoint filename, handle the same way
            dfnames_todo = [self.converter.get_pagerange_output_dfname(self.jobinfo)]
        elif self.checkpoint_file:
            dfnames_todo = [self.checkpoint_file]
        elif self._checkpoints_enabled:
            do_bitesize = True
            stub_pageranges, dfnames_todo = self.get_todos_for_checkpoints(
                runner.dump_dir, runner.wiki.date)
        else:
            dfnames_todo = self.get_todos_no_checkpoints(runner.dump_dir)

        if self._checkpoints_enabled and do_bitesize:
            dfnames_todo = self.get_dfnames_from_cached_pageranges(
                stub_pageranges, dfnames_todo, runner.log_and_print, runner)
        return dfnames_todo

    def get_final_output_dfname(self, command_series, runner):
        """given a command series that produces one output file,
        return the dfname for the output file as given in the appropriate
        command_info element in self.commands_submitted, and without any
        INPROG marker etc. Returns None if none found"""
        for command_info in self.commands_submitted:
            if command_info['series'] == command_series:
                filenames = command_info['output_files']
        if len(filenames) != 1:
            return None
        # turn the one file into a dfname without INPROG marker and return it
        filename = filenames[0]
        if filename.endswith(DumpFilename.INPROG):
            filename = filename[:-1 * len(DumpFilename.INPROG)]
        dfname = DumpFilename(runner.wiki)
        dfname.new_from_filename(filename)
        return dfname

    def get_command_batch(self, commands, runner):
        '''
        return a batch of commands, filtered so that any which
        produce an output file that already exists, are omitted;
        this prevents us from interfering with runs on another
        host or manual runs that we may not know about
        '''
        commands = self.filter_commands(commands, runner)
        batchsize = self.get_batchsize()
        commands_todo = commands[:batchsize]
        commands_left = commands[batchsize:]
        return (commands_todo, commands_left)

    def filter_commands(self, commands, runner):
        '''
        remove any commands from the list that produce an output file
        which already exists
        '''
        commands_filtered = []
        for command_series in commands:
            # each series produces one output file only, and we want the name without INPROG markers
            final_output_dfname = self.get_final_output_dfname(command_series, runner)
            # if the file is already there, move on, don't rerun.
            if final_output_dfname is None or not exists(
                    os.path.join(runner.dump_dir.filename_public_path(final_output_dfname))):
                commands_filtered.append(command_series)
        return commands_filtered

    def run_page_content_commands(self, commands, runner):
        """
        generate page content output in batches, with retries if configured
        """
        # don't do them all at once, do only up to _parts commands at the same time
        batchsize = self.get_batchsize()
        errors = False
        failed_commands = []
        max_retries = self.wiki.config.max_retries
        retries = 0
        commands_left = commands
        while commands_left and (retries < max_retries or retries == 0):
            commands_todo, commands_left = self.get_command_batch(commands_left, runner)
            broken = self.run_batch(commands_todo, runner)
            if broken:
                failed_commands.append(broken)
                errors = True

            if not commands_todo and failed_commands:
                retries += 1
                if retries < max_retries:
                    # retry failed commands
                    commands_todo = failed_commands
                    failed_commands = []
                    # no instant retries, give the servers a break
                    time.sleep(self.wiki.config.retry_wait)
                    errors = False
        if errors:
            raise BackupError("error producing xml file(s) %s" % self.get_dumpname())

    def run(self, runner):
        """
        do all phases of the page content job, starting with cleanup,
        possibly generating temporary stub files to cover missing
        page ranges if we are filling in files not generated from a
        previous run of this job, and running the commands to
        generate the page content files in batches
        """
        # here we will either clean up or not depending on how we were called
        # FIXME callers should set this appropriately and they don't right now
        self.cleanup_old_files(runner.dump_dir, runner)

        # clean up all tmp output files from previous attempts of this job
        # for this dump wiki and date; they may have been left around from
        # an interrupted or failed earlier run
        self.cleanup_tmp_files(runner.dump_dir, runner)

        # get the names of the output files we want to produce
        dfnames_todo = self.get_content_dfnames_todo(runner)

        # set up a prefetch arg generator if needed
        prefetcher = self.get_prefetcher(runner.wiki)

        # accumulate all the info about stub inputs, page content inputs
        # for prefetches, output files and so on
        wanted = self.get_wanted(dfnames_todo, runner, prefetcher)

        # figure out what temp stub files we need to write, if we
        # are producing output files covering page ranges (each
        # output file will cover the same content as its stub input
        # file)
        to_generate = self.get_to_generate_for_temp_stubs(wanted)

        # figure out how many stub input files we generate at once
        batchsize = self.get_batchsize(stubs=True)

        commands, output_dfnames = self.stubber.get_commands_for_temp_stubs(to_generate, runner)
        self.stubber.run_temp_stub_commands(runner, commands, batchsize)

        # check that the temp stubs are not garbage, though they may be empty so
        # we should (but don't yet) skip that check. FIXME
        self.stubber.check_temp_stubs(runner, self.move_if_truncated, output_dfnames)

        # if we had to generate temp stubs, skip over those with no pages in them
        # it's possible a page range has nothing in the stub file because they were all deleted.
        # we have some projects with e.g. 35k pages in a row deleted!
        todo = [entry for entry in wanted if not entry['generate'] or
                not self.stubber.has_no_pages(entry['stub'], runner, tempdir=True)]

        commands = self.get_commands_for_pagecontent(todo, runner)

        self.run_page_content_commands(commands, runner)

    @classmethod
    def build_eta(cls):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--current"

    # takes name of the output file
    def build_filters(self, runner, input_dfname):
        """
        Construct the output filter options for dumpTextPass.php
        args:
            Runner, DumpFilename
        """
        # do we need checkpoints? ummm
        xmlbz2_path = runner.dump_dir.filename_public_path(input_dfname)

        if 'history' in self.jobinfo['subset'] and runner.wiki.config.lbzip2forhistory:
            # we will use lbzip2 for compression of pages-meta-history for this wiki
            # if configured
            bz2mode = "lbzip2"
            if not exists(self.wiki.config.lbzip2):
                raise BackupError("lbzip2 command %s not found" % self.wiki.config.lbzip2)
        elif self.wiki.config.bzip2[-6:] == "dbzip2":
            bz2mode = "dbzip2"
        else:
            bz2mode = "bzip2"
            if not exists(self.wiki.config.bzip2):
                raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        return "--output=%s:%s" % (bz2mode, DumpFilename.get_inprogress_name(xmlbz2_path))

    def build_command(self, runner, stub_dfname, prefetch, output_dfname):
        """
        Build the command line for the dump, minus output and filter options
        args:
            Runner, stub DumpFilename, ....
        """
        stub_path = os.path.join(
            FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
            stub_dfname.filename)
        if os.path.exists(stub_path):
            # if this is a pagerange stub file in temp dir, use that
            stub_option = "--stub=gzip:%s" % stub_path
        else:
            # use regular stub file
            stub_option = "--stub=gzip:%s" % runner.dump_dir.filename_public_path(stub_dfname)
        if self.jobinfo['spawn']:
            spawn = "--spawn=%s" % (self.wiki.config.php)
        else:
            spawn = ""

        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)

        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpTextPass.php")
        dump_command = [self.wiki.config.php]
        dump_command.extend(script_command)
        dump_command.extend(["--wiki=%s" % runner.db_name,
                             "%s" % stub_option,
                             "%s" % prefetch,
                             "--report=1000",
                             "%s" % spawn])

        dump_command = [entry for entry in dump_command if entry is not None]
        dump_command.extend([self.build_filters(runner, output_dfname), self.build_eta()])
        pipeline = [dump_command]
        # return a command series of one pipeline
        series = [pipeline]
        return series

    def get_tmp_files(self, dump_dir, dump_names=None):
        """
        list temporary output files currently existing
        returns:
            list of DumpFilename
        """
        dfnames = self.oflister.list_outfiles_for_cleanup(
            self.oflister.makeargs(dump_dir, dump_names))
        return [dfname for dfname in dfnames if dfname.is_temp_file]


class XmlFileLister(OutputFileLister):
    """
    special output file list methods for xml page content dump jobs

    we must account for retries of just a single page content
    file, perhaps with a specific page range, if asked to clean up
    existing files before the retry.
    """
    def __init__(self, dumpname, file_type, file_ext, fileparts_list,
                 checkpoint_file, checkpoints_enabled, list_dumpnames=None,
                 pageid_range=None):
        super().__init__(dumpname, file_type, file_ext, fileparts_list,
                         checkpoint_file, checkpoints_enabled, list_dumpnames)
        self.pageid_range = pageid_range

    def list_outfiles_for_cleanup(self, args):
        """
        list output files including checkpoint files currently existing
        (from the dump run for the current wiki and date), in case
        we have been requested to clean up before a retry

        expects: args.dump_dir, optional args.dump_names
        returns: list of DumpFilename
        """
        dfnames = super().list_outfiles_for_cleanup(args)
        dfnames_to_return = []

        if self.pageid_range:
            # this file is for one page range only
            if ',' in self.pageid_range:
                (first_page_id, last_page_id) = self.pageid_range.split(',', 2)
                first_page_id = int(first_page_id)
                last_page_id = int(last_page_id)
            else:
                first_page_id = int(self.pageid_range)
                last_page_id = None

            # checkpoint files cover specific page ranges. for those,
            # list only files within the given page range for cleanup
            for dfname in dfnames:
                if dfname.is_checkpoint_file:
                    if (not first_page_id or
                            (dfname.first_page_id and
                             (int(dfname.first_page_id) >= first_page_id))):
                        if (not last_page_id or
                                (dfname.last_page_id and
                                 (int(dfname.last_page_id) <= last_page_id))):
                            dfnames_to_return.append(dfname)
                else:
                    dfnames_to_return.append(dfname)
        else:
            dfnames_to_return = dfnames

        return dfnames_to_return


class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    @classmethod
    def build_eta(cls):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"
