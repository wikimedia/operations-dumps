#!/usr/bin/python3
'''
All xml content dump jobs are defined here
'''

import os
from os.path import exists
import functools
import time
import signal

from dumps.exceptions import BackupError
from dumps.fileutils import DumpContents, DumpFilename, FileUtils
from dumps.utils import MultiVersion
from dumps.jobs import Dump
from dumps.wikidump import Locker
import dumps.pagerange
from dumps.pagerange import PageRange, QueryRunner
from dumps.pagerangeinfo import PageRangeInfo
from dumps.prefetch import PrefetchFinder
import dumps.intervals


class StubProvider():
    """
    make or find stub files for use in page content dump
    """
    def __init__(self, wiki, jobinfo, verbose):
        self.wiki = wiki
        self.jobinfo = jobinfo
        self.verbose = verbose

    def get_stub_dfname(self, partnum, dump_dir):
        '''
        get the stub file pertaining to our dumpname
        (one of articles, pages-current, pages-history)
        and our desired subjob or page range etc.
        we will either return the one full stubs file that exists
        or the one stub file part, if we are (re)running a specific
        file part (subjob), or all file parts if we are (re)running
        the entire job which is configured for subjobs.

        arguments:
           partnum   - subjob number if any,
           runner    - Runner
        returns:
           DumpFilename
        '''
        if not self.jobinfo['dumpname'].startswith(self.jobinfo['dumpnamebase']):
            raise BackupError("dumpname %s of unknown form for this job" % self.jobinfo['dumpname'])

        dumpname = self.jobinfo['dumpname'][len(self.jobinfo['dumpnamebase']):]
        stub_dumpnames = self.jobinfo['item_for_stubs'].list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_dfnames = self.jobinfo['item_for_stubs'].list_outfiles_for_input(
            dump_dir, [stub_dumpname])
        if partnum is not None:
            input_dfnames = [dfname for dfname in input_dfnames
                             if dfname.partnum_int == int(partnum)]
        if len(input_dfnames) > 1:
            # this is an error
            return None
        return input_dfnames[0]

    def write_pagerange_stubs(self, iofile_pairs, runner, batchsize, move_if_truncated):
        """
        put the io file pairs in ascending order (per part if there
        are parts), for each pair write out a stub file corresponding
        to the page range in the output filename, combining up
        those outputs that require the same input file into
        one command

        args: pairs of (DumpFilename, DumpFilename), Runner
        """
        if not iofile_pairs:
            return

        # split up into batches where the input file is the same
        # and the pairs are ordered by output file name
        in_dfnames = [pair[0] for pair in iofile_pairs]
        # get just the unique ones
        in_dfnames_uniq = []
        for in_dfname in in_dfnames:
            if in_dfname.filename not in [dfname.filename for dfname in in_dfnames_uniq]:
                in_dfnames_uniq.append(in_dfname)

        out_dfnames = {}
        output_dfnames_to_check = []
        for in_dfname in in_dfnames_uniq:
            out_dfnames[in_dfname.filename] = sorted([pair[1] for pair in iofile_pairs
                                                      if pair[0].filename == in_dfname.filename],
                                                     key=functools.cmp_to_key(DumpFilename.compare))
        commands = []
        for in_dfname in in_dfnames_uniq:
            pipeline = self.get_stub_gen_cmd_for_input(
                in_dfname, out_dfnames[in_dfname.filename], runner)
            if pipeline is not None:
                # list of command series. each series is a list of pipelines.
                commands.append([pipeline])
                output_dfnames_to_check.extend(out_dfnames[in_dfname.filename])

        errors = False
        while commands:
            command_batch = commands[:batchsize]
            error, broken = runner.run_command(command_batch)
            if error:
                for series in broken:
                    for pipeline in series:
                        failed_cmds_retcodes = pipeline.get_failed_cmds_with_retcode()
                        for cmd_retcode in failed_cmds_retcodes:
                            if (cmd_retcode[1] == -signal.SIGPIPE or
                                    cmd_retcode[1] == signal.SIGPIPE + 128):
                                pass
                            else:
                                runner.log_and_print("error from commands: %s" % " ".join(
                                    [entry for entry in pipeline]))
                                errors = True
            commands = commands[batchsize:]
        if errors:
            raise BackupError("failed to write pagerange stub files")

        if runner.dryrun:
            return

        # check the output files to see if we like them;
        # if not, we will move the bad ones out of the way and
        # whine about them
        bad_dfnames = []
        output_dir = FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir)
        for temp_stub_dfname in output_dfnames_to_check:
            if os.path.exists(os.path.join(output_dir, temp_stub_dfname.filename)):
                bad = move_if_truncated(runner, temp_stub_dfname, emptycheck=200, tmpdir=True)
                if bad:
                    bad_dfnames.append(temp_stub_dfname)
        if bad_dfnames:
            error_string = " ".join([bad_dfname.filename for bad_dfname in bad_dfnames])
            raise BackupError(
                "failed to write pagerange stub files (bad contents) " + error_string)

    def get_stub_gen_cmd_for_input(self, input_dfname, output_dfnames, runner):
        """
        for the given input dumpfile (stub), write the requested output file (stub)
        """
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" %
                              self.wiki.config.writeuptopageid)

        inputfile_path = runner.dump_dir.filename_public_path(input_dfname)

        output_dir = FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir)
        argstrings = []

        for output_dfname in output_dfnames:
            output_fname = output_dfname.filename
            # don't generate the file if we already have it (i.e. this is a retry)
            if not os.path.exists(os.path.join(output_dir, output_fname)):
                first_age_id = output_dfname.first_page_id
                if (output_dfname.last_page_id is not None and
                        output_dfname.last_page_id != "00000"):
                    last_page_id = str(int(output_dfname.last_page_id) + 1)
                else:
                    last_page_id = ""
                argstrings.append("{outfile}:{firstpage}:{lastpage}".format(
                    outfile=output_fname, firstpage=first_age_id, lastpage=last_page_id))

        # don't generate an output file if there are no filespecs
        if not argstrings:
            return None

        if input_dfname.file_ext == "gz":
            # command1 = "%s -dc %s" % (self.wiki.config.gzip, inputfile_path)
            command1 = [self.wiki.config.gzip, "-dc", inputfile_path]
        elif input_dfname.file_ext == '7z':
            # command1 = "%s e -si %s" % (self.wiki.config.sevenzip, inputfile_path)
            command1 = [self.wiki.config.sevenzip, "e", "-si", inputfile_path]
        elif input_dfname.file_ext == 'bz':
            # command1 = "%s -dc %s" % (self.wiki.config.bzip2, inputfile_path)
            command1 = [self.wiki.config.bzip2, "-dc", inputfile_path]
        else:
            raise BackupError("unknown stub file extension %s" % input_dfname.file_ext)

        command2 = [self.wiki.config.writeuptopageid, "--odir", output_dir,
                    "--fspecs", ";".join(argstrings)]
        pipeline = [command1]
        pipeline.append(command2)
        return pipeline

    def get_pagerange_stub_dfname(self, wanted, dump_dir):
        """
        return the dumpfilename for stub file that would have
        the page range in 'wanted'
        """
        stub_input_dfname = self.get_stub_dfname(wanted['partnum'], dump_dir)
        stub_output_dfname = DumpFilename(
            self.wiki, stub_input_dfname.date, stub_input_dfname.dumpname,
            stub_input_dfname.file_type,
            stub_input_dfname.file_ext,
            stub_input_dfname.partnum,
            DumpFilename.make_checkpoint_string(
                wanted['outfile'].first_page_id, wanted['outfile'].last_page_id), temp=False)
        return stub_output_dfname

    def has_no_pages(self, xmlfile, runner, tempdir=False):
        '''
        see if it has a page id in it or not. no? then return True
        '''
        if xmlfile.is_temp_file or tempdir:
            path = os.path.join(
                FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
                xmlfile.filename)
        else:
            path = runner.dump_dir.filename_public_path(xmlfile, self.wiki.date)
        dcontents = DumpContents(self.wiki, path, xmlfile, self.verbose)
        return bool(dcontents.find_first_page_id_in_file() == 0)


class XmlDump(Dump):
    """Primary XML dumps, one section at a time."""
    def __init__(self, subset, name, desc, detail, item_for_stubs, prefetch,
                 prefetchdate, spawn,
                 wiki, partnum_todo, parts=False, checkpoints=False, checkpoint_file=None,
                 page_id_range=None, verbose=False):
        self.jobinfo = {'subset': subset, 'detail': detail, 'desc': desc,
                        'prefetch': prefetch, 'prefetchdate': prefetchdate,
                        'spawn': spawn, 'partnum_todo': partnum_todo,
                        'pageid_range': page_id_range, 'item_for_stubs': item_for_stubs}
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
            self.onlyparts = True

        self.wiki = wiki
        self.verbose = verbose
        self._prerequisite_items = [self.jobinfo['item_for_stubs']]
        self.stubber = StubProvider(
            self.wiki, {'dumpname': self.get_dumpname(), 'parts': self._parts,
                        'dumpnamebase': self.get_dumpname_base(),
                        'item_for_stubs': item_for_stubs,
                        'partnum_todo': self.jobinfo['partnum_todo']},
            self.verbose)
        Dump.__init__(self, name, desc, self.verbose)

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

    def get_pagerange_output_dfname(self):
        """
        given page range passed in,
        return a dumpfilename for the appropriate checkpoint file
        """
        if ',' in self.jobinfo['pageid_range']:
            first_page_id, last_page_id = self.jobinfo['pageid_range'].split(',', 1)
        else:
            first_page_id = self.jobinfo['pageid_range']
            # really? ewww gross
            last_page_id = "00000"  # indicates no last page id specified, go to end of stub
        if self.jobinfo['partnum_todo']:
            partnum = self.jobinfo['partnum_todo']
        else:
            # fixme is that right? maybe NOT
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
        output_dfname = DumpFilename(self.wiki, self.wiki.date, self.get_dumpname(),
                                     self.get_filetype(), self.get_file_ext(),
                                     str(partnum), checkpoint=checkpoint_string,
                                     temp=False)
        return output_dfname

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
            elif exists(dump_dir.filename_private_path(finfo)):
                os.remove(dump_dir.filename_private_path(finfo))

    def get_done_pageranges(self, dump_dir, date):
        """
        get the current checkpoint files and from them get
        the page ranges that are covered by the files

        returns: sorted
        """
        chkpt_dfnames = self.list_checkpt_files(
            dump_dir, [self.get_dumpname()], date, parts=None)
        # chkpt_dfnames = sorted(chkpt_dfnames, key=lambda thing: thing.filename)
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
        return self.get_reg_files_for_filepart_possible(
            dump_dir, self.get_fileparts_list(), self.list_dumpnames())

    def get_first_last_page_ids(self, xml_dfname, dump_dir):
        """
        return the first and last page ids in a stub file based on
        looking at the content, can be slow because getting the last
        page id relies on decompression of the entire file
        """
        first_id = xml_dfname.first_page_id_int
        if not first_id:
            # get it from the file part and the config
            first_id = sum([int(self._parts[i]) for i in range(0, xml_dfname.partnum_int - 1)]) + 1

        last_id = xml_dfname.last_page_id_int
        if not last_id:
            if xml_dfname.partnum_int < len(self._parts):
                last_id = sum([int(self._parts[i]) for i in range(0, xml_dfname.partnum_int)])
            else:
                # last part. no way to compute a value from config, look at the file
                dcontents = DumpContents(
                    self.wiki,
                    dump_dir.filename_public_path(xml_dfname, xml_dfname.date),
                    xml_dfname, self.verbose)
                last_id = dcontents.find_last_page_id()
        return first_id, last_id

    def get_ranges_covered_by_stubs(self, dump_dir):
        """
        get the page ranges covered by stubs
        returns a list of tuples: (startpage<str>, endpage<str>, partnum<str>)
        """
        output_dfnames = self.get_reg_files_for_filepart_possible(
            dump_dir, self.get_fileparts_list(), self.list_dumpnames())
        stub_dfnames = [self.stubber.get_stub_dfname(dfname.partnum, dump_dir)
                        for dfname in output_dfnames]
        stub_dfnames = sorted(stub_dfnames, key=lambda thing: thing.filename)

        stub_ranges = []
        for stub_dfname in stub_dfnames:
            # why do we do this instead of getting the theoretical page
            # ranges (which are used in page content files anyways, aren't they?)
            # via the wiki config? FIXME

            first_page, last_page = self.get_first_last_page_ids(stub_dfname, dump_dir)
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
        for partnum in parts:
            if not [1 for chkpt_range in done_pageranges
                    if chkpt_range[2] == partnum]:
                # entire page range for a particular file part (subjob)
                # is missing so generate the regular output file
                output_dfnames = self.get_reg_files_for_filepart_possible(
                    dump_dir, self.get_fileparts_list(), self.list_dumpnames())
                todo.extend([dfname for dfname in output_dfnames
                             if dfname.partnum_int == partnum])
            else:
                # at least some page ranges are covered, just do those that
                # are missing (maybe none are and list is empty)
                todo.extend([self.make_dfname_from_pagerange((first, last), part)
                             for (first, last, part) in missing_ranges
                             if part == partnum])
        return todo

    def get_pagerange_jobs_for_file(self, partnum, page_start, page_end):
        """
        given an output filename, the start and end pages it should cover,
        split up into output filenames that will each contain roughly the same
        number of revisions, so that each dump to produce them doesn't
        take a ridiculous length of time

        args: DumpFilename, startpage<str>, endpage<str>
        returns: list of DumpFilename
        """
        output_dfnames = []
        if 'history' in self.jobinfo['subset']:
            prange = PageRange(QueryRunner(self.wiki.db_name, self.wiki.config,
                                           self.verbose), self.verbose)
            ranges = prange.get_pageranges_for_revs(page_start, page_end,
                                                    self.wiki.config.revs_per_job,
                                                    self.wiki.config.maxrevbytes)
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
        to_return = []
        for dfname in output_dfnames:
            # FIXME TEST TEST TEST
            pageranges = dumps.intervals.get_intervals_by_group(dfname.partnum_int, stub_pageranges)
            if not dfname.is_checkpoint_file:
                # we get all the ranges for the whole part
                for prange in pageranges:
                    to_return.extend(self.get_pagerange_jobs_for_file(
                        dfname.partnum_int, prange[0], prange[1]))
            else:
                # we get just the one range
                to_return.extend(self.get_pagerange_jobs_for_file(
                    dfname.partnum_int, dfname.first_page_id_int, dfname.last_page_id_int))
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

    def get_dfnames_from_pageranges(self, pageranges):
        """
        given a list of tuples of (startpageid, endpageid, partnum),
        return a list of corresponding dfnames
        """
        dfnames = []
        for startpage, endpage, partnum in pageranges:
            dfname = DumpFilename(
                self.wiki, self.wiki.date, self.get_dumpname(),
                self.get_filetype(), self.file_ext, partnum,
                DumpFilename.make_checkpoint_string(startpage, endpage),
                False)
            dfnames.append(dfname)
        return dfnames

    def get_dfnames_from_cached_pageranges(self, stub_pageranges, dfnames_todo, logger):
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
            dfnames_todo = self.get_dfnames_from_pageranges(bitesize_pageranges)
        else:
            dfnames_todo = self.make_bitesize_jobs(dfnames_todo, stub_pageranges)
            bitesize_pageranges = self.get_pageranges_from_dfnames(dfnames_todo)
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
        output_dfnames = self.get_reg_files_for_filepart_possible(
            dump_dir, self.get_fileparts_list(), self.list_dumpnames())
        # at least some page ranges are covered, just do those that
        dfnames_todo = [
            dfname for dfname in output_dfnames if not os.path.exists(
                dump_dir.filename_public_path(dfname))]
        return dfnames_todo

    def get_wanted_write_stubs(self, dfnames_todo, runner, prefetcher):
        """
        collect some info about the command for each output file we want to run,
        including input stubs, input prefetch files, output files, and so on;
        then generate stubs corresponding to each output file if needed.
        finally, return the collected command info.
        """
        wanted = [self.setup_wanted(dfname, runner, prefetcher) for dfname in dfnames_todo]

        to_generate = []
        for entry in wanted:
            if entry['generate']:
                to_generate.append((entry['stub_input'], entry['stub']))
        if self._parts:
            batchsize = int(len(self._parts) / 2)
        else:
            batchsize = 1
        self.stubber.write_pagerange_stubs(to_generate, runner, batchsize, self.move_if_truncated)

        # if we had to generate temp stubs, skip over those with no pages in them
        # it's possible a page range has nothing in the stub file because they were all deleted.
        # we have some projects with e.g. 35k pages in a row deleted!
        return [entry for entry in wanted if not entry['generate'] or
                not self.stubber.has_no_pages(entry['stub'], runner, tempdir=True)]

    def get_commands_for_wanted(self, wanted, runner):
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
                    runner.log_and_print("error from commands: %s" % " ".join(
                        [entry for entry in pipeline]))
        return broken

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

        # in cases where we have request of specific file, do it as asked,
        # no splitting it up into smaller pieces
        do_bitesize = False

        # clean up all tmp output files from previous attempts of this job
        # for this dump wiki and date; they may have been left around from
        # an interrupted or failed earlier run
        self.cleanup_tmp_files(runner.dump_dir, runner)

        dfnames_todo = []
        if self.jobinfo['pageid_range'] is not None:
            # convert to checkpoint filename, handle the same way
            dfnames_todo = [self.get_pagerange_output_dfname()]
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
                stub_pageranges, dfnames_todo, runner.log_and_print)

        if self.jobinfo['prefetch']:
            if runner.wiki.config.sevenzip_prefetch:
                file_exts = ['7z', self.file_ext]
            else:
                file_exts = [self.file_ext]
            prefetcher = PrefetchFinder(
                self.wiki,
                {'name': self.name(), 'desc': self.jobinfo['desc'],
                 'dumpname': self.get_dumpname(),
                 'ftype': self.file_type, 'fexts': file_exts,
                 'subset': self.jobinfo['subset']},
                {'date': self.jobinfo['prefetchdate'], 'parts': self._parts},
                self.verbose)
        else:
            prefetcher = None

        wanted = self.get_wanted_write_stubs(dfnames_todo, runner, prefetcher)

        commands = self.get_commands_for_wanted(wanted, runner)

        # don't do them all at once, do only up to _parts commands at the same time
        if self._parts:
            batchsize = len(self._parts)
        else:
            batchsize = 1
        errors = False
        failed_commands = []
        max_retries = self.wiki.config.max_retries
        retries = 0
        while commands and (retries < max_retries or retries == 0):
            broken = self.run_batch(commands[:batchsize], runner)
            if broken:
                failed_commands.append(broken)
                errors = True
            commands = commands[batchsize:]

            if not commands and failed_commands:
                retries += 1
                if retries < max_retries:
                    # retry failed commands
                    commands = failed_commands
                    failed_commands = []
                    # no instant retries, give the servers a break
                    time.sleep(self.wiki.config.retry_wait)
                    errors = False
        if errors:
            raise BackupError("error producing xml file(s) %s" % self.get_dumpname())

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
        dfnames = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        return [dfname for dfname in dfnames if dfname.is_temp_file]

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        """
        list output files including checkpoint files currently existing
        (from the dump run for the current wiki and date), in case
        we have been requested to clean up before a retry

        args:
            DumpDir, list of dump names ("stub-meta-history", ...)
        returns:
            list of DumpFilename
        """
        dfnames = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        dfnames_to_return = []

        if self.jobinfo['pageid_range']:
            # this file is for one page range only
            if ',' in self.jobinfo['pageid_range']:
                (first_page_id, last_page_id) = self.jobinfo['pageid_range'].split(',', 2)
                first_page_id = int(first_page_id)
                last_page_id = int(last_page_id)
            else:
                first_page_id = int(self.jobinfo['pageid_range'])
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
