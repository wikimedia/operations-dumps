#!/usr/bin/python3
"""
test suite for xml content job
"""
import os
import gzip
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
import dumps.dumpitemlist
import dumps.wikidump
import dumps.xmlcontentjobs
from dumps.xmlcontentjobs import XmlDump
from dumps.utils import FilePartInfo
from dumps.fileutils import DumpFilename
from dumps.xmljobs import XmlStub
from dumps.runner import Runner
from dumps.pagerangeinfo import PageRangeInfo
from dumps.pagerange import PageRange, QueryRunner


class TestXmlDumpWithFixtures(BaseDumpsTestCase):
    """
    some basic tests for production of xml content files
    that use fixture files for stub and page content
    """
    def test_get_first_last_page_ids(self):
        """
        check that we can get the first and last page ids of an xml stubs
        file
        """
        self.setup_xml_files_chkpts(['stub'], self.today)
        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)
        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        xml_dfname = DumpFilename(self.wd['wiki'])
        xml_dfname.new_from_filename('wikidatawiki-{today}-stub-articles1.xml.gz'.format(
            today=self.today))
        firstid, lastid = content_job.stubber.get_first_last_page_ids(
            xml_dfname, self.wd['dump_dir'], pages_per_part)
        expected_ids = [1, 4330]
        self.assertEqual([firstid, lastid], expected_ids)

    def test_get_ranges_covered_by_stubs(self):
        """
        make sure that we get good list of page ranges covered by
        stubs when we have sample stub files
        """

        self.setup_xml_files_chkpts(['stub'], self.today)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        expected_stub_ranges = [(1, 4330, 1), (4331, 4443, 2), (4444, 4605, 3), (4606, 5344, 4)]
        stub_ranges = content_job.get_ranges_covered_by_stubs(self.wd['dump_dir'])
        self.assertEqual(stub_ranges, expected_stub_ranges)

    def test_get_done_pageranges(self):
        """
        make sure that we get a reasonable list of completed pageranges when
        some stub and page content files are present, for checkpoint files
        """
        self.setup_xml_files_chkpts(['stub', 'content'], self.today)

        expected_pageranges = [(1, 1500, 1), (1501, 4000, 1), (4001, 4321, 1), (4322, 4330, 1),
                               (4331, 4350, 2), (4351, 4380, 2), (4381, 4443, 2),
                               (4444, 4445, 3), (4446, 4600, 3), (4601, 4605, 3),
                               (4606, 5340, 4), (5341, 5345, 4)]

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        done_pageranges = content_job.get_done_pageranges(self.wd['dump_dir'], self.wd['wiki'].date)
        self.assertEqual(done_pageranges, expected_pageranges)

    def test_get_dfnames_for_missing_pranges(self):
        """
        make sure that we get an appropriate list of pageranges to generate, when
        (all) stub files and only some page content files are present
        """
        missing = {1: ['p1501p4000', 'p4322p4330'],
                   3: ['p4446p4600', 'p4601p4605'],
                   4: ['p5341p5345']}

        missing_ranges = missing[1] + missing[3] + missing[4]

        self.setup_xml_files_chkpts(['stub', 'content'], self.today, excluded=missing_ranges)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # note that 4 is not p5341p5345 because it's based off of actual pages in the
        # stub for the last page in the last part. the other filenames can cover
        # intervals even where the last pages in those intervals are missing;
        # just figure out the interval start and end for eac part from the wiki config.
        expected_ranges = {1: ['p1501p4000', 'p4322p4330'],
                           3: ['p4446p4605'],
                           4: ['p5341p5344']}

        expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                      shuffle=False)

        stub_pageranges = content_job.get_ranges_covered_by_stubs(self.wd['dump_dir'])
        stub_pageranges = sorted(stub_pageranges, key=lambda x: x[0])
        todo_dfnames = content_job.get_dfnames_for_missing_pranges(
            self.wd['dump_dir'], self.wd['wiki'].date, stub_pageranges)
        self.assertEqual(todo_dfnames, expected_dfnames)

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_get_dfnames_from_cached_pageranges(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure we can get good dfnames todo given stub files,
        and some content files with some page ranges missing,
        for wikis with checkpoints enabled
        """
        missing = {1: ['p1501p4000', 'p4322p4330'],
                   3: ['p4446p4600', 'p4601p4605'],
                   4: ['p5341p5345']}

        missing_ranges = missing[1] + missing[3] + missing[4]

        self.setup_xml_files_chkpts(['stub', 'content'], self.today, excluded=missing_ranges)
        self.setup_xml_files_noparts(['stub'], self.today)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        stub_pageranges, dfnames_todo = content_job.get_todos_for_checkpoints(
            self.wd['dump_dir'], self.wd['wiki'].date)

        # call our method for testing at last
        dfnames_todo = content_job.get_dfnames_from_cached_pageranges(
            stub_pageranges, dfnames_todo, print, runner)

        # split up the jobs per number of revisions according to the config
        expected_ranges = {1: ['p1501p2000', 'p2001p2500', 'p2501p3000', 'p3001p3500',
                               'p3501p4000', 'p4322p4330'],
                           3: ['p4446p4605'],
                           4: ['p5341p5344']}
        # make DumpFilenames out of them
        expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                      shuffle=False)

        self.assertEqual(dfnames_todo, expected_dfnames)

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_get_wanted_rerun(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure we can get good dfnames todo given stub files,
        and some content files with some page ranges missing,
        for wikis with checkpoints enabled
        this is the code path that would be executed on a rerun
        of a failed job
        """
        missing = {1: ['p1501p4000', 'p4322p4330'],    # nonconsecutive ranges in a part
                   3: ['p4446p4600', 'p4601p4605'],    # whole part
                   4: ['p5341p5345']}

        missing_ranges = missing[1] + missing[3] + missing[4]

        self.setup_xml_files_chkpts(['stub', 'content'], self.today, excluded=missing_ranges)
        self.setup_xml_files_noparts(['stub'], self.today)

        # set up a fake pagerangeinfo file too
        fake_pageranges = [(1501, 4000, 1), (1, 1500, 1), (4001, 4321, 1), (4322, 4330, 1),
                           (4331, 4350, 2), (4351, 4380, 2), (4381, 4443, 2),
                           (4444, 4445, 3), (4446, 4600, 3), (4601, 4605, 3),
                           (4606, 5340, 4), (5341, 5345, 4)]
        pr_info = PageRangeInfo(self.wd['wiki'], enabled=True, fileformat="json",
                                error_callback=None, verbose=False)
        pr_info.update_pagerangeinfo(self.wd['wiki'], self.jobname, fake_pageranges)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        stub_pageranges, dfnames_todo = content_job.get_todos_for_checkpoints(
            self.wd['dump_dir'], self.wd['wiki'].date)

        # see what pagerangeinfo has for us
        dfnames_todo = content_job.get_dfnames_from_cached_pageranges(
            stub_pageranges, dfnames_todo, print, runner)

        # call first test method
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)

        # grab the list of dfnames wanted to produce
        wanted_dfnames = [entry['outfile'] for entry in wanted]

        # split up the jobs per number of revisions according to the config
        expected_ranges = {1: ['p1501p2000', 'p2001p2500', 'p2501p3000', 'p3001p3500',
                               'p3501p4000', 'p4322p4330'],
                           3: ['p4446p4605'],
                           4: ['p5341p5344']}
        # make DumpFilenames out of them
        expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                      shuffle=False)
        with self.subTest('check wanted dfnames'):
            self.assertEqual(wanted_dfnames, expected_dfnames)

        to_generate = content_job.get_to_generate_for_temp_stubs(wanted)

        # call the final test method
        commands, output_dfnames = content_job.stubber.get_commands_for_temp_stubs(
            to_generate, runner)

        expected_stub_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                           shuffle=False, stubs=True)

        expected_fspecs = {1: ['1501:2001', '2001:2501', '2501:3001', '3001:3501',
                               '3501:4001', '4322:4331'],
                           3: ['4446:4606'],
                           4: ['5341:5345']}

        expected_commands = self.make_expected_stub_commands(
            expected_ranges,
            expected_fspecs,
            BaseDumpsTestCase.PUBLICDIR + '/' + self.wd['wiki'].db_name,
            self.wd['wiki'])

        with self.subTest('check stub output dfnames'):
            self.assertEqual(output_dfnames, expected_stub_dfnames)
            self.assertEqual(commands, expected_commands)

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_get_wanted_firstrun(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure we can get good dfnames todo given stub files,
        and some content files with some page ranges missing,
        for wikis with checkpoints enabled
        this is the code path that would be executed on a first
        run of the job
        """
        self.setup_xml_files_chkpts(['stub'], self.today)
        # self.setup_stub_history_files(self.today)
        self.setup_xml_files_noparts(['stub'], self.today)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)
        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        stub_pageranges, dfnames_todo = content_job.get_todos_for_checkpoints(
            self.wd['dump_dir'], self.wd['wiki'].date)

        # pagerangeinfo should be nonexistent
        dfnames_todo = content_job.get_dfnames_from_cached_pageranges(
            stub_pageranges, dfnames_todo, print, runner)

        # call our first test method
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)

        # grab the list of dfnames wanted to produce
        wanted_dfnames = [entry['outfile'] for entry in wanted]

        # split up the jobs per number of revisions according to the wikidata config
        expected_ranges = {1: ['p1p500', 'p501p1000', 'p1001p1500', 'p1501p2000',
                               'p2001p2500', 'p2501p3000', 'p3001p3500', 'p3501p4000',
                               'p4001p4330'],
                           2: ['p4331p4443'],
                           3: ['p4444p4605'],
                           4: ['p4606p5105', 'p5106p5344']}
        # make DumpFilenames out of them
        expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                      shuffle=False)

        with self.subTest('check wanted dfnames'):
            self.assertEqual(wanted_dfnames, expected_dfnames)

        to_generate = content_job.get_to_generate_for_temp_stubs(wanted)

        # call the final test method
        commands, output_dfnames = content_job.stubber.get_commands_for_temp_stubs(
            to_generate, runner)

        expected_stub_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                           shuffle=False, stubs=True)

        expected_fspecs = {1: ['1:501', '501:1001', '1001:1501', '1501:2001', '2001:2501',
                               '2501:3001', '3001:3501', '3501:4001', '4001:4331'],
                           2: ['4331:4444'],
                           3: ['4444:4606'],
                           4: ['4606:5106', '5106:5345']}

        expected_commands = self.make_expected_stub_commands(
            expected_ranges,
            expected_fspecs,
            BaseDumpsTestCase.PUBLICDIR + '/' + self.wd['wiki'].db_name,
            self.wd['wiki'])

        with self.subTest('check stub output dfnames'):
            self.assertEqual(output_dfnames, expected_stub_dfnames)
            self.assertEqual(commands, expected_commands)

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_get_wanted_no_chkpts(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure we can get good dfnames todo given stub files,
        and some content files with some parts missing,
        for wikis without checkpoints enabled
        """
        self.setup_xml_files_parts(['stub', 'content'], self.today, excluded=['2', '4'])
        self.wd['wiki'].config.checkpoint_time = 0

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=False, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        dfnames_todo = content_job.get_todos_no_checkpoints(self.wd['dump_dir'])

        # call first test method
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)
        # grab the list of dfnames wanted to produce
        wanted_dfnames = [entry['outfile'] for entry in wanted]

        # set up filenames for the missing parts
        expected_files = [
            "wikidatawiki-{today}-pages-articles2.xml.bz2".format(today=self.today),
            "wikidatawiki-{today}-pages-articles4.xml.bz2".format(today=self.today)]
        # make DumpFilenames out of them
        expected_dfnames = self.dfnames_from_filenames(expected_files)

        with self.subTest('check wanted dfnames'):
            self.assertEqual(wanted_dfnames, expected_dfnames)

        to_generate = content_job.get_to_generate_for_temp_stubs(wanted)

        # call the final test method
        commands, output_dfnames = content_job.stubber.get_commands_for_temp_stubs(
            to_generate, runner)

        with self.subTest('check stub output dfnames'):
            self.assertEqual(output_dfnames, [])
            self.assertEqual(commands, [])

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_get_wanted_vanilla(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure we can get good dfname todo given the single stub file
        and no content,
        for wikis without checkpoints or parts
        """
        self.setup_xml_files_noparts(['stub'], self.today)
        self.wd['wiki'].config.parts_enabled = 0
        self.wd['wiki'].config.checkpoint_time = 0

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=None)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=None,
                              checkpoints=False, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        dfnames_todo = content_job.get_todos_no_checkpoints(self.wd['dump_dir'])

        # call first test method
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)

        # grab the list of dfnames wanted to produce
        wanted_dfnames = [entry['outfile'] for entry in wanted]

        # set up our one output file
        expected_files = ["wikidatawiki-{today}-pages-articles.xml.bz2".format(today=self.today)]
        # make DumpFilename out of it
        expected_dfnames = self.dfnames_from_filenames(expected_files)

        with self.subTest('check wanted dfnames'):
            self.assertEqual(wanted_dfnames, expected_dfnames)

        to_generate = content_job.get_to_generate_for_temp_stubs(wanted)

        # call the final test method
        commands, output_dfnames = content_job.stubber.get_commands_for_temp_stubs(
            to_generate, runner)

        with self.subTest('check stub output dfnames'):
            self.assertEqual(output_dfnames, [])
            self.assertEqual(commands, [])

    def make_expected_stub_commands(self, expected_ranges, expected_fspecs, dump_dir, wiki):
        """
        concoct writeuptopageid commands given the page ranges and fspec info for them
        """
        stub_commands = []
        for part in expected_ranges:
            input_stub = '{dumpdir}/{date}/{wiki}-{date}-stub-articles{part}.xml.gz'.format(
                dumpdir=dump_dir, wiki=wiki.db_name, date=wiki.date, part=part)
            temp_dir = self.TEMPDIR + '/' + wiki.db_name[0] + '/' + wiki.db_name
            fspecs_list = []
            for index, prange in enumerate(expected_ranges[part]):
                fspecs_list.append(
                    '{wiki}-{date}-stub-articles{part}.xml-{prange}.gz:{fspec}'.format(
                        wiki=wiki.db_name, date=wiki.date, part=part, prange=prange,
                        fspec=expected_fspecs[part][index]))
            fspecs_string = ';'.join(fspecs_list)
            command = [[['/usr/bin/gzip', '-dc', input_stub],
                        ['/usr/local/bin/writeuptopageid', '--odir', temp_dir,
                         '--fspecs', fspecs_string]]]
            stub_commands.append(command)
        return stub_commands

    def get_bz2_todos(self, page_ranges):
        '''
        given part numbers and page ranges for some output files,
        produce the commands that would be run to generate them
        from stubs of the same names
        '''
        expected_todo = []
        dirname = 'test/output/public/{wiki}/{date}'.format(
            wiki=self.wd['wiki'].db_name, date=self.today)
        for prange in page_ranges:
            infile = '{dirname}/{wiki}-{date}-stub-articles{prange}.gz'.format(
                dirname=dirname, wiki=self.wd['wiki'].db_name,
                date=self.today, prange=prange)
            outfile = '{dirname}/{wiki}-{date}-pages-articles{prange}.bz2.inprog'.format(
                dirname=dirname, wiki=self.wd['wiki'].db_name,
                date=self.today, prange=prange)
            # FIXME what do we think about that empty '' entry in there? should we filter it out??
            expected_todo.append([[[
                '/usr/bin/php',
                'test/mediawiki/maintenance/dumpTextPass.php',
                '--wiki={wiki}'.format(wiki=self.wd['wiki'].db_name),
                '--stub=gzip:{infile}'.format(infile=infile),
                '', '--report=1000', '--spawn=/usr/bin/php',
                '--output=bzip2:{outfile}'.format(outfile=outfile),
                '--current']]])
        return expected_todo

    def setup_fake_bz2_files_chkpts(self, wikiname, date, page_ranges):
        '''
        make some junk bz2 files with names corresponding to pageranges
        passed in
        '''
        for part in page_ranges:
            for prange in page_ranges[part]:
                basefilename = '{wiki}-{date}-pages-articles{part}.xml-{prange}.bz2'.format(
                    wiki=wikiname, date=date, part=part, prange=prange)
                outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname,
                                       date, basefilename)
                with open(outpath, "w") as output:
                    output.write("fake\n")

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_batch_command_generation(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure that given stub files, we generate good commands for
        producing  bz2-compressed page content files from those, in batches,
        for wikis with checkpoints enabled.
        also check that we skip commands where the page content file has
        magically appeared before a batch of commands is to be generated
        """
        self.setup_xml_files_chkpts(['stub'], self.today)
        self.setup_xml_files_noparts(['stub'], self.today)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # done with setup, now get args for the method
        stub_pageranges, dfnames_todo = content_job.get_todos_for_checkpoints(
            self.wd['dump_dir'], self.wd['wiki'].date)
        # pagerangeinfo should be nonexistent
        dfnames_todo = content_job.get_dfnames_from_cached_pageranges(
            stub_pageranges, dfnames_todo, print, runner)
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)
        # we don't filter out anything here, no checks for empty stubs, who cares
        todo = wanted
        commands_left = content_job.get_commands_for_pagecontent(todo, runner)

        with self.subTest('unfiltered bz2 command batch'):
            commands_todo, commands_left = content_job.get_command_batch(commands_left, runner)
            page_ranges = ['1.xml-p1p500', '1.xml-p501p1000',
                           '1.xml-p1001p1500', '1.xml-p1501p2000']
            expected_todo = self.get_bz2_todos(page_ranges)
            self.assertEqual(commands_todo, expected_todo)

        with self.subTest('bz2 command batch with some output files existing'):
            # don't actually run any commands, just pretend we did and check the next batch,
            # but first add a few bz2 files to the directory; we want to see that those
            # are skipped over when the next command batch is generated
            page_ranges = {'1': ['p2501p3000', 'p3501p4000']}
            self.setup_fake_bz2_files_chkpts(self.wd['wiki'].db_name, self.today, page_ranges)

            commands_todo, commands_left = content_job.get_command_batch(commands_left, runner)
            page_ranges = ['1.xml-p2001p2500', '1.xml-p3001p3500',
                           '1.xml-p4001p4330', '2.xml-p4331p4443']
            expected_todo = self.get_bz2_todos(page_ranges)
            self.assertEqual(commands_todo, expected_todo)

    @patch('dumps.xmlcontentjobs.XmlDump.get_revinfofile_path')
    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_revinfo_generation(self, _mock_get_some_stats, _mock_get_known_tables,
                                mock_get_revinfofile_path):
        '''
        generate revinfo from a sample stub, writing it to a file via the c util,
        and verify the output
        '''
        self.wd['wiki'].config.revinfostash = 1

        temp_dir = self.TEMPDIR + '/' + self.wd['wiki'].db_name[0] + '/' + self.wd['wiki'].db_name
        # ordinarily we call wiki_tempdir to get the temp dir name and that mkdirs for us
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        revinfo_path = os.path.join(temp_dir, 'wikidata-' + self.today + '-revinfo.gz')
        mock_get_revinfofile_path.return_value = revinfo_path

        self.setup_stub_history_files(self.today)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("meta-history", "metahistorybz2dump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        # put the rev info into a file in the right location
        content_job.stash_revinfo(runner, batchsize=2)
        revinfo_path = content_job.get_revinfofile_path()
        with gzip.open(revinfo_path, "r") as revinfo_in:
            revinfo = revinfo_in.read().decode('utf-8')
        expected_revinfo = "\n".join(["40:11798:8", "42:38582:11", "44:449:7",
                                      "46:402:8", "48:372:8", "52:411:8",
                                      "54:438:10", "56:1321:11", "58:440:9",
                                      "60:154253:38"]) + "\n"
        self.assertEqual(revinfo, expected_revinfo)

    def test_revinfo_use(self):
        '''
        write fake revinfo to the appropriate file, then use this to generate
        small job page ranges for a specific page start and end of a (presumed) range
        too large for one job
        '''
        temp_dir = self.TEMPDIR + '/' + self.wd['wiki'].db_name[0] + '/' + self.wd['wiki'].db_name

        revinfo_path = os.path.join(temp_dir, 'wikidata-' + self.today + '-revinfo.gz')

        page_start = 40
        page_end = 60

        revinfo = "\n".join(["40:11798:8", "42:38582:11", "44:449:7",
                             "46:402:8", "48:372:8", "52:411:8",
                             "54:438:10", "56:1321:11", "58:440:9",
                             "60:154253:38"]) + "\n"

        # ordinarily we call wiki_tempdir to get the temp dir name and that mkdirs for us
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        with gzip.open(revinfo_path, "w+") as revinfo_out:
            revinfo_out.write(revinfo.encode("utf-8"))

        prange = PageRange(QueryRunner(self.wd['wiki'].db_name, self.wd['wiki'].config,
                                       False), False)

        # FIXME here we should make sure the output is right
        ranges = prange.get_pageranges_for_revs(page_start, page_end,
                                                20, 20000, revinfo_path, 2)

        expected_ranges = [(40, 41), (42, 47), (48, 55), (56, 60)]
        self.assertEqual(ranges, expected_ranges)


if __name__ == '__main__':
    unittest.main()
