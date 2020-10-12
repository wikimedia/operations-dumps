#!/usr/bin/python3
"""
test suite for prefetch module (used to dump
xml page contents by reading and extracting content
xfrom previous dumps)
"""
import os
import shutil
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.xmlcontentjobs import XmlDump, DFNamePageRangeConverter
from dumps.utils import FilePartInfo
from dumps.runner import Runner
from dumps.prefetch import PrefetchFinder


class TestPrefetch(BaseDumpsTestCase):
    """
    some basic tests for location of appropriate prefetch
    files during the production of xml page content files
    """
    @staticmethod
    def setup_prefetch_dir(wikiname, date):
        """
        make a fake dir for the specified wiki for the given date;
        this dir should be cleaned up after your test
        """
        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname, date)
        if not os.path.exists(fullpath):
            os.makedirs(fullpath)
        # add status file
        contents = "name:articlesdump; status:done; updated:2020-02-14 13:44:07\n"
        with open(os.path.join(fullpath, 'dumpruninfo.txt'), "w") as outfile:
            outfile.write(contents)

    @staticmethod
    def cleanup_prefetch_dir(wikiname, date):
        """
        remove the directory for the givein wiki and date, and all its contents
        """
        shutil.rmtree(os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname, date))

    def test_get_pagerange_to_prefetch(self):
        """
        FIXME add this later and write this code
        """
        return

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_find_prefetch_files_from_run(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        for a give run date, wiki, page range, and dumps job,
        make sure we can get a list of the appropriate page
        content files for prefetch
        """
        # any old date will do as long as it's earlier than today
        date = '20200101'

        # test batch one, checkpoints and parts

        self.setup_prefetch_dir(self.wd['wiki'].db_name, date)
        missing = {1: ['p1501p4000', 'p4322p4330'],
                   3: ['p4446p4600', 'p4601p4605'],
                   4: ['p5341p5345']}
        missing_ranges = missing[1] + missing[3] + missing[4]
        self.setup_xml_files_chkpts(['content'], date, excluded=missing_ranges)

        runner = Runner(self.wd['wiki'], prefetch=True, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)
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
        prefetcher = PrefetchFinder(
            content_job.wiki,
            {'name': content_job.name(), 'desc': content_job.jobinfo['desc'],
             'dumpname': content_job.get_dumpname(),
             'ftype': content_job.file_type, 'fexts': ['bz2', '7z'],
             'subset': content_job.jobinfo['subset']},
            {'date': content_job.jobinfo['prefetchdate'], 'pagesperpart': pages_per_part},
            content_job.verbose)

        with self.subTest('two consecutive ranges in one part, exactly matching existing files'):
            pagerange = {'start': 4331, 'end': 4380}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            expected_ranges = {2: ['p4331p4350', 'p4351p4380']}
            expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                          date=date, shuffle=False)
            self.assertEqual(prefetch_dfnames, expected_dfnames)

        with self.subTest('range in one part, no exact matches to existing files'):
            pagerange = {'start': 4375, 'end': 4398}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            expected_ranges = {2: ['p4351p4380', 'p4381p4443']}
            expected_dfnames = self.set_checkpt_filenames(expected_ranges, wiki=self.wd['wiki'],
                                                          date=date, shuffle=False)
            self.assertEqual(prefetch_dfnames, expected_dfnames)

        with self.subTest('range not covered by existing files'):
            pagerange = {'start': 4448, 'end': 4602}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            self.assertEqual(prefetch_dfnames, None)

        with self.subTest('get_prefetch_arg, range needs multiple files'):
            converter = DFNamePageRangeConverter(self.wd['wiki'], "pages-articles", "xml",
                                                 "bz2", verbose=False)
            to_produce = converter.make_dfname_from_pagerange((4375, 4398), 2)
            stub_converter = DFNamePageRangeConverter(self.wd['wiki'], "stub-articles", "xml",
                                                      "gz", verbose=False)
            corresponding_stub = converter.make_dfname_from_pagerange((4375, 4398), 2)
            prefetch_args = prefetcher.get_prefetch_arg(runner, to_produce, corresponding_stub)
            basedir = 'test/output/public/wikidatawiki/20200101/'
            expected_args = ('--prefetch=bzip2:' +
                             basedir + 'wikidatawiki-20200101-pages-articles2.xml-p4351p4380.bz2;' +
                             basedir + 'wikidatawiki-20200101-pages-articles2.xml-p4381p4443.bz2')
            self.assertEqual(prefetch_args, expected_args)

        self.cleanup_prefetch_dir(self.wd['wiki'].db_name, date)

        # test batch two, no checkpoint, parts only

        self.setup_prefetch_dir(self.wd['wiki'].db_name, date)
        self.setup_xml_files_parts(['stub', 'content'], date=date, excluded=['1', '4'])
        self.wd['wiki'].config.checkpoint_time = 0

        runner = Runner(self.wd['wiki'], prefetch=True, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)
        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=False, checkpoint_file=None,
                              page_id_range=None, verbose=False)
        prefetcher = PrefetchFinder(
            content_job.wiki,
            {'name': content_job.name(), 'desc': content_job.jobinfo['desc'],
             'dumpname': content_job.get_dumpname(),
             'ftype': content_job.file_type, 'fexts': ['bz2', '7z'],
             'subset': content_job.jobinfo['subset']},
            {'date': content_job.jobinfo['prefetchdate'], 'pagesperpart': pages_per_part},
            content_job.verbose)

        with self.subTest('range covered'):
            pagerange = {'start': 4331, 'end': 4380}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            expected_files = ["wikidatawiki-{date}-pages-articles2.xml.bz2".format(date=date)]
            expected_dfnames = self.dfnames_from_filenames(expected_files)
            self.assertEqual(prefetch_dfnames, expected_dfnames)

        with self.subTest('range covered by two parts'):
            pagerange = {'start': 4334, 'end': 4480}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            expected_files = ["wikidatawiki-{date}-pages-articles2.xml.bz2".format(date=date),
                              "wikidatawiki-{date}-pages-articles3.xml.bz2".format(date=date)]
            expected_dfnames = self.dfnames_from_filenames(expected_files)
            self.assertEqual(prefetch_dfnames, expected_dfnames)

        with self.subTest('range not covered in existing files'):
            pagerange = {'start': 4606, 'end': 4700}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            self.assertEqual(prefetch_dfnames, None)

        self.cleanup_prefetch_dir(self.wd['wiki'].db_name, date)

        # test batch three, no checkpoints or parts, just a single output file

        self.setup_prefetch_dir(self.wd['wiki'].db_name, date)
        self.setup_xml_files_noparts(['stub', 'content'], date)
        self.wd['wiki'].config.checkpoint_time = 0
        self.wd['wiki'].config.parts_enabled = 0

        runner = Runner(self.wd['wiki'], prefetch=True, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, item_for_stubs_recombine=None,
                              prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=None,
                              checkpoints=False, checkpoint_file=None,
                              page_id_range=None, verbose=False)
        prefetcher = PrefetchFinder(
            content_job.wiki,
            {'name': content_job.name(), 'desc': content_job.jobinfo['desc'],
             'dumpname': content_job.get_dumpname(),
             'ftype': content_job.file_type, 'fexts': ['bz2', '7z'],
             'subset': content_job.jobinfo['subset']},
            {'date': content_job.jobinfo['prefetchdate'], 'pagesperpart': False},
            content_job.verbose)

        with self.subTest('range covered but file too small'):
            # FIXME should capture the 'too small' warning and check we got it
            pagerange = {'start': 4331, 'end': 4380}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            self.assertEqual(prefetch_dfnames, None)

        self.cleanup_prefetch_dir(self.wd['wiki'].db_name, date)
        # don't set up anything, let's see how it works with no files present
        with self.subTest('range not covered in existing file'):
            pagerange = {'start': 4606, 'end': 4700}
            prefetch_dfnames = prefetcher.find_prefetch_files_from_run(runner, date,
                                                                       pagerange, 'bz2')
            self.assertEqual(prefetch_dfnames, None)


if __name__ == '__main__':
    unittest.main()
