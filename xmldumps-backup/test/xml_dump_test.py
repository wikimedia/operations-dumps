#!/usr/bin/python3
"""
text suite for xml content job
"""
import os
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.xmlcontentjobs import XmlDump, DFNamePageRangeConverter
from dumps.xmljobs import XmlStub
from dumps.utils import FilePartInfo
import dumps.dumpitemlist
import dumps.pagerange
import dumps.intervals


class TestXmlDump(BaseDumpsTestCase):
    """
    some basic tests for production of xml content files
    """
    def test_make_dfname_from_pagerange(self):
        """
        make sure we can get a dfname with at least the filename attribute
        being correct, from a pagerange passed in
        """
        converter = DFNamePageRangeConverter(self.en['wiki'], "pages-articles", "xml",
                                             "bz2", verbose=False)
        dfname = converter.make_dfname_from_pagerange((230, 295), 2)
        expected_filename = 'enwiki-{today}-pages-articles2.xml-p230p295.bz2'.format(
            today=self.today)
        self.assertEqual(dfname.filename, expected_filename)

    def test_get_nochkpt_outputfiles(self):
        """
        make sure that for conf with checkpoints disabled, we get a
        good list of output files to be produced, with or without part numbers
        """
        # turn off checkpoints in the config but keep part numbers
        self.en['wiki'].config.checkpoint_time = 0

        parts = FilePartInfo.convert_comma_sep(self.en['wiki'].config.pages_per_filepart_history)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=parts,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        dfnames = content_job.get_nochkpt_outputfiles(self.en['dump_dir'])
        expected_files = [
            "enwiki-{today}-pages-articles1.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles2.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles3.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles4.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        self.assertEqual(dfnames, expected_dfnames)

        # turn off part numbers now
        self.en['wiki'].config.parts_enabled = 0

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=False,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        dfnames = content_job.get_nochkpt_outputfiles(self.en['dump_dir'])
        expected_files = [
            "enwiki-{today}-pages-articles.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        self.assertEqual(dfnames, expected_dfnames)

    @patch('dumps.xmljobs.XmlStub.list_outfiles_for_input')
    @patch('dumps.stubprovider.StubProvider.get_first_last_page_ids')
    def test_get_ranges_covered_by_stubs(self,
                                         mock_get_first_last_page_ids,
                                         mock_list_outfiles_for_input):
        """
        make sure that we get good list of page ranges covered by
        stubs when we feed in fake tuples describing what the stubs cover
        """
        mock_list_outfiles_for_input.return_value = self.set_stub_output_filenames([1, 2, 3, 4])
        mock_get_first_last_page_ids.side_effect = self.get_fake_first_last_pageids

        parts = FilePartInfo.convert_comma_sep(self.en['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.en['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            parts=parts)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=parts,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        expected_stub_ranges = [(1, 100, 1), (101, 300, 2),
                                (301, 600, 3), (601, 3400, 4)]
        stub_ranges = content_job.get_ranges_covered_by_stubs(self.en['dump_dir'])
        self.assertEqual(stub_ranges, expected_stub_ranges)

    @patch('dumps.xmlcontentjobs.XmlDump.list_checkpt_files')
    def test_get_done_pageranges(self, mock_list_checkpt_files):
        """
        make sure that we get a reasonable list of completed pageranges when
        we feed in a list of complete output files (supposedly found in the dump run
        output directory)
        """
        pagerange_strings = {1: ['p1p48', 'p49p65', 'p66p82'],
                             2: ['p135p151', 'p152p168', 'p169p185', 'p203p295'],
                             3: ['p301p319', 'p320p384', 'p438p461', 'p577p599'],
                             4: ['p601p659', 'p660p690', 'p691p712', 'p713p735', 'p736p3024']}
        mock_list_checkpt_files.return_value = self.set_checkpt_filenames(pagerange_strings,
                                                                          wiki=self.en['wiki'])

        expected_pageranges = [(1, 48, 1), (49, 65, 1), (66, 82, 1),
                               (135, 151, 2), (152, 168, 2),
                               (169, 185, 2), (203, 295, 2),
                               (301, 319, 3), (320, 384, 3),
                               (438, 461, 3), (577, 599, 3),
                               (601, 659, 4), (660, 690, 4),
                               (691, 712, 4), (713, 735, 4),
                               (736, 3024, 4)]

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=FilePartInfo.convert_comma_sep(
                                  self.en['wiki'].config.pages_per_filepart_history),
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        done_pageranges = content_job.get_done_pageranges(self.en['dump_dir'], self.en['wiki'].date)
        self.assertEqual(done_pageranges, expected_pageranges)

    def set_stub_output_filenames(self, parts):
        """
        given a number of parts, put together a list of DumpFilenames
        for the corresponding stub output files and return them
        """
        stub_filenames = []
        for partnum in parts:
            stub_filenames.append(
                "{wiki}-{date}-stub-articles{partnum}.xml.gz".format(
                    wiki=self.en['wiki'].db_name, date=self.today,
                    partnum=partnum))
        return self.dfnames_from_filenames(stub_filenames)

    @staticmethod
    def get_fake_first_last_pageids(xml_dfname, _dump_dir, _parts):
        """given a DumpFilename, return a reasonable first
        and last page id for the file for testing"""
        page_id_info = {1: [1, 100],
                        2: [101, 300],
                        3: [301, 600],
                        4: [601, 3400]}
        if xml_dfname.partnum_int is None or xml_dfname.partnum_int not in page_id_info:
            return None, None
        return page_id_info[xml_dfname.partnum_int][0], page_id_info[xml_dfname.partnum_int][1]

    @patch('dumps.xmljobs.XmlStub.list_outfiles_for_input')
    @patch('dumps.xmlcontentjobs.XmlDump.list_checkpt_files')
    @patch('dumps.stubprovider.StubProvider.get_first_last_page_ids')
    def test_get_todos_for_checkpoints(self,
                                       mock_get_first_last_page_ids,
                                       mock_list_checkpt_files,
                                       mock_list_outfiles_for_input):
        """
        make sure that we get reasonable list of output files to be produced,
        when fed in appropriate page ranges covered (supposedly) by stubs and by
        (supposedly existing) page content files
        for wikis with checkpoints and part numbers
        """
        covered_pagerange_strings = {1: ['p1p48', 'p49p65', 'p66p82'],
                                     2: ['p135p151', 'p152p168', 'p169p185', 'p203p295'],
                                     3: ['p301p319', 'p320p384', 'p438p461', 'p577p599'],
                                     4: ['p601p659', 'p660p690', 'p691p712', 'p713p735',
                                         'p736p3024']}

        expected_pageranges = [(83, 100, 1),
                               (101, 134, 2), (186, 202, 2), (296, 300, 2),
                               (385, 437, 3), (462, 576, 3), (600, 600, 3),
                               (3025, 3400, 4)]

        expected_files = [
            'enwiki-{today}-pages-articles1.xml-p83p100.bz2'.format(today=self.today),

            'enwiki-{today}-pages-articles2.xml-p101p134.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles2.xml-p186p202.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles2.xml-p296p300.bz2'.format(today=self.today),

            'enwiki-{today}-pages-articles3.xml-p385p437.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles3.xml-p462p576.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles3.xml-p600p600.bz2'.format(today=self.today),

            'enwiki-{today}-pages-articles4.xml-p3025p3400.bz2'.format(today=self.today)]

        expected_dfnames = self.dfnames_from_filenames(expected_files)
        mock_list_checkpt_files.return_value = self.set_checkpt_filenames(covered_pagerange_strings,
                                                                          wiki=self.en['wiki'])
        mock_list_outfiles_for_input.return_value = self.set_stub_output_filenames([1, 2, 3, 4])
        # given the attributes that were passed into the constructor, we want to send,
        # based on the partnum in the file, a different starting page number each time
        # so we need access to the dfname passed into the constructor.
        # so we need access to the object, and it needs to be the one in dumps.xmlcontentjobs
        # in the XmlDump class, and there are two instances of it btw, no only
        # one is ever created based on a branch. Fine.
        mock_get_first_last_page_ids.side_effect = self.get_fake_first_last_pageids

        parts = FilePartInfo.convert_comma_sep(self.en['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.en['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            parts=parts)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=parts,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        stub_pageranges, dfnames_todo = content_job.get_todos_for_checkpoints(
            self.en['dump_dir'], self.en['wiki'].date)
        self.assertEqual(stub_pageranges, expected_pageranges)
        self.assertEqual(dfnames_todo, expected_dfnames)

    def setup_empty_pagecontent_files_parts(self, partnums):
        """
        create empty fake page content files in output directory
        for testing, with part numbers
        """
        basedir = os.path.join(TestXmlDump.PUBLICDIR, 'enwiki', self.today)
        for partnum in partnums:
            filename = "{wiki}-{date}-pages-articles{partnum}.xml.bz2".format(
                wiki=self.en['wiki'].db_name, date=self.today, partnum=partnum)
            path = os.path.join(basedir, filename)
            with open(path, "w") as output:
                output.write("fake\n")

    def setup_empty_pagecontent_file(self):
        """
        create single empty fake page content file in output directory
        (coveing all page content for the wiki) for testing
        """
        basedir = os.path.join(TestXmlDump.PUBLICDIR, 'enwiki', self.today)
        filename = "{wiki}-{date}-pages-articles.xml.bz2".format(
            wiki=self.en['wiki'].db_name, date=self.today)
        path = os.path.join(basedir, filename)
        with open(path, "w") as output:
            output.write("fake\n")

    def test_get_todos_no_checkpoints(self):
        """
        in the case the wiki is not configured for checkpoint files,
        make sure that, given the existence of some (empty, we
        don't care) page content files in the output directory,
        a reasonable list of DumpFilenames covering the missing
        parts is returned
        for wikis without checkpoints, with or without part numbers
        """
        # turn off checkpoints in the config but keep part numbers
        self.en['wiki'].config.checkpoint_time = 0

        parts = FilePartInfo.convert_comma_sep(self.en['wiki'].config.pages_per_filepart_history)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=parts,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        expected_files = [
            "enwiki-{today}-pages-articles1.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles2.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles3.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles4.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        with self.subTest('no output files ready'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)

        self.setup_empty_pagecontent_files_parts([3, 4])
        expected_files = [
            "enwiki-{today}-pages-articles1.xml.bz2".format(today=self.today),
            "enwiki-{today}-pages-articles2.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        with self.subTest('two output files ready'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)

        self.setup_empty_pagecontent_files_parts([1, 3, 4])
        expected_files = [
            "enwiki-{today}-pages-articles2.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        with self.subTest('all but one output file ready'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)

        expected_dfnames = []
        self.setup_empty_pagecontent_files_parts([1, 2, 3, 4])
        with self.subTest('all output files ready'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)

        # turn off part numbers now
        self.en['wiki'].config.parts_enabled = 0

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=None, prefetch=True, prefetchdate=None,
                              spawn=True, wiki=self.en['wiki'], partnum_todo=False,
                              parts=False,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        expected_files = [
            "enwiki-{today}-pages-articles.xml.bz2".format(today=self.today)]
        expected_dfnames = self.dfnames_from_filenames(expected_files)
        with self.subTest('no parts, content missing'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)

        self.setup_empty_pagecontent_file()
        expected_dfnames = []
        with self.subTest('no parts, content present'):
            dfnames_todo = content_job.get_todos_no_checkpoints(self.en['dump_dir'])
            self.assertEqual(dfnames_todo, expected_dfnames)


if __name__ == '__main__':
    unittest.main()
