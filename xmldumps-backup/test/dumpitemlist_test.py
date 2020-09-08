"""
test suite for DumpItemList
"""
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.utils import FilePartInfo
from dumps.dumpitemlist import DumpItemList
from dumps.runnerutils import DumpRunJobData, RunSettings


class TestDumpItemList(BaseDumpsTestCase):
    """
    test assembly of jobs into a DumpItemList
    """
    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_list_creation(self, _mock_get_some_stats, mock_get_known_tables):
        '''
        make sure we get the right command list for tables depending
        on their type (public, None, private)
        '''
        known_tables = ['site_stats', 'langlinks', 'page_props',
                        'wb_items_per_site', 'wb_terms'
                        'wb_entity_per_page', 'wb_property_info'
                        'wb_changes_subscription', 'wbc_entity_usage']

        mock_get_known_tables.return_value = known_tables

        filepart_info = FilePartInfo(self.wd['wiki'], self.wd['wiki'].db_name)
        filepart_info._pages_per_filepart_abstract = [100, 1000, 1000, 1000]
        filepart_info._logitems_per_filepart_pagelogs = [100, 1000, 1000, 1000]

        dumpjobdata = DumpRunJobData(self.wd['wiki'], self.wd['dump_dir'], notice="",
                                     enabled=[RunSettings.NAME])

        expected_item_names = ['sitestatstable', 'langlinkstable', 'pagepropstable',
                               'wbitemspersitetable', 'wbcentityusagetable', 'namespaces',
                               'pagetitlesdump', 'allpagetitlesdump', 'abstractsdump',
                               'abstractsdumprecombine', 'xmlstubsdump', 'xmlstubsdumprecombine',
                               'articlesdump', 'articlesdumprecombine', 'metacurrentdump',
                               'metacurrentdumprecombine', 'xmlpagelogsdump',
                               'xmlpagelogsdumprecombine', 'metahistorybz2dump',
                               'metahistory7zdump', 'articlesmultistreamdump',
                               'articlesmultistreamdumprecombine']

        with self.subTest('skip nothing'):
            dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                          spawn=True, partnum_todo=None, checkpoint_file=None,
                                          singleJob='tables', skip_jobs=[],
                                          filepart=filepart_info, page_id_range=None,
                                          dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                          numbatches=0, verbose=False)

            item_names = [item.name() for item in dump_item_list.dump_items]
            self.assertEqual(item_names, expected_item_names)

        with self.subTest('skip allpagetitlesdump and sitelistdump'):
            self.wd['wiki'].config.skipjobs = ['allpagetitlesdump', 'sitelistdump']
            dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                          spawn=True, partnum_todo=None, checkpoint_file=None,
                                          singleJob='tables', skip_jobs=[],
                                          filepart=filepart_info, page_id_range=None,
                                          dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                          numbatches=0, verbose=False)

            expected_item_names.remove('allpagetitlesdump')
            item_names = [item.name() for item in dump_item_list.dump_items]
            self.assertEqual(item_names, expected_item_names)

        with self.subTest('skip allpagetitlesdump, xmlpagelogsdump and sitelistdump'):
            self.wd['wiki'].config.skipjobs = ['allpagetitlesdump', 'xmlpagelogsdump', 'sitelistdump']
            dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                          spawn=True, partnum_todo=None, checkpoint_file=None,
                                          singleJob='tables', skip_jobs=[],
                                          filepart=filepart_info, page_id_range=None,
                                          dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                          numbatches=0, verbose=False)
            expected_item_names.remove('xmlpagelogsdump')
            expected_item_names.remove('xmlpagelogsdumprecombine')
            item_names = [item.name() for item in dump_item_list.dump_items]
            self.assertEqual(item_names, expected_item_names)
