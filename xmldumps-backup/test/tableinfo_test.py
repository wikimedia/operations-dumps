"""
test suite for tables dumps
"""
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.utils import FilePartInfo
from dumps.dumpitemlist import DumpItemList
from dumps.runnerutils import DumpRunJobData, RunSettings
from dumps.tableinfo import TableInfo


class TestTableInfo(BaseDumpsTestCase):
    """
    test use of tableinfo file in tables dump jobs
    """
    @patch('dumps.wikidump.Wiki.get_known_tables_from_db')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_tableinfo_file(self, _mock_get_some_stats, mock_get_known_tables_from_db):
        '''
        read and write tableinfo file, make sure it gets used appropriately
        '''
        known_tables = ['site_stats', 'langlinks', 'page_props',
                        'wb_items_per_site', 'wb_terms'
                        'wb_entity_per_page', 'wb_property_info'
                        'wb_changes_subscription', 'wbc_entity_usage']

        mock_get_known_tables_from_db.return_value = known_tables

        filepart_info = FilePartInfo(self.wd['wiki'], self.wd['wiki'].db_name)
        filepart_info._pages_per_filepart_abstract = [100, 1000, 1000, 1000]
        filepart_info._logitems_per_filepart_pagelogs = [100, 1000, 1000, 1000]

        dumpjobdata = DumpRunJobData(self.wd['wiki'], self.wd['dump_dir'], notice="",
                                     enabled=[RunSettings.NAME])

        dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                      spawn=True, partnum_todo=None, checkpoint_file=None,
                                      singleJob='tables', skip_jobs=[],
                                      filepart=filepart_info, page_id_range=None,
                                      dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                      verbose=False)

        # at this point we should have a table info file written, let's make sure
        tinfo = TableInfo(self.wd['wiki'], "json")
        tables = sorted(tinfo.get_tableinfo())
        expected_tables = ['langlinks', 'page_props', 'site_stats',
                           'wb_items_per_site', 'wb_property_infowb_changes_subscription',
                           'wb_termswb_entity_per_page', 'wbc_entity_usage']
        self.assertEqual(tables, expected_tables)

        # let's replace the db lookup return value with empty, and make sure it
        # doesn't get used in DumpItemList init now, but only the file read
        mock_get_known_tables_from_db.return_value = []

        dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                      spawn=True, partnum_todo=None, checkpoint_file=None,
                                      singleJob='tables', skip_jobs=[],
                                      filepart=filepart_info, page_id_range=None,
                                      dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                      verbose=False)

        job_names = [item.get_dumpname() for item in dump_item_list.dump_items]
        # check that one of the tables we list as known is in the list of jobs
        # (if getting from db were called, we would have no tables at all)
        self.assertTrue('langlinks' in job_names)
