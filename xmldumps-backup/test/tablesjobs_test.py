"""
test suite for tables dumps
"""
import os
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.utils import FilePartInfo
from dumps.dumpitemlist import DumpItemList
from dumps.runnerutils import DumpRunJobData, RunSettings
from dumps.runner import Runner


class TestTablesJobs(BaseDumpsTestCase):
    """
    test methods of tables dumps jobs
    """
    @patch('dumps.utils.exists', return_value=True)
    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_do_prep(self, _mock_get_some_stats, mock_get_known_tables, _mock_exists):
        '''
        make sure we get the right command list for tables depending
        on their type (public, None, private)
        '''
        known_tables = ['site_stats', 'langlinks', 'page_props',
                        'wb_items_per_site', 'wb_terms',
                        'wb_entity_per_page', 'wb_property_info',
                        'wb_changes_subscription', 'sites', 'wbc_entity_usage',
                        'change_tag', 'change_tag_def']

        mock_get_known_tables.return_value = known_tables

        filepart_info = FilePartInfo(self.wd['wiki'], self.wd['wiki'].db_name)
        filepart_info._logitems_per_filepart_pagelogs = [100, 1000, 1000, 1000]

        dumpjobdata = DumpRunJobData(self.wd['wiki'], self.wd['dump_dir'], notice="",
                                     enabled=[RunSettings.NAME])

        dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                      spawn=True, partnum_todo=None, checkpoint_file=None,
                                      singleJob='tables', skip_jobs=[],
                                      filepart=filepart_info, page_id_range=None,
                                      dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                      numbatches=0, verbose=False)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)
        runner.db_server_info.db_server = 'localhost'
        runner.db_server_info.db_port = '3306'
        runner.db_server_info.db_table_prefix = ''

        expected_command_part = ['/usr/bin/mysqldump',
                                 '--defaults-extra-file=/etc/dumps/extra/my-client.conf',
                                 '-h', '127.0.0.1', '--port', '3306',
                                 '--max_allowed_packet=32M',
                                 '--opt', '--quick', '--skip-add-locks', '--skip-lock-tables',
                                 'wikidatawiki']
        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name, self.today)
        for item in dump_item_list.dump_items:
            if item.name() == 'sitestatstable':
                # table marked as public in config file
                table_path = os.path.join(
                    fullpath, '{name}-{date}-site_stats.sql.gz.inprog'.format(
                        name=self.wd['wiki'].db_name, date=self.today))
                expected_commands = [[
                    expected_command_part + ['site_stats'],
                    ['/usr/bin/gzip', '>', table_path]]]

                produced_commands = item.do_prep(runner)
                self.assertEqual(produced_commands, expected_commands)
            elif item.name() == 'pagepropstable':
                # table type not marked in config file (should default to public)
                table_path = os.path.join(
                    fullpath, '{name}-{date}-page_props.sql.gz.inprog'.format(
                        name=self.wd['wiki'].db_name, date=self.today))
                expected_commands = [[
                    expected_command_part + ['page_props'],
                    ['/usr/bin/gzip', '>', table_path]]]
                produced_commands = item.do_prep(runner)
                self.assertEqual(produced_commands, expected_commands)
            elif item.name() == 'sitestable':
                # table marked as private in config file (should except out)
                with self.assertRaises(Exception) as context:
                    item.do_prep(runner)
                self.assertTrue('Unknown table type' in str(context.exception))
            elif item.name() == 'changetagstable':
                # table marked as partially public
                table_path = os.path.join(
                    fullpath, '{name}-{date}-change_tag.sql.gz.inprog'.format(
                        name=self.wd['wiki'].db_name, date=self.today))
                expected_command_part_for_change_tag = expected_command_part[:]
                expected_command_part_for_change_tag.insert(
                    expected_command_part_for_change_tag.index('--skip-lock-tables') + 1,
                    '--where=EXISTS(SELECT 1 FROM change_tag_def WHERE ctd_id = ct_tag_id ' +
                    'AND ctd_name NOT LIKE \'mw-private-%\')'
                )
                expected_commands = [[
                    expected_command_part_for_change_tag + ['change_tag'],
                    ['/usr/bin/gzip', '>', table_path]]]
                produced_commands = item.do_prep(runner)
                self.assertEqual(produced_commands, expected_commands)
            elif item.name() == 'changetagdeftable':
                # table marked as partially public
                table_path = os.path.join(
                    fullpath, '{name}-{date}-change_tag_def.sql.gz.inprog'.format(
                        name=self.wd['wiki'].db_name, date=self.today))
                expected_command_part_for_change_tag = expected_command_part[:]
                expected_command_part_for_change_tag.insert(
                    expected_command_part_for_change_tag.index('--skip-lock-tables') + 1,
                    '--where=ctd_name NOT LIKE \'mw-private-%\''
                )
                expected_commands = [[
                    expected_command_part_for_change_tag + ['change_tag_def'],
                    ['/usr/bin/gzip', '>', table_path]]]
                produced_commands = item.do_prep(runner)
                self.assertEqual(produced_commands, expected_commands)
