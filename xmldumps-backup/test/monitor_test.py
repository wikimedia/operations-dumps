#!/usr/bin/python3
"""
test suite for pagerangeinfo module
"""
import json
import os
import time
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.pagerangeinfo import PageRangeInfo
from dumps.batch import PageContentBatches
import monitor


class TestMonitor(BaseDumpsTestCase):
    """
    test some methods from the monitor module
    """
    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_cleanup_stale_batch_jobfiles(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        check that we can detect and remove stale batch jobfiles
        and mark any stale claimed batches as aborted
        """
        # set up a fake pagerangeinfo file
        fake_pageranges = [(1501, 4000, 1), (1, 1500, 1), (4001, 4321, 1), (4322, 4330, 1),
                           (4331, 4350, 2), (4351, 4380, 2), (4381, 4443, 2),
                           (4444, 4445, 3), (4446, 4600, 3), (4601, 4605, 3),
                           (4606, 5340, 4), (5341, 5344, 4)]
        pr_info = PageRangeInfo(self.wd['wiki'], enabled=True, fileformat="json",
                                error_callback=None, verbose=False)
        self.jobname = "meta-history"
        pr_info.update_pagerangeinfo(self.wd['wiki'], self.jobname, fake_pageranges)

        # set up batch file
        pcbatches = PageContentBatches(self.wd['wiki'], 'metahistorybz2dump', 2)
        pcbatches.create()

        # claim one but don't do it.
        batch_range = pcbatches.batchesfile.claim()
        pcbatches.set_batchrange("p" + batch_range[0] + "p" + batch_range[1])
        # make the batch job file that goes with it
        pcbatches.create_batchfile(pcbatches.batchrange)

        # set the stale age to something tiny
        self.config.batchjobs_stale_age = 10
        # this should do nothing, make sure that's what happens (not stale yet)
        monitor.cleanup_stale_batch_jobfiles(self.config, [self.wd['wiki'].db_name])

        path = pcbatches.get_batchrange_filepath(pcbatches.batchrange)

        with self.subTest('try to abort before stale'):
            self.assertEqual(os.path.exists(path), True)

            batchesfilepath = pcbatches.batchesfile.get_path()
            with open(batchesfilepath, "r") as fhandle:
                contents = fhandle.read()
                batches_info = json.loads(contents)
                batch_entry_range_start = batches_info['batches'][0]['batch']['range']['start']
                self.assertEqual(batch_entry_range_start, '1')
                batch_entry_status = batches_info['batches'][0]['batch']['status']
                self.assertEqual(batch_entry_status, 'claimed')

        # sleep some so the thing can get stale
        time.sleep(15)
        # this should clean things up and mark the range as aborted
        monitor.cleanup_stale_batch_jobfiles(self.config, [self.wd['wiki'].db_name])

        with self.subTest('try to abort after stale'):
            self.assertEqual(os.path.exists(path), False)

            batchesfilepath = pcbatches.batchesfile.get_path()
            with open(batchesfilepath, "r") as fhandle:
                contents = fhandle.read()
                batches_info = json.loads(contents)
                batch_entry_range_start = batches_info['batches'][0]['batch']['range']['start']
                self.assertEqual(batch_entry_range_start, '1')
                batch_entry_status = batches_info['batches'][0]['batch']['status']
                self.assertEqual(batch_entry_status, 'aborted')


if __name__ == '__main__':
    unittest.main()
