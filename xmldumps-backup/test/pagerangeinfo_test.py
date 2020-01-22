#!/usr/bin/python3
"""
test suite for pagerangeinfo module
"""
import unittest
from test.basedumpstest import BaseDumpsTestCase
from dumps.pagerangeinfo import PageRangeInfo


class TestPageRangeInfo(BaseDumpsTestCase):
    """
    test read and update of pagerangeinfo file
    """
    def test_pagerangeinfo_update_and_get(self):
        """
        check that unsorted page ranges with dup entries are properly
        filtered and sorted before being written to the pagerangeinfo file
        and are retrieved as tuples after JSON turns them into arrays
        """
        good_ranges = [(1, 20, 1), (21, 500, 2), (501, 620, 3), (621, 780, 3), (781, 900, 4),
                       (901, 1283, 4), (1284, 4302, 4)]
        bad_ranges = good_ranges[:]
        bad_ranges.extend([(781, 900, 4), (21, 500, 2)])
        pr_info = PageRangeInfo(self.en['wiki'], enabled=True, fileformat="json",
                                error_callback=None, verbose=False)

        # can store and read back an unsorted page range with dups and
        # get sorted deduped ranges out
        pr_info.update_pagerangeinfo(self.en['wiki'], self.jobname, bad_ranges)
        new_ranges = pr_info.get_pagerange_info(self.en['wiki'])
        self.assertEqual(new_ranges, {self.jobname: good_ranges})

        self.interim_cleanup()

        # can store an empty entry and get back an empty entry
        pr_info.update_pagerangeinfo(self.en['wiki'], self.jobname, None)
        new_ranges = pr_info.get_pagerange_info(self.en['wiki'])
        self.assertEqual(new_ranges, {self.jobname: []})


if __name__ == '__main__':
    unittest.main()
