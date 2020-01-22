#!/usr/bin/python3
"""
text suite for intervals module
"""
import unittest
import dumps.dumpitemlist
import dumps.pagerange
import dumps.intervals


class TestIntervals(unittest.TestCase):
    """
    tests of page range manipulation used for xml content job production
    """
    def test_get_preceding_intervals(self):
        """
        verify that given a list of all grouped intervals,
        an interval 'have' and an interval 'wanted',
        we will return a list of grouped intervals in the 'wanted' range
        for all values up to the first value in 'have'
        """
        all_intervals = [(1, 100, 1), (101, 300, 2),
                         (301, 600, 3), (601, 3400, 4)]

        wanted = (1, 100, 1)
        have = (49, 65, 1)
        expected_missing = [(1, 48, 1)]
        with self.subTest("some values missing from 'wanted' before 'have' in single group (1)"):
            missing_ranges = dumps.intervals.get_preceding_intervals(
                wanted, have, all_intervals)
            self.assertEqual(missing_ranges, expected_missing)

        have = (250, 275, 2)
        expected_missing = [(1, 100, 1), (101, 249, 2)]
        with self.subTest("some values missing from 'wanted' before 'have' consecutively in groups 1,2"):
            missing_ranges = dumps.intervals.get_preceding_intervals(
                wanted, have, all_intervals)
            self.assertEqual(missing_ranges, expected_missing)

        have = (1, 50, 1)
        with self.subTest("no values missing from 'wanted' before 'have'"):
            missing_ranges = dumps.intervals.get_preceding_intervals(
                wanted, have, all_intervals)
            self.assertEqual(missing_ranges, None)

    def test_get_group_for_value(self):
        """
        given a number n and a list of tuples (a, b, m),
        make sure that the m's are unique, and that we return the
        m from the tuple in the list where n is in (a,b)
        or None if there's no match
        """
        intervals = [(1, 100, 1), (101, 175, 2), (176, 340, 3)]

        with self.subTest("regular match"):
            group = dumps.intervals.get_group_for_value(130, intervals)
            self.assertEqual(group, 2)

        with self.subTest("no match"):
            group = dumps.intervals.get_group_for_value(400, intervals)
            self.assertEqual(group, None)

        intervals.append((341, 380, 3))
        with self.subTest("groups not unique"):
            self.assertRaises(ValueError, dumps.intervals.get_group_for_value,
                              130, intervals)

    def test_get_endval_for_group(self):
        """
        verify that given a list of grouped intervals, with possibly multiple
        ranges for any given group, we return the largest end value
        from intervals for that group, or None if there aren't any
        """
        needed_ranges = [(1, 20, 1), (21, 40, 2), (41, 67, 2), (68, 100, 3)]

        # multiple intervals, return the right end val
        with self.subTest("multiple intervals, return the right end val"):
            endpage = dumps.intervals.get_endval_for_group(2, needed_ranges)
            self.assertEqual(endpage, 67)

        with self.subTest("no intervals for that group"):
            endpage = dumps.intervals.get_endval_for_group(4, needed_ranges)
            self.assertEqual(endpage, None)

    def test_get_intervals_by_group_upto_val(self):
        """
        verify that given a group num and a max value, we can return a list of
        the grouped intervals (int1, int2, group) with the same group with
        ranges up to and including the max val
        """
        intervals = [(1, 20, 1), (21, 40, 2), (41, 67, 2),
                     (68, 100, 2), (101, 150, 3)]
        group = 2

        expected_intervals = [(21, 40, 2), (41, 45, 2)]
        with self.subTest("maxval in middle of a range"):
            returned_intervals = dumps.intervals.get_intervals_by_group_upto_val(
                45, group, intervals)
            self.assertEqual(returned_intervals, expected_intervals)

        expected_intervals = [(21, 40, 2), (41, 41, 2)]
        with self.subTest("maxval at start of a range"):
            returned_intervals = dumps.intervals.get_intervals_by_group_upto_val(
                41, group, intervals)
            self.assertEqual(returned_intervals, expected_intervals)

        expected_intervals = [(21, 40, 2), (41, 67, 2)]
        with self.subTest("maxval at end of a range"):
            returned_intervals = dumps.intervals.get_intervals_by_group_upto_val(
                67, group, intervals)
            self.assertEqual(returned_intervals, expected_intervals)

        group = 3
        with self.subTest("group not in the interval list"):
            returned_intervals = dumps.intervals.get_intervals_by_group_upto_val(
                41, group, intervals)
            self.assertEqual(returned_intervals, [])

    def test_merge_ranges(self):
        """
        checks that if a series of tuples of the form
        (startnum, endnum, <maybe other stuff>) is passed in, the output
        will be tuples with just the first two fields, and with any
        consecutive tuples merged into one (i.e. [(1,5), (6, 20), (23, 44)]
        would be merged into [(1, 20), (23, 44)]
        """
        to_merge = [(21, 34), (35, 500), (621, 780), (550, 600), (501, 549), (601, 620),
                    (1284, 1290), (1291, 4302), (4500, 4560)]
        expected_pageranges = [(21, 780), (1284, 4302), (4500, 4560)]
        with self.subTest("regular merge"):
            merged_pageranges = dumps.intervals.merge_ranges(to_merge)
            self.assertEqual(merged_pageranges, expected_pageranges)

        # expect everything after first two elements in
        # each tuple to be tossed
        to_merge = [(21, 34, 1), (35, 500, 1), (621, 780, 3), (550, 600, 2), (501, 549, 2),
                    (601, 620, 4), (1284, 1290, 3), (1291, 4302, 3), (4500, 4560, 8)]
        with self.subTest("merge grouped intervals"):
            merged_pageranges = dumps.intervals.merge_ranges(to_merge)
            self.assertEqual(merged_pageranges, expected_pageranges)

    def test_filter_ranges(self):
        """
        checks that we can filter one list of intervals ('wanted') against another list
        ('done') and get a list of ranges that are not yet done
        """
        # 'done'
        done = [(21, 34), (35, 500), (621, 780), (550, 600), (501, 549), (601, 620),
                (1284, 1290), (1291, 4302)]
        # 'wanted', note that intervals here may not be split up the same way as 'done'
        wanted = [(1284, 4302), (1, 20), (21, 500), (501, 620), (621, 780), (781, 900),
                  (901, 1283)]
        # not yet done...
        expected_ranges = [(1, 20), (781, 900), (901, 1283)]
        filtered_ranges = dumps.intervals.filter_ranges(wanted, done)
        self.assertEqual(filtered_ranges, expected_ranges)

    def test_get_covered_ranges(self):
        """
        checks that given a list of intervals ('possibles') against another list
        ('have') we can get a list of ranges that are covered by the ranges in 'have'
        """
        # 'have'
        have = [(21, 34), (35, 500), (621, 780), (550, 600), (501, 549), (601, 620),
                (1284, 1290), (1291, 4302)]
        # 'possibles', note that intervals here may not be split up the same way as 'have'
        possibles = [(1284, 4302), (1, 20), (21, 500), (501, 620), (621, 780), (781, 900),
                     (901, 1283)]
        expected_ranges = [(1, 20), (781, 900), (901, 1283)]
        expected_ranges = [(21, 500), (501, 620), (621, 780), (1284, 4302)]
        covered_ranges = dumps.intervals.get_covered_ranges(possibles, have)
        self.assertEqual(covered_ranges, expected_ranges)

    def test_find_missing_ranges(self):
        """
        check that, when provided a list of tuples (startid, endid, partnum)
        of stubs (i.e. of all page content we want to produce) and a list
        of similar tuples of page content already produced, that we get
        back a list of tuples that cover just the missing page content ranges
        """
        wanted = [(1, 20, 1), (21, 100, 2), (101, 150, 3)]
        have = [(43, 50, 2), (53, 58, 2), (101, 130, 3)]
        expected_missing = [(1, 20, 1), (21, 42, 2), (51, 52, 2),
                            (59, 100, 2), (131, 150, 3)]
        missing = dumps.intervals.find_missing_ranges(wanted, have)
        self.assertEqual(missing, expected_missing)


if __name__ == '__main__':
    unittest.main()
