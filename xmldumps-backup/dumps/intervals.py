#!/usr/bin/python3
"""
methods for handling intervals and ranges
"""


def convert_intervals_to_ints(intervals):
    """
    given a list of intervals, convert all the elements
    to ints and return that
    """
    if intervals is None:
        return None
    to_return = []
    for entry in intervals:
        fields = list(entry)
        to_return.append(tuple([int(field) for field in fields]))
    return to_return


def interval_overlaps(first_a, last_a, first_b, last_b):
    """
    return True if there is overlap between the two ranges,
    False otherwise
    for purposes of checking, if last in either range is None,
    consider it to be after first in both ranges
    """
    if (first_a <= first_b and
            (last_a is None or
             last_a >= first_b)):
        return True
    if (first_a >= first_b and
            (last_b is None or
             first_a <= last_b)):
        return True
    return False


def unbounded_intervals_overlap(first, second):
    """
    given two ranges of numbers where first or second
    pair has the endpoint missing (set to 0) and therefore
    presumed to be infinity, compare and return
    True if overlap, False otherwise
    """
    # one or both end values are missing:
    if not first[1] and not second[1]:
        return True
    if not first[1] and second[1] < first[0]:
        return True
    if not second[1] and first[1] < second[0]:
        return True
    return False


def bounded_intervals_overlap(first, second):
    """
    given two ranges of numbers, compare and return
    True if overlap, False otherwise
    """
    # should we put a min 10 pages at one end or the other?
    if first[0] <= second[0] and second[0] <= first[1]:
        return True
    if second[0] <= first[0] and first[0] <= second[1]:
        return True
    return False


def chkptfile_in_pagerange(dfname, chkpt_dfname):
    """
    return False if both files are checkpoint files (with page ranges)
    and the second file page range does not overlap with the first one

    args: DumpFilename, checkpoint file DumpFilename
    """
    # one or both are not both checkpoint files, default to 'true'
    if not dfname.is_checkpoint_file or not chkpt_dfname.is_checkpoint_file:
        return True

    if not dfname.last_page_id or not chkpt_dfname.last_page_id:
        # one or both end values are missing (0):
        return unbounded_intervals_overlap(
            (dfname.first_page_id_int, dfname.last_page_id_int),
            (chkpt_dfname.first_page_id_int, chkpt_dfname.last_page_id_int))
    # have end values for both files:
    return bounded_intervals_overlap(
        (dfname.first_page_id_int, dfname.last_page_id_int),
        (chkpt_dfname.first_page_id_int, chkpt_dfname.last_page_id_int))


def get_group_for_value(value, intervals):
    """
    given a value and a series of grouped intervals (int1, int2, group),
    where the group number is distinct in each tuple, return the group num
    where the value is in the range (int1, int2), or None if there are none
    """
    groupnums = [interval[2] for interval in intervals]
    if len(intervals) != len(list(set(groupnums))):
        raise ValueError
    for entry in intervals:
        if entry[0] <= value <= entry[1]:
            return entry[2]
    return None


def get_group_preceding_value(value, intervals):
    """
    given a value and a series of grouped intervals (int1, int2, group),
    where the group number is distinct in each tuple, return the group num
    of the interval preceding the one where the value is in the range
    (int1, int2) or less than the range, or None if there are none
    """
    groupnums = [interval[2] for interval in intervals]
    if len(intervals) != len(list(set(groupnums))):
        raise ValueError
    current_group = None
    for entry in intervals:
        if value <= entry[1]:
            return current_group
        current_group = entry[2]
    return None


def get_endval_for_group(group, needed_ranges):
    """
    given a group num, and a series of grouped intervals (int1, int2, group),
    return the greatest end value for intervals with that group
    or None if there are none
    """
    last_id = 0
    for entry in needed_ranges:
        if entry[2] == group and entry[1] > last_id:
            last_id = entry[1]
    if not last_id:
        return None
    return last_id


def get_intervals_by_group(group, intervals):
    """
    given a group num, and a list of grouped intervals (int1, int2, group),
    return all the intervals in order with that group
    or an empty list if there are none
    """
    return [entry for entry in intervals if entry[2] == group]


def get_intervals_by_group_upto_val(maxval, group, intervals):
    """
    given a group num, and a list of intervals (int1, int2, group),
    return all the intervals in order with that group up to the specified
    maxval, including constructing an interval to cover the last range
    if needed, or an empty list if there are none
    """
    to_return = [entry for entry in intervals if entry[2] == group and
                 int(maxval) >= int(entry[1])]
    # get any partial range if there is one
    for entry in intervals:
        if entry[2] == group and int(maxval) >= int(entry[0]) and int(maxval) < int(entry[1]):
            to_return.append((entry[0], maxval, group))
    return to_return


def get_preceding_intervals(wanted, have, all_intervals):
    """
    given interval wanted and interval we have,
    and list of all grouped intervals,
    return interval covering the range in 'all' before first interval we have,
    (split into intervals if the range spans groups)
    or None if none
    args:
        grouped interval (int1, int2, group) for 'wanted',
        grouped interval for 'have',
        list of grouped intervals for 'all_intervals'
    """
    if have is None:
        return [wanted]
    if wanted is None or have[0] <= int(wanted[0]):
        return None
    group_for_start = get_group_for_value(wanted[0], all_intervals)
    group_for_end = get_group_for_value(have[0] - 1, all_intervals)
    if group_for_end is None:
        group_for_end = get_group_preceding_value(have[0], all_intervals)
    if group_for_end != group_for_start:
        to_return = []
        to_return.append(
            (wanted[0], get_endval_for_group(group_for_start, all_intervals), group_for_start))
        for group_middle in range(group_for_start + 1, group_for_end):
            to_return.extend(get_intervals_by_group(group_middle, all_intervals))
        to_return.extend(get_intervals_by_group_upto_val(
            have[0] - 1, group_for_end, all_intervals))
    else:
        to_return = [(wanted[0], have[0] - 1, wanted[2])]
    return to_return


def find_missing_ranges(needed, have):
    """
    given lists of grouped intervals (int1 int2, groupnum) needed,
    and grouped intervals we have,
    return a list of grouped intervals we need and don't have
    args:
        sorted asc list of grouped intervals needed,
        sorted asc list of grouped intervals already have,
    returns: list of grouped intervals
    """
    needed_index = 0
    have_index = 0
    missing = []

    if not needed:
        return missing
    if not have:
        return needed

    needed_interval = needed[needed_index]
    have_interval = have[have_index]

    while True:
        # if we're out of haves, append everything we need
        if have_interval is None:
            missing.append(needed_interval)
            needed_index += 1
            if needed_index >= len(needed):
                # end of needed. done
                return missing
            needed_interval = needed[needed_index]

        before_have = get_preceding_intervals(needed_interval, have_interval, needed)
        # write anything we don't have
        if before_have is not None:
            missing.extend(before_have)
        # if we haven't already exhausted all the ranges we have...
        if have_interval is not None:
            # skip over the current range of what we have
            skip_up_to = have_interval[1] + 1
            while needed_interval[1] < skip_up_to:
                needed_index += 1
                if needed_index >= len(needed):
                    # end of needed. done
                    return missing
                needed_interval = needed[needed_index]

            if needed_interval[0] < skip_up_to:
                needed_interval = (skip_up_to, needed_interval[1], needed_interval[2])
            # get the next range we have
            have_index += 1
            if have_index < len(have):
                have_interval = have[have_index]
            else:
                have_interval = None

    return missing


def merge_ranges(intervals):
    """
    given a list of intervals (int1, int2, <ignored if present>), combine any
    intervals that cover consecutive ranges and return the new list
    WITH ONLY the first two elements of every tuple however, we toss
    anything else.
    """
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: int(x[0]))
    to_return = []
    start = None
    end = None
    for interval in intervals:
        if start is None:
            start = interval[0]
            end = interval[1]
        elif interval[0] <= end + 1 and interval[0] >= start:
            # this interval is right after the last one we looked at or
            # overlaps it, so merge it in
            end = interval[1]
        else:
            to_return.append((start, end))
            start = interval[0]
            end = interval[1]
    if start:
        to_return.append((start, end))
    return to_return


# unused
def filter_ranges(intervals_to_filter, intervals_have):
    """
    given two lists of intervals (int1, int2, <ignored if present>), return
    the ranges in the first list that are NOT covered by ranges in the second list
    example:
       (2, 200, *) is in [(1,2, *), (2,50, *), (50,500, *)]
        but not in [(30,600, *), ...]
    where the '*' are ignored values
    """
    to_remove = []
    covered = merge_ranges(intervals_have)
    intervals_to_filter = sorted(intervals_to_filter, key=lambda x: int(x[0]))
    for interval in intervals_to_filter:
        for entry in covered:
            if interval[0] >= entry[0] and interval[1] <= entry[1]:
                to_remove.append(interval)
    return [interval for interval in intervals_to_filter
            if interval not in to_remove]


def get_covered_ranges(intervals_to_filter, intervals_missing):
    """
    given two lists of intervals (int1, int2, <ignored if present>), return
    the ranges in the first list that are covered by ranges in the second list
    example:
       (2, 200, *) is in [(1,2, *), (2,50, *), (50,500, *)]
        but not in [(30,600, *), ...]
    where the '*' are ignored values
    """
    to_keep = []
    covered = merge_ranges(intervals_missing)
    intervals_to_filter = sorted(intervals_to_filter, key=lambda x: int(x[0]))
    for interval in intervals_to_filter:
        for entry in covered:
            if interval[0] >= entry[0] and interval[1] <= entry[1]:
                to_keep.append(interval)
    return [interval for interval in intervals_to_filter
            if interval in to_keep]
