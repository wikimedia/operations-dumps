#!/usr/bin/python3
"""
test suite for cgroup-derived 7za flags (T436913)
"""
import os
import shutil
import tempfile
import unittest
from dumps.utils import MiscUtils


class TestSevenzipCgroupFlags(unittest.TestCase):
    """
    check that 7za -mmemuse/-mmt switches are derived correctly from
    cgroup (v1 and v2) memory and cpu limit files, and omitted when no
    limit applies
    """
    GIB = 1 << 30

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.saved = (MiscUtils.CGROUP_MEM_PATHS, MiscUtils.CGROUP_CPU_PATHS)
        MiscUtils.CGROUP_MEM_PATHS = []
        MiscUtils.CGROUP_CPU_PATHS = []

    def tearDown(self):
        MiscUtils.CGROUP_MEM_PATHS, MiscUtils.CGROUP_CPU_PATHS = self.saved
        shutil.rmtree(self.tempdir)

    def fake_cgroup_file(self, name, content):
        """write a fake cgroup limit file, return its path"""
        path = os.path.join(self.tempdir, name)
        with open(path, "w") as fhandle:
            fhandle.write(content)
        return [path]

    def test_memuse_from_v2_limit(self):
        """12Gi cgroup v2 limit yields 65 percent, rounded down, in gigs"""
        MiscUtils.CGROUP_MEM_PATHS = self.fake_cgroup_file(
            "memory.max", str(12 * self.GIB))
        self.assertEqual(MiscUtils.sevenzip_memuse(), "-mmemuse=7g")

    def test_memuse_unlimited(self):
        """cgroup v2 'max', the v1 sentinel, and no file all mean no flag"""
        for content in ["max", "9223372036854771712"]:
            MiscUtils.CGROUP_MEM_PATHS = self.fake_cgroup_file(
                "memory.max", content)
            self.assertEqual(MiscUtils.sevenzip_memuse(), "")
        MiscUtils.CGROUP_MEM_PATHS = []
        self.assertEqual(MiscUtils.sevenzip_memuse(), "")

    def test_memuse_never_below_one_gig(self):
        """tiny limits still produce a sane minimum of 1g"""
        MiscUtils.CGROUP_MEM_PATHS = self.fake_cgroup_file(
            "memory.max", str(self.GIB // 2))
        self.assertEqual(MiscUtils.sevenzip_memuse(), "-mmemuse=1g")

    def test_mmt_from_v2_quota(self):
        """cgroup v2 'quota period' yields threads, rounding up"""
        for content, expected in [("400000 100000", "-mmt=4"),
                                  ("1000000 100000", "-mmt=10"),
                                  ("50000 100000", "-mmt=1")]:
            MiscUtils.CGROUP_CPU_PATHS = self.fake_cgroup_file(
                "cpu.max", content)
            self.assertEqual(MiscUtils.sevenzip_mmt(), expected)

    def test_mmt_from_v1_quota(self):
        """cgroup v1 quota in us against the default 100ms period"""
        MiscUtils.CGROUP_CPU_PATHS = self.fake_cgroup_file(
            "cpu.cfs_quota_us", "800000")
        self.assertEqual(MiscUtils.sevenzip_mmt(), "-mmt=8")

    def test_mmt_unlimited(self):
        """cgroup v2 'max', the v1 -1 sentinel, and no file all mean no flag"""
        for content in ["max 100000", "-1"]:
            MiscUtils.CGROUP_CPU_PATHS = self.fake_cgroup_file(
                "cpu.max", content)
            self.assertEqual(MiscUtils.sevenzip_mmt(), "")
        MiscUtils.CGROUP_CPU_PATHS = []
        self.assertEqual(MiscUtils.sevenzip_mmt(), "")

    def test_combined_flags(self):
        """both, one, or neither flag as the respective limits exist"""
        MiscUtils.CGROUP_MEM_PATHS = self.fake_cgroup_file(
            "memory.max", str(16 * self.GIB))
        MiscUtils.CGROUP_CPU_PATHS = self.fake_cgroup_file(
            "cpu.max", "800000 100000")
        self.assertEqual(MiscUtils.sevenzip_cgroup_flags(),
                         "-mmemuse=10g -mmt=8")
        MiscUtils.CGROUP_CPU_PATHS = []
        self.assertEqual(MiscUtils.sevenzip_cgroup_flags(), "-mmemuse=10g")
        MiscUtils.CGROUP_MEM_PATHS = []
        self.assertEqual(MiscUtils.sevenzip_cgroup_flags(), "")


if __name__ == '__main__':
    unittest.main()
