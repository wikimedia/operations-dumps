#!/usr/bin/python3
"""
test suite for xml content job
"""
import os
import unittest
from test.basedumpstest import BaseDumpsTestCase
from dumps.wikidump import Wiki
from dumps.fileutils import DumpFilename, DumpDir


class TestFileUtils(BaseDumpsTestCase):
    """
    tests for classes in the fileutils module
    """
    def test_dumpdir_public_path(self):
        """
        verify that DumpDir.filename_public_path gives the private
        path for wikis configured as private
        """
        wiki = Wiki(self.config, 'badwiki')
        wiki.set_date(self.today)

        dumpdir = DumpDir(wiki, wiki.db_name)
        filename = 'badwiki-{today}-pages-articles.xml.bz2'.format(today=self.today)
        expected_path = os.path.join(BaseDumpsTestCase.PRIVATEDIR, 'badwiki', self.today, filename)
        dfname = DumpFilename(wiki)
        dfname.new_from_filename(filename)
        private_path = dumpdir.filename_public_path(dfname)
        self.assertEqual(private_path, expected_path)

    def test_dumpfilename_eq(self):
        """
        make sure the __eq__ method for DumpFilename works as we expect
        """
        some_filenames = [
            'enwiki-{today}-pages-articles2.xml-p101p134.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles2.xml-p186p202.bz2'.format(today=self.today),
            'enwiki-{today}-pages-articles2.xml-p296p300.bz2'.format(today=self.today)]

        some_dfnames = []
        for filename in some_filenames:
            dfname = DumpFilename(self.en['wiki'])
            dfname.new_from_filename(filename)
            some_dfnames.append(dfname)
        same_dfnames = []
        for filename in some_filenames:
            dfname = DumpFilename(self.en['wiki'])
            dfname.new_from_filename(filename)
            same_dfnames.append(dfname)

        self.assertEqual(some_dfnames, same_dfnames)

        some_filename = 'enwiki-{today}-pages-articles2.xml-p186p202.bz2'.format(
            today=self.today)
        some_dfname = DumpFilename(self.en['wiki'])
        some_dfname.new_from_filename(some_filename)

        for other_filename in [
                "enwiki-{today}-pages-articles3.xml-p186p202.bz2".format(today=self.today),
                "enwiki-{today}-pages-articles2.xml-p186p203.bz2".format(today=self.today),
                "elwiki-{today}-pages-articles2.xml-p186p202.bz2".format(today=self.today),
                "elwiki-{today}-pages-articles2.xml-p186p202.gz".format(today=self.today)]:
            other_dfname = DumpFilename(self.en['wiki'])
            other_dfname.new_from_filename(other_filename)
            self.assertFalse(some_dfname == other_dfname)

        other_dfname = None
        self.assertFalse(some_dfname == other_dfname)


if __name__ == '__main__':
    unittest.main()
