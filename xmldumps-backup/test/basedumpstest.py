#!/usr/bin/python3
"""
base class for dumps tests
"""
import os
import random
import shutil
import unittest
from dumps.wikidump import Wiki, Config
from dumps.utils import TimeUtils
from dumps.fileutils import DumpDir, DumpFilename


class BaseDumpsTestCase(unittest.TestCase):
    """
    base class with setup and teardown methods
    for dumps tests
    """
    PUBLICDIR = 'test/output/public'
    PRIVATEDIR = 'test/output/private'
    TEMPDIR = 'test/output/temp'

    def setUp(self):
        """
        make the output directory, make the directory for today's date
        """
        self.today = TimeUtils.today()

        # make dump output dirs
        for wikiname in ['enwiki', 'wikidatawiki']:
            fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname, self.today)
            if not os.path.exists(fullpath):
                os.makedirs(fullpath)
            fullpath = os.path.join(BaseDumpsTestCase.PRIVATEDIR, wikiname, self.today)
            if not os.path.exists(fullpath):
                os.makedirs(fullpath)
        if not os.path.exists(BaseDumpsTestCase.TEMPDIR):
            os.makedirs(BaseDumpsTestCase.TEMPDIR)

        # set up the config and wiki objects
        self.config = Config('./test/wikidump.conf.test:bigwikis')
        self.en = {}  # pylint: disable=invalid-name
        self.en['wiki'] = Wiki(self.config, 'enwiki')
        self.en['wiki'].set_date(self.today)
        self.en['dump_dir'] = DumpDir(self.en['wiki'], self.en['wiki'].db_name)

        self.wd = {}  # pylint: disable=invalid-name
        self.wd['wiki'] = Wiki(self.config, 'wikidatawiki')
        self.wd['wiki'].set_date(self.today)
        self.wd['dump_dir'] = DumpDir(self.wd['wiki'], self.wd['wiki'].db_name)
        self.jobname = 'articlesdump'

    def interim_cleanup(self):
        """
        cleanup output files in between tests
        """
        for wikiname in ['enwiki', 'wikidatawiki']:
            fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname, self.today)
            shutil.rmtree(fullpath)
            os.makedirs(fullpath)

        shutil.rmtree(BaseDumpsTestCase.TEMPDIR)
        os.makedirs(BaseDumpsTestCase.TEMPDIR)

    def tearDown(self):
        """
        clean up any crap we wrote for these tests
        """
        for wikiname in ['enwiki', 'wikidatawiki']:
            shutil.rmtree(os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname))
            shutil.rmtree(os.path.join(BaseDumpsTestCase.PRIVATEDIR, wikiname))

        shutil.rmtree(BaseDumpsTestCase.TEMPDIR)

    def dfnames_from_filenames(self, filenames):
        """
        given a list of filenames, generate and
        return a list of the corresponding DumpFilenames
        """
        dfnames = []
        for filename in filenames:
            dfname = DumpFilename(self.en['wiki'])
            dfname.new_from_filename(filename)
            dfnames.append(dfname)
            # might as well check this, if it's not true then the basis for
            # the rest of the test is bogus
            self.assertEqual(dfname.filename, filename)
        return dfnames

    def set_checkpt_filenames(self, pagerange_strings, wiki, shuffle=True, stubs=False):
        """
        given a dict of of checkpoint file strings (pxxxpyyy) and their corresponding
        partnums, put together a list of DumpFilenames for the page content dumps and
        return them
        """
        checkpt_filenames = []
        if stubs:
            body = "-stub-articles"
            ext = ".gz"
        else:
            body = "-pages-articles"
            ext = ".bz2"
        for partnum in pagerange_strings:
            for rangestring in pagerange_strings[partnum]:
                checkpt_filenames.append(
                    "{wiki}-{date}{body}{partnum}.xml-{rangestring}{ext}".format(
                        wiki=wiki.db_name, date=self.today, body=body,
                        partnum=partnum, rangestring=rangestring, ext=ext))
        if shuffle:
            random.shuffle(checkpt_filenames)
        dfnames = self.dfnames_from_filenames(checkpt_filenames)
        return dfnames


if __name__ == '__main__':
    unittest.main()
