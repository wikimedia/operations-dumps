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

    def set_checkpt_filenames(self, pagerange_strings, wiki, date=None, shuffle=True, stubs=False):
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
        if date is None:
            date = self.today
        for partnum in pagerange_strings:
            for rangestring in pagerange_strings[partnum]:
                checkpt_filenames.append(
                    "{wiki}-{date}{body}{partnum}.xml-{rangestring}{ext}".format(
                        wiki=wiki.db_name, date=date, body=body,
                        partnum=partnum, rangestring=rangestring, ext=ext))
        if shuffle:
            random.shuffle(checkpt_filenames)
        dfnames = self.dfnames_from_filenames(checkpt_filenames)
        return dfnames

    @staticmethod
    def setup_xml_files_chkpts(todos, date, excluded=None):
        """
        make copies of our sample stub and page content files in the right
        directory with the right names
        'excluded' contains the list of pageranges not to copy in, if desired
        """
        parts_pageranges = {'1': ['p1p1500', 'p1501p4000', 'p4001p4321', 'p4322p4330'],
                            '2': ['p4331p4350', 'p4351p4380', 'p4381p4443'],
                            '3': ['p4444p4445', 'p4446p4600', 'p4601p4605'],
                            '4': ['p4606p5340', 'p5341p5345']}

        if 'stub' in todos:
            for part in parts_pageranges:
                inpath = './test/files/stub-articles-sample' + part + '.xml.gz'
                basefilename = ('wikidatawiki-{date}-stub-articles'.format(date=date) +
                                part + '.xml.gz')
                outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                       date, basefilename)
                shutil.copyfile(inpath, outpath)

        if 'content' in todos:
            for part in parts_pageranges:
                for pagerange in parts_pageranges[part]:
                    if excluded and pagerange in excluded:
                        continue
                    inpath = ('./test/files/pages-articles-sample' + part + '.xml-' +
                              pagerange + '.bz2')
                    # wikidatawiki-20200205-pages-articles1.xml-p1p100.bz2
                    basefilename = ('wikidatawiki-{date}-pages-articles'.format(date=date) +
                                    part + '.xml-' + pagerange + '.bz2')
                    outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                           date, basefilename)
                    shutil.copyfile(inpath, outpath)

    @staticmethod
    def setup_xml_files_parts(todos, date, excluded=None):
        """
        make copies of our sample stub and page content files in the right
        directory with the right names
        'excluded' contains the list of parts not to copy in, if desired
        """
        parts = ['1', '2', '3', '4']

        if 'stub' in todos:
            for part in parts:
                inpath = './test/files/stub-articles-sample' + part + '.xml.gz'
                basefilename = ('wikidatawiki-{date}-stub-articles'.format(date=date) +
                                part + '.xml.gz')
                outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                       date, basefilename)
                shutil.copyfile(inpath, outpath)

        if 'content' in todos:
            for part in parts:
                if excluded and part in excluded:
                    continue
                inpath = ('./test/files/pages-articles-sample' + part + '.xml.bz2')
                # wikidatawiki-20200205-pages-articles1.xml.bz2
                basefilename = ('wikidatawiki-{date}-pages-articles'.format(date=date) +
                                part + '.xml.bz2')
                outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                       date, basefilename)
                shutil.copyfile(inpath, outpath)

    @staticmethod
    def setup_xml_files_noparts(todos, date):
        """
        make copies of our sample stub and page content files in the right
        directory with the right names
        """
        if 'stub' in todos:
            inpath = './test/files/stub-articles-sample.xml.gz'
            basefilename = ('wikidatawiki-{date}-stub-articles'.format(date=date) +
                            '.xml.gz')
            outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                   date, basefilename)
            shutil.copyfile(inpath, outpath)

        if 'content' in todos:
            inpath = ('./test/files/pages-articles-sample.xml.bz2')
            # wikidatawiki-20200205-pages-articles1.xml.bz2
            basefilename = ('wikidatawiki-{date}-pages-articles'.format(date=date) +
                            '.xml.bz2')
            outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                   date, basefilename)
            if os.path.exists(inpath):
                shutil.copyfile(inpath, outpath)
            else:
                # in some cases we don't care about content but we do care
                # that the file exists
                with open(outpath, "w") as outfile:
                    outfile.write("<mediawiki></mediawiki>")

    @staticmethod
    def setup_stub_history_files(date):
        """
        make copies of our sample stub history file in the right
        directory with the right name, then copy the other sample
        stub parts and pretend they are history ones and the same wiki, heh
        """
        inpath = './test/files/stub-history-sample.xml.gz'
        basefilename = ('wikidatawiki-{date}-stub-meta-history'.format(date=date) +
                        '.xml.gz')
        outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                               date, basefilename)
        shutil.copyfile(inpath, outpath)

        parts = ['1', '2', '3', '4']

        for part in parts:
            inpath = './test/files/stub-articles-sample' + part + '.xml.gz'
            basefilename = ('wikidatawiki-{date}-stub-meta-history'.format(date=date) +
                            part + '.xml.gz')
            outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, 'wikidatawiki',
                                   date, basefilename)
            shutil.copyfile(inpath, outpath)


if __name__ == '__main__':
    unittest.main()
