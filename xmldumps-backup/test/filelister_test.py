#!/usr/bin/python3
"""
text suite for various methods that list files,
across all jobs
"""
import os
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
import dumps.dumpitemlist
import dumps.wikidump
import dumps.xmlcontentjobs
from dumps.xmlcontentjobs import XmlDump
from dumps.recompressjobs import XmlMultiStreamDump, XmlRecompressDump
from dumps.utils import FilePartInfo
from dumps.xmljobs import XmlStub
from dumps.recombinejobs import RecombineXmlMultiStreamDump
# from dumps.fileutils import DumpFilename


class TestFileLister(BaseDumpsTestCase):
    """
    some basic tests for listing output file for various dump
    jobs, for publishing, checking for truncation, etc
    """
    def setup_empty_xml_files_parts(self, wiki, partnums, jobname, file_ext,
                                    truncated=False, inprog=False):
        """
        create empty fake dump xml files in output directory
        for testing, with part numbers
        """
        basedir = os.path.join(BaseDumpsTestCase.PUBLICDIR, wiki.db_name, self.today)
        for partnum in partnums:
            filename = "{wiki}-{date}-{jobname}{partnum}.xml.{ext}".format(
                wiki=wiki.db_name, date=self.today,
                jobname=jobname, partnum=partnum, ext=file_ext)
            path = os.path.join(basedir, filename)
            if truncated:
                path = path + '.truncated'
            elif inprog:
                path = path + '.inprog'
            with open(path, "w") as output:
                output.write("fake\n")

    def setup_empty_xml_file(self, wiki, jobname, file_ext,
                             truncated=False, inprog=False):
        """
        create single empty dump xml file in output directory
        for testing, with no part numbers
        """
        basedir = os.path.join(BaseDumpsTestCase.PUBLICDIR, wiki.db_name, self.today)
        filename = "{wiki}-{date}-{jobname}.xml.{ext}".format(
            wiki=wiki.db_name, date=self.today,
            jobname=jobname, ext=file_ext)
        path = os.path.join(basedir, filename)
        if truncated:
            path = path + '.truncated'
        elif inprog:
            path = path + '.inprog'
        with open(path, "w") as output:
            output.write("fake\n")

    def remove_empty_xml_files(self, wiki):
        '''
        remove any xml files in the wiki's public directory
        '''
        basedir = os.path.join(BaseDumpsTestCase.PUBLICDIR, wiki.db_name, self.today)
        for xml_file in os.listdir(basedir):
            if 'xml' in xml_file:
                os.unlink(os.path.join(basedir, xml_file))

    @staticmethod
    def dfsort(dfnames):
        '''one liner sort to avoid retyping this everywhere'''
        if dfnames:
            return sorted(dfnames, key=lambda thing: thing.filename)
        return dfnames

    @staticmethod
    def get_xmlstubs_job(wiki, partnum_todo, parts):
        '''return an XmlStub instance suitable for testing its list file methods'''
        return XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                       partnum_todo=partnum_todo,
                       jobsperbatch=dumps.dumpitemlist.get_int_setting(
                           wiki.config.jobsperbatch, "xmlstubsdump"),
                       parts=parts)

    @staticmethod
    def get_xmlcontent_type(shortname):
        '''
        XmlDump() requires a short name of the job and a long name; this
        method, given the short name, returns the long name
        It will need be kept in sync with the dump scripts if these values
        ever change
        '''
        if shortname == 'articles':
            return 'articlesdump'
        if shortname == 'meta-current':
            return 'metacurrentdump'
        if shortname == 'meta-history':
            return 'metahistorybz2dump'
        return None

    def get_xmlcontent_job(self, wiki, shortname, xmlstubs_job, partnum_todo, parts,
                           checkpoints, checkpoint_file):
        '''return an XmlDump instance suitable for testing its list file methods;
        this can be any of the article, meta-current or meta-history jobs'''
        return XmlDump(shortname, self.get_xmlcontent_type(shortname), "short description here",
                       "long description here",
                       item_for_stubs=xmlstubs_job, prefetch=False, prefetchdate=None,
                       spawn=True, wiki=wiki, partnum_todo=partnum_todo,
                       parts=parts,
                       checkpoints=checkpoints, checkpoint_file=checkpoint_file,
                       page_id_range=None, verbose=False)

    @staticmethod
    def get_articles_multistream_job(wiki, xmlarticles_job, partnum_todo, parts,
                                     checkpoints, checkpoint_file):
        '''return an XmlMultiStreamDump instance suitable for testing its list file methods'''
        return XmlMultiStreamDump("articles", "articlesmultistreamdump",
                                  "short description here",
                                  "long description here",
                                  xmlarticles_job, wiki=wiki,
                                  partnum_todo=partnum_todo, parts=parts,
                                  checkpoints=checkpoints, checkpoint_file=checkpoint_file)

    @staticmethod
    def get_recombine_xmlmultistream_job(articles_multistream_job):
        '''return a RecombineXmlMultiStreamDump instance suitable for testing
        its list file methods'''
        return RecombineXmlMultiStreamDump("articlesmultistreamdumprecombine",
                                           "short description here",
                                           articles_multistream_job)

    @staticmethod
    def get_xml_recompress_job(wiki, xmlcontent_job, partnum_todo,
                               checkpoints, checkpoint_file):
        '''return an XmlRecompressDump instance suitable for testing
        its list file methods; this only applies to meta-history content files'''
        filepartinfo = FilePartInfo(wiki, wiki.db_name)
        return XmlRecompressDump("meta-history", "metahistory7zdump",
                                 "short description here", "long description here",
                                 xmlcontent_job,
                                 wiki, partnum_todo=partnum_todo,
                                 parts=filepartinfo.get_attr('_pages_per_filepart_history'),
                                 checkpoints=checkpoints, checkpoint_file=checkpoint_file)

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_xmlstubs_lister(self, _mock_get_some_stats, _mock_get_known_tables):
        '''test the various list methods for stubs jobs, for parts, parts and checkpoints,
        and no parts.'''

        # first batch of tests

        stubs_job = self.get_xmlstubs_job(self.en['wiki'], partnum_todo=None, parts=[1, 2, 3, 4])

        building = self.dfsort(stubs_job.list_outfiles_for_build_command(
            stubs_job.makeargs(self.en['dump_dir'])))
        publish = self.dfsort(stubs_job.list_outfiles_to_publish(
            stubs_job.makeargs(self.en['dump_dir'])))
        stub_pattern = 'enwiki-{date}-stub-{stubtype}{part}.xml.gz'
        all_parts_stubs = [stub_pattern.format(date=self.today, stubtype=stubtype, part=part)
                           for stubtype in ['articles', 'meta-current', 'meta-history']
                           for part in [1, 2, 3, 4]]
        all_parts_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(all_parts_stubs))
        with self.subTest('list stub outputs to build commands (parts)'):
            self.assertEqual(building, all_parts_stubs_dfnames)
        with self.subTest('list stub outputs to publish (parts)'):
            self.assertEqual(publish, all_parts_stubs_dfnames)

        for name in ['stub-articles']:
            self.setup_empty_xml_files_parts(self.en['wiki'], [1, 2, 4], name, 'gz')
            self.setup_empty_xml_files_parts(self.en['wiki'], [3], name, 'gz', inprog=True)
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz')
        for name in ['stub-meta-current']:
            self.setup_empty_xml_files_parts(self.en['wiki'], [2, 3], name, 'gz', truncated=True)
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz', inprog=True)
        for name in ['stub-meta-history']:
            self.setup_empty_xml_files_parts(self.en['wiki'], [1, 2, 4], name, 'gz')
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz', truncated=True)

        cleanup_inprog = self.dfsort(stubs_job.list_inprog_files_for_cleanup(
            stubs_job.makeargs(self.en['dump_dir'])))
        cleanup = self.dfsort(stubs_job.list_outfiles_for_cleanup(
            stubs_job.makeargs(self.en['dump_dir'])))
        for_input = self.dfsort(stubs_job.list_outfiles_for_input(
            stubs_job.makeargs(self.en['dump_dir'])))

        expected_inprog_stubs = ['enwiki-{date}-stub-articles3.xml.gz.inprog'.format(
            date=self.today)]
        expected_inprog_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            expected_inprog_stubs))

        with self.subTest('list inprog stubs for cleanup'):
            self.assertEqual(cleanup_inprog, expected_inprog_stubs_dfnames)

        some_parts_stubs = [stub_pattern.format(date=self.today, stubtype=stubtype, part=part)
                            for stubtype in ['articles', 'meta-history']
                            for part in [1, 2, 4]]
        some_parts_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            some_parts_stubs))
        with self.subTest('list stub outputs for cleanup'):
            self.assertEqual(cleanup, some_parts_stubs_dfnames)
        with self.subTest('list stub outputs for input'):
            self.assertEqual(for_input, some_parts_stubs_dfnames)

        truncated = self.dfsort(stubs_job.list_truncated_empty_outfiles(
            stubs_job.makeargs(self.en['dump_dir'])))

        truncated_parts_stubs = [
            stub_pattern.format(date=self.today, stubtype=stubtype, part=part) + '.truncated'
            for stubtype in ['meta-current']
            for part in [2, 3]]
        truncated_parts_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            truncated_parts_stubs))
        with self.subTest('list truncated stub outputs'):
            self.assertEqual(truncated, truncated_parts_stubs_dfnames)

        # second batch of tests

        # remove all stubs
        self.remove_empty_xml_files(self.en['wiki'])
        stubs_job = self.get_xmlstubs_job(self.en['wiki'], partnum_todo=None, parts=False)

        building = self.dfsort(stubs_job.list_outfiles_for_build_command(
            stubs_job.makeargs(self.en['dump_dir'])))
        publish = self.dfsort(stubs_job.list_outfiles_to_publish(
            stubs_job.makeargs(self.en['dump_dir'])))
        stubs_noparts = ['enwiki-{date}-stub-meta-history.xml.gz'.format(date=self.today),
                         'enwiki-{date}-stub-meta-current.xml.gz'.format(date=self.today),
                         'enwiki-{date}-stub-articles.xml.gz'.format(date=self.today)]
        stubs_noparts_dfnames = self.dfsort(self.dfnames_from_filenames(
            stubs_noparts))
        with self.subTest('list stub outputs to build (parts)'):
            self.assertEqual(building, stubs_noparts_dfnames)
        with self.subTest('list stub outputs to publish (parts)'):
            self.assertEqual(publish, stubs_noparts_dfnames)

        # set up stubs for this test
        for name in ['stub-articles']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz')
        for name in ['stub-meta-current']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz', inprog=True)
        for name in ['stub-meta-history']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz', truncated=True)

        cleanup_inprog = stubs_job.list_inprog_files_for_cleanup(
            stubs_job.makeargs(self.en['dump_dir']))
        cleanup = stubs_job.list_outfiles_for_cleanup(
            stubs_job.makeargs(self.en['dump_dir']))
        for_input = stubs_job.list_outfiles_for_input(
            stubs_job.makeargs(self.en['dump_dir']))
        expected_inprog_stubs = ['enwiki-{date}-stub-meta-current.xml.gz.inprog'.format(
            date=self.today)]
        expected_inprog_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            expected_inprog_stubs))
        with self.subTest('list inprog stubs for cleanup'):
            self.assertEqual(cleanup_inprog, expected_inprog_stubs_dfnames)

        some_stubs = ['enwiki-{date}-stub-articles.xml.gz'.format(date=self.today)]
        some_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            some_stubs))
        with self.subTest('list stub outputs for cleanup'):
            self.assertEqual(cleanup, some_stubs_dfnames)
        with self.subTest('list stub outputs for input'):
            self.assertEqual(for_input, some_stubs_dfnames)

        truncated = stubs_job.list_truncated_empty_outfiles(
            stubs_job.makeargs(self.en['dump_dir']))

        truncated_stubs = ['enwiki-{date}-stub-meta-history.xml.gz.truncated'.format(
            date=self.today)]
        truncated_stubs_dfnames = self.dfsort(self.dfnames_from_filenames(
            truncated_stubs))
        with self.subTest('list truncated stub outputs'):
            self.assertEqual(truncated, truncated_stubs_dfnames)

    def test_xmlcontent_lister(self):
        '''test the various list methods for page content jobs, for parts, parts and checkpoints,
        and no parts.'''

        # first batch of tests, parts

        # parts files for stubs
        for name in ['stub-articles', 'stub-meta-current', 'stub-meta-history']:
            self.setup_empty_xml_files_parts(self.en['wiki'], [1, 2, 3, 4],
                                             name, 'gz')
            # recombined file for stubs
            self.setup_empty_xml_file(self.en['wiki'], name, 'gz')

        stubs_job = self.get_xmlstubs_job(self.en['wiki'], partnum_todo=None, parts=[1, 2, 3, 4])
        articles_job = self.get_xmlcontent_job(self.en['wiki'], 'articles', stubs_job,
                                               partnum_todo=None, parts=[1, 2, 3, 4],
                                               checkpoints=False, checkpoint_file=None)

        building = self.dfsort(articles_job.list_outfiles_for_build_command(
            articles_job.makeargs(self.en['dump_dir'])))
        publish = self.dfsort(articles_job.list_outfiles_to_publish(
            articles_job.makeargs(self.en['dump_dir'])))

        articles_pattern = 'enwiki-{date}-pages-articles{part}.xml.bz2'
        all_parts_articles = [articles_pattern.format(date=self.today, part=part)
                              for part in [1, 2, 3, 4]]
        all_parts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            all_parts_articles))
        with self.subTest('list article outputs for building command'):
            self.assertEqual(building, all_parts_articles_dfnames)
        with self.subTest('list article outputs for publishing'):
            self.assertEqual(publish, all_parts_articles_dfnames)

        # remove them all, put back some
        self.remove_empty_xml_files(self.en['wiki'])

        # parts files for articles
        for name in ['pages-articles']:
            self.setup_empty_xml_files_parts(self.en['wiki'], [1, 2], name, 'bz2')
            self.setup_empty_xml_files_parts(self.en['wiki'], [3], name, 'bz2', inprog=True)
            self.setup_empty_xml_files_parts(self.en['wiki'], [4], name, 'bz2', truncated=True)
            # recombined file for articles
            self.setup_empty_xml_file(self.en['wiki'], name, 'bz2')

        cleanup_inprog = self.dfsort(articles_job.list_inprog_files_for_cleanup(
            articles_job.makeargs(self.en['dump_dir'])))
        inprog_parts_articles = ['enwiki-{date}-pages-articles3.xml.bz2.inprog'.format(
            date=self.today)]
        inprog_parts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            inprog_parts_articles))
        with self.subTest('list inprog article outputs for cleanup'):
            self.assertEqual(cleanup_inprog, inprog_parts_articles_dfnames)

        cleanup = self.dfsort(articles_job.list_outfiles_for_cleanup(
            articles_job.makeargs(self.en['dump_dir'])))
        some_parts_articles = [
            articles_pattern.format(date=self.today, part=part) for part in [1, 2]]
        some_parts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            some_parts_articles))
        with self.subTest('list article outputs for cleanup'):
            self.assertEqual(cleanup, some_parts_articles_dfnames)
        for_input = self.dfsort(articles_job.list_outfiles_for_input(
            articles_job.makeargs(self.en['dump_dir'])))
        with self.subTest('list article outputs for input'):
            self.assertEqual(for_input, some_parts_articles_dfnames)

        truncated = self.dfsort(articles_job.list_truncated_empty_outfiles(
            articles_job.makeargs(self.en['dump_dir'])))
        truncated_parts_articles = ['enwiki-{date}-pages-articles4.xml.bz2.truncated'.format(
            date=self.today)]
        truncated_parts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            truncated_parts_articles))
        with self.subTest('list truncated article outputs for input'):
            self.assertEqual(truncated, truncated_parts_articles_dfnames)

        # second batch of tests, no parts
        # remove them all, put back one
        self.remove_empty_xml_files(self.en['wiki'])

        for name in ['pages-articles']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'bz2')

        stubs_job = self.get_xmlstubs_job(self.en['wiki'], partnum_todo=None, parts=False)
        articles_job = self.get_xmlcontent_job(self.en['wiki'], 'articles', stubs_job,
                                               partnum_todo=None, parts=False,
                                               checkpoints=False, checkpoint_file=None)

        building = self.dfsort(articles_job.list_outfiles_for_build_command(
            articles_job.makeargs(self.en['dump_dir'])))
        publish = self.dfsort(articles_job.list_outfiles_to_publish(
            articles_job.makeargs(self.en['dump_dir'])))

        noparts_articles = ['enwiki-{date}-pages-articles.xml.bz2'.format(date=self.today)]
        noparts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            noparts_articles))
        with self.subTest('list article outputs for building command (no parts)'):
            self.assertEqual(building, noparts_articles_dfnames)
        with self.subTest('list article outputs for publishing (no parts)'):
            self.assertEqual(publish, noparts_articles_dfnames)

        self.remove_empty_xml_files(self.en['wiki'])
        for name in ['pages-articles']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'bz2', inprog=True)

        cleanup_inprog = self.dfsort(articles_job.list_inprog_files_for_cleanup(
            articles_job.makeargs(self.en['dump_dir'])))
        inprog_noparts_articles = ['enwiki-{date}-pages-articles.xml.bz2.inprog'.format(
            date=self.today)]
        inprog_noparts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            inprog_noparts_articles))
        with self.subTest('list inprog article outputs for cleanup (no parts)'):
            self.assertEqual(cleanup_inprog, inprog_noparts_articles_dfnames)

        self.remove_empty_xml_files(self.en['wiki'])
        for name in ['pages-articles']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'bz2')
        cleanup = self.dfsort(articles_job.list_outfiles_for_cleanup(
            articles_job.makeargs(self.en['dump_dir'])))
        with self.subTest('list article outputs for cleanup (no parts)'):
            self.assertEqual(cleanup, noparts_articles_dfnames)
        for_input = self.dfsort(articles_job.list_outfiles_for_input(
            articles_job.makeargs(self.en['dump_dir'])))
        with self.subTest('list article outputs for input (no parts)'):
            self.assertEqual(for_input, noparts_articles_dfnames)

        self.remove_empty_xml_files(self.en['wiki'])
        for name in ['pages-articles']:
            self.setup_empty_xml_file(self.en['wiki'], name, 'bz2', truncated=True)

        truncated = self.dfsort(articles_job.list_truncated_empty_outfiles(
            articles_job.makeargs(self.en['dump_dir'])))
        truncated_noparts_articles = ['enwiki-{date}-pages-articles.xml.bz2.truncated'.format(
            date=self.today)]
        truncated_noparts_articles_dfnames = self.dfsort(self.dfnames_from_filenames(
            truncated_noparts_articles))
        with self.subTest('list truncated article outputs for input'):
            self.assertEqual(truncated, truncated_noparts_articles_dfnames)

        # third batch of tests, checkpoints

    def test_articles_multistream_lister(self):
        '''test the various list methods for the articles multistream job, for parts,
        parts and checkpoints, and no parts.'''
        pass

    def test_recombine_xmlmultistream_lister(self):
        '''test the various list methods for the recombine articles multistream job, for
        parts, parts and checkpoints, and no parts. although the output is guaranteed
        to be a single file, the input can be any of the above, and we test both the
        listing of files for input from within this class, as well as the single output.'''
        pass

    def test_xml_recompress_lister(self):
        '''test the various list methods for the xml recompress job (for meta history only),
        for parts, parts and checkpoints, and no parts.'''
        pass

        # might want these in other tests
        # xml_dfname = DumpFilename(self.wd['wiki'])
        # xml_dfname.new_from_filename('wikidatawiki-{today}-stub-articles1.xml.gz'.format(
        #     today=self.today))
        # parts = FilePartInfo.convert_comma_sep(self.wd['wiki'].config.pages_per_filepart_history)


if __name__ == '__main__':
    unittest.main()
