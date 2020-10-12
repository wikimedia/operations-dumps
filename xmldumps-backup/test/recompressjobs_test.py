#!/usr/bin/python3
"""
test suite for recompression dump jobs
"""
import os
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
import dumps.dumpitemlist
import dumps.wikidump
import dumps.xmlcontentjobs
from dumps.utils import FilePartInfo
from dumps.xmljobs import XmlStub
from dumps.xmlcontentjobs import XmlDump
from dumps.recompressjobs import XmlRecompressDump
from dumps.runner import Runner


class TestRecompressJobs(BaseDumpsTestCase):
    """
    test (parts of) generation of recompression of output files
    """
    @staticmethod
    def setup_7z_files_chkpts(wikiname, date, parts_pageranges):
        '''
        make some junk 7z files with names constructed from the pageranges passed in
        '''
        for part in parts_pageranges:
            for prange in parts_pageranges[part]:
                basefilename = '{name}-{date}-pages-articles{part}.xml-{prange}.7z'.format(
                    name=wikiname, date=date, part=part, prange=prange)
                outpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, wikiname,
                                       date, basefilename)
                with open(outpath, "w") as output:
                    output.write("fake\n")
        return

    def get_7z_todo(self, pageranges):
        '''
        given pagerange strings ('1.xml-p123p456'),
        return the expected commands to generate 7z files
        from the bzip page content input
        '''
        expected_todo = []
        dumpname = 'pages-articles'
        decompress = '/usr/bin/lbzip2 -dc -n 1'
        compress = '/usr/bin/7za a -mx=4 -si'
        for prange in pageranges:
            infilename = '{dirname}/{wiki}/{date}/{wiki}-{date}-{name}{rangeinfo}.bz2'.format(
                dirname='test/output/public', wiki='wikidatawiki', date=self.today,
                name=dumpname, rangeinfo=prange)
            outfilename = '{dirname}/{wiki}/{date}/{wiki}-{date}-{name}{rangeinfo}.7z.inprog'.format(
                dirname='test/output/public', wiki='wikidatawiki', date=self.today,
                name=dumpname, rangeinfo=prange)
            expected_todo.append([[['{decompress} {infile} | {compress} {outfile}'.format(
                decompress=decompress, infile=infilename, compress=compress, outfile=outfilename)]]])
        return expected_todo

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_batch_command_generation(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        make sure that given page content file, we generate good commands
        for producing 7z recompressed output from those,
        for wikis with checkpoints enabled.
        also check that we skip commands where the page content file has
        magically appeared before a batch of commands is to be generated
        """
        self.setup_xml_files_chkpts(['content'], self.today)

        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=True,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)

        pages_per_part = FilePartInfo.convert_comma_sep(
            self.wd['wiki'].config.pages_per_filepart_history)

        stubs_job = XmlStub("xmlstubsdump", "First-pass for page XML data dumps",
                            partnum_todo=False,
                            jobsperbatch=dumps.dumpitemlist.get_int_setting(
                                self.wd['wiki'].config.jobsperbatch, "xmlstubsdump"),
                            pages_per_part=pages_per_part)

        content_job = XmlDump("articles", "articlesdump", "short description here",
                              "long description here",
                              item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                              prefetch=False, prefetchdate=None,
                              spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                              pages_per_part=pages_per_part,
                              checkpoints=True, checkpoint_file=None,
                              page_id_range=None, verbose=False)

        recompress_job = XmlRecompressDump("meta-history", "metahistory7zdump",
                                           "short description here",
                                           "long description here",
                                           item_for_recompress=content_job,
                                           wiki=self.wd['wiki'], partnum_todo=False,
                                           pages_per_part=pages_per_part,
                                           checkpoints=True)

        commands_left = recompress_job.get_all_commands(runner)

        with self.subTest('unfiltered 7z command batch'):
            commands_todo, commands_left = recompress_job.get_command_batch(commands_left, runner)
            pageranges = ['1.xml-p1p1500', '1.xml-p1501p4000', '1.xml-p4001p4321', '1.xml-p4322p4330']
            expected_todo = self.get_7z_todo(pageranges)
            self.assertEqual(commands_todo, expected_todo)

        with self.subTest('7z command batch with some output files existing'):
            # we don't actually run any commands, we just pretend we did and check the next batch,
            # but first let's add a few 7z files to the directory; we want to see that those
            # are skipped over when command batches are generated
            parts_pageranges = {'2': ['p4351p4380', 'p4381p4443']}
            self.setup_7z_files_chkpts(self.wd['wiki'].db_name, self.today, parts_pageranges)
            commands_todo, commands_left = recompress_job.get_command_batch(commands_left, runner)
            pageranges = ['2.xml-p4331p4350', '3.xml-p4444p4445', '3.xml-p4446p4600', '3.xml-p4601p4605']
            expected_todo = self.get_7z_todo(pageranges)
            self.assertEqual(commands_todo, expected_todo)
