#!/usr/bin/python3
"""
test suite for page content jobs split into batches and
 run separately by different processes, potentially on
 separate hosts
"""
import json
import os
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
import dumps.dumpitemlist
from dumps.batch import PageContentBatches, BatchProgressCallback
from dumps.pagerangeinfo import PageRangeInfo
from dumps.runner import Runner
from dumps.utils import FilePartInfo
from dumps.xmlcontentjobs import BigXmlDump
from dumps.xmljobs import XmlStub


class PageContentBatchesTestCase(BaseDumpsTestCase):
    """
    test production of files with page content batch jobs
    and running of these batches, where the primary worker
    produces and claims batches, no secondary worker
    """
    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_pagecontent_batch_jobs(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        check that with pagerangeinfo file used, batches on, we get the commands
        we expect for generating page content files
        """
        # we don't want to run the revinfo generation in this test
        self.wd['wiki'].config.revinfostash = 0
        self.jobname = "meta-history"

        self.setup_xml_files_chkpts(['stub'], self.today, job='meta-history')

        # set up a fake pagerangeinfo file too
        fake_pageranges = [(1501, 4000, 1), (1, 1500, 1), (4001, 4321, 1), (4322, 4330, 1),
                           (4331, 4350, 2), (4351, 4380, 2), (4381, 4443, 2),
                           (4444, 4445, 3), (4446, 4600, 3), (4601, 4605, 3),
                           (4606, 5340, 4), (5341, 5344, 4)]
        pr_info = PageRangeInfo(self.wd['wiki'], enabled=True, fileformat="json",
                                error_callback=None, verbose=False)
        pr_info.update_pagerangeinfo(self.wd['wiki'], self.jobname, fake_pageranges)

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

        content_job = BigXmlDump("meta-history", "metahistorybz2dump", "short description here",
                                 "long description here",
                                 item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                                 prefetch=False, prefetchdate=None,
                                 spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                                 pages_per_part=pages_per_part,
                                 checkpoints=True, checkpoint_file=None,
                                 page_id_range=None, verbose=False)

        # get the names of the output files we want to produce
        dfnames_todo = content_job.get_content_dfnames_todo(runner)

        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)

        batchsize = content_job.get_batchsize()

        # we don't filter out anything here, we don't generate any temp stubs files
        # anyways so no point in checking for empty ones
        todo = wanted

        pcbatches = PageContentBatches(self.wd['wiki'], content_job.name(), batchsize)

        # we are pretending to be a primary worker
        pcbatches.create()

        content_job.batchprogcallback = BatchProgressCallback(pcbatches)
        counter = 1
        batch_range = pcbatches.batchesfile.claim()
        batch_ranges_gotten = [[counter, batch_range]]
        commands_gotten = []
        while batch_range:
            todo_batch = content_job.get_batch_todos(todo, batch_range)
            if todo_batch:
                commands = content_job.get_commands_for_pagecontent(
                    todo_batch, runner)
                commands_gotten.append([counter, commands])
                # don't actually run them, heh
                # content_job.run_page_content_commands(commands, runner, batch_type)
            else:
                pcbatches.batchesfile.done(batch_range)
            batch_range = pcbatches.batchesfile.claim()
            counter += 1
            if batch_range:
                batch_ranges_gotten.append([counter, batch_range])

        expected_commands = []
        expected_batch_ranges = [[1, ('1', '4330')], [2, ('4331', '4445')], [3, ('4446', '5344')]]
        self.assertEqual(sorted(batch_ranges_gotten), expected_batch_ranges)

        output_ranges = {'1': [('1', "p1p1500"), ('1', "p1501p4000"), ('1', "p4001p4321"),
                               ('1', "p4322p4330")],
                         '2': [('2', "p4331p4350"), ('2', "p4351p4380"), ('2', "p4381p4443"),
                               ('3', "p4444p4445")],
                         '3': [('3', "p4446p4600"), ('3', "p4601p4605"), ('4', "p4606p5340"),
                               ('4', "p5341p5344")]}
        for batchnum in output_ranges:
            thisbatch = []
            for (part, prange) in output_ranges[batchnum]:
                stubfile = "wikidatawiki-{today}-stub-meta-history{part}.xml-{prange}.gz".format(
                    today=self.today, part=part, prange=prange)
                stubpath = "--stub=gzip:test/output/public/wikidatawiki/{today}/{stubfile}".format(
                    today=self.today, stubfile=stubfile)
                outfile = "wikidatawiki-{today}-pages-meta-history{part}.xml-{prange}.bz2.inprog"
                outfile = outfile.format(today=self.today, part=part, prange=prange)
                outpath = ("--output=lbzip2:test/output/public/wikidatawiki/"
                           "{today}/{outfile}".format(
                               today=self.today, outfile=outfile))
                command = ['/usr/bin/php',
                           'test/mediawiki/maintenance/dumpTextPass.php',
                           '--wiki=wikidatawiki',
                           stubpath,
                           '', '--report=1000', '--spawn=/usr/bin/php',
                           outpath, '--full']
                thisbatch.append([[command]])
            expected_commands.append([int(batchnum), thisbatch])
        self.assertEqual(commands_gotten, expected_commands)

    @staticmethod
    def get_batches_info_expected():
        """
        we reuse this string in several tests so define it once here

        ordinarily first_claimed and completed_time are strings like
        '20201031103336' except that the completed time is None
        for the third batch which wasn't run.
        We set them all to None so we can compare easily
        """
        batches_info_expected = {
            'batches':
            [{'batch':
              {'status': 'done',
               'owner': {'host': 'localhost.localdomain', 'pid': 807915},
               'first_claimed': None,
               'completed_time': None,
               'runs': '1',
               'range': {'start': '1', 'end': '4330'}}},
             {'batch':
              {'status': 'done',
               'owner': {'host': 'localhost.localdomain', 'pid': 807915},
               'first_claimed': None,
               'completed_time': None,
               'runs': '1',
               'range': {'start': '4331', 'end': '4445'}}},
             {'batch':
              {'status': 'unclaimed',
               'owner': {'host': None, 'pid': None},
               'first_claimed': None,
               'completed_time': None,
               'runs': '0',
               'range': {'start': '4446', 'end': '5344'}}}]}
        return batches_info_expected

    def batch_job_test_setup(self, numbatches):
        """
        set up page range info file, get stubs job, get content job and
        return that
        """
        # we don't want to run the revinfo generation in this test
        self.wd['wiki'].config.revinfostash = 0
        self.jobname = "meta-history"

        self.setup_xml_files_chkpts(['stub'], self.today, job='meta-history')

        # set up a fake pagerangeinfo file too
        fake_pageranges = [(1501, 4000, 1), (1, 1500, 1), (4001, 4321, 1), (4322, 4330, 1),
                           (4331, 4350, 2), (4351, 4380, 2), (4381, 4443, 2),
                           (4444, 4445, 3), (4446, 4600, 3), (4601, 4605, 3),
                           (4606, 5340, 4), (5341, 5344, 4)]
        pr_info = PageRangeInfo(self.wd['wiki'], enabled=True, fileformat="json",
                                error_callback=None, verbose=False)
        pr_info.update_pagerangeinfo(self.wd['wiki'], self.jobname, fake_pageranges)

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

        content_job = BigXmlDump("meta-history", "metahistorybz2dump", "short description here",
                                 "long description here",
                                 item_for_stubs=stubs_job, item_for_stubs_recombine=None,
                                 prefetch=False, prefetchdate=None,
                                 spawn=True, wiki=self.wd['wiki'], partnum_todo=False,
                                 pages_per_part=pages_per_part,
                                 checkpoints=True, checkpoint_file=None,
                                 page_id_range=None, numbatches=numbatches, verbose=False)

        return content_job, runner

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    @patch('dumps.runner.Runner.run_command')
    def test_batch_jobs(self, mock_run_command, _mock_get_some_stats, _mock_get_known_tables):
        """
        run batches as primary worker and check that batch info file
        looks to have been updated properly, then do the same as
        secondary worker, and finally do one primary and one secondary
        """
        mock_run_command.return_value = (0, None)

        # we'll do two primary batches only and see what happens
        content_job, runner = self.batch_job_test_setup(2)

        # get the names of the output files we want to produce
        dfnames_todo = content_job.get_content_dfnames_todo(runner)

        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)
        batchsize = content_job.get_batchsize()

        # we don't filter out anything here, we don't generate any temp stubs files
        # anyways so no point in checking for empty ones
        todo = wanted
        content_job.do_run_batches(todo, batchsize, 'batch_primary', runner)

        btype = 'primary'
        self.check_batchinfo_file(content_job, batchsize, btype, failed=[], completed=[0, 1])

        # create the file anew and we'll try to rerun both as secondary
        # then we'll look at the results again
        pcbatches = PageContentBatches(self.wd['wiki'], content_job.name(), batchsize)
        pcbatches.create()

        # same todos etc, just rerun as secondary worker
        content_job.do_run_batches(todo, batchsize, 'batch_secondary', runner)

        btype = 'secondary'
        self.check_batchinfo_file(content_job, batchsize, btype, [], [0, 1])

        # we'll do one primary batch and one secondary batch now
        content_job, runner = self.batch_job_test_setup(1)

        dfnames_todo = content_job.get_content_dfnames_todo(runner)
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)
        batchsize = content_job.get_batchsize()

        # we don't filter out anything here, we don't generate any temp stubs files
        # anyways so no point in checking for empty ones
        todo = wanted
        content_job.do_run_batches(todo, batchsize, 'batch_primary', runner)

        content_job.do_run_batches(todo, batchsize, 'batch_secondary', runner)
        btype = 'both'
        self.check_batchinfo_file(content_job, batchsize, btype, failed=[], completed=[0, 1])

        # we'll do one primary batch, fail it, do 1 secondary batch
        # and see what happend
        content_job, runner = self.batch_job_test_setup(1)

        dfnames_todo = content_job.get_content_dfnames_todo(runner)
        wanted = content_job.get_wanted(dfnames_todo, runner, prefetcher=None)
        batchsize = content_job.get_batchsize()

        # we don't filter out anything here, we don't generate any temp stubs files
        # anyways so no point in checking for empty ones
        todo = wanted

        # we want to mulate getting a failure from the commands to generate
        # page content output; this will also cause the batch to be marked as failed
        mock_run_command.return_value = (1, [[['some commands (expect this output)']]])
        with self.assertRaises(Exception):
            content_job.do_run_batches(todo, batchsize, 'batch_primary', runner)

        mock_run_command.return_value = (0, None)
        content_job.do_run_batches(todo, batchsize, 'batch_secondary', runner)
        btype = 'primary_fail_secondary_ok'
        self.check_batchinfo_file(content_job, batchsize, btype, failed=[0], completed=[1])

    def check_batchinfo_file(self, content_job, batchsize, btype, failed, completed):
        """
        for a run of two batches of history page content files, check that
        the batch info file is as we expect
        """
        pcbatches = PageContentBatches(self.wd['wiki'], content_job.name(), batchsize)
        batchesfilepath = pcbatches.batchesfile.get_path()
        with open(batchesfilepath, "r") as fhandle:
            contents = fhandle.read()
            batches_info = json.loads(contents)
            batches_info_expected = self.get_batches_info_expected()

            with self.subTest('check completion times of batches ' + btype):
                # check all the completed times
                completion_times = [entry['batch']['completed_time']
                                    for entry in batches_info['batches']]
                result = self.check_times(completion_times, 3, completed)
                if not result:
                    print(completion_times)
                self.assertEqual(result, True)

            with self.subTest('check claimed times of batches ' + btype):
                # check all the first claimed times
                claimed_times = [entry['batch']['first_claimed']
                                 for entry in batches_info['batches']]
                result = self.check_times(claimed_times, 3, [0, 1])
                if not result:
                    print(claimed_times)
                self.assertEqual(result, True)

            with self.subTest('chech batchinfo except for claimed/completion times ' + btype):
                self.fixup_batchesinfo_fails(batches_info_expected, failed)
                self.fixup_batchesinfo_pids(batches_info_expected)
                self.fixup_batchesinfo_times(batches_info)
                self.assertEqual(batches_info, batches_info_expected)

    @staticmethod
    def fixup_batchesinfo_fails(batches_info, failed):
        '''
        for any entry in the failed list, mark the status as failed
        '''
        for index in failed:
            batches_info['batches'][index]['batch']['status'] = 'failed'

    @staticmethod
    def fixup_batchesinfo_times(batches_info):
        '''
        times should all get set to None, since they vary
        '''
        for entry in batches_info['batches']:
            entry['batch']['completed_time'] = None
            entry['batch']['first_claimed'] = None

    @staticmethod
    def fixup_batchesinfo_pids(batches_info):
        '''
        pids should all get set to the current pid
        '''
        mypid = os.getpid()
        for entry in batches_info['batches']:
            entry['batch']['completed_time'] = None
            entry['batch']['first_claimed'] = None
        for entry in batches_info['batches']:
            if entry['batch']['owner']['pid'] is not None:
                entry['batch']['owner']['pid'] = mypid

    def check_times(self, times, length, dates):
        """
        we expect length times, the specified ones in dates should be actual YYYYMMDD
        and any others should be None
        """
        if len(times) != length:
            return False
        for index in dates:
            if not times[index].isdigit() or not len(times[index]) == 14:
                return False
            if not times[index][0:4] == self.today[0:4]:
                # close enough check for valid date
                return False
        for index in range(0, length):
            if index not in dates and times[index] is not None:
                return False
        return True
