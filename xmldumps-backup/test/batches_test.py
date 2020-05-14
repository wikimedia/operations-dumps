#!/usr/bin/python3
"""
test suite for jobs split into batches and run separately
by different processes, potentially on separate hosts
"""
import unittest
from test.basedumpstest import BaseDumpsTestCase
import os
import time
import multiprocessing
import sys
from dumps.batch import BatchesFile


def lock_batchesfile(wiki, seconds, maxretries):
    '''
    lock the batches file for the wiki.db_name,
    sleeping the specified number of seconds
    afterwards to hold the lock
    '''
    job = 'pagesmetahistorybz2dump'
    batchesfile = BatchesFile(wiki, job, maxretries)
    with open(batchesfile.get_path(), 'r+') as fhandle:
        result = batchesfile.get_lock(fhandle)
        if result:
            time.sleep(seconds)
            result = 0
        else:
            result = -1
    sys.exit(result)


unittest.TestLoader.sortTestMethodsUsing = None


class BatchesFileTestCase(BaseDumpsTestCase):
    """
    test writing and reading entries in a file of batches
    """
    def cleanup_files(self, job):
        '''
        clean up any files we may have created in previous tests
        '''
        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))
        if os.path.exists(fullpath):
            os.unlink(fullpath)

    def setup_initial_batchesfile(self, job):
        '''
        we use the same batches file content for all the tests;
        this method sets up the file with the initial content,
        and is used in the first test. of course if that test
        fails, all the rest will too.
        '''
        batchesfile = BatchesFile(self.wd['wiki'], job, 1)
        pageranges = [(1, 600), (901, 1500), (601, 900), (1501, 2100), (2101, 3500)]
        batchesfile.create(pageranges)
        return batchesfile

    @staticmethod
    def get_initial_batches():
        '''
        get the batch values we expect to be initially in the batchesfile
        '''
        batches = [
            '{"batch": {"status": "unclaimed", "owner": {"host": null, "pid": null}, ' +
            '"first_claimed": null, "completed_time": null, "runs": "0", ' +
            '"range": {"start": "1", "end": "600"}}}, ',
            '{"batch": {"status": "unclaimed", "owner": {"host": null, "pid": null}, ' +
            '"first_claimed": null, "completed_time": null, "runs": "0", ' +
            '"range": {"start": "601", "end": "900"}}}, ',
            '{"batch": {"status": "unclaimed", "owner": {"host": null, "pid": null}, ' +
            '"first_claimed": null, "completed_time": null, "runs": "0", ' +
            '"range": {"start": "901", "end": "1500"}}}, ',
            '{"batch": {"status": "unclaimed", "owner": {"host": null, "pid": null}, ' +
            '"first_claimed": null, "completed_time": null, "runs": "0", ' +
            '"range": {"start": "1501", "end": "2100"}}}, ',
            '{"batch": {"status": "unclaimed", "owner": {"host": null, "pid": null}, ' +
            '"first_claimed": null, "completed_time": null, "runs": "0", ' +
            '"range": {"start": "2101", "end": "3500"}}}'
        ]
        return batches

    def test_create(self):
        '''
        create, update and lock the batches file
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        self.setup_initial_batchesfile(job)

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))
        with self.subTest('batches file creation'):
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            batches = self.get_initial_batches()
            expected_contents = '{"batches": [' + ''.join(batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)

    @staticmethod
    def make_claimed_batch(numrange):
        '''
        make a single batch entry with the given page range,
        current time/host/pid, and 'claimed' status
        '''
        return ('{"batch": {"status": "claimed", ' +
                '"owner": {"host": "localhost.localdomain", "pid": ' +
                str(os.getpid()) + '}, ' +
                '"first_claimed": "' + time.strftime("%Y%m%d%H%M%S", time.gmtime()) +
                '", "completed_time": null, "runs": "1", ' +
                '"range": {"start": "' + numrange[0] + '", "end": "' + numrange[1] + '"}}}, ')

    def test_claims(self):
        '''
        test marking batches with claim
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        batchesfile = self.setup_initial_batchesfile(job)

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))

        with self.subTest('claim existing range'):
            batchesfile.claim(('601', '900'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            batches = self.get_initial_batches()
            new_batch = self.make_claimed_batch(('601', '900'))
            new_batches = [batches[0], new_batch] + batches[2:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)

        with self.subTest('claim NON-existing range'):
            with self.assertRaises(Exception):
                batchesfile.claim(('601', '1000'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            self.assertEqual(produced_contents, expected_contents)
        with self.subTest('claim already-claimed range'):
            result = batchesfile.claim(('601', '900'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            self.assertEqual(produced_contents, expected_contents)
            self.assertEqual(result, None)
        with self.subTest('claim first unclaimed range'):
            batchesfile.claim()
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            new_batch = self.make_claimed_batch(('1', '600'))
            new_batches = [new_batch] + new_batches[1:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)

    def test_aborts(self):
        '''
        test marking batches with aborted
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        batchesfile = self.setup_initial_batchesfile(job)
        batchesfile.claim(('601', '900'))

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))

        batches = self.get_initial_batches()
        new_batch = self.make_claimed_batch(('601', '900'))
        new_batches = [batches[0], new_batch] + batches[2:]

        with self.subTest('abort already-claimed range'):
            batchesfile.abort(('601', '900'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            new_batch = new_batches[1].replace('"claimed"', '"aborted"')
            new_batches = [new_batches[0], new_batch] + new_batches[2:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)
        with self.subTest('abort unclaimed range'):
            batchesfile.abort(('1501', '2100'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            new_batch = new_batches[3].replace('"unclaimed"', '"aborted"')
            new_batches = new_batches[0:3] + [new_batch] + new_batches[4:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)
        with self.subTest('abort nonexistent range'):
            with self.assertRaises(Exception):
                batchesfile.abort(('601', '1000'))

    def test_fails(self):
        '''
        test marking batches with failed
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        batchesfile = self.setup_initial_batchesfile(job)
        batchesfile.claim(('1', '600'))

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))

        batches = self.get_initial_batches()
        new_batch = self.make_claimed_batch(('1', '600'))
        new_batches = [new_batch] + batches[1:]

        with self.subTest('fail already-claimed range'):
            batchesfile.fail(('1', '600'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            new_batch = new_batches[0].replace('"claimed"', '"failed"')
            new_batches = [new_batch] + new_batches[1:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)
        with self.subTest('fail unclaimed range'):
            result = batchesfile.fail(('1501', '2100'))
            self.assertEqual(result, None)
        with self.subTest('fail nonexistent range'):
            with self.assertRaises(Exception):
                batchesfile.fail(('601', '1000'))

    def test_done(self):
        '''
        test marking batches with done
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        batchesfile = self.setup_initial_batchesfile(job)
        batchesfile.claim(('601', '900'))

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))

        batches = self.get_initial_batches()
        new_batch = self.make_claimed_batch(('601', '900'))
        new_batches = [batches[0], new_batch] + batches[2:]

        with self.subTest('mark done already-claimed range'):
            result = batchesfile.claim(('601', '900'))
            datetime = time.strftime("%Y%m%d%H%M%S", time.gmtime())
            batchesfile.done(('601', '900'))
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            new_batch = new_batches[1].replace(
                '"completed_time": null',
                '"completed_time": "' + datetime + '"')
            new_batch = new_batch.replace('"claimed"', '"done"')
            new_batches = [new_batches[0], new_batch] + new_batches[2:]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)
        with self.subTest('mark done unclaimed range'):
            result = batchesfile.done(('1501', '2100'))
            self.assertEqual(result, None)
        with self.subTest('mark done nonexistent range'):
            with self.assertRaises(Exception):
                batchesfile.done(('601', '1000'))

    def test_locks(self):
        '''
        test locking batches
        '''
        job = 'pagesmetahistorybz2dump'
        self.cleanup_files(job)
        batchesfile = self.setup_initial_batchesfile(job)

        fullpath = os.path.join(
            BaseDumpsTestCase.PRIVATEDIR, self.wd['wiki'].db_name, self.today,
            'batches-{job}.json'.format(job=job))

        with self.subTest('lock file from other process (fail)'):
            mpctx = multiprocessing.get_context('fork')
            lockproc = mpctx.Process(target=lock_batchesfile, args=(self.wd['wiki'], 30, 1))
            lockproc.start()
            # give the proc some time to get the lock
            time.sleep(1)
            # claim unclaimed range, should fail because we can't get the lock
            with self.assertRaises(Exception):
                batchesfile.claim(('2101', '3500'))
            lockproc.join()

        with self.subTest('lock file from other process (succeed)'):
            mpctx = multiprocessing.get_context('fork')
            lockproc = mpctx.Process(target=lock_batchesfile, args=(self.wd['wiki'], 5, 1))
            lockproc.start()
            # give the proc some time to get the lock
            time.sleep(1)
            # claim unclaimed range, should succeed because the sleep time of the first
            # process is short so it gives up the lock before we are done retrying
            batchesfile.set_retries(5)
            batchesfile.claim(('2101', '3500'))
            lockproc.join()
            with open(fullpath, "r") as infile:
                produced_contents = infile.read()
            batches = self.get_initial_batches()
            # the batches are made expecting not to be the last entry, so toss the space and comma
            # at the end
            new_batch = self.make_claimed_batch(('2101', '3500'))[0:-2]

            new_batches = batches[0:4] + [new_batch]
            expected_contents = '{"batches": [' + ''.join(new_batches) + ']}'
            self.assertEqual(produced_contents, expected_contents)
