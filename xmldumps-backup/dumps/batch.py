#!/usr/bin/python3
import os
import fcntl
import json
import socket
import random
import time
from dumps.fileutils import FileUtils
from dumps.exceptions import BackupError
from dumps.pagerangeinfo import PageRangeInfo
from dumps.jobs import ProgressCallback


class BatchesFile():
    '''
    for a given wiki, dump date, and job, if the job is done in batches,
    a file listing those batches, with information about the status
    of each batch, is managed by this class.

    file contents format (converted to json):

    {'batches':
        {'batch':
            {'range':
                {'start': <num>, 'end': <end>}},
            {'runs': <num>},
            {'status': <status-string>},
            {'owner':
                {'host': <hostname>, 'pid': <process_id>}},
            {'first_claimed': <timestamp>},
            {'completed_time': <timestamp>}},
        {'batch':....},
        ....
     }
    '''
    STATUSES = ['unclaimed', 'claimed', 'aborted', 'done', 'failed']
    MAX_LOCK_RETRIES = 5
    BAK = ".bak"

    @staticmethod
    def get_components(filename):
        '''return jobname and batch range from the filename'''
        # job-<jobname>-batch-<range>-running.txt
        fields = filename.split('-')
        return fields[1], fields[3]

    @staticmethod
    def is_batchjob_file(filename):
        '''good enough check that a filename is one of these here batch job files'''
        return bool(filename.startswith('job-') and filename.endswith('-running.txt'))

    def __init__(self, wiki, jobname, maxretries=MAX_LOCK_RETRIES):
        self.wiki = wiki
        self.jobname = jobname
        self.maxretries = maxretries

    def set_retries(self, maxretries):
        '''
        allow the caller to change the maxretries if desired
        '''
        self.maxretries = maxretries

    def get_path(self):
        '''return path to the batches json file for the given
        wiki and date'''
        return os.path.join(self.wiki.private_dir(), self.wiki.date,
                            'batches-{jobname}.json'.format(jobname=self.jobname))

    @staticmethod
    def sanity_check_ranges(ranges):
        '''check that ranges have reasonable content
        expect: list of tuples'''
        bad_ranges = []
        for entry in ranges:
            if int(entry[1]) < int(entry[0]) or (int(entry[0]) <= 0) or len(entry) != 2:
                bad_ranges.append(entry)
        # fixme we should check for overlapping ranges too
        return bad_ranges

    def get_lock(self, fhandle):
        '''try to get lock on the specific filehandle
        with increasing waits between retries'''
        base_wait = 5
        retries = 0
        while retries < self.maxretries:
            try:
                fcntl.lockf(fhandle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except OSError:
                retries += 1
                rand_incr = random.randrange(3, 10)
                base_wait = base_wait * 2
                time.sleep(base_wait + rand_incr)
        return False

    def count_unclaimed_batches(self):
        '''count and return the number of unclaimed batches,
        returning 0 if there is not even a batchfile'''
        try:
            with open(self.get_path(), "r") as fhandle:
                contents = fhandle.read()
                batches_info = json.loads(contents)
                return len(batches_info['batches'])
        except Exception:
            return 0

    def backup(self, contents):
        '''
        write the (text) contents supplied to the backup file path
        '''
        path = self.get_path() + BatchesFile.BAK
        with open(path, "wt") as fhandle:
            fhandle.write(contents)
            fhandle.close()

    @staticmethod
    def clear(batch_info, fields):
        '''
        reset the values of the specified fields to their initial values
        (i.e. None, 0)
        '''
        if 'runs' in fields:
            batch_info['batch']['runs'] = '0'
        if 'first_claimed' in fields:
            batch_info['batch']['first_claimed'] = None
        if 'completed_time' in fields:
            batch_info['batch']['completed_time'] = None
        if 'owner' in fields:
            batch_info['batch']['owner'] = {'host': None, 'pid': None}
        if 'status' in fields:
            batch_info['batch']['status'] = 'unclaimed'

    @staticmethod
    def get_batch_entry(batch_range, batches_info):
        '''
        get the batch entry from batches_info
        '''
        if not batches_info:
            return None

        for entry in batches_info['batches']:
            if (entry['batch']['range']['start'] == batch_range[0] and
                    entry['batch']['range']['end'] == batch_range[1]):
                return entry
        # fixme really? maybe we should just whine?
        return None

    def _do_claim(self, batch_entry):
        '''
        update the batch entry fields to claim the batch
        '''
        batch_entry['batch']['status'] = 'claimed'
        hostname = socket.getfqdn()
        process_id = os.getpid()
        batch_entry['batch']['owner'] = {'host': hostname, 'pid': process_id}
        if not batch_entry['batch']['first_claimed']:
            batch_entry['batch']['first_claimed'] = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self.clear(batch_entry, ['completed_time'])
        batch_entry['batch']['runs'] = str(int(batch_entry['batch']['runs']) + 1)

    def _do_fail(self, batch_entry):
        '''
        update the batch entry fields to mark the batch as failed
        '''
        batch_entry['batch']['status'] = 'failed'
        self.clear(batch_entry, ['completed_time'])

    def _do_abort(self, batch_entry):
        '''
        update the batch entry fields to mark the batch as aborted
        '''
        batch_entry['batch']['status'] = 'aborted'
        self.clear(batch_entry, ['completed_time'])

    def _do_done(self, batch_entry):
        '''
        update the batch entry fields to mark the batch as done
        '''
        batch_entry['batch']['status'] = 'done'
        batch_entry['batch']['completed_time'] = time.strftime("%Y%m%d%H%M%S", time.gmtime())

    def _do_unclaim(self, batch_entry):
        '''
        update the batch entry fields to mark the batch as unclaimed
        '''
        batch_entry['batch']['status'] = 'unclaimed'
        self.clear(batch_entry, ['first_claimed', 'owner', 'completed_time', 'runs'])

    def load_from_backup(self):
        '''
        return batch info from our backup file if it exists, None otherwise
        '''
        path = self.get_path() + BatchesFile.BAK
        if os.path.exists(path):
            with open(self.get_path() + BatchesFile.BAK, "r") as fhandle:
                contents = fhandle.read()
                batches_info = json.loads(contents)
                return batches_info
        return None

    def get_first_entry(self, allowed_statuses, batches_info):
        '''
        return the first entry from batches_info with one of the allowed statuses
        or () if no entry found
        '''
        for entry in batches_info['batches']:
            if entry['batch']['status'] in allowed_statuses:
                return entry
        return ()

    def _do_command(self, status, batch_entry):
        if status == 'claimed':
            self._do_claim(batch_entry)
        elif status == 'failed':
            self._do_fail(batch_entry)
        elif status == 'aborted':
            self._do_abort(batch_entry)
        elif status == 'done':
            self._do_done(batch_entry)
        elif status == 'unclaimed':
            self._do_unclaim(batch_entry)

    def do_update(self, batch_range, status, current_statuses=None):
        '''
        update the status and appropriate related fields for
        the batch entry corresponding to the specified range

        if current statuses is set, then the current status must
        be one of those in the list in order for the action to be taken

        if the status is wrong, None will be returned, otherwise
        the upated entry will be returned

        if a range is specified that does not exist in the file,
        an exception will be raised
        '''
        with open(self.get_path(), 'r+') as fhandle:
            if not self.get_lock(fhandle):
                raise BackupError("failed to get lock on " + self.get_path())
            old_contents = fhandle.read()
            if old_contents:
                try:
                    batches_info = json.loads(old_contents)
                except json.decoder.JSONDecodeError:
                    # try to load from the backup file, since the contents
                    # of the current file are apparently corrupt
                    batches_info = self.load_from_backup()
            else:
                # we should not be updating an empty file. ever.
                raise BackupError("batches file is empty but we are trying to update batches, why?")

            if status == 'claimed' and batch_range is None:
                # get the first entry and claim that, if there is one
                batch_entry = self.get_first_entry(current_statuses, batches_info)
                if not batch_entry:
                    # no entries left. done!
                    return None
                batch_range = (batch_entry['batch']['range']['start'],
                               batch_entry['batch']['range']['end'])
            else:
                batch_entry = self.get_batch_entry(batch_range, batches_info)
            if not batch_entry:
                raise BackupError("batches file is missing the batch we are trying to update!")

            if status == 'rerun':
                status = 'claimed'

            if status not in BatchesFile.STATUSES:
                raise BackupError("bad status passed: " + status)

            self.backup(old_contents)

            if current_statuses is None or batch_entry['batch']['status'] in current_statuses:
                self._do_command(status, batch_entry)
            else:
                return None

            new_contents = json.dumps(batches_info)
            fhandle.seek(0)
            fhandle.write(new_contents)
            fhandle.truncate()
            fhandle.close()
            return batch_range

    @staticmethod
    def get_default_batchinfo():
        '''
        return a dict with sensible defaults for a batch entry in the file
        '''
        return {'status': 'unclaimed',
                'owner': {'host': None, 'pid': None},
                'first_claimed': None,
                'completed_time': None,
                'runs': '0'}

    def create(self, ranges):
        '''
        create a file with all of the ranges in it (page ranges, row ranges, whatever)
        and a status of 'unclaimed' for each one, with some sensible defaults
        '''
        if ranges is not None:
            ranges = sorted(list(set(ranges)), key=lambda thing: int(thing[0]))
            bad_ranges = self.sanity_check_ranges(ranges)
            if bad_ranges:
                # fixme should convert the ranges passed into something printable, oh well
                raise BackupError("bad ranges passed")
        if ranges:
            batch_info = {'batches': []}

            for batch_range in ranges:
                new_entry = self.get_default_batchinfo()
                new_entry['range'] = {'start': str(batch_range[0]),
                                      'end': str(batch_range[1])}
                batch_info['batches'].append({'batch': new_entry})

            contents = json.dumps(batch_info)
        else:
            # fixme decide if this is what we want for empty contents
            contents = '{}'

        # make a backup right away
        self.backup(contents)
        FileUtils.write_file(
            FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir,
                                   create=True),
            self.get_path(),
            contents,
            int('0o644', 0))

    def claim(self, batch_range=None):
        '''
        if the batch range exists in the batches file, and it is
        aborted or unclaimed, claim it;
        if no range is supplied, claim the first one that is unclaimed
        or aborted, if any

        if no such range is available, return ()

        if an error was encountered e.g. with locking, return None
        '''
        return self.do_update(batch_range, 'claimed', ['unclaimed', 'aborted'])

    def unclaim(self, batch_range):
        '''
        if the batch range exists in the batches file, and it is
        claimed, unclaim it
        '''
        return self.do_update(batch_range, 'unclaimed', ['claimed'])

    def abort(self, batch_range):
        '''
        if the batch range exists in the batches file, abort it
        we don't check its current status; this should be called
        when the regularly updated empty file for the batch is out
        of date, indicating that the process has hung or died
        '''
        return self.do_update(batch_range, 'aborted')

    def fail(self, batch_range):
        '''
        if the batch range exists in the batches file, and it is
        claimed, fail it
        '''
        return self.do_update(batch_range, 'failed', ['claimed'])

    def done(self, batch_range):
        '''
        if the batch range exists in the batches file, and it is
        claimed, fail it
        '''
        return self.do_update(batch_range, 'done', ['claimed'])


class BatchProgressCallback(ProgressCallback):
    def __init__(self, batchjobs):
        '''
        jobname as it appears in dumpruninfo.txt
        batchsize: number of jobs we run in parallel
        (the Dump class will have this available
        as len(pages_per_part)
        '''
        self.batchjobs = batchjobs
        super().__init__()

    def secondary_batched_progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files, as a secondary
        worker processing batches."""
        # pass through...
        self.progress_log(runner, line)
        self.batchjobs.touch_batchfile(self.batchjobs.batchrange)

    def main_batched_progress_callback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files, as a primary
        worker (i.e. the worker that sets up the batches file etc) processing batches."""
        self.progress_log(runner, line)
        self.batchjobs.touch_batchfile(self.batchjobs.batchrange)
        self.progress_updates(runner)


class BatchJobs():
    def __init__(self, wiki, jobname, batchsize, maxretries=BatchesFile.MAX_LOCK_RETRIES):
        '''
        jobname as it appears in dumpruninfo.txt
        batchsize: number of jobs we run in parallel
        (the Dump class will have this available
        as len(pages_per_part)
        '''
        self.wiki = wiki
        self.jobname = jobname
        self.batchesfile = BatchesFile(wiki, jobname, maxretries)
        self.batchsize = batchsize
        self.batchrange = None

    def set_batchrange(self, batchrange):
        '''
        set a particular batchrange as the one being worked on right now
        '''
        self.batchrange = batchrange

    def get_batchrange_filepath(self, batchrange):
        '''
        return full path to the empty batch range file for this wiki,
        date and job
        '''
        return os.path.join(self.wiki.private_dir(), self.wiki.date,
                            'job-{jobname}-batch-{batchrange}-running.txt'.format(
                                jobname=self.jobname, batchrange=batchrange))

    def touch_batchfile(self, batchrange):
        '''
        touch a specific file for this job and batch range
        so that monitors know that this job is still being processed;
        stale files get removed and their batch jobs marked as aborted so
        that they can be reclaimed and rerun later

        this assumes that the file has already been created
        '''
        path = self.get_batchrange_filepath(batchrange)
        os.utime(path, None)

    def create_batchfile(self, batchrange):
        '''
        create an empty batchfile for this job and batch range
        '''
        path = self.get_batchrange_filepath(batchrange)
        with open(path, 'a') as fhandle:
            fhandle.close()

    def cleanup_batchfile(self, batchrange):
        '''
        remove the batchfile for this job and batch range, if
        it exists
        '''
        path = self.get_batchrange_filepath(batchrange)
        try:
            os.unlink(path)
        except Exception:
            # if it's already gone or was never there,
            # we do not care
            pass


class PageContentBatches(BatchJobs):
    '''
    handle page content jobs
    '''
    @staticmethod
    def get_range_sequences(extended_ranges):
        '''
        given a list of (sorted in ascending order, non-overlapping)
        ranges, split the list into the largest possible sublists
        consisting only of consecutive ranges
        '''
        range_sequences = []
        range_seq_building = []
        end = None
        for entry in extended_ranges:
            if end is None or int(entry[0]) == int(end) + 1:
                # sequential, stick it onto the current sublist
                range_seq_building.append(entry)
                end = entry[1]
            else:
                # start a new sublist
                range_sequences.append(range_seq_building)
                range_seq_building = [entry]
                end = entry[1]
        # handle last sublist
        if range_seq_building:
            range_sequences.append(range_seq_building)
        return range_sequences

    def split_range_sequences(self, range_seqs):
        '''
        split sequences so that they are batchsize length or (the last one
        in each split) less, return the new list of shorter sequences
        '''
        short_seqs = []
        for long_seq in range_seqs:
            if len(long_seq) <= self.batchsize:
                short_seqs.append(long_seq)
            else:
                short_seqs.extend([long_seq[x:x + self.batchsize]
                                   for x in range(0, len(long_seq), self.batchsize)])
        return short_seqs

    def get_pagerangeinfo_for_job(self):
        '''
        for our job, get the page range info from the file,
        sort the page ranges and consolidate sequential
        ranges such that one consolidated range covers
        batchsize original ranges (or less if there are
        not enough sequential ranges to do it)

        this will ensure that the job will be run
        in parallel appropriately
        '''
        if self.jobname == 'articlesdump':
            prinfo_job = 'articles'
        elif self.jobname == 'metacurrentdump,':
            prinfo_job = 'meta-current'
        elif self.jobname == 'metahistorybz2dump':
            prinfo_job = 'meta-history'
        else:
            return None

        prinfo = PageRangeInfo(self.wiki, True, "json")
        pageranges = prinfo.get_pagerange_info(self.wiki)
        if prinfo_job not in pageranges:
            return None

        # toss extra fields and convert to tuples
        extended_ranges = [(prange[0], prange[1]) for prange in pageranges[prinfo_job]]
        extended_ranges = sorted(extended_ranges, key=lambda x: int(x[0]))

        # get lists of consecutive ranges (if all are consecutive then we'll get one giant list)
        range_seqs = self.get_range_sequences(extended_ranges)

        # split up these sublists into batchsize length
        split_range_seqs = self.split_range_sequences(range_seqs)

        # collect start and end of each consecutive sequence, turn these into ranges
        batched_ranges = [(split_seq[0][0], split_seq[-1][1]) for split_seq in split_range_seqs]

        return batched_ranges

    def create(self):
        '''
        create the batches file for the specified job from the
        info from the pagerangeinfo file
        '''
        prinfo = self.get_pagerangeinfo_for_job()
        if not prinfo:
            raise BackupError("no page range info available for job " + self.jobname +
                              ", no batch file created")
        self.batchesfile.create(prinfo)
