#!/usr/bin/python3
'''
All xml content dump jobs are defined here
'''

import os
from os.path import exists

from dumps.fileutils import DumpContents, PARTS_ANY
from dumps.filelister import JobFileLister
import dumps.pagerange


class PrefetchFinder():
    """
    finding appropriate prefetch files for a page
    content dump
    """
    def __init__(self, wiki, jobinfo, prefetchinfo, verbose):
        self.wiki = wiki
        self.jobinfo = jobinfo
        self.prefetchinfo = prefetchinfo
        self.verbose = verbose

    def get_relevant_prefetch_dfnames(self, file_list, pagerange, date, runner):
        """
        given list of page content files from a dump run and its date, find from that run
        files that cover the specific page range
        pagerange = {'start': <num>, 'end': <num>}

        args: list of DumpFilename, pagerange dict, string in format YYYYMMDD, Runner
        returns: list of DumpFilename
        """
        possibles = []
        if file_list:
            # collect copies of all the dfnames with the first page id of each
            dfnames_page_coverage = []
            for dfname in sorted(file_list, key=lambda thing: thing.filename):
                if dfname.first_page_id:
                    first_page = dfname.first_page_id_int
                else:
                    if runner.wiki.is_private():
                        path = runner.dump_dir.filename_private_path(dfname, date)
                    else:
                        path = runner.dump_dir.filename_public_path(dfname, date)
                    dcontents = DumpContents(self.wiki, path, dfname, self.verbose)
                    first_page = dcontents.find_first_page_id_in_file()
                dfnames_page_coverage.append({'dfname': dfname, 'first': first_page, 'last': None})

            for index, coverage in enumerate(dfnames_page_coverage):
                if coverage['dfname'].last_page_id:
                    coverage['last'] = coverage['dfname'].last_page_id_int
                else:
                    # here we can fill it in. if it's the last file we can't, it remains
                    # 'None' and it will be treated as covering everything to infinity,
                    # which is ok in this context.
                    if index < len(dfnames_page_coverage) + 1:
                        # claim this file covers up to the page just before the next file's
                        # starting page, may not literally be true because of deletes, but
                        # because this is prefetch, we can't rely on using the config
                        # settings, they may have changed
                        coverage['last'] = dfnames_page_coverage[index]['first'] - 1

                if dumps.intervals.interval_overlaps(coverage['first'], coverage['last'],
                                                     pagerange['start'], pagerange['end']):
                    possibles.append(coverage['dfname'])

        return possibles

    def get_pagerange_to_prefetch(self, partnum):
        """
        for the given partnum or for the whole job,
        return the page range for which we want prefetch files

        args: string (digits)
        returns: {'start': <num>, 'end': <num> or None}
        """
        pagerange = {}
        if partnum:
            pagerange['start'] = sum([self.prefetchinfo['pagesperpart'][i]
                                      for i in range(0, int(partnum) - 1)]) + 1
            if len(self.prefetchinfo['pagesperpart']) > int(partnum):
                pagerange['end'] = sum([self.prefetchinfo['pagesperpart'][i]
                                        for i in range(0, int(partnum))])
            else:
                pagerange['end'] = None
        else:
            pagerange['start'] = 1
            pagerange['end'] = None
        return pagerange

    def find_prefetch_files_from_run(self, runner, date,
                                     pagerange, file_ext):
        """
        for a given wiki and date, see if there are dump content
        files lying about that can be used for prefetch to the
        current job, with the given file extension (might be bz2s
        or 7zs or whatever) for the given range of pages
        """
        flister = JobFileLister(self.jobinfo['dumpname'], self.jobinfo['ftype'], file_ext,
                                None, None)
        # list checkpt files for all available parts, not relying on current config
        # as to parts, because this is a different run and the config
        # may have been different
        dfnames = flister.list_checkpt_files(flister.makeargs(
            runner.dump_dir, self.jobinfo['dumpname'], parts=PARTS_ANY, date=date))
        possible_prefetch_dfnames = self.get_relevant_prefetch_dfnames(
            dfnames, pagerange, date, runner)
        if possible_prefetch_dfnames:
            return possible_prefetch_dfnames

        # ok, let's check for file parts only (no checkpt) instead, from any run; again
        # parts config may not be the same as current run
        dfnames = flister.list_reg_files(flister.makeargs(
            runner.dump_dir, self.jobinfo['dumpname'], parts=PARTS_ANY, date=date))
        possible_prefetch_dfnames = self.get_relevant_prefetch_dfnames(
            dfnames, pagerange, date, runner)
        if possible_prefetch_dfnames:
            return possible_prefetch_dfnames

        # last shot, get single output file that contains all the pages, if there is one
        dfnames = flister.list_reg_files(flister.makeargs(
            runner.dump_dir, self.jobinfo['dumpname'], parts=None, date=date))
        # there is only one, don't bother to check for relevance :-P
        possible_prefetch_dfnames = dfnames
        dfnames = []
        for prefetch_dfname in possible_prefetch_dfnames:
            if runner.wiki.is_private():
                possible_path = runner.dump_dir.filename_private_path(prefetch_dfname, date)
            else:
                possible_path = runner.dump_dir.filename_public_path(prefetch_dfname, date)
            size = os.path.getsize(possible_path)
            if size < 70000:
                runner.debug("small %d-byte prefetch dump at %s, skipping" % (
                    size, possible_path))
                continue
            dfnames.append(prefetch_dfname)
        if dfnames:
            return dfnames
        return None

    def find_previous_dump(self, runner, partnum=None):
        """
        this finds the content file or files from the first previous successful dump
        to be used as input ("prefetch") for this run.

        args:
            Runner, partnum (string of digits)
        returns:
            list of DumpFilename
        """
        pagerange = self.get_pagerange_to_prefetch(partnum)
        if self.prefetchinfo['date']:
            dumpdates = [self.prefetchinfo['date']]
        else:
            dumpdates = self.wiki.dump_dirs()
        dumpdates = sorted(dumpdates, reverse=True)
        for date in dumpdates:
            if date == self.wiki.date:
                runner.debug("skipping current dump for prefetch of job %s, date %s" %
                             (self.jobinfo['name'], self.wiki.date))
                continue

            # see if this job from that date was successful
            if not runner.dumpjobdata.runinfo.status_of_old_dump_is_done(
                    runner, date, self.jobinfo['name'], self.jobinfo['desc']):
                runner.debug("skipping incomplete or failed dump for prefetch date %s" % date)
                continue

            # might look first for 7z files, then for bz2,
            # in any case go through the entire dance for each extension
            # before giving up and moving to next one
            for file_ext in self.jobinfo['fexts']:

                dfnames_found = self.find_prefetch_files_from_run(
                    runner, date, pagerange, file_ext)
                if dfnames_found:
                    return dfnames_found

        runner.debug("Could not locate a prefetchable dump.")
        return None

    def get_prefetch_arg(self, runner, output_dfname, stub_file):
        """
        Try to pull text from the previous run; most stuff hasn't changed
        Source=$OutputDir/pages_$section.xml.bz2

        args:
            Runner, DumpFilename, DumpFilename
        returns:
            list of DumpFilename
        """
        sources = []

        possible_sources = self.find_previous_dump(runner, output_dfname.partnum)
        # if we have a list of more than one then
        # we need to check existence for each and put them together in a string
        if possible_sources:
            for sourcefile in possible_sources:
                # if we are doing pagerange stub run, include only the analogous
                # checkpointed prefetch files, if there are checkpointed files
                # otherwise we'll use the all the sourcefiles reported
                if not dumps.intervals.chkptfile_in_pagerange(stub_file, sourcefile):
                    continue
                if runner.wiki.is_private():
                    source_path = runner.dump_dir.filename_private_path(sourcefile, sourcefile.date)
                else:
                    source_path = runner.dump_dir.filename_public_path(sourcefile, sourcefile.date)
                if exists(source_path):
                    sources.append(source_path)

        if output_dfname.partnum:
            partnum_str = "%s" % stub_file.partnum
        else:
            partnum_str = ""
        if sources:
            if sources[0].endswith('7z'):
                source = "7zip:%s" % (";".join(sources))
            else:
                source = "bzip2:%s" % (";".join(sources))
            message = ("... building {subset} {num} XML dump, for output {out}"
                       " with text prefetch from {where}...")
            runner.show_runner_state(message.format(subset=self.jobinfo['subset'], num=partnum_str,
                                                    out=output_dfname.filename, where=source))
            prefetch = "--prefetch=%s" % (source)
        else:
            message = ("... building {subset} {num} XML dump, for output {out},"
                       " no text prefetch...")
            runner.show_runner_state(message.format(subset=self.jobinfo['subset'], num=partnum_str,
                                                    out=output_dfname.filename))
            prefetch = ""
        return prefetch
