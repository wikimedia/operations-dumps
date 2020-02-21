#!/usr/bin/python3
'''
All xml content dump jobs are defined here
'''

import os
from os.path import exists

from dumps.fileutils import DumpContents
from dumps.filelister import get_checkpt_files, get_reg_files
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
            # (a) nasty hack, see below (b)
            maxparts = 0
            for dfname in file_list:
                if dfname.is_file_part and dfname.partnum_int > maxparts:
                    maxparts = dfname.partnum_int
                if not dfname.first_page_id:
                    if runner.wiki.is_private():
                        dcontents = DumpContents(
                            self.wiki, runner.dump_dir.filename_private_path(dfname, date),
                            dfname, self.verbose)
                    else:
                        dcontents = DumpContents(
                            self.wiki, runner.dump_dir.filename_public_path(dfname, date),
                            dfname, self.verbose)
                    dfname.set_first_page_id(dcontents.find_first_page_id_in_file())

            # get the files that cover our range
            for dfname in file_list:
                if dumps.pagerange.xmlfile_covers_range(dfname, pagerange,
                                                        maxparts, file_list, runner):
                    possibles.append(dfname)
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
            pagerange['start'] = sum([self.prefetchinfo['parts'][i]
                                      for i in range(0, int(partnum) - 1)]) + 1
            if len(self.prefetchinfo['parts']) > int(partnum):
                pagerange['end'] = sum([self.prefetchinfo['parts'][i]
                                        for i in range(0, int(partnum))])
            else:
                pagerange['end'] = None
        else:
            pagerange['start'] = 1
            pagerange['end'] = None
        return pagerange

    def _find_prefetch_files_from_run(self, runner, date, jobinfo,
                                      pagerange, file_ext):
        """
        for a given wiki and date, see if there are dump content
        files lying about that can be used for prefetch to the
        current job, with the given file extension (might be bz2s
        or 7zs or whatever) for the given range of pages
        """
        dfnames = get_checkpt_files(
            runner.dump_dir, [jobinfo['dumpname']], self.jobinfo['ftype'],
            file_ext, date, parts=None)
        possible_prefetch_dfnames = self.get_relevant_prefetch_dfnames(
            dfnames, pagerange, date, runner)
        if possible_prefetch_dfnames:
            return possible_prefetch_dfnames

        # ok, let's check for file parts instead, from any run
        # (may not conform to our numbering for this job)
        dfnames = get_reg_files(
            runner.dump_dir, [jobinfo['dumpname']], jobinfo['ftype'],
            file_ext, date, parts=True)
        possible_prefetch_dfnames = self.get_relevant_prefetch_dfnames(
            dfnames, pagerange, date, runner)
        if possible_prefetch_dfnames:
            return possible_prefetch_dfnames

        # last shot, get output file that contains all the pages, if there is one
        dfnames = get_reg_files(
            runner.dump_dir, [jobinfo['dumpname']],
            jobinfo['ftype'], file_ext, date, parts=False)
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
            else:
                dfnames.append(prefetch_dfname)
        if dfnames:
            return dfnames
        return None

    def _find_previous_dump(self, runner, partnum=None):
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

                dfnames_found = self._find_prefetch_files_from_run(
                    runner, date, self.jobinfo, pagerange, file_ext)
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

        possible_sources = self._find_previous_dump(runner, output_dfname.partnum)
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
            runner.show_runner_state("... building %s %s XML dump, with text prefetch from %s..." %
                                     (self.jobinfo['subset'], partnum_str, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.show_runner_state("... building %s %s XML dump, no text prefetch..." %
                                     (self.jobinfo['subset'], partnum_str))
            prefetch = ""
        return prefetch
