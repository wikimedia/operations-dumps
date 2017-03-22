'''
All xml content dump jobs are defined here
'''

import re
import os
from os.path import exists
import signal

from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError
from dumps.fileutils import DumpContents, DumpFilename
from dumps.utils import MultiVersion, MiscUtils
from dumps.jobs import Dump
from dumps.jobs import get_checkpt_files, get_reg_files
from dumps.WikiDump import Locker


class StubProvider(object):
    """
    make or find stub files for use in page content dump
    """
    def __init__(self, wiki, jobinfo, verbose):
        self.wiki = wiki
        self.jobinfo = jobinfo
        self.verbose = verbose

    def get_stub_files(self, runner, partnum=None):
        '''
        get the stub files pertaining to our dumpname, which is *one* of
        articles, pages-current, pages-history.
        stubs include all of these together.
        we will either return the one full stubs file that exists
        or the one stub file part, if we are (re)running a specific
        file part (subjob), or all file parts if we are (re)running
        the entire job which is configured for subjobs.

        arguments:
           runner        - Runner
           partnum (int) - number of file part (subjob) if any
        '''
        if partnum is None:
            partnum = self.jobinfo['partnum_todo']
        if not self.jobinfo['dumpname'].startswith(self.jobinfo['dumpnamebase']):
            raise BackupError("dumpname %s of unknown form for this job" % self.jobinfo['dumpname'])

        dumpname = self.jobinfo['dumpname'][len(self.jobinfo['dumpnamebase']):]
        stub_dumpnames = self.jobinfo['item_for_stubs'].list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_dfnames = self.jobinfo['item_for_stubs'].list_outfiles_for_input(
            runner.dump_dir, [stub_dumpname])
        if self.jobinfo['parts']:
            if partnum is not None:
                for inp_dfname in input_dfnames:
                    if inp_dfname.partnum_int == partnum:
                        input_dfnames = [inp_dfname]
                        break
        return input_dfnames

    def write_partial_stub(self, input_dfname, output_dfname, runner):
        """
        write out a stub file corresponding to the page range
        in the output filename

        args: DumpFilename, DumpFilename, Runner
        """
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" %
                              self.wiki.config.writeuptopageid)

        inputfile_path = runner.dump_dir.filename_public_path(input_dfname)
        output_file_path = os.path.join(self.wiki.config.temp_dir, output_dfname.filename)
        if input_dfname.file_ext == "gz":
            command1 = "%s -dc %s" % (self.wiki.config.gzip, inputfile_path)
            command2 = "%s > %s" % (self.wiki.config.gzip, output_file_path)
        elif input_dfname.file_ext == '7z':
            command1 = "%s e -si %s" % (self.wiki.config.sevenzip, inputfile_path)
            command2 = "%s e -so %s" % (self.wiki.config.sevenzip, output_file_path)
        elif input_dfname.file_ext == 'bz':
            command1 = "%s -dc %s" % (self.wiki.config.bzip2, inputfile_path)
            command2 = "%s > %s" % (self.wiki.config.bzip2, output_file_path)
        else:
            raise BackupError("unknown stub file extension %s" % input_dfname.file_ext)
        if output_dfname.last_page_id is not None and output_dfname.last_page_id is not "00000":
            command = [command1 + ("| %s %s %s |" % (self.wiki.config.writeuptopageid,
                                                     output_dfname.first_page_id,
                                                     output_dfname.last_page_id)) + command2]
        else:
            # no lastpageid? read up to eof of the specific stub file that's used for input
            command = [command1 + ("| %s %s |" % (self.wiki.config.writeuptopageid,
                                                  output_dfname.first_page_id)) + command2]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], shell=True)
        if error:
            raise BackupError("failed to write partial stub file %s" % output_dfname.filename)

    def get_partial_stubs(self, todo, runner):
        partial_stubs = []
        if self.verbose:
            print "todo is", [to.filename for to in todo]

        for dfname in todo:
            stub_for_file = self.get_stub_files(runner, dfname.partnum_int)[0]

            if dfname.first_page_id is None:
                partial_stubs.append(stub_for_file)
            else:
                stub_output_dfname = DumpFilename(
                    self.wiki, dfname.date, dfname.dumpname,
                    self.jobinfo['item_for_stubs'].get_filetype(),
                    self.jobinfo['item_for_stubs'].get_file_ext(),
                    dfname.partnum,
                    DumpFilename.make_checkpoint_string(
                        dfname.first_page_id, dfname.last_page_id), temp=True)

                self.write_partial_stub(stub_for_file, stub_output_dfname, runner)
                if not self.has_no_entries(stub_output_dfname, runner):
                    partial_stubs.append(stub_output_dfname)

        if self.verbose:
            print "partial_stubs is", [ps.filename for ps in partial_stubs]
        return partial_stubs

    def has_no_entries(self, xmlfile, runner):
        '''
        see if it has a page id in it or not. no? then return True
        '''
        if xmlfile.is_temp_file:
            path = os.path.join(self.wiki.config.temp_dir, xmlfile.filename)
        else:
            path = runner.dump_dir.filename_public_path(xmlfile, self.wiki.date)
        dcontents = DumpContents(self.wiki, path, xmlfile, self.verbose)
        return bool(dcontents.find_first_page_id_in_file() is None)


class Prefetch(object):
    """
    finding appropriate prefetch files for a page
    content dump
    """
    def __init__(self, wiki, jobinfo, prefetchinfo, verbose):
        self.wiki = wiki
        self.jobinfo = jobinfo
        self.prefetchinfo = prefetchinfo
        self.verbose = verbose

    def get_relevant_prefetch_files(self, file_list, start_page_id, end_page_id, date, runner):
        possibles = []
        if len(file_list):
            # (a) nasty hack, see below (b)
            maxparts = 0
            for dfname in file_list:
                if dfname.is_file_part and dfname.partnum_int > maxparts:
                    maxparts = dfname.partnum_int
                if not dfname.first_page_id:
                    dcontents = DumpContents(
                        self.wiki, runner.dump_dir.filename_public_path(dfname, date),
                        dfname, self.verbose)
                    dfname.first_page_id = dcontents.find_first_page_id_in_file()

            # get the files that cover our range
            for dfname in file_list:
                # If some of the dumpfilenames in file_list could not be properly be parsed, some of
                # the (int) conversions below will fail. However, it is of little use to us,
                # which conversion failed. /If any/ conversion fails, it means, that that we do
                # not understand how to make sense of the current dumpfilename. Hence we cannot use
                # it as prefetch object and we have to drop it, to avoid passing a useless file
                # to the text pass. (This could days as of a comment below, but by not passing
                # a likely useless file, we have to fetch more texts from the database)
                #
                # Therefore try...except-ing the whole block is sufficient: If whatever error
                # occurs, we do not abort, but skip the file for prefetch.
                try:
                    # If we could properly parse
                    first_page_id_in_file = int(dfname.first_page_id)

                    # fixme what do we do here? this could be very expensive. is that worth it??
                    if not dfname.last_page_id:
                        # (b) nasty hack, see (a)
                        # it's not a checkpoint fle or we'd have the pageid in the filename
                        # so... temporary hack which will give expensive results
                        # if file part, and it's the last one, put none
                        # if it's not the last part, get the first pageid in the next
                        #  part and subtract 1
                        # if not file part, put none.
                        if dfname.is_file_part and dfname.partnum_int < maxparts:
                            for filename in file_list:
                                if filename.partnum_int == dfname.partnum_int + 1:
                                    # not true!  this could be a few past where it really is
                                    # (because of deleted pages that aren't included at all)
                                    dfname.last_page_id = str(int(filename.first_page_id) - 1)
                    if dfname.last_page_id:
                        last_page_id_in_file = int(dfname.last_page_id)
                    else:
                        last_page_id_in_file = None

                    # FIXME there is no point in including files that have just a
                    # few rev ids in them that we need, and having to read through
                    # the whole file... could take hours or days (later it won't matter,
                    # right? but until a rewrite, this is important)
                    # also be sure that if a critical page is deleted by the time we
                    # try to figure out ranges, that we don't get hosed
                    if ((first_page_id_in_file <= int(start_page_id) and
                         (last_page_id_in_file is None or
                          last_page_id_in_file >= int(start_page_id))) or
                            (first_page_id_in_file >= int(start_page_id) and
                             (end_page_id is None or
                              first_page_id_in_file <= int(end_page_id)))):
                        possibles.append(dfname)
                except Exception as ex:
                    runner.debug(
                        "Couldn't process %s for prefetch. Format update? Corrupt file?"
                        % dfname.filename)
        return possibles

    def _find_previous_dump(self, runner, partnum=None):
        """
        this finds the content file or files from the first previous successful dump
        to be used as input ("prefetch") for this run.

        args:
            Runner, partnum (string of digits)
        returns:
            list of DumpFilename
        """
        if partnum:
            start_page_id = sum([self.prefetchinfo['parts'][i]
                                 for i in range(0, int(partnum) - 1)]) + 1
            if len(self.prefetchinfo['parts']) > int(partnum):
                end_page_id = sum([self.prefetchinfo['parts'][i]
                                   for i in range(0, int(partnum))])
            else:
                end_page_id = None
        else:
            start_page_id = 1
            end_page_id = None

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

            # first check if there are checkpoint files from this run we can use
            dfnames = get_checkpt_files(
                runner.dump_dir, [self.jobinfo['dumpname']], self.jobinfo['ftype'],
                self.jobinfo['fext'], date, parts=None)
            possible_prefetch_dfnames = self.get_relevant_prefetch_files(
                dfnames, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_dfnames):
                return possible_prefetch_dfnames

            # ok, let's check for file parts instead, from any run
            # (may not conform to our numbering for this job)
            dfnames = get_reg_files(
                runner.dump_dir, [self.jobinfo['dumpname']], self.jobinfo['ftype'],
                self.jobinfo['fext'], date, parts=True)
            possible_prefetch_dfnames = self.get_relevant_prefetch_files(
                dfnames, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_dfnames):
                return possible_prefetch_dfnames

            # last shot, get output file that contains all the pages, if there is one
            dfnames = get_reg_files(
                runner.dump_dir, [self.jobinfo['dumpname']],
                self.jobinfo['ftype'], self.jobinfo['fext'], date, parts=False)
            # there is only one, don't bother to check for relevance :-P
            possible_prefetch_dfnames = dfnames
            dfnames = []
            for prefetch_dfname in possible_prefetch_dfnames:
                possible_path = runner.dump_dir.filename_public_path(prefetch_dfname, date)
                size = os.path.getsize(possible_path)
                if size < 70000:
                    runner.debug("small %d-byte prefetch dump at %s, skipping" % (
                        size, possible_path))
                    continue
                else:
                    dfnames.append(prefetch_dfname)
            if len(dfnames):
                return dfnames

        runner.debug("Could not locate a prefetchable dump.")
        return None

    def get_prefetch(self, runner, output_dfname, stub_file):
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
                # if we are doing partial stub run, include only the analogous
                # checkpointed prefetch files, if there are checkpointed files
                # otherwise we'll use the all the sourcefiles reported
                if not self.chkptfile_in_pagerange(stub_file, sourcefile):
                    continue
                source_path = runner.dump_dir.filename_public_path(sourcefile, sourcefile.date)
                if exists(source_path):
                    sources.append(source_path)

        if output_dfname.partnum:
            partnum_str = "%s" % stub_file.partnum
        else:
            partnum_str = ""
        if len(sources) > 0:
            source = "bzip2:%s" % (";".join(sources))
            runner.show_runner_state("... building %s %s XML dump, with text prefetch from %s..." %
                                     (self.jobinfo['subset'], partnum_str, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.show_runner_state("... building %s %s XML dump, no text prefetch..." %
                                     (self.jobinfo['subset'], partnum_str))
            prefetch = ""
        return prefetch

    def chkptfile_in_pagerange(self, dfname, chkpt_dfname):
        """
        return False if both files are checkpoint files (with page ranges)
        and the second file page range does not overlap with the first one

        args: DumpFilename, checkpoint file DumpFilename
        """
        # not both checkpoint files:
        if not dfname.is_checkpoint_file or not chkpt_dfname.is_checkpoint_file:
            return True
        # one or both end values are missing:
        if not dfname.last_page_id and not chkpt_dfname.last_page_id:
            return True
        elif not dfname.last_page_id and int(chkpt_dfname.last_page_id) < int(dfname.first_page_id):
            return True
        elif (not chkpt_dfname.last_page_id and
              int(dfname.last_page_id) < int(chkpt_dfname.first_page_id)):
            return True
        # have end values for both files:
        elif (int(dfname.first_page_id) <= int(chkpt_dfname.first_page_id) and
              int(chkpt_dfname.first_page_id) <= int(dfname.last_page_id)):
            return True
        elif (int(chkpt_dfname.first_page_id) <= int(dfname.first_page_id) and
              int(dfname.first_page_id) <= int(chkpt_dfname.last_page_id)):
            return True
        else:
            return False


class XmlDump(Dump):
    """Primary XML dumps, one section at a time."""
    def __init__(self, subset, name, desc, detail, item_for_stubs, prefetch,
                 prefetchdate, spawn,
                 wiki, partnum_todo, parts=False, checkpoints=False, checkpoint_file=None,
                 page_id_range=None, verbose=False):
        self._subset = subset
        self._detail = detail
        self._desc = desc
        self._prefetch = prefetch
        self._prefetchdate = prefetchdate
        self._spawn = spawn
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
            self.onlyparts = True
        self._page_id = {}
        self._partnum_todo = partnum_todo

        self.wiki = wiki
        self.item_for_stubs = item_for_stubs
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        self.page_id_range = page_id_range
        self.verbose = verbose
        self._prerequisite_items = [self.item_for_stubs]
        self._check_truncation = True
        self.stubber = StubProvider(
            self.wiki, {'dumpname': self.get_dumpname(), 'parts': self._parts,
                        'dumpnamebase': self.get_dumpname_base(),
                        'item_for_stubs': self.item_for_stubs,
                        'partnum_todo': self._partnum_todo}, self.verbose)
        Dump.__init__(self, name, desc, self.verbose)

    def get_dumpname_base(self):
        return 'pages-'

    def get_dumpname(self):
        return self.get_dumpname_base() + self._subset

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "bz2"

    def get_chkptfile_from_pageids(self):
        if ',' in self.page_id_range:
            first_page_id, last_page_id = self.page_id_range.split(',', 1)
        else:
            first_page_id = self.page_id_range
            last_page_id = "00000"  # indicates no last page id specified, go to end of stub
        checkpoint_string = DumpFilename.make_checkpoint_string(first_page_id, last_page_id)
        if self._partnum_todo:
            partnum = self._partnum_todo
        else:
            # fixme is that right? maybe NOT
            partnum = None
        dfname = DumpFilename(self.get_dumpname(), self.wiki.date, self.get_filetype(),
                              self.get_file_ext(), partnum, checkpoint_string)
        return dfname.filename

    def get_missing_before(self, needed_range, have_range):
        # given range of numbers needed and range of numbers we have,
        # return range of numbers needed before first number we have,
        # or None if none
        if have_range is None:
            return needed_range
        elif needed_range is None or int(have_range[0]) <= int(needed_range[0]):
            return None
        else:
            return (needed_range[0], str(int(have_range[0]) - 1), needed_range[2])

    def find_missing_ranges(self, needed, have):
        # given list tuples of ranges of numbers needed, and ranges of numbers we have,
        # determine the ranges of numbers missing and return list of said tuples
        needed_index = 0
        have_index = 0
        missing = []

        if not needed:
            return missing
        if not have:
            return needed

        needed_range = needed[needed_index]
        have_range = have[have_index]

        while True:
            # if we're out of haves, append everything we need
            if have_range is None:
                missing.append(needed_range)
                needed_index += 1
                if needed_index < len(needed):
                    needed_range = needed[needed_index]
                else:
                    # end of needed. done
                    return missing

            before_have = self.get_missing_before(needed_range, have_range)

            # write anything we don't have
            if before_have is not None:
                missing.append(before_have)

            # if we haven't already exhausted all the ranges we have...
            if have_range is not None:
                # skip over the current range of what we have
                skip_up_to = str(int(have_range[1]) + 1)
                while int(needed_range[1]) < int(skip_up_to):
                    needed_index += 1
                    if needed_index < len(needed):
                        needed_range = needed[needed_index]
                    else:
                        # end of needed. done
                        return missing

                if int(needed_range[0]) < int(skip_up_to):
                    needed_range = (skip_up_to, needed_range[1], needed_range[2])

                # get the next range we have
                have_index += 1
                if have_index < len(have):
                    have_range = have[have_index]
                else:
                    have_range = None

        return missing

    def chkpt_file_from_page_range(self, page_range, partnum):
        checkpoint_string = DumpFilename.make_checkpoint_string(
            page_range[0], page_range[1])
        output_dfname = DumpFilename(self.wiki, self.wiki.date, self.dumpname,
                                     self.get_filetype(), self.get_file_ext(),
                                     partnum, checkpoint=checkpoint_string,
                                     temp=False)
        return output_dfname

    def cleanup_tmp_files(self, dump_dir, runner):
        """
        with checkpoint files turned on, this job writes output
        to <something>.xml<-maybemorestuff>.bz2-tmp
        and if those files are lying around after such a job dies,
        we should clean them up
        """
        if "cleanup_tmp_files" not in runner.enabled:
            return

        # if we don't have the lock it's possible some
        # other process is writing tmp files, don't touch
        locker = Locker(self.wiki, self.wiki.date)
        lockfiles = locker.is_locked()
        if not lockfiles:
            return
        if len(lockfiles) > 1:
            # more than one process with the lock? should not
            # be possible, but if it is... touch nothing!
            return
        if not locker.check_owner(lockfiles[0], str(os.getpid())):
            return

        to_delete = self.get_tmp_files(dump_dir)
        for finfo in to_delete:
            if exists(dump_dir.filename_public_path(finfo)):
                os.remove(dump_dir.filename_public_path(finfo))
            elif exists(dump_dir.filename_private_path(finfo)):
                os.remove(dump_dir.filename_private_path(finfo))

    def run(self, runner):
        # here we will either clean up or not depending on how we were called
        # FIXME callers should set this appropriately and they don't right now
        self.cleanup_old_files(runner.dump_dir, runner)

        # clean up all tmp output files from previous attempts of this job
        # for this dump wiki and date, otherwise we'll wind up indexing
        # them and hashsumming them etc.
        # they may have been left around from an interrupted or failed earlier
        # run
        self.cleanup_tmp_files(runner.dump_dir, runner)

        commands = []

        dfnames_todo = []

        if self.page_id_range is not None:
            # convert to checkpoint filename, handle the same way
            self.checkpoint_file = self.get_chkptfile_from_pageids()

        if self.checkpoint_file:
            dfnames_todo = [self.checkpoint_file]
        else:
            # list all the output files that would be produced w/o
            # checkpoint files on
            out_dfnames = self.get_reg_files_for_filepart_possible(
                runner.dump_dir, self.get_fileparts_list(), self.list_dumpnames())
            if self._checkpoints_enabled:
                # get the stub list that would be used for the current run
                stub_dfnames = self.stubber.get_stub_files(runner)
                stub_dfnames = sorted(stub_dfnames, key=lambda thing: thing.filename)

                # get the page ranges covered by stubs
                stub_ranges = []
                for stub_dfname in stub_dfnames:
                    dcontents = DumpContents(self.wiki,
                                             runner.dump_dir.filename_public_path(
                                                 stub_dfname, stub_dfname.date),
                                             stub_dfname, self.verbose)
                    stub_ranges.append((dcontents.find_first_page_id_in_file(),
                                        self.find_last_page_id(stub_dfname, runner),
                                        stub_dfname.partnum))

                # get list of existing checkpoint files
                chkpt_dfnames = self.list_checkpt_files(
                    runner.dump_dir, [self.dumpname], runner.wiki.date, parts=None)
                chkpt_dfnames = sorted(chkpt_dfnames, key=lambda thing: thing.filename)
                # get the page ranges covered by existing checkpoint files
                checkpoint_ranges = [(chkptfile.first_page_id, chkptfile.last_page_id,
                                      chkptfile.partnum)
                                     for chkptfile in chkpt_dfnames]
                if self.verbose:
                    print "checkpoint_ranges is", checkpoint_ranges
                    print "stub_ranges is", stub_ranges

                if not checkpoint_ranges:
                    # no page ranges covered by checkpoints. do all output files
                    # the usual way
                    dfnames_todo = out_dfnames
                else:
                    dfnames_todo = []
                    missing_ranges = self.find_missing_ranges(stub_ranges, checkpoint_ranges)
                    parts = self.get_fileparts_list()
                    for partnum in parts:
                        if not [1 for chkpt_range in checkpoint_ranges
                                if int(chkpt_range[2]) == partnum]:
                            # entire page range for a particular file part (subjob)
                            # is missing so generate the regular output file
                            dfnames_todo.extend([out_dfname for out_dfname in out_dfnames
                                                 if int(out_dfname.partnum) == partnum])
                        else:
                            # at least some page ranges are covered, just do those that
                            # are missing (maybe none are and list is empty)
                            dfnames_todo.extend(
                                [self.chkpt_file_from_page_range((first, last), part)
                                 for (first, last, part) in missing_ranges
                                 if int(part) == partnum])
            else:
                # do the missing files only
                dfnames_todo = [out_dfname for out_dfname in out_dfnames
                                if not os.path.exists(
                                    runner.dump_dir.filename_public_path(out_dfname))]

        partial_stubs = self.stubber.get_partial_stubs(dfnames_todo, runner)
        if partial_stubs:
            stub_files = partial_stubs
        else:
            # what was this about? dunno
            return

        for stub_file in stub_files:
            series = self.build_command(runner, stub_file)
            commands.append(series)

        error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing xml file(s) %s" % self.dumpname)

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--current"

    # takes name of the output file
    def build_filters(self, runner, input_dfname):
        """
        Construct the output filter options for dumpTextPass.php
        args:
            Runner, DumpFilename
        """
        # do we need checkpoints? ummm
        xmlbz2_path = runner.dump_dir.filename_public_path(input_dfname)

        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if self.wiki.config.bzip2[-6:] == "dbzip2":
            bz2mode = "dbzip2"
        else:
            bz2mode = "bzip2"
        return "--output=%s:%s" % (bz2mode, xmlbz2_path)

    def get_last_lines_from_n(self, dfname, runner, count):
        if not dfname.filename or not exists(runner.dump_dir.filename_public_path(dfname)):
            return None

        dumpfile = DumpContents(self.wiki,
                                runner.dump_dir.filename_public_path(dfname, self.wiki.date),
                                dfname, self.verbose)
        pipeline = dumpfile.setup_uncompression_command()

        tail = self.wiki.config.tail
        if not exists(tail):
            raise BackupError("tail command %s not found" % tail)
        tail_esc = MiscUtils.shell_escape(tail)
        pipeline.append([tail, "-n", "+%s" % count])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        last_lines = ""
        if (proc.exited_successfully() or
                (proc.get_failed_cmds_with_retcode() ==
                 [[-signal.SIGPIPE, pipeline[0]]]) or
                (proc.get_failed_cmds_with_retcode() ==
                 [[signal.SIGPIPE + 128, pipeline[0]]])):
            last_lines = proc.output()
        return last_lines

    def get_lineno_last_page(self, dfname, runner):
        if not dfname.filename or not exists(runner.dump_dir.filename_public_path(dfname)):
            return None
        dumpfile = DumpContents(self.wiki,
                                runner.dump_dir.filename_public_path(dfname, self.wiki.date),
                                dfname, self.verbose)
        pipeline = dumpfile.setup_uncompression_command()
        grep = self.wiki.config.grep
        if not exists(grep):
            raise BackupError("grep command %s not found" % grep)
        pipeline.append([grep, "-n", "<page>"])
        tail = self.wiki.config.tail
        if not exists(tail):
            raise BackupError("tail command %s not found" % tail)
        pipeline.append([tail, "-1"])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        if (proc.exited_successfully() or
                (proc.get_failed_cmds_with_retcode() ==
                 [[-signal.SIGPIPE, pipeline[0]]]) or
                (proc.get_failed_cmds_with_retcode() ==
                 [[signal.SIGPIPE + 128, pipeline[0]]])):
            output = proc.output()
            # 339915646:  <page>
            if ':' in output:
                linecount = output.split(':')[0]
                if linecount.isdigit():
                    return linecount
        return None

    def find_last_page_id(self, dfname, runner):
        """
        find and return the last page id in a compressed
        stub or page content xml file, using uncompression
        command (bzcat, zcat) and tail

        arg: DumpFilename, Runner
        """
        count = self.get_lineno_last_page(dfname, runner)
        lastlines = self.get_last_lines_from_n(dfname, runner, count)
        # now look for the last page id in here. eww
        if not lastlines:
            return None
        title_and_id_pattern = re.compile(r'<title>(?P<title>.+?)</title>\s*' +
                                          r'(<ns>[0-9]+</ns>\s*)?' +
                                          r'<id>(?P<pageid>\d+?)</id>')
        result = None
        for result in re.finditer(title_and_id_pattern, lastlines):
            pass
        if result is not None:
            return result.group('pageid')
        else:
            return None

    def build_command(self, runner, stub_dfname):
        """
        Build the command line for the dump, minus output and filter options
        args:
            Runner, stub DumpFilename, ....
        """
        # we write a temp file, it will be checkpointed every so often.
        temp = bool(self._checkpoints_enabled)

        output_dfname = DumpFilename(self.wiki, stub_dfname.date, self.get_dumpname(),
                                     self.get_filetype(), self.file_ext, stub_dfname.partnum,
                                     DumpFilename.make_checkpoint_string(stub_dfname.first_page_id,
                                                                         stub_dfname.last_page_id),
                                     temp)

        stub_path = os.path.join(self.wiki.config.temp_dir, stub_dfname.filename)
        if os.path.exists(stub_path):
            # if this is a partial stub file in temp dir, use that
            stub_option = "--stub=gzip:%s" % stub_path
        else:
            # use regular stub file
            stub_option = "--stub=gzip:%s" % runner.dump_dir.filename_public_path(stub_dfname)

        if self._prefetch:
            prefetcher = Prefetch(self.wiki,
                                  {'name': self.name(), 'desc': self._desc,
                                   'dumpname': self.dumpname,
                                   'ftype': self.file_type, 'fext': self.file_ext},
                                  {'date': self._prefetchdate, 'parts': self._parts,
                                   'subset': self._subset},
                                  self.verbose)
            prefetch = prefetcher.get_prefetch(runner, output_dfname, stub_dfname)
        else:
            prefetch = ""

        if self._spawn:
            spawn = "--spawn=%s" % (self.wiki.config.php)
        else:
            spawn = ""

        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)

        if self._checkpoints_enabled:
            checkpoint_time = "--maxtime=%s" % (self.wiki.config.checkpoint_time)
            checkpoint_file = "--checkpointfile=%s" % output_dfname.new_filename(
                output_dfname.dumpname, output_dfname.file_type, output_dfname.file_ext,
                output_dfname.date, output_dfname.partnum, "p%sp%s", None)
        else:
            checkpoint_time = ""
            checkpoint_file = ""
        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpTextPass.php")
        dump_command = [self.wiki.config.php]
        dump_command.extend(script_command)
        dump_command.extend(["--wiki=%s" % runner.db_name,
                             "%s" % stub_option,
                             "%s" % prefetch,
                             "%s" % checkpoint_time,
                             "%s" % checkpoint_file,
                             "--report=1000",
                             "%s" % spawn])

        dump_command = [entry for entry in dump_command if entry is not None]
        command = dump_command
        filters = self.build_filters(runner, output_dfname)
        eta = self.build_eta(runner)
        command.extend([filters, eta])
        pipeline = [command]
        series = [pipeline]
        return series

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_nicely(self, mylist):
        """ Sort the given list in the way that humans expect.
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        mylist = sorted(mylist, key=alphanum_key)

    def get_tmp_files(self, dump_dir, dump_names=None):
        """
        list temporary output files currently existing
        returns:
            list of DumpFilename
        """
        dfnames = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        return [dfname for dfname in dfnames if dfname.is_temp_file]

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        """
        list output files including checkpoint files currently existing
        (from the dump run for the current wiki and date), in case
        we have been requested to clean up before a retry

        args:
            DumpDir, list of dump names ("stub-meta-history", ...)
        returns:
            list of DumpFilename
        """
        dfnames = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        dfnames_to_return = []

        if self.page_id_range:
            # this file is for one page range only
            if ',' in self.page_id_range:
                (first_page_id, last_page_id) = self.page_id_range.split(',', 2)
                first_page_id = int(first_page_id)
                last_page_id = int(last_page_id)
            else:
                first_page_id = int(self.page_id_range)
                last_page_id = None

            # checkpoint files cover specific page ranges. for those,
            # list only files within the given page range for cleanup
            for dfname in dfnames:
                if dfname.is_checkpoint_file:
                    if (not first_page_id or
                            (dfname.first_page_id and
                             (int(dfname.first_page_id) >= first_page_id))):
                        if (not last_page_id or
                                (dfname.last_page_id and
                                 (int(dfname.last_page_id) <= last_page_id))):
                            dfnames_to_return.append(dfname)
                else:
                    dfnames_to_return.append(dfname)
        else:
            dfnames_to_return = dfnames

        return dfnames_to_return


class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"
