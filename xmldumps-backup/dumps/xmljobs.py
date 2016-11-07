'''
All xml dump jobs are defined here
'''

import os
import re
from os.path import exists
import signal

from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFile, DumpFilename
from dumps.utils import MultiVersion, MiscUtils
from dumps.jobs import Dump


def batcher(items, batchsize):
    '''
    given a list of items and a batchsize, return
    list of batches (lists) of these items, each
    batch with batchsize items and the last batch
    with however many items are left over
    '''
    if batchsize == 0:
        return [items]
    return (items[pos:pos + batchsize] for pos in xrange(0, len(items), batchsize))


class XmlStub(Dump):
    """Create lightweight skeleton dumps, minus bulk text.
    A second pass will import text from prior dumps or the database to make
    full files for the public."""

    def __init__(self, name, desc, partnum_todo, jobsperbatch=None, parts=False, checkpoints=False):
        self._partnum_todo = partnum_todo
        self.jobsperbatch = jobsperbatch
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
            self.onlyparts = True
        self.history_dump_name = "stub-meta-history"
        self.current_dump_name = "stub-meta-current"
        self.articles_dump_name = "stub-articles"
        if checkpoints:
            self._checkpoints_enabled = True
        self._check_truncation = True
        Dump.__init__(self, name, desc)

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_dumpname(self):
        return 'stub'

    def list_dumpnames(self):
        dump_names = [self.history_dump_name, self.current_dump_name, self.articles_dump_name]
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_build_command(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_cleanup(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        if dump_names is None:
            dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return files

    def build_command(self, runner, outf):
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        articles_file = runner.dump_dir.filename_public_path(outf)
        history_file = runner.dump_dir.filename_public_path(DumpFilename(
            runner.wiki, outf.date, self.history_dump_name, outf.file_type,
            outf.file_ext, outf.partnum, outf.checkpoint, outf.temp))
        current_file = runner.dump_dir.filename_public_path(DumpFilename(
            runner.wiki, outf.date, self.current_dump_name, outf.file_type,
            outf.file_ext, outf.partnum, outf.checkpoint, outf.temp))
#        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        command = ["/usr/bin/python", "xmlstubs.py", "--config", runner.wiki.config.files[0],
                   "--wiki", runner.db_name, "--articles", articles_file,
                   "--history", history_file, "--current", current_file]

        if outf.partnum:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._parts[i] for i in range(0, outf.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if outf.partnum_int < len(self._parts):
                end = sum([self._parts[i] for i in range(0, outf.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        # pick out the articles_dump files, setting up the stubs command for these
        # will cover all the other cases, as we generate all three stub file types
        # (article, meta-current, meta-history) at once
        files = [fileinfo for fileinfo in files if fileinfo.dumpname == self.articles_dump_name]
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(files)
        for batch in batcher(files, maxjobs):
            commands = []
            for fname in batch:
                series = self.build_command(runner, fname)
                commands.append(series)
            error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                       callback_stderr_arg=runner)
            if error:
                raise BackupError("error producing stub files")


class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, parts=False):
        Dump.__init__(self, "xmlpagelogsdump", desc)

    def detail(self):
        return "This contains the log of actions performed on pages and users."

    def get_dumpname(self):
        return "pages-logging"

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_temp_filename(self, name, number):
        return name + "-" + str(number)

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(files) > 1:
            raise BackupError("logging table job wants to produce more than one output file")
        output_file_obj = files[0]
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)
#        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        logging = runner.dump_dir.filename_public_path(output_file_obj)

        command = ["/usr/bin/python", "xmllogs.py", "--config",
                   runner.wiki.config.files[0], "--wiki", runner.db_name,
                   "--outfile", logging]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error dumping log files")


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
        Dump.__init__(self, name, desc, self.verbose)

    def get_dumpname_base(self):
        return 'pages-'

    def get_dumpname(self):
        return self.get_dumpname_base() + self._subset

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "bz2"

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
           runner        - Runner object
           partnum (int) - number of file part (subjob) if any
        '''
        if partnum is None:
            partnum = self._partnum_todo
        if not self.dumpname.startswith(self.get_dumpname_base()):
            raise BackupError("dumpname %s of unknown form for this job" % self.dumpname)

        dumpname = self.dumpname[len(self.get_dumpname_base()):]
        stub_dumpnames = self.item_for_stubs.list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_files = self.item_for_stubs.list_outfiles_for_input(runner.dump_dir, [stub_dumpname])
        if self._parts_enabled:
            if partnum is not None:
                for inp_file in input_files:
                    if inp_file.partnum_int == partnum:
                        input_files = [inp_file]
                        break
        return input_files

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
        fileobj = DumpFilename(self.get_dumpname(), self.wiki.date, self.get_filetype(),
                               self.get_file_ext(), partnum, checkpoint_string)
        return fileobj.filename

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
        output_file = DumpFilename(self.wiki, self.wiki.date, self.dumpname,
                                   self.get_filetype(), self.get_file_ext(),
                                   partnum, checkpoint=checkpoint_string,
                                   temp=False)
        return output_file

    def run(self, runner):
        # here we will either clean up or not depending on how we were called FIXME
        self.cleanup_old_files(runner.dump_dir, runner)
        commands = []

        todo = []

        if self.page_id_range is not None:
            # convert to checkpoint filename, handle the same way
            self.checkpoint_file = self.get_chkptfile_from_pageids()

        if self.checkpoint_file:
            todo = [self.checkpoint_file]
        else:
            # list all the output files that would be produced w/o
            # checkpoint files on
            outfiles = self.get_reg_files_for_filepart_possible(
                runner.dump_dir, self.get_fileparts_list(), self.list_dumpnames())
            if self._checkpoints_enabled:

                # get the stub list that would be used for the current run
                stubs = self.get_stub_files(runner)
                stubs = sorted(stubs, key=lambda thing: thing.filename)

                # get the page ranges covered by stubs
                stub_ranges = []
                for stub in stubs:
                    fname = DumpFile(self.wiki,
                                     runner.dump_dir.filename_public_path(stub, stub.date),
                                     stub, self.verbose)
                    stub_ranges.append((fname.find_first_page_id_in_file(),
                                        self.find_last_page_id(stub, runner), stub.partnum))

                # get list of existing checkpoint files
                chkpt_files = self.list_checkpt_files(
                    runner.dump_dir, [self.dumpname], runner.wiki.date, parts=None)
                chkpt_files = sorted(chkpt_files, key=lambda thing: thing.filename)
                # get the page ranges covered by existing checkpoint files
                checkpoint_ranges = [(chkptfile.first_page_id, chkptfile.last_page_id,
                                      chkptfile.partnum)
                                     for chkptfile in chkpt_files]
                if self.verbose:
                    print "checkpoint_ranges is", checkpoint_ranges
                    print "stub_ranges is", stub_ranges

                if not checkpoint_ranges:
                    # no page ranges covered by checkpoints. do all output files
                    # the usual way
                    todo = outfiles
                else:
                    todo = []
                    parts = self.get_fileparts_list()
                    for partnum in parts:
                        if not [int(chkpt_range[2]) for chkpt_range in checkpoint_ranges
                                if int(chkpt_range[2]) == int(partnum)]:
                            # no page ranges covered by checkpoints for a particular
                            # file part (subjob) so do that output file the
                            # regular way
                            todo.extend([outfile for outfile in outfiles
                                         if int(outfile.partnum) == int(partnum)])

                    missing = self.find_missing_ranges(stub_ranges, checkpoint_ranges)
                    todo.extend([self.chkpt_file_from_page_range((first, last), partnum)
                                 for (first, last, partnum) in missing])

            else:
                # do the missing files only
                # FIXME public or private depending on the wiki!
                todo = [outfile for outfile in outfiles
                        if not os.path.exists(runner.dump_dir.filename_public_path(outfile))]

        partial_stubs = []
        if self.verbose:
            print "todo is", [to.filename for to in todo]
        for fileobj in todo:

            stub_for_file = self.get_stub_files(runner, fileobj.partnum_int)[0]

            if fileobj.first_page_id is None:
                partial_stubs.append(stub_for_file)
            else:
                stub_output_file = DumpFilename(
                    self.wiki, fileobj.date, fileobj.dumpname,
                    self.item_for_stubs.get_filetype(),
                    self.item_for_stubs.get_file_ext(),
                    fileobj.partnum,
                    DumpFilename.make_checkpoint_string(
                        fileobj.first_page_id, fileobj.last_page_id), temp=True)

                self.write_partial_stub(stub_for_file, stub_output_file, runner)
                if not self.has_no_entries(stub_output_file, runner):
                    partial_stubs.append(stub_output_file)

        if self.verbose:
            print "partial_stubs is", [ps.filename for ps in partial_stubs]
        if partial_stubs:
            stub_files = partial_stubs
        else:
            return

        for stub_file in stub_files:
            series = self.build_command(runner, stub_file)
            commands.append(series)

        error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing xml file(s) %s" % self.dumpname)

    def has_no_entries(self, xmlfile, runner):
        '''
        see if it has a page id in it or not. no? then return True
        '''
        if xmlfile.is_temp_file:
            path = os.path.join(self.wiki.config.temp_dir, xmlfile.filename)
        else:
            path = runner.dump_dir.filename_public_path(xmlfile, self.wiki.date)
        fname = DumpFile(self.wiki, path, xmlfile, self.verbose)
        return bool(fname.find_first_page_id_in_file() is None)

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--current"

    # takes name of the output file
    def build_filters(self, runner, inp_file):
        """Construct the output filter options for dumpTextPass.php"""
        # do we need checkpoints? ummm
        xmlbz2 = runner.dump_dir.filename_public_path(inp_file)

        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if self.wiki.config.bzip2[-6:] == "dbzip2":
            bz2mode = "dbzip2"
        else:
            bz2mode = "bzip2"
        return "--output=%s:%s" % (bz2mode, xmlbz2)

    def write_partial_stub(self, input_file, output_file, runner):
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" %
                              self.wiki.config.writeuptopageid)

        inputfile_path = runner.dump_dir.filename_public_path(input_file)
        output_file_path = os.path.join(self.wiki.config.temp_dir, output_file.filename)
        if input_file.file_ext == "gz":
            command1 = "%s -dc %s" % (self.wiki.config.gzip, inputfile_path)
            command2 = "%s > %s" % (self.wiki.config.gzip, output_file_path)
        elif input_file.file_ext == '7z':
            command1 = "%s e -si %s" % (self.wiki.config.sevenzip, inputfile_path)
            command2 = "%s e -so %s" % (self.wiki.config.sevenzip, output_file_path)
        elif input_file.file_ext == 'bz':
            command1 = "%s -dc %s" % (self.wiki.config.bzip2, inputfile_path)
            command2 = "%s > %s" % (self.wiki.config.bzip2, output_file_path)
        else:
            raise BackupError("unknown stub file extension %s" % input_file.file_ext)
        if output_file.last_page_id is not None and output_file.last_page_id is not "00000":
            command = [command1 + ("| %s %s %s |" % (self.wiki.config.writeuptopageid,
                                                     output_file.first_page_id,
                                                     output_file.last_page_id)) + command2]
        else:
            # no lastpageid? read up to eof of the specific stub file that's used for input
            command = [command1 + ("| %s %s |" % (self.wiki.config.writeuptopageid,
                                                  output_file.first_page_id)) + command2]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], shell=True)
        if error:
            raise BackupError("failed to write partial stub file %s" % output_file.filename)

    def get_last_lines_from_n(self, fileobj, runner, count):
        if not fileobj.filename or not exists(runner.dump_dir.filename_public_path(fileobj)):
            return None

        dumpfile = DumpFile(self.wiki,
                            runner.dump_dir.filename_public_path(fileobj, self.wiki.date),
                            fileobj, self.verbose)
        pipeline = dumpfile.setup_uncompression_command()

        tail = self.wiki.config.tail
        if not exists(tail):
            raise BackupError("tail command %s not found" % tail)
        tail_esc = MiscUtils.shell_escape(tail)
        pipeline.append([tail, "-n", "+%s" % count])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        if (proc.exited_successfully() or
            (proc.get_failed_cmds_with_retcode() ==
             [[-signal.SIGPIPE, pipeline[0]]]) or
            (proc.get_failed_cmds_with_retcode() ==
             [[signal.SIGPIPE + 128, pipeline[0]]])):
            last_lines = proc.output()
        return last_lines

    def get_lineno_last_page(self, fileobj, runner):
        if not fileobj.filename or not exists(runner.dump_dir.filename_public_path(fileobj)):
            return None
        dumpfile = DumpFile(self.wiki,
                            runner.dump_dir.filename_public_path(fileobj, self.wiki.date),
                            fileobj, self.verbose)
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

    def find_last_page_id(self, fileobj, runner):
        count = self.get_lineno_last_page(fileobj, runner)
        lastlines = self.get_last_lines_from_n(fileobj, runner, count)
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

    def build_command(self, runner, stub_file):
        """Build the command line for the dump, minus output and filter options"""

        # we write a temp file, it will be checkpointed every so often.
        temp = bool(self._checkpoints_enabled)

        output_file = DumpFilename(self.wiki, stub_file.date, self.dumpname,
                                   self.get_filetype(), self.file_ext, stub_file.partnum,
                                   DumpFilename.make_checkpoint_string(stub_file.first_page_id,
                                                                       stub_file.last_page_id),
                                   temp)

        stub_path = os.path.join(self.wiki.config.temp_dir, stub_file.filename)
        if os.path.exists(stub_path):
            # if this is a partial stub file in temp dir, use that
            stub_option = "--stub=gzip:%s" % stub_path
        else:
            # use regular stub file
            stub_option = "--stub=gzip:%s" % runner.dump_dir.filename_public_path(stub_file)

        # Try to pull text from the previous run; most stuff hasn't changed
        # Source=$OutputDir/pages_$section.xml.bz2
        sources = []
        possible_sources = None
        if self._prefetch:
            possible_sources = self._find_previous_dump(runner, output_file.partnum)
            # if we have a list of more than one then
            # we need to check existence for each and put them together in a string
            if possible_sources:
                for sourcefile in possible_sources:
                    sname = runner.dump_dir.filename_public_path(sourcefile, sourcefile.date)
                    if exists(sname):
                        sources.append(sname)
        if output_file.partnum:
            partnum_str = "%s" % stub_file.partnum
        else:
            partnum_str = ""
        if len(sources) > 0:
            source = "bzip2:%s" % (";".join(sources))
            runner.show_runner_state("... building %s %s XML dump, with text prefetch from %s..." %
                                     (self._subset, partnum_str, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.show_runner_state("... building %s %s XML dump, no text prefetch..." %
                                     (self._subset, partnum_str))
            prefetch = ""

        if self._spawn:
            spawn = "--spawn=%s" % (self.wiki.config.php)
        else:
            spawn = ""

        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)

        if self._checkpoints_enabled:
            checkpoint_time = "--maxtime=%s" % (self.wiki.config.checkpoint_time)
            checkpoint_file = "--checkpointfile=%s" % output_file.new_filename(
                output_file.dumpname, output_file.file_type, output_file.file_ext,
                output_file.date, output_file.partnum, "p%sp%s", None)
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
        filters = self.build_filters(runner, output_file)
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

    def get_relevant_prefetch_files(self, file_list, start_page_id, end_page_id, date, runner):
        possibles = []
        if len(file_list):
            # (a) nasty hack, see below (b)
            maxparts = 0
            for file_obj in file_list:
                if file_obj.is_file_part and file_obj.partnum_int > maxparts:
                    maxparts = file_obj.partnum_int
                if not file_obj.first_page_id:
                    fname = DumpFile(
                        self.wiki, runner.dump_dir.filename_public_path(file_obj, date),
                        file_obj, self.verbose)
                    file_obj.first_page_id = fname.find_first_page_id_in_file()

            # get the files that cover our range
            for file_obj in file_list:
                # If some of the file_objs in file_list could not be properly be parsed, some of
                # the (int) conversions below will fail. However, it is of little use to us,
                # which conversion failed. /If any/ conversion fails, it means, that that we do
                # not understand how to make sense of the current file_obj. Hence we cannot use
                # it as prefetch object and we have to drop it, to avoid passing a useless file
                # to the text pass. (This could days as of a comment below, but by not passing
                # a likely useless file, we have to fetch more texts from the database)
                #
                # Therefore try...except-ing the whole block is sufficient: If whatever error
                # occurs, we do not abort, but skip the file for prefetch.
                try:
                    # If we could properly parse
                    first_page_id_in_file = int(file_obj.first_page_id)

                    # fixme what do we do here? this could be very expensive. is that worth it??
                    if not file_obj.last_page_id:
                        # (b) nasty hack, see (a)
                        # it's not a checkpoint fle or we'd have the pageid in the filename
                        # so... temporary hack which will give expensive results
                        # if file part, and it's the last one, put none
                        # if it's not the last part, get the first pageid in the next
                        #  part and subtract 1
                        # if not file part, put none.
                        if file_obj.is_file_part and file_obj.partnum_int < maxparts:
                            for fname in file_list:
                                if fname.partnum_int == file_obj.partnum_int + 1:
                                    # not true!  this could be a few past where it really is
                                    # (because of deleted pages that aren't included at all)
                                    file_obj.last_page_id = str(int(fname.first_page_id) - 1)
                    if file_obj.last_page_id:
                        last_page_id_in_file = int(file_obj.last_page_id)
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
                        possibles.append(file_obj)
                except Exception as ex:
                    runner.debug(
                        "Couldn't process %s for prefetch. Format update? Corrupt file?"
                        % file_obj.filename)
        return possibles

    # this finds the content file or files from the first previous successful dump
    # to be used as input ("prefetch") for this run.
    def _find_previous_dump(self, runner, partnum=None):
        """The previously-linked previous successful dump."""
        if partnum:
            start_page_id = sum([self._parts[i] for i in range(0, int(partnum) - 1)]) + 1
            if len(self._parts) > int(partnum):
                end_page_id = sum([self._parts[i] for i in range(0, int(partnum))])
            else:
                end_page_id = None
        else:
            start_page_id = 1
            end_page_id = None

        if self._prefetchdate:
            dumps = [self._prefetchdate]
        else:
            dumps = self.wiki.dump_dirs()
        dumps = sorted(dumps, reverse=True)
        for date in dumps:
            if date == self.wiki.date:
                runner.debug("skipping current dump for prefetch of job %s, date %s" %
                             (self.name(), self.wiki.date))
                continue

            # see if this job from that date was successful
            if not runner.dumpjobdata.runinfofile.status_of_old_dump_is_done(
                    runner, date, self.name(), self._desc):
                runner.debug("skipping incomplete or failed dump for prefetch date %s" % date)
                continue

            # first check if there are checkpoint files from this run we can use
            files = self.list_checkpt_files(
                runner.dump_dir, [self.dumpname], date, parts=None)
            possible_prefetch_list = self.get_relevant_prefetch_files(
                files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

            # ok, let's check for file parts instead, from any run
            # (may not conform to our numbering for this job)
            files = self.list_reg_files(
                runner.dump_dir, [self.dumpname], date, parts=True)
            possible_prefetch_list = self.get_relevant_prefetch_files(
                files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

            # last shot, get output file that contains all the pages, if there is one
            files = self.list_reg_files(runner.dump_dir, [self.dumpname],
                                        date, parts=False)
            # there is only one, don't bother to check for relevance :-P
            possible_prefetch_list = files
            files = []
            for prefetch in possible_prefetch_list:
                possible = runner.dump_dir.filename_public_path(prefetch, date)
                size = os.path.getsize(possible)
                if size < 70000:
                    runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, possible))
                    continue
                else:
                    files.append(prefetch)
            if len(files):
                return files

        runner.debug("Could not locate a prefetchable dump.")
        return None

    def list_outfiles_for_cleanup(self, dump_dir, dump_names=None):
        files = Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names)
        files_to_return = []

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
            for fname in files:
                if fname.is_checkpoint_file:
                    if (not first_page_id or
                            (fname.first_page_id and
                             (int(fname.first_page_id) >= first_page_id))):
                        if (not last_page_id or
                                (fname.last_page_id and
                                 (int(fname.last_page_id) <= last_page_id))):
                            files_to_return.append(fname)
                else:
                    files_to_return.append(fname)
        else:
            files_to_return = files

        return files_to_return


class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"


class AbstractDump(Dump):
    """XML dump for Yahoo!'s Active Abstracts thingy"""

    def __init__(self, name, desc, partnum_todo, db_name, parts=False):
        self._partnum_todo = partnum_todo
        self._parts = parts
        if self._parts:
            self._parts_enabled = True
            self.onlyparts = True
        self.db_name = db_name
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return "abstract"

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return ""

    def build_command(self, runner, fname):
        command = ["/usr/bin/python", "xmlabstracts.py", "--config",
                   runner.wiki.config.files[0], "--wiki", self.db_name]

        outputs = []
        variants = []
        for variant in self._variants():
            variant_option = self._variant_option(variant)
            dumpname = self.dumpname_from_variant(variant)
            file_obj = DumpFilename(runner.wiki, fname.date, dumpname,
                                    fname.file_type, fname.file_ext,
                                    fname.partnum, fname.checkpoint)
            outputs.append(runner.dump_dir.filename_public_path(file_obj))
            variants.append(variant_option)

            command.extend(["--outfiles=%s" % ",".join(outputs),
                            "--variants=%s" % ",".join(variants)])

        if fname.partnum:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._parts[i] for i in range(0, fname.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if fname.partnum_int < len(self._parts):
                end = sum([self._parts[i] for i in range(0, fname.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        # choose the empty variant to pass to buildcommand, it will fill in the rest if needed
        output_files = self.list_outfiles_for_build_command(runner.dump_dir)
        dumpname0 = self.list_dumpnames()[0]
        for fname in output_files:
            if fname.dumpname == dumpname0:
                series = self.build_command(runner, fname)
                commands.append(series)
        error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing abstract dump")

    # If the database name looks like it's marked as Chinese language,
    # return a list including Simplified and Traditional versions, so
    # we can build separate files normalized to each orthography.
    def _variants(self):
        if self.db_name[0:2] == "zh" and self.db_name[2:3] != "_":
            variants = ["", "zh-cn", "zh-tw"]
        else:
            variants = [""]
        return variants

    def _variant_option(self, variant):
        if variant == "":
            return ""
        else:
            return ":variant=%s" % variant

    def dumpname_from_variant(self, variant):
        dumpname_base = 'abstract'
        if variant == "":
            return dumpname_base
        else:
            return dumpname_base + "-" + variant

    def list_dumpnames(self):
        # need this first for build_command and other such
        dump_names = []
        variants = self._variants()
        for variant in variants:
            dump_names.append(self.dumpname_from_variant(variant))
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return files

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_build_command(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_cleanup(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return files

    def list_outfiles_for_input(self, dump_dir):
        dump_names = self.list_dumpnames()
        files = []
        files.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return files
