'''
All xml dump jobs are defined here
'''

import os
import re
from os.path import exists

from dumps.exceptions import BackupError
from dumps.fileutils import DumpFile, DumpFilename
from dumps.utils import MultiVersion
from dumps.jobs import Dump


class XmlStub(Dump):
    """Create lightweight skeleton dumps, minus bulk text.
    A second pass will import text from prior dumps or the database to make
    full files for the public."""

    def __init__(self, name, desc, chunkToDo, chunks=False, checkpoints=False):
        self._chunk_todo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
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
            outf.file_ext, outf.chunk, outf.checkpoint, outf.temp))
        current_file = runner.dump_dir.filename_public_path(DumpFilename(
            runner.wiki, outf.date, self.current_dump_name, outf.file_type,
            outf.file_ext, outf.chunk, outf.checkpoint, outf.temp))
#        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        command = ["/usr/bin/python", "xmlstubs.py", "--config", runner.wiki.config.files[0],
                   "--wiki", runner.db_name, "--articles", articles_file,
                   "--history", history_file, "--current", current_file]

        if outf.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._chunks[i] for i in range(0, outf.chunk_int-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if outf.chunk_int < len(self._chunks):
                end = sum([self._chunks[i] for i in range(0, outf.chunk_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        files = self.list_outfiles_for_build_command(runner.dump_dir)
        for fname in files:
            # choose arbitrarily one of the dump_names we do (= articles_dump_name)
            # buildcommand will figure out the files for the rest
            if fname.dumpname == self.articles_dump_name:
                series = self.build_command(runner, fname)
                commands.append(series)
        error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing stub files")


class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, chunks=False):
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
    def __init__(self, subset, name, desc, detail, item_for_stubs, prefetch, spawn,
                 wiki, chunkToDo, chunks=False, checkpoints=False, checkpoint_file=None,
                 page_id_range=None, verbose=False):
        self._subset = subset
        self._detail = detail
        self._desc = desc
        self._prefetch = prefetch
        self._spawn = spawn
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
        self._page_id = {}
        self._chunk_todo = chunkToDo

        self.wiki = wiki
        self.item_for_stubs = item_for_stubs
        if checkpoints:
            self._checkpoints_enabled = True
        self.checkpoint_file = checkpoint_file
        if self.checkpoint_file is not None:
            # we don't checkpoint the checkpoint file.
            self._checkpoints_enabled = False
        self.page_id_range = page_id_range
        self._prerequisite_items = [self.item_for_stubs]
        self._check_truncation = True
        Dump.__init__(self, name, desc)

    def get_dumpname_base(self):
        return 'pages-'

    def get_dumpname(self):
        return self.get_dumpname_base() + self._subset

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "bz2"

    def run(self, runner):
        commands = []
        self.cleanup_old_files(runner.dump_dir, runner)
        # just get the files pertaining to our dumpname, which is *one* of
        # articles, pages-current, pages-history.
        # stubs include all of them together.
        if not self.dumpname.startswith(self.get_dumpname_base()):
            raise BackupError("dumpname %s of unknown form for this job" % self.dumpname)
        dumpname = self.dumpname[len(self.get_dumpname_base()):]
        stub_dumpnames = self.item_for_stubs.list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_files = self.item_for_stubs.list_outfiles_for_input(runner.dump_dir, [stub_dumpname])
        if self._chunks_enabled and self._chunk_todo:
            # reset inputfiles to just have the one we want.
            for inp_file in input_files:
                if inp_file.chunk_int == self._chunk_todo:
                    input_files = [inp_file]
                    break
            if len(input_files) > 1:
                raise BackupError("Trouble finding stub files for xml dump run")

        if self.checkpoint_file is not None:
            # fixme this should be an input file, not the output checkpoint file. move
            # the code out of build_command that does the conversion and put it here.
            series = self.build_command(runner, self.checkpoint_file)
            commands.append(series)
        else:
            for inp_file in input_files:
                # output_file = DumpFilename(self.wiki, inp_file.date, inp_file.dumpname,
                #                            inp_file.file_type, self.file_ext)
                series = self.build_command(runner, inp_file)
                commands.append(series)

        error = runner.run_command(commands, callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error producing xml file(s) %s" % self.dumpname)

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

    def write_partial_stub(self, input_file, output_file, start_page_id, end_page_id, runner):
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
        if end_page_id:
            command = [command1 + ("| %s %s %s |" % (self.wiki.config.writeuptopageid,
                                                     start_page_id, end_page_id)) + command2]
        else:
            # no lastpageid? read up to eof of the specific stub file that's used for input
            command = [command1 + ("| %s %s |" % (self.wiki.config.writeuptopageid,
                                                  start_page_id)) + command2]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], shell=True)
        if error:
            raise BackupError("failed to write partial stub file %s" % output_file.filename)

    def build_command(self, runner, outfile):
        """Build the command line for the dump, minus output and filter options"""

        if self.checkpoint_file is not None:
            output_file = outfile
        elif self._checkpoints_enabled:
            # we write a temp file, it will be checkpointed every so often.
            output_file = DumpFilename(self.wiki, outfile.date, self.dumpname,
                                       outfile.file_type, self.file_ext, outfile.chunk,
                                       outfile.checkpoint, temp=True)
        else:
            # we write regular files
            output_file = DumpFilename(self.wiki, outfile.date, self.dumpname,
                                       outfile.file_type, self.file_ext, outfile.chunk,
                                       checkpoint=False, temp=False)

        # Page and revision data pulled from this skeleton dump...
        # FIXME we need the stream wrappers for proper use of writeupto. this is a hack.
        if self.checkpoint_file is not None or self.page_id_range:
            # fixme I now have this code in a couple places, make it a function.
            if not self.dumpname.startswith(self.get_dumpname_base()):
                raise BackupError("dumpname %s of unknown form for this job" % self.dumpname)
            dumpname = self.dumpname[len(self.get_dumpname_base()):]
            stub_dumpnames = self.item_for_stubs.list_dumpnames()
            for sname in stub_dumpnames:
                if sname.endswith(dumpname):
                    stub_dumpname = sname

        if self.checkpoint_file is not None:
            stub_input_filename = self.checkpoint_file.new_filename(
                stub_dumpname, self.item_for_stubs.get_filetype(),
                self.item_for_stubs.get_file_ext(), self.checkpoint_file.date,
                self.checkpoint_file.chunk)
            stub_input_file = DumpFilename(self.wiki)
            stub_input_file.new_from_filename(stub_input_filename)
            stub_output_filename = self.checkpoint_file.new_filename(
                stub_dumpname, self.item_for_stubs.get_filetype(),
                self.item_for_stubs.get_file_ext(), self.checkpoint_file.date,
                self.checkpoint_file.chunk, self.checkpoint_file.checkpoint)
            stub_output_file = DumpFilename(self.wiki)
            stub_output_file.new_from_filename(stub_output_filename)
            self.write_partial_stub(stub_input_file, stub_output_file,
                                    self.checkpoint_file.first_page_id,
                                    str(int(self.checkpoint_file.last_page_id) + 1), runner)
            stub_option = ("--stub=gzip:%s" % os.path.join(
                self.wiki.config.temp_dir, stub_output_file.filename))
        elif self.page_id_range:
            # two cases. redoing a specific chunk, OR no chunks,
            # redoing the whole output file. ouch, hope it isn't huge.
            if self._chunk_todo or not self._chunks_enabled:
                stub_input_file = outfile

            stub_output_filename = stub_input_file.new_filename(
                stub_dumpname, self.item_for_stubs.get_filetype(),
                self.item_for_stubs.get_file_ext(), stub_input_file.date,
                stub_input_file.chunk, stub_input_file.checkpoint)
            stub_output_file = DumpFilename(self.wiki)
            stub_output_file.new_from_filename(stub_output_filename)
            if ',' in self.page_id_range:
                (first_page_id, last_page_id) = self.page_id_range.split(',', 2)
            else:
                first_page_id = self.page_id_range
                last_page_id = None
            self.write_partial_stub(stub_input_file, stub_output_file,
                                    first_page_id, last_page_id, runner)

            stub_option = "--stub=gzip:%s" % os.path.join(self.wiki.config.temp_dir,
                                                          stub_output_file.filename)
        else:
            stub_option = "--stub=gzip:%s" % runner.dump_dir.filename_public_path(outfile)

        # Try to pull text from the previous run; most stuff hasn't changed
        # Source=$OutputDir/pages_$section.xml.bz2
        sources = []
        possible_sources = None
        if self._prefetch:
            possible_sources = self._find_previous_dump(runner, outfile.chunk)
            # if we have a list of more than one then
            # we need to check existence for each and put them together in a string
            if possible_sources:
                for sourcefile in possible_sources:
                    sname = runner.dump_dir.filename_public_path(sourcefile, sourcefile.date)
                    if exists(sname):
                        sources.append(sname)
        if outfile.chunk:
            chunkinfo = "%s" % outfile.chunk
        else:
            chunkinfo = ""
        if len(sources) > 0:
            source = "bzip2:%s" % (";".join(sources))
            runner.show_runner_state("... building %s %s XML dump, with text prefetch from %s..." %
                                     (self._subset, chunkinfo, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.show_runner_state("... building %s %s XML dump, no text prefetch..." %
                                     (self._subset, chunkinfo))
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
                output_file.date, output_file.chunk, "p%sp%s", None)
        else:
            checkpoint_time = ""
            checkpoint_file = ""
        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpTextPass.php")
        dump_command = ["%s" % self.wiki.config.php, "-q"]
        dump_command.extend(script_command)
        dump_command.extend(["--wiki=%s" % runner.db_name,
                             "%s" % stub_option,
                             "%s" % prefetch,
                             "%s" % checkpoint_time,
                             "%s" % checkpoint_file,
                             "--report=1000",
                             "%s" % spawn])

        dump_command = filter(None, dump_command)
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
        mylist.sort(key=alphanum_key)

    def get_relevant_prefetch_files(self, file_list, start_page_id, end_page_id, date, runner):
        possibles = []
        if len(file_list):
            # (a) nasty hack, see below (b)
            maxchunks = 0
            for file_obj in file_list:
                if file_obj.is_chunk_file and file_obj.chunk_int > maxchunks:
                    maxchunks = file_obj.chunk_int
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
                        # if chunk file, and it's the last chunk, put none
                        # if it's not the last chunk, get the first pageid in the next
                        #  chunk and subtract 1
                        # if not chunk, put none.
                        if file_obj.is_chunk_file and file_obj.chunk_int < maxchunks:
                            for fname in file_list:
                                if fname.chunk_int == file_obj.chunk_int + 1:
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
                except:
                    runner.debug(
                        "Couldn't process %s for prefetch. Format update? Corrupt file?"
                        % file_obj.filename)
        return possibles

    # this finds the content file or files from the first previous successful dump
    # to be used as input ("prefetch") for this run.
    def _find_previous_dump(self, runner, chunk=None):
        """The previously-linked previous successful dump."""
        if chunk:
            start_page_id = sum([self._chunks[i] for i in range(0, int(chunk)-1)]) + 1
            if len(self._chunks) > int(chunk):
                end_page_id = sum([self._chunks[i] for i in range(0, int(chunk))])
            else:
                end_page_id = None
        else:
            start_page_id = 1
            end_page_id = None

        dumps = self.wiki.dump_dirs()
        dumps.sort()
        dumps.reverse()
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
            files = self.list_checkpt_files_existing(
                runner.dump_dir, [self.dumpname], date, chunks=None)
            possible_prefetch_list = self.get_relevant_prefetch_files(
                files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

            # ok, let's check for chunk files instead, from any run
            # (may not conform to our numbering for this job)
            files = self.list_reg_files_existing(
                runner.dump_dir, [self.dumpname], date, chunks=True)
            possible_prefetch_list = self.get_relevant_prefetch_files(
                files, start_page_id, end_page_id, date, runner)
            if len(possible_prefetch_list):
                return possible_prefetch_list

            # last shot, get output file that contains all the pages, if there is one
            files = self.list_reg_files_existing(runner.dump_dir, [self.dumpname],
                                                 date, chunks=False)
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
            if ',' in self.page_id_range:
                (first_page_id, last_page_id) = self.page_id_range.split(',', 2)
                first_page_id = int(first_page_id)
                last_page_id = int(last_page_id)
            else:
                first_page_id = int(self.page_id_range)
                last_page_id = None
            # filter any checkpoint files, removing from the list any with
            # page range outside of the page range this job will cover
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
        return files_to_return


class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    def build_eta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"


class AbstractDump(Dump):
    """XML dump for Yahoo!'s Active Abstracts thingy"""

    def __init__(self, name, desc, chunkToDo, db_name, chunks=False):
        self._chunk_todo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunks_enabled = True
            self.onlychunks = True
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
                                    fname.chunk, fname.checkpoint)
            outputs.append(runner.dump_dir.filename_public_path(file_obj))
            variants.append(variant_option)

            command.extend(["--outfiles=%s" % ",".join(outputs),
                            "--variants=%s" % ",".join(variants)])

        if fname.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._chunks[i] for i in range(0, fname.chunk_int-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if fname.chunk_int < len(self._chunks):
                end = sum([self._chunks[i] for i in range(0, fname.chunk_int)]) + 1
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
