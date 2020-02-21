#!/usr/bin/python3
'''
All xml content dump jobs are defined here
'''

import os
from os.path import exists
import functools

from dumps.exceptions import BackupError
from dumps.fileutils import DumpContents, DumpFilename, FileUtils
from dumps.utils import MiscUtils


class StubProvider():
    """
    make or find stub files for use in page content dump
    """
    def __init__(self, wiki, jobinfo, verbose):
        self.wiki = wiki
        self.jobinfo = jobinfo
        self.verbose = verbose

    def get_stub_dfname(self, partnum, dump_dir):
        '''
        get the stub file pertaining to our dumpname
        (one of articles, pages-current, pages-history)
        and our desired subjob or page range etc.
        we will either return the one full stubs file that exists
        or the one stub file part, if we are (re)running a specific
        file part (subjob), or all file parts if we are (re)running
        the entire job which is configured for subjobs.

        arguments:
           partnum   - subjob number if any,
           runner    - Runner
        returns:
           DumpFilename
        '''
        if not self.jobinfo['dumpname'].startswith(self.jobinfo['dumpnamebase']):
            raise BackupError("dumpname %s of unknown form for this job" % self.jobinfo['dumpname'])

        dumpname = self.jobinfo['dumpname'][len(self.jobinfo['dumpnamebase']):]
        stub_dumpnames = self.jobinfo['item_for_stubs'].list_dumpnames()
        for sname in stub_dumpnames:
            if sname.endswith(dumpname):
                stub_dumpname = sname
        input_dfnames = self.jobinfo['item_for_stubs'].list_outfiles_for_input(
            self.jobinfo['item_for_stubs'].makeargs(dump_dir, [stub_dumpname]))
        if partnum is not None:
            input_dfnames = [dfname for dfname in input_dfnames
                             if dfname.partnum_int == int(partnum)]
        if len(input_dfnames) > 1:
            # this is an error
            return None
        return input_dfnames[0]

    def get_commands_for_temp_stubs(self, iofile_pairs, runner):
        """
        put the io file pairs in ascending order (per part if there
        are parts), produce commands for generating temp stub files
        for each pair, combining up those outputs that require the
        same input file into one command
        return those commands and the corresponding output DumpFilenames

        args: pairs of (DumpFilename, DumpFilename), Runner
        """
        if not iofile_pairs:
            return [], []

        # split up into batches where the input file is the same
        # and the pairs are ordered by output file name
        in_dfnames = [pair[0] for pair in iofile_pairs]
        # get just the unique ones
        in_dfnames_uniq = []
        for in_dfname in in_dfnames:
            if in_dfname.filename not in [dfname.filename for dfname in in_dfnames_uniq]:
                in_dfnames_uniq.append(in_dfname)

        out_dfnames = {}
        output_dfnames_to_check = []
        for in_dfname in in_dfnames_uniq:
            out_dfnames[in_dfname.filename] = sorted([pair[1] for pair in iofile_pairs
                                                      if pair[0].filename == in_dfname.filename],
                                                     key=functools.cmp_to_key(DumpFilename.compare))
        commands = []
        for in_dfname in in_dfnames_uniq:
            pipeline = self.get_stub_gen_cmd_for_input(
                in_dfname, out_dfnames[in_dfname.filename], runner)
            if pipeline is not None:
                # list of command series. each series is a list of pipelines.
                commands.append([pipeline])
                output_dfnames_to_check.extend(out_dfnames[in_dfname.filename])
        return commands, output_dfnames_to_check

    @staticmethod
    def run_temp_stub_commands(runner, commands, batchsize):
        """
        run the commands to generate the temp stub files, without
        output file checking
        """
        errors = False

        while commands:
            command_batch = commands[:batchsize]
            error, broken_pipelines = runner.run_command_without_errorcheck(command_batch)
            if error:
                for pipeline in broken_pipelines:
                    failed_cmds_retcodes = pipeline.get_failed_cmds_with_retcode()
                    for cmd_retcode in failed_cmds_retcodes:
                        if cmd_retcode[0] in MiscUtils.get_sigpipe_values():
                            pass
                        else:
                            runner.log_and_print("error from commands: %s" %
                                                 pipeline.pipeline_string())
                            errors = True

            commands = commands[batchsize:]
        if errors:
            raise BackupError("failed to write pagerange stub files")

    def check_temp_stubs(self, runner, move_if_truncated, output_dfnames):
        """
        check that temp stubs produced are ok

        args: pairs of (DumpFilename, DumpFilename), Runner
        """
        if runner.dryrun:
            return

        # check the output files to see if we like them;
        # if not, we will move the bad ones out of the way and
        # whine about them
        bad_dfnames = []
        output_dir = FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir)
        for temp_stub_dfname in output_dfnames:
            if os.path.exists(os.path.join(output_dir, temp_stub_dfname.filename)):
                # FIXME 2000 is simply wrong, we need to check by looking at the db. Ugh.
                bad = move_if_truncated(runner, temp_stub_dfname, emptycheck=2000, tmpdir=True)
                if bad:
                    bad_dfnames.append(temp_stub_dfname)
        if bad_dfnames:
            error_string = " ".join([bad_dfname.filename for bad_dfname in bad_dfnames])
            raise BackupError(
                "failed to write pagerange stub files (bad contents) " + error_string)

    def get_stub_gen_cmd_for_input(self, input_dfname, output_dfnames, runner):
        """
        for the given input dumpfile (stub), write the requested output file (stub)
        """
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" %
                              self.wiki.config.writeuptopageid)

        inputfile_path = runner.dump_dir.filename_public_path(input_dfname)

        output_dir = FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir)
        argstrings = []

        for output_dfname in output_dfnames:
            output_fname = output_dfname.filename
            # don't generate the file if we already have it (i.e. this is a retry)
            if not os.path.exists(os.path.join(output_dir, output_fname)):
                first_age_id = output_dfname.first_page_id
                if (output_dfname.last_page_id is not None and
                        output_dfname.last_page_id != "00000"):
                    last_page_id = str(int(output_dfname.last_page_id) + 1)
                else:
                    last_page_id = ""
                argstrings.append("{outfile}:{firstpage}:{lastpage}".format(
                    outfile=output_fname, firstpage=first_age_id, lastpage=last_page_id))

        # don't generate an output file if there are no filespecs
        if not argstrings:
            return None

        if input_dfname.file_ext == "gz":
            # command1 = "%s -dc %s" % (self.wiki.config.gzip, inputfile_path)
            command1 = [self.wiki.config.gzip, "-dc", inputfile_path]
        elif input_dfname.file_ext == '7z':
            # command1 = "%s e -si %s" % (self.wiki.config.sevenzip, inputfile_path)
            command1 = [self.wiki.config.sevenzip, "e", "-si", inputfile_path]
        elif input_dfname.file_ext == 'bz':
            # command1 = "%s -dc %s" % (self.wiki.config.bzip2, inputfile_path)
            command1 = [self.wiki.config.bzip2, "-dc", inputfile_path]
        else:
            raise BackupError("unknown stub file extension %s" % input_dfname.file_ext)

        command2 = [self.wiki.config.writeuptopageid, "--odir", output_dir,
                    "--fspecs", ";".join(argstrings)]
        pipeline = [command1]
        pipeline.append(command2)
        return pipeline

    def get_pagerange_stub_dfname(self, wanted, dump_dir):
        """
        return the dumpfilename for stub file that would have
        the page range in 'wanted'
        """
        stub_input_dfname = self.get_stub_dfname(wanted['partnum'], dump_dir)
        stub_output_dfname = DumpFilename(
            self.wiki, stub_input_dfname.date, stub_input_dfname.dumpname,
            stub_input_dfname.file_type,
            stub_input_dfname.file_ext,
            stub_input_dfname.partnum,
            DumpFilename.make_checkpoint_string(
                wanted['outfile'].first_page_id, wanted['outfile'].last_page_id), temp=False)
        return stub_output_dfname

    def has_no_pages(self, xmlfile, runner, tempdir=False):
        '''
        see if it has a page id in it or not. no? then return True
        '''
        if xmlfile.is_temp_file or tempdir:
            path = os.path.join(
                FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
                xmlfile.filename)
        else:
            path = runner.dump_dir.filename_public_path(xmlfile, self.wiki.date)
        dcontents = DumpContents(self.wiki, path, xmlfile, self.verbose)
        return bool(dcontents.find_first_page_id_in_file() == 0)

    def get_first_last_page_ids(self, xml_dfname, dump_dir, parts):
        """
        return the first and last page ids in a stub file based on
        looking at the content, can be slow because getting the last
        page id relies on decompression of the entire file
        """
        first_id = xml_dfname.first_page_id_int
        if not first_id:
            # get it from the file part and the config
            first_id = sum([int(parts[i]) for i in range(0, xml_dfname.partnum_int - 1)]) + 1

        last_id = xml_dfname.last_page_id_int
        if not last_id:
            if xml_dfname.partnum_int < len(parts):
                last_id = sum([int(parts[i]) for i in range(0, xml_dfname.partnum_int)])
            else:
                # last part. no way to compute a value from config, look at the file
                dcontents = DumpContents(
                    self.wiki,
                    dump_dir.filename_public_path(xml_dfname, xml_dfname.date),
                    xml_dfname, self.verbose)
                last_id = dcontents.find_last_page_id()
        return first_id, last_id
