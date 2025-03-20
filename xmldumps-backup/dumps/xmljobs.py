#!/usr/bin/python3
'''
All xml dump jobs except content dump jobs are defined here
'''

import os
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
from dumps.jobs import Dump, ProgressCallback
from dumps.outfilelister import OutputFileLister


def batcher(items, batchsize):
    '''
    given a list of items and a batchsize, return
    list of batches (lists) of these items, each
    batch with batchsize items and the last batch
    with however many items are left over
    '''
    if batchsize == 0:
        return [items]
    return (items[pos:pos + batchsize] for pos in range(0, len(items), batchsize))


class XmlStub(Dump):
    """Create lightweight skeleton dumps, minus bulk text.
    A second pass will import text from prior dumps or the database to make
    full files for the public."""

    @staticmethod
    def get_history_dump_name():
        return "stub-meta-history"

    @staticmethod
    def get_current_dump_name():
        return "stub-meta-current"

    @staticmethod
    def get_articles_dump_name():
        return "stub-articles"

    def __init__(self, name, desc, partnum_todo, jobsperbatch=None,
                 pages_per_part=None, checkpoints=False):
        self._partnum_todo = partnum_todo
        self.jobsperbatch = jobsperbatch
        self._pages_per_part = pages_per_part
        if self._pages_per_part:
            self._parts_enabled = True
            self.onlyparts = True
        if checkpoints:
            self._checkpoints_enabled = True
        Dump.__init__(self, name, desc)
        self.oflister = XmlStubFileLister(self.dumpname, self.file_type, self.file_ext,
                                          self.get_fileparts_list(), self.checkpoint_file,
                                          self._checkpoints_enabled, self.list_dumpnames)

    def check_truncation(self):
        return True

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def get_filetype(self):
        return "xml"

    def get_file_ext(self):
        return "gz"

    def get_dumpname(self):
        return 'stub'

    @staticmethod
    def list_dumpnames():
        dump_names = [XmlStub.get_history_dump_name(), XmlStub.get_current_dump_name(),
                      XmlStub.get_articles_dump_name()]
        return dump_names

    def build_command(self, runner, output_dfname, history_dfname, current_dfname):
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", self.get_command_abspath("xmlstubs.py"), "--config", config_file_arg,
                   "--wiki", runner.db_name]
        output_dir = self.get_output_dir(runner)
        if output_dfname is not None:
            command.extend(["--articles", DumpFilename.get_inprogress_name(
                os.path.join(output_dir, output_dfname.filename))])
        if history_dfname is not None:
            command.extend(["--history", DumpFilename.get_inprogress_name(
                os.path.join(output_dir, history_dfname.filename))])
        if current_dfname is not None:
            command.extend(["--current", DumpFilename.get_inprogress_name(
                os.path.join(output_dir, current_dfname.filename))])

        partnum = None
        if output_dfname is not None:
            partnum = output_dfname.partnum
        elif history_dfname is not None:
            partnum = history_dfname.partnum
        elif current_dfname is not None:
            partnum = current_dfname.partnum
        if partnum is not None:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([int(self._pages_per_part[i]) for i in range(0, int(partnum) - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if int(partnum) < len(self._pages_per_part):
                end = sum([int(self._pages_per_part[i]) for i in range(0, int(partnum))]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        self.cleanup_inprog_files(runner.dump_dir, runner)
        dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        # pick out the articles_dump files, setting up the stubs command for these
        # will cover all the other cases, as we generate all three stub file types
        # (article, meta-current, meta-history) at once
        dfnames = [dfname for dfname in dfnames if dfname.dumpname == self.get_articles_dump_name()]
        output_dir = self.get_output_dir(runner)
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(dfnames)
        for batch in batcher(dfnames, maxjobs):
            commands = []
            for output_dfname in batch:
                history_dfname = DumpFilename(
                    runner.wiki, output_dfname.date, self.get_history_dump_name(),
                    output_dfname.file_type, output_dfname.file_ext,
                    output_dfname.partnum, output_dfname.checkpoint,
                    output_dfname.temp)
                current_dfname = DumpFilename(
                    runner.wiki, output_dfname.date, self.get_current_dump_name(),
                    output_dfname.file_type, output_dfname.file_ext,
                    output_dfname.partnum, output_dfname.checkpoint,
                    output_dfname.temp)
                if os.path.exists(os.path.join(output_dir, history_dfname.filename)):
                    history_dfname = None
                if os.path.exists(os.path.join(output_dir, current_dfname.filename)):
                    current_dfname = None
                if os.path.exists(os.path.join(output_dir, output_dfname.filename)):
                    output_dfname = None
                if history_dfname is None and current_dfname is None and output_dfname is None:
                    # these files in the batch are done, don't rerun it
                    continue
                # at least one file in the batch needs to be rerun, do so
                command_series = self.build_command(runner, output_dfname,
                                                    history_dfname, current_dfname)
                self.setup_command_info(runner, command_series,
                                        [output_dfname, current_dfname, history_dfname])
                commands.append(command_series)

            if not commands:
                continue

            prog = ProgressCallback()
            error, _broken = runner.run_command(
                commands, callback_stderr=prog.progress_callback,
                callback_stderr_arg=runner,
                callback_on_completion=self.command_completion_callback)
            if error:
                raise BackupError("error producing stub files")
        return True


class XmlStubFileLister(OutputFileLister):
    """
    special output file listing methods for stubs dumps

    because stubs with metadata for current main namespace content, all
    current content, and all historical content are produced in one step,
    each of these files has a different base name (dump name) and must
    be accounted for
    """

    def list_outfiles_to_publish(self, args):
        """
        expects: args.dump_dir
        returns: list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        return super().list_outfiles_to_publish(args)

    def list_outfiles_for_build_command(self, args):
        """
        expects: args.dump_dir
        returns: list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        return super().list_outfiles_for_build_command(args)

    def list_inprog_files_for_cleanup(self, args):
        """
        expects: args.dump_dir
        returns: list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        return super().list_inprog_files_for_cleanup(args)

    def list_outfiles_for_cleanup(self, args):
        """
        expects: args.dump_dir
        returns: list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        return super().list_outfiles_for_cleanup(args)

    def list_outfiles_for_input(self, args):
        """
        expects: args.dump_dir
        returns: list of DumpFilename
        """
        if args.dump_names is None:
            args = args._replace(dump_names=self.list_dumpnames())
        return super().list_outfiles_for_input(args)

    def list_truncated_empty_outfiles(self, args):
        """
        expects: args.dump_dir, optional args.dump_names
        returns: list of DumpFilename
        """
        if args.dump_names is None:
            args = args._replace(dump_names=self.list_dumpnames())
        return super().list_truncated_empty_outfiles(args)

    def list_truncated_empty_outfiles_for_input(self, args):
        """
        expects: args.dump_dir, optional args.dump_names
        returns: list of DumpFilename
        """
        if args.dump_names is None:
            args = args._replace(dump_names=self.list_dumpnames())
        return super().list_truncated_empty_outfiles_for_input(args)


class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, partnum_todo, jobsperbatch=None, pages_per_part=None):
        self._partnum_todo = partnum_todo
        self.jobsperbatch = jobsperbatch
        self._pages_per_part = pages_per_part
        if self._pages_per_part:
            self._parts_enabled = True
            self.onlyparts = True
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

    def build_command(self, runner, output_dfname):
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        logging_path = runner.dump_dir.filename_public_path(output_dfname)

        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", self.get_command_abspath("xmllogs.py"), "--config",
                   config_file_arg, "--wiki", runner.db_name,
                   "--outfile", DumpFilename.get_inprogress_name(logging_path)]

        if output_dfname.partnum:
            # set up start end end pageids for this piece
            # note there is no item id 0 I guess. so we start with 1
            start = sum([int(self._pages_per_part[i])
                         for i in range(0, output_dfname.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last log item id,
            # whatever that is.
            command.append(startopt)
            if output_dfname.partnum_int < len(self._pages_per_part):
                end = sum([int(self._pages_per_part[i])
                           for i in range(0, output_dfname.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_inprog_files(runner.dump_dir, runner)
        dfnames = self.oflister.list_outfiles_for_build_command(
            self.oflister.makeargs(runner.dump_dir))
        output_dir = self.get_output_dir(runner)
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(dfnames)
        error = None
        for batch in batcher(dfnames, maxjobs):
            commands = []
            for output_dfname in batch:
                if os.path.exists(os.path.join(output_dir, output_dfname.filename)):
                    # this file in the batch was done already, don't redo it
                    continue
                command_series = self.build_command(runner, output_dfname)
                self.setup_command_info(runner, command_series, [output_dfname])
                commands.append(command_series)

            if not commands:
                continue

            prog = ProgressCallback()
            error, _broken = runner.run_command(
                commands, callback_stderr=prog.progress_callback,
                callback_stderr_arg=runner,
                callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error dumping log files")
        return True
