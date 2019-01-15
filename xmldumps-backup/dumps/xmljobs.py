#!/usr/bin/python3
'''
All xml dump jobs except content dump jobs are defined here
'''

import os
from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename
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
    return (items[pos:pos + batchsize] for pos in range(0, len(items), batchsize))


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
        Dump.__init__(self, name, desc)

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

    def list_dumpnames(self):
        dump_names = [self.history_dump_name, self.current_dump_name, self.articles_dump_name]
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return dfnames

    def list_inprog_files_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_inprog_files_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        """
        returns: list of DumpFilename
        """
        if dump_names is None:
            dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, dump_dir, dump_names=None):
        """
        returns: list of DumpFilename
        """
        if dump_names is None:
            dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_truncated_empty_outfiles_for_input(
            self, dump_dir, dump_names))
        return dfnames

    def build_command(self, runner, output_dfname, history_dfname, current_dfname):
        if not os.path.exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", "xmlstubs.py", "--config", config_file_arg,
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
            start = sum([int(self._parts[i]) for i in range(0, int(partnum) - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if int(partnum) < len(self._parts):
                end = sum([int(self._parts[i]) for i in range(0, int(partnum))]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        self.cleanup_inprog_files(runner.dump_dir, runner)
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        # pick out the articles_dump files, setting up the stubs command for these
        # will cover all the other cases, as we generate all three stub file types
        # (article, meta-current, meta-history) at once
        dfnames = [dfname for dfname in dfnames if dfname.dumpname == self.articles_dump_name]
        output_dir = self.get_output_dir(runner)
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(dfnames)
        for batch in batcher(dfnames, maxjobs):
            commands = []
            for output_dfname in batch:
                history_dfname = DumpFilename(
                    runner.wiki, output_dfname.date, self.history_dump_name,
                    output_dfname.file_type, output_dfname.file_ext,
                    output_dfname.partnum, output_dfname.checkpoint,
                    output_dfname.temp)
                current_dfname = DumpFilename(
                    runner.wiki, output_dfname.date, self.current_dump_name,
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

            error, _broken = runner.run_command(
                commands, callback_stderr=self.progress_callback,
                callback_stderr_arg=runner,
                callback_on_completion=self.command_completion_callback)
            if error:
                raise BackupError("error producing stub files")


class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, partnum_todo, jobsperbatch=None, parts=False):
        self._partnum_todo = partnum_todo
        self.jobsperbatch = jobsperbatch
        self._parts = parts
        if self._parts:
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

        if runner.wiki.is_private():
            logging_path = runner.dump_dir.filename_private_path(output_dfname)
        else:
            logging_path = runner.dump_dir.filename_public_path(output_dfname)

        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", "xmllogs.py", "--config",
                   config_file_arg, "--wiki", runner.db_name,
                   "--outfile", DumpFilename.get_inprogress_name(logging_path)]

        if output_dfname.partnum:
            # set up start end end pageids for this piece
            # note there is no item id 0 I guess. so we start with 1
            start = sum([int(self._parts[i]) for i in range(0, output_dfname.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last log item id,
            # whatever that is.
            command.append(startopt)
            if output_dfname.partnum_int < len(self._parts):
                end = sum([int(self._parts[i]) for i in range(0, output_dfname.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_inprog_files(runner.dump_dir, runner)
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
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

            error, _broken = runner.run_command(
                commands, callback_stderr=self.progress_callback,
                callback_stderr_arg=runner,
                callback_on_completion=self.command_completion_callback)
        if error:
            raise BackupError("error dumping log files")


class AbstractDump(Dump):
    """XML dump for Yahoo!'s Active Abstracts thingy"""

    def __init__(self, name, desc, partnum_todo, db_name, jobsperbatch=None, parts=False):
        self._partnum_todo = partnum_todo
        self.jobsperbatch = jobsperbatch
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
        return "gz"

    def get_variant_from_dumpname(self, dumpname):
        fields = dumpname.split("-")
        if fields[0] != self.get_dumpname() or len(fields) > 3:
            # got garbage.
            return None
        if len(fields) == 1:
            return ""
        return "-".join(fields[1:])

    def build_command(self, runner, novariant_dfname, output_dfnames):
        """
        args:
            Runner, DumpFilename for output without any language variant
        """
        config_file_arg = runner.wiki.config.files[0]
        if runner.wiki.config.override_section:
            config_file_arg = config_file_arg + ":" + runner.wiki.config.override_section
        command = ["/usr/bin/python3", "xmlabstracts.py", "--config",
                   config_file_arg, "--wiki", self.db_name]

        output_paths = []
        variants = []
        for dfname in output_dfnames:
            variant = self.get_variant_from_dumpname(dfname.dumpname)
            variant_option = self._variant_option(variant)
            if runner.wiki.is_private():
                output_paths.append(DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_private_path(dfname)))
            else:
                output_paths.append(DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_public_path(dfname)))
            variants.append(variant_option)

        command.extend(["--outfiles=%s" % ",".join(output_paths),
                        "--variants=%s" % ",".join(variants)])

        if novariant_dfname.partnum:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([int(self._parts[i])
                         for i in range(0, novariant_dfname.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if novariant_dfname.partnum_int < len(self._parts):
                end = sum([int(self._parts[i]) for i in range(0, novariant_dfname.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        self.cleanup_old_files(runner.dump_dir, runner)
        self.cleanup_inprog_files(runner.dump_dir, runner)
        commands = []
        # choose the empty variant to pass to buildcommand, it will fill in the rest if needed
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        dumpname0 = self.list_dumpnames()[0]
        wanted_dfnames = [dfname for dfname in output_dfnames if dfname.dumpname == dumpname0]
        output_dir = self.get_output_dir(runner)
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(wanted_dfnames)
        for batch in batcher(wanted_dfnames, maxjobs):
            commands = []
            for dfname in batch:
                produced_dfnames = []
                for variant in self._variants():
                    dumpname = self.dumpname_from_variant(variant)
                    produced_dfnames.append(
                        DumpFilename(runner.wiki, dfname.date, dumpname,
                                     dfname.file_type, dfname.file_ext,
                                     dfname.partnum, dfname.checkpoint))

                fullpaths = [os.path.join(output_dir, produced_dfname.filename)
                             for produced_dfname in produced_dfnames]
                do_not_exist = [fullpath for fullpath in fullpaths if not os.path.exists(fullpath)]
                if not do_not_exist:
                    # all output files are already there for this command in the batch,
                    # move on
                    continue

                command_series = self.build_command(runner, dfname, produced_dfnames)
                self.setup_command_info(runner, command_series, produced_dfnames)
                commands.append(command_series)

            if not commands:
                continue

            error, _broken = runner.run_command(
                commands, callback_stderr=self.progress_callback,
                callback_stderr_arg=runner,
                callback_on_completion=self.command_completion_callback)
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
        return ":variant=%s" % variant

    def dumpname_from_variant(self, variant):
        dumpname_base = 'abstract'
        if variant == "":
            return dumpname_base
        return dumpname_base + "-" + variant

    def list_dumpnames(self):
        # need this first for build_command and other such
        dump_names = []
        variants = self._variants()
        for variant in variants:
            dump_names.append(self.dumpname_from_variant(variant))
        return dump_names

    def list_outfiles_to_publish(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return dfnames

    def list_inprog_files_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_inprog_files_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, dump_dir):
        """
        returns: list of DumpFilename
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_truncated_empty_outfiles_for_input(self, dump_dir, dump_names))
        return dfnames
