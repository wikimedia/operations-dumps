'''
All xml dump jobs except content dump jobs are defined here
'''

from os.path import exists

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
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir, dump_names=None):
        """
        returns: list of DumpFilenames
        """
        if dump_names is None:
            dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return dfnames

    def build_command(self, runner, outf):
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        articles_filepath = runner.dump_dir.filename_public_path(outf)
        history_filepath = runner.dump_dir.filename_public_path(DumpFilename(
            runner.wiki, outf.date, self.history_dump_name, outf.file_type,
            outf.file_ext, outf.partnum, outf.checkpoint, outf.temp))
        current_filepath = runner.dump_dir.filename_public_path(DumpFilename(
            runner.wiki, outf.date, self.current_dump_name, outf.file_type,
            outf.file_ext, outf.partnum, outf.checkpoint, outf.temp))
#        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        command = ["/usr/bin/python", "xmlstubs.py", "--config", runner.wiki.config.files[0],
                   "--wiki", runner.db_name, "--articles", articles_filepath,
                   "--history", history_filepath, "--current", current_filepath]

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
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        # pick out the articles_dump files, setting up the stubs command for these
        # will cover all the other cases, as we generate all three stub file types
        # (article, meta-current, meta-history) at once
        dfnames = [dfname for dfname in dfnames if dfname.dumpname == self.articles_dump_name]
        if self.jobsperbatch is not None:
            maxjobs = self.jobsperbatch
        else:
            maxjobs = len(dfnames)
        for batch in batcher(dfnames, maxjobs):
            commands = []
            for dfname in batch:
                series = self.build_command(runner, dfname)
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
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("logging table job wants to produce more than one output file")
        output_dfname = dfnames[0]
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)
#        script_command = MultiVersion.mw_script_as_array(runner.wiki.config, "dumpBackup.php")

        logging_path = runner.dump_dir.filename_public_path(output_dfname)

        command = ["/usr/bin/python", "xmllogs.py", "--config",
                   runner.wiki.config.files[0], "--wiki", runner.db_name,
                   "--outfile", logging_path]

        pipeline = [command]
        series = [pipeline]
        error = runner.run_command([series], callback_stderr=self.progress_callback,
                                   callback_stderr_arg=runner)
        if error:
            raise BackupError("error dumping log files")


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

    def build_command(self, runner, novariant_dfname):
        """
        args:
            Runner, DumpFilename for output without any language variant
        """
        command = ["/usr/bin/python", "xmlabstracts.py", "--config",
                   runner.wiki.config.files[0], "--wiki", self.db_name]

        output_paths = []
        variants = []
        for variant in self._variants():
            # if variants is the empty string, then we will wind up with
            # one output file using the dumpname base only
            # otherwise we will wind up with one per variant, with filename
            # containing that variant string
            variant_option = self._variant_option(variant)
            dumpname = self.dumpname_from_variant(variant)
            dfname = DumpFilename(runner.wiki, novariant_dfname.date, dumpname,
                                  novariant_dfname.file_type,
                                  novariant_dfname.file_ext,
                                  novariant_dfname.partnum,
                                  novariant_dfname.checkpoint)
            output_paths.append(runner.dump_dir.filename_public_path(dfname))
            variants.append(variant_option)

            command.extend(["--outfiles=%s" % ",".join(output_paths),
                            "--variants=%s" % ",".join(variants)])

        if novariant_dfname.partnum:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            start = sum([self._parts[i] for i in range(0, novariant_dfname.partnum_int - 1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last file part, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if novariant_dfname.partnum_int < len(self._parts):
                end = sum([self._parts[i] for i in range(0, novariant_dfname.partnum_int)]) + 1
                endopt = "--end=%s" % end
                command.append(endopt)
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        # choose the empty variant to pass to buildcommand, it will fill in the rest if needed
        output_dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        dumpname0 = self.list_dumpnames()[0]
        for dfname in output_dfnames:
            if dfname.dumpname == dumpname0:
                series = self.build_command(runner, dfname)
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
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_publish(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_to_check_for_truncation(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_to_check_for_truncation(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_build_command(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_build_command(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_cleanup(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_cleanup(self, dump_dir, dump_names))
        return dfnames

    def list_outfiles_for_input(self, dump_dir):
        """
        returns: list of DumpFilenames
        """
        dump_names = self.list_dumpnames()
        dfnames = []
        dfnames.extend(Dump.list_outfiles_for_input(self, dump_dir, dump_names))
        return dfnames
