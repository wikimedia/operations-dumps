'''
Jobs that dump sql tables are defined here
'''

import time

import os.path
from os.path import exists

from dumps.exceptions import BackupError
from dumps.jobs import Dump
from dumps.fileutils import DumpFilename


class PublicTable(Dump):
    """Dump of a table using MySQL's mysqldump utility."""

    def __init__(self, table, name, desc):
        self._table = table
        self._parts_enabled = False
        self.private = False
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return self._table

    def get_filetype(self):
        return "sql"

    def get_file_ext(self):
        return "gz"

    def build_command(self, runner, output_dfname):
        commands = runner.db_server_info.build_sqldump_command(self._table, runner.wiki.config.gzip)
        if self.private or runner.wiki.is_private():
            command_series = runner.get_save_command_series(
                commands, DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_private_path(output_dfname)))
        else:
            command_series = runner.get_save_command_series(
                commands, DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_public_path(output_dfname)))
        return command_series

    def run(self, runner):
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpname)
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        output_dfname = dfnames[0]
        if self.private:
            output_dir = runner.wiki.private_dir()
        else:
            output_dir = runner.wiki.public_dir()
        command_series = self.build_command(runner, output_dfname)
        self.setup_command_info(runner, command_series, [output_dfname],
                                os.path.join(output_dir, runner.wiki.date))

        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        error, broken = self.save_table(runner, command_series)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error, broken = self.save_table(runner, command_series)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    # returns 0 on success, 1 on error
    def save_table(self, runner, command_series):
        """
        Dump a table from the current DB with mysqldump, save to a gzipped sql file.
        args:
            table name (e.g. "site_stats"), path to output file, Runner
        """
        return runner.save_command(command_series, self.command_completion_callback)


class PrivateTable(PublicTable):
    """Hidden table dumps for private data."""

    def __init__(self, table, name, desc):
        PublicTable.__init__(self, table, name, desc)
        self.private = True

    def check_truncation(self):
        # Truncation checks require output to public dir, hence we
        # cannot use them. The default would be 'False' anyways, but
        # if that default changes, we still cannot use automatic
        # truncation checks.
        return False

    def description(self):
        return self._desc + " (private)"

    def list_outfiles_to_publish(self, dump_dir):
        """Private table won't have public files to list."""
        return []


class TitleDump(Dump):
    """This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""

    def get_dumpname(self):
        return "all-titles-in-ns0"

    def get_filetype(self):
        return ""

    def get_file_ext(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        query = "select page_title from page where page_namespace=0;"
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("page title dump trying to produce more than one output file")
        dfname = dfnames[0]
        command_series = self.build_command(runner, query, dfname)
        self.setup_command_info(runner, command_series, [dfname])
        error, broken = self.save_sql(runner, command_series)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error, broken = self.save_sql(runner, command_series)
        if error:
            raise BackupError("error dumping titles list")

    def build_command(self, runner, query, out_dfname):
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        series = runner.db_server_info.build_sql_command(query, runner.wiki.config.gzip)
        if runner.wiki.is_private():
            return runner.get_save_command_series(
                series, DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_private_path(out_dfname)))
        else:
            return runner.get_save_command_series(
                series, DumpFilename.get_inprogress_name(
                    runner.dump_dir.filename_public_path(out_dfname)))

    def save_sql(self, runner, command_series):
        """Pass some SQL commands to the server for this DB and save output to a gzipped file."""
        return runner.save_command(command_series, self.command_completion_callback)


class AllTitleDump(TitleDump):

    def get_dumpname(self):
        return "all-titles"

    def run(self, runner):
        retries = 0
        maxretries = 3
        query = "select page_namespace, page_title from page;"
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("all titles dump trying to produce more than one output file")
        dfname = dfnames[0]
        command_series = self.build_command(runner, query, dfname)
        self.setup_command_info(runner, command_series, [dfname])

        error, broken = self.save_sql(runner, command_series)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_sql(runner, command_series)
        if error:
            raise BackupError("error dumping all titles list")
