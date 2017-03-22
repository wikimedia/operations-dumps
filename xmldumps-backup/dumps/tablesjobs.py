'''
Jobs that dump sql tables are defined here
'''

import time

from os.path import exists

from dumps.exceptions import BackupError
from dumps.jobs import Dump


class PublicTable(Dump):
    """Dump of a table using MySQL's mysqldump utility."""

    def __init__(self, table, name, desc):
        self._table = table
        self._parts_enabled = False
        Dump.__init__(self, name, desc)

    def get_dumpname(self):
        return self._table

    def get_filetype(self):
        return "sql"

    def get_file_ext(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpname)
        output_dfname = dfnames[0]
        error = self.save_table(
            self._table, runner.dump_dir.filename_public_path(output_dfname), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_table(
                self._table, runner.dump_dir.filename_public_path(output_dfname), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    # returns 0 on success, 1 on error
    def save_table(self, table, outfilepath, runner):
        """
        Dump a table from the current DB with mysqldump, save to a gzipped sql file.
        args:
            table name (e.g. "site_stats"), path to output file, Runner
        """
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        commands = runner.db_server_info.build_sqldump_command(table, runner.wiki.config.gzip)
        return runner.save_command(commands, outfilepath)


class PrivateTable(PublicTable):
    """Hidden table dumps for private data."""

    def __init__(self, table, name, desc):
        # Truncation checks require output to public dir, hence we
        # cannot use them. The default would be 'False' anyways, but
        # if that default changes, we still cannot use automatic
        # truncation checks.
        self._check_truncation = False
        PublicTable.__init__(self, table, name, desc)

    def description(self):
        return self._desc + " (private)"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        dfnames = self.list_outfiles_for_build_command(runner.dump_dir)
        if len(dfnames) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpname)
        output_dfname = dfnames[0]
        error = self.save_table(
            self._table, runner.dump_dir.filename_private_path(output_dfname), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_table(
                self._table, runner.dump_dir.filename_private_path(output_dfname), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

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
        outpath = runner.dump_dir.filename_public_path(dfname)
        error = self.save_sql(query, outpath, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_sql(query, outpath, runner)
        if error:
            raise BackupError("error dumping titles list")

    def save_sql(self, query, outfile, runner):
        """Pass some SQL commands to the server for this DB and save output to a gzipped file."""
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        command = runner.db_server_info.build_sql_command(query, runner.wiki.config.gzip)
        return runner.save_command(command, outfile)


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
        outpath = runner.dump_dir.filename_public_path(dfname)
        error = self.save_sql(query, outpath, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.save_sql(query, outpath, runner)
        if error:
            raise BackupError("error dumping all titles list")
