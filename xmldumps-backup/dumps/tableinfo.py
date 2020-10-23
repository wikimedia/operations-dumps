#!/usr/bin/python3
# Read/write list of known tables for a wiki
import os
import sys
import traceback
from dumps.specialfilesregistry import SpecialFileWriter


class TableInfo(SpecialFileWriter):
    """
    management of the retrieval and reading of a the list of known tables
    for a wiki
    """
    NAME = "tableinfo"
    FILENAME = "tableinfo"

    # might add more someday, but not today
    known_formats = ["json"]

    def write_tableinfo(self, tableinfo):
        """
        given text that should be a list of tables one per line,
        write it into the tableinfo file
        """
        if not self.wiki.date:
            # no date no directory, no file.
            return
        try:
            self.write_contents(tableinfo)
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't update tableinfo file. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)

    def get_tableinfo(self):
        """
        read and return the contents of the json pagerange file
        for the wiki, a dict of page ranges for each jobname
        """
        if self.fileformat != "json":
            # fixme really something else should happen here
            return {}
        try:
            if os.path.exists(self.filepath):
                contents = SpecialFileWriter.load_json_file(self.wiki, self.fileformat, TableInfo)
            else:
                contents = None
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't read tableinfo file. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)
            contents = None

        return contents
