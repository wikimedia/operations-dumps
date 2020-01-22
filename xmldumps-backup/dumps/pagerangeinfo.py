#!/usr/bin/python3
"""
classes and methods for writing out file
with information about the pageranges chosen
for dumping page content for a specific job
(either articles, meta-current or meta-history)
in json format, for reuse when restarting failed
jobs, this saves recomputation of these numbers
and means temp stub files can be re-used too
"""


import os
import sys
import traceback
from dumps.specialfilesregistry import SpecialFileWriter


class PageRangeInfo(SpecialFileWriter):
    """
    write page range info for page content jobs
    to a single file, or update portions
    of an existing file
    """

    NAME = "pagerangeinfo"
    FILENAME = "pagerangeinfo"

    # might add more someday, but not today
    known_formats = ["json"]

    def __init__(self, wiki, enabled, fileformat="json", error_callback=None, verbose=False):
        super().__init__(wiki, fileformat="json", error_callback=None, verbose=False)
        self._enabled = enabled

    def write_pagerange_info(self, pagerange_info):
        """
        expecting a dict of page ranges per jobname, write the appropriately
        serialized output to the pagerangeinfo file
        """
        try:
            self.write_contents(pagerange_info)
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't update pagerangeinfo file. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)

    def get_pagerange_info(self, wiki):
        """
        read and return the contents of the json pagerange file
        for the wiki, a dict of page ranges for each jobname
        """
        if self.fileformat != "json":
            # fixme really something else should happen here
            return {}
        try:
            if os.path.exists(self.filepath):
                contents = SpecialFileWriter.load_json_file(wiki, self.fileformat, PageRangeInfo)
            else:
                contents = None
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't read pagerangeinfo file. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)
            contents = None

        if contents:
            # convert the lists to tuples, thanks JSON
            to_return = {}
            for jobname in contents:
                if contents[jobname] is not None:
                    to_return[jobname] = [(entry[0], entry[1], entry[2])
                                          for entry in contents[jobname]]
                else:
                    to_return[jobname] = None
        else:
            to_return = None

        return to_return

    def update_pagerangeinfo(self, wiki, jobname, pagerangeinfo):
        """
        given pagerange info about some page content job,
        add the info to the pagerangeinfo file if the job had no info,
        or update the info if the job had some info already
        """
        if pagerangeinfo is None:
            pagerangeinfo = []
        current_info = self.get_pagerange_info(wiki)
        if current_info is None:
            current_info = {}

        if jobname in current_info:
            current_info[jobname].extend(pagerangeinfo)
        else:
            current_info[jobname] = pagerangeinfo
        current_info[jobname] = list(set(current_info[jobname]))
        # note that we DO NOT check for overlapping page ranges, that's on the caller
        current_info[jobname] = sorted(current_info[jobname], key=lambda x: int(x[0]))
        self.write_pagerange_info(current_info)
