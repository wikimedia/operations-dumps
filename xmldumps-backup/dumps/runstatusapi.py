#!/usr/bin/python3
"""
classes and methods for writing out file
with information about the current dump run in
json format, for downloaders' use
"""


import os
import sys
import traceback
from dumps.checksummers import Checksummer
from dumps.runnerutils import RunInfo
from dumps.report import Report
from dumps.specialfilesregistry import SpecialFileWriter
from dumps.fileutils import DumpFilename


class StatusAPI(SpecialFileWriter):
    """
    write specific run status information
    to a single file, or update portions
    of an existing file
    """

    NAME = "statusapi"
    FILENAME = "dumpstatus"
    VERSION = "0.8"

    # might add more someday, but not today
    known_formats = ["json"]

    @staticmethod
    def get_wiki_info(wiki, fmt="json"):
        """
        read and return the contents of the json status file
        for the wiki
        """
        return SpecialFileWriter.load_json_file(wiki, fmt, StatusAPI)

    @staticmethod
    def complete_fileinfo(jobname, reportinfo, hashinfo_list):
        """
        given a list of dicts with info about each file,
        grab the related hash info out of the hashinfo dicts for
        each file and add it to that file's dict in the list
        then return the modified dict
        """
        filenames = Report.get_filenames_for_job(jobname, reportinfo)
        for filename in filenames:
            for hashinfo in hashinfo_list:
                hashtypes_sums = Checksummer.get_hashinfo(filename, hashinfo)
                for (hashtype, checksum) in hashtypes_sums:
                    Report.add_file_property(jobname, filename, hashtype, checksum, reportinfo)

    @staticmethod
    def combine_status_sources(dumpruninfo, filehashinfo, reportinfo):
        """
        given the json dumpruninfo, the json report info, and the json file hash infos,
        convert them all into one structure for writing later as json to a
        status output file
        """
        # list of filecontents passed in for filehashinfo
        # because there is more than one hash type
        if filehashinfo is None:
            filehashinfo = [Checksummer.get_empty_json()]
        if dumpruninfo is None:
            dumpruninfo = RunInfo.get_empty_json()
        if reportinfo is not None:
            dumpruninfo_jobs = RunInfo.get_jobs(dumpruninfo)
            for jobname in Report.get_jobs(reportinfo):
                if jobname in dumpruninfo_jobs:
                    # fold hash info into the report
                    StatusAPI.complete_fileinfo(jobname, reportinfo, filehashinfo)
                    # fold the report info (file info for all job-related files)
                    # into the dumpruninfo
                    fileinfo = Report.get_fileinfo_for_job(jobname, reportinfo)
                    RunInfo.add_job_property(jobname, "files", fileinfo, dumpruninfo)
        return dumpruninfo

    def __init__(self, wiki, enabled, fileformat="json", error_callback=None, verbose=False):
        super().__init__(wiki, fileformat="json", error_callback=None, verbose=False)
        self._enabled = enabled

    def get_dumprun_info(self):
        """
        retrieve dump run info from dump run info file
        """
        runinfo = RunInfo(self.wiki, True)
        dumpruninfo_path = os.path.join(
            self.wiki.public_dir(), self.wiki.date, runinfo.get_runinfo_basename() + ".json")
        return self.get_json_file_contents(dumpruninfo_path)

    def get_filehash_info(self):
        """
        return a list of jsonified contents of hash files, one entry per hash type,
        each entry containing the hashes and names of all files produced
        """
        contents = []
        for hashtype in Checksummer.HASHTYPES:
            dfname = DumpFilename(
                self.wiki, None, Checksummer.get_checksum_filename_basename(hashtype, "json"))
            path = os.path.join(self.wiki.public_dir(), self.wiki.date, dfname.filename)

            contents.append(self.get_json_file_contents(path))
        return [item for item in contents if item is not None]

    def get_report_info(self):
        """
        retrieve dump report info from dump report info file
        """
        reportinfo_path = os.path.join(self.wiki.public_dir(),
                                       self.wiki.date, Report.JSONFILE)
        return self.get_json_file_contents(reportinfo_path)

    def write_statusapi_file(self):
        """
        gather status info and write it in json format to output file
        """
        if StatusAPI.NAME not in self._enabled:
            return

        try:
            # we have three sources of information that need to
            # be read and combined into the json file usable by
            # dump status requesters
            dumprun_info = self.get_dumprun_info()
            filehash_info = self.get_filehash_info()
            report_info = self.get_report_info()

            contents = StatusAPI.combine_status_sources(dumprun_info, filehash_info, report_info)
            contents['version'] = StatusAPI.VERSION
            self.write_contents(contents)
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't update status files. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)
