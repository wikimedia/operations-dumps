"""
classes and methods for writing out file
with information about the current dump run in
json format, for downloaders' use
"""


import os
import sys
import traceback
import json
from dumps.runnerutils import Checksummer
from dumps.runnerutils import RunInfo
from dumps.runnerutils import Report
from dumps.fileutils import DumpFilename
from dumps.fileutils import DumpDir


class StatusAPI(object):
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
        if fmt not in StatusAPI.known_formats:
            return {}
        date = wiki.latest_dump()
        if date:
            fname = os.path.join(wiki.public_dir(),
                                 date, StatusAPI.FILENAME + "." + fmt)
            with open(fname, "r") as status_file:
                contents = status_file.read()
                status_file.close()
            return json.loads(contents)
        else:
            return {}

    def __init__(self, wiki, enabled, fileformat="json", error_callback=None, verbose=False):
        self.wiki = wiki
        self._enabled = enabled
        self.fileformat = fileformat
        self.filepath = self.get_output_filepath()
        if not self.filepath:
            # here we should log something. FIXME
            pass
        self.error_callback = error_callback
        self.verbose = verbose

    def get_output_filepath(self):
        if not self.check_format():
            return None
        return os.path.join(self.wiki.public_dir(),
                            self.wiki.date, StatusAPI.FILENAME + "." + self.fileformat)

    def check_format(self):
        return bool(self.fileformat in StatusAPI.known_formats)

    def get_dumprun_info(self):
        dumpruninfo_path = os.path.join(self.wiki.public_dir(),
                                        self.wiki.date, "dumpruninfo.json")
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
            dump_dir = DumpDir(self.wiki, self.wiki.db_name)

            basefilename = dump_dir.filename_public_path(dfname)
            path = os.path.join(self.wiki.public_dir(), self.wiki.date, basefilename)

            contents.append(self.get_json_file_contents(path))
        return [item for item in contents if item is not None]

    def get_report_info(self):
        reportinfo_path = os.path.join(self.wiki.public_dir(),
                                       self.wiki.date, "report.json")
        return self.get_json_file_contents(reportinfo_path)

    def get_json_file_contents(self, path):
        try:
            fullpath = os.path.join(self.wiki.public_dir(),
                                    self.wiki.date, path)
            with open(fullpath, "r") as settings_fd:
                contents = settings_fd.read()
                if not contents:
                    return None
                return json.loads(contents)
        except IOError:
            # may not exist, we don't care
            return None

    def complete_fileinfo(self, jobname, reportinfo, hashinfo_list):
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

    def combine_status_sources(self, dumpruninfo, filehashinfo, reportinfo):
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
                    self.complete_fileinfo(jobname, reportinfo, filehashinfo)
                    # fold the report info (file info for all job-related files)
                    # into the dumpruninfo
                    fileinfo = Report.get_fileinfo_for_job(jobname, reportinfo)
                    RunInfo.add_job_property(jobname, "files", fileinfo, dumpruninfo)
        return dumpruninfo

    def write_contents_json(self, contents):
        if not self.filepath:
            return
        with open(self.filepath, "w+") as fdesc:
            fdesc.write(json.dumps(contents) + "\n")

    def write_contents(self, contents):
        if not self.check_format():
            return None
        if self.fileformat == "json":
            return self.write_contents_json(contents)

    def write_statusapi_file(self):
        if StatusAPI.NAME not in self._enabled:
            return

        try:
            # we have three sources of information that need to
            # be read and combined into the json file usable by
            # dump status requesters
            dumprun_info = self.get_dumprun_info()
            filehash_info = self.get_filehash_info()
            report_info = self.get_report_info()

            contents = self.combine_status_sources(dumprun_info, filehash_info, report_info)
            contents['version'] = StatusAPI.VERSION
            self.write_contents(contents)
        except Exception as ex:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't update status files. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)
