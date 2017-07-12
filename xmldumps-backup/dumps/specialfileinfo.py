"""
classes and methods for writing out file
with information about the status, hash, html
and other files produced during a dump run,
in json format, for downloaders' use
"""


import os
import sys
import traceback
from dumps.specialfilesregistry import SpecialFileWriter
from dumps.specialfilesregistry import SpecialFilesRegistry
from dumps.specialfilesregistry import Registered
from dumps.fileutils import DumpFilename
from dumps.fileutils import DumpDir


class SpecialFileInfo(SpecialFileWriter):
    """
    write information about special files
    (those that do not contain dump content)
    to a single file, or update portions
    of an existing file
    """

    NAME = "specialfileinfo"
    FILENAME = "dumpspecialfiles"
    VERSION = "0.8"

    # might add more someday, but not today
    known_formats = ["json"]

    @staticmethod
    def get_wiki_specialfile_info(wiki, fmt="json"):
        """
        read and return the contents of the json special files list
        for the wiki
        """
        return SpecialFileWriter.load_json_file(wiki, fmt, SpecialFileInfo)

    @staticmethod
    def get_all_output_files():
        """
        return list of filenames for writing special file lists,
        in all known formats
        """
        files = []
        for fmt in SpecialFileInfo.known_formats:
            files.append(SpecialFileInfo.FILENAME + "." + fmt)
        return files

    def __init__(self, wiki, enabled, fileformat="json", error_callback=None, verbose=False):
        super(SpecialFileInfo, self).__init__(wiki, fileformat="json",
                                              error_callback=None, verbose=False)
        self._enabled = enabled

    def get_special_filenames(self):
        """
        get and return a list of all files in the dump directory for the current
        wiki and date that are not dump job output files and that would be of
        interest to dump consumers or downloaders
        """
        files = []
        for classname in SpecialFilesRegistry.SPECIALFILES_REGISTRY:
            try:
                files.extend(Registered.list_special_files(
                    SpecialFilesRegistry.SPECIALFILES_REGISTRY[classname], self.wiki))
            except Exception:
                pass
        return files

    def write_specialfilesinfo_file(self):
        """
        get info about all files for the most current dump of a given
        wiki, possibly in progress, that don't contain dump job
        output; write this info to an output file
        """
        if SpecialFileInfo.NAME not in self._enabled:
            return

        dump_dir = DumpDir(self.wiki, self.wiki.db_name)
        files = self.get_special_filenames()
        fileinfo = {}
        for filename in files:
            fileinfo[filename] = {}
            path = os.path.join(self.wiki.public_dir(), self.wiki.date, filename)
            fileinfo[filename]['status'] = 'present'
            try:
                size = os.path.getsize(path)
                fileinfo[filename]['size'] = size
            except Exception:
                fileinfo[filename]['status'] = 'missing'
                continue

            dfname = DumpFilename(self.wiki)
            dfname.new_from_filename(os.path.basename(path))
            fileinfo[filename]['url'] = dump_dir.web_path_relative(dfname)

        contents = {}
        contents['files'] = fileinfo
        contents['version'] = SpecialFileInfo.VERSION

        try:
            self.write_contents(contents)
        except Exception:
            if self.verbose:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value,
                                                                 exc_traceback)))
            message = "Couldn't write special files info. Continuing anyways"
            if self.error_callback:
                self.error_callback(message)
            else:
                sys.stderr.write("%s\n" % message)
