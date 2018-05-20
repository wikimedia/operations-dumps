"""
Handle creation, updating and moving the files that
contain md5 or other checksums of dumps content
"""
import os
import time
import json

from dumps.fileutils import DumpContents, DumpFilename, FileUtils
from dumps.specialfilesregistry import Registered


class Checksummer(Registered):
    """
    methods for producing and managing files of md5
    or other checksums of dump content files
    """
    NAME = "checksum"
    HASHTYPES = ['md5', 'sha1']
    FORMATS = ['txt', 'json']

    @staticmethod
    def get_checksum_filename_basename(htype, fmt="txt"):
        """
        given the hash type and the file format, return
        the base filename that would contain the hashes
        for that file format
        """
        if fmt == "json":
            ext = "json"
        else:
            # default
            ext = "txt"

        if htype == "md5":
            return "md5sums." + ext
        elif htype == "sha1":
            return "sha1sums." + ext
        else:
            return None

    @staticmethod
    def get_checksum_basename_perfile(htype, filename):
        '''
        return the base name of the file where the hash of the
        specific type will be written for the specified file
        this is only in txt format.
        '''
        if htype == "md5":
            return "md5sums-{fname}.txt".format(fname=filename)
        elif htype == "sha1":
            return "sha1sums-{fname}.txt".format(fname=filename)
        else:
            return None

    @staticmethod
    def get_hashinfo(filename, jsoninfo):
        """
        given json output from the checksum json file,
        find and return list of tuples (hashtype, sum) for the
        file
        """
        results = []
        if not jsoninfo:
            return results
        for htype in jsoninfo:
            if filename in jsoninfo[htype]["files"]:
                results.append((htype, jsoninfo[htype]["files"][filename]))
        return results

    @staticmethod
    def get_empty_json():
        """
        return dict suitable for conversion to json file with
        no hash entries in it
        """
        return {}

    @staticmethod
    def is_more_recent(fileone, filetwo):
        '''
        return True if fileone is more recent (mtime) than filetwo,
        False otherwise
        if fileone or filetwo is not available or stat can't be done,
        return False then too
        '''
        try:
            mtimeone = os.stat(fileone).st_mtime
            mtimetwo = os.stat(filetwo).st_mtime
        except Exception:
            return False
        return bool(mtimeone > mtimetwo)

    @staticmethod
    def get_checksum_from_file(path):
        '''
        get the checksum recorded in a file which should have
        one line, consisting of the checksum, two spaces, and
        the filename that was checksummed
        return None on any error
        '''
        try:
            content = FileUtils.read_file(path)
            checksum, _filename = content.split('  ', 1)
            return checksum
        except Exception:
            return None

    def __init__(self, wiki, enabled, dump_dir=None, verbose=False):
        super(Checksummer, self).__init__()
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.verbose = verbose
        self.timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        self._enabled = enabled

    def get_per_file_path(self, htype, filename):
        '''
        return the full path to the file containing the checksum of
        the specified type for the given filename. this is only in txt format
        '''
        dfname = DumpFilename(self.wiki, None)
        # fixme check to see if this is right or what
        dfname.new_from_filename(Checksummer.get_checksum_basename_perfile(htype, filename))
        return self.dump_dir.filename_public_path(dfname)

    def prepare_checksums(self):
        """Create a temporary md5 or other checksum file.
        Call this at the start of the dump run, and move the file
        into the final location at the completion of the dump run."""
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                for fmt in Checksummer.FORMATS:
                    checksum_filename = self._get_checksum_filename_tmp(htype, fmt)
                    with open(checksum_filename, "w") as output_fhandle:
                        if fmt == "json":
                            output_fhandle.write(json.dumps({htype: {"files": {}}}))
                        output_fhandle.close()

    def checksums(self, dfname, dumpjobdata):
        """
        Run checksum for an output file, and append to the list.
        args:
            DumpFilename, ...
        """
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                checksum_filename_txt = self._get_checksum_filename_tmp(htype, "txt")
                checksum_filename_json = self._get_checksum_filename_tmp(htype, "json")
                output_txt = file(checksum_filename_txt, "a")
                # for txt file, append our new line. for json file, must read
                # previous contents, stuff our new info into the dict, write it
                # back out
                output = {}
                try:
                    with open(checksum_filename_json, "r") as fhandle:
                        contents = fhandle.read()
                        output = json.loads(contents)
                except Exception:
                    # might be empty file, as at the start of a run
                    pass
                if not output:
                    # at least let's not write new bad content into a
                    # possibly corrupt file.
                    output = {htype: {"files": {}}}
                output_json = file(checksum_filename_json, "w")
                checksum = None
                update_per_file = False

                per_file_path = self.get_per_file_path(htype, dfname.filename)
                if os.path.exists(per_file_path) and self.is_more_recent(
                        per_file_path, dumpjobdata.dump_dir.filename_public_path(dfname)):
                    dumpjobdata.debugfn("Reading %s checksum for %s from file %s" % (
                        dfname.filename, htype, per_file_path))
                    checksum = self.get_checksum_from_file(per_file_path)

                if checksum is None:
                    dcontents = DumpContents(
                        self.wiki, dumpjobdata.dump_dir.filename_public_path(dfname),
                        None, self.verbose)
                    dumpjobdata.debugfn("Checksumming %s via %s" % (dfname.filename, htype))
                    checksum = dcontents.checksum(htype)
                    update_per_file = True

                if checksum is not None:
                    output_txt.write("%s  %s\n" % (checksum, dfname.filename))
                    if update_per_file:
                        output_perfile_txt = file(per_file_path, "wt")
                        output_perfile_txt.write("%s  %s\n" % (checksum, dfname.filename))
                        output_perfile_txt.close()
                    output[htype]["files"][dfname.filename] = checksum
                # always write a json stanza, even if no file info included.
                output_json.write(json.dumps(output))
                output_txt.close()
                output_json.close()

    def move_chksumfiles_into_place(self):
        """
        to be called once a checksums file has been
        fully updated during dumps run:
        move the checksums file from temporary name
        into permanent location
        """
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                for fmt in Checksummer.FORMATS:
                    tmp_filename = self._get_checksum_filename_tmp(htype, fmt)
                    real_filename = self._get_checksum_path(htype, fmt)
                    os.rename(tmp_filename, real_filename)

    def cp_chksum_tmpfiles_to_permfile(self):
        """
        during a dump run, checksum files are written to a temporary
        location and updated there; we copy the content from these
        files into the permanent location after each dump job
        completes
        """
        if Checksummer.NAME in self._enabled:
            for htype in Checksummer.HASHTYPES:
                for fmt in Checksummer.FORMATS:
                    tmp_filename = self._get_checksum_filename_tmp(htype, fmt)
                    real_filename = self._get_checksum_path(htype, fmt)
                    content = FileUtils.read_file(tmp_filename)
                    FileUtils.write_file(
                        FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
                        real_filename, content,
                        self.wiki.config.fileperms)

    def get_all_output_files(self):
        """
        return a list of all checksum files in all formats
        """
        files = []
        for htype in Checksummer.HASHTYPES:
            for fmt in Checksummer.FORMATS:
                files.append(self._get_checksum_filename(htype, fmt))
        return files

    #
    # functions internal to the class
    #

    def _get_checksum_filename(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt))
        return dfname.filename

    def _get_checksum_path(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            full path of output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt))
        return self.dump_dir.filename_public_path(dfname)

    def _get_checksum_filename_tmp(self, htype, fmt):
        """
        args:
            hashtype ('md5', 'sha1',...)
            format of output ('json', 'txt', ...)
        returns:
            full path of a unique-enough temporary output file for wiki and date
        """
        dfname = DumpFilename(self.wiki, None,
                              Checksummer.get_checksum_filename_basename(htype, fmt) +
                              "." + self.timestamp + ".tmp")
        return os.path.join(FileUtils.wiki_tempdir(
            self.wiki.db_name, self.wiki.config.temp_dir), dfname.filename)

    def _getmd5file_dir_name(self):
        """
        returns:
            directory for dump wiki and date, in which hashfiles reside
        """
        return os.path.join(self.wiki.public_dir(), self.wiki.date)
