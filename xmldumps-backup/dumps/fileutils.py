#!/usr/bin/python3
# Worker process, does the actual dumping

import hashlib
import os
from os.path import exists
import errno
import re
import sys
import time
import traceback
import tempfile
import shutil

from dumps.commandmanagement import CommandPipeline
from dumps.exceptions import BackupError
from dumps.utils import MiscUtils


class FileUtils():

    @staticmethod
    def file_age(filepath):
        return time.time() - os.stat(filepath).st_mtime

    @staticmethod
    def atomic_create(filepath, mode='w'):
        """Create a file, aborting if it already exists..."""
        fdesc = os.open(filepath, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
        return os.fdopen(fdesc, mode)

    @staticmethod
    def write_file(dirname, filepath, text, perms=0):
        """Write text to a file, as atomically as possible,
        via a temporary file in a specified directory.
        Arguments: dirname = where temp file is created,
        filepath = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation"""

        if not os.path.isdir(dirname):
            try:
                os.makedirs(dirname)
            except Exception as ex:
                raise IOError("The given directory '%s' is neither "
                              "a directory nor can it be created" % dirname)

        (fdesc, temp_filepath) = tempfile.mkstemp("_txt", "wikidump_", dirname)
        os.write(fdesc, text.encode('utf-8'))
        os.close(fdesc)
        if perms:
            os.chmod(temp_filepath, perms)
        # This may fail across filesystems or on Windows.
        # Of course nothing else will work on Windows. ;)
        shutil.move(temp_filepath, filepath)

    @staticmethod
    def write_file_in_place(filepath, text, perms=0):
        """Write text to a file, after opening it for write with truncation.
        This assumes that only one process or thread accesses the given file at a time.
        Arguments: filepath = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation,
        if it did not exist already"""

        fhandle = open(filepath, "wt")
        fhandle.write(text)
        fhandle.close()
        if perms:
            os.chmod(filepath, perms)

    @staticmethod
    def read_file(filepath):
        """Read text from a file in one fell swoop."""
        fhandle = open(filepath, "r")
        text = fhandle.read()
        fhandle.close()
        return text

    @staticmethod
    def split_path(path):
        # For some reason, os.path.split only does one level.
        parts = []
        (path, filename) = os.path.split(path)
        if not filename:
            # Probably a final slash
            (path, filename) = os.path.split(path)
        while filename:
            parts.insert(0, filename)
            (path, filename) = os.path.split(path)
        return parts

    @staticmethod
    def relative_path(path, base):
        """Return a relative path to 'path' from the directory 'base'."""
        path = FileUtils.split_path(path)
        base = FileUtils.split_path(base)
        while base and path[0] == base[0]:
            path.pop(0)
            base.pop(0)
        for _prefix in base:
            path.insert(0, "..")
        return os.path.join(*path)

    @staticmethod
    def pretty_size(size):
        """Return a string with an attractively formatted file size."""
        quanta = ("%d bytes", "%d KB", "%0.1f MB", "%0.1f GB", "%0.1f TB")
        return FileUtils._pretty_size(size, quanta)

    @staticmethod
    def _pretty_size(size, quanta):
        if size < 1024 or len(quanta) == 1:
            return quanta[0] % size
        return FileUtils._pretty_size(size / 1024.0, quanta[1:])

    @staticmethod
    def file_info(path):
        """Return a tuple of date/time and size of a file, or None, None"""
        try:
            timestamp = time.gmtime(os.stat(path).st_mtime)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
            size = os.path.getsize(path)
            return (timestamp, size)
        except Exception as ex:
            return(None, None)

    @staticmethod
    def wiki_tempdir(wikiname, tempdirbase, create=False):
        """Return path to subdir for temp files for specified wiki"""
        wikisubdir = os.path.join(tempdirbase, wikiname[0], wikiname)
        if create:
            try:
                os.makedirs(wikisubdir)
            except OSError as ex:
                if ex.errno == errno.EEXIST:
                    pass
        return wikisubdir


class DumpFilename():
    """
    filename without directory name, and the methods that go with it,
    primarily for filenames that follow the standard naming convention, i.e.
    projectname-date-dumpname.sql/xml.gz/bz2/7z (possibly with a file part
    number, possibly with start/end page id information embedded in the name).

    Constructor:
    DumpFilename(dumpname, date = None, filetype, ext, partnum = None,
                 checkpoint = None, temp = False) -- pass in dumpname and
                      filetype/extension at least. filetype is one of xml sql,
                      extension is one of bz2/gz/7z.  Or you can pass in
                      the entire string without project name and date,
                      e.g. pages-meta-history5.xml.bz2
                      If dumpname is not passed, no member variables will be initialized,
                      and the caller is expected to invoke new_from_filename as an alternate
                      constructor before doing anything else with the object.

    new_from_filename(filename)  -- pass in full filename. This is called by the
                      regular constructor and is what sets all attributes

    attributes:

    is_checkpoint_file  filename of form dbname-date-dumpname-pxxxxpxxxx.xml.bz2
    is_file_part        filename of form dbname-date-dumpnamex.xml.gz/bz2/7z
    is_temp_file        filename of form dbname-date-dumpname.xml.gz/bz2/7z-tmp
    is_inprog           filename of form dbname-date-dumpname.<stuff>.inprog
    first_page_id       for checkpoint files, taken from value in filename
    first_page_id_int   for checkpoint files, taken from value in filename
    last_page_id      for checkpoint files, value taken from filename
    last_page_id_int  for checkpoint files, value taken from filename
    filename          full filename
    basename          part of the filename after the project name and date (for
                          "enwiki-20110722-pages-meta-history12.xml.bz2" this would be
                          "pages-meta-history12.xml.bz2")
    file_ext          extension (everything after the last ".") of the file
    date              date embedded in filename
    dumpname          dump name embedded in filename (eg "pages-meta-history"), if any
    partnum           number of file part as string (for "pages-meta-history5.xml.bz2"
                      this would be "5")
    partnum_int       part number as int
    """

    INPROG = ".inprog"  # extension for dump output files that are in progress (not fully written)

    @staticmethod
    def safe_lessthan(numstring_a, numstring_b):
        """
        compare two numeric strings either of which might be None; None is treated as
        equivalent to 0
        strings that are not None must be numeric; we're not THAT safe
        return True if first string is less than the second
        """
        if numstring_a is None:
            return bool(numstring_b is None)
        if numstring_b is None:
            return True
        return bool(int(numstring_a) < int(numstring_b))

    @staticmethod
    def safe_greaterthan(numstring_a, numstring_b):
        """
        compare two numeric strings either of which might be None; None is treated as
        equivalent to 0
        strings that are not None must be numeric; we're not THAT safe
        return True if first string is greater than the second
        """
        return DumpFilename.safe_lessthan(numstring_b, numstring_a)

    @staticmethod
    def compare(dfname1, dfname2):
        """
        return -1, 0 or 1 depending on whether dfname1's filename is less than,
        equal to, or greater than dfname2's filename
        if the dumpname components of the filenames are not equal, then
        the appropriate value will be returned based on that comparison
        otherwise ordering is done by:
        partnum then page range values, for filenames that contain those
        a file without a partnum comes before a file that has one
        a file without a page range comes before a file that has one
        """
        if dfname1.dumpname < dfname2.dumpname:
            return -1
        if dfname1.dumpname > dfname2.dumpname:
            return 1

        if dfname1.partnum_int < dfname2.partnum_int:
            return -1
        if dfname1.partnum_int > dfname2.partnum_int:
            return 1

        if DumpFilename.safe_lessthan(dfname1.first_page_id, dfname2.first_page_id):
            return -1
        if DumpFilename.safe_greaterthan(dfname1.first_page_id, dfname2.first_page_id):
            return 1

        if DumpFilename.safe_lessthan(dfname1.last_page_id, dfname2.last_page_id):
            return -1
        if DumpFilename.safe_greaterthan(dfname1.last_page_id, dfname2.last_page_id):
            return 1

        return 0

    @staticmethod
    def make_checkpoint_string(first_page_id, last_page_id):
        """
        args may be either:
            string or None
            int or 0
        in the case of None (or 0) for either arg, this method is
        undefined and will return None
        """
        if first_page_id is None or last_page_id is None:
            return None
        if first_page_id and last_page_id:
            return "p" + str(first_page_id) + "p" + str(last_page_id)
        return None

    @staticmethod
    def get_inprogress_name(filename):
        return filename + DumpFilename.INPROG

    def __init__(self, wiki, date=None, dump_name=None, filetype=None,
                 ext=None, partnum=None, checkpoint=None, temp=False):
        """Constructor.  Arguments: the dump name as it should appear in the filename,
        the date if different than the date of the dump run, the file part number
        if there is one, and temp which is true if this is a temp file (ending in "-tmp")
        Alternatively, one can leave off all other other stuff and just pass the entire
        filename minus the dbname and the date. Returns true on success, false otherwise.."""
        self.wiki = wiki
        # if dump_name is not set, the caller can call
        # new_from_filename to initialize various values instead
        if dump_name:
            filename = self.new_filename(dump_name, filetype, ext, date, partnum, checkpoint, temp)
            self.new_from_filename(filename)

    def __eq__(self, other):
        """
        it's all boring old attributes, so we can do this
        """
        if other is None:
            return False
        if ((self.is_checkpoint_file, self.is_file_part, self.is_temp_file, self.is_inprog) !=
                (other.is_checkpoint_file, other.is_file_part,
                 other.is_temp_file, other.is_inprog)):
            return False
        if ((self.first_page_id, self.last_page_id, self.filename, self.basename) !=
                (other.first_page_id, other.last_page_id, other.filename, other.basename)):
            return False
        if ((self.file_ext, self.date, self.dumpname, self.partnum) !=
                (other.file_ext, other.date, other.dumpname, other.partnum)):
            return False
        return self.partnum_int == other.partnum_int

    def is_ext(self, ext):
        return bool(ext in ["gz", "bz2", "7z", "html", "txt"])

    def new_from_filename(self, filename):
        '''
        Constructor.  Arguments: the full file name including
        the file part number, the extension, etc BUT NOT the dir name.
        returns True if successful, False otherwise
        (filename is not in the canonical form that we manage)
        '''
        self.filename = filename

        self.db_name = None
        self.date = None
        self.dumpname = None

        self.basename = None
        self.file_ext = None
        self.file_type = None

        self.file_prefix = ""
        self.file_prefix_length = 0

        self.is_file_part = False
        self.partnum = None
        self.partnum_int = 0

        self.is_checkpoint_file = False
        self.checkpoint = None
        self.first_page_id = None
        self.last_page_id = None
        self.first_page_id_int = 0
        self.last_page_id_int = 0

        self.is_temp_file = False
        self.temp = None
        self.is_inprog = False

        # example filenames:
        # elwikidb-20110729-all-titles-in-ns0.gz
        # elwikidb-20110729-abstract.xml
        # elwikidb-20110727-pages-meta-history2.xml-p000048534p000051561.bz2

        # we need to handle cases without the projectname-date stuff in them too, as this gets used
        # for all files now
        if self.filename.endswith("-tmp"):
            self.is_temp_file = True
            self.temp = "-tmp"

        if self.filename.endswith(".inprog"):
            self.is_inprog = True

        if self.is_inprog:
            splitme = self.filename[:-7]  # get rid of .inprog at end
        else:
            splitme = self.filename
        if '.' in splitme:
            (file_base, self.file_ext) = splitme.rsplit('.', 1)
            if self.temp:
                self.file_ext = self.file_ext[:-4]
        else:
            return False

        if not self.is_ext(self.file_ext):
            self.file_type = self.file_ext
#            self.file_ext = None
            self.file_ext = ""
        else:
            if '.' in file_base:
                (file_base, self.file_type) = file_base.split('.', 1)

        # some files are not of this form, we skip them
        if '-' not in file_base:
            return False

        (self.db_name, self.date, self.dumpname) = file_base.split('-', 2)
        if not self.date or not self.dumpname:
            self.dumpname = file_base
        else:
            self.file_prefix = "%s-%s-" % (self.db_name, self.date)
            self.file_prefix_length = len(self.file_prefix)

        if self.filename.startswith(self.file_prefix):
            self.basename = self.filename[self.file_prefix_length:]

        self.checkpoint_pattern = r"-p(?P<first>[0-9]+)p(?P<last>[0-9]+)\." + self.file_ext
        if self.temp is not None:
            self.checkpoint_pattern += self.temp + "$"
        elif self.is_inprog:
            self.checkpoint_pattern += ".inprog$"
        else:
            self.checkpoint_pattern += "$"

        self.compiled_checkpoint_pattern = re.compile(self.checkpoint_pattern)
        result = self.compiled_checkpoint_pattern.search(self.filename)
        if result:
            self.is_checkpoint_file = True
            self.first_page_id = result.group('first')
            self.last_page_id = result.group('last')
            self.checkpoint = "p" + self.first_page_id + "p" + self.last_page_id
            if self.file_type and self.file_type.endswith("-" + self.checkpoint):
                self.file_type = self.file_type[:-1 * (len(self.checkpoint) + 1)]
        if self.first_page_id is not None:
            self.first_page_id_int = int(self.first_page_id)
        if self.last_page_id is not None:
            self.last_page_id_int = int(self.last_page_id)

        self.partnum_pattern = "(?P<partnum>[0-9]+)$"
        self.compiled_partnum_pattern = re.compile(self.partnum_pattern)
        result = self.compiled_partnum_pattern.search(self.dumpname)
        if result:
            self.is_file_part = True
            self.partnum = result.group('partnum')
            self.partnum_int = int(self.partnum)
            # the dumpname has the file part number in it so lose it
            self.dumpname = self.dumpname.rstrip('0123456789')

        return True

    def new_filename(self, dump_name, filetype, ext, date=None,
                     partnum=None, checkpoint=None, temp=None):
        if not partnum:
            partnum = ""
        if not date:
            date = self.wiki.date
        # fixme do the right thing in case no filetype or no ext
        fields = []
        fields.append(self.wiki.db_name + "-" + date + "-" + dump_name + "%s" % partnum)
        if checkpoint:
            filetype = filetype + "-" + checkpoint
        if filetype:
            fields.append(filetype)
        if ext:
            fields.append(ext)
        filename = ".".join(fields)
        if temp:
            filename = filename + "-tmp"
        return filename

    def set_first_page_id(self, value):
        if value is None:
            self.first_page_id = None
            self.first_page_id_int = 0
        else:
            self.first_page_id = str(value)
            self.first_page_id_int = int(value)

    def set_last_page_id(self, value):
        if value is None:
            self.last_page_id = None
            self.last_page_id_int = 0
        else:
            self.last_page_id = str(value)
            self.last_page_id_int = int(value)


class DumpContents():
    """Methods for dealing with dump contents in a file containing output
    created by any job of a jump run.  This includes
    any file that follows the standard naming convention, i.e.
    projectname-date-dumpname.sql/xml.gz/bz2/7z (possibly with a file part
    number, possibly with start/end page id information embedded in the name).

    Methods:

    md5sum(): return md5sum of the contents.
    sha1sum(): return sha1sum of the contents.
    checksum(htype): return checksum of the specified type, of the contents.
    check_if_truncated(): for compressed files, check if the file is truncated (stops
       abruptly before the end of the compressed data) or not, and set and return
         self.is_truncated accordingly.  This is fast for bzip2 files
       and slow for gz and 7z fles, since for the latter two types it must serially
       read through the file to determine if it is truncated or not.
    get_size(): returns the current size of the file contents in bytes
    rename(newname): rename the file. Arguments: the new name of the file without
       the directory.
    find_first_page_id_in_file(): set self.first_page_id by examining the contents,
       returning the value, or None if there is no pageID.  We uncompress the file
       if needed and look through the first 500 lines.
    def find_last_page_id_in_file(self): set self.last_page_id by examining the contents

    useful variables:

    first_page_id       Determined by examining the first few hundred lines of the contents,
                          looking for page and id tags, wihout other tags in between. (hmm)
    filename          full filename with directory
    """
    def __init__(self, wiki, filename, dfname=None, verbose=False):
        """
        args:
            Wiki, full path to file, DumpFilename, bool

        If dfname is set, it will be used, otherwise 'filename' must be not None
        """
        self._wiki = wiki
        self.filename = filename
        self.first_lines = None
        self.is_truncated = None
        self.is_empty = None
        self.is_binary = None
        self.first_page_id = None
        self.first_page_id_int = 0
        self.last_page_id = None
        self.last_page_id_int = 0
        self.dirname = os.path.dirname(filename)
        if dfname:
            self.dfname = dfname
        else:
            self.dfname = DumpFilename(wiki)
            self.dfname.new_from_filename(os.path.basename(filename))
        if verbose:
            sys.stderr.write("setting up info for %s\n" % filename)

    def _checksum(self, summer):
        if not self.filename:
            return None
        infhandle = open(self.filename, "rb")
        bufsize = 4192 * 32
        fbuffer = infhandle.read(bufsize)
        while fbuffer:
            summer.update(fbuffer)
            fbuffer = infhandle.read(bufsize)
        infhandle.close()
        return summer.hexdigest()

    def md5sum(self):
        summer = hashlib.md5()
        return self._checksum(summer)

    def sha1sum(self):
        summer = hashlib.sha1()
        return self._checksum(summer)

    def checksum(self, htype):
        if htype == "md5":
            return self.md5sum()
        if htype == "sha1":
            return self.sha1sum()
        return None

    def get_first_500_lines(self):
        if self.first_lines:
            return self.first_lines

        if not self.filename or not exists(self.filename):
            return None

        pipeline = self.setup_uncompression_command()

        if not exists(self._wiki.config.head):
            raise BackupError("head command %s not found" % self._wiki.config.head)
        head = self._wiki.config.head
        pipeline.append([head, "-500"])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        if (proc.exited_successfully() or
                proc.get_failed_cmds_with_retcode() in [
                    [[pipevalue, pipeline[0]]]
                    for pipevalue in MiscUtils.get_sigpipe_values()]):
            self.first_lines = proc.output()
        return self.first_lines.decode('utf-8')

    # unused
    # xml, sql, text
    def determine_file_contents_type(self):
        output = self.get_first_500_lines()
        if output:
            page_data = output
            if page_data.startswith('<mediawiki'):
                return 'xml'
            if page_data.startswith('-- MySQL dump'):
                return 'sql'
            return 'txt'
        return None

    def setup_uncompression_command(self):
        if not self.filename or not exists(self.filename):
            return None
        pipeline = []
        if self.dfname.file_ext == 'bz2':
            command = [self._wiki.config.bzip2, '-dc']
        elif self.dfname.file_ext == 'gz':
            command = [self._wiki.config.gzip, '-dc']
        elif self.dfname.file_ext == '7z':
            command = [self._wiki.config.sevenzip, "e", "-so"]
        else:
            command = [self._wiki.config.cat]

        if not exists(command[0]):
            raise BackupError("command %s to uncompress/read file not found" % command[0])
        command.append(self.filename)
        pipeline.append(command)
        return pipeline

    # unused
    # return its first and last page ids from name or from contents, depending
    # return its date

    # fixme what happens if this is not an xml dump? errr. must detect and bail immediately?
    # maybe instead of all that we should just open the file ourselves, read a few lines... oh.
    # right. stupid compressed files. um.... do we have stream wrappers? no. this is python
    # what's the easy was to read *some* compressed data into a buffer?
    def find_first_page_id_in_file(self):
        if self.first_page_id_int:
            return self.first_page_id_int
        output = self.get_first_500_lines()
        if output:
            page_data = output
            title_and_id_pattern = re.compile(r'<title>(?P<title>.+?)</title>\s*' +
                                              r'(<ns>[0-9]+</ns>\s*)?' +
                                              r'<id>(?P<pageid>\d+?)</id>')
            result = title_and_id_pattern.search(page_data)
            if result:
                self.first_page_id = result.group('pageid')
                self.first_page_id_int = int(self.first_page_id)
        return self.first_page_id_int

    def get_last_lines(self, count):
        """
        read last count lines from compressed file,
        using tail to get the lines, and return the content
        as one long string

        args: Runner, number of lines to read
        """
        pipeline = self.setup_uncompression_command()

        tail = self._wiki.config.tail
        if not exists(tail):
            raise BackupError("tail command %s not found" % tail)
        pipeline.append([tail, "-n", "+%s" % count])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        last_lines = ""
        if (proc.exited_successfully() or
                proc.get_failed_cmds_with_retcode() in [
                    [[pipevalue, pipeline[0]]]
                    for pipevalue in MiscUtils.get_sigpipe_values()]):

            last_lines = proc.output()
        return last_lines.decode('utf-8')

    def get_lineno_last_page(self):
        """
        find and return the line number of the last page tag
        in the file
        """
        pipeline = self.setup_uncompression_command()
        grep = self._wiki.config.grep
        if not exists(grep):
            raise BackupError("grep command %s not found" % grep)
        pipeline.append([grep, "-n", "<page>"])
        tail = self._wiki.config.tail
        if not exists(tail):
            raise BackupError("tail command %s not found" % tail)
        pipeline.append([tail, "-1"])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()

        if (proc.exited_successfully() or
                proc.get_failed_cmds_with_retcode() in [
                    [[pipevalue, pipeline[0]]]
                    for pipevalue in MiscUtils.get_sigpipe_values()]):

            output = proc.output().decode('utf-8')
            # 339915646:  <page>
            if ':' in output:
                linecount = output.split(':')[0]
                if linecount.isdigit():
                    return linecount
        return None

    def find_last_page_id(self):
        """
        find and return the last page id in a compressed
        stub or page content xml file, using uncompression
        command (bzcat, zcat) and tail

        arg: Runner
        """
        if self.last_page_id_int:
            return self.last_page_id_int
        count = self.get_lineno_last_page()
        lastlines = self.get_last_lines(count)
        # now look for the last page id in here. eww
        if not lastlines:
            return None
        title_and_id_pattern = re.compile(r'<title>(?P<title>.+?)</title>\s*' +
                                          r'(<ns>[0-9]+</ns>\s*)?' +
                                          r'<id>(?P<pageid>\d+?)</id>')
        result = None
        for result in re.finditer(title_and_id_pattern, lastlines):
            pass
        if result:
            self.last_page_id = result.group('pageid')
            self.last_page_id_int = int(self.last_page_id)
        return self.last_page_id_int

    def check_if_truncated(self, last_tag=None):
        '''
        NOTE: last_tag, if set, must be a b' type, so that we can compare it
        to output retrieved from a command run by Popen
        '''
        if self.is_truncated:
            return self.is_truncated

        shell = bool(last_tag)

        # Setting up the pipeline depending on the file extension
        if self.dfname.file_ext == "bz2":
            if not exists(self._wiki.config.checkforbz2footer):
                raise BackupError("checkforbz2footer command %s not found" %
                                  self._wiki.config.checkforbz2footer)
            if last_tag:
                # see if file is compressed right AND has appropriate last tag
                pipeline = [self._wiki.config.dumplastbz2block, self.filename, '|',
                            self._wiki.config.tail, '-1']
            else:
                pipeline = [self._wiki.config.checkforbz2footer, self.filename]
        elif self.dfname.file_ext == 'gz':
            if last_tag:
                pipeline = [self._wiki.config.gzip, "-dc", self.filename,
                            "|", self._wiki.config.tail, '-1']
            else:
                pipeline = [self._wiki.config.gzip, "-dc", self.filename,
                            ">", "/dev/null"]
        elif self.dfname.file_ext == '7z':
            # Note that 7z does return 0, if archive contains
            # garbage /after/ the archive end
            if last_tag:
                pipeline = [self._wiki.config.sevenzip, "e", "-so",
                            self.filename, "|", self._wiki.config.tail, '-1']
            else:
                pipeline = [self._wiki.config.sevenzip, "e", "-so",
                            self.filename, ">", "/dev/null"]
        else:
            # we do't know how to handle this type of file.
            return self.is_truncated

        if shell:
            pipeline = ' '.join(pipeline)
        pipeline = [pipeline]
        # Run the prepared pipeline
        proc = CommandPipeline(pipeline, shell=shell, quiet=True)
        proc.run_pipeline_get_output()
        self.is_truncated = not proc.exited_successfully()
        if last_tag and not self.is_truncated:
            # there might be a newline or something after the tag, so 'startswith' is good enough
            output = proc.output()
            self.is_truncated = bool(not output or not output.startswith(last_tag))
        return self.is_truncated

    def check_if_empty(self):
        if self.is_empty:
            return self.is_empty
        if self.dfname.file_ext == "bz2":
            pipeline = [["%s -dc  %s | head -5" % (self._wiki.config.bzip2, self.filename)]]
        elif self.dfname.file_ext == "gz":
            pipeline = [["%s -dc %s | head -5" % (self._wiki.config.gzip, self.filename)]]
        elif self.dfname.file_ext == '7z':
            pipeline = [["%s e -so %s | head -5" % (self._wiki.config.sevenzip, self.filename)]]
        elif (self.dfname.file_ext == '' or self.dfname.file_ext == 'txt' or
              self.dfname.file_ext == 'html'):
            pipeline = [["head -5 %s" % self.filename]]
        else:
            # we do't know how to handle this type of file.
            return self.is_empty

        proc = CommandPipeline(pipeline, quiet=True, shell=True)
        proc.run_pipeline_get_output()
        self.is_empty = bool(not proc.output())

        return self.is_empty

    def check_if_binary_crap(self):
        '''
        check if the file has binary junk in it
        Only good for xml files, they are expected to start
        with the mediawiki tag. Other files (eg sql) start
        with something else, we don't check those
        Note we expect real content to be present within the
        2000 lines arbitrarily chosen as the cutoff
        '''
        if 'xml' not in self.dfname.filename:
            return False

        if self.is_binary:
            return self.is_binary
        rest = "| head -2000 | cat -vte | grep '@^@^@' | wc -l"
        if self.dfname.file_ext == "bz2":
            pipeline = [["{bzip2} -dc  {fname} {rest}".format(
                bzip2=self._wiki.config.bzip2, fname=self.filename, rest=rest)]]
        elif self.dfname.file_ext == "gz":
            pipeline = [["{gzip} -dc {fname} {rest}".format(
                gzip=self._wiki.config.gzip, fname=self.filename, rest=rest)]]
        elif self.dfname.file_ext == '7z':
            pipeline = [["{sevenz} e -so {fname} {rest}".format(
                sevenz=self._wiki.config.sevenzip, fname=self.filename, rest=rest)]]
        elif (self.dfname.file_ext == '' or self.dfname.file_ext == 'txt' or
              self.dfname.file_ext == 'html'):
            pipeline = [["{cat} {fname} {rest}".format(
                cat=self._wiki.config.cat, fname=self.filename, rest=rest)]]
        else:
            # we do't know how to handle this type of file.
            return self.is_binary

        proc = CommandPipeline(pipeline, quiet=True, shell=True)
        proc.run_pipeline_get_output()
        result = proc.output().rstrip()
        self.is_binary = bool(result != b'0')

        return self.is_binary

    def get_size(self):
        if exists(self.filename):
            return os.path.getsize(self.filename)
        return None

    def rename(self, newname):
        try:
            os.rename(self.filename, os.path.join(self.dirname, newname))
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise BackupError("failed to rename file %s" % self.filename)

        self.filename = os.path.join(self.dirname, newname)


class DumpDir():
    BAD = [".truncated", ".empty"]

    def __init__(self, wiki, db_name):
        self._wiki = wiki
        self._db_name = db_name
        self._dir_cache = {}
        self._dir_cache_time = {}
        self._filepart_cache = {}
        self._checkpoint_file_cache = {}

    def filename_private_path(self, dfname, date_string=None):
        """Given a DumpFilename object, produce the full path to the filename in the date subdir
        of the the private dump dir for the selected database.
        If a different date is specified, use that instead"""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.private_dir(), date_string, dfname.filename)

    def filename_public_path(self, dfname, date_string=None):
        """Given a DumpFilename object produce the full path to the filename in the date subdir
        of the public dump dir for the selected database.
        If this database is marked as private, use the private dir instead.
        If a different date is specified, use that instead"""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.public_dir(), date_string, dfname.filename)

    def latest_dir(self):
        """Return 'latest' directory for the current project being dumped, e.g.
        if the current project is enwiki, this would return something like
        /mnt/data/xmldatadumps/public/enwiki/latest (if the directory /mnt/data/xmldatadumps/public
        is the path to the directory for public dumps)."""
        return os.path.join(self._wiki.public_dir(), "latest")

    def web_path(self, dfname, date_string=None):
        """Given a DumpFilename object produce the full url to the filename for the date of
        the dump for the selected database."""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.web_dir(), date_string, dfname.filename)

    def web_path_relative(self, dfname, date_string=None):
        """Given a DumpFilename object produce the url relative
        to the docroot for the filename for the date of
        the dump for the selected database."""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.web_dir_relative(), date_string, dfname.filename)

    def dir_cache_outdated(self, date):
        if not date:
            date = self._wiki.date
        directory = os.path.join(self._wiki.public_dir(), date)
        if exists(directory):
            dir_time_stamp = os.stat(directory).st_mtime
            return bool(date not in self._dir_cache or dir_time_stamp > self._dir_cache_time[date])
        return True

    # warning: date can also be "latest"
    def get_files_in_dir(self, date=None):
        """
        args:
            date in YYYYMMDD format
        returns:
            list of DumpFilename
        """
        if not date:
            date = self._wiki.date
        if self.dir_cache_outdated(date):
            directory = os.path.join(self._wiki.public_dir(), date)
            if exists(directory):
                dir_time_stamp = os.stat(directory).st_mtime
                filenames = os.listdir(directory)
                dfnames = []
                for filename in filenames:
                    dfname = DumpFilename(self._wiki)
                    dfname.new_from_filename(filename)
                    dfnames.append(dfname)
                self._dir_cache[date] = dfnames
                # The directory listing should get cached. However, some tyical file
                # system's (eg. ext2, ext3) mtime's resolution is 1s. If we would
                # unconditionally cache, it might happen that we cache at x.1 seconds
                # (with mtime x). If a new file is added to the filesystem at x.2,
                # the directory's mtime would still be set to x. Hence we would not
                # detect that the cache needs to be purged. Therefore, we cache only,
                # if adding a file now would yield a /different/ mtime.
                if time.time() >= dir_time_stamp + 1:
                    self._dir_cache_time[date] = dir_time_stamp
                else:
                    # By setting _dir_cache_time to 0, we provoke an outdated cache
                    # on the next check. Hence, we effectively do not cache.
                    self._dir_cache_time[date] = 0
            else:
                self._dir_cache[date] = []
        return self._dir_cache[date]

    def _get_files_filtered(self, date=None, dump_name=None, file_type=None,
                            file_ext=None, parts=None, temp=None, checkpoint=None,
                            skip_suffixes=None, required_suffixes=None, inprog=False):
        '''
        list all files that exist, filtering by the given args.
        if we get None for an arg then we accept all values
        for that arg in the filename, including missing
        if we get False for an arg (parts, temp, checkpoint),
        we reject any filename which contains a value for that arg
        if we get True for an arg (parts, temp, checkpoint),
        we include only filenames which contain a value for that arg
        parts should be a list of value(s) or True / False / None

        note that we ignore files with ".truncated". these are known to be bad.
        args:
            date in YYYYMMDD format, string identifying dump type eg "stub-articles",
            file type eg "xml", "sql", file ext e.g. "gz", "bz2",
            bool (if subjobs are enabled), ...
        '''
        if not date:
            date = self._wiki.date
        dfnames = self.get_files_in_dir(date)
        dfnames_matched = []
        for dfname in dfnames:
            if skip_suffixes:
                for suffix in skip_suffixes:
                    if dfname.filename.endswith(suffix):
                        continue

            if required_suffixes is not None:
                matches_required = False
                for suffix in required_suffixes:
                    if dfname.filename.endswith(suffix):
                        matches_required = True
                if not matches_required:
                    continue

            if dump_name and dfname.dumpname != dump_name:
                continue
            if file_type is not None and dfname.file_type != file_type:
                continue
            if file_ext is not None and dfname.file_ext != file_ext:
                continue
            if parts is False and dfname.is_file_part:
                continue
            if parts is True and not dfname.is_file_part:
                continue
            # parts is a list...
            if parts and parts is not True and dfname.partnum_int not in parts:
                continue
            if (temp is False and dfname.is_temp_file) or (temp and not dfname.is_temp_file):
                continue
            if (inprog is False and dfname.is_inprog) or (inprog is True and not dfname.is_inprog):
                continue
            if ((checkpoint is False and dfname.is_checkpoint_file) or
                    (checkpoint and not dfname.is_checkpoint_file)):
                continue
            dfnames_matched.append(dfname)
            dfnames_matched = self.sort_dumpfilenames(dfnames_matched)
        return dfnames_matched

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_dumpfilenames(self, mylist):
        """
        Sort the given list in the way that humans expect.
        args:
            list of DumpFilename
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key.filename)]
        mylist = sorted(mylist, key=alphanum_key)
        return mylist

    def get_checkpt_files(self, date=None, dump_name=None,
                          file_type=None, file_ext=None, parts=False, temp=False, inprog=False):
        '''
        list all checkpoint files that exist, filtering by the given args.
        if we get None for an arg then we accept all values for that arg in the filename
        if we get False for an arg (parts, temp),
        we reject any filename which contains a value for that arg
        if we get True for an arg (parts, temp),
        we accept only filenames which contain a value for the arg
        parts should be a list of value(s), or True / False / None
        '''
        return self._get_files_filtered(date, dump_name, file_type,
                                        file_ext, parts, temp, checkpoint=True,
                                        required_suffixes=None,
                                        skip_suffixes=self.BAD, inprog=inprog)

    def get_truncated_empty_checkpt_files(self, date=None, dump_name=None,
                                          file_type=None, file_ext=None,
                                          parts=False, temp=False):
        '''
        list all checkpoint files that exist, filtering by the given args.
        if we get None for an arg then we accept all values for that arg in the filename
        if we get False for an arg (parts, temp),
        we reject any filename which contains a value for that arg
        if we get True for an arg (parts, temp),
        we accept only filenames which contain a value for the arg
        parts should be a list of value(s), or True / False / None
        '''
        return self._get_files_filtered(date, dump_name, file_type,
                                        file_ext, parts, temp, checkpoint=True,
                                        required_suffixes=self.BAD,
                                        skip_suffixes=None)

    def get_reg_files(self, date=None, dump_name=None,
                      file_type=None, file_ext=None, parts=False, temp=False,
                      suffix=None, inprog=False):
        '''
        list all non-checkpoint files that exist, filtering by the given args.
        if we get None for an arg then we accept all values for that arg in the filename
        if we get False for an arg (parts, temp),
        we reject any filename which contains a value for that arg
        if we get True for an arg (parts, temp),
        we accept only filenames which contain a value for the arg
        parts should be a list of value(s), or True / False / None

        suffix, if passed, is a string that would be tacked on after the
        end of the filename (after .xml.gz)
        '''
        if suffix is not None:
            skip_suffixes = [entry for entry in self.BAD if entry != suffix]
            required_suffixes = suffix
        else:
            skip_suffixes = self.BAD
            required_suffixes = None
        return self._get_files_filtered(date, dump_name, file_type,
                                        file_ext, parts, temp, checkpoint=False,
                                        required_suffixes=required_suffixes,
                                        skip_suffixes=skip_suffixes, inprog=inprog)
