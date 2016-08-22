# Worker process, does the actual dumping

import hashlib
import os
from os.path import exists
import re
import sys
import time
import signal
import traceback
import tempfile
import shutil

from dumps.utils import MiscUtils
from dumps.CommandManagement import CommandPipeline
from dumps.exceptions import BackupError


class FileUtils(object):

    @staticmethod
    def file_age(filename):
        return time.time() - os.stat(filename).st_mtime

    @staticmethod
    def atomic_create(filename, mode='w'):
        """Create a file, aborting if it already exists..."""
        fhandle = os.open(filename, os.O_EXCL + os.O_CREAT + os.O_WRONLY)
        return os.fdopen(fhandle, mode)

    @staticmethod
    def write_file(dirname, filename, text, perms=0):
        """Write text to a file, as atomically as possible,
        via a temporary file in a specified directory.
        Arguments: dirname = where temp file is created,
        filename = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation"""

        if not os.path.isdir(dirname):
            try:
                os.makedirs(dirname)
            except:
                raise IOError("The given directory '%s' is neither "
                              "a directory nor can it be created" % dirname)

        (fhandle, temp_filename) = tempfile.mkstemp("_txt", "wikidump_", dirname)
        os.write(fhandle, text)
        os.close(fhandle)
        if perms:
            os.chmod(temp_filename, perms)
        # This may fail across filesystems or on Windows.
        # Of course nothing else will work on Windows. ;)
        shutil.move(temp_filename, filename)

    @staticmethod
    def write_file_in_place(filename, text, perms=0):
        """Write text to a file, after opening it for write with truncation.
        This assumes that only one process or thread accesses the given file at a time.
        Arguments: filename = full path to actual file, text = contents
        to write to file, perms = permissions that the file will have after creation,
        if it did not exist already"""

        filehdl = open(filename, "wt")
        filehdl.write(text)
        filehdl.close()
        if perms:
            os.chmod(filename, perms)

    @staticmethod
    def read_file(filename):
        """Read text from a file in one fell swoop."""
        filehdl = open(filename, "r")
        text = filehdl.read()
        filehdl.close()
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
        for prefix_unused in base:
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
        else:
            return FileUtils._pretty_size(size / 1024.0, quanta[1:])

    @staticmethod
    def file_info(path):
        """Return a tuple of date/time and size of a file, or None, None"""
        try:
            timestamp = time.gmtime(os.stat(path).st_mtime)
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
            size = os.path.getsize(path)
            return (timestamp, size)
        except:
            return(None, None)


class DumpFilename(object):
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
    first_page_id       for checkpoint files, taken from value in filename
    last_page_id      for checkpoint files, value taken from filename
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

    @staticmethod
    def make_checkpoint_string(first_page_id, last_page_id):
        if first_page_id is not None and last_page_id is not None:
            return "p" + first_page_id + "p" + last_page_id
        else:
            return None

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

    def is_ext(self, ext):
        return bool(ext == "gz" or ext == "bz2" or ext == "7z" or ext == "html" or ext == "txt")

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

        self.is_temp_file = False
        self.temp = None

        # example filenames:
        # elwikidb-20110729-all-titles-in-ns0.gz
        # elwikidb-20110729-abstract.xml
        # elwikidb-20110727-pages-meta-history2.xml-p000048534p000051561.bz2

        # we need to handle cases without the projectname-date stuff in them too, as this gets used
        # for all files now
        if self.filename.endswith("-tmp"):
            self.is_temp_file = True
            self.temp = "-tmp"

        if '.' in self.filename:
            (file_base, self.file_ext) = self.filename.rsplit('.', 1)
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


class DumpFile(object):
    """File containing output created by any job of a jump run.  This includes
    any file that follows the standard naming convention, i.e.
    projectname-date-dumpname.sql/xml.gz/bz2/7z (possibly with a file part
    number, possibly with start/end page id information embedded in the name).

    Methods:

    md5sum(): return md5sum of the file contents.
    sha1sum(): return sha1sum of the file contents.
    checksum(htype): return checksum of the specified type, of the file contents.
    check_if_truncated(): for compressed files, check if the file is truncated (stops
       abruptly before the end of the compressed data) or not, and set and return
         self.is_truncated accordingly.  This is fast for bzip2 files
       and slow for gz and 7z fles, since for the latter two types it must serially
       read through the file to determine if it is truncated or not.
    get_size(): returns the current size of the file in bytes
    rename(newname): rename the file. Arguments: the new name of the file without
       the directory.
    find_first_page_id_in_file(): set self.first_page_id by examining the file contents,
       returning the value, or None if there is no pageID.  We uncompress the file
       if needed and look through the first 500 lines.

#    plus the usual file methods (read, write, open, close)

    useful variables:

    first_page_id       Determined by examining the first few hundred lines of the contents,
                          looking for page and id tags, wihout other tags in between. (hmm)
    filename          full filename with directory
    """
    def __init__(self, wiki, filename, file_obj=None, verbose=False):
        """takes full filename including path"""
        self._wiki = wiki
        self.filename = filename
        self.first_lines = None
        self.is_truncated = None
        self.is_empty = None
        self.first_page_id = None
        self.dirname = os.path.dirname(filename)
        if file_obj:
            self.file_obj = file_obj
        else:
            self.file_obj = DumpFilename(wiki)
            self.file_obj.new_from_filename(os.path.basename(filename))
        if verbose:
            sys.stderr.write("setting up info for %s\n" % filename)

#        super(DumpFile,self).__init__(ops, args, s3Sess)

    def _checksum(self, summer):
        if not self.filename:
            return None
        infile = file(self.filename, "rb")
        bufsize = 4192 * 32
        fbuffer = infile.read(bufsize)
        while fbuffer:
            summer.update(fbuffer)
            fbuffer = infile.read(bufsize)
        infile.close()
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
        elif htype == "sha1":
            return self.sha1sum()
        else:
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
        head_esc = MiscUtils.shell_escape(head)
        pipeline.append([head, "-500"])
        # without shell
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        if (proc.exited_successfully() or
                (proc.get_failed_cmds_with_retcode() ==
                 [[-signal.SIGPIPE, pipeline[0]]]) or
                (proc.get_failed_cmds_with_retcode() ==
                 [[signal.SIGPIPE + 128, pipeline[0]]])):
            self.first_lines = proc.output()
        return self.first_lines

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
        if self.file_obj.file_ext == 'bz2':
            command = [self._wiki.config.bzip2, '-dc']
        elif self.file_obj.file_ext == 'gz':
            command = [self._wiki.config.gzip, '-dc']
        elif self.file_obj.file_ext == '7z':
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
        if self.first_page_id:
            return self.first_page_id
        output = self.get_first_500_lines()
        if output:
            page_data = output
            title_and_id_pattern = re.compile(r'<title>(?P<title>.+?)</title>\s*' +
                                              r'(<ns>[0-9]+</ns>\s*)?' +
                                              r'<id>(?P<pageid>\d+?)</id>')
            result = title_and_id_pattern.search(page_data)
            if result:
                self.first_page_id = result.group('pageid')
        return self.first_page_id

    def check_if_truncated(self):
        if self.is_truncated:
            return self.is_truncated

        # Setting up the pipeline depending on the file extension
        if self.file_obj.file_ext == "bz2":
            if not exists(self._wiki.config.checkforbz2footer):
                raise BackupError("checkforbz2footer command %s not found" %
                                  self._wiki.config.checkforbz2footer)
            checkforbz2footer = self._wiki.config.checkforbz2footer
            pipeline = []
            pipeline.append([checkforbz2footer, self.filename])
        else:
            if self.file_obj.file_ext == 'gz':
                pipeline = [[self._wiki.config.gzip, "-dc", self.filename,
                             ">", "/dev/null"]]
            elif self.file_obj.file_ext == '7z':
                # Note that 7z does return 0, if archive contains
                # garbage /after/ the archive end
                pipeline = [[self._wiki.config.sevenzip, "e", "-so",
                             self.filename, ">", "/dev/null"]]
            else:
                # we do't know how to handle this type of file.
                return self.is_truncated

        # Run the perpared pipeline
        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        self.is_truncated = not proc.exited_successfully()

        return self.is_truncated

    def check_if_empty(self):
        if self.is_empty:
            return self.is_empty
        if self.file_obj.file_ext == "bz2":
            pipeline = [[self._wiki.config.bzip2, "-dc", self.filename, "|",
                         "head", "-5"]]
        elif self.file_obj.file_ext == "bz2":
            pipeline = [[self._wiki.config.gzip, "-dc", self.filename, "|",
                         "head", "-5"]]
        elif self.file_obj.file_ext == '7z':
            pipeline = [[self._wiki.config.sevenzip, "e", "-so", self.filename, "|",
                         "head", "-5"]]
        else:
            # we do't know how to handle this type of file.
            return self.is_empty

        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()
        self.is_empty = bool(not len(proc.output()))

        return self.is_empty

    def get_size(self):
        if exists(self.filename):
            return os.path.getsize(self.filename)
        else:
            return None

    def rename(self, newname):
        try:
            os.rename(self.filename, os.path.join(self.dirname, newname))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise BackupError("failed to rename file %s" % self.filename)

        self.filename = os.path.join(self.dirname, newname)


class DumpDir(object):
    def __init__(self, wiki, db_name):
        self._wiki = wiki
        self._db_name = db_name
        self._dir_cache = {}
        self._dir_cache_time = {}
        self._filepart_cache = {}
        self._checkpoint_file_cache = {}

    def filename_private_path(self, dump_file, date_string=None):
        """Given a DumpFilename object, produce the full path to the filename in the date subdir
        of the the private dump dir for the selected database.
        If a different date is specified, use that instead"""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.private_dir(), date_string, dump_file.filename)

    def filename_public_path(self, dump_file, date_string=None):
        """Given a DumpFilename object produce the full path to the filename in the date subdir
        of the public dump dir for the selected database.
        If this database is marked as private, use the private dir instead.
        If a different date is specified, use that instead"""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.public_dir(), date_string, dump_file.filename)

    def latest_dir(self):
        """Return 'latest' directory for the current project being dumped, e.g.
        if the current project is enwiki, this would return something like
        /mnt/data/xmldatadumps/public/enwiki/latest (if the directory /mnt/data/xmldatadumps/public
        is the path to the directory for public dumps)."""
        return os.path.join(self._wiki.public_dir(), "latest")

    def web_path(self, dump_file, date_string=None):
        """Given a DumpFilename object produce the full url to the filename for the date of
        the dump for the selected database."""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.web_dir(), date_string, dump_file.filename)

    def web_path_relative(self, dump_file, date_string=None):
        """Given a DumpFilename object produce the url relative
        to the docroot for the filename for the date of
        the dump for the selected database."""
        if not date_string:
            date_string = self._wiki.date
        return os.path.join(self._wiki.web_dir_relative(), date_string, dump_file.filename)

    def dir_cache_outdated(self, date):
        if not date:
            date = self._wiki.date
        directory = os.path.join(self._wiki.public_dir(), date)
        if exists(directory):
            dir_time_stamp = os.stat(directory).st_mtime
            return bool(date not in self._dir_cache or dir_time_stamp > self._dir_cache_time[date])
        else:
            return True

    # warning: date can also be "latest"
    def get_files_in_dir(self, date=None):
        if not date:
            date = self._wiki.date
        if self.dir_cache_outdated(date):
            directory = os.path.join(self._wiki.public_dir(), date)
            if exists(directory):
                dir_time_stamp = os.stat(directory).st_mtime
                files = os.listdir(directory)
                file_objs = []
                for filename in files:
                    file_obj = DumpFilename(self._wiki)
                    file_obj.new_from_filename(filename)
                    file_objs.append(file_obj)
                self._dir_cache[date] = file_objs
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
                            file_ext=None, parts=None, temp=None, checkpoint=None):
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
        '''
        if not date:
            date = self._wiki.date
        file_objs = self.get_files_in_dir(date)
        files_matched = []
        for fobj in file_objs:
            # fixme this is a bit hackish
            if fobj.filename.endswith("truncated"):
                continue

            if dump_name and fobj.dumpname != dump_name:
                continue
            if file_type is not None and fobj.file_type != file_type:
                continue
            if file_ext is not None and fobj.file_ext != file_ext:
                continue
            if parts is False and fobj.is_file_part:
                continue
            if parts is True and not fobj.is_file_part:
                continue
            # parts is a list...
            if parts and parts is not True and fobj.partnum_int not in parts:
                continue
            if (temp is False and fobj.is_temp_file) or (temp and not fobj.is_temp_file):
                continue
            if ((checkpoint is False and fobj.is_checkpoint_file) or
                    (checkpoint and not fobj.is_checkpoint_file)):
                continue
            files_matched.append(fobj)
            self.sort_fileobjs(files_matched)
        return files_matched

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_fileobjs(self, mylist):
        """ Sort the given list in the way that humans expect.
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key.filename)]
        mylist.sort(key=alphanum_key)

    def get_checkpt_files(self, date=None, dump_name=None,
                          file_type=None, file_ext=None, parts=False, temp=False):
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
                                        file_ext, parts, temp, checkpoint=True)

    def get_reg_files(self, date=None, dump_name=None,
                      file_type=None, file_ext=None, parts=False, temp=False):
        '''
        list all non-checkpoint files that exist, filtering by the given args.
        if we get None for an arg then we accept all values for that arg in the filename
        if we get False for an arg (parts, temp),
        we reject any filename which contains a value for that arg
        if we get True for an arg (parts, temp),
        we accept only filenames which contain a value for the arg
        parts should be a list of value(s), or True / False / None
        '''
        return self._get_files_filtered(date, dump_name, file_type,
                                        file_ext, parts, temp, checkpoint=False)