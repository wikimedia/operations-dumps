#!/usr/bin/python3
"""
classes and methods for writing out file
with information about the current dump run in
json format, for downloaders' use
"""


import os
import json


class SpecialFilesRegistry(type):
    """
    use this as part of a class autoregistry setup
    """
    SPECIALFILES_REGISTRY = {}

    def __new__(cls, clsname, base_classes, attrs):
        newclass = super().__new__(cls, clsname, base_classes, attrs)
        if clsname not in ['SpecialFilesRegistry', 'Registered']:
            SpecialFilesRegistry.SPECIALFILES_REGISTRY[clsname] = newclass
        return newclass


class Registered(metaclass=SpecialFilesRegistry):
    """
    base class for all dump classes that write special files that don't
    contain dump job output.
    """

    @staticmethod
    def list_special_files(classname, wiki):
        """
        for a given registered class, pass in a configured Wiki object,
        get back a list of the special output files it writes.
        """
        filewriter = classname(wiki, True)
        return filewriter.get_all_output_files()


class SpecialFileWriter(Registered):
    """
    base class for classes writing special files such as status files, in
    various formats, primarily json
    """

    # override these in subclass
    FILENAME = None
    known_formats = []

    @staticmethod
    def load_json_file(wiki, fmt, classname):
        """
        read and return the contents of a json special file
        for the wiki
        """
        if fmt not in getattr(classname, 'known_formats'):
            return {}
        date = wiki.latest_dump()
        if date:
            filepath = os.path.join(wiki.public_dir(), date,
                                    getattr(classname, 'FILENAME') + "." + fmt)
            with open(filepath, "r") as infile:
                contents = infile.read()
                infile.close()
            return json.loads(contents)
        return {}

    def __init__(self, wiki, fileformat="json", error_callback=None, verbose=False):
        super().__init__()
        self.wiki = wiki
        self.fileformat = fileformat
        self.filepath = self.get_output_filepath()
        if not self.filepath:
            # here we should log something. FIXME
            pass
        self.error_callback = error_callback
        self.verbose = verbose

    def get_json_file_contents(self, path):
        """
        retrieve contents from a json file and return them
        """
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

    def get_output_filepath(self):
        """
        return the full path to the output file to be created
        """
        if not self.check_format():
            return None
        return os.path.join(self.wiki.public_dir(),
                            self.wiki.date, self.FILENAME +
                            "." + self.fileformat)

    def check_format(self):
        """
        return True, if the format passed in at instance creation is in the list
        of recognized formats, False otherwise
        """
        # return bool(self.fileformat in getattr(self.get_classname(), 'known_formats'))
        return bool(self.fileformat in self.known_formats)

    def write_contents_json(self, contents):
        """
        convert contents to json and write the results to output file
        """
        if not self.filepath:
            return
        with open(self.filepath, "w+") as fhandle:
            fhandle.write(json.dumps(contents) + "\n")

    def write_contents(self, contents):
        """
        write contents in appropriate format to output file
        """
        if not self.check_format():
            return None
        if self.fileformat == "json":
            return self.write_contents_json(contents)
        return None

    def get_all_output_files(self):
        """
        return list of known status file names in all formats
        """
        files = []
        for fmt in self.known_formats:
            files.append(self.FILENAME + "." + fmt)
        return files
