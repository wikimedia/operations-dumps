#!/usr/bin/python3
from dumps.fileutils import DumpFilename


def _get_checkpt_files(dump_dir, dump_names, file_type, file_ext, date=None,
                       parts=None):
    '''
    return all checkpoint files that exist
    returns:
        list of DumpFilename
    '''
    dfnames = []
    for dump_name in dump_names:
        dfnames.extend(dump_dir.get_checkpt_files(
            date, dump_name, file_type, file_ext, parts, temp=False))
    return dfnames


def _get_reg_files(dump_dir, dump_names, file_type, file_ext, date=None, parts=None):
    '''
    get all regular output files that exist
    returns:
        list of DumpFilename
    '''
    dfnames = []
    for dump_name in dump_names:
        dfnames.extend(dump_dir.get_reg_files(
            date, dump_name, file_type, file_ext, parts, temp=False))
    return dfnames


class JobFileLister():
    '''
    list files associated with dump jobs
    '''
    def __init__(self, dumpname, file_type, file_ext, fileparts_list, checkpoint_file):
        self.dumpname = dumpname
        self.file_type = file_type
        self.file_ext = file_ext
        self.fileparts_list = fileparts_list
        self.checkpoint_file = checkpoint_file

    @staticmethod
    def makeargs(dump_dir, dump_names=None, parts=None, date=None, inprog=False,
                 partnum=None, flister=None):
        '''
        turn a list of params into an args dict for all list_... _files and related methods
        '''
        return {'dump_dir': dump_dir,
                'dump_names': dump_names,
                'date': date,
                'parts': parts,
                'inprog': inprog,
                'partnum': partnum,
                'flister': flister}

    @staticmethod
    def set_defaults(args, names):
        '''
        add default values to the args
        '''
        for name in names:
            if name not in args:
                if name == 'inprog':
                    # the one special case
                    args['name'] = False
                else:
                    args['name'] = None

    def list_reg_files(self, args):
        '''
        list all regular output files that exist
        expects:
            dump_dir, dump_names=None, date=None, parts=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names', 'date', 'parts'])
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        return _get_reg_files(args['dump_dir'], args['dump_names'], self.file_type,
                              self.file_ext, args['date'], args['parts'])

    def list_checkpt_files(self, args):
        '''
        list all checkpoint files that exist
        expects:
            dump_dir, dump_names=None, date=None, parts=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names', 'date', 'parts'])
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        return _get_checkpt_files(
            args['dump_dir'], args['dump_names'], self.file_type,
            self.file_ext, args['date'], args['parts'])

    def list_truncated_empty_checkpt_files_for_filepart(self, args):
        '''
        list checkpoint files that have been produced for specified file part(s)
        that are either empty or truncated

        expects:
            dump_dir, parts, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_truncated_empty_checkpt_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=False))
        return dfnames

    def list_checkpt_files_for_filepart(self, args):
        '''
        list checkpoint files that have been produced for specified file part(s)
        expects:
            dump_dir, parts, dump_names=None, inprog=False
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names', 'inprog'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_checkpt_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=False,
                inprog=args['inprog']))
        return dfnames

    def list_reg_files_for_filepart(self, args):
        '''
        list noncheckpoint files that have been produced for specified file part(s)
        expects:
            dump_dir, parts, dump_names=None, inprog=False
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names', 'inprog'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_reg_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=False,
                inprog=args['inprog']))
        return dfnames

    def list_truncated_empty_reg_files_for_filepart(self, args):
        '''
        list noncheckpoint files that have been produced for specified file part(s)
        expects:
            dump_dir, parts, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_truncated_empty_reg_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=False))
        return dfnames

    def list_temp_files_for_filepart(self, args):
        '''
        list temp output files that have been produced for specified file part(s)
        expects:
            dump_dir, parts, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_checkpt_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=True))
            dfnames.extend(args['dump_dir'].get_reg_files(
                None, dname, self.file_type, self.file_ext, args['parts'], temp=True))
        return dfnames

    def _get_files_possible(self, dump_dir, date=None, dumpname=None,
                            file_type=None, file_ext=None, parts=False, temp=False,
                            suffix=None):
        '''
        internal function which all the public get_*_possible functions call
        list all files that could be created for the given dumpname with field
        values from the given args.

        by definition, checkpoint files are never returned in such a list, as we don't
        know where a checkpoint might be taken (which pageId start/end).

        if date is omitted, the date in the dump_dir.wiki object will be used

        the dumpname field must be supplied or this will throw an exception

        if file_type or file_ext are omitted the resultant filename(s) will have none

        the parts arg may be False/None, in which case a filename without partnums are returned,
            or a list of numbers, to get a list of filenames with those partnums.

        the temp arg may be True, in which case filename(s) with temp extension are returned,
            or False/None, in which case regular filenames are returned

        if a suffix is supplied, this will be added on to the end of all filenames; e.g.
            truncated files might end in ".truncated"

        returns:
            list of DumpFilename
        '''

        dfnames = []
        if dumpname is None:
            dumpname = self.dumpname
        if suffix is not None:
            file_ext += suffix

        if parts is False:
            dfnames.append(DumpFilename(dump_dir.get_wiki(), date, dumpname,
                                        file_type, file_ext, None, None, temp))
        else:
            for partnum in parts:
                dfnames.append(DumpFilename(dump_dir.get_wiki(), date, dumpname,
                                            file_type, file_ext, partnum, None, temp))
        return dfnames

    def get_reg_files_for_filepart_possible(self, args):
        '''
        based on dump name, parts, etc. get all the
        output files we expect to generate for these parts
        expects:
            dump_dir, parts, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names'])
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        dfnames = []
        for dname in args['dump_names']:
            dfnames.extend(self._get_files_possible(
                args['dump_dir'], None, dname, self.file_type, self.file_ext,
                args['parts'], temp=False))
        return dfnames

    def get_truncated_empty_reg_files_for_filepart(self, args):
        '''
        based on dump name, parts, etc. get all the
        output files we expect to generate for these parts
        expects:
            dump_dir, parts, dump_names=None
        returns:
            list of DumpFilename
        '''
        self.set_defaults(args, ['dump_names'])
        dfnames = []
        if not args['dump_names']:
            args['dump_names'] = [self.dumpname]
        for dname in args['dump_names']:
            dfnames.extend(args['dump_dir'].get_reg_files(
                None, dname, self.file_type, self.file_ext, args['parts'],
                temp=False, suffix=".truncated"))
            dfnames.extend(args['dump_dir'].get_reg_files(
                None, dname, self.file_type, self.file_ext, args['parts'],
                temp=False, suffix=".empty"))
        return dfnames
