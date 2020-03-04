#!/usr/bin/python3
"""
list output files associated with a Dump job.
"""
from dumps.filelister import JobFileLister


class OutputFileLister(JobFileLister):
    '''
    list output files associated with dump jobs
    '''
    def __init__(self, dumpname, file_type, file_ext, fileparts_list,
                 checkpoint_file, checkpoints_enabled, list_dumpnames=None):
        super().__init__(dumpname, file_type, file_ext, fileparts_list,
                         checkpoint_file)
        self.checkpoints_enabled = checkpoints_enabled
        self.list_dumpnames = list_dumpnames

    def list_outfiles_to_publish(self, args):
        '''
        this is the complete list of files produced by a dump step.
        Includes: checkpoints, parts, complete files, temp files if they
        exist. At end of run temp files must be gone.
        even if only one file part (one subjob) is being rerun, this lists all output files,
        not just those for the one part.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''

        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(args))
        return dfnames

    def list_truncated_empty_outfiles(self, args):
        '''
        lists all files that have been found to be truncated or empty and renamed
        as such
        Includes: checkpoint files, file parts, whole files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        args = args._replace(parts=self.fileparts_list)
        if self.checkpoint_file is not None:
            problems = self.get_truncated_empty_reg_files_for_filepart(args)
            if self.checkpoint_file.filename in [problem.filename for problem in problems]:
                dfnames.append(self.checkpoint_file)
                return dfnames

        if self.checkpoints_enabled:
            dfnames.extend(self.list_truncated_empty_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.get_truncated_empty_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_build_command(self, args):
        '''
        called when the job command is generated.
        Includes: parts, whole files, temp files.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.get_reg_files_for_filepart_possible(args))
        return dfnames

    def list_outfiles_for_cleanup(self, args):
        '''
        called before job run to cleanup old files left around from any previous run(s)
        Includes: checkpoints, parts, whole files, temp files if they exist.
        This includes only the files that should be produced from this specific
        run, so if only one file part (subjob) is being redone, then only those files
        will be listed.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []

        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
            dfnames.extend(self.list_temp_files_for_filepart(args))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(args))
        return dfnames

    def list_inprog_files_for_cleanup(self, args):
        """
        list output files 'in progress' generated from a dump step,
        presumably left lying around from an earlier failed attempt
        at the step.

        expects:
            dump_dir, dump_names=None
        returns: list of DumpFilename
        """
        args = args._replace(dump_names=self.list_dumpnames())
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        if self.checkpoint_file is not None:
            dfnames.append(self.checkpoint_file)
            return dfnames

        args = args._replace(parts=self.fileparts_list)
        args = args._replace(inprog=True)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(args))
        return dfnames

    def list_outfiles_for_input(self, args):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.list_reg_files_for_filepart(args))
        return dfnames

    def list_truncated_empty_outfiles_for_input(self, args):
        '''
        used to generate list of files output from one dump step to be used as
        input for other dump step (e.g. recombine, recompress)
        returns only truncated or empty files
        Includes: checkpoints, partial files and/or whole files.
        Even if only file part is being rerun, this will return the list
        of all file parts.
        expects:
            dump_dir, dump_names=None
        returns:
            list of DumpFilename
        '''
        if args.dump_names is None:
            args = args._replace(dump_names=[self.dumpname])
        dfnames = []
        args = args._replace(parts=self.fileparts_list)
        if self.checkpoints_enabled:
            dfnames.extend(self.list_truncated_empty_checkpt_files_for_filepart(args))
        else:
            dfnames.extend(self.list_truncated_empty_reg_files_for_filepart(args))
        return dfnames
