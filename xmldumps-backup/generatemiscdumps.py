#!/usr/bin/python3
'''
for every wiki, run the specific dump type for today
or the given date
'''
import getopt
import os
from os.path import exists
import sys
import time
import logging

from miscdumplib import STATUS_TODO, STATUS_GOOD, STATUS_FAILED
from miscdumplib import StatusFile, IndexFile
from miscdumplib import md5sums, MD5File
from miscdumplib import MiscDumpDirs, MiscDumpDir
from miscdumplib import MiscDumpLock, StatusInfo
from miscdumplib import setup_logging, safe, make_link, skip_wiki
from miscdumpfactory import MiscDumpFactory

from dumps.wikidump import Wiki
from dumps.exceptions import BackupError
from dumps.wikidump import FileUtils, TimeUtils


# pylint: disable=broad-except


class Index():
    '''
    generate index.html page containing information for the dump
    run of the specified date for all wikis
    '''
    def __init__(self, dryrun, args, log):
        '''
        pass a dict of the standard args
        (config, date, dumptype, args)
        '''
        self.args = args
        self.dryrun = dryrun
        self.log = log
        self.indexfile = IndexFile(self.args['config'])
        self.dumpdir = MiscDumpDir(self.args['config'])

    def do_all_wikis(self):
        '''
        generate index.html file for all wikis for the given date.
        This should only be done if the run is the most recent, otherwise
        your nice fresh index.html file willbe replaced with old data.
        '''
        text = ""
        for wikiname in self.args['config'].all_wikis_list:
            result = self.do_one_wiki(wikiname)
            if result:
                self.log.info("result for wiki %s is %s", wikiname, result)
                text = text + "<li>" + result + "</li>\n"
        index_text = (self.args['config'].read_template(self.args['config'].indextmpl)
                      % {"items": text})
        if self.dryrun:
            print("would write {path} with index text".format(path=self.indexfile.get_path()))
        else:
            FileUtils.write_file_in_place(self.indexfile.get_path(),
                                          index_text, self.args['config'].fileperms)

    def get_outputfile_indextxt(self, filenames_tocheck, expected, wikiname, dump_date):
        '''
        generate and return a list of text strings that provide a
        link to the given files, along with filename, size and date.
        if the file does not exist, it will be silently excluded from
        the list.
        the expected list is a list of filenames that are expected to
        be produced by the dump; currently no errors are generated
        on this basis but this may change in the future.
        '''
        dirinfo = MiscDumpDir(self.args['config'], dump_date)
        path = dirinfo.get_dumpdir(wikiname)
        output_fileinfo = {}
        for filename in filenames_tocheck:
            output_fileinfo[filename] = FileUtils.file_info(os.path.join(path, filename))
        files_text = []
        filenames = sorted(output_fileinfo.keys())
        for filename in filenames:
            file_date, file_size = output_fileinfo[filename]
            self.log.info("output file %s for %s %s %s",
                          filename, wikiname, safe(file_date), safe(file_size))
            if filename in expected and file_date is None:
                # may do more with this sort of error in the future
                # for now, just get stats on the other files
                continue
            if file_date:
                files_text.append(
                    "%s: %s (size %s)<br />"
                    % (make_link(
                        os.path.join(
                            wikiname, dump_date,
                            os.path.basename(filename)),
                        os.path.basename(filename)), file_date, file_size))
        return files_text

    def get_stat_text(self, dump_date, wikiname):
        '''
        generate and return the text string describing
        the status of the dump of the wiki for the given date
        '''
        stat = StatusFile(self.args['config'], dump_date, wikiname)
        stat_contents = FileUtils.read_file(stat.get_path())
        self.log.info("status for %s %s", wikiname, safe(stat_contents))
        if stat_contents:
            stat_text = "(%s)" % (stat_contents)
        else:
            stat_text = None
        return stat_text

    def get_files_text(self, wiki):
        '''
        given wiki object, return the list of links and descriptions
        for the output files for that wiki of the current dump type
        and date
        '''
        dump_class = MiscDumpFactory.get_dumper(self.args['dumptype'])
        dumper = dump_class(wiki, self.log, False, self.args['args'])
        output_files, expected = dumper.get_output_files()
        files_text = self.get_outputfile_indextxt(output_files, expected,
                                                  wiki.db_name, wiki.date)

        md5file = MD5File(wiki.config, wiki.date, wiki.db_name)
        md5file_text = self.get_outputfile_indextxt(
            [md5file.get_filename()], [], wiki.db_name, wiki.date)
        files_text.extend(md5file_text)
        return files_text

    def do_one_wiki(self, wikiname, date=None):
        '''
        collect the text strings for one wiki to be inserted into
        the index.html file
        '''
        if not skip_wiki(wikiname, self.args['config']):
            dumps_dirs = MiscDumpDirs(self.args['config'], wikiname, self.log)
            if not exists(self.dumpdir.get_dumpdir_no_date(wikiname)):
                self.log.info("No dump for wiki %s", wikiname)
                return None
            if date is not None:
                dump_date = date
            else:
                dump_date = dumps_dirs.get_latest_dump_date(True)
            if not dump_date:
                self.log.info("No dump for wiki %s", wikiname)
                return None

            other_runs_text = "other runs: %s<br />" % make_link(wikiname, wikiname)

            try:
                wiki = Wiki(self.args['config'], wikiname)
                wiki.set_date(dump_date)
                files_text = self.get_files_text(wiki)
                stat_text = self.get_stat_text(dump_date, wikiname)

            except Exception as ex:
                self.log.warning("Error encountered, no information available"
                                 " for wiki %s", wikiname, exc_info=ex)
                return ("<strong>%s</strong> Error encountered,"
                        " no information available | %s" % (wikiname, other_runs_text))

            try:
                wikiname_text = "<strong>%s</strong>" % wikiname

                wiki_info = (" ".join([entry for entry in [wikiname_text, stat_text]
                                       if entry is not None]) + "<br />")
                wiki_info = (wiki_info + "&nbsp;&nbsp;" + "\n&nbsp;&nbsp;".join(files_text))
                wiki_info = wiki_info + "\n&nbsp;" + other_runs_text
            except Exception as ex:
                self.log.warning("Error encountered formatting information"
                                 " for wiki %s", wikiname, exc_info=ex)
                return ("Error encountered formatting information"
                        " for wiki %s" % wikiname)

            return wiki_info
        return None


class MiscDumpOne():
    '''
    run dump of specified name on all wikis, or if do_dump
    is False, only generate the index.html file containing
    information on the dump run, for all wikis.

    args are keyword args converted to a dict, these get passed
    through to the class for the specific dump you want
    '''
    def __init__(self, args, wikiname, flags, log):
        self.args = args
        self.wiki = Wiki(self.args['config'], wikiname)
        self.wiki.set_date(self.args['date'])
        self.flags = flags
        self.log = log
        dump_class = MiscDumpFactory.get_dumper(self.args['dumptype'])
        self.dumper = dump_class(self.wiki, self.log, flags['dryrun'],
                                 self.args['args'])

    def do_one_wiki(self):
        '''
        run dump of specified type for one wiki, for given date
        unless it is among the wikis we skip, has already been run
        for the date, or some other process has the lock and is
        therefore presumably already dumping it
        '''
        if not self.flags['do_dump']:
            return STATUS_GOOD

        if not skip_wiki(self.wiki.db_name, self.wiki.config):
            dumpdir = MiscDumpDir(self.args['config'], self.args['date'])
            if not exists(dumpdir.get_dumpdir(self.wiki.db_name)) and not self.flags['dryrun']:
                os.makedirs(dumpdir.get_dumpdir(self.wiki.db_name))

            status_info = StatusInfo(self.args['config'], self.wiki.date, self.wiki.db_name)
            status = status_info.get_status()
            if status == "done:all" and not self.flags['forcerun']:
                self.log.info("wiki %s skipped, %s dump already"
                              " complete", self.wiki.db_name, self.args['dumptype'])
                return STATUS_GOOD

            if not self.flags['dryrun']:

                if not self.flags['skiplocks']:
                    lock = MiscDumpLock(self.args['config'], self.wiki.date,
                                        self.wiki.db_name, self.log)

                    # if lock is stale, remove it
                    lock.remove_if_stale(self.wiki.config.lock_stale)

                    # try to get the lock ourselves
                    if not lock.get_lock():
                        self.log.info("wiki %s skipped, wiki is locked,"
                                      " another process should be doing the job",
                                      self.wiki.db_name)
                        return STATUS_TODO

                    self.dumper.set_lockinfo(lock)

                dumps_dirs = MiscDumpDirs(self.wiki.config, self.wiki.db_name, self.log)
                dumps_dirs.cleanup_old_dumps(self.wiki.date)

            self.log.info("Doing run for wiki: %s", self.wiki.db_name)

            try:
                result = self.dumper.run()
                if not result:
                    return STATUS_FAILED

                if not self.flags['dryrun']:
                    output_files, expected = self.dumper.get_output_files()
                    if not md5sums(self.wiki, self.wiki.config.fileperms,
                                   output_files, expected, self.log):
                        return STATUS_FAILED
                    status_info.set_status("done:" + self.dumper.get_steps_done())

                    if not self.flags['skiplocks']:
                        lock.unlock_if_owner()

            except Exception as ex:
                self.log.warning("error from dump run"
                                 " for wiki %s", self.wiki.db_name, exc_info=ex)
                if not self.flags['dryrun']:
                    if not self.flags['skiplocks']:
                        lock.unlock_if_owner()
                return STATUS_FAILED
        self.log.info("Success!  Wiki %s %s dump complete.",
                      self.wiki.db_name, self.args['dumptype'])
        return STATUS_GOOD


class MiscDumpLoop():
    '''
    do the specified dumptype for all wikis for the given date, including
    regeneration of the index.html file, with various dump phases optionally
    skipped according to the supplied args
    '''
    def __init__(self, args, flags, log):
        self.args = args
        self.flags = flags
        self.log = log

    def is_most_recent_run(self):
        '''
        return True if there are no runs on any wikis more recent
        than the current run date, otherwise False
        '''
        for wikiname in self.args['config'].all_wikis_list:
            if not skip_wiki(wikiname, self.args['config']):
                dumps_dirs = MiscDumpDirs(self.args['config'], wikiname, self.log)
                dirs = dumps_dirs.get_misc_dumpdirs()
                if not dirs or dirs[-1] > self.args['date']:
                    return False
        return True

    def do_run_on_all_wikis(self):
        '''
        run dump of given type on all wikis for given date; some
        dump phases may be skipped depending on the supplied args.

        no retries are performed.  wikis currently locked (i.e.
        being handled by some other process) are skipped.

        the number of failures and pending wikis are returned.
        '''
        failures = []
        todos = 0
        for wikiname in self.args['config'].all_wikis_list:
            try:
                dump = MiscDumpOne(self.args, wikiname, self.flags, self.log)
                result = dump.do_one_wiki()
            except Exception as ex:
                self.log.warning("error from dump run"
                                 " for wiki %s", wikiname, exc_info=ex)
                result = STATUS_FAILED
            if result == STATUS_FAILED:
                failures.append(wikiname)
            elif result == STATUS_TODO:
                todos = todos + 1
        if self.flags['do_index'] and self.is_most_recent_run():
            index = Index(self.flags['dryrun'], self.args, self.log)
            index.do_all_wikis()
        return (failures, todos)

    def do_all_wikis_til_done(self, num_fails):
        '''
        run dump of given type on all wikis for given date; some
        dump phases may be skipped depending on the supplied args.

        skipped wikis will be retried until they are available
        for processing

        failed wikis will be retried until they succeed or
        until there are too many failures
        '''
        fails = 0
        while 1:
            (failures, todos) = self.do_run_on_all_wikis()
            if not failures and not todos:
                break
            fails = fails + 1
            if fails > num_fails:
                raise BackupError("Too many consecutive failures, " +
                                  "giving up: last failures on " +
                                  ",".join(failures))
            time.sleep(300)


def usage(message=None):
    '''
    display a usage message for this script and
    the arguments for each known dump type.

    an optional message may be passed that will be
    displayed first.
    '''
    if message:
        print(message)
    usage_message = (
        """Usage: python3 generatemiscdumps.py --dumptype <type> [options] [args]

Options: --configfile, --dumptype, --wiki, --date,
         --dumponly, --indexonly, --forcerun,
         --logfile, --skiplocks, --verbose, --quiet, --dryrun, --wiki

 --configfile:  Specify an alternate config file to read. Default
                file is 'miscdump.conf' in the current directory.
 --dumptype:    type of dump to be run.  Known types include:
                {0}
 --wiki:        Run the dumps only for the specific wiki.
 --date:        (Re)run dump of a given date (use with care).

 --dumponly:    Do only the dump without rebuilding the index.html file.
 --indexonly:   Generate the index.html file only, don't run the dump.
 --forcerun:    Do the run even if there is already a successful run in place.

 --logfile:     Name of file to which error messages and progress messages
                are to be logged. Default: log to stderr
 --skiplocks:   Don't do any file locking (use only if one process is running
                at a time)
                Default: false
 --verbose:     Print error messages and other informative messages.
                Default: print errors and warnings
 --quiet:       Print only serious error messages
                Default: print errors and warnings
 --dryrun:      Don't dump anything but print the commands that would be run.
 --help:        Display this message.

Args:  If your dump needs specific arguments passed to the class that
       are not provided for here, you can pass them on the command line
       before the final wikidbname argument.  Arguments with values should
       be passed as argname:value, and arguments without values (flags that
       will be set as True) should be passed simply as argname.
""").format(", ".join(MiscDumpFactory.get_known_dumptypes()))
    sys.stderr.write(usage_message)
    secondary_message = MiscDumpFactory.get_secondary_usage_all()
    sys.stderr.write("\n" + secondary_message)
    sys.exit(1)


def get_standard_args(options):
    '''
    get and return the args that get passed around
    to the basic classes
    '''
    args = {'config': None, 'date': None, 'dumptype': None,
            'args': None, 'logfile': None}
    for (opt, val) in options:
        if opt == "--date":
            args['date'] = val
        elif opt == "--dumptype":
            args['dumptype'] = val
        elif opt == "--logfile":
            args['logfile'] = val
    return args


def get_secondary_args(remainder):
    '''
    if there are args left over from the command line,
    turn them into name/value pairs or name/True flags
    '''
    args = {}
    if remainder:
        for opt in remainder:
            if ':' in opt:
                name, value = opt.split(':', 1)
                args[name] = value
            else:
                args[opt] = True
    return args


def get_config(config_file, dumptype):
    '''
    return the config for the given dumptype
    '''
    configurator = MiscDumpFactory.get_configurator(dumptype)
    if config_file:
        config = configurator(config_file)
    else:
        config = configurator()
    return config


def check_usage(flags, standard_args):
    '''
    check validity of specified args
    '''
    if not flags['do_dump'] and not flags['do_index']:
        usage("You may not specify more than one of dumpsonly "
              "and indexonly together.")

    if standard_args['dumptype'] is None:
        usage("Mandatory dumptype argument not specified")
    elif standard_args['dumptype'] not in MiscDumpFactory.get_known_dumptypes():
        usage("No such known dump " + standard_args['dumptype'])


def get_flags(options):
    '''
    get and return flags from command line options
    '''
    flags = {'do_dump': True, 'do_index': True,
             'dryrun': False, 'forcerun': False,
             'skiplocks': True}

    for (opt, _) in options:
        if opt == "--dumpsonly":
            flags['do_index'] = False
        elif opt == "--indexonly":
            flags['do_dump'] = False
        elif opt == "--dryrun":
            flags['dryrun'] = True
        elif opt == "--forcerun":
            flags['forcerun'] = True
        elif opt == "--skiplocks":
            flags['skiplocks'] = True
    return flags


def get_logger(logfile, loglevel):
    '''
    given a logfile name (may be None) and log level indicating
    verbosity, get and return the logger we want
    '''
    if loglevel == 'verbose':
        log_type = 'verbose'
        console_level = 'INFO'
    elif loglevel == 'quiet':
        log_type = 'normal'
        console_level = 'ERROR'
    else:
        log_type = 'normal'
        console_level = 'WARNING'

    setup_logging(logfile, console_level)
    if logfile:
        log_type += '_file'

    return logging.getLogger(log_type)    # pylint: disable=invalid-name


def main():
    '''
    entry point:
    get and process args, verify args,
    run specified dumptype for one or all wikis in config file
    for today or specified date
    '''
    config_file = False
    wikiname = None
    loglevel = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ['date=', 'dumptype=', 'configfile=', 'wiki=', 'dumpsonly',
             'indexonly', 'logfile=', 'dryrun', 'skiplocks', 'verbose', 'quiet', 'forcerun'])
    except Exception:
        usage("Unknown option specified")

    # these args get passed to all the dump classes
    standard_args = get_standard_args(options)
    # these flags get passed around to everything
    flags = get_flags(options)

    # the rest of the options
    for (opt, val) in options:
        if opt == "--configfile":
            config_file = val
        elif opt == "--wiki":
            wikiname = val
        elif opt == "--verbose":
            if loglevel:
                usage("Only one of --quiet or --verbose may be specified")
            loglevel = 'verbose'
        elif opt == "--quiet":
            if loglevel:
                usage("Only one of --quiet or --verbose may be specified")
            loglevel = 'quiet'

    log = get_logger(standard_args['logfile'], loglevel)

    check_usage(flags, standard_args)

    standard_args['config'] = get_config(config_file, standard_args['dumptype'])
    if not standard_args['date']:
        standard_args['date'] = TimeUtils.today()

    standard_args['args'] = get_secondary_args(remainder)

    if wikiname is not None:
        dump_one = MiscDumpOne(standard_args, wikiname, flags, log)
        dump_one.do_one_wiki()
        # we won't dump them all but we want to rebuild the index as if
        # we did
        dump_all = MiscDumpLoop(standard_args, flags, log)
        if flags['do_index'] and dump_all.is_most_recent_run():
            index = Index(flags['dryrun'], dump_all.args, log)
            index.do_all_wikis()
    else:
        dump_all = MiscDumpLoop(standard_args, flags, log)
        dump_all.do_all_wikis_til_done(3)


if __name__ == "__main__":
    main()
