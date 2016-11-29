# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

import getopt
import os
from os.path import exists
import sys
import time
import traceback
from miscdumplib import STATUS_TODO, STATUS_GOOD, STATUS_FAILED
from miscdumplib import StatusFile, IndexFile
from miscdumplib import md5sums, MD5File
from miscdumplib import MiscDumpDirs, MiscDumpDir
from miscdumplib import MiscDumpLock, StatusInfo
from miscdumplib import log, safe, make_link
from miscdumpfactory import MiscDumpFactory

from dumps.WikiDump import Wiki
from dumps.exceptions import BackupError
from dumps.WikiDump import FileUtils, TimeUtils


class Index(object):
    '''
    generate index.html page containing information for the dump
    run of the specified date for all wikis
    '''
    def __init__(self, config, date, dumptype, verbose, args):
        '''
        pass a dict of the standard args
        (config, date, dumptype, args)
        '''
        self._config = config
        self.date = date
        self.dumptype = dumptype
        self.indexfile = IndexFile(self._config)
        self.dumpdir = MiscDumpDir(self._config)
        self.verbose = verbose
        self.args = args

    def do_all_wikis(self):
        '''
        generate index.html file for all wikis for the given date.
        FIXME maybe this should be for the latest run date? Hrm.
        '''
        text = ""
        for wikiname in self._config.all_wikis_list:
            result = self.do_one_wiki(wikiname)
            if result:
                log(self.verbose, "result for wiki %s is %s"
                    % (wikiname, result))
                text = text + "<li>" + result + "</li>\n"
        index_text = (self._config.read_template(self._config.indextmpl)
                      % {"items": text})
        FileUtils.write_file_in_place(self.indexfile.get_path(),
                                      index_text, self._config.fileperms)

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
        dirinfo = MiscDumpDir(self._config, dump_date)
        path = dirinfo.get_dumpdir(wikiname)
        output_fileinfo = {}
        for filename in filenames_tocheck:
            output_fileinfo[filename] = FileUtils.file_info(os.path.join(path, filename))
        files_text = []
        filenames = sorted(output_fileinfo.keys())
        for filename in filenames:
            file_date, file_size = output_fileinfo[filename]
            log(self.verbose, "output file %s for %s %s %s"
                % (filename, wikiname, safe(file_date), safe(file_size)))
            if filename in expected and file_date is None:
                # may do more with this sort of error in the future
                # for now, just get stats on the other files
                continue
            if file_date:
                files_text.append(
                    "%s: %s (size %s)<br />"
                    # FIXME check that this link is correct
                    % (make_link(
                        os.path.join(
                            wikiname, dump_date,
                            filename),
                        os.path.basename(filename)), file_date, file_size))
        return files_text

    def get_stat_text(self, dump_date, wikiname):
        '''
        generate and return the text string describing
        the status of the dump of the wiki for the given date
        '''
        stat = StatusFile(self._config, dump_date, wikiname)
        stat_contents = FileUtils.read_file(stat.get_path())
        log(self.verbose, "status for %s %s" % (wikiname, safe(stat_contents)))
        if stat_contents:
            stat_text = "(%s)" % (stat_contents)
        else:
            stat_text = None
        return stat_text

    def do_one_wiki(self, wikiname, date=None):
        '''
        collect the text strings for one wiki to be inserted into
        the index.html file
        '''
        if (wikiname not in self._config.private_wikis_list and
                wikiname not in self._config.closed_wikis_list and
                wikiname not in self._config.skip_wikis_list):
            dumps_dirs = MiscDumpDirs(self._config, wikiname)
            if not exists(self.dumpdir.get_dumpdir_no_date(wikiname)):
                log(self.verbose, "No dump for wiki %s" % wikiname)
                return
            if date is not None:
                dump_date = date
            else:
                dump_date = dumps_dirs.get_latest_dump_date(True)
            if not dump_date:
                log(self.verbose, "No dump for wiki %s" % wikiname)
                return

            other_runs_text = "other runs: %s<br />" % make_link(wikiname, wikiname)
            try:
                wiki = Wiki(self._config, wikiname)
                wiki.set_date(dump_date)
                # fixme this is icky
                dump_class = MiscDumpFactory.get_dumper(self.dumptype)
                dumper = dump_class(wiki, False, self.verbose, self.args)
                output_files, expected = dumper.get_output_files()
                files_text = self.get_outputfile_indextxt(output_files, expected,
                                                          wikiname, dump_date)

                # fixme this is icky too
                md5file = MD5File(wiki.config, wiki.date, wikiname)
                md5file_text = self.get_outputfile_indextxt([md5file.get_filename()], [],
                                                            wikiname, dump_date)
                files_text.extend(md5file_text)

                stat_text = self.get_stat_text(dump_date, wikiname)

            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                log(self.verbose, "Error encountered, no information available"
                    " for wiki %s" % wikiname)
                return ("<strong>%s</strong> Error encountered,"
                        " no information available | %s" % (wikiname, other_runs_text))

            try:
                wikiname_text = "<strong>%s</strong>" % wikiname

                wiki_info = (" ".join([entry for entry in [wikiname_text, stat_text]
                                       if entry is not None]) + "<br />")
                wiki_info = (wiki_info + "&nbsp;&nbsp;" + "\n&nbsp;&nbsp;".join(files_text))
                wiki_info = wiki_info + "\n&nbsp;" + other_runs_text
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                log(self.verbose, "Error encountered formatting information"
                    " for wiki %s" % wikiname)
                return ("Error encountered formatting information"
                        " for wiki %s" % wikiname)

            return wiki_info


class MiscDumpOne(object):
    '''
    run dump of specified name on all wikis, or if do_dump
    is False, only generate the index.html file containing
    information on the dump run, for all wikis.

    args are keyword args converted to a dict, these get passed
    through to the class for the specific dump you want
    '''
    def __init__(self, config, date, wikiname, dumptype, do_dumps,
                 do_index, dryrun, verbose, forcerun, args):
        self._config = config
        self.wiki = Wiki(self._config, wikiname)
        self.date = date
        self.wiki.set_date(self.date)
        self.wikiname = wikiname
        self.dumpdir = MiscDumpDir(self._config, self.date)
        self.do_dumps = do_dumps
        self.do_index = do_index
        self.dryrun = dryrun
        self.forcerun = forcerun
        self.status_info = StatusInfo(self._config, self.date, self.wikiname)
        self.dumps_dirs = MiscDumpDirs(self._config, self.wikiname)
        self.verbose = verbose
        self.dumptype = dumptype
        dump_class = MiscDumpFactory.get_dumper(self.dumptype)
        self.dumper = dump_class(self.wiki, self.dryrun, self.verbose, args)
        self.args = args

    def do_one_wiki(self):
        '''
        run dump of specified type for one wiki, for given date
        unless it is among the wikis we skip, has already been run
        for the date, or some other process has the lock and is
        therefore presumably already dumping it
        '''
        if (self.wikiname not in self._config.private_wikis_list and
                self.wikiname not in self._config.closed_wikis_list and
                self.wikiname not in self._config.skip_wikis_list):
            if not exists(self.dumpdir.get_dumpdir(self.wikiname)):
                os.makedirs(self.dumpdir.get_dumpdir(self.wikiname))
            status = self.status_info.get_status()
            if status == "done:all" and not self.forcerun:
                log(self.verbose, "wiki %s skipped, adds/changes dump already"
                    " complete" % self.wikiname)
                return STATUS_GOOD

            if not self.dryrun:
                lock = MiscDumpLock(self._config, self.date, self.wikiname)
                if not lock.get_lock():
                    log(self.verbose, "wiki %s skipped, wiki is locked,"
                        " another process should be doing the job"
                        % self.wikiname)
                    return STATUS_TODO

                self.dumps_dirs.cleanup_old_dumps(self.date)

            log(self.verbose, "Doing run for wiki: %s" % self.wikiname)

            try:
                result = self.dumper.run()
                if not result:
                    return STATUS_FAILED

                if not self.dryrun:
                    output_files, expected = self.dumper.get_output_files()
                    if not md5sums(self.wiki, self.wiki.config.fileperms,
                                   output_files, expected):
                        return STATUS_FAILED
                    self.status_info.set_status("done:" + self.dumper.get_stages_done())
                    lock.unlock()

                if self.do_index:
                    index = Index(self._config, self.date, self.dumptype, self.verbose, self.args)
                    index.do_all_wikis()
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                if not self.dryrun:
                    lock.unlock()
                return STATUS_FAILED
        log(self.verbose, "Success!  Wiki %s %s dump complete."
            % (self.wikiname, self.dumptype))
        return STATUS_GOOD


class MiscDumpLoop(object):
    '''
    do the specified dumptype for all wikis for the given date, including
    regeneration of the index.html file, with various dump phases optionally
    skipped according to the supplied args
    '''
    def __init__(self, config, date, dumptype, do_dump,
                 do_index, dryrun, verbose, forcerun, args):
        self._config = config
        self.date = date
        self.dumptype = dumptype
        self.do_dump = do_dump
        self.do_index = do_index
        self.dryrun = dryrun
        self.verbose = verbose
        self.forcerun = forcerun
        self.args = args

    def do_run_on_all_wikis(self):
        '''
        run dump of given type on all wikis for given date; some
        dump phases may be skipped depending on the supplied args.

        no retries are performed.  wikis currently locked (i.e.
        being handled by some other process) are skipped.

        the number of failures and pending wikis are returned.
        '''
        failures = 0
        todos = 0
        for wikiname in self._config.all_wikis_list:
            dump = MiscDumpOne(self._config, self.date, wikiname,
                               self.dumptype, self.do_dump, self.do_index,
                               self.dryrun, self.verbose, self.forcerun,
                               self.args)
            result = dump.do_one_wiki()
            if result == STATUS_FAILED:
                failures = failures + 1
            elif result == STATUS_TODO:
                todos = todos + 1
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
                raise BackupError("Too many consecutive failures,"
                                  "giving up")
            time.sleep(300)


def usage(message=None):
    '''
    display a usage message for this script and
    the arguments for each known dump type.

    an optional message may be passed that will be
    displayed first.
    '''
    if message:
        print message
    usage_message = (
        """Usage: python generatemiscdumps.py --dumptype <type> [options] [args] [wikidbname]

Options: --configfile, --date, --dumponly, --indexonly,
         --dryrun, --forcerun, --verbose

 --dumptype:    type of dump to be run.  Known types include:
                {0}
 --configfile:  Specify an alternate config file to read. Default
                file is 'miscdump.conf' in the current directory.
 --date:        (Re)run dump of a given date (use with care).
 --dumponly:    Do only the dump without rebuilding the index.html file.
 --indexonly:   Generate the index.html file only, don't run the dump.
 --dryrun:      Don't dump anything but print the commands that would be run.
 --forcerun:    Do the run even if there is already a successful run in place.
 --verbose:     Print error messages and other informative messages
                (normally the script runs silently).

 wikidbname:    Run the dumps only for the specific wiki.

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


def main():
    '''
    entry point:
    get and process args, verify args,
    run specified dumptype for one or all wikis in config file
    for today or specified date
    '''
    config_file = False
    date = None
    dumptype = None
    do_dump = True
    do_index = True
    dryrun = False
    verbose = False
    forcerun = False
    wikiname = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ['date=', 'dumptype=', 'configfile=', 'wiki=', 'dumpsonly',
             'indexonly', 'dryrun', 'verbose', 'forcerun'])
    except Exception as ex:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--date":
            date = val
        elif opt == "--configfile":
            config_file = val
        elif opt == "--wiki":
            wikiname = val
        elif opt == "--dumptype":
            dumptype = val
        elif opt == "--dumpsonly":
            do_index = False
        elif opt == "--indexonly":
            do_dump = False
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--forcerun":
            forcerun = True

    if not do_dump and not do_index:
        usage("You may not specify more than one of dumpsonly "
              "and indexonly together.")

    if dumptype is None:
        usage("Mandatory dumptype argument not specified")
    elif dumptype not in MiscDumpFactory.get_known_dumptypes():
        usage("No such known dump " + dumptype)

    configurator = MiscDumpFactory.get_configurator(dumptype)
    if config_file:
        config = configurator(config_file)
    else:
        config = configurator()

    args = {}
    if not date:
        date = TimeUtils.today()

    if len(remainder) > 0:
        for opt in remainder:
            if ':' in opt:
                name, value = opt.split(':', 1)
                args[name] = value
            else:
                args[opt] = True

    if wikiname is not None:
        dump_one = MiscDumpOne(config, date, wikiname, dumptype, do_dump, do_index,
                               dryrun, verbose, forcerun, args)
        dump_one.do_one_wiki()
    else:
        dump_all = MiscDumpLoop(config, date, dumptype, do_dump, do_index, dryrun,
                                verbose, forcerun, args)
        dump_all.do_all_wikis_til_done(3)


if __name__ == "__main__":
    main()
