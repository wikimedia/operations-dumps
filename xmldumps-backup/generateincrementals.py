# for every wiki, generate the max rev id if it isn't already
# present from a previous attempt at a run, read the max rev id
# from the previous adds changes dump, dump stubs, dump history file
# based on stubs.

import getopt
import os
from os.path import exists
import sys
import time
import hashlib
import traceback
from miscdumplib import STATUS_TODO, STATUS_GOOD, STATUS_FAILED
from miscdumplib import StatusFile, IndexFile
from miscdumplib import MD5File, MiscDumpDirs, MiscDumpDir
from miscdumplib import MiscDumpLock, StatusInfo
from miscdumplib import log, safe, make_link
from incr_dumps import IncrDump
from incr_dumps import DumpConfig
from incr_dumps import cutoff_from_date
from dumps.WikiDump import Wiki
from dumps.exceptions import BackupError
from dumps.WikiDump import FileUtils, TimeUtils


class Index(object):
    def __init__(self, config, date, dumper, verbose):
        self._config = config
        self.date = date
        self.dumper = dumper
        self.indexfile = IndexFile(self._config)
        self.dumpdir = MiscDumpDir(self._config)
        self.verbose = verbose

    def do_all_wikis(self):
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

    def do_one_wiki(self, wikiname, date=None):
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

            other_runs_text = "other runs: %s" % make_link(wikiname, wikiname)

            try:
                wiki = Wiki(self._config, wikiname)
                wiki.set_date(dump_date)
                output_files, expected = self.dumper.get_output_files()
                dirinfo = MiscDumpDir(self._config, dump_date)
                path = dirinfo.get_dumpdir(wiki.db_name)
                output_fileinfo = {}
                for filename in output_files:
                    output_fileinfo[filename] = FileUtils.file_info(os.path.join(path, filename))
                files_text = []
                errors = False
                for filename in output_fileinfo:
                    file_date, file_size = output_fileinfo[filename]
                    log(self.verbose, "output file %s for %s %s %s"
                        % (filename, wikiname, safe(file_date), safe(file_size)))
                    if filename in expected and file_date is None:
                        # may do more with this sort of error in the future
                        # for now, just get stats on the other files
                        continue
                    if file_date:
                        files_text.append(
                            "%s: %s (size %s)"
                            % (os.path.basename(filename), make_link(
                                os.path.join(
                                    wikiname, dump_date,
                                    filename),
                                file_date), file_size))

                stat = StatusFile(self._config, dump_date, wikiname)
                stat_contents = FileUtils.read_file(stat.get_path())
                log(self.verbose, "status for %s %s" % (wikiname, safe(stat_contents)))
                if stat_contents:
                    stat_text = "(%s)" % (stat_contents)
                else:
                    stat_text = None

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
                wiki_info = (wiki_info + " &nbsp;&nbsp; " + " |  ".join(files_text))
                wiki_info = wiki_info + "|  " + other_runs_text
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                log(self.verbose, "Error encountered formatting information"
                    " for wiki %s" % wikiname)
                return ("Error encountered formatting information"
                        " for wiki %s" % wikiname)

            return wiki_info


class MiscDumpOne(object):
    def __init__(self, config, date, wikiname, do_dumps,
                 do_index, dryrun, verbose, forcerun, args):
        self._config = config
        self.wiki = Wiki(self._config, wikiname)
        self.date = date
        self.wiki.set_date(self.date)
        self.cutoff = args['cutoff']
        self.wikiname = wikiname
        self.dumpdir = MiscDumpDir(self._config, self.date)
        self.do_dumps = do_dumps

        self.do_stubs = args['do_stubs']
        self.do_revs = args['do_revs']
        self.do_index = do_index
        self.dryrun = dryrun
        self.forcerun = forcerun
        self.status_info = StatusInfo(self._config, self.date, self.wikiname)
        self.dumps_dirs = MiscDumpDirs(self._config, self.wikiname)
        self.verbose = verbose
        self.incr = IncrDump(self.wiki, self.dryrun, self.verbose, args)

    def do_one_wiki(self):
        if (self.wikiname not in self._config.private_wikis_list and
                self.wikiname not in self._config.closed_wikis_list and
                self.wikiname not in self._config.skip_wikis_list):
            if not exists(self.dumpdir.get_dumpdir(self.wikiname)):
                os.makedirs(self.dumpdir.get_dumpdir(self.wikiname))
            status = self.status_info.get_status()
            if status == "done" and not self.forcerun:
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
                result = self.incr.run()
                if not result:
                    return STATUS_FAILED

                if not self.dryrun:
                    output_files, expected = self.incr.get_output_files()
                    if not md5sums(self.wiki, self.wiki.config.fileperms,
                                   output_files, expected):
                        return STATUS_FAILED
                    self.status_info.set_status("done")
                    lock.unlock()

                if self.do_index:
                    index = Index(self._config, self.date, self.incr, self.verbose)
                    index.do_all_wikis()
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                if not self.dryrun:
                    lock.unlock()
                return STATUS_FAILED
        log(self.verbose, "Success!  Wiki %s incremental dump complete."
            % self.wikiname)
        return STATUS_GOOD


def md5sum_one_file(filename):
    summer = hashlib.md5()
    infile = file(filename, "rb")
    bufsize = 4192 * 32
    buff = infile.read(bufsize)
    while buff:
        summer.update(buff)
        buff = infile.read(bufsize)
    infile.close()
    return summer.hexdigest()


def md5sums(wiki, fileperms, files, mandatory):
    md5file = MD5File(wiki.config, wiki.db_name, wiki.date)
    text = ""
    files = []
    errors = False
    for fname in files:
        try:
            text = text + "%s\n" % md5sum_one_file(fname)
            FileUtils.write_file_in_place(md5file.get_path(),
                                          text, fileperms)
        except:
            if fname in mandatory:
                errors = True
    return not errors


class MiscDumpLoop(object):
    def __init__(self, config, date, do_dump,
                 do_index, dryrun, verbose, forcerun, args):
        self._config = config
        self.date = date
        self.do_dump = do_dump
        self.do_index = do_index
        self.dryrun = dryrun
        self.verbose = verbose
        self.forcerun = forcerun
        self.args = args

    def do_run_on_all_wikis(self):
        failures = 0
        todos = 0
        for wikiname in self._config.all_wikis_list:
            dump = MiscDumpOne(self._config, self.date, wikiname,
                               self.do_dump, self.do_index,
                               self.dryrun, self.verbose, self.forcerun,
                               self.args)
            result = dump.do_one_wiki()
            if result == STATUS_FAILED:
                failures = failures + 1
            elif result == STATUS_TODO:
                todos = todos + 1
        return (failures, todos)

    def do_all_wikis_til_done(self, num_fails):
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
    if message:
        print message
    usage_message = (
        """Usage: python generateincrementals.py [options] [args] [wikidbname]

Options: --configfile, --date, --dumponly, --indexonly,
         --dryrun, --forcerun, --verbose

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
       before the final wikidbname argument.  These arguments will be
       in pairs, first the argument name, then whitespace, then the argument
       value.
""")
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    config_file = False
    date = None
    do_dump = True
    do_index = True
    dryrun = False
    verbose = False
    forcerun = False
    args = {'do_stubs': True, 'do_revs': True, 'cutoff': None}

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ['date=', 'configfile=', 'stubsonly', 'revsonly',
             'indexonly', 'dryrun', 'verbose', 'forcerun'])
    except Exception as ex:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--date":
            date = val
        elif opt == "--configfile":
            config_file = val
        elif opt == "--stubsonly":
            do_index = False
            args['do_revs'] = False
        elif opt == "--revsonly":
            do_index = False
            args['do_stubs'] = False
        elif opt == "--indexonly":
            do_dump = False
            args['do_stubs'] = False
            args['do_revs'] = False
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--forcerun":
            forcerun = True

    if not do_dump and not do_index:
        usage("You may not specify more than one of dumpsonly "
              "and indexonly together.")

    if config_file:
        config = DumpConfig(config_file)
    else:
        config = DumpConfig()

    if not date:
        date = TimeUtils.today()
        args['cutoff'] = time.strftime("%Y%m%d%H%M%S",
                                       time.gmtime(time.time() - config.delay))
    else:
        args['cutoff'] = cutoff_from_date(date, config)

    if len(remainder) > 0:
        dump_one = MiscDumpOne(config, date, remainder[0], do_dump, do_index,
                               dryrun, verbose, forcerun, args)
        dump_one.do_one_wiki()
    else:
        dump_all = MiscDumpLoop(config, date, do_dump, do_index, dryrun, verbose, forcerun, args)
        dump_all.do_all_wikis_til_done(3)


if __name__ == "__main__":
    main()
