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
from miscdumplib import StatusFile, IndexFile
from miscdumplib import MD5File, MiscDumpDirs, MiscDumpDir
from miscdumplib import IncrDumpLock, StatusInfo
from miscdumplib import log, safe, make_link
from incr_dumps import IncrDump
from incr_dumps import MaxRevID
from incr_dumps import StubFile
from incr_dumps import RevsFile
from incr_dumps import DumpConfig
from incr_dumps import cutoff_from_date
from dumps.WikiDump import Wiki
from dumps.exceptions import BackupError
from dumps.WikiDump import FileUtils, TimeUtils


class Index(object):
    def __init__(self, config, date, verbose):
        self._config = config
        self.date = date
        self.indexfile = IndexFile(self._config)
        self.incrdir = MiscDumpDir(self._config)
        self.verbose = verbose

    def do_all_wikis(self):
        text = ""
        for wiki in self._config.all_wikis_list:
            result = self.do_one_wiki(wiki)
            if result:
                log(self.verbose, "result for wiki %s is %s"
                    % (wiki, result))
                text = text + "<li>" + result + "</li>\n"
        index_text = (self._config.read_template("incrs-index.html")
                      % {"items": text})
        FileUtils.write_file_in_place(self.indexfile.get_path(),
                                      index_text, self._config.fileperms)

    def do_one_wiki(self, wiki, date=None):
        if (wiki not in self._config.private_wikis_list and
                wiki not in self._config.closed_wikis_list and
                wiki not in self._config.skip_wikis_list):
            incr_dumps_dirs = MiscDumpDirs(self._config, wiki)
            if not exists(self.incrdir.get_dumpdir_no_date(wiki)):
                log(self.verbose, "No dump for wiki %s" % wiki)
                return
            if date is not None:
                incr_date = date
            else:
                incr_date = incr_dumps_dirs.get_latest_dump_date(True)
            if not incr_date:
                log(self.verbose, "No dump for wiki %s" % wiki)
                return

            other_runs_text = "other runs: %s" % make_link(wiki, wiki)
            try:
                stub = StubFile(self._config, incr_date, wiki)
                (stub_date, stub_size) = stub.get_fileinfo()
                log(self.verbose, "stub for %s %s %s"
                    % (wiki, safe(stub_date), safe(stub_size)))
                if stub_date:
                    stub_text = ("stubs: %s (size %s)"
                                 % (make_link(
                                     os.path.join(
                                         wiki, incr_date,
                                         stub.get_filename()),
                                     stub_date), stub_size))
                else:
                    stub_text = None

                revs = RevsFile(self._config, incr_date, wiki)
                (revs_date, revs_size) = revs.get_fileinfo()
                log(self.verbose, "revs for %s %s %s"
                    % (wiki, safe(revs_date), safe(revs_size)))
                if revs_date:
                    revs_text = (
                        "revs: %s (size %s)" % (
                            make_link(
                                os.path.join(
                                    wiki, incr_date, revs.get_filename()),
                                revs_date), revs_size))
                else:
                    revs_text = None

                stat = StatusFile(self._config, incr_date, wiki)
                stat_contents = FileUtils.read_file(stat.get_path())
                log(self.verbose, "status for %s %s" % (wiki, safe(stat_contents)))
                if stat_contents:
                    stat_text = "(%s)" % (stat_contents)
                else:
                    stat_text = None

            except Exception as ex:
                log(self.verbose, "Error encountered, no information available"
                    " for wiki %s" % wiki)
                return ("<strong>%s</strong> Error encountered,"
                        " no information available | %s" % (wiki, other_runs_text))

            try:
                wikiname_text = "<strong>%s</strong>" % wiki

                wiki_info = (" ".join([entry for entry in [wikiname_text, stat_text]
                                       if entry is not None]) + "<br />")
                wiki_info = (wiki_info + " &nbsp;&nbsp; " +
                             " |  ".join([entry for entry in [stub_text, revs_text, other_runs_text]
                                          if entry is not None]))
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                log(self.verbose, "Error encountered formatting information"
                    " for wiki %s" % wiki)
                return ("Error encountered formatting information"
                        " for wiki %s" % wiki)

            return wiki_info


class DumpResults(object):
    TODO = 1
    FAILED = -1
    GOOD = 0


class IncrDumpOne(object):
    def __init__(self, config, date, cutoff, wikiname, do_stubs,
                 do_revs, do_index_update, dryrun, verbose, forcerun):
        self._config = config
        self.wiki = Wiki(self._config, wikiname)
        self.date = date
        self.wiki.set_date(self.date)
        self.cutoff = cutoff
        self.wikiname = wikiname
        self.incrdir = MiscDumpDir(self._config, self.date)
        self.do_stubs = do_stubs
        self.do_revs = do_revs
        self.do_index_update = do_index_update
        self.dryrun = dryrun
        self.forcerun = forcerun
        self.max_revid_obj = MaxRevID(self.wiki, cutoff, self.dryrun)
        self.status_info = StatusInfo(self._config, self.date, self.wikiname)
        self.stubfile = StubFile(self._config, self.date, self.wikiname)
        self.revsfile = RevsFile(self._config, self.date, self.wikiname)
        self.incr_dumps_dirs = MiscDumpDirs(self._config, self.wikiname)
        self.verbose = verbose
        self.incr = IncrDump(self.wiki, self.dryrun, self.verbose)

    def do_one_wiki(self):
        if (self.wikiname not in self._config.private_wikis_list and
                self.wikiname not in self._config.closed_wikis_list and
                self.wikiname not in self._config.skip_wikis_list):
            if not exists(self.incrdir.get_dumpdir(self.wikiname)):
                os.makedirs(self.incrdir.get_dumpdir(self.wikiname))

            status = self.status_info.get_status()
            if status == "done" and not self.forcerun:
                log(self.verbose, "wiki %s skipped, adds/changes dump already"
                    " complete" % self.wikiname)
                return DumpResults.GOOD

            if not self.dryrun:
                lock = IncrDumpLock(self._config, self.date, self.wikiname)
                if not lock.get_lock():
                    log(self.verbose, "wiki %s skipped, wiki is locked,"
                        " another process should be doing the job"
                        % self.wikiname)
                    return DumpResults.TODO

                self.incr_dumps_dirs.cleanup_old_incrdumps(self.date)

            log(self.verbose, "Doing run for wiki: %s" % self.wikiname)

            try:
                result = self.incr.run()
                if not result:
                    return DumpResults.FAILED

                if not self.dryrun:
                    if not self.md5sums():
                        return DumpResults.FAILED
                    self.status_info.set_status("done")
                    lock.unlock()

                if self.do_index_update:
                    index = Index(self._config, self.date, self.verbose)
                    index.do_all_wikis()
            except Exception as ex:
                if self.verbose:
                    traceback.print_exc(file=sys.stdout)
                if not self.dryrun:
                    lock.unlock()
                return DumpResults.FAILED
        log(self.verbose, "Success!  Wiki %s incremental dump complete."
            % self.wikiname)
        return DumpResults.GOOD

    def md5sum_one_file(self, filename):
        summer = hashlib.md5()
        infile = file(filename, "rb")
        bufsize = 4192 * 32
        buff = infile.read(bufsize)
        while buff:
            summer.update(buff)
            buff = infile.read(bufsize)
        infile.close()
        return summer.hexdigest()

    def md5sums(self):
        try:
            md5file = MD5File(self._config, self.date, self.wikiname)
            text = ""
            files = []
            if self.do_stubs:
                files.append(self.stubfile.get_path())
            if self.do_revs:
                files.append(self.revsfile.get_path())
            for fname in files:
                text = text + "%s\n" % self.md5sum_one_file(fname)
                FileUtils.write_file_in_place(md5file.get_path(),
                                              text, self._config.fileperms)
            return True
        except Exception as ex:
            return False


class IncrDumpLoop(object):
    def __init__(self, config, date, cutoff, do_stubs, do_revs,
                 do_index_update, dryrun, verbose, forcerun):
        self._config = config
        self.date = date
        self.cutoff = cutoff
        self.do_stubs = do_stubs
        self.do_revs = do_revs
        self.do_index_update = do_index_update
        self.dryrun = dryrun
        self.verbose = verbose
        self.forcerun = forcerun

    def do_run_on_all_wikis(self):
        failures = 0
        todos = 0
        for wiki in self._config.all_wikis_list:
            dump = IncrDumpOne(self._config, self.date, self.cutoff, wiki,
                               self.do_stubs, self.do_revs, self.do_index_update,
                               self.dryrun, self.verbose, self.forcerun)
            result = dump.do_one_wiki()
            if result == DumpResults.FAILED:
                failures = failures + 1
            elif result == DumpResults.TODO:
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
        """Usage: python generateincrementals.py [options] [wikidbname]

Options: --configfile, --date, --dryrun, --revsonly, --stubsonly, --verbose"

 --configfile:  Specify an alternate config file to read. Default
                file is 'dumpincr.conf' in the current directory."
 --date:        (Re)run incremental of a given date (use with care)."
 --dryrun:      Don't dump anything but print the commands that would be run."
 --forcerun:    Do the run even if there is already a successful run in place."
 --revsonly:    Do only the stubs part of the dumps."
 --stubsonly:   Do only the revision text part of the dumps."
 --verbose:     Print error messages and other informative messages"
                (normally the script runs silently)."

 wikidbname:    Run the dumps only for the specific wiki.
""")
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    config_file = False
    date = None
    do_stubs = True
    do_revs = True
    do_index_update = True
    dryrun = False
    verbose = False
    forcerun = False

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
            do_revs = False
            do_index_update = False
        elif opt == "--revsonly":
            do_stubs = False
            do_index_update = False
        elif opt == "--indexonly":
            do_stubs = False
            do_revs = False
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--verbose":
            verbose = True
        elif opt == "--forcerun":
            forcerun = True

    if not do_revs and not do_stubs and not do_index_update:
        usage("You may not specify more than one of stubsonly,"
              "revsonly and indexonly together.")

    if config_file:
        config = DumpConfig(config_file)
    else:
        config = DumpConfig()

    if not date:
        date = TimeUtils.today()
        cutoff = time.strftime("%Y%m%d%H%M%S",
                               time.gmtime(time.time() - config.delay))
    else:
        cutoff = cutoff_from_date(date, config)

    if len(remainder) > 0:
        dump = IncrDumpOne(config, date, cutoff, remainder[0], do_stubs,
                           do_revs, do_index_update, dryrun, verbose, forcerun)
        dump.do_one_wiki()
    else:
        dump = IncrDumpLoop(config, date, cutoff, do_stubs, do_revs,
                            do_index_update, dryrun, verbose, forcerun)
        dump.do_all_wikis_til_done(3)


if __name__ == "__main__":
    main()
