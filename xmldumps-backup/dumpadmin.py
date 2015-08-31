'''
run a set of commands in a given order,
given information about how many free slots
the host has for such processes and how many
slots each process takes.
'''
import os
import sys
import getopt
import glob
import socket
import signal
from dumps.utils import RunInfoFile, Chunk
from dumps.runnerutils import Checksummer, Status, NoticeFile, SymLinks
from dumps.jobs import DumpDir
from worker import DumpItemList
from dumps.WikiDump import Wiki, Config


def command_has_wiki(pid, wikiname):
    '''
    see if the process with the given pid is operating on the
    given wiki by checking process command line args
    '''

    if wikiname is None:
        # no check needed
        return True

    try:
        process_command = open("/proc/%s/cmdline" % pid, "r")
    except IOError:
        # permission or gone, anyways not us
        return False
    for line in process_command:
        if line:
            fields = line.split("\x00")
            for field in fields:
                if field == wikiname or field == "--wiki=" + wikiname:
                    process_command.close()
                    return True
    process_command.close()
    return False


def get_job_output_files(wiki, job, dump_item_list):
    '''
    return list of output files produced by job
    '''
    job_files = []
    for item in dump_item_list:
        if item.name() == job:
            job_files = item.list_outfiles_for_cleanup(DumpDir(wiki, wiki.db_name))
            break
    return job_files


def check_process_running(pid):
    '''
    see if process with given pid is running
    and if we started it.

    it's possible for the process to die or be killed
    in the meantime after this returns, what can you do
    '''
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False

    try:
        process_environ = open("/proc/%s/environ" % pid, "r")
    except IOError:
        # permission or gone, anyways not us
        return False
    for line in process_environ:
        if line:
            fields = line.split("\x00")
            for field in fields:
                if field == "DUMPS":
                    process_environ.close()
                    return True
    process_environ.close()
    return False


def get_lockfile_content(filename):
    '''
    return the host running the process
    and the pid of the process that created
    the lockfile
    '''
    with open(filename, "r") as lockfile:
        content = lockfile.read()
        lines = content.splitlines()
        if len(lines) != 1:
            return(None, None)
        else:
            host, pid = lines[0].split(" ", 1)
            return(host, pid)


def create_file(filename):
    '''
    create an empty file
    '''
    open(filename, 'a').close()


def remove_file(filename):
    '''
    remove a file
    '''
    try:
        os.unlink(filename)
    except:
        pass


class ActionHandler(object):
    '''
    methods for all actions, whether on one wiki or on all
    '''

    def __init__(self, actions, message, undo, configfile,
                 wikiname, dryrun, verbose):
        '''
        constructor.
        reads configs for every wiki, this might be wasteful
        but really how long can it take? even with 1k wikis
        '''
        self.verbose = verbose
        if not actions and not undo:
            if self.verbose:
                sys.stderr.write("No actions specified.\n")
            return

        self.actions = actions
        self.undo = undo
        self.dryrun = dryrun
        self.wikiname = wikiname
        self.configfile = configfile
        self.message = message
        self.conf = Config(self.configfile)

        if self.wikiname is None:
            self.wikilist = self.conf.db_list
        else:
            self.wikilist = [self.wikiname]

        self.wikiconfs = {}
        for wiki in self.wikilist:
            self.wikiconfs[wiki] = self.get_wiki_config(wiki)

    def get_wiki_config(self, wikiname):
        '''
        parse and return the configuration for a particular wiki
        '''
        wikiconf = Config(self.configfile)
        wikiconf.parse_conffile_per_project(wikiname)
        return wikiconf

    def do_all(self):
        '''
        do all actions specified at instantiation time
        '''
        self.conf.parse_conffile_globally()
        self.do_global_actions()
        self.undo_global_actions()
        self.do_per_wiki_actions()
        self.undo_per_wiki_actions()

    def do_global_actions(self):
        '''
        do all actions that either do not
        reference a particular wiki (maintenance,
        exit) or may run on one or all wikis
        '''
        for item in self.actions:
            if item == "kill":
                self.do_kill()
            elif item == "unlock":
                self.do_unlock()
            elif item == "remove":
                self.do_remove()
            elif item == "maintenance":
                self.do_maintenance()
            elif item == "exit":
                self.do_exit()

    def do_per_wiki_actions(self):
        '''
        do all actions that must reference
        only one wiki
        '''
        for item in self.actions:
            for wiki in self.wikiconfs:
                if item == "notice":
                    self.do_notice(wiki)

    def undo_global_actions(self):
        '''
        undo all specified actions that do not
        reference a particular wiki
        '''
        for item in self.undo:
            if item == "maintenance":
                self.undo_maintenance()
            elif item == "exit":
                self.undo_exit()

    def undo_per_wiki_actions(self):
        '''
        undo all specified actions that must
        reference a particular wiki
        '''
        for wiki in self.wikiconfs:
            for item in self.undo:
                if item == "notice":
                    self.undo_notice(wiki)

    def get_dump_pids(self):
        '''
        get list of pids either for one wiki or for all
        which are running dumps; these must have been started by
        either the scheduler, the bash wrapper or the worker.py
        script.  i.e. if a user runs dumpBackups.php by hand
        that is not going to be picked up.

        don't rely on lock files, they may have been removed or not created
        look up processes with DUMPS environ var set. values:
           'scheduler' (the dumps scheduler)
           'wrapper' (the bash dumps wrapper that runs across all wikis
           pid (the worker that runs on one wiki and any processes it spawned)
        we want at all costs to avoid hardcoded list of commands
        '''
        pids = []
        uid = os.geteuid()
        for process_id in os.listdir('/proc'):
            if process_id.isdigit():
                # owned by us
                puid = os.stat(os.path.join('/proc', process_id)).st_uid
                if puid == uid:
                    # has DUMPS environ var
                    try:
                        process_environ = open("/proc/%s/environ" % process_id, "r")
                    except IOError:
                        # permission or gone, anyways not us
                        continue
                    for line in process_environ:
                        if line:
                            fields = line.split("\x00")
                            for field in fields:
                                if field.startswith("DUMPS="):
                                    # if no wiki specified for instance, get procs for all
                                    if self.wikiname is None or command_has_wiki(process_id, self.wikiname):
                                        pids.append(process_id)
                                    break
                    process_environ.close()
        return pids

    def do_kill(self):
        '''
        kill all dump related processes for the wiki specified
        at instantiation or all wikis; good only for processes
        started by the scheduler, the bash wrapper script or
        the python worker script
        '''
        pids = self.get_dump_pids()
        if self.dryrun:
            print "would kill processes", pids
            return
        elif self.verbose:
            print "killing these processes:", pids

        for pid in pids:
            os.kill(int(pid), signal.SIGTERM)

    def do_unlock(self):
        '''
        unlock either wiki specified at instantiation or
        all wikis, provided they were locked on current host
        '''
        lock_info = self.find_dump_lockinfo()
        # fixme does this iter over keys?
        for wiki in lock_info:
            if check_process_running(lock_info[wiki]['pid']):
                continue
            if self.dryrun:
                print "would remove lock", lock_info[wiki]['name']
            else:
                if self.verbose:
                    print "removing lock for", wiki
                os.unlink(lock_info[wiki]['filename'])

    def find_failed_dumps_for_wiki(self, wikiname):
        '''
        return list of failed jobs for the latest run
        for the specified wiki or empty list if there are none
        '''

        failed_jobs = []
        # fixme how is the above a string, shouldn't it be a function?
        wiki = Wiki(self.wikiconfs[wikiname], wikiname)
        date = wiki.latest_dump()
        if date is None:
            return [], None

        wiki.set_date(date)
        run_info_file = RunInfoFile(wiki, False)
        results = run_info_file.get_old_runinfo_from_file()
        if not results:
            return [], None

        for entry in results:
            if entry.status() == "failed":
                failed_jobs.append(entry.name())
        return failed_jobs, date

    def find_failed_dumps(self):
        '''
        return dict of failed jobs per wiki during most recent run,
        skipping over wikis with no failed jobs
        '''

        failed_dumps = {}
        for wiki in self.wikilist:
            results, date = self.find_failed_dumps_for_wiki(wiki)
            if results and date is not None:
                failed_dumps[wiki] = {}
                failed_dumps[wiki][date] = results

        if self.verbose:
            print "failed dumps info:", failed_dumps
        return failed_dumps

    def do_remove(self):
        '''
        find all failed dump jobs for unlocked wikis
        clean them up after getting lock on each one
        first, then remove lock

        if a specific wiki was specified at instantiation,
        clean up only that wiki
        '''
        failed_dumps = self.find_failed_dumps()
        for wikiname in failed_dumps:
            for date in failed_dumps[wikiname]:
                wiki = Wiki(self.wikiconfs[wikiname], wikiname)
                wiki.set_date(date)

                try:
                    wiki.lock()
                except:
                    sys.stderr.write("Couldn't lock %s, can't do cleanup\n" % wikiname)
                    continue
                self.cleanup_dump(wiki, failed_dumps[wikiname][date])
                wiki.unlock()

    def cleanup_dump(self, wiki, failed_jobs):
        '''
        for the specified wiki, and the given list
        of failed jobs, find all the output files, toss
        them, then rebuild: md5sums file, symlinks
        into latest dir, dump run info file
        '''
        chunk_info = Chunk(wiki, wiki.db_name)
        dump_dir = DumpDir(wiki, wiki.db_name)
        run_info_file = RunInfoFile(wiki, True)
        dump_item_list = DumpItemList(wiki, False, False, False, None, None,
                                      True, chunk_info, None, run_info_file, dump_dir)
        if not failed_jobs:
            if self.verbose:
                print "no failed jobs for wiki", wiki

        for job in failed_jobs:
            files = get_job_output_files(wiki, job, dump_item_list.dump_items)
            paths = [dump_dir.filename_public_path(fileinfo) for fileinfo in files]
            if self.verbose:
                print "for job", job, "these are the output files:", paths
            for filename in paths:
                if self.dryrun:
                    print "would unlink", filename
                else:
                    try:
                        os.unlink(filename)
                    except:
                        continue

        if self.dryrun:
            print "would update dumpruninfo file, checksums file, ",
            print "status file, index.html file and symlinks to latest dir"
            return

        # need to update status files, dumpruninfo, checksums file
        # and latest links.
        checksums = Checksummer(wiki, dump_dir, True, False)
        html_notice_file = NoticeFile(wiki, "", True)
        status = Status(wiki, dump_dir, dump_item_list.dump_items, checksums,
                        True, False, html_notice_file, None, self.verbose)
        if self.verbose:
            print "updating status files for wiki", wiki.db_name
        status.update_status_files()
        run_info_file = RunInfoFile(wiki, True)
        if self.verbose:
            print "updating dump run info file for wiki", wiki.db_name
        run_info_file.save_dump_runinfo_file(dump_item_list.report_dump_runinfo())
        symlinks = SymLinks(wiki, dump_dir, False, False, True)
        if self.verbose:
            print "updating symlinks for wiki", wiki.db_name
        symlinks.cleanup_symlinks()

    def do_maintenance(self):
        '''
        create an empty maintenance.txt file
        causes the dump runners after the next job
        to run no jobs per wiki
        and sleep 5 minutes in between each wiki

        this is a global action that affects all wikis
        run on the given host
        '''
        if self.dryrun:
            print "would create maintenance file"
            return
        elif self.verbose:
            print "creating maintenance file"
        create_file("maintenance.txt")

    def do_exit(self):
        '''
        create an empty exit.txt file; causes the
        dump runners to exit after next job

        this is a global action that affects all wikis
        run on the given host
        '''
        if self.dryrun:
            print "would create exit file"
            return
        elif self.verbose:
            print "creating exit file"
        create_file("exit.txt")

    def do_notice(self, wikiname):
        '''
        create a notice.txt file for the particular wiki for
        the most recent run. the contents will appear on its
        web page for that dump run
        '''
        wiki = Wiki(self.wikiconfs[wikiname], wikiname)
        date = wiki.latest_dump()
        if date is None:
            print "dump never run, not adding notice file for wiki", wikiname
            return

        if self.dryrun:
            print "would add notice.txt for wiki", wikiname, "date", date
            return
        elif self.verbose:
            print "creating notice file for wiki", wikiname, "date", date

        wiki.set_date(date)
        NoticeFile(wiki, self.message, True)

    def undo_maintenance(self):
        '''
        remove any maintenance.txt file that may exist,
        resumes normal operations
        '''
        if self.dryrun:
            print "would remove maintenance file"
            return
        elif self.verbose:
            print "removing maintenance file"
        remove_file("maintenance.txt")

    def undo_exit(self):
        '''
        remove any exit.txt file that may exist,
        resumes normal operations
        '''
        if self.dryrun:
            print "would remove exit file"
            return
        elif self.verbose:
            print "removing exit file"
        remove_file("exit.txt")

    def undo_notice(self, wikiname):
        '''
        remove any notice.txt file that may exist
        for the most current run for the given wiki
        '''
        wiki = Wiki(self.wikiconfs[wikiname], wikiname)
        date = wiki.latest_dump()
        if date is None:
            print "dump never run, no notice file to remove for wiki", wikiname
            return

        if self.dryrun:
            print "would remove notice.txt for wiki", wikiname, "date", date
            return
        elif self.verbose:
            print "removing notice file for wiki", wikiname, "date", date

        wiki.set_date(date)
        NoticeFile(wiki, False, True)

    def find_dump_lockinfo(self):
        '''
        get host and pid information for lockfiles for the wiki
        specified at instantiation or for all wikis
        '''
        my_hostname = socket.getfqdn()

        lockfiles = []
        results = {}
        if self.wikiname is not None:
            path = os.path.join(self.wikiconfs[self.wikiname].private_dir, self.wikiname, "lock")
            if os.path.exists(path):
                lockfiles = [path]

        else:
            lockfiles = glob.glob(os.path.join(self.conf.private_dir, "*", "lock"))

        for filename in lockfiles:
            host, pid = get_lockfile_content(filename)
            wiki = self.get_wiki_from_lockfilename(filename)
            if host == my_hostname:
                results[wiki] = {'pid': pid, 'host': host, 'filename': filename}
        return results

    def get_wiki_from_lockfilename(self, filename):
        '''
        given the full lockfile name, grab the wiki name out of it
        and return it
        '''
        if filename.endswith("lock"):
            filename = filename[:-4]
        if filename.startswith(self.conf.private_dir):
            filename = filename[len(self.conf.private_dir):]
        filename = filename.strip(os.path.sep)
        return filename


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: dumpadmin.py --<action> [--<action>...]
    [--configfile] [--wiki] [--dryrun] [--verbose] [--help]

    where <action> is one of the following:

    kill        (-k) kill all running workers and their children
    unlock      (-u) unlock all locked wikis that have lock
                     files created by a process that is no
                     longer running on the current host
    remove      (-r) remove all failed wiki jobs from most
                     recent dump, reset wiki status. This
                     removes ALL related files, so for a
                     job that produces 4 pages-article files
                     but only one is actually bad, it will
                     remove them all.
    maintenance (-m) touch maintenance.txt in cwd, causing
                     workers to run no wikis and sleep 5
                     minutes in between checks to see if
                     maintenance is done
    exit        (-e) touch exit.txt in cwd, causing workers
                     to exit after next job
    notice      (-n) message supplied will be put into notice
                     file for the given wiki for the most recent
                     dump or for all wikis
                     this notice file is incorporated into
                     the web page shown to users, once
                     the page is regenerated (during runs)

    OR

    undo        (-U) comma-separated list of 'maintenance',
                     'notice', 'exit'
                     the options specified will be undone

    wiki        (-w) run on the specified wiki: default, runs on
                     all given by the config file
    configfile  (-c) path to config file
                     default: wikidump.conf in cwd
    dryrun      (-d) don't do it but show what would be done
    verbose     (-v) print many progress messages
    help        (-h) show this message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def check_options(remainder, configfile):
    '''
    whine if these options have problems
    '''
    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if not os.path.exists(configfile):
        usage("no such file found: " + configfile)


def fixup_undo(undo):
    '''
    convert comma sep argument into list
    '''
    if undo is not None:
        undo = [(item).strip() for item in undo.split(",")]
    else:
        undo = []
    return undo


def check_actions(undo, actions):
    '''
    make sure no specified action is also in the undo list
    '''
    problems = []
    for item in undo:
        if item in actions:
            problems.append(item)
    if problems:
        usage("action and undo of action cannot be specified together " +
              ", ".join(problems))


def get_action_opt(option):
    '''
    return action correspodning to command line option
    '''
    action_options = ['kill', 'unlock', 'remove', 'maintenance', 'exit']
    if option.startswith("--"):
        option = option[2:]
        if option in action_options:
            return option
    elif option.startswith("-"):
        option = option[1:]
        for action in action_options:
            if action.startswith(option):
                return action

    return None


def main():
    'main entry point, does all the work'

    actions = []
    configfile = "wikidump.conf"
    dryrun = False
    verbose = False
    message = None
    undo = None
    wiki = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:n:U:w:kurmedvh",
                                                 ["configfile=", "notice=", "no=", "undo=",
                                                  "wiki=", "kill", "unlock", "remove",
                                                  "maintenance", "exit", "dryrun",
                                                  "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-n", "--notice"]:
            actions.append("notice")
            message = val
        elif opt in ["-U", "--undo"]:
            undo = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-d", "--dryrun"]:
            dryrun = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            result = get_action_opt(opt)
            if result is not None:
                actions.append(result)
            else:
                usage("Unknown option specified: <%s>" % opt)

    check_options(remainder, configfile)
    undo = fixup_undo(undo)
    check_actions(undo, actions)

    handler = ActionHandler(actions, message, undo, configfile,
                            wiki, dryrun, verbose)
    handler.do_all()


if __name__ == '__main__':
    main()
