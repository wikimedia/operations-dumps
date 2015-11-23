# Worker process, does the actual dumping

import getopt
import os
import sys

from os.path import exists
from dumps.WikiDump import Wiki, Config, cleanup, Locker
from dumps.jobs import DumpFilename
from dumps.runner import Runner
from dumps.utils import TimeUtils

def check_jobs(wiki, date, job, skipjobs, page_id_range, partnum_todo,
               checkpoint_file, prefetch, spawn, dryrun, skipdone, verbose,
               html_notice, prereqs=False, restart=False):
    '''
    if prereqs is False:
    see if dump run on specific date completed specific job(s)
    or if no job was specified, ran to completion

    if prereqs is True:
    see if dump run on specific date completed prereqs for specific job(s)
    or if no job was specified, return True

    '''
    if not date:
        return False

    if date == 'last':
        dumps = sorted(wiki.dump_dirs())
        if dumps:
            date = dumps[-1]
        else:
            # never dumped so that's the same as 'job didn't run'
            return False

    if not job and prereqs:
        return True

    wiki.set_date(date)

    runner = Runner(wiki, prefetch=prefetch, spawn=spawn, job=job,
                    skip_jobs=skipjobs, restart=restart, notice=html_notice, dryrun=dryrun,
                    enabled=None, partnum_todo=partnum_todo, checkpoint_file=checkpoint_file,
                    page_id_range=page_id_range, skipdone=skipdone, verbose=verbose)

    if not runner.dump_item_list.old_runinfo_retrieved:
        # failed to get the run's info so let's call it 'didn't run'
        return False

    results = runner.dumpjobdata.runinfofile.get_old_runinfo_from_file()
    if results:
        for runinfo_obj in results:
            runner.dump_item_list._set_dump_item_runinfo(runinfo_obj)

    # mark the jobs we would run
    if job:
        runner.dump_item_list.mark_dumps_to_run(job, True)
        if restart:
            runner.dump_item_list.mark_following_jobs_to_run(True)
    else:
        runner.dump_item_list.mark_all_jobs_to_run(True)

    if not prereqs:
        # see if there are any to run. no? then return True (all job(s) done)
        # otherwise return False (still some to do)
        for item in runner.dump_item_list.dump_items:
            if item.to_run():
                return False
        return True
    else:
        # get the list of prereqs, see if they are all status done, if so
        # return True, otherwise False (still some to do)
        prereq_items = []
        for item in runner.dump_item_list.dump_items:
            if item.name() == job:
                prereq_items = item._prerequisite_items
                break

        for item in prereq_items:
            if item.status() != "done":
                return False
        return True


def find_lock_next_wiki(config, locks_enabled, cutoff, prefetch, spawn, dryrun,
                        html_notice, bystatustime=False,
                        check_job_status=False, check_prereq_status=False,
                        date=None, job=None, skipjobs=None, page_id_range=None,
                        partnum_todo=None, checkpoint_file=None, skipdone=False, restart=False,
                        verbose=False):
    if config.halt:
        sys.stderr.write("Dump process halted by config.\n")
        return None

    nextdbs = config.db_list_by_age(bystatustime)
    nextdbs.reverse()

    if verbose and not cutoff:
        sys.stderr.write("Finding oldest unlocked wiki...\n")

    # if we skip locked wikis which are missing the prereqs for this job,
    # there are still wikis where this job needs to run
    missing_prereqs = False

    for dbname in nextdbs:
        wiki = Wiki(config, dbname)
        if cutoff:
            last_updated = wiki.date_touched_latest_dump()
            if last_updated >= cutoff:
                continue
        if check_job_status:
            if check_jobs(wiki, date, job, skipjobs, page_id_range,
                          partnum_todo, checkpoint_file, restart,
                          prefetch, spawn, dryrun, skipdone, verbose, html_notice):
                continue
        try:
            if locks_enabled:
                locker = Locker(wiki)
                locker.lock()
            return wiki
        except:
            if check_prereq_status:
                # if we skip locked wikis which are missing the prereqs for this job,
                # there are still wikis where this job needs to run
                if not check_jobs(wiki, date, job, skipjobs, page_id_range, partnum_todo,
                                  checkpoint_file, prefetch, spawn, dryrun, skipdone, verbose,
                                  html_notice, prereqs=True, restart=restart):
                    missing_prereqs = True
            sys.stderr.write("Couldn't lock %s, someone else must have got it...\n" % dbname)
            continue
    if missing_prereqs:
        return False
    else:
        return None


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    usage_text = """Usage: python worker.py [options] [wikidbname]
Options: --aftercheckpoint, --checkpoint, --partnum, --configfile, --date, --job,
         --skipjobs, --addnotice, --delnotice, --force, --noprefetch,
         --nospawn, --restartfrom, --log, --cleanup, --cutoff\n")
--aftercheckpoint: Restart this job from the after specified checkpoint file, doing the
               rest of the job for the appropriate part number if parallel subjobs each
               doing one part are configured, or for the all the rest of the revisions
               if no parallel subjobs are configured;
               only for jobs articlesdump, metacurrentdump, metahistorybz2dump.
--checkpoint:  Specify the name of the checkpoint file to rerun (requires --job,
               depending on the file this may imply --partnum)
--partnum:     Specify the number of the part to rerun (use with a specific job
               to rerun, only if parallel jobs (parts) are enabled).
--configfile:  Specify an alternative configuration file to read.
               Default config file name: wikidump.conf
--date:        Rerun dump of a given date (probably unwise)
               If 'last' is given as the value, will rerun dump from last run date if any,
               or today if there has never been a previous run
--addnotice:   Text message that will be inserted in the per-dump-run index.html
               file; use this when rerunning some job and you want to notify the
               potential downloaders of problems, for example.  This option
               remains in effective for the specified wiki and date until
               the delnotice option is given.
--delnotice:   Remove any notice that has been specified by addnotice, for
               the given wiki and date.
--job:         Run just the specified step or set of steps; for the list,
               give the option --job help
               This option requires specifiying a wikidbname on which to run.
               This option cannot be specified with --force.
--skipjobs:    Comma separated list of jobs not to run on the wiki(s)
               give the option --job help
--dryrun:      Don't really run the job, just print what would be done (must be used
               with a specified wikidbname on which to run
--force:       remove a lock file for the specified wiki (dangerous, if there is
               another process running, useful if you want to start a second later
               run while the first dump from a previous date is still going)
               This option cannot be specified with --job.
--exclusive    Even if rerunning just one job of a wiki, get a lock to make sure no other
               runners try to work on that wiki. Default: for single jobs, don't lock
--noprefetch:  Do not use a previous file's contents for speeding up the dumps
               (helpful if the previous files may have corrupt contents)
--nospawn:     Do not spawn a separate process in order to retrieve revision texts
--restartfrom: Do all jobs after the one specified via --job, including that one
--skipdone:    Do only jobs that are not already succefully completed
--log:         Log progress messages and other output to logfile in addition to
               the usual console output
--cutoff:      Given a cutoff date in yyyymmdd format, display the next wiki for which
               dumps should be run, if its last dump was older than the cutoff date,
               and exit, or if there are no such wikis, just exit
--cleanup:     Remove all files that may already exist for the spefici wiki and
               run, for the specified job or all jobs
--verbose:     Print lots of stuff (includes printing full backtraces for any exception)
               This is used primarily for debugging
"""
    sys.stderr.write(usage_text)
    sys.exit(1)


def main():
    os.environ['DUMPS'] = str(os.getpid())

    try:
        date = None
        config_file = False
        force_lock = False
        prefetch = True
        spawn = True
        restart = False
        job_requested = None
        skip_jobs = None
        enable_logging = False
        html_notice = ""
        dryrun = False
        partnum_todo = None
        after_checkpoint = False
        checkpoint_file = None
        page_id_range = None
        cutoff = None
        exitcode = 1
        skipdone = False
        do_locking = False
        verbose = False
        cleanup_files = False

        try:
            (options, remainder) = getopt.gnu_getopt(
                sys.argv[1:], "",
                ['date=', 'job=', 'skipjobs=', 'configfile=', 'addnotice=',
                 'delnotice', 'force', 'dryrun', 'noprefetch', 'nospawn',
                 'restartfrom', 'aftercheckpoint=', 'log', 'partnum=',
                 'checkpoint=', 'pageidrange=', 'cutoff=', "skipdone",
                 "exclusive", "cleanup", 'verbose'])
        except:
            usage("Unknown option specified")

        for (opt, val) in options:
            if opt == "--date":
                date = val
            elif opt == "--configfile":
                config_file = val
            elif opt == '--checkpoint':
                checkpoint_file = val
            elif opt == '--partnum':
                partnum_todo = int(val)
            elif opt == "--force":
                force_lock = True
            elif opt == '--aftercheckpoint':
                after_checkpoint = True
                checkpoint_file = val
            elif opt == "--noprefetch":
                prefetch = False
            elif opt == "--nospawn":
                spawn = False
            elif opt == "--dryrun":
                dryrun = True
            elif opt == "--job":
                job_requested = val
            elif opt == "--skipjobs":
                skip_jobs = val
            elif opt == "--restartfrom":
                restart = True
            elif opt == "--log":
                enable_logging = True
            elif opt == "--addnotice":
                html_notice = val
            elif opt == "--delnotice":
                html_notice = False
            elif opt == "--pageidrange":
                page_id_range = val
            elif opt == "--cutoff":
                cutoff = val
                if not cutoff.isdigit() or not len(cutoff) == 8:
                    usage("--cutoff value must be in yyyymmdd format")
            elif opt == "--skipdone":
                skipdone = True
            elif opt == "--cleanup":
                cleanup_files = True
            elif opt == "--exclusive":
                do_locking = True
            elif opt == "--verbose":
                verbose = True

        if dryrun and (len(remainder) == 0):
            usage("--dryrun requires the name of a wikidb to be specified")
        if job_requested and force_lock:
            usage("--force cannot be used with --job option")
        if restart and not job_requested:
            usage("--restartfrom requires --job and the job from which to restart")
        if partnum_todo is not None and not job_requested:
            usage("--partnum option requires a specific job for which to rerun that part")
        if partnum_todo is not None and restart:
            usage("--partnum option can be specified only for one specific job")
        if checkpoint_file is not None and (len(remainder) == 0):
            usage("--checkpoint option requires the name of a wikidb to be specified")
        if checkpoint_file is not None and not job_requested:
            usage("--checkpoint option requires --job and the job from which to restart")
        if page_id_range and not job_requested:
            usage("--pageidrange option requires --job and the job from which to restart")
        if page_id_range and checkpoint_file is not None:
            usage("--pageidrange option cannot be used with --checkpoint option")

        if skip_jobs is None:
            skip_jobs = []
        else:
            skip_jobs = skip_jobs.split(",")

        # allow alternate config file
        if config_file:
            config = Config(config_file)
        else:
            config = Config()
        externals = ['php', 'mysql', 'mysqldump', 'head', 'tail',
                     'checkforbz2footer', 'grep', 'gzip', 'bzip2',
                     'writeuptopageid', 'recompressxml', 'sevenzip', 'cat']

        failed = False
        unknowns = []
        notfound = []
        for external in externals:
            try:
                ext = getattr(config, external)
            except AttributeError:
                unknowns.append(external)
                failed = True
            else:
                if not exists(ext):
                    notfound.append(ext)
                    failed = True
        if failed:
            if unknowns:
                sys.stderr.write("Unknown config param(s): %s\n" % ", ".join(unknowns))
            if notfound:
                sys.stderr.write("Command(s) not found: %s\n" % ", ".join(notfound))
            sys.stderr.write("Exiting.\n")
            sys.exit(1)

        if dryrun or partnum_todo is not None or (job_requested and not restart and not do_locking):
            locks_enabled = False
        else:
            locks_enabled = True

        if dryrun:
            print "***"
            print "Dry run only, no files will be updated."
            print "***"

        if len(remainder) > 0:
            wiki = Wiki(config, remainder[0])
            if cutoff:
                # fixme if we asked for a specific job then check that job only
                # not the dir
                last_ran = wiki.latest_dump()
                if last_ran >= cutoff:
                    wiki = None
            if wiki is not None and locks_enabled:
                locker = Locker(wiki)
                if force_lock and locker.is_locked():
                    locker.unlock()
                if locks_enabled:
                    locker.lock()

        else:
            # if the run is across all wikis and we are just doing one job,
            # we want the age of the wikis by the latest status update
            # and not the date the run started
            if job_requested:
                check_status_time = True
            else:
                check_status_time = False
            if skipdone:
                check_job_status = True
            else:
                check_job_status = False
            if job_requested and skipdone:
                check_prereq_status = True
            else:
                check_prereq_status = False
            wiki = find_lock_next_wiki(config, locks_enabled, cutoff, prefetch, spawn,
                                       dryrun, html_notice, check_status_time,
                                       check_job_status, check_prereq_status,
                                       date, job_requested, skip_jobs, page_id_range,
                                       partnum_todo, checkpoint_file, skipdone, restart, verbose)

        if wiki is not None and wiki:
            # process any per-project configuration options
            config.parse_conffile_per_project(wiki.db_name)

            if date == 'last':
                dumps = sorted(wiki.dump_dirs())
                if dumps:
                    date = dumps[-1]
                else:
                    date = None

            if date is None or not date:
                date = TimeUtils.today()
            wiki.set_date(date)

            if after_checkpoint:
                fname = DumpFilename(wiki)
                fname.new_from_filename(checkpoint_file)
                if not fname.is_checkpoint_file:
                    usage("--aftercheckpoint option requires the "
                          "name of a checkpoint file, bad filename provided")
                page_id_range = str(int(fname.last_page_id) + 1)
                partnum_todo = fname.partnum_int
                # now we don't need this.
                checkpoint_file = None
                after_checkpoint_jobs = ['articlesdump', 'metacurrentdump',
                                         'metahistorybz2dump']
                if not job_requested or job_requested not in [
                        'articlesdump', 'metacurrentdump', 'metahistorybz2dump']:
                    usage("--aftercheckpoint option requires --job option with one of %s"
                          % ", ".join(after_checkpoint_jobs))

            enabled = {}
            if enable_logging:
                enabled = {"logging": True}
            runner = Runner(wiki, prefetch, spawn, job_requested, skip_jobs,
                            restart, html_notice, dryrun, enabled,
                            partnum_todo, checkpoint_file, page_id_range, skipdone,
                            cleanup_files, verbose)

            if restart:
                sys.stderr.write("Running %s, restarting from job %s...\n" %
                                 (wiki.db_name, job_requested))
            elif job_requested:
                sys.stderr.write("Running %s, job %s...\n" % (wiki.db_name, job_requested))
            else:
                sys.stderr.write("Running %s...\n" % wiki.db_name)
            result = runner.run()
            if result is not None and result:
                exitcode = 0
            # if we are doing one piece only of the dump, we don't unlock either
            if locks_enabled:
                locker = Locker(wiki)
                locker.unlock()
        elif wiki is not None:
            sys.stderr.write("Wikis available to run but prereqs not complete.\n")
            exitcode = 0
        else:
            sys.stderr.write("No wikis available to run.\n")
            exitcode = 255
    finally:
        cleanup()
    sys.exit(exitcode)

if __name__ == "__main__":
    main()
