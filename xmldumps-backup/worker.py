#!/usr/bin/python3
# Worker process, does the actual dumping

import getopt
import os
from os.path import exists
import sys

from dumps.wikidump import Wiki, Config, cleanup, Locker
from dumps.jobs import DumpFilename
from dumps.runner import Runner
from dumps.utils import TimeUtils
from dumps.batch import BatchesFile


def check_jobs(wiki, date, job, skipjobs, page_id_range, partnum_todo,
               checkpoint_file, prefetch, prefetchdate, spawn, dryrun, skipdone, verbose,
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

    runner = Runner(wiki, prefetch=prefetch, prefetchdate=prefetchdate, spawn=spawn, job=job,
                    skip_jobs=skipjobs, restart=restart, notice=html_notice, dryrun=dryrun,
                    enabled=None, partnum_todo=partnum_todo, checkpoint_file=checkpoint_file,
                    page_id_range=page_id_range, skipdone=skipdone, verbose=verbose)

    if not runner.dump_item_list.old_runinfo_retrieved:
        # failed to get the run's info so let's call it 'didn't run'
        return False

    results = runner.dumpjobdata.runinfo.get_old_runinfo_from_file()
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


def find_next_wiki_with_batches(config, jobs_requested, verbose):
    # look for wikis that are not done and have batches to be claimed, choose
    # the.. um... one of them anyways. heh. which one?

    # note that fixed_dump_order had better be used only with the skipdone option,
    # otherwise the first wiki in the list will be run over and over :-P
    # we like this order because we can put one of the "bigwikis" that takes
    # forever to finish, at the head of the list,letting it take however many cores
    # and be slow, while the rest of the wikis run on the other cores one after
    # another and finish up.  If we start the slowest one lots later, it might
    # be the only thing running for several days when the rest of the wikis have
    # already finished, it doesn't expand to use all available cores (this would be
    # too hard on the db servers)
    nextdbs = config.db_list_unsorted
    maxbatches = 0
    wiki_todo = None

    if verbose:
        sys.stderr.write("Finding next wiki with the most unclaimed batches...\n")

    for dbname in nextdbs:
        wiki = Wiki(config, dbname)
        batchfile = BatchesFile(wiki, jobs_requested)
        batches_unclaimed = batchfile.count_unclaimed_batches()
        if batches_unclaimed > maxbatches:
            maxbatches = batches_unclaimed
            wiki_todo = wiki
    return wiki_todo


def find_lock_next_wiki(config, locks_enabled, cutoff, prefetch, prefetchdate,
                        spawn, html_notice, bystatustime=False,
                        check_job_status=False, check_prereq_status=False,
                        date=None, job=None, skipjobs=None, page_id_range=None,
                        partnum_todo=None, checkpoint_file=None, skipdone=False, restart=False,
                        verbose=False):
    # note that fixed_dump_order had better be used only with the skipdone option,
    # otherwise the first wiki in the list will be run over and over :-P
    # we like this order because we can put one of the "bigwikis" that takes
    # forever to finish, at the head of the list,letting it take however many cores
    # and be slow, while the rest of the wikis run on the other cores one after
    # another and finish up.  If we start the slowest one lots later, it might
    # be the only thing running for several days when the rest of the wikis have
    # already finished, it doesn't expand to use all available cores (this would be
    # too hard on the db servers)
    if config.fixed_dump_order:
        nextdbs = config.db_list_unsorted
    else:
        nextdbs = config.db_list_by_age(bystatustime)
        nextdbs.reverse()

    if verbose and not cutoff:
        if config.fixed_dump_order:
            sys.stderr.write("Finding next unlocked wiki in list...\n")
        else:
            sys.stderr.write("Finding oldest unlocked wiki...\n")

    # if we skip locked wikis which are missing the prereqs for this job,
    # there are still wikis where this job needs to run
    missing_prereqs = False

    for dbname in nextdbs:
        wiki = Wiki(config, dbname)
        if cutoff:
            if bystatustime:
                last_updated = wiki.date_touched_latest_dump()
            else:
                last_updated = wiki.latest_dump()

            if last_updated >= cutoff:
                continue
        if check_job_status:
            if check_jobs(wiki, date, job, skipjobs, page_id_range,
                          partnum_todo, checkpoint_file, restart,
                          prefetch, prefetchdate, spawn, True,
                          skipdone, verbose, html_notice):
                continue
        try:
            if locks_enabled:
                locker = Locker(wiki, date)
                locker.lock()
            return wiki
        except Exception as ex:
            if check_prereq_status:
                # if we skip locked wikis which are missing the prereqs for this job,
                # there are still wikis where this job needs to run
                if not check_jobs(wiki, date, job, skipjobs, page_id_range, partnum_todo,
                                  checkpoint_file, prefetch, prefetchdate,
                                  spawn, True, skipdone, verbose,
                                  html_notice, prereqs=True, restart=restart):
                    missing_prereqs = True
            sys.stderr.write("Couldn't lock %s, someone else must have got it...\n" % dbname)
            continue
    if missing_prereqs:
        return False
    return None


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    usage_text = """Usage: python3 worker.py [options] [wikidbname]
Options: --aftercheckpoint, --checkpoint, --partnum, --configfile, --date, --job,
         --skipjobs, --addnotice, --delnotice, --force, --noprefetch,
         --prefetchdate, --nospawn, --restartfrom, --log, --cleanup, --cutoff,
         --batches, --numbatches\n")
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
               More than one job can be specified as a comma-separated list
               This option cannot be specified with --force.
--skipjobs:    Comma separated list of jobs not to run on the wiki(s)
               give the option --job help
--dryrun:      Don't really run the job, just print what would be done (must be used
               with a specified wikidbname on which to run
--force:       steal the lock for the specified wiki; dangerous, if there is
               another process doing a dump run for that wiki and that date.
--exclusive    Even if rerunning just one job of a wiki, get a lock to make sure no other
               runners try to work on that wiki. Default: for single jobs, don't lock
--noprefetch:  Do not use a previous file's contents for speeding up the dumps
               (helpful if the previous files may have corrupt contents)
--prefetchdate:  Read page content from the dump of the specified date (YYYYMMDD)
                 and reuse for the current page content dumps.  If not specified
                 and prefetch is enabled (the default), the most recent good
                 dump will be used.
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
--prereqs:     If a job fails because the prereq is not done, try to do the prereq,
               a chain of up to 5 such dependencies is permitted.
--batches:     Look for a batch file, claim and run batches for an executing dump
               run until there are none left.
               In this mode the script does not update the index.html file or various
               status files. This requires the --job argument and the --date argument.
--numbatches:  If we create a batch file (we are a primary worker), or we simply
               process an existing batch file (we are a secondary worker invoked with
               --batches), claim and run only this many batches; if numbatches is 0,
               do as many as we can with no limit, until done or failure.
               If we are not either creating batches or processing them but are a
               regular nonbatched worker, this setting has no effect.
               default: 0 (do as many batches as we can until done)
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
        prefetchdate = None
        spawn = True
        restart = False
        jobs_requested = None
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
        do_prereqs = False
        batchworker = False
        numbatches = 0

        try:
            (options, remainder) = getopt.gnu_getopt(
                sys.argv[1:], "",
                ['date=', 'job=', 'skipjobs=', 'configfile=', 'addnotice=',
                 'delnotice', 'force', 'dryrun', 'noprefetch', 'prefetchdate=',
                 'nospawn', 'restartfrom', 'aftercheckpoint=', 'log', 'partnum=',
                 'checkpoint=', 'pageidrange=', 'cutoff=', "batches", "numbatches",
                 "skipdone", "exclusive", "prereqs", "cleanup", 'verbose'])
        except Exception as ex:
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
            elif opt == "--prefetchdate":
                prefetchdate = val
            elif opt == "--nospawn":
                spawn = False
            elif opt == "--dryrun":
                dryrun = True
            elif opt == "--job":
                jobs_requested = val
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
            elif opt == "--batches":
                batchworker = True
            elif opt == "--numbatches":
                numbatches = val
                if not numbatches.isdigit():
                    usage("--numbatches must be a number")
            elif opt == "--skipdone":
                skipdone = True
            elif opt == "--cleanup":
                cleanup_files = True
            elif opt == "--exclusive":
                do_locking = True
            elif opt == "--verbose":
                verbose = True
            elif opt == "--prereqs":
                do_prereqs = True

        if jobs_requested is not None:
            if ',' in jobs_requested:
                jobs_todo = jobs_requested.split(',')
            else:
                jobs_todo = [jobs_requested]
        else:
            jobs_todo = []

        if dryrun and not remainder:
            usage("--dryrun requires the name of a wikidb to be specified")
        if restart and not jobs_requested:
            usage("--restartfrom requires --job and the job from which to restart")
        if restart and len(jobs_todo) > 1:
            usage("--restartfrom requires --job and exactly one job from which to restart")
        if partnum_todo is not None and not jobs_requested:
            usage("--partnum option requires specific job(s) for which to rerun that part")
        if partnum_todo is not None and restart:
            usage("--partnum option can be specified only for a specific list of jobs")
        if checkpoint_file is not None and not remainder:
            usage("--checkpoint option requires the name of a wikidb to be specified")
        if checkpoint_file is not None and not jobs_requested:
            usage("--checkpoint option requires --job")
        if page_id_range and not jobs_requested:
            usage("--pageidrange option requires --job")
        if page_id_range and checkpoint_file is not None:
            usage("--pageidrange option cannot be used with --checkpoint option")
        if prefetchdate is not None and not prefetch:
            usage("prefetchdate and noprefetch options may not be specified together")
        if prefetchdate is not None and (not prefetchdate.isdigit() or len(prefetchdate) != 8):
            usage("prefetchdate must be of the form YYYYMMDD")
        if batchworker and not jobs_requested:
            usage("--batches option requires --job")
        if batchworker and not date:
            usage("--batches option requires --date")
        if batchworker and restart:
            usage("--batches and --restart options are mutually exclusive")
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
            except AttributeError as ex:
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

        if batchworker or dryrun or partnum_todo is not None:
            locks_enabled = False
        elif (jobs_requested is not None and
              not restart and not do_locking and not force_lock):
            locks_enabled = False
        else:
            locks_enabled = True

        if dryrun:
            print("***")
            print("Dry run only, no files will be updated.")
            print("***")

        if remainder:
            wiki = Wiki(config, remainder[0])
            if cutoff:
                # fixme if we asked for a specific job then check that job only
                # not the dir
                last_ran = wiki.latest_dump()
                if last_ran >= cutoff:
                    wiki = None
            if wiki is not None and locks_enabled:
                locker = Locker(wiki, date)
                if force_lock and locks_enabled:
                    lockfiles = locker.is_locked()
                    locker.unlock(lockfiles, owner=False)
                if locks_enabled:
                    locker.lock()

        else:
            # if the run is across all wikis and we are just doing one job,
            # we want the age of the wikis by the latest status update
            # and not the date the run started

            if jobs_requested is not None and jobs_todo[0] == 'createdirs':
                check_status_time = False
                # there won't actually be a status for this job but we want
                # to ensure that the directory and the status file are present
                # and intact
                check_job_status = True
                check_prereq_status = False
            else:
                check_status_time = bool(jobs_requested is not None)
                check_job_status = bool(skipdone)
                check_prereq_status = bool(jobs_requested is not None and skipdone)
            if batchworker:
                wiki = find_next_wiki_with_batches(config, jobs_requested, verbose=False)
            else:
                wiki = find_lock_next_wiki(config, locks_enabled, cutoff, prefetch,
                                           prefetchdate, spawn,
                                           html_notice, check_status_time,
                                           check_job_status, check_prereq_status, date,
                                           jobs_todo[0] if jobs_todo else None,
                                           skip_jobs, page_id_range,
                                           partnum_todo, checkpoint_file,
                                           skipdone, restart, verbose)

        if wiki is not None and wiki:
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
                if (jobs_requested is None or
                        not set(jobs_requested).issubset(set(after_checkpoint_jobs))):
                    usage("--aftercheckpoint option requires --job option with one or more of %s"
                          % ", ".join(after_checkpoint_jobs))

            enabled = {}
            if enable_logging:
                enabled = {"logging": True}

            if restart:
                sys.stderr.write("Running %s, restarting from job %s...\n" %
                                 (wiki.db_name, jobs_todo[0]))
            elif jobs_requested:
                sys.stderr.write("Running %s, jobs %s...\n" % (wiki.db_name, jobs_requested))
            else:
                sys.stderr.write("Running %s...\n" % wiki.db_name)

            # no specific jobs requested, runner will do them all
            if not jobs_todo:
                runner = Runner(wiki, prefetch, prefetchdate, spawn, None, skip_jobs,
                                restart, html_notice, dryrun, enabled,
                                partnum_todo, checkpoint_file, page_id_range, skipdone,
                                cleanup_files, do_prereqs, batchworker, numbatches, verbose)

                result = runner.run()
                if result is not None and result:
                    exitcode = 0

            else:
                # do each job requested one at a time
                for job in jobs_todo:
                    runner = Runner(wiki, prefetch, prefetchdate, spawn, job, skip_jobs,
                                    restart, html_notice, dryrun, enabled,
                                    partnum_todo, checkpoint_file, page_id_range, skipdone,
                                    cleanup_files, do_prereqs, batchworker, numbatches, verbose)

                    result = runner.run()
                    if result is not None and result:
                        exitcode = 0

            # if we are doing one piece only of the dump, we don't unlock either
            if locks_enabled:
                locker = Locker(wiki, date)
                lockfiles = locker.is_locked()
                locker.unlock(lockfiles, owner=True)
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
