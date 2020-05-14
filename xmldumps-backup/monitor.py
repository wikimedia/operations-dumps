#!/usr/bin/python3
# Wiki dump-generation monitor

import os
from os.path import exists
import sys
import traceback
import json
from dumps.wikidump import Wiki, Config, Locker
from dumps.fileutils import FileUtils
from dumps.report import StatusHtml
from dumps.runstatusapi import StatusAPI
from dumps.batch import BatchesFile


VERBOSE = False


def add_to_filename(filename, infix):
    base, suffix = filename.split('.', 1)
    return base + "-" + infix + ("." + suffix if suffix else "")


def cleanup_stale_dumplocks(config, dbs):
    '''
    clean up all stale locks for dump runs, where staleness
    is determined by the wiki dump configuration
    '''
    running = False
    states = []
    for db_name in dbs:
        try:
            wiki = Wiki(config, db_name)
            locker = Locker(wiki)
            lockfiles = locker.is_stale(all_locks=True)
            if lockfiles:
                locker.cleanup_stale_locks(lockfiles)
            running = running or locker.is_locked(all_locks=True)
            states.append(StatusHtml.status_line(wiki))
        except Exception:
            # if there's a problem with one wiki at least
            # let's show the rest
            if VERBOSE:
                traceback.print_exc(file=sys.stdout)
    return running, states


def cleanup_batch_jobfile_if_stale(basedir, filename, wiki):
    '''
    if the file exists and has an mtime older than the wiki
    config specifies, remove it and mark the corresponding
    batch job as aborted
    '''
    filepath = os.path.join(basedir, filename)
    try:
        age = FileUtils.file_age(filepath)
    except FileNotFoundError:
        return

    try:
        if age > wiki.config.batchjobs_stale_age:
            os.unlink(filepath)
            jobname, batch_range = BatchesFile.get_components(filename)
            batches = BatchesFile(wiki, jobname)
            # range is of form pnnnpmmm in the filename, but we need
            # a pair for the batches functions
            fields = batch_range.split('p')
            batches.abort((fields[1], fields[2]))

    except Exception:
        # FIXME we should probably be louder about this
        if VERBOSE:
            traceback.print_exc(file=sys.stdout)


def cleanup_stale_batch_jobfiles(config, dbs):
    '''
    check all existing batch jobfiles (empty files touched periodically
    as a batch runs) and remove any if they are stale (have not been
    updated within a certain period of time); also mark the corresponding
    batch as aborted
    '''
    for db_name in dbs:
        wiki = Wiki(config, db_name)
        # get the most recent run, assuming that's the one we care about
        # if someone is rerunning parts of an old run and needs to clean
        # up stale locks for that, they can manually intervene
        subdirs = wiki.dump_dirs(private=True)
        if not subdirs:
            continue
        date = subdirs[-1]
        basedir = os.path.join(wiki.private_dir(), date)
        files = os.listdir(basedir)
        for filename in files:
            if BatchesFile.is_batchjob_file(filename):
                wiki.set_date(date)
                cleanup_batch_jobfile_if_stale(basedir, filename, wiki)


def generate_index(config, other_indexhtml=None, sorted_by_db=False):

    if sorted_by_db:
        dbs = sorted(config.db_list)
    else:
        dbs = config.db_list_by_age()

    cleanup_stale_batch_jobfiles(config, dbs)
    running, states = cleanup_stale_dumplocks(config, dbs)
    if running:
        status = "Dumps are in progress..."
    elif exists("maintenance.txt"):
        status = FileUtils.read_file("maintenance.txt")
    else:
        status = "Dump process is idle."

    if other_indexhtml is None:
        other_index_link = ""
    else:
        if sorted_by_db:
            other_sortedby = "dump date"
        else:
            other_sortedby = "wiki name"

        other_index_link = ('Also view sorted by <a href="%s">%s</a>'
                            % (os.path.basename(other_indexhtml), other_sortedby))

    return config.read_template("download-index.html") % {
        "otherIndexLink": other_index_link,
        "status": status,
        "items": "\n".join(states)}


def generate_json(config):
    """
    go through all the latest dump dirs, collect up all the json
    contents from the dumpstatusapi file, and shovel them into
    one ginormous json object and scribble that out. heh.
    """
    json_out = {"wikis": {}}

    dbs = config.db_list

    for db_name in dbs:
        try:
            wiki = Wiki(config, db_name)
            json_out["wikis"][wiki.db_name] = StatusAPI.get_wiki_info(wiki)
        except Exception:
            # if there's a problem with one wiki at least
            # let's show the rest
            if VERBOSE:
                traceback.print_exc(file=sys.stdout)
    return json_out


def update_index(config):
    output_fname = os.path.join(config.public_dir, config.index)
    output_fname_sorted_by_db = add_to_filename(os.path.join(
        config.public_dir, config.index), "bydb")

    temp_fname = output_fname + ".tmp"
    fhandle = open(temp_fname, "wt")
    fhandle.write(generate_index(config, other_indexhtml=output_fname_sorted_by_db))
    fhandle.close()
    os.rename(temp_fname, output_fname)

    temp_fname = output_fname_sorted_by_db + ".tmp"
    fhandle = open(temp_fname, "wt")
    fhandle.write(generate_index(config, other_indexhtml=output_fname,
                                 sorted_by_db=True))
    fhandle.close()
    os.rename(temp_fname, output_fname_sorted_by_db)


def update_json(config):
    output_fname = os.path.join(config.public_dir, "index.json")
    temp_fname = output_fname + ".tmp"
    fhandle = open(temp_fname, "wt")
    fhandle.write(json.dumps(generate_json(config)))
    fhandle.close()
    os.rename(temp_fname, output_fname)


def main():
    # can specify name of alternate config file
    if len(sys.argv) >= 2:
        if sys.argv[1] in ['--help', '-h']:
            message = """Usage: python3 monitor.py [<configfilepath>]
Writes main index.html file for xml/sql dump tree, covering most
recent dump run for each wiki; also cleans up stale locks
            """
            sys.stderr.write(message)
            sys.exit(1)
        config = Config(sys.argv[1])
    else:
        config = Config()
    update_index(config)
    update_json(config)


if __name__ == "__main__":
    main()
