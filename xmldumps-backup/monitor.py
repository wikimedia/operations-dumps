# Wiki dump-generation monitor

import os
from os.path import exists
import sys
import traceback
import json
from dumps.WikiDump import Wiki, Config, Locker
from dumps.fileutils import FileUtils
from dumps.runnerutils import StatusHtml
from dumps.runstatusapi import StatusAPI


VERBOSE = False


def add_to_filename(filename, infix):
    base, suffix = filename.split('.', 1)
    return base + "-" + infix + ("." + suffix if suffix else "")


def generate_index(config, other_indexhtml=None, sorted_by_db=False):
    running = False
    states = []

    if sorted_by_db:
        dbs = sorted(config.db_list)
    else:
        dbs = config.db_list_by_age()

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
    running = False
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
    filehdl = open(temp_fname, "wt")
    filehdl.write(generate_index(config, other_indexhtml=output_fname_sorted_by_db))
    filehdl.close()
    os.rename(temp_fname, output_fname)

    temp_fname = output_fname_sorted_by_db + ".tmp"
    filehdl = open(temp_fname, "wt")
    filehdl.write(generate_index(config, other_indexhtml=output_fname,
                                 sorted_by_db=True))
    filehdl.close()
    os.rename(temp_fname, output_fname_sorted_by_db)


def update_json(config):
    output_fname = os.path.join(config.public_dir, "index.json")
    temp_fname = output_fname + ".tmp"
    filehdl = open(temp_fname, "wt")
    filehdl.write(json.dumps(generate_json(config)))
    filehdl.close()
    os.rename(temp_fname, output_fname)


def main():
    # can specify name of alternate config file
    if len(sys.argv) >= 2:
        config = Config(sys.argv[1])
    else:
        config = Config()
    update_index(config)
    update_json(config)


if __name__ == "__main__":
    main()
