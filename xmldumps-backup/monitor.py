# Wiki dump-generation monitor

import os
import sys
from os.path import exists
from dumps.WikiDump import Wiki, Config, Locker
from dumps.fileutils import FileUtils
from dumps.runnerutils import StatusHtml


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
        wiki = Wiki(config, db_name)
        locker = Locker(wiki)
        lockfiles = locker.is_stale(all_locks=True)
        if lockfiles:
            locker.cleanup_stale_locks(lockfiles)
        running = running or locker.is_locked(all_locks=True)
        states.append(StatusHtml.status_line(wiki))

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


def main():
    # can specify name of alternate config file
    if len(sys.argv) >= 2:
        config = Config(sys.argv[1])
    else:
        config = Config()
    update_index(config)


if __name__ == "__main__":
    main()
