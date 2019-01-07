"""
this script copies the most recent files into
 a deployment directory with the current date
run on fenari/bastion host as root or fail.
"""

import os
import sys
import time
import shutil
import re


BASEDIR = "/home/wikipedia/downloadserver/snapshothosts/dumps"
DEPLOY = "deploy"  # subdir where deployment trees are stored by date
CONFS = "confs"    # subdir where config files live
MONTHNAMES = ["jan", "feb", "mar", "apr", "may", "jun",
              "jul", "aug", "sep", "oct", "nov", "dec"]


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


def date_to_digits(date_string):
    """convert date string in form mon-dd-yyyy to tuple
    of year, monthnum, day and return it, or None
    on error"""
    if '-' not in date_string:
        return None
    month, day, year = date_string.split('-', 2)
    if not month.isdigit():
        if month not in MONTHNAMES:
            return None
        month = int(MONTHNAMES.index(month)) + 1
        day = int(day)
    return "%s%02d%02d" % (year, month, day)


def get_latest_depl_date(deploydir):
    try:
        subdirs = os.listdir(deploydir)
    except Exception:
        sys.stderr.write("Failed to read contents of %s\n" % deploydir)
        raise
    deploy_dates = {}
    for dname in subdirs:
        if not os.path.isdir(os.path.join(deploydir, dname)):
            continue
        # expect mon-dd-yyyy
        canonical_dirname = date_to_digits(dname)
        if not canonical_dirname:
            continue
        deploy_dates[canonical_dirname] = dname
    if not deploy_dates.keys():
        return None
    dates = deploy_dates.keys()
    dates.sort(reverse=True)
    return deploy_dates[dates[0]]


# modified from the python copytree implementation
def do_copy(sourcedir, targetdir):
    """only for regular files, symlinks, dirs:
    will attempt to remove files/symlinks in target dir that
    are to be copied from the source dir, leaving any other contents
    of target dir tree in place"""
    names = os.listdir(sourcedir)

    errors = []
    if not os.path.isdir(targetdir):
        # if this fails we want to give up on the spot since
        # there will be no target directory to receive the contents
        try:
            os.makedirs(targetdir)
        # fixme is this the right set of errors?
        except (IOError, os.error) as why:
            errors.append((sourcedir, targetdir, str(why)))
            raise Error(errors)

    for name in names:
        sourcepath = os.path.join(sourcedir, name)
        targetpath = os.path.join(targetdir, name)

        # try to remove file/symlink if it's there
        if os.path.isfile(targetpath) or os.path.islink(targetpath):
            try:
                os.unlink(targetpath)
            except (IOError, os.error) as why:
                errors.append((sourcepath, targetpath, str(why)))
                continue
        # do the copy
        try:
            if os.path.islink(sourcepath):
                linkto = os.readlink(sourcepath)
                os.symlink(linkto, targetpath)
            elif os.path.isdir(sourcepath):
                do_copy(sourcepath, targetpath)
            elif os.path.isfile(sourcepath):
                shutil.copy2(sourcepath, targetpath)
            else:
                errors.append("refusing to remove %s, not file or dir or symlink\n")
        except (IOError, os.error) as why:
            errors.append((sourcepath, targetpath, str(why)))
        # catch the Error from the recursive do_copy so that we can
        # continue with other files
        except Error as err:
            errors.extend(err.args[0])
    try:
        shutil.copystat(sourcedir, targetdir)
    except OSError as why:
        errors.extend((sourcedir, targetdir, str(why)))
    if errors:
        raise Error(errors)


def set_conf_perms(path):
    """make sure all files in the conf dir are read only
    by owner"""
    conf_dir = os.path.join(path, CONFS)
    for fname in os.listdir(conf_dir):
        os.chmod(os.path.join(conf_dir, fname), 0o600)


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """
Usage: python prep-dumps-deploy.py [deploydate]

This script copies the currently deployed junk to a new directory
tree named with today's date, in preparation for the user updating
specific files for push to a cluster of hosts.

This script takes one option, the deploydate, which is the name
of the new directory to create or update. This can be specified
either as mm-dd-yyy or as mon-dd-yyyy where 'mon' is the first three
letters of the month, in lower case, eg. 'mar-12-2012'. If this
argument is not specified, today's date will be used to generate
 the dir name.
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    subdir = None

    deploydir = os.path.join(BASEDIR, DEPLOY)

    today = time.strftime("%m-%d-%Y", time.gmtime())
    month, day, year = today.split('-')
    today_dir = "%s-%s-%s" % (MONTHNAMES[int(month) - 1], day, year)

    if len(sys.argv) > 1:
        new_date = sys.argv[1]
        month, rest = new_date.split('-', 1)
        if not re.match("^[0-9][0-9]-20[0-9][0-9]$", rest):
            usage("bad date format")
        if month.isdigit():
            subdir = "%s-%s" % (MONTHNAMES[int(month) - 1], rest)
        elif month not in MONTHNAMES:
            usage("bad date format")
        else:
            subdir = new_date

    if os.geteuid() != 0:
        sys.stderr.write("Script must be run as root.\n")
        sys.exit(1)

    if not subdir:
        subdir = today_dir

    full_path_dest = os.path.join(deploydir, subdir)

    if not os.path.isdir(deploydir):
        sys.stderr.write("Directory %s does not exist or "
                         "is not a directory, giving up.\n" % deploydir)
        sys.exit(1)

    # what's the dir date that's most recent?
    latest_deploy_date = get_latest_depl_date(deploydir)
    if not latest_deploy_date:
        sys.stderr.write("There seems to be no deployment directory"
                         " in %s we can copy, giving up \n" % deploydir)
        sys.exit(1)

    full_path_src = os.path.join(deploydir, latest_deploy_date)

    print("Setting up deployment dir", full_path_dest, "from", latest_deploy_date)

    if os.path.isdir(full_path_dest):
        print(("New deployment dir already exists. overwrite,"
               "remove and copy, or skip [O/r/s]? "),)
        reply = sys.stdin.readline().strip()
        if reply in ['O', 'o']:
            do_copy(full_path_src, full_path_dest)
        elif reply in ['r', 'R']:
            shutil.rmtree(full_path_dest)
            do_copy(full_path_src, full_path_dest)
        elif reply in ['s', 'S']:
            print("Skipping at user's request")
            sys.exit(0)
        else:
            print("Unknown response, giving up.")
            sys.exit(1)
    else:
        do_copy(full_path_src, full_path_dest)
    set_conf_perms(full_path_dest)


if __name__ == "__main__":
    main()
