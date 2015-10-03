import os, sys, time, shutil, re
# this script copies the most recent files into a deployment directory with the current date
# run on fenari/bastion host as root or fail.

basedir = "/home/wikipedia/downloadserver/snapshothosts/dumps"
deploy = "deploy" # subdir where deployment trees are stored by date
confs = "confs" # subdir where config files live

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

def dateToDigits(dateString):
    if '-' not in dateString:
        return None
    month, day, year  = dateString.split('-', 2)
    if not month.isdigit():
        if not month in monthNames:
            return None
        else:
            month = int(monthNames.index(month)) + 1
        day = int(day)
    return "%s%02d%02d" % ( year, month, day)

def getLatestDeployDate(deploydir):
    try:
        subdirs = os.listdir(deploydir)
    except:
        sys.stderr.write("Failed to read contents of %s\n" % deploydir)
        raise
    deployDates = {}
    for d in subdirs:
        if not os.path.isdir(os.path.join(deploydir, d)):
            continue
        # expect mon-dd-yyyy
        canonicalDirName = dateToDigits(d)
        if not canonicalDirName:
            continue
        deployDates[canonicalDirName] = d
    if not len(deployDates.keys()):
        return None
    dates = deployDates.keys()
    dates.sort(reverse = True)
    return deployDates[dates[0]]

# modified from the python copytree implementation
def doCopy(sourcedir, targetdir):
    """only for regular files, symlinks, dirs:
    will attempt to remove files/symlinks in target dir that
    are to be copied from the source dir, leaving any other contents
    of target dir tree in place"""
    names = os.listdir(sourcedir)

    if not os.path.isdir(targetdir):
        # if this fails we want to give up on the spot since
        # there will be no target directory to receive the contents
        try:
            os.makedirs(targetdir)
        # fixme is this the right set of errors?
        except (IOError, os.error), why:
            errors.append((sourcepath, targetpath, str(why)))
            raise Error(errors)
    
    errors = []
    for name in names:
        sourcepath = os.path.join(sourcedir, name)
        targetpath = os.path.join(targetdir, name)

        # try to remove file/symlink if it's there
        if os.path.isfile(targetpath) or os.path.islink(targetpath):
            try:
                os.unlink(targetpath)
            except (IOError, os.error), why:
                errors.append((sourcepath, targetpath, str(why)))
                continue
        # do the copy
        try:
            if os.path.islink(sourcepath):
                linkto = os.readlink(sourcepath)
                os.symlink(linkto, targetpath)
            elif os.path.isdir(sourcepath):
                doCopy(sourcepath, targetpath)
            elif os.path.isfile(sourcepath):
                shutil.copy2(sourcepath, targetpath)
            else:
                errors.append("refusingto remove %s, not file or dir or symlink\n")
        except (IOError, os.error), why:
            errors.append((sourcepath, targetpath, str(why)))
        # catch the Error from the recursive doCopy so that we can
        # continue with other files
        except Error, err:
            errors.extend(err.args[0])
    try:
        shutil.copystat(sourcedir, targetdir)
    except OSError, why:
        errors.extend((sourcedir, targetdir, str(why)))
    if errors:
        raise Error(errors)

def setConfPerms(path):
    confDir = os.path.join(path, confs)
    for f in os.listdir(confDir):
        os.chmod(os.path.join(confDir,f), 0600)

def usage(message = None):
    if message:
        sys.stderr.write("%s\n" % message)
    sys.stderr.write("Usage: python prep-dumps-deploy.py [deploydate]\n")
    sys.stderr.write("\n")
    sys.stderr.write("This script copies the currently deployed junk to a new directory tree named\n")
    sys.stderr.write("with today's date, in preparation for the user updating specific files for\n")
    sys.stderr.write("push to a cluster of hosts.\n")
    sys.stderr.write("\n")
    sys.stderr.write("This script takes one option, the deploydate, which is the name of the new directory\n")
    sys.stderr.write("to create or update. This can be specified either as mm-dd-yyy or as mon-dd-yyyy where\n")
    sys.stderr.write("mon is the first three  letters of the month, in lower case, eg. 'mar-12-2012'\n")
    sys.stderr.write("If this argument is not specified, today's date will be used to generate the dir name.\n")
    sys.exit(1)

def main():
    subdir = None

    monthNames = [ "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" ]
    deploydir = os.path.join(basedir, deploy)

    today=time.strftime("%m-%d-%Y", time.gmtime())
    month, day, year = today.split('-')
    todayDir = "%s-%s-%s" % (monthNames[int(month)-1], day, year)

    if len(sys.argv) > 1:
        newDate = sys.argv[1]
        month, rest = newDate.split('-', 1)
        if not re.match("^[0-9][0-9]-20[0-9][0-9]$", rest):
            usage("bad date format")
        if month.isdigit():
            subdir = "%s-%s" % (monthNames[int(month)-1], rest)
        elif month not in monthNames:
            usage("bad date format")
        else:
            subdir = newDate
            
    if os.geteuid() != 0:
        sys.stderr.write("Script must be run as root.\n")
        sys.exit(1)

    if not subdir:
        subdir = todayDir

    fullPathDest = os.path.join(deploydir, subdir)

    if not os.path.isdir(deploydir):
        sys.stderr.write("Directory %s does not exist or is not a directory, giving up.\n" % deploydir)
        sys.exit(1)
    
    # what's the dir date that's most recent?
    latestDeployDate = getLatestDeployDate(deploydir)
    if not latestDeployDate:
        sys.stderr.write("There seems to be no deployment directory in %s we can copy, giving up \n" % deploydir)
        sys.exit(1)
        
    fullPathSrc = os.path.join(deploydir,latestDeployDate)

    print "Setting up deployment dir", fullPathDest, "from", latestDeployDate

    if os.path.isdir(fullPathDest):
        print "New deployment dir already exists. overwrite, remove and copy, or skip [O/r/s]? ",
        input = sys.stdin.readline().strip()
        if input == 'O' or input == 'o':
            doCopy(fullPathSrc, fullPathDest)
        elif input == 'r' or input == 'R':
            shutil.rmtree(fullPathDest)
            doCopy(fullPathSrc, fullPathDest)
        elif input == 's' or input == 'S':
            print "Skipping at user's request"
            sys.exit(0)
        else: 
            print "Unknown response, giving up."
            sys.exit(1)
    else:
        doCopy(fullPathSrc, fullPathDest)
    setConfPerms(fullPathDest)

if __name__ == "__main__":
    main()
