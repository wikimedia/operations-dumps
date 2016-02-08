import os
import sys
import subprocess
import getopt
from subprocess import Popen, PIPE


class UploadDir(object):

    def __init__(self, multiversion, scriptPath, wmfhack):
        self.multiversion = multiversion
        self.scriptPath = scriptPath
        self.hack = wmfhack

    def getMediaDir(self, wiki):
        if self.hack:
            # wmf-specific magic. hate hate hate
            site, lang = self.getDirFromSiteAndLang(wiki)
            if site and lang:
                return os.path.join(site, lang)
            else:
                return None
        else:
            return self.getUploadDir(wiki)

    def getDirFromSiteAndLang(self, wiki):
        """using wmf hack... get $site and $lang and build a relative path
        out of those."""
        inputText = 'global $site, $lang; echo \"$site\t$lang\";'
        # expect to find the runphpscriptlet script in dir with this script
        currentDir = os.path.dirname(os.path.realpath(__file__))
        command = ["python", os.path.join(currentDir,
                                          "runphpscriptletonallwikis.py"),
                   "--scriptpath", self.scriptPath, "--scriptlet", inputText]
        result = self.runCommand(command, inputText, wiki)
        if not result:
            return None, None
        if not '\t' in result:
            commandString = ' '.join(command)
            sys.stderr.write("unexpected output from '%s'"
                             "(getting site and lang for %s)\n"
                             % (commandString, wiki))
            sys.stderr.write("output received was: %s\n" % result)
            return None, None
        site, lang = result.split('\t', 1)
        return site, lang

    def getUploadDir(self, wiki):
        """yay, someone is running this elsewhere. they get
        the nice wgUploadDirectory value, hope that's what they
        want."""
        inputText = "global $wgUploadDirectory; echo \"$wgUploadDirectory\";"
        command = ["python", "runphpscriptletonallwikis.py", "--scriptpath",
                   self.scriptPath, "--scriptlet", inputText]
        return self.runCommand(command, inputText, wiki)

    def runCommand(self, command, inputText, wiki):
        """run a generic command (per wiki) and whine as required
        or return the stripped output"""
        if self.multiversion:
            command.append("--multiversion")
        command.append(wiki)

        commandString = ' '.join(command)
        error = None
        try:
            proc = Popen(command, stdout=PIPE, stderr=PIPE)
            output, error = proc.communicate()
        except:
            sys.stderr.write("exception encountered running command %s for "
                             " wiki %s with error: %s\n" % (commandString,
                                                            wiki, error))
            return None
        if proc.returncode or error:
            sys.stderr.write("command %s failed with return code %s and "
                             "error %s\n" % (commandString,
                                             proc.returncode,  error))
            return None
        if not output or not output.strip():
            sys.stderr.write("No output from: '%s' (getting site and lang "
                             "for wiki %s)\n" % (commandString, wiki))
            return None
        return (output.strip())


def usage(message=None):
    if message:
        sys.stderr.write(message)
    usagemessage = """Usage: python listwikiuploaddirs.py --allwikis filename --scriptpath dir
                                   [multiversion] [closedwikis filename]
                                   [privatewikis filename] [skipwikis filename] [wmfhack]

This script dumps a list of media upload dirs for all specified wikis.
If names of closed and/or private wikis are provided, files in these lists
will be skipped.
Note that this produces an *absolute path* based on the value of
$wgUploadDirectory unless the 'wmfhack' option is specified, see below for that

--allwikis:      name of a file which contains all wikis to be processed,
                 one per line; if '-' is specified, the list will be read
                 from stdin
--scriptpath:    path to MWVersion.php, if multiversion option is set,
                  (see 'multiversion' below), or to Maintenance.php otherwise

Optional arguments:

--multiversion:  use the WMF multiversion het deployment infrastructure
--closedwikis:   name of a file which contains all closed wikis (these will be
                 skipped even if they are included in the allwikis file
--privatewikis:  name of a file which contains all private wikis (these will be
                 skipped even if they are included in the allwikis file
--skipwikis:     name of a file which contains other wikis to be skipped
                 even if they are included in the allwikis file
--wmfhack:       use $site/$lang to put together the upload dir; works for WMF
                 wikis only and it is a hack so you have been warned. Note that
                 this produces a path relative to the root of the WMF upload
                 directory
"""
    sys.stderr.write(usagemessage)
    sys.exit(1)

if __name__ == "__main__":
    closedWikis = []
    privateWikis = []
    skipWikis = []
    allWikis = None
    multiversion = False
    scriptPath = None
    wmfhack = False

    allWikisFile = closedWikisFile = privateWikisFile = skipWikisFile = None

    try:
        (options, rem) = getopt.gnu_getopt(sys.argv[1:], "",
                                           ["allwikis=", "closedwikis=",
                                            "privatewikis=", "scriptpath=",
                                            "skipwikis=", "multiversion", "wmfhack"])
    except:
        usage("Unknown option specified\n")

    for (opt, val) in options:
        if opt == "--allwikis":
            allWikisFile = val
        elif opt == "--closedwikis":
            closedWikisFile = val
        elif opt == "--privatewikis":
            privateWikisFile = val
        elif opt == "--multiversion":
            multiversion = True
        elif opt == "--scriptpath":
            scriptPath = val
        elif opt == "--skipwikis":
            skipWikisFile = val
        elif opt == "--wmfhack":
            wmfhack = True

    if len(rem) > 0:
        usage("Unknown option specified\n")

    if not allWikisFile or not scriptPath:
        usage("One or more mandatory options is missing\n")

    if closedWikisFile:
        fd = open(closedWikisFile, "r")
        closedWikis = [line.strip() for line in fd]
        fd.close()

    if privateWikisFile:
        fd = open(privateWikisFile, "r")
        privateWikis = [line.strip() for line in fd]
        fd.close()

    if skipWikisFile:
        fd = open(skipWikisFile, "r")
        skipWikis = [line.strip() for line in fd]
        fd.close()

    if allWikisFile == "-":
        fd = sys.stdin
    else:
        fd = open(allWikisFile, "r")
    wikiListTemp = [l.strip() for l in fd]
    wikiList = [l for l in wikiListTemp if l not in privateWikis
                and l not in closedWikis and l not in skipWikis]
    if fd != sys.stdin:
        fd.close()

    ud = UploadDir(multiversion, scriptPath, wmfhack)

    for w in wikiList:
        result = ud.getMediaDir(w)
        if result:
            print "%s\t%s" % (w, result)
