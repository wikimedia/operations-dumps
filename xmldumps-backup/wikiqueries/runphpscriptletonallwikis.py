import os
import sys
import getopt
import subprocess
from subprocess import Popen, PIPE


class PhpRunner(object):
    """Run a maintenance 'scriptlet' on all wikis
    The maintenance class framework is set up already;
    the caller should supply a few lines of code that would
    go into the execute function."""
    def __init__(self, scriptPath, phpBody, multiversion, wiki):
        self.scriptPath = scriptPath
        self.phpBody = phpBody
        self.multiversion = multiversion
        self.wiki = wiki
        pass

    def runPhpScriptlet(self):
        command = ["php", "--", "--wiki=%s" % self.wiki]
        return self.runCommand(command)

    def runCommand(self, command):
        result = True
        try:
            proc = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, error = proc.communicate(self.getPhpCode())
            if proc.returncode:
                # don't barf, let the caller decide what to do
                sys.stderr.write("command '%s failed with return code %s and "
                                 "error %s\n" % (command,
                                                 proc.returncode,  error))
                result = False
            print output
            if error:
                sys.stderr.write(error + '\n')
        except:
            sys.stderr.write("command %s failed\n" % command)
            raise
        return result

    def getPhpCode(self):
        if multiversion:
            phpSetup = ("require_once( '%s/MWVersion.php' ); "
                        "$dir = getMediaWikiCli(''); "
                        "require_once( \"$dir/maintenance/Maintenance.php\" );"
                        % self.scriptPath)
        else:
            phpSetup = ("require_once( '%s/Maintenance.php' );"
                        % self.scriptPath)
        return "<?php\n" + phpSetup + self.fillinScriptletTemplate()

    def fillinScriptletTemplate(self):
        return """
class MaintenanceScriptlet extends Maintenance {
    public function __construct() {
        parent::__construct();
    }
    public function execute() {
    %s
    }
}
$maintClass = "MaintenanceScriptlet";
require_once( RUN_MAINTENANCE_IF_MAIN );
""" % self.phpBody


def usage(message):
    if message:
        sys.stderr.write(message + '\n')
    usagemessage = """Usage: runphpscriptletonallwikis.py --scriptpath path
                 [--wikilist value] [--multiversion] [--multiversion]
                 [--scriptlet text] [--scriptletfile filename] [wikiname]

Options:

--scriptpath:    path to MWVersion.php, if multiversion option is set,
                 or to maintenance/Maintenance.php, otherwise
--wikilist:        path to list of wiki database names one per line
                 if filename is '-' then the list will be read from stdin
--multiversion:  use the WMF multiversion het deployment infrastructure
--scriptlet:     the php code to run, if not provided in a file
--scriptletfile: a filename from which to read the php code to run

Arguments:

wikiname:  name of wiki to process, if specified overrides wikilist

Example:
python runphpscriptletonallwikis.py enwiki
"""
    sys.stderr.write(usagemessage)
    sys.exit(1)

if __name__ == "__main__":
    wikiListFile = None
    scriptPath = None
    scriptlet = None
    scriptletFile = None
    multiversion = False
    wiki = None

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
                                                 ["wikilist=", "multiversion",
                                                  "scriptpath=", "scriptlet=",
                                                  "scriptletfile="])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--wikilist":
            wikiListFile = val
        elif opt == "--multiversion":
            multiversion = True
        elif opt == "--scriptpath":
            scriptPath = val
        elif opt == "--scriptlet":
            scriptlet = val
        elif opt == "--scriptletfile":
            scriptletFile = val

    if len(remainder) > 0:
        if len(remainder) > 1 or remainder[0].startswith("--"):
            usage("Unknown option specified")
        wiki = remainder[0]

    if (not wiki and not wikiListFile) or not scriptPath:
        usage("One of wiki or wikilist must be specified")

    if not scriptlet and not scriptletFile:
        usage("One of scriptlet or scriptletfile must be specified")

    if scriptlet and scriptletFile:
        usage("Only one of scriptlet or scriptletfile may be specified")

    if wiki:
        wikiList = [wiki]
    else:
        if wikiListFile == "-":
            fd = sys.stdin
        else:
            fd = open(wikiListFile, "r")
        wikiList = [line.strip() for line in fd]

        if fd != sys.stdin:
            fd.close()

    if scriptletFile:
        fd = open(scriptletFile, "r")
        scriptlet = fd.read()
        fd.close()

    fails = 0
    for w in wikiList:
        pr = PhpRunner(scriptPath, scriptlet, multiversion, w)
        if not pr.runPhpScriptlet():
            fails += 1
    if fails:
        sys.stderr.write("%s job(s) failed, see output for details.\n" % fails)
