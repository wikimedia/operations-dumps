'''
for every wiki, run a specified maintenance script
with gz or bz2 compression of the output as desired,
output to the specified directory in a subdirectory
for the given date
'''

import getopt
import os
import re
import sys
import time
import traceback

from dumps.WikiDump import Config, Wiki
from dumps.utils import MultiVersion, TimeUtils, RunSimpleCommand
from dumps.exceptions import BackupError


class ScriptRunner(object):
    '''
    methods for handling maintenance scripts
    '''
    def __init__(self, scriptname, args, dryrun, verbose):
        self.scriptname = scriptname
        self.args = args
        self.dryrun = dryrun
        self.verbose = verbose

    def get_script_command(self, wiki, output_dir, outfile_base):
        '''
        given the output directory and filename and the wiki
        object, put together and return an array consisting
        of the script name, args, and any multiversion
        invocations that need to precede it
        '''
        script_command = MultiVersion.mw_script_as_array(
            wiki.config, self.scriptname)
        script_command = [wiki.config.php, "-q"] + script_command
        script_command.extend(["--wiki", wiki.db_name])
        if self.args is not None:
            script_command.extend(self.args)
        script_command = [field.format(
            DIR=output_dir, FILE=outfile_base) for field in script_command]
        return script_command

    def run_script(self, wiki, filenameformat, output_dir, overwrite):
        '''
        run a (maintenance) script on one wiki, expecting relevant output to
        go to a file
        '''
        outfile_base = filenameformat.format(
            w=wiki.db_name, d=wiki.date, s=self.scriptname)
        outfile_path = os.path.join(output_dir, outfile_base)
        if not overwrite and os.path.exists(outfile_path):
            # don't overwrite existing file, just return a happy value
            if self.verbose:
                print ("Skipping wiki %s, file %s exists already"
                       % (wiki.db_name, outfile_path))
            return True

        script_command = self.get_script_command(wiki, output_dir,
                                                 outfile_base)
        return RunSimpleCommand.run_with_output(
            script_command, maxtries=1, shell=False, verbose=self.verbose)


class WikiRunner(object):
    '''
    methods for running a script once on one wiki
    '''
    def __init__(self, scriptrunner, wiki,
                 filenameformat, output_dir):
        self.wiki = wiki
        self.wiki.config.parse_conffile_per_project(wiki.db_name)
        self.scriptrunner = scriptrunner
        self.filenameformat = filenameformat
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.output_dir = output_dir

    def get_output_dir(self):
        '''
        return the path to the directory where script output will
        be stashed
        '''
        return self.output_dir.format(
            w=self.wiki.db_name, d=self.wiki.date,
            s=self.scriptrunner.scriptname)

    def do_one_wiki(self, overwrite):
        """returns true on success"""
        if (self.wiki.db_name not in self.wiki.config.private_list and
                self.wiki.db_name not in self.wiki.config.closed_list and
                self.wiki.db_name not in self.wiki.config.skip_db_list):
            try:
                if self.scriptrunner.verbose:
                    print "Doing run for wiki: ", self.wiki.db_name
                if not self.scriptrunner.dryrun:
                    if not self.scriptrunner.run_script(
                            self.wiki, self.filenameformat,
                            self.get_output_dir(), overwrite):
                        return False
            except Exception:
                if self.scriptrunner.verbose:
                    traceback.print_exc(file=sys.stdout)
                return False
        if self.scriptrunner.verbose:
            print "Success!  Wiki", self.wiki.db_name, "script complete."
        return True


class WikiRunnerLoop(object):
    '''
    methods for running a script across all wikis, with retries
    '''
    def __init__(self, config, scriptrunner, filenameformat,
                 output_dir):
        self.config = config
        self.scriptrunner = scriptrunner
        self.output_dir = output_dir
        self.filenameformat = filenameformat
        self.wikis_todo = self.config.db_list

    def do_all_wikis(self, overwrite, date):
        '''
        run a script on all wikis, removing the completed wikis
        from the todo list in case the caller wants to retry the rest
        '''
        for wiki_name in self.wikis_todo[:]:
            wiki = Wiki(self.config, wiki_name)
            wiki.set_date(date)
            runner = WikiRunner(self.scriptrunner,
                                wiki, self.filenameformat,
                                self.output_dir)
            if runner.do_one_wiki(overwrite):
                self.wikis_todo.remove(wiki_name)

    def do_all_wikis_til_done(self, num_fails, overwrite, date):
        """Run through all wikis, retrying up to numFails
        times in case of error"""
        if not date:
            date = TimeUtils.today()
        fails = 0
        while 1:
            self.do_all_wikis(overwrite, date)
            if not len(self.wikis_todo):
                break
            fails = fails + 1
            if fails > num_fails:
                raise BackupError("Too many failures, giving up")
            # wait 5 minutes and try another loop
            time.sleep(300)


def usage(message=None):
    '''
    display a usage message with an optional explanatory
    message first
    '''
    if message:
        sys.stderr.write(message + "\n")
    usage_message = """Usage: python wikiqueries.py [options] [script-args]

Args following the options will be treated as arguments to the script and
passed on.  The strings {DIR} and {FILE}, if they occur in any argument,
will be replaced by the output directory and the expanded output filename,
respectively.

Example:

python onallwikis.py -c confs/wikidump.conf   \\
    -f "{w}-{d}-testing"                      \\
    -o `pwd`                                  \\
    -s generateSitemap.php                    \\
    --retries 1 --nooverwrite                 \\
    --verbose -- --fspath "{DIR}/{FILE}"

Note that because this extension uses fspath to create a subdirectory
instead of a file, the output lands in {DIR}{FILE}/variousfiles.gz; it's up
to the enduser to make sure the terms {DIR} and {FILE} are used properly
in the script args.

Options:

--configfile     (c): Specify config file to read
                      Default: wikiqueries.conf
--date           (d): Date that will appear in filename and/or dirname as
                      specified, in the format: YYYYMMDD.  If not specified,
                      today's date will be used.
--filenameformat (f): Format string for the name of each file, with {w} for
                      wikiname, optional {d} for date and {s} for script.
                      Default: {w}-{d}-<scriptname>
--outdir         (o): Put output files for all projects in this directory;
                      it will be created if it does not exist.  Accepts
                      '{w}', '{d}', '{s}' for substituting wikiname, date,
                      and/or script into the name.
--script         (s): Path of script to run on each project.
--wikiname       (w): Run the query only for the specific wiki
--retries        (r): Number of times to try running the query on all wikis
                      in case of error, before giving up
                      Default: 3
--nooverwrite    (n): Do not overwrite existing file of the same name, skip
                      run for the specific wiki
--dryrun         (D): Don't execute commands, show the commands that would
                      be run
--verbose        (v): Print various informative messages
--help           (h): Display this message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def validate_args(date, output_dir, retries):
    '''
    check specified args for validity, whining
    and bailing if the values have problems
    '''
    if date and not re.match("^20[0-9]{6}$", date):
        usage("Date must be in the format YYYYMMDD"
              " (four digit year, two digit month, two digit date)")

    if not output_dir:
        usage("Mandatory argument 'outputdir' was not specified.")

    if not retries.isdigit():
        usage("A positive number must be specified for retries.")


def get_args():
    '''
    get and return command line args
    '''
    configfile = False
    dryrun = False
    date = None
    output_dir = None
    overwrite = True
    script = None
    retries = None
    wikiname = None
    verbose = False
    filenameformat = "{w}-{d}-{s}"

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:d:f:o:w:s:r:nDvh",
            ['configfile=', 'date=', 'filenameformat=',
             'outdir=', 'script=', 'retries=', 'wiki=',
             'dryrun', 'nooverwrite', 'verbose', 'help'])

    except getopt.GetoptError as err:
        print str(err)
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-d", "--date"]:
            date = val
        elif opt in ["-f", "--filenameformat"]:
            filenameformat = val
        elif opt in ["-o", "--outdir"]:
            output_dir = val
        elif opt in ["-w", "--wiki"]:
            wikiname = val
        elif opt in ["-s", "--script"]:
            script = val
        elif opt in ["-r", "--retries"]:
            retries = val
        elif opt in ["-n", "--nooverwrite"]:
            overwrite = False
        elif opt in ["-D", "--dryrun"]:
            dryrun = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script:")

    return(configfile, date, dryrun, filenameformat,
           output_dir, overwrite, wikiname, script,
           retries, verbose, remainder)


def do_main():
    '''
    main entry point, do all the work
    '''

    (configfile, date, dryrun, filenameformat,
     output_dir, overwrite, wikiname, script,
     retries, verbose, remainder) = get_args()

    validate_args(date, output_dir, retries)

    if retries is None:
        retries = "3"
    retries = int(retries)

    if configfile:
        config = Config(configfile)
    else:
        config = Config()

    if date is None:
        date = TimeUtils.today()

    scriptrunner = ScriptRunner(script, remainder, dryrun, verbose)

    if wikiname is not None:
        wiki = Wiki(config, wikiname)
        wiki.set_date(date)
        wikirunner = WikiRunner(scriptrunner, wiki, filenameformat,
                                output_dir)
        wikirunner.do_one_wiki(overwrite)
    else:
        wikirunner = WikiRunnerLoop(config, scriptrunner, filenameformat,
                                    output_dir)
        wikirunner.do_all_wikis_til_done(retries, overwrite, date)

if __name__ == "__main__":
    do_main()
