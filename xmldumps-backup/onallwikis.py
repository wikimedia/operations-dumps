#!/usr/bin/python3
'''
for every wiki, run a specified maintenance script
or mysql query, output to the specified directory
with filename both of which may vary by wiki name
and date
'''

import getopt
import os
import re
import sys
import time
import traceback

from dumps.wikidump import Config, Wiki
from dumps.utils import MultiVersion, TimeUtils, RunSimpleCommand, DbServerInfo
from dumps.exceptions import BackupError
from dumps.fileutils import FileUtils


class Runner():
    '''
    base classs for script, query or other runners on a specified
    wiki, no retries, option to skip wiki run if output already exists
    '''
    def __init__(self, dryrun, verbose):
        self.dryrun = dryrun
        self.verbose = verbose

    def skip_if_done(self, wiki, filenameformat, output_dir, overwrite):
        '''
        set up and return output dir and filenames
        if overwrite is not True, check to see if the output
        file already exists, and if it does, display a
        warning and return None, None instead
        '''
        outfile_base = filenameformat.format(
            w=wiki.db_name, d=wiki.date,)
        outfile_path = os.path.join(output_dir, outfile_base)
        if not overwrite and os.path.exists(outfile_path):
            # don't overwrite existing file, just return a happy value
            if self.verbose:
                print("Skipping wiki %s, file %s exists already"
                      % (wiki.db_name, outfile_path))
            return None, None
        return (outfile_base, outfile_path)

    def run(self, wiki, filenameformat, output_dir, overwrite, base):
        '''
        implement this in your subclass
        '''
        raise NotImplementedError


class ScriptRunner(Runner):
    '''
    maintenance script runner for a specific wiki
    '''
    def __init__(self, scriptname, args, dryrun, verbose):
        self.scriptname = scriptname
        self.args = args
        super().__init__(dryrun, verbose)

    def get_command(self, wiki, output_dir, outfile_base, base):
        '''
        given the output directory and filename and the wiki
        object, put together and return an array consisting
        of the script name, args, and any multiversion
        invocations that need to precede it
        '''
        if base is None:
            base = wiki
        if self.scriptname.endswith('.php'):
            script_command = MultiVersion.mw_script_as_array(
                base.config, self.scriptname)
            script_command = [base.config.php] + script_command
            script_command.extend(["--wiki", base.db_name])
        else:
            script_command = [self.scriptname]
        if self.args is not None:
            script_command.extend(self.args)
        script_command = [field.format(DIR=output_dir, FILE=outfile_base, w=wiki.db_name)
                          for field in script_command]
        return script_command

    def run(self, wiki, filenameformat, output_dir, overwrite, base=None):
        '''
        run a (maintenance) script on one wiki, expecting relevant output to
        go to a file
        '''
        filenameformat = filenameformat.replace('{d}', '{{d}}')
        filenameformat = filenameformat.replace('{w}', '{{w}}')
        filenameformat = filenameformat.format(s=self.scriptname)
        (outfile_base, outfile_path) = self.skip_if_done(
            wiki, filenameformat, output_dir, overwrite)
        if outfile_base is None:
            return True
        command = self.get_command(wiki, outfile_path, outfile_base, base)
        if self.dryrun:
            print("Would run:", command)
            return True
        return RunSimpleCommand.run_with_output(
            command, maxtries=1, shell=False)


class QueryRunner(Runner):
    '''
    mysql query runner for a specific wiki
    '''
    def __init__(self, query, dryrun, verbose):
        self.query = query
        super().__init__(dryrun, verbose)

    def get_command(self, wiki, outfile_path, outfile_base, base):
        '''
        given the output directory and filename and the wiki
        object, put together and return a command string
        for mysql to run the query and dump the output
        where required.
        '''
        if base is None:
            base = wiki

        dbserver = DbServerInfo(base, base.db_name)

        if outfile_base.endswith(".gz"):
            compress = "gzip"
        elif outfile_base.endswith(".bz2"):
            compress = "bzip2"
        else:
            compress = ""
        pipeto = "%s > %s" % (compress, outfile_path)

        query = self.query.format(w=wiki.db_name)
        return dbserver.build_sql_command(query, pipeto)

    def run(self, wiki, filenameformat, output_dir, overwrite, base=None):
        '''
        run a (maintenance) script on one wiki, expecting relevant output to
        go to a file
        '''
        (outfile_base, outfile_path) = self.skip_if_done(
            wiki, filenameformat, output_dir, overwrite)
        if outfile_base is None:
            return True

        command = self.get_command(wiki, outfile_path,
                                   outfile_base, base)

        if not isinstance(command, str):
            # see if the list elts are lists that need to be turned into strings
            command = [element if isinstance(element, str)
                       else ' '.join(element) for element in command]
            command = '|'.join(command)
        if self.dryrun:
            print("Would run:", command)
            return True
        return RunSimpleCommand.run_with_no_output(
            command, maxtries=1, shell=True, verbose=self.verbose)


class WikiRunner():
    '''
    methods for running a script once on one wiki
    '''
    def __init__(self, runner, wiki,
                 filenameformat, output_dir,
                 base):
        self.wiki = wiki
        self.runner = runner
        self.filenameformat = filenameformat
        self.output_dir = output_dir
        self.base = base

    def get_output_dir(self):
        '''
        return the path to the directory where script output will
        be stashed
        '''
        return self.output_dir.format(
            w=self.wiki.db_name, d=self.wiki.date)

    def do_one_wiki(self, overwrite):
        """returns true on success"""
        if (self.wiki.db_name not in self.wiki.config.private_list and
                self.wiki.db_name not in self.wiki.config.closed_list and
                self.wiki.db_name not in self.wiki.config.skip_db_list):
            try:
                if self.runner.verbose:
                    if self.base is not None:
                        print("Doing run for wiki: ", self.wiki.db_name, "on", self.base.db_name)
                    else:
                        print("Doing run for wiki: ", self.wiki.db_name)
                if not self.runner.dryrun:
                    if not os.path.exists(self.get_output_dir()):
                        os.makedirs(self.get_output_dir())
                if not self.runner.run(
                        self.wiki, self.filenameformat,
                        self.get_output_dir(), overwrite,
                        self.base):
                    return False
            except Exception as ex:
                if self.runner.verbose:
                    traceback.print_exc(file=sys.stdout)
                return False
        if self.runner.verbose:
            print("Success!  Wiki", self.wiki.db_name, "Run complete.")
        return True


class WikiRunnerLoop():
    '''
    methods for running a script across all wikis, with retries
    '''
    def __init__(self, config, runner, filenameformat,
                 output_dir, base):
        self.config = config
        self.runner = runner
        self.output_dir = output_dir
        self.base = base
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
            runner = WikiRunner(self.runner,
                                wiki, self.filenameformat,
                                self.output_dir, self.base)
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
            if not self.wikis_todo:
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
    usage_message = """Usage: python3 onallwikis.py [options] [script-args]

This script runs a mysql query or a php maintenance script or any
other specified script across all wikis, stashing the outputs in files
in the directory(ies) specified.

Scripts ending in '.php' will be treated as MediaWiki maintenance scripts
and will be run via MW multiversion lookups. The --wiki argument to
those scripts will be handled automatically, as will the invocation of
php itself.

For all other scripts, the full command line must be given. It's best
to specify -s /path/to/interpreter /path/to/script -- --all --other --args...
at the end of the argument list to onallwikis.

Args following the options will be treated as arguments to the script if
a script is to be run rather than a query, and they will be passed on.
The strings {DIR}, {FILE} and {w}, if they occur in any argument, will be
replaced by the output directory, the expanded output filename and the
wiki name, respectively.

Filenames ending in .gz or .bz2 will result in compression of that type
of query results, if a query is run.

Examples:

python3 onallwikis.py -c confs/wikidump.conf   \\
    -f "{d}"                                  \\
    -o `pwd`                                  \\
    -s generateSitemap.php                    \\
    --retries 1                               \\
    --verbose -- --fspath "{DIR}"

Note that because the extension uses fspath to create a subdirectory
instead of a file, the output lands in {DIR}/variousfiles.gz; it's up
to the enduser to make sure the terms {DIR} and {FILE} are used properly
in the script args.

python3 onallwikis.py -c confs/wikidump.conf                    \\
    -f "{w}-{d}-all-titles-in-ns-0.gz"                         \\
    -o '/home/tester/wmf/dumps/xmldumps/{d}'                   \\
    -q "'select page_title from page where page_namespace=0;'" \\
    --retries 1 --nooverwrite                                  \\
    --verbose

Options:

--configfile     (c): Specify config file to read
                      Default: wikiqueries.conf
--date           (d): Date that will appear in filename and/or dirname as
                      specified, in the format: YYYYMMDD.  If not specified,
                      today's date will be used.
--filenameformat (f): Format string for the name of each file, with {w} for
                      wikiname, optional {d} for date and, if a script is to
                      be run, {s} for the scriptname.
                      Default: {w}-{d}-{s}
--outdir         (o): Put output files for all projects in this directory;
                      it will be created if it does not exist.  Accepts
                      '{w}' and '{d}' for substituting wikiname and/or date
                      into the name.
--script         (s): Path of script to run on each project.
--query          (q): MySQL query to be run on each project, if no script is
                      specified to run.  If no script is specified and this
                      option is also not supplied, the 'queryfile' option
                      in the config file will be checked and that file read
                      for the contents of the query.
                      Query string may have '{w}' in it for substituting the
                      wikiname.
--wiki           (w): Run the script/query only for the specific wiki
--base           (b): Use the specified wiki as a base: run the script/query
                      from there.  In this case as wikis are looped through
                      they will only be used to modify script args or the
                      query string supplied, as well as the output directory
                      and filenames; if the 'wiki' argument is given, then
                      the wikis looped through will be just the wiki supplied
                      to that argument.
                      This permits you to e.g. run a script on a media repo
                      which dumps all image names in use on each wiki.
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


def validate_args(date, output_dir, retries, script, query):
    '''
    check specified args for validity, whining
    and bailing if the values have problems
    '''
    if date and not re.match("^20[0-9]{6}$", date):
        usage("Date must be in the format YYYYMMDD"
              " (four digit year, two digit month, two digit date)")

    if not output_dir:
        usage("Mandatory argument 'outputdir' was not specified.")

    if retries is not None and not retries.isdigit():
        usage("A positive number must be specified for retries.")

    if script is not None and query is not None:
        usage("Only one of the 'script' or 'query' options may be specified.")


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
    query = None
    retries = None
    wikiname = None
    basename = None
    verbose = False
    filenameformat = "{w}-{d}-{s}"

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:d:f:o:w:b:s:q:r:nDvh",
            ['configfile=', 'date=', 'filenameformat=',
             'outdir=', 'script=', 'query=', 'retries=',
             'wiki=', 'base=', 'dryrun', 'nooverwrite',
             'verbose', 'help'])

    except getopt.GetoptError as err:
        print(str(err))
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
        elif opt in ["-b", "--base"]:
            basename = val
        elif opt in ["-s", "--script"]:
            script = val
        elif opt in ["-q", "--query"]:
            query = val
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

    return (configfile, date, dryrun, filenameformat,
            output_dir, overwrite, wikiname, script,
            basename, query, retries, verbose, remainder)


def do_main():
    '''
    main entry point, do all the work
    '''

    (configfile, date, dryrun, filenameformat,
     output_dir, overwrite, wikiname, script,
     basename, query, retries, verbose, remainder) = get_args()

    validate_args(date, output_dir, retries, script, query)

    if retries is None:
        retries = "3"
    retries = int(retries)

    if configfile:
        config = Config(configfile)
    else:
        config = Config()

    if date is None:
        date = TimeUtils.today()

    if script is not None:
        runner = ScriptRunner(script, remainder, dryrun, verbose)
    else:
        if query is None:
            query = FileUtils.read_file(config.queryfile)
        runner = QueryRunner(query, dryrun, verbose)

    if basename is not None:
        base = Wiki(config, basename)
        base.set_date(date)
    else:
        base = None

    if wikiname is not None:
        wiki = Wiki(config, wikiname)
        wiki.set_date(date)
        wikirunner = WikiRunner(runner, wiki, filenameformat,
                                output_dir, base)
        wikirunner.do_one_wiki(overwrite)
    else:
        wikirunner = WikiRunnerLoop(config, runner, filenameformat,
                                    output_dir, base)
        wikirunner.do_all_wikis_til_done(retries, overwrite, date)


if __name__ == "__main__":
    do_main()
