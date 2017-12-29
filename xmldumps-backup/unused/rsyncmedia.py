import os
import sys
import getopt
from subprocess import Popen, PIPE


def make_path(dir_list):
    dirs = filter(None, dir_list)
    if len(dirs) == 0:
        return None
    elif len(dirs) == 1:
        # this is ok even with 'None'
        return dirs[0]
    else:
        return os.path.join(*dirs)


class Rsyncer(object):
    def __init__(self, rsync_host, remote_base_dir, output_dir, verbose, dryrun):
        self.rsync_host = rsync_host
        self.remote_base_dir = remote_base_dir
        self.output_dir = output_dir
        self.verbose = verbose
        self.dryrun = dryrun
        self.dir_list = []

    def do_rsync(self, files_to_do, get_dir_list=False):

        command = ["rsync", "-rltDp"]
        if get_dir_list:
            if files_to_do:
                files_to_do_list = files_to_do.split('\n')
                if len(files_to_do_list) > 1:
                    sys.stderr.write("refusing to generate wanted "
                                     "dir list for multiple toplevel dirs %s\n"
                                     % files_to_do)
                    return
                # we want the first level of hash dirs (to see what
                # exists, so we can request only those)
                # but we don't want anything below that.
                exclude_levels = 3 + files_to_do_list[0].count('/')
                exclude_string = "/*" * exclude_levels
                command.extend(["-f", "- " + exclude_string])
            command.extend(["--list-only"])
            dryrun_saved = self.dryrun
            self.dryrun = False  # we don't actually change anything with --list-only so run it
        if files_to_do:
            command.extend(["--files-from", "-"])
        if self.rsync_host:
            command.extend([self.rsync_host + "::" + self.remote_base_dir, self.output_dir])
        else:
            # "remote" dir is accessible as a local filesystem
            command.extend([self.remote_base_dir, self.output_dir])

        # 23 = Partial transfer due to error
        # 24 = Partial transfer due to vanished source files
        # we can see these from rsync because 1) the source dir doesn't exist, for
        # small projects which now have media upload disabled, or 2) the file
        # about to be rsynced is deleted.  Since we will likely encounter
        # some of each type of error on every single run, log things
        # but don't bail

        if get_dir_list:
            result_unused, output = self.dir_list = self.do_command(
                command, files_to_do, [23, 24], display_output=False)
        else:
            result_unused, output = self.do_command(command, files_to_do, [23, 24])
        if get_dir_list:
            self.dryrun = dryrun_saved
        return output

    def do_command(self, command, input_to_command, return_codes_allowed, display_output=True):
        output = None
        command_string = " ".join(command)
        if self.dryrun:
            sys.stderr.write("would run commmand: ")
        elif self.verbose:
            sys.stderr.write("about to run command: ")
        if self.dryrun or self.verbose:
            sys.stderr.write(command_string)
            if input_to_command:
                sys.stderr.write("\nwith input: %s" % input_to_command)
            sys.stderr.write("\n")
        if self.dryrun:
            return 0, output

        try:
            error = None
            proc = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, error = proc.communicate(input_to_command)
            if proc.returncode and proc.returncode not in return_codes_allowed:
                sys.stderr.write("command '%s failed with return code %s "
                                 "and error %s\n" % (command, proc.returncode, error))
                # we don't bail here, let the caller decide what to do about it"
        except:
            sys.stderr.write("command %s failed\n" % command)
            if error:
                sys.stderr.write("%s\n" % error)
            # the problem is probably serious enough that we should refuse to do further processing
            raise
        if output and display_output:
            print output
        if error:
            if error:
                sys.stderr.write("%s\n" % error)
        return proc.returncode, output


class RsyncProject(object):
    def __init__(self, rsyncer, wiki, wtype, wikidir):
        self.rsyncer = rsyncer
        self.wiki = wiki
        self.wtype = wtype
        self.wikidir = wikidir

    def do_rsync(self):

        if self.wtype == "huge":
            # do all 256 shards separately
            self.do_huge_rsync()

        elif self.wtype == "big":
            # do the top 16 shards separately
            self.do_big_rsync()
        else:
            # do the whole thing at once
            self.do_normal_rsync()

    def get_files_from(self, hashdir=None, subdir=None):
        """get list of directories for rsync that will
        be fed to the "--files-from -" option"""
        return make_path([self.wikidir, hashdir, subdir])

    def do_huge_rsync(self):
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            sys.stderr.write("doing 256 separate shards for wiki %s\n" % self.wiki)

        dirs = ["0", "1", "2", "3", "4", "5", "6", "7", "8",
                "9", "a", "b", "c", "d", "e", "f"]
        subdirs = ["0", "1", "2", "3", "4", "5", "6", "7", "8",
                   "9", "a", "b", "c", "d", "e", "f"]
        for dname in dirs:
            for subdname in subdirs:
                files_from = self.get_files_from(dname, dname+subdname)
                self.rsyncer.do_rsync(files_from)
        # now get the archive dir
        for dname in dirs:
            files_from = self.get_files_from("archive", dname)
            self.rsyncer.do_rsync(files_from)

    def do_big_rsync(self):
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            sys.stderr.write("doing 16 separate shards for wiki %s\n" % self.wiki)

        dirs = ["0", "1", "2", "3", "4", "5", "6", "7", "8",
                "9", "a", "b", "c", "d", "e", "f", "archive"]
        for dname in dirs:
            files_from = self.get_files_from(dname)
            self.rsyncer.do_rsync(files_from)

    def do_normal_rsync(self):
        # for anything not big or huge, get list of media dirs that
        # the wiki has, this will be the list of dirs we want
        if self.rsyncer.verbose or self.rsyncer.dryrun:
            sys.stderr.write("retrieving dir list for wiki %s\n" % self.wiki)
        dirs_found = self.rsyncer.do_rsync(
            self.wikidir, get_dir_list=True)

        # explicitly list the 17 dirs we want
        dirs_wanted = [make_path([self.wikidir, d])
                       for d in ["0", "1", "2", "3", "4", "5", "6", "7", "8",
                                 "9", "a", "b", "c", "d", "e", "f", "archive"]]
        # filter out the ones not in dirList, keeps rsync from
        # whining about nonexistent dirs

        # format of the returned lines is
        # drwxrwxr-x        4096 2012/04/06 10:45:34 blahblah/8
        files_from = [f.rsplit(None, 1)[1] for f in dirs_found.split('\n') if '/' in f]
        files_from = "\n".join([f for f in files_from if f in dirs_wanted])
        if files_from:
            if self.rsyncer.verbose or self.rsyncer.dryrun:
                sys.stderr.write("doing 1 shard for wiki %s\n" % self.wiki)
            self.rsyncer.do_rsync(files_from)
        else:
            if self.rsyncer.verbose or self.rsyncer.dryrun:
                sys.stderr.write("skipping wiki %s, no dirs to sync\n" % self.wiki)


def usage(message=None):
    if message:
        sys.stderr.write("%s\n" % message)
    usage_message = """
Usage: python rsyncmedia.py [--remotehost hostname] --remotedir dirname
                      --localdir dirname --wikilist filename
                      [--big wiki1,wiki2,...] [--huge wiki3,wiki4,...]
                      [--verbose] [--dryrun]

This script rsyncs media from a primary media host. getting only media
publically available (no deleted images, no data from private wikis)
and skipping thumbs, math, timeline, temp, old and misc other directories
that may have been created over time.

--remotehost:    hostname of the remote host form which we are rsyncing.
                 if this option is ommited, the remotedir option is assumed
                 to refer to a local filesystem (for example nfs-mounted)
--remotedir:     path to point in remote directory in which media for the
                 wiki(s) are stored; this path is relative to the rsync root.
--localdir:      path to root of local directory tree in which media for
                 the wiki(s) will be copied.
--wikilist       filename which contains names of the wiki databases and their
                 corresponding media upload directories,  one wiki per line,
                 line, to be rsynced. The wikiname and the directory should be
                 separated by a tab character.  If '-' is given as the name
                 wiki db names and directories will be read from stdin.
--big            comma-separated list of wiki db names which have enough media
                 that we should rsync them in 16 batches, one per subdir
                 instead of all at once.
--huge           comma-separated list of wiki db names which have enough media
                 that we should rsync them in 256 batches, one per 2nd level
                 subdir instead of all at once.
--verbose:       print lots of status messages.
--dryrun:        don't do the rsync, print what would be done.

wiki             name of wikidb for rsync; if specified, this will override
                 any file given for 'wikilist'.
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_comma_sep_list(text):
    if text:
        if ',' in text:
            result = text.split(',')
        else:
            result = [text]
    else:
        result = []
    return result


def do_main():
    remote_dir = None
    rsync_host = None
    local_dir = None
    big = None
    huge = None
    wiki_list_file = None
    verbose = False
    dryrun = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ["big=", "huge=", "localdir=", "remotedir=",
                               "remotehost=", "localdir=", "wikilist=",
                               "verbose", "dryrun"])
    except Exception:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--remotedir":
            remote_dir = val
        elif opt == "--remotehost":
            rsync_host = val
        elif opt == "--localdir":
            local_dir = val
        elif opt == "--big":
            big = val
        elif opt == "--huge":
            huge = val
        elif opt == "--wikilist":
            wiki_list_file = val
        elif opt == "--verbose":
            verbose = True
        elif opt == "--dryrun":
            dryrun = True

    if len(remainder) > 0:
        usage("Unknown option specified")

    if not remote_dir or not local_dir or not wiki_list_file:
        usage("One or more mandatory options missing")

    if wiki_list_file == "-":
        fdesc = sys.stdin
    else:
        fdesc = open(wiki_list_file, "r")
    wiki_list = [line.strip() for line in fdesc]

    if fdesc != sys.stdin:
        fdesc.close()

    # eg enwiki
    big_wikis = get_comma_sep_list(big)
    # eg commonswiki
    huge_wikis = get_comma_sep_list(huge)

    rsyncer = Rsyncer(rsync_host, remote_dir, local_dir, verbose, dryrun)

    for winfo in wiki_list:
        # first skip blank lines and comments
        if not winfo or winfo[0] == '#':
            continue
        if '\t' not in winfo:
            sys.stderr.write("unexpected line with no tab in wikilist: %s\n" % winfo)
            continue

        # expect <wikiname>\t<directory>
        wikiname, wikidir = winfo.split('\t', 1)

        if wikiname in huge_wikis:
            wtype = "huge"
        elif wikiname in big_wikis:
            wtype = "big"
        else:
            wtype = "normal"

        rsync_proj = RsyncProject(rsyncer, wikiname, wtype, wikidir)
        rsync_proj.do_rsync()


if __name__ == "__main__":
    do_main()
