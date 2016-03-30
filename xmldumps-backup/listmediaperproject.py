import os
import sys
import getopt
import time
from subprocess import Popen, PIPE
from dumps.WikiDump import Config


def get_file_name_format(phase):
    return "{w}-{d}-" + phase + "-wikiqueries.gz"


class MediaPerProject(object):
    def __init__(self, conf, output_dir, remote_repo_name,
                 verbose, wq_config_file, wq_path, overwrite, wiki=None):
        self.conf = conf
        self.output_dir = output_dir
        self.remote_repo_name = remote_repo_name
        self.verbose = verbose
        self.date = time.strftime("%Y%m%d", time.gmtime())
        self.file_name_format = "{w}-{d}-wikiqueries.gz"
        self.wq_config_file = wq_config_file
        self.wq_path = wq_path
        self.overwrite = overwrite
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if wiki is not None:
            self.wikis_to_do = [wiki]
        else:
            self.wikis_to_do = [w for w in self.conf.db_list
                                if w not in self.conf.private_list and
                                w not in self.conf.closed_list and
                                w not in self.conf.skip_db_list]

    def write_local_media(self):
        if self.verbose:
            print "Starting round one wikiqueries for image table"
        if len(self.wikis_to_do) == 1:
            wiki = self.wikis_to_do[0]
        else:
            wiki = None
        self.do_wiki_queries('select img_name, img_timestamp from image',
                             get_file_name_format("local"), wiki)
        if self.verbose:
            print "Done round one!!"

    def do_wiki_queries(self, query, file_name_format, wiki=None):
        if not os.path.exists(self.wq_config_file):
            print "config file  %s does not exist" % self.wq_config_file
            sys.exit(1)
        command = ["python", self.wq_path, "--configfile", self.wq_config_file,
                   "--query", query, "--outdir", self.output_dir,
                   "--filenameformat", file_name_format]
        if self.verbose:
            command.append("--verbose")
        if not self.overwrite:
            command.append("--nooverwrite")
        if wiki:
            command.append(wiki)
        command_string = " ".join(["'" + c + "'" for c in command])

        if self.verbose:
            print "About to run wikiqueries:", command_string
        try:
            proc = Popen(command, stderr=PIPE)
            output_unused, error = proc.communicate()
            if proc.returncode:
                print ("command '%s failed with return code %s and error %s"
                       % (command, proc.returncode, error))
                sys.exit(1)
        except:
            print "command %s failed" % command
            raise

    def write_remote_media(self):
        if self.verbose:
            print "Starting round two wikiqueries for global image links table"

        for wiki in self.wikis_to_do:
            if wiki == self.remote_repo_name:
                if self.verbose:
                    print "Skipping", wiki, "because it's the remote repo"
            else:
                if self.verbose:
                    print "Doing db", wiki
                self.do_wiki_queries('select gil_to from globalimagelinks'
                                     ' where gil_wiki= "%s"' % wiki,
                                     get_file_name_format("remote").format(
                                         w=wiki, d='{d}'), self.remote_repo_name)
        if self.verbose:
            print "Done round two!!"


def usage(message=None):
    if message:
        sys.stderr.write(message + "\n")

    usage_message = """Usage: python listmediaperproject.py --outputdir dirname
                  [--remoterepo reponame] [--localonly] [--remoteonly]
                  [--verbose] [--wqconfig filename] [wqpath filename] [wiki]

This script produces a list of media files in use on the local wiki stored on a
remote repo (e.g. commons).

--outputdir:      where to put the list of remotely hosted media per project
--remotereponame: name of the remote repo that houses media for projects
                  default: 'commonswiki'
--nooverwrite:    if run for the same wiki(s) on the same date, don't overwrite
                  existing files
--verbose:        print lots of status messages
--wqconfig:       relative or absolute path of wikiquery config file
                  default: wikiqueries.conf
--wqpath:         relative or absolute path of the wikiqieries python script
                  default: wikiqueries.py
--localonly:      only generate the lists of local media (first half of run)
--remoteonly:     only generate the lists of remotely hosted media (second half
                  of run)
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    output_dir = None
    remote_repo_name = "commonswiki"
    verbose = False
    wiki = None
    remote_only = False
    local_only = False
    # by default we will overwrite existing files for
    # the same date and wiki(s)
    overwrite = True
    wq_path = os.path.join(os.getcwd(), "wikiqueries.py")
    wq_config_file = os.path.join(os.getcwd(), "wikiqueries.conf")

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [
            "outputdir=", "remotereponame=", "wqconfig=", "wqpath=",
            "remoteonly", "localonly",
            "nooverwrite", "verbose"])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--outputdir":
            output_dir = val
        elif opt == "--remotereponame":
            remote_repo_name = val
        elif opt == "--remoteonly":
            remote_only = True
        elif opt == "--localonly":
            local_only = True
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--verbose":
            verbose = True
        elif opt == "--wqconfig":
            wq_config_file = val
            if os.sep not in val:
                wq_config_file = os.path.join(os.getcwd(), wq_config_file)
            # bummer but we can't really avoid ita
        elif opt == "--wqpath":
            wq_path = val
            if os.sep not in val:
                wq_path = os.path.join(os.getcwd(), wq_path)

    if len(remainder) == 1:
        if not remainder[0].isalpha():
            usage("Unknown argument(s) specified")
        else:
            wiki = remainder[0]
    elif len(remainder) > 1:
        usage("Unknown argument(s) specified")

    if not output_dir:
        usage("One or more mandatory options missing")
    if local_only and remote_only:
        usage("Only one of 'localonly' and 'remoteonly'"
              " may be specified at once.")

    config = Config(wq_config_file)

    mpp = MediaPerProject(config, output_dir, remote_repo_name,
                          verbose, wq_config_file, wq_path, overwrite, wiki)
    if not remote_only:
        if verbose:
            print "generating lists of local media on each project"
        mpp.write_local_media()
    if not local_only:
        if verbose:
            print "generating remote hosted media lists for all projects"
        mpp.write_remote_media()
    if verbose:
        print "all projects completed."


if __name__ == "__main__":
    do_main()
