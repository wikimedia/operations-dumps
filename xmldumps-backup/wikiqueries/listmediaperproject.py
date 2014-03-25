import os
import sys
import getopt
import time
from subprocess import Popen, PIPE
from wikiqueries import Config


class MediaPerProject(object):
    def __init__(self, conf, outputDir, remoteRepoName,
                 verbose, wqConfigFile, wqPath, overwrite, wiki=None):
        self.conf = conf
        self.outputDir = outputDir
        self.remoteRepoName = remoteRepoName
        self.verbose = verbose
        self.date = time.strftime("%Y%m%d", time.gmtime())
        self.fileNameFormat = "{w}-{d}-wikiqueries.gz"
        self.wqConfigFile = wqConfigFile
        self.wqPath = wqPath
        self.overwrite = overwrite
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        if wiki is not None:
            self.wikisToDo = [wiki]
        else:
            self.wikisToDo = [w for w in self.conf.allWikisList
                              if w not in self.conf.privateWikisList and
                              w not in self.conf.closedWikisList]

    def getFileNameFormat(self, phase):
        return "{w}-{d}-" + phase + "-wikiqueries.gz"

    def writeLocalMedia(self):
        if self.verbose:
            print "Starting round one wikiqueries for image table"
        if len(self.wikisToDo) == 1:
            wiki = self.wikisToDo[0]
        else:
            wiki = None
        self.doWikiQueries('select img_name, img_timestamp from image',
                           self.getFileNameFormat("local"), wiki)
        if self.verbose:
            print "Done round one!!"

    def doWikiQueries(self, query, fileNameFormat, wiki=None):
        if not os.path.exists(wqConfigFile):
            print "config file  %s does not exist" % wqConfigFile
            sys.exit(1)
        command = ["python", self.wqPath, "--configfile", wqConfigFile,
                   "--query", query, "--outdir", self.outputDir,
                   "--filenameformat", fileNameFormat]
        if self.verbose:
            command.append("--verbose")
        if not self.overwrite:
            command.append("--nooverwrite")
        if wiki:
            command.append(wiki)
        commandString = " ".join(["'" + c + "'" for c in command])

        if self.verbose:
            print "About to run wikiqueries:", commandString
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

    def writeRemoteMedia(self):
        if self.verbose:
            print "Starting round two wikiqueries for global image links table"

        for w in self.wikisToDo:
            if w == self.remoteRepoName:
                if self.verbose:
                    print "Skipping", w, "because it's the remote repo"
            else:
                if self.verbose:
                    print "Doing db", w
                self.doWikiQueries('select gil_to from globalimagelinks'
                                   ' where gil_wiki= "%s"' % w,
                                   self.getFileNameFormat("remote").format(
                                       w=w, d='{d}'), self.remoteRepoName)
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


if __name__ == "__main__":
    outputDir = None
    remoteRepoName = "commonswiki"
    verbose = False
    wiki = None
    remoteOnly = False
    localOnly = False
    # by default we will overwrite existing files for
    # the same date and wiki(s)
    overwrite = True
    wqPath = os.path.join(os.getcwd(), "wikiqueries.py")
    wqConfigFile = os.path.join(os.getcwd(), "wikiqueries.conf")

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", [
            "outputdir=", "remotereponame=", "wqconfig=", "wqpath=",
            "remoteonly", "localonly",
            "nooverwrite", "verbose"])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--outputdir":
            outputDir = val
        elif opt == "--remotereponame":
            remoteRepoName = val
        elif opt == "--remoteonly":
            remoteOnly = True
        elif opt == "--localonly":
            localOnly = True
        elif opt == "--nooverwrite":
            overwrite = False
        elif opt == "--verbose":
            verbose = True
        elif opt == "--wqconfig":
            wqConfigFile = val
            if not os.sep in val:
                wqConfigFile = os.path.join(os.getcwd(), wqConfigFile)
            # bummer but we can't really avoid ita
        elif opt == "--wqpath":
            wqPath = val
            if not os.sep in val:
                wqPath = os.path.join(os.getcwd(), wqPath)

    if len(remainder) == 1:
        if not remainder[0].isalpha():
            usage("Unknown argument(s) specified")
        else:
            wiki = remainder[0]
    elif len(remainder) > 1:
        usage("Unknown argument(s) specified")

    if not outputDir:
        usage("One or more mandatory options missing")
    if localOnly and remoteOnly:
        usage("Only one of 'localonly' and 'remoteonly'"
              " may be specified at once.")

    config = Config(wqConfigFile)

    mpp = MediaPerProject(config, outputDir, remoteRepoName,
                          verbose, wqConfigFile, wqPath, overwrite, wiki)
    if not remoteOnly:
        if verbose:
            print "generating lists of local media on each project"
        mpp.writeLocalMedia()
    if not localOnly:
        if verbose:
            print "generating remote hosted media lists for all projects"
        mpp.writeRemoteMedia()
    if verbose:
        print "all projects completed."
