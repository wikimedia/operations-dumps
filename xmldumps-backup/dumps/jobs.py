'''
All dump jobs are defined here
from xml page dumps to dumps of private mysql tables
to recombining multiple stub dumps into one file
'''

import getopt, hashlib, os, re, sys, errno, time
import subprocess, select
import shutil, stat, signal, glob
import Queue, thread, traceback, socket

from os.path import exists
from subprocess import Popen, PIPE
from WikiDump import FileUtils, MiscUtils, TimeUtils
from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel

from dumps.exceptions import *
from dumps.fileutils import *
from dumps.utils import RunInfo, MultiVersion, RunInfoFile, Chunk

class Dump(object):
    def __init__(self, name, desc, verbose=False):
        self._desc = desc
        self.verbose = verbose
        self.progress = ""
        self.runInfo = RunInfo(name, "waiting", "")
        self.dumpName = self.getDumpName()
        self.fileType = self.getFileType()
        self.fileExt = self.getFileExt()
        # if var hasn't been defined by a derived class already.  (We get
        # called last by child classes in their constructor, so that
        # their functions overriding things like the dumpbName can
        # be set up before we use them to set class attributes.)
        if not hasattr(self, 'onlychunks'):
            self.onlychunks = False
        if not hasattr(self, '_chunksEnabled'):
            self._chunksEnabled = False
        if not hasattr(self, '_checkpointsEnabled'):
            self._checkpointsEnabled = False
        if not hasattr(self, 'checkpointFile'):
            self.checkpointFile = False
        if not hasattr(self, '_chunkToDo'):
            self._chunkToDo = False
        if not hasattr(self, '_prerequisiteItems'):
            self._prerequisiteItems = []
        if not hasattr(self, '_checkTruncation'):
            # Automatic checking for truncation of produced files is
            # (due to dumpDir handling) only possible for public dir
            # right now. So only set this to True, when all files of
            # the item end in the public dir.
            self._checkTruncation = False

    def name(self):
        return self.runInfo.name()

    def status(self):
        return self.runInfo.status()

    def updated(self):
        return self.runInfo.updated()

    def toBeRun(self):
        return self.runInfo.toBeRun()

    def setName(self, name):
        self.runInfo.setName(name)

    def setToBeRun(self, toBeRun):
        self.runInfo.setToBeRun(toBeRun)

    def setSkipped(self):
        self.setStatus("skipped")
        self.setToBeRun(False)

    # sometimes this will be called to fill in data from an old
    # dump run; in those cases we don't want to clobber the timestamp
    # with the current time.
    def setStatus(self, status, setUpdated=True):
        self.runInfo.setStatus(status)
        if setUpdated:
            self.runInfo.setUpdated(TimeUtils.prettyTime())

    def setUpdated(self, updated):
        self.runInfo.setUpdated(updated)

    def description(self):
        return self._desc

    def detail(self):
        """Optionally return additional text to appear under the heading."""
        return None

    def getDumpName(self):
        """Return the dumpName as it appears in output files for this phase of the dump
        e.g. pages-meta-history, all-titles-in-ns0, etc"""
        return ""

    def listDumpNames(self):
        """Returns a list of names as they appear in output files for this phase of the dump
        e.g. [pages-meta-history], or [stub-meta-history, stub-meta-current, stub-articles], etc"""
        return [self.getDumpName()]

    def getFileExt(self):
        """Return the extension of output files for this phase of the dump
        e.g. bz2 7z etc"""
        return ""

    def getFileType(self):
        """Return the type of output files for this phase of the dump
        e.g. sql xml etc"""
        return ""

    def start(self, runner):
        """Set the 'in progress' flag so we can output status."""
        self.setStatus("in-progress")

    def dump(self, runner):
        """Attempt to run the operation, updating progress/status info."""
        try:
            for prerequisiteItem in self._prerequisiteItems:
                if prerequisiteItem.status() == "failed":
                    raise BackupError("Required job %s failed, not starting job %s" % (prerequisiteItem.name(), self.name()))
                elif prerequisiteItem.status() != "done":
                    raise BackupPrereqError("Required job %s not marked as done, not starting job %s" % (prerequisiteItem.name(), self.name()))

            self.run(runner)
            self.postRun(runner)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if self.verbose:
                sys.stderr.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            if exc_type.__name__ == 'BackupPrereqError':
                self.setStatus("waiting")
            else:
                self.setStatus("failed")
            raise

        self.setStatus("done")

    def run(self, runner):
        """Actually do something!"""
        pass

    def postRun(self, runner):
        """Common tasks to run after performing this item's actual dump"""
        # Checking for truncated files
        truncatedFilesCount = self.checkForTruncatedFiles(runner)
        if truncatedFilesCount:
            raise BackupError("Encountered %d truncated files for %s" % (truncatedFilesCount, self.dumpName))

    def checkForTruncatedFiles(self, runner):
        """Returns the number of files that have been detected to be truncated. This function expects that all files to check for truncation live in the public dir"""
        ret = 0

        if not runner._checkForTruncatedFilesEnabled or not self._checkTruncation:
            return ret

        # dfn is the DumpFilename
        # df  is the DumpFile
        for dfn in self.listOutputFilesToCheckForTruncation(runner.dumpDir):
            df = DumpFile(runner.wiki, runner.dumpDir.filenamePublicPath(dfn), dfn);

            fileTruncated=True;
            if exists(df.filename):
                if df.checkIfTruncated():
                    # The file exists and is truncated, we move it out of the way
                    df.rename(df.filename + ".truncated")

                    # We detected a failure and could abort right now. However,
                    # there might still be some further chunk files, that are good.
                    # Hence, we go on treating the remaining files and in the end
                    # /all/ truncated files have been moved out of the way. So we
                    # see, which chunks (instead of the whole job) need a rerun.
                else:
                    # The file exists and is not truncated. Heck, it's a good file!
                    fileTruncated=False

            if fileTruncated:
                ret+=1

        return ret

    def progressCallback(self, runner, line=""):
        """Receive a status line from a shellout and update the status files."""
        # pass through...
        if line:
            if runner.log:
                runner.log.addToLogQueue(line)
            sys.stderr.write(line)
        self.progress = line.strip()
        runner.status.updateStatusFiles()
        runner.runInfoFile.saveDumpRunInfoFile(runner.dumpItemList.reportDumpRunInfo())

    def timeToWait(self):
        # we use wait this many secs for a command to complete that
        # doesn't produce output
        return 5

    def waitAlarmHandler(self, signum, frame):
        pass

    def buildRecombineCommandString(self, runner, files, outputFile, compressionCommand, uncompressionCommand, endHeaderMarker="</siteinfo>"):
        outputFilename = runner.dumpDir.filenamePublicPath(outputFile)
        chunkNum = 0
        recombines = []
        if not exists(runner.wiki.config.head):
            raise BackupError("head command %s not found" % runner.wiki.config.head)
        head = runner.wiki.config.head
        if not exists(runner.wiki.config.tail):
            raise BackupError("tail command %s not found" % runner.wiki.config.tail)
        tail = runner.wiki.config.tail
        if not exists(runner.wiki.config.grep):
            raise BackupError("grep command %s not found" % runner.wiki.config.grep)
        grep = runner.wiki.config.grep

        # we assume the result is always going to be run in a subshell.
        # much quicker than this script trying to read output
        # and pass it to a subprocess
        outputFilenameEsc = MiscUtils.shellEscape(outputFilename)
        headEsc = MiscUtils.shellEscape(head)
        tailEsc = MiscUtils.shellEscape(tail)
        grepEsc = MiscUtils.shellEscape(grep)

        uncompressionCommandEsc = uncompressionCommand[:]
        for u in uncompressionCommandEsc:
            u = MiscUtils.shellEscape(u)
        for u in compressionCommand:
            u = MiscUtils.shellEscape(u)

        if not files:
            raise BackupError("No files for the recombine step found in %s." % self.name())

        for fileObj in files:
            # uh oh FIXME
#            f = MiscUtils.shellEscape(fileObj.filename)
            f = runner.dumpDir.filenamePublicPath(fileObj)
            chunkNum = chunkNum + 1
            pipeline = []
            uncompressThisFile = uncompressionCommand[:]
            uncompressThisFile.append(f)
            pipeline.append(uncompressThisFile)
            # warning: we figure any header (<siteinfo>...</siteinfo>) is going to be less than 2000 lines!
            pipeline.append([head, "-2000"])
            pipeline.append([grep, "-n", endHeaderMarker])
            # without shell
            p = CommandPipeline(pipeline, quiet=True)
            p.runPipelineAndGetOutput()
            if (p.output()) and (p.exitedSuccessfully() or p.getFailedCommandsWithExitValue() == [[-signal.SIGPIPE, uncompressThisFile]] or p.getFailedCommandsWithExitValue() == [[signal.SIGPIPE + 128, uncompressThisFile]]):
                (headerEndNum, junk) = p.output().split(":", 1)
                # get headerEndNum
            else:
                raise BackupError("Could not find 'end of header' marker for %s" % f)
            recombine = " ".join(uncompressThisFile)
            headerEndNum = int(headerEndNum) + 1
            if chunkNum == 1:
                # first file, put header and contents
                recombine = recombine + " | %s -n -1 " % headEsc
            elif chunkNum == len(files):
                # last file, put footer
                recombine = recombine + (" | %s -n +%s" % (tailEsc, headerEndNum))
            else:
                # put contents only
                recombine = recombine + (" | %s -n +%s" % (tailEsc, headerEndNum))
                recombine = recombine + " | %s -n -1 " % head
            recombines.append(recombine)
        recombineCommandString = "(" + ";".join(recombines) + ")" + "|" + "%s %s" % (compressionCommand, outputFilename)
        return recombineCommandString

    def cleanupOldFiles(self, dumpDir, runner, chunks=False):
        if runner._cleanupOldFilesEnabled:
            if self.checkpointFile:
                # we only rerun this one, so just remove this one
                if exists(dumpDir.filenamePublicPath(self.checkpointFile)):
                    os.remove(dumpDir.filenamePublicPath(self.checkpointFile))
                elif exists(dumpDir.filenamePrivatePath(self.checkpointFile)):
                    os.remove(dumpDir.filenamePrivatePath(self.checkpointFile))
            files = self.listOutputFilesForCleanup(dumpDir)
            for f in files:
                if exists(dumpDir.filenamePublicPath(f)):
                    os.remove(dumpDir.filenamePublicPath(f))
                elif exists(dumpDir.filenamePrivatePath(f)):
                    os.remove(dumpDir.filenamePrivatePath(f))

    def getChunkList(self):
        if self._chunksEnabled:
            if self._chunkToDo:
                return [self._chunkToDo]
            else:
                return range(1, len(self._chunks)+1)
        else:
            return False

    # list all regular output files that exist
    def listRegularFilesExisting(self, dumpDir, dumpNames=None, date=None, chunks=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getRegularFilesExisting(date, d, self.fileType, self.fileExt, chunks, temp=False))
        return files

    # list all checkpoint files that exist
    def listCheckpointFilesExisting(self, dumpDir, dumpNames=None, date=None, chunks=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getCheckpointFilesExisting(date, d, self.fileType, self.fileExt, chunks, temp=False))
        return files

    # unused
    # list all temp output files that exist
    def listTempFilesExisting(self, dumpDir, dumpNames=None, date=None, chunks=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks=None, temp=True))
            files.extend(dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks=None, temp=True))
        return files

    # list checkpoint files that have been produced for specified chunk(s)
    def listCheckpointFilesPerChunkExisting(self, dumpDir, chunks, dumpNames=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp=False))
        return files

    # list noncheckpoint files that have been produced for specified chunk(s)
    def listRegularFilesPerChunkExisting(self, dumpDir, chunks, dumpNames=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp=False))
        return files

    # list temp output files that have been produced for specified chunk(s)
    def listTempFilesPerChunkExisting(self, dumpDir, chunks, dumpNames=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp=True))
            files.extend(dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks, temp=True))
        return files


    # unused
    # list noncheckpoint chunk files that have been produced
    def listRegularFilesChunkedExisting(self, dumpDir, runner, dumpNames=None, date=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks=self.getChunkList(), temp=False))
        return files

    # unused
    # list temp output chunk files that have been produced
    def listTempFilesChunkedExisting(self, runner, dumpNames=None):
        files = []
        if not dumpNames:
            dumpNames = [self.dumpName]
        for d in dumpNames:
            files.extend(runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks=self.getChunkList(), temp=True))
            files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks=self.getChunkList(), temp=True))
        return files

    # unused
    # list checkpoint files that have been produced for chunkless run
    def listCheckpointFilesChunklessExisting(self, runner, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks=False, temp=False))
        return files

    # unused
    # list non checkpoint files that have been produced for chunkless run
    def listRegularFilesChunklessExisting(self, runner, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks=False, temp=False))
        return files

    # unused
    # list non checkpoint files that have been produced for chunkless run
    def listTempFilesChunklessExisting(self, runner, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(runner.dumpDir.getCheckpointFilesExisting(None, d, self.fileType, self.fileExt, chunks=False, temp=True))
            files.extend(runner.dumpDir.getRegularFilesExisting(None, d, self.fileType, self.fileExt, chunks=False, temp=True))
        return files


    # internal function which all the public get*Possible functions call
    # list all files that could be created for the given dumpName, filtering by the given args.
    # by definition, checkpoint files are never returned in such a list, as we don't
    # know where a checkpoint might be taken (which pageId start/end).
    #
    # if we get None for an arg then we accept all values for that arg in the filename
    # if we get False for an arg (chunk, temp), we reject any filename which contains a value for that arg
    # if we get True for an arg (temp), we accept only filenames which contain a value for the arg
    # chunks should be a list of value(s), or True / False / None
    def _getFilesPossible(self, dumpDir, date=None, dumpName=None, fileType=None, fileExt=None, chunks=None, temp=False):
        files = []
        if dumpName == None:
            dumpname = self.dumpName
        if chunks == None or chunks == False:
            files.append(DumpFilename(dumpDir._wiki, date, dumpName, fileType, fileExt, None, None, temp))
        if chunks == True or chunks == None:
            chunks = self.getChunksList()
        if chunks:
            for i in chunks:
                files.append(DumpFilename(dumpDir._wiki, date, dumpName, fileType, fileExt, i, None, temp))
        return files

    # unused
    # based on dump name, get all the output files we expect to generate except for temp files
    def getRegularFilesPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=None, temp=False))
        return files

    # unused
    # based on dump name, get all the temp output files we expect to generate
    def getTempFilesPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=None, temp=True))
        return files

    # based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
    def getRegularFilesPerChunkPossible(self, dumpDir, chunks, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks, temp=False))
        return files

    # unused
    # based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
    def getTempFilesPerChunkPossible(self, dumpDir, chunks, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks, temp=True))
        return files


    # unused
    # based on dump name, chunks, etc. get all the output files we expect to generate for these chunks
    def getRegularFilesChunkedPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=True, temp=False))
        return files

    # unused
    # based on dump name, chunks, etc. get all the temp files we expect to generate for these chunks
    def getTempFilesPerChunkedPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=True, temp=True))
        return files

    # unused
    # list noncheckpoint files that should be produced for chunkless run
    def getRegularFilesChunklessPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=False, temp=False))
        return files

    # unused
    # list temp output files that should be produced for chunkless run
    def getTempFilesChunklessPossible(self, dumpDir, dumpNames=None):
        if not dumpNames:
            dumpNames = [self.dumpName]
        files = []
        for d in dumpNames:
            files.extend(self._getFilesPossible(dumpDir, None, d, self.fileType, self.fileExt, chunks=False, temp=True))
        return files

################################
#
# these routines are all used for listing output files for various purposes...
#
#
    # Used for updating md5 lists, index.html
    # Includes: checkpoints, chunks, chunkless, temp files if they exist. At end of run temp files must be gone.
    # This is *all* output files for the dumpName, regardless of what's being re-run.
    def listOutputFilesToPublish(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self.checkpointFile:
            files.append(self.checkpointFile)
            return files

        if self._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
            files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
                # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
        return files

    # called at end of job run to see if results are intact or are garbage and must be tossed/rerun.
    # Includes: checkpoints, chunks, chunkless.  Not included: temp files.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self.checkpointFile:
            files.append(self.checkpointFile)
            return files

        if self._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
        return files

    # called when putting together commands to produce output for the job.
    # Includes: chunks, chunkless, temp files.   Not included: checkpoint files.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def listOutputFilesForBuildCommand(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self.checkpointFile:
            files.append(self.checkpointFile)
            return files

        if self._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
                # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.getRegularFilesPerChunkPossible(dumpDir, self.getChunkList(), dumpNames))
        return files

    # called before job run to cleanup old files left around from any previous run(s)
    # Includes: checkpoints, chunks, chunkless, temp files if they exist.
    # This is only the files that should be produced from this run. So it is limited to a specific
    # chunk if that's being redone, or to all chunks if the whole job is being redone, or to the chunkless
    # files if there are no chunks enabled.
    def listOutputFilesForCleanup(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self.checkpointFile:
            files.append(self.checkpointFile)
            return files

        if self._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
            files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        return files

    # used to generate list of input files for other phase (e.g. recombine, recompress)
    # Includes: checkpoints, chunks/chunkless files depending on whether chunks are enabled. Not included: temp files.
    # This is *all* output files for the job, regardless of what's being re-run. The caller can sort out which
    # files go to which chunk, in case input is needed on a per chunk basis. (Is that going to be annoying? Nah,
    # and we only do it once per job so who cares.)
    def listOutputFilesForInput(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self._checkpointsEnabled:
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
            files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        return files

class PublicTable(Dump):
    """Dump of a table using MySQL's mysqldump utility."""

    def __init__(self, table, name, desc):
        self._table = table
        self._chunksEnabled = False
        Dump.__init__(self, name, desc)

    def getDumpName(self):
        return self._table

    def getFileType(self):
        return "sql"

    def getFileExt(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(files) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpName)
        outputFile = files[0]
        error = self.saveTable(self._table, runner.dumpDir.filenamePublicPath(outputFile), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.saveTable(self._table, runner.dumpDir.filenamePublicPath(outputFile), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    # returns 0 on success, 1 on error
    def saveTable(self, table, outfile, runner):
        """Dump a table from the current DB with mysqldump, save to a gzipped sql file."""
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        commands = runner.dbServerInfo.buildSqlDumpCommand(table, runner.wiki.config.gzip)
        return runner.saveCommand(commands, outfile)

class PrivateTable(PublicTable):
    """Hidden table dumps for private data."""

    def __init__(self, table, name, desc):
        # Truncation checks require output to public dir, hence we
        # cannot use them. The default would be 'False' anyways, but
        # if that default changes, we still cannot use automatic
        # truncation checks.
        self._checkTruncation = False
        PublicTable.__init__(self, table, name, desc)

    def description(self):
        return self._desc + " (private)"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(files) > 1:
            raise BackupError("table dump %s trying to produce more than one file" % self.dumpName)
        outputFile = files[0]
        error = self.saveTable(self._table, runner.dumpDir.filenamePrivatePath(outputFile), runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.saveTable(self._table, runner.dumpDir.filenamePrivatePath(outputFile), runner)
        if error:
            raise BackupError("error dumping table %s" % self._table)

    def listOutputFilesToPublish(self, dumpDir):
        """Private table won't have public files to list."""
        return []

class XmlStub(Dump):
    """Create lightweight skeleton dumps, minus bulk text.
    A second pass will import text from prior dumps or the database to make
    full files for the public."""

    def __init__(self, name, desc, chunkToDo, chunks=False, checkpoints=False):
        self._chunkToDo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunksEnabled = True
            self.onlychunks = True
        self.historyDumpName = "stub-meta-history"
        self.currentDumpName = "stub-meta-current"
        self.articlesDumpName = "stub-articles"
        if checkpoints:
            self._checkpointsEnabled = True
        self._checkTruncation = True
        Dump.__init__(self, name, desc)

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def getFileType(self):
        return "xml"

    def getFileExt(self):
        return "gz"

    def getDumpName(self):
        return 'stub'

    def listDumpNames(self):
        dumpNames =  [self.historyDumpName, self.currentDumpName, self.articlesDumpName]
        return dumpNames

    def listOutputFilesToPublish(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
        return files

    def listOutputFilesToCheckForTruncation(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForBuildCommand(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForBuildCommand(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForCleanup(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForInput(self, dumpDir, dumpNames=None):
        if dumpNames == None:
            dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForInput(self, dumpDir, dumpNames))
        return files

    def buildCommand(self, runner, f):
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)

        articlesFile = runner.dumpDir.filenamePublicPath(f)
        historyFile = runner.dumpDir.filenamePublicPath(DumpFilename(runner.wiki, f.date, self.historyDumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint, f.temp))
        currentFile = runner.dumpDir.filenamePublicPath(DumpFilename(runner.wiki, f.date, self.currentDumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint, f.temp))
        scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpBackup.php")

        command = ["/usr/bin/python", "xmlstubs.py", "--config", runner.wiki.config.files[0], "--wiki", runner.dbName,
                    runner.forceNormalOption(), "--articles", articlesFile,
                    "--history", historyFile, "--current", currentFile]

        if f.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            # start = runner.pagesPerChunk()*(chunk-1) + 1
            start = sum([self._chunks[i] for i in range(0, f.chunkInt-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if f.chunkInt < len(self._chunks):
                end = sum([self._chunks[i] for i in range(0, f.chunkInt)]) +1
                endopt = "--end=%s" % end
                command.append(endopt)

        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        self.cleanupOldFiles(runner.dumpDir, runner)
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        for f in files:
            # choose arbitrarily one of the dumpNames we do (= articlesDumpName)
            # buildcommand will figure out the files for the rest
            if f.dumpName == self.articlesDumpName:
                series = self.buildCommand(runner, f)
                commands.append(series)
        error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
        if error:
            raise BackupError("error producing stub files")

class RecombineXmlStub(Dump):
    def __init__(self, name, desc, itemForXmlStubs):
        self.itemForXmlStubs = itemForXmlStubs
        self._prerequisiteItems = [self.itemForXmlStubs]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpointsEnabled = False

    def detail(self):
        return "These files contain no page text, only revision metadata."

    def listDumpNames(self):
        return self.itemForXmlStubs.listDumpNames()

    def listOutputFilesToPublish(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
        return files

    def listOutputFilesToCheckForTruncation(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
        return files

    def getFileType(self):
        return self.itemForXmlStubs.getFileType()

    def getFileExt(self):
        return self.itemForXmlStubs.getFileExt()

    def getDumpName(self):
        return self.itemForXmlStubs.getDumpName()

    def run(self, runner):
        error=0
        files = self.itemForXmlStubs.listOutputFilesForInput(runner.dumpDir)
        outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir, self.listDumpNames())
        for outputFileObj in outputFileList:
            inputFiles = []
            for inFile in files:
                if inFile.dumpName == outputFileObj.dumpName:
                    inputFiles.append(inFile)
            if not len(inputFiles):
                self.setStatus("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(runner.wiki.config.gzip):
                raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
            compressionCommand = runner.wiki.config.gzip
            compressionCommand = "%s > " % runner.wiki.config.gzip
            uncompressionCommand = ["%s" % runner.wiki.config.gzip, "-dc"]
            recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFileObj, compressionCommand, uncompressionCommand)
            recombineCommand = [recombineCommandString]
            recombinePipeline = [recombineCommand]
            series = [recombinePipeline]
            result = runner.runCommand([series], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining stub files")

class XmlLogging(Dump):
    """ Create a logging dump of all page activity """

    def __init__(self, desc, chunks=False):
        Dump.__init__(self, "xmlpagelogsdump", desc)

    def detail(self):
        return "This contains the log of actions performed on pages and users."

    def getDumpName(self):
        return "pages-logging"

    def getFileType(self):
        return "xml"

    def getFileExt(self):
        return "gz"

    def getTempFilename(self, name, number):
        return name + "-" + str(number)

    def run(self, runner):
        self.cleanupOldFiles(runner.dumpDir, runner)
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(files) > 1:
            raise BackupError("logging table job wants to produce more than one output file")
        outputFileObj = files[0]
        if not exists(runner.wiki.config.php):
            raise BackupError("php command %s not found" % runner.wiki.config.php)
        scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpBackup.php")

        logging = runner.dumpDir.filenamePublicPath(outputFileObj)

        command = ["/usr/bin/python", "xmllogs.py", "--config", runner.wiki.config.files[0], "--wiki", runner.dbName,
                    runner.forceNormalOption(), "--outfile", logging]

        pipeline = [command]
        series = [pipeline]
        error = runner.runCommand([series], callbackStderr=self.progressCallback, callbackStderrArg=runner)
        if error:
            raise BackupError("error dumping log files")

class XmlDump(Dump):
    """Primary XML dumps, one section at a time."""
    def __init__(self, subset, name, desc, detail, itemForStubs, prefetch, spawn, wiki, chunkToDo, chunks=False, checkpoints=False, checkpointFile=None, pageIDRange=None, verbose=False):
        self._subset = subset
        self._detail = detail
        self._desc = desc
        self._prefetch = prefetch
        self._spawn = spawn
        self._chunks = chunks
        if self._chunks:
            self._chunksEnabled = True
            self.onlychunks = True
        self._pageID = {}
        self._chunkToDo = chunkToDo

        self.wiki = wiki
        self.itemForStubs = itemForStubs
        if checkpoints:
            self._checkpointsEnabled = True
        self.checkpointFile = checkpointFile
        if self.checkpointFile:
            # we don't checkpoint the checkpoint file.
            self._checkpointsEnabled = False
        self.pageIDRange = pageIDRange
        self._prerequisiteItems = [self.itemForStubs]
        self._checkTruncation = True
        Dump.__init__(self, name, desc)

    def getDumpNameBase(self):
        return 'pages-'

    def getDumpName(self):
        return self.getDumpNameBase() + self._subset

    def getFileType(self):
        return "xml"

    def getFileExt(self):
        return "bz2"

    def run(self, runner):
        commands = []
        self.cleanupOldFiles(runner.dumpDir, runner)
        # just get the files pertaining to our dumpName, which is *one* of articles, pages-current, pages-history.
        # stubs include all of them together.
        if not self.dumpName.startswith(self.getDumpNameBase()):
            raise BackupError("dumpName %s of unknown form for this job" % self.dumpName)
        dumpName = self.dumpName[len(self.getDumpNameBase()):]
        stubDumpNames = self.itemForStubs.listDumpNames()
        for s in stubDumpNames:
            if s.endswith(dumpName):
                stubDumpName = s
        inputFiles = self.itemForStubs.listOutputFilesForInput(runner.dumpDir, [stubDumpName])
        if self._chunksEnabled and self._chunkToDo:
            # reset inputfiles to just have the one we want.
            for f in inputFiles:
                if f.chunkInt == self._chunkToDo:
                    inputFiles = [f]
                    break
            if len(inputFiles) > 1:
                raise BackupError("Trouble finding stub files for xml dump run")

        if self.checkpointFile:
            # fixme this should be an input file, not the output checkpoint file. move
            # the code out of buildCommand that does the conversion and put it here.
            series = self.buildCommand(runner, self.checkpointFile)
            commands.append(series)
        else:
            for f in inputFiles:
                outputFile = DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt)
                series = self.buildCommand(runner, f)
                commands.append(series)

        error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
        if error:
            raise BackupError("error producing xml file(s) %s" % self.dumpName)

    def buildEta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--current"

    # takes name of the output file
    def buildFilters(self, runner, f):
        """Construct the output filter options for dumpTextPass.php"""
        # do we need checkpoints? ummm
        xmlbz2 = runner.dumpDir.filenamePublicPath(f)

        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if self.wiki.config.bzip2[-6:] == "dbzip2":
            bz2mode = "dbzip2"
        else:
            bz2mode = "bzip2"
        return "--output=%s:%s" % (bz2mode, xmlbz2)

    def writePartialStub(self, inputFile, outputFile, startPageID, endPageID, runner):
        if not exists(self.wiki.config.writeuptopageid):
            raise BackupError("writeuptopageid command %s not found" % self.wiki.config.writeuptopageid)
        writeuptopageid = self.wiki.config.writeuptopageid

        inputFilePath = runner.dumpDir.filenamePublicPath(inputFile)
        outputFilePath = os.path.join(self.wiki.config.tempDir, outputFile.filename)
        if inputFile.fileExt == "gz":
            command1 =  "%s -dc %s" % (self.wiki.config.gzip, inputFilePath)
            command2 = "%s > %s" % (self.wiki.config.gzip, outputFilePath)
        elif inputFile.fileExt == '7z':
            command1 =  "%s e -si %s" % (self.wiki.config.sevenzip, inputFilePath)
            command2 =  "%s e -so %s" % (self.wiki.config.sevenzip, outputFilePath)
        elif inputFile.fileExt == 'bz':
            command1 =  "%s -dc %s" % (self.wiki.config.bzip2, inputFilePath)
            command2 =  "%s > %s" % (self.wiki.config.bzip2, outputFilePath)
        else:
            raise BackupError("unknown stub file extension %s" % inputFile.fileExt)
        if endPageID:
            command = [command1 + ("| %s %s %s |" % (self.wiki.config.writeuptopageid, startPageID, endPageID)) + command2]
        else:
            # no lastpageid? read up to eof of the specific stub file that's used for input
            command = [command1 + ("| %s %s |" % (self.wiki.config.writeuptopageid, startPageID)) + command2]

        pipeline = [command]
        series = [pipeline]
        error = runner.runCommand([series], shell=True)
        if error:
            raise BackupError("failed to write partial stub file %s" % outputFile.filename)

    def buildCommand(self, runner, f):
        """Build the command line for the dump, minus output and filter options"""

        if self.checkpointFile:
            outputFile = f
        elif self._checkpointsEnabled:
            # we write a temp file, it will be checkpointed every so often.
            outputFile = DumpFilename(self.wiki, f.date, self.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, temp=True)
        else:
            # we write regular files
            outputFile = DumpFilename(self.wiki, f.date, self.dumpName, f.fileType, self.fileExt, f.chunk, checkpoint=False, temp=False)

        # Page and revision data pulled from this skeleton dump...
        # FIXME we need the stream wrappers for proper use of writeupto. this is a hack.
        if self.checkpointFile or self.pageIDRange:
            # fixme I now have this code in a couple places, make it a function.
            if not self.dumpName.startswith(self.getDumpNameBase()):
                raise BackupError("dumpName %s of unknown form for this job" % self.dumpName)
            dumpName = self.dumpName[len(self.getDumpNameBase()):]
            stubDumpNames = self.itemForStubs.listDumpNames()
            for s in stubDumpNames:
                if s.endswith(dumpName):
                    stubDumpName = s

        if self.checkpointFile:
            stubInputFilename = self.checkpointFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), self.checkpointFile.date, self.checkpointFile.chunk)
            stubInputFile = DumpFilename(self.wiki)
            stubInputFile.newFromFilename(stubInputFilename)
            stubOutputFilename = self.checkpointFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), self.checkpointFile.date, self.checkpointFile.chunk, self.checkpointFile.checkpoint)
            stubOutputFile = DumpFilename(self.wiki)
            stubOutputFile.newFromFilename(stubOutputFilename)
            self.writePartialStub(stubInputFile, stubOutputFile, self.checkpointFile.firstPageID, str(int(self.checkpointFile.lastPageID) + 1), runner)
            stubOption = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stubOutputFile.filename)
        elif self.pageIDRange:
            # two cases. redoing a specific chunk, OR no chunks, redoing the whole output file. ouch, hope it isn't huge.
            if self._chunkToDo or not self._chunksEnabled:
                stubInputFile = f

            stubOutputFilename = stubInputFile.newFilename(stubDumpName, self.itemForStubs.getFileType(), self.itemForStubs.getFileExt(), stubInputFile.date, stubInputFile.chunk, stubInputFile.checkpoint)
            stubOutputFile = DumpFilename(self.wiki)
            stubOutputFile.newFromFilename(stubOutputFilename)
            if ',' in self.pageIDRange:
                (firstPageID, lastPageID) = self.pageIDRange.split(',', 2)
            else:
                firstPageID = self.pageIDRange
                lastPageID = None
            self.writePartialStub(stubInputFile, stubOutputFile, firstPageID, lastPageID, runner)

            stubOption = "--stub=gzip:%s" % os.path.join(self.wiki.config.tempDir, stubOutputFile.filename)
        else:
            stubOption = "--stub=gzip:%s" % runner.dumpDir.filenamePublicPath(f)

        # Try to pull text from the previous run; most stuff hasn't changed
        #Source=$OutputDir/pages_$section.xml.bz2
        sources = []
        possibleSources = None
        if self._prefetch:
            possibleSources = self._findPreviousDump(runner, f.chunk)
            # if we have a list of more than one then we need to check existence for each and put them together in a string
            if possibleSources:
                for sourceFile in possibleSources:
                    s = runner.dumpDir.filenamePublicPath(sourceFile, sourceFile.date)
                    if exists(s):
                        sources.append(s)
        if f.chunk:
            chunkinfo = "%s" % f.chunk
        else:
            chunkinfo =""
        if len(sources) > 0:
            source = "bzip2:%s" % (";".join(sources))
            runner.showRunnerState("... building %s %s XML dump, with text prefetch from %s..." % (self._subset, chunkinfo, source))
            prefetch = "--prefetch=%s" % (source)
        else:
            runner.showRunnerState("... building %s %s XML dump, no text prefetch..." % (self._subset, chunkinfo))
            prefetch = ""

        if self._spawn:
            spawn = "--spawn=%s" % (self.wiki.config.php)
        else:
            spawn = ""

        if not exists(self.wiki.config.php):
            raise BackupError("php command %s not found" % self.wiki.config.php)

        if self._checkpointsEnabled:
            checkpointTime = "--maxtime=%s" % (self.wiki.config.checkpointTime)
            checkpointFile = "--checkpointfile=%s" % outputFile.newFilename(outputFile.dumpName, outputFile.fileType, outputFile.fileExt, outputFile.date, outputFile.chunk, "p%sp%s", None)
        else:
            checkpointTime = ""
            checkpointFile = ""
        scriptCommand = MultiVersion.MWScriptAsArray(runner.wiki.config, "dumpTextPass.php")
        dumpCommand = ["%s" % self.wiki.config.php, "-q"]
        dumpCommand.extend(scriptCommand)
        dumpCommand.extend(["--wiki=%s" % runner.dbName,
                    "%s" % stubOption,
                    "%s" % prefetch,
                    "%s" % runner.forceNormalOption(),
                    "%s" % checkpointTime,
                    "%s" % checkpointFile,
                    "--report=1000",
                    "%s" % spawn
                   ])

        dumpCommand = filter(None, dumpCommand)
        command = dumpCommand
        filters = self.buildFilters(runner, outputFile)
        eta = self.buildEta(runner)
        command.extend([filters, eta])
        pipeline = [command]
        series = [pipeline]
        return series

    # taken from a comment by user "Toothy" on Ned Batchelder's blog (no longer on the net)
    def sort_nicely(self, l):
        """ Sort the given list in the way that humans expect.
        """
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        l.sort(key=alphanum_key)

    def getRelevantPrefetchFiles(self, fileList, startPageID, endPageID, date, runner):
        possibles = []
        if len(fileList):
            # (a) nasty hack, see below (b)
            maxchunks = 0
            for fileObj in fileList:
                if fileObj.isChunkFile and fileObj.chunkInt > maxchunks:
                    maxchunks = fileObj.chunkInt
                if not fileObj.firstPageID:
                    f = DumpFile(self.wiki, runner.dumpDir.filenamePublicPath(fileObj, date), fileObj, self.verbose)
                    fileObj.firstPageID = f.findFirstPageIDInFile()

                        # get the files that cover our range
                for fileObj in fileList:
                # If some of the fileObjs in fileList could not be properly be parsed, some of
                # the (int) conversions below will fail. However, it is of little use to us,
                # which conversion failed. /If any/ conversion fails, it means, that that we do
                # not understand how to make sense of the current fileObj. Hence we cannot use
                # it as prefetch object and we have to drop it, to avoid passing a useless file
                # to the text pass. (This could days as of a comment below, but by not passing
                # a likely useless file, we have to fetch more texts from the database)
                #
                # Therefore try...except-ing the whole block is sufficient: If whatever error
                # occurs, we do not abort, but skip the file for prefetch.
                    try:
                        # If we could properly parse
                        firstPageIdInFile = int(fileObj.firstPageID)

                        # fixme what do we do here? this could be very expensive. is that worth it??
                        if not fileObj.lastPageID:
                            # (b) nasty hack, see (a)
                            # it's not a checkpoint fle or we'd have the pageid in the filename
                            # so... temporary hack which will give expensive results
                            # if chunk file, and it's the last chunk, put none
                            # if it's not the last chunk, get the first pageid in the next chunk and subtract 1
                            # if not chunk, put none.
                            if fileObj.isChunkFile and fileObj.chunkInt < maxchunks:
                                for f in fileList:
                                    if f.chunkInt == fileObj.chunkInt + 1:
                                        # not true!  this could be a few past where it really is
                                        # (because of deleted pages that aren't included at all)
                                        fileObj.lastPageID = str(int(f.firstPageID) - 1)
                        if fileObj.lastPageID:
                            lastPageIdInFile = int(fileObj.lastPageID)
                        else:
                            lastPageIdInFile = None

                            # FIXME there is no point in including files that have just a few rev ids in them
                            # that we need, and having to read through the whole file... could take
                            # hours or days (later it won't matter, right? but until a rewrite, this is important)
                            # also be sure that if a critical page is deleted by the time we try to figure out ranges,
                            # that we don't get hosed
                        if (firstPageIdInFile <= int(startPageID) and (lastPageIdInFile == None or lastPageIdInFile >= int(startPageID))) or (firstPageIdInFile >= int(startPageID) and (endPageID == None or firstPageIdInFile <= int(endPageID))):
                            possibles.append(fileObj)
                    except:
                        runner.debug("Could not make sense of %s for prefetch. Format update? Corrupt file?" % fileObj.filename)
        return possibles

    # this finds the content file or files from the first previous successful dump
    # to be used as input ("prefetch") for this run.
    def _findPreviousDump(self, runner, chunk=None):
        """The previously-linked previous successful dump."""
        if chunk:
            startPageID = sum([self._chunks[i] for i in range(0, int(chunk)-1)]) + 1
            if len(self._chunks) > int(chunk):
                endPageID = sum([self._chunks[i] for i in range(0, int(chunk))])
            else:
                endPageID = None
        else:
            startPageID = 1
            endPageID = None

        dumps = self.wiki.dumpDirs()
        dumps.sort()
        dumps.reverse()
        for date in dumps:
            if date == self.wiki.date:
                runner.debug("skipping current dump for prefetch of job %s, date %s" % (self.name(), self.wiki.date))
                continue

            # see if this job from that date was successful
            if not runner.runInfoFile.statusOfOldDumpIsDone(runner, date, self.name(), self._desc):
                runner.debug("skipping incomplete or failed dump for prefetch date %s" % date)
                continue

            # first check if there are checkpoint files from this run we can use
            files = self.listCheckpointFilesExisting(runner.dumpDir, [self.dumpName], date, chunks=None)
            possiblePrefetchList = self.getRelevantPrefetchFiles(files, startPageID, endPageID, date, runner)
            if len(possiblePrefetchList):
                return possiblePrefetchList

            # ok, let's check for chunk files instead, from any run (may not conform to our numbering
            # for this job)
            files = self.listRegularFilesExisting(runner.dumpDir, [self.dumpName], date, chunks=True)
            possiblePrefetchList = self.getRelevantPrefetchFiles(files, startPageID, endPageID, date, runner)
            if len(possiblePrefetchList):
                return possiblePrefetchList

                    # last shot, get output file that contains all the pages, if there is one
            files = self.listRegularFilesExisting(runner.dumpDir, [self.dumpName], date, chunks=False)
            # there is only one, don't bother to check for relevance :-P
            possiblePrefetchList = files
            files = []
            for p in possiblePrefetchList:
                possible = runner.dumpDir.filenamePublicPath(p, date)
                size = os.path.getsize(possible)
                if size < 70000:
                    runner.debug("small %d-byte prefetch dump at %s, skipping" % (size, possible))
                    continue
                else:
                    files.append(p)
            if len(files):
                return files

        runner.debug("Could not locate a prefetchable dump.")
        return None

    def listOutputFilesForCleanup(self, dumpDir, dumpNames=None):
        files = Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames)
        filesToReturn = []
        if self.pageIDRange:
            if ',' in self.pageIDRange:
                (firstPageID, lastPageID) = self.pageIDRange.split(',', 2)
                firstPageID = int(firstPageID)
                lastPageID = int(lastPageID)
            else:
                firstPageID = int(self.pageIDRange)
                lastPageID = None
            # filter any checkpoint files, removing from the list any with
            # page range outside of the page range this job will cover
            for f in files:
                if f.isCheckpointFile:
                    if not firstPageID or (f.firstPageID and (int(f.firstPageID) >= firstPageID)):
                        if not lastPageID or (f.lastPageID and (int(f.lastPageID) <= lastPageID)):
                            filesToReturn.append(f)
                else:
                    filesToReturn.append(f)
        return filesToReturn

class RecombineXmlDump(XmlDump):
    def __init__(self, name, desc, detail, itemForXmlDumps):
        # no prefetch, no spawn
        self.itemForXmlDumps = itemForXmlDumps
        self._detail = detail
        self._prerequisiteItems = [self.itemForXmlDumps]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpointsEnabled = False

    def listDumpNames(self):
        return self.itemForXmlDumps.listDumpNames()

    def getFileType(self):
        return self.itemForXmlDumps.getFileType()

    def getFileExt(self):
        return self.itemForXmlDumps.getFileExt()

    def getDumpName(self):
        return self.itemForXmlDumps.getDumpName()

    def run(self, runner):
        files = self.itemForXmlDumps.listOutputFilesForInput(runner.dumpDir)
        outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(outputFiles) > 1:
            raise BackupError("recombine XML Dump trying to produce more than one output file")

        error=0
        if not exists(runner.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % runner.wiki.config.bzip2)
        compressionCommand = runner.wiki.config.bzip2
        compressionCommand = "%s > " % runner.wiki.config.bzip2
        uncompressionCommand = ["%s" % runner.wiki.config.bzip2, "-dc"]
        recombineCommandString = self.buildRecombineCommandString(runner, files, outputFiles[0], compressionCommand, uncompressionCommand)
        recombineCommand = [recombineCommandString]
        recombinePipeline = [recombineCommand]
        series = [recombinePipeline]
        error = runner.runCommand([series], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)

        if error:
            raise BackupError("error recombining xml bz2 files")

class XmlMultiStreamDump(XmlDump):
    """Take a .bz2 and recompress it as multistream bz2, 100 pages per stream."""

    def __init__(self, subset, name, desc, detail, itemForRecompression, wiki, chunkToDo, chunks=False, checkpoints=False, checkpointFile=None):
        self._subset = subset
        self._detail = detail
        self._chunks = chunks
        if self._chunks:
            self._chunksEnabled = True
        self._chunkToDo = chunkToDo
        self.wiki = wiki
        self.itemForRecompression = itemForRecompression
        if checkpoints:
            self._checkpointsEnabled = True
        self.checkpointFile = checkpointFile
        self._prerequisiteItems = [self.itemForRecompression]
        Dump.__init__(self, name, desc)

    def getDumpName(self):
        return "pages-" + self._subset

    def listDumpNames(self):
        d = self.getDumpName();
        return [self.getDumpNameMultistream(d), self.getDumpNameMultistreamIndex(d)];

    def getFileType(self):
        return "xml"

    def getIndexFileType(self):
        return "txt"

    def getFileExt(self):
        return "bz2"

    def getDumpNameMultistream(self, name):
        return name + "-multistream"

    def getDumpNameMultistreamIndex(self, name):
        return self.getDumpNameMultistream(name) + "-index"

    def getFileMultistreamName(self, f):
        """assuming that f is the name of an input file,
        return the name of the associated multistream output file"""
        return DumpFilename(self.wiki, f.date, self.getDumpNameMultistream(f.dumpName), f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp)

    def getFileMultistreamIndexName(self, f):
        """assuming that f is the name of a multistream output file,
        return the name of the associated index file"""
        return DumpFilename(self.wiki, f.date, self.getDumpNameMultistreamIndex(f.dumpName), self.getIndexFileType(), self.fileExt, f.chunk, f.checkpoint, f.temp)

    # output files is a list of checkpoint files, otherwise it is a list of one file.
    # checkpoint files get done one at a time. we can't really do parallel recompression jobs of
    # 200 files, right?
    def buildCommand(self, runner, outputFiles):
        # FIXME need shell escape
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.recompressxml):
            raise BackupError("recompressxml command %s not found" % self.wiki.config.recompressxml)

        commandSeries = []
        for f in outputFiles:
            inputFile = DumpFilename(self.wiki, None, f.dumpName, f.fileType, self.itemForRecompression.fileExt, f.chunk, f.checkpoint)
            outfile = runner.dumpDir.filenamePublicPath(self.getFileMultistreamName(f))
            outfileIndex = runner.dumpDir.filenamePublicPath(self.getFileMultistreamIndexName(f))
            infile = runner.dumpDir.filenamePublicPath(inputFile)
            commandPipe = [["%s -dc %s | %s --pagesperstream 100 --buildindex %s > %s"  % (self.wiki.config.bzip2, infile, self.wiki.config.recompressxml, outfileIndex, outfile)]]
            commandSeries.append(commandPipe)
        return commandSeries

    def run(self, runner):
        commands = []
        self.cleanupOldFiles(runner.dumpDir, runner)
        if self.checkpointFile:
            outputFile = DumpFilename(self.wiki, None, self.checkpointFile.dumpName, self.checkpointFile.fileType, self.fileExt, self.checkpointFile.chunk, self.checkpointFile.checkpoint)
            series = self.buildCommand(runner, [outputFile])
            commands.append(series)
        elif self._chunksEnabled and not self._chunkToDo:
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for i in range(1, len(self._chunks)+1):
                outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir, i)
                series = self.buildCommand(runner, outputFiles)
                commands.append(series)
        else:
            outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
            series = self.buildCommand(runner, outputFiles)
            commands.append(series)

        error = runner.runCommand(commands, callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    # shows all files possible if we don't have checkpoint files. without temp files of course
    def listOutputFilesToPublish(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            files.append(self.getFileMultistreamName(f))
            files.append(self.getFileMultistreamIndexName(f))
        return files

    # shows all files possible if we don't have checkpoint files. without temp files of course
    # only the chunks we are actually supposed to do (if there is a limit)
    def listOutputFilesToCheckForTruncation(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            if self._chunkToDo and f.chunkInt != self._chunkToDo:
                continue
            files.append(self.getFileMultistreamName(f))
            files.append(self.getFileMultistreamIndexName(f))
        return files

    # shows all files possible if we don't have checkpoint files. no temp files.
    # only the chunks we are actually supposed to do (if there is a limit)
    def listOutputFilesForBuildCommand(self, dumpDir, chunk=None):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            # if this param is set it takes priority
            if chunk and f.chunkInt != chunk:
                continue
            elif self._chunkToDo and f.chunkInt != self._chunkToDo:
                continue
            # we don't convert these names to the final output form, we'll do that in the build command
            # (i.e. add "multistream" and "index" to them)
            files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
        return files

    # shows all files possible if we don't have checkpoint files. should include temp files
    # does just the chunks we do if there is a limit
    def listOutputFilesForCleanup(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        multistreamNames = []
        for d in dumpNames:
            multistreamNames.extend([self.getDumpNameMultistream(d), self.getDumpNameMultistreamIndex(d)])

        files = []
        if self.itemForRecompression._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
            files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), multistreamNames))
        return files

    # must return all output files that could be produced by a full run of this stage,
    # not just whatever we happened to produce (if run for one chunk, say)
    def listOutputFilesForInput(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            files.append(self.getFileMultistreamName(f))
            files.append(self.getFileMultistreamIndexName(f))
        return files

class BigXmlDump(XmlDump):
    """XML page dump for something larger, where a 7-Zip compressed copy
    could save 75% of download time for some users."""

    def buildEta(self, runner):
        """Tell the dumper script whether to make ETA estimate on page or revision count."""
        return "--full"

class XmlRecompressDump(Dump):
    """Take a .bz2 and recompress it as 7-Zip."""

    def __init__(self, subset, name, desc, detail, itemForRecompression, wiki, chunkToDo, chunks=False, checkpoints=False, checkpointFile=None):
        self._subset = subset
        self._detail = detail
        self._chunks = chunks
        if self._chunks:
            self._chunksEnabled = True
        self._chunkToDo = chunkToDo
        self.wiki = wiki
        self.itemForRecompression = itemForRecompression
        if checkpoints:
            self._checkpointsEnabled = True
        self.checkpointFile = checkpointFile
        self._prerequisiteItems = [self.itemForRecompression]
        Dump.__init__(self, name, desc)

    def getDumpName(self):
        return "pages-" + self._subset

    def getFileType(self):
        return "xml"

    def getFileExt(self):
        return "7z"

    # output files is a list of checkpoint files, otherwise it is a list of one file.
    # checkpoint files get done one at a time. we can't really do parallel recompression jobs of
    # 200 files, right?
    def buildCommand(self, runner, outputFiles):
        # FIXME need shell escape
        if not exists(self.wiki.config.bzip2):
            raise BackupError("bzip2 command %s not found" % self.wiki.config.bzip2)
        if not exists(self.wiki.config.sevenzip):
            raise BackupError("7zip command %s not found" % self.wiki.config.sevenzip)

        commandSeries = []
        for f in outputFiles:
            inputFile = DumpFilename(self.wiki, None, f.dumpName, f.fileType, self.itemForRecompression.fileExt, f.chunk, f.checkpoint)
            outfile = runner.dumpDir.filenamePublicPath(f)
            infile = runner.dumpDir.filenamePublicPath(inputFile)
            commandPipe = [["%s -dc %s | %s a -mx=4 -si %s"  % (self.wiki.config.bzip2, infile, self.wiki.config.sevenzip, outfile)]]
            commandSeries.append(commandPipe)
        return commandSeries

    def run(self, runner):
        commands = []
        # Remove prior 7zip attempts; 7zip will try to append to an existing archive
        self.cleanupOldFiles(runner.dumpDir, runner)
        if self.checkpointFile:
            outputFile = DumpFilename(self.wiki, None, self.checkpointFile.dumpName, self.checkpointFile.fileType, self.fileExt, self.checkpointFile.chunk, self.checkpointFile.checkpoint)
            series = self.buildCommand(runner, [outputFile])
            commands.append(series)
        elif self._chunksEnabled and not self._chunkToDo:
            # must set up each parallel job separately, they may have checkpoint files that
            # need to be processed in series, it's a special case
            for i in range(1, len(self._chunks)+1):
                outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir, i)
                series = self.buildCommand(runner, outputFiles)
                commands.append(series)
        else:
            outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
            series = self.buildCommand(runner, outputFiles)
            commands.append(series)

        error = runner.runCommand(commands, callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)
        if error:
            raise BackupError("error recompressing bz2 file(s)")

    # shows all files possible if we don't have checkpoint files. without temp files of course
    def listOutputFilesToPublish(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
        return files

    # shows all files possible if we don't have checkpoint files. without temp files of course
    # only the chunks we are actually supposed to do (if there is a limit)
    def listOutputFilesToCheckForTruncation(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            if self._chunkToDo and f.chunkInt != self._chunkToDo:
                continue
            files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
        return files

    # shows all files possible if we don't have checkpoint files. no temp files.
    # only the chunks we are actually supposed to do (if there is a limit)
    def listOutputFilesForBuildCommand(self, dumpDir, chunk=None):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            # if this param is set it takes priority
            if chunk and f.chunkInt != chunk:
                continue
            elif self._chunkToDo and f.chunkInt != self._chunkToDo:
                continue
            files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
        return files

    # shows all files possible if we don't have checkpoint files. should include temp files
    # does just the chunks we do if there is a limit
    def listOutputFilesForCleanup(self, dumpDir, dumpNames=None):
        # some stages (eg XLMStubs) call this for several different dumpNames
        if dumpNames == None:
            dumpNames = [self.dumpName]
        files = []
        if self.itemForRecompression._checkpointsEnabled:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listCheckpointFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
            files.extend(self.listTempFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        else:
            # we will pass list of chunks or chunkToDo, or False, depending on the job setup.
            files.extend(self.listRegularFilesPerChunkExisting(dumpDir, self.getChunkList(), dumpNames))
        return files

    # must return all output files that could be produced by a full run of this stage,
    # not just whatever we happened to produce (if run for one chunk, say)
    def listOutputFilesForInput(self, dumpDir):
        files = []
        inputFiles = self.itemForRecompression.listOutputFilesForInput(dumpDir)
        for f in inputFiles:
            files.append(DumpFilename(self.wiki, f.date, f.dumpName, f.fileType, self.fileExt, f.chunk, f.checkpoint, f.temp))
        return files


class RecombineXmlRecompressDump(Dump):
    def __init__(self, name, desc, detail, itemForRecombine, wiki):
        self._detail = detail
        self._desc = desc
        self.wiki = wiki
        self.itemForRecombine = itemForRecombine
        self._prerequisiteItems = [self.itemForRecombine]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpointsEnabled = False
        self._chunksEnabled = False

        def getFileType(self):
            return self.itemForRecombine.getFileType()

        def getFileExt(self):
            return self.itemForRecombine.getFileExt()

        def getDumpName(self):
            return self.itemForRecombine.getDumpName()

    def run(self, runner):
        error = 0
        self.cleanupOldFiles(runner.dumpDir, runner)
        outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir)
        for outputFile in outputFileList:
            inputFiles = []
            files = self.itemForRecombine.listOutputFilesForInput(runner.dumpDir)
            for inFile in files:
                if inFile.dumpName == outputFile.dumpName:
                    inputFiles.append(inFile)
            if not len(inputFiles):
                self.setStatus("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(self.wiki.config.sevenzip):
                raise BackupError("sevenzip command %s not found" % self.wiki.config.sevenzip)
            compressionCommand = "%s a -mx=4 -si" % self.wiki.config.sevenzip
            uncompressionCommand = ["%s" % self.wiki.config.sevenzip, "e", "-so"]

            recombineCommandString = self.buildRecombineCommandString(runner, files, outputFile, compressionCommand, uncompressionCommand)
            recombineCommand = [recombineCommandString]
            recombinePipeline = [recombineCommand]
            series = [recombinePipeline]
            result = runner.runCommand([series], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining xml bz2 file(s)")

class AbstractDump(Dump):
    """XML dump for Yahoo!'s Active Abstracts thingy"""

    def __init__(self, name, desc, chunkToDo, dbName, chunks=False):
        self._chunkToDo = chunkToDo
        self._chunks = chunks
        if self._chunks:
            self._chunksEnabled = True
            self.onlychunks = True
        self.dbName = dbName
        Dump.__init__(self, name, desc)

    def getDumpName(self):
        return "abstract"

    def getFileType(self):
        return "xml"

    def getFileExt(self):
        return ""

    def buildCommand(self, runner, f):
        command = ["/usr/bin/python", "xmlabstracts.py", "--config", runner.wiki.config.files[0],
                    "--wiki", self.dbName, runner.forceNormalOption()]

        outputs = []
        variants = []
        for v in self._variants():
            variantOption = self._variantOption(v)
            dumpName = self.dumpNameFromVariant(v)
            fileObj = DumpFilename(runner.wiki, f.date, dumpName, f.fileType, f.fileExt, f.chunk, f.checkpoint)
            outputs.append(runner.dumpDir.filenamePublicPath(fileObj))
            variants.append(variantOption)

            command.extend(["--outfiles=%s" % ",".join(outputs),
                              "--variants=%s" %  ",".join(variants)])

        if f.chunk:
            # set up start end end pageids for this piece
            # note there is no page id 0 I guess. so we start with 1
            # start = runner.pagesPerChunk()*(chunk-1) + 1
            start = sum([self._chunks[i] for i in range(0, f.chunkInt-1)]) + 1
            startopt = "--start=%s" % start
            # if we are on the last chunk, we should get up to the last pageid,
            # whatever that is.
            command.append(startopt)
            if f.chunkInt < len(self._chunks):
                # end = start + runner.pagesPerChunk()
                end = sum([self._chunks[i] for i in range(0, f.chunkInt)]) +1
                endopt = "--end=%s" % end
                command.append(endopt)
        pipeline = [command]
        series = [pipeline]
        return series

    def run(self, runner):
        commands = []
        # choose the empty variant to pass to buildcommand, it will fill in the rest if needed
        outputFiles = self.listOutputFilesForBuildCommand(runner.dumpDir)
        dumpName0 = self.listDumpNames()[0]
        for f in outputFiles:
            if f.dumpName == dumpName0:
                series = self.buildCommand(runner, f)
                commands.append(series)
        error = runner.runCommand(commands, callbackStderr=self.progressCallback, callbackStderrArg=runner)
        if error:
            raise BackupError("error producing abstract dump")

    # If the database name looks like it's marked as Chinese language,
    # return a list including Simplified and Traditional versions, so
    # we can build separate files normalized to each orthography.
    def _variants(self):
        if self.dbName[0:2] == "zh" and self.dbName[2:3] != "_":
            variants = ["", "zh-cn", "zh-tw"]
        else:
            variants = [""]
        return variants

    def _variantOption(self, variant):
        if variant == "":
            return ""
        else:
            return ":variant=%s" % variant

    def dumpNameFromVariant(self, v):
        dumpNameBase = 'abstract'
        if v == "":
            return dumpNameBase
        else:
            return dumpNameBase + "-" + v

    def listDumpNames(self):
        # need this first for buildCommand and other such
        dumpNames = []
        variants = self._variants()
        for v in variants:
            dumpNames.append(self.dumpNameFromVariant(v))
        return dumpNames

    def listOutputFilesToPublish(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToPublish(self, dumpDir, dumpNames))
        return files

    def listOutputFilesToCheckForTruncation(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesToCheckForTruncation(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForBuildCommand(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForBuildCommand(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForCleanup(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForCleanup(self, dumpDir, dumpNames))
        return files

    def listOutputFilesForInput(self, dumpDir):
        dumpNames =  self.listDumpNames()
        files = []
        files.extend(Dump.listOutputFilesForInput(self, dumpDir, dumpNames))
        return files


class RecombineAbstractDump(Dump):
    def __init__(self, name, desc, itemForRecombine):
        # no chunkToDo, no chunks generally (False, False), even though input may have it
        self.itemForRecombine = itemForRecombine
        self._prerequisiteItems = [self.itemForRecombine]
        Dump.__init__(self, name, desc)
        # the input may have checkpoints but the output will not.
        self._checkpointsEnabled = False

    def getFileType(self):
        return self.itemForRecombine.getFileType()

    def getFileExt(self):
        return self.itemForRecombine.getFileExt()

    def getDumpName(self):
        return self.itemForRecombine.getDumpName()

    def run(self, runner):
        error = 0
        files = self.itemForRecombine.listOutputFilesForInput(runner.dumpDir)
        outputFileList = self.listOutputFilesForBuildCommand(runner.dumpDir)
        for outputFile in outputFileList:
            inputFiles = []
            for inFile in files:
                if inFile.dumpName == outputFile.dumpName:
                    inputFiles.append(inFile)
            if not len(inputFiles):
                self.setStatus("failed")
                raise BackupError("No input files for %s found" % self.name())
            if not exists(runner.wiki.config.cat):
                raise BackupError("cat command %s not found" % runner.wiki.config.cat)
            compressionCommand = "%s > " % runner.wiki.config.cat
            uncompressionCommand = ["%s" % runner.wiki.config.cat]
            recombineCommandString = self.buildRecombineCommandString(runner, inputFiles, outputFile, compressionCommand, uncompressionCommand, "<feed>")
            recombineCommand = [recombineCommandString]
            recombinePipeline = [recombineCommand]
            series = [recombinePipeline]
            result = runner.runCommand([series], callbackTimed=self.progressCallback, callbackTimedArg=runner, shell=True)
            if result:
                error = result
        if error:
            raise BackupError("error recombining abstract dump files")

class TitleDump(Dump):
    """This is used by "wikiproxy", a program to add Wikipedia links to BBC news online"""

    def getDumpName(self):
        return "all-titles-in-ns0"

    def getFileType(self):
        return ""

    def getFileExt(self):
        return "gz"

    def run(self, runner):
        retries = 0
        # try this initially and see how it goes
        maxretries = 3
        query="select page_title from page where page_namespace=0;"
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(files) > 1:
            raise BackupError("page title dump trying to produce more than one output file")
        fileObj = files[0]
        outFilename = runner.dumpDir.filenamePublicPath(fileObj)
        error = self.saveSql(query, outFilename, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.saveSql(query, outFilename, runner)
        if error:
            raise BackupError("error dumping titles list")

    def saveSql(self, query, outfile, runner):
        """Pass some SQL commands to the server for this DB and save output to a gzipped file."""
        if not exists(runner.wiki.config.gzip):
            raise BackupError("gzip command %s not found" % runner.wiki.config.gzip)
        command = runner.dbServerInfo.buildSqlCommand(query, runner.wiki.config.gzip)
        return runner.saveCommand(command, outfile)

class AllTitleDump(TitleDump):

    def getDumpName(self):
        return "all-titles"

    def run(self, runner):
        retries = 0
        maxretries = 3
        query="select page_title from page;"
        files = self.listOutputFilesForBuildCommand(runner.dumpDir)
        if len(files) > 1:
            raise BackupError("all titles dump trying to produce more than one output file")
        fileObj = files[0]
        outFilename = runner.dumpDir.filenamePublicPath(fileObj)
        error = self.saveSql(query, outFilename, runner)
        while error and retries < maxretries:
            retries = retries + 1
            time.sleep(5)
            error = self.saveSql(query, outFilename, runner)
        if error:
            raise BackupError("error dumping all titles list")
