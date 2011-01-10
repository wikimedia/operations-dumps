import getopt
import os
import re
import sys
import time
import subprocess
import select
import signal
import Queue
import thread

from os.path import dirname, exists, getsize, join, realpath
from subprocess import Popen, PIPE

# FIXME no explicit stderr handling, is this ok?

class CommandPipeline(object):
	"""Run a series of commands in a pipeline, e.g.  ps -ef | grep convert 
	The pipeline can be one command long (in which case nothing special happens)
	It takes as args: list of commands in the pipeline (each command is a list: command name and args)
	If the last command in the pipeline has at the end of the arg list > filename then
	the output of the pipeline will be written into the specified file.
	If the last command in the pipeline has at the end of the arg list >> filename then
	the output of the pipeline will be appended to the specified file.
	"""
	def __init__(self, commands, quiet = False, shell = False):
		if (not isinstance(commands,list)):
			self._commands = [ commands ]
		else:
			self._commands = commands
		self._output = None
		self._exitValues = []
		self._lastProcessInPipe = None
		self._firstProcessInPipe = None
		self._lastPollState = None
		self._processes = []
		self._saveFile = None
		self._saveFileName = None
		self._quiet = quiet
		self._poller = None
		self._shell = shell
		commandStrings = []
		for c in self._commands:
			commandStrings.append(" ".join(c))
		self._pipelineString = " | ".join(commandStrings)
		self._lastCommandString = None

		# do we write into saveFile or append into it (if there is one)?
		self._append = False

		# if this runs in a shell, the shell will manage this stuff
		if (not self._shell):
			# if the last command has ">", "filename", then we stick that into save file and toss those two args
			lastCommandInPipe = self._commands[-1]
			if (len(lastCommandInPipe) > 1):
				if (lastCommandInPipe[-2] == ">"):
					# get the filename
					self._saveFileName = lastCommandInPipe.pop()
					# lose the > symbol
					lastCommandInPipe.pop()

			# if the last command has ">>", "filename", then we append into save file and toss those two args. 
			lastCommandInPipe = self._commands[-1]
			if (len(lastCommandInPipe) > 1):
				if (lastCommandInPipe[-2] == ">>"):
					# get the filename
					self._saveFileName = lastCommandInPipe.pop()
					self._append = True
					# lose the >> symbol
					lastCommandInPipe.pop()
			
	def pipelineString(self):
		return self._pipelineString

	def saveFile(self):
		return self._saveFile

	# note that this (no "b" mode) probably means bad data on windoze...
	# but then this whole module won't run over there :-P
	def openSaveFile(self):
		if (self._saveFileName):
			if (self._append):
				self._saveFile = open(self._saveFileName,"a")
			else:
				self._saveFile = open(self._saveFileName,"w")
		
	def subprocess_setup(self):
		# Python installs a SIGPIPE handler by default. This is usually not what
		# non-Python subprocesses expect.
		signal.signal(signal.SIGPIPE, signal.SIG_DFL)

	def startCommands(self, readInputFromCaller=False):
		previousProcess=None
		if self.saveFileName():
			if (not self.saveFile()):
				self.openSaveFile()
		for command in self._commands:
			commandString = " ".join(command)

			# first process might read from us
			if (command == self._commands[0]):
				if (readInputFromCaller):
					stdinOpt = PIPE
				else:
					stdinOpt = None
			# anything later reads from the prev cmd in the pipe
			else:
				stdinOpt = previousProcess.stdout

			# last cmd in pipe might write to an output file
			if (command == self._commands[-1]) and (self.saveFile()):
				stdoutOpt = self.saveFile()
			else:
				stdoutOpt = PIPE

			stderrOpt = PIPE

			process = Popen( command, stdout=stdoutOpt, stdin=stdinOpt, stderr=stderrOpt,
					 preexec_fn=self.subprocess_setup, shell= self._shell)
			
			if (command == self._commands[0]):
				self._firstProcessInPipe = process
				# because otherwise the parent has these intermediate pipes open
				# and in case of an early close the previous guy in the pipeline
				# will never get sigpipe. (which it should so it can bail)
			if (previousProcess):
			        previousProcess.stdout.close()

			if not self._quiet:
				print "command %s (%s) started... " % (commandString, process.pid)
			self._processes.append( process )
			previousProcess = process

		self._lastProcessInPipe = process
		self._lastCommandString = commandString

	# FIXME if one end of the pipeline completes but others have hung...
	# is this possible?  would we then be screwed?
	# FIXME it's a bit hackish to just close the saveFile here and we don't say so.
	def setReturnCodes(self):
		# wait for these in reverse order I guess! because..
		# it is possible the last one completed and we haven't gotten
		# the exit status.  then if we try to get the exit status from
		# the ones earlier in the pipe, they will be waiting for
		# it to have exited. and they will hange forever... and we
		# will hang forever in the wait() on them.
		self._processes.reverse()
		for p in self._processes:
			print "DEBUG: trying to get return code for %s" %  p.pid
			self._exitValues.append(p.wait())
			retcode = p.poll() 
			print "DEBUG: return code %s for %s" % (retcode, p.pid)
		self._exitValues.reverse()
		self._processes.reverse()
		if (self.saveFile()):
			self.saveFile().close()

		
	def isRunning(self):
		"""Check if process is running."""
		# Note that poll() returns None if the process
		# is not completed, or some value (may be 0) otherwise
		if (self._lastProcessInPipe.poll() == None):
			return(True)
		else:
			return(False)

	def saveFileName(self):
		return self._saveFileName

	def exitedSuccessfully(self):
		for v in self._exitValues:
			if v != 0:
				return False
		return True

	def exitedWithErrors(self):
		if not self.exitedSuccessfully():
			# we wil return the whole pipeline I guess, they might as well 
			# see it in the error report instead of the specific issue in the pipe.
			return self.pipelineString()
		return None

	def processToPoll(self):
		return self._lastProcessInPipe

	def processToWriteTo(self):
		return self._firstProcessInPipe

	def setPollState(self, event):
		self._lastPollState = event

	def checkPollReadyForRead(self):
		if (not self._lastPollState):
			# this means we never had a poll return with activity on the current object... which counts as false
			return False
		if (self._lastPollState & (select.POLLIN|select.POLLPRI)):
			return True
		else:
			return False

	def checkForPollErrors(self):
		if (not self._lastPollState):
			# this means we never had a poll return with activity on the current object... which counts as false
			return False
		if (self._lastPollState & select.POLLHUP or self._lastPollState & select.POLLNVAL or self._lastPollState & select.POLLERR):
			return True
		else:
			return False

	def readlineAlarmHandler(self, signum, frame):
		raise IOError("Hung in the middle of reading a line")

	def getOneLineOfOutputWithTimeout(self, timeout = None):
		# if there is a save file you are not going to see any output.
		if (self.saveFile()):
			return(0)

		if (not self._poller):
			self._poller = select.poll()
			self._poller.register(self._lastProcessInPipe.stdout,select.POLLIN|select.POLLPRI)

		# FIXME we should return something reasonable if we unregistered this the last time
		if (timeout == None):
			fdReady = self._poller.poll()
		else:
			# FIXME so poll doesn't take an arg :-P ...?
			fdReady = self._poller.poll(timeout)

		if (fdReady):
			for (fd,event) in fdReady:
				self.setPollState(event)
				if (self.checkPollReadyForRead()):
					signal.signal(signal.SIGALRM, self.readlineAlarmHandler)
					# exception after 5 seconds, in case something happened
					# to the other end of the pipe (like the writing process
					# blocked in the middle of the write)
					signal.alarm(5)
					# FIXME we might have buffered output which we read
					# part of and then the rest of the line is in 
					# the next (not full and so not written to us)
					# buffer... how can we fix this??
					# we could do our own readline I guess, accumulate
					# til there is no more data, indicate it's a partial 
					# line, let it get written to the caller anyways...
					# when we poll do we get a byte count of how much is available? no.
					out = self._lastProcessInPipe.stdout.readline()

					# DEBUG
#					if (out):
#						sys.stdout.write("DEBUG: got from %s out %s" % (self._lastCommandString, out))


					signal.alarm(0)
					return(out)
				elif self.checkForPollErrors():
					self._poller.unregister(fd)
					return None
				else:
					# it wasn't ready for read but no errors...
					return 0
		# no poll events
		return 0

	# this returns "immediately" (after 1 millisecond) even if there is nothing to read
	def getOneLineOfOutputIfReady(self):
		# FIXME is waiting a millisecond and returning the best way to do this? Why isn't
		# there a genuine nonblicking poll()??
		return (self.getOneLineOfOutputWithTimeout(timeout = 1))

	# this will block waiting for output.
	def getOneLineOfOutput(self):
		return (self.getOneLineOfOutputWithTimeout())

	def output(self):
		return self._output

	def getAllOutput(self):
		# gather output (from end of pipeline) and record array of exit values
		(stdout, stderr) = self.processToPoll().communicate()
		self._output  = stdout

	def runPipelineAndGetOutput(self):
		"""Run just the one pipeline, all output is concatenated and can be
		retrieved from self.output.  Redirection to an output file is honored.
		This function will block waiting for output"""
		self.startCommands()
		self.getAllOutput()
		self.setReturnCodes()

class CommandSeries(object):
	"""Run a list of command pipelines in serial ( e.g. tar cvfp distro/ distro.tar; chmod 644 distro.tar   )
	It takes as args: series of pipelines (each pipeline is a list of commands)"""
	def __init__(self, commandSeries, quiet = False, shell = False):
		self._commandSeries = commandSeries
		self._commandPipelines = []
		for pipeline in commandSeries:
			self._commandPipelines.append( CommandPipeline(pipeline, quiet, shell) )
		self._inProgressPipeline = None

	def startCommands(self, readInputFromCaller=False):
		self._commandPipelines[0].startCommands(readInputFromCaller)
		self._inProgressPipeline = self._commandPipelines[0]

	# This checks only whether the particular pipeline in the series that was
	# running is still running
	def isRunning(self):
		if not self._inProgressPipeline:
			return False
		return(self._inProgressPipeline.isRunning())

	def inProgressPipeline(self):
		"""Return which pipeline in the series of commands is running now"""
		return self._inProgressPipeline

	def processProducingOutput(self):
		"""Get the last process in the pipeline that is currently running
		This is the one we would be collecting output from"""
		if self._inProgressPipeline:
			return self._inProgressPipeline.processToPoll()
		else:
			return None

	def exitedSuccessfully(self):
		for pipeline in self._commandPipelines:
			if not pipeline.exitedSuccessfully():
				return False
		return True

	def exitedWithErrors(self):
		"""Return list of commands that exited with errors."""
		commands = []
		for pipeline in self._commandPipelines:
			if not pipeline.exitedSuccessfully():
				command = pipeline.exitedWithErrors()
				if command != None:
					commands.append(command)
		return(commands)

	def allOutputReadFromPipelineInProgress(self):
		if (self._inProgressPipeline.checkForPollErrors() and not self._inProgressPipeline.checkPollReadyForRead()):
			return True
		# there is no output to read, it's all going somewhere to a file.
		elif not self.inProgressPipeline()._lastProcessInPipe.stdout:
			return True
		else:
			return False
	
	def continueCommands(self, getOutput=False, readInputFromCaller=False):
		if self._inProgressPipeline:
			# so we got all the output and the job's not running any more... get exit codes and run the next one
			if self.allOutputReadFromPipelineInProgress() and not self._inProgressPipeline.isRunning():
				self._inProgressPipeline.setReturnCodes()
				# oohh ohhh start thenext one, w00t!
				index = self._commandPipelines.index(self._inProgressPipeline)
				if index + 1 < len(self._commandPipelines):
					self._inProgressPipeline = self._commandPipelines[index + 1]
					self._inProgressPipeline.startCommands(readInputFromCaller)
				else:
					self._inProgressPipeline = None

	def getOneLineOfOutputIfReady(self):
		"""This will retrieve one line of output from the end of the currently 
		running pipeline, if there is something available"""
		return(self._inProgressPipeline.getOneLineOfOutputIfReady())

	def getOneLineOfOutput(self):
		"""This will retrieve one line of output from the end of the currently 
		running pipeline, blocking if necessary"""
		return(self._inProgressPipeline.getOneLineOfOutput())

	# FIXME this needs written, but for what use?
	# it also needs tested :-P
	def runCommands(self, readInputFromCaller = False):
		self.startCommands(readInputFromCaller)
		while True:
			self.getOneLineOfOutput()
			self.continueCommands()
			if (self.allCommandsCompleted() and not len(self._processesToPoll)):
				break

class CommandsInParallel(object):
	"""Run a pile of commandSeries in parallel ( e.g. dump articles 1 to 100K, 
	dump articles 100K+1 to 200K, ...).  This takes as arguments: a list of series 
	of pipelines (each pipeline is a list of commands, each series is a list of 
	pipelines), as well as a possible callback which is used to capture all output
	from the various commmand series.  If the callback takes an argument other than
	the line of output, it should be passed in the arg parameter (and it will be passed
	to the callback function first before the output line).  If no callback is provided 
	and the individual pipelines are not provided with a file to save output, 
	then output is written 	to stderr."""
	def __init__(self, commandSeriesList, callback = None, arg=None, quiet = False, shell = False ):
		self._commandSeriesList = commandSeriesList
		self._commandSerieses = []
		for series in self._commandSeriesList:
			self._commandSerieses.append( CommandSeries(series, quiet, shell) )
		# for each command series running in parallel,
		# in cases where a command pipeline in the series generates output, the callback
		# will be called with a line of output from the pipeline as it becomes available
		self._callback = callback
		self._arg = arg
		self._commandSeriesQueue = Queue.Queue()

		# number millisecs we will wait for select.poll()
		self._defaultPollTime = 500
		
		# for programs that don't generate output, wait this many seconds between 
		# invoking callback if there is one
		self._defaultCallbackInterval = 20

	def startCommands(self):
		for series in self._commandSerieses:
			series.startCommands()

	# one of these as a thread to monitor each command series.
	def seriesMonitor(self, timeout, queue):
		series = queue.get()
		poller = select.poll()
		while series.processProducingOutput():
			p = series.processProducingOutput()
			poller.register(p.stderr,select.POLLIN|select.POLLPRI)
			if (p.stdout):
				fdToStream = { p.stdout.fileno(): p.stdout, p.stderr.fileno(): p.stderr }
			else:
				fdToStream = { p.stderr.fileno(): p.stderr }
			# if we have a savefile, this won't be set. 
			if (p.stdout):
				poller.register(p.stdout,select.POLLIN|select.POLLPRI)

				commandCompleted = False

				while not commandCompleted:
					waiting = poller.poll(self._defaultPollTime)
					if (waiting):
						for (fd,event) in waiting:
							series.inProgressPipeline().setPollState(event)
							if series.inProgressPipeline().checkPollReadyForRead():

								# so what happens if we get more than one line of output
								# in the poll interval? it will sit there waiting... 
								# could have accumulation. FIXME. for our purposes we want 
								# one line only, the latest. but for other uses of this 
								# module?  really we should read whatever is available only,
								# pass it to callback, let callback handle multiple lines,
								# partial lines etc.
								# out = p.stdout.readline()
								out = fdToStream[fd].readline()
								if out:
									if self._callback:
										if (self._arg):
											self._callback(self._arg, out)
										else:
											self._callback(out)
									else:
										# fixme this behavior is different, do we want it?
										sys.stderr.write(out)   
								else:
									# possible eof? (empty string from readline)
									pass
							elif series.inProgressPipeline().checkForPollErrors():
								poller.unregister(fd)
								p.wait()
								# FIXME put the returncode someplace?
								print "returned from %s with %s" % (p.pid, p.returncode)
								commandCompleted = True
						if commandCompleted:
							break


				# run next command in series, if any
				series.continueCommands()

			else:
				# no output from this process, just wait for it and do callback if there is one
				waited = 0
				while p.poll() == None:
					if waited > self._defaultCallbackInterval and self._callback:
						if (self._arg):
							self._callback(self._arg)
						else:
							self._callback()
						waited = 0
					time.sleep(1)
					waited = waited + 1

				print "returned from %s with %s" % (p.pid, p.returncode)
				series.continueCommands()

		# completed the whole series. time to go home.
		queue.task_done()


	def setupOutputMonitoring(self):
		for series in self._commandSerieses:
			self._commandSeriesQueue.put(series)
			thread.start_new_thread(self.seriesMonitor, (500, self._commandSeriesQueue))

	def allCommandsCompleted(self):
		"""Check if all series have run to completion."""
		for series in self._commandSerieses:
			if series.inProgressPipeline():
				# something is still running
				return False
		return True

	def exitedSuccessfully(self):
		for series in self._commandSerieses:
			if not series.exitedSuccessfully():
				return False
		return True

	def commandsWithErrors(self):
		commands = []
		for series in self._commandSerieses:
			if not series.exitedSuccessfully():
				commands.extend(series.exitedWithErrors())
		return(commands)

	def runCommands(self):
		self.startCommands()
		self.setupOutputMonitoring()
		self._commandSeriesQueue.join()


def testcallback(output = None):
	outputFile = open("/home/ariel/src/mediawiki/testing/outputsaved.txt","a")
	if (output == None):
		outputFile.write( "no output for me.\n" )
	else:
		outputFile.write(output)
	outputFile.close()

if __name__ == "__main__":
	command1 = [ "/usr/bin/vmstat", "1", "10" ]
	command2 = [ "/usr/sbin/lnstat", "-i", "7", "-c", "5", "-k", "arp_cache:entries,rt_cache:in_hit,arp_cache:destroys", ">", "/home/ariel/src/mediawiki/testing/savelnstat.txt" ]
	command3 = [ "/usr/bin/iostat", "9", "2" ]
	command4 = [ '/bin/touch', "/home/ariel/src/mediawiki/testing/touchfile" ]
	command5 = [ "/bin/grep", "write", "/home/ariel/src/mediawiki/testing/mysubsagain.py" ]
	command6 = [ "/bin/grep", "-v", "FIXME" ]
	# this file does not end in a newline. let's see what happens.
	command7 = [ "/bin/cat", "/home/ariel/src/mediawiki/testing/blob" ]
	pipeline1 = [ command1 ]
	pipeline2 = [ command2 ]
	pipeline3 = [ command3 ]
	pipeline4 = [ command4 ]
	pipeline5 = [ command5, command6 ]
	pipeline6 = [ command7 ]
	series1 = [ pipeline1, pipeline4 ]
	series2 = [ pipeline2 ]
	series3 = [ pipeline3 ]
	series4 = [ pipeline5 ]
	series5 = [ pipeline6 ]
	parallel = [ series1, series2, series3, series4, series5 ]
	commands = CommandsInParallel(parallel, callback=testcallback)
	commands.runCommands()
	if commands.exitedSuccessfully():
		print "w00t!"
	else:
		print "big bummer!"


