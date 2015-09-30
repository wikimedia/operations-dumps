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
import fcntl
import threading

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
    def __init__(self, commands, quiet=False, shell=False):
        if not isinstance(commands, list):
            self._commands = [commands]
        else:
            self._commands = commands
        self._output = None
        self._exit_values = []
        self._last_process_in_pipe = None
        self._first_process_in_pipe = None
        self._last_poll_state = None
        self._processes = []
        self._save_file = None
        self._save_filename = None
        self._quiet = quiet
        self._poller = None
        self._shell = shell
        command_strings = []
        for command in self._commands:
            command_strings.append(" ".join(command))
        self._pipeline_string = " | ".join(command_strings)
        self._last_command_string = None

        # do we write into save_file or append into it (if there is one)?
        self._append = False

        # if this runs in a shell, the shell will manage this stuff
        if not self._shell:
            # if the last command has ">", "filename", then we stick that into save file and toss those two args
            last_command_in_pipe = self._commands[-1]
            if len(last_command_in_pipe) > 1:
                if last_command_in_pipe[-2] == ">":
                    # get the filename
                    self._save_filename = last_command_in_pipe.pop()
                    # lose the > symbol
                    last_command_in_pipe.pop()

            # if the last command has ">>", "filename", then we append into save file and toss those two args.
            last_command_in_pipe = self._commands[-1]
            if len(last_command_in_pipe) > 1:
                if last_command_in_pipe[-2] == ">>":
                    # get the filename
                    self._save_filename = last_command_in_pipe.pop()
                    self._append = True
                    # lose the >> symbol
                    last_command_in_pipe.pop()

    def pipeline_string(self):
        return self._pipeline_string

    def save_file(self):
        return self._save_file

    # note that this (no "b" mode) probably means bad data on windoze...
    # but then this whole module won't run over there :-P
    def open_save_file(self):
        if self._save_filename:
            if self._append:
                self._save_file = open(self._save_filename, "a")
            else:
                self._save_file = open(self._save_filename, "w")

    def subprocess_setup(self):
        # Python installs a SIGPIPE handler by default. This is usually not what
        # non-Python subprocesses expect.
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    def start_commands(self, read_input_from_caller=False):
        previous_process=None
        if self.save_filename():
            if not self.save_file():
                self.open_save_file()
        for command in self._commands:
            command_string = " ".join(command)

            # first process might read from us
            if command == self._commands[0]:
                if read_input_from_caller:
                    stdin_opt = PIPE
                else:
                    stdin_opt = None
            # anything later reads from the prev cmd in the pipe
            else:
                stdin_opt = previous_process.stdout

            # last cmd in pipe might write to an output file
            if (command == self._commands[-1]) and (self.save_file()):
                stdout_opt = self.save_file()
            else:
                stdout_opt = PIPE

            stderr_opt = PIPE

            process = Popen(command, stdout=stdout_opt, stdin=stdin_opt, stderr=stderr_opt,
                            preexec_fn=self.subprocess_setup, shell=self._shell)

            if command == self._commands[0]:
                self._first_process_in_pipe = process
                # because otherwise the parent has these intermediate pipes open
                # and in case of an early close the previous guy in the pipeline
                # will never get sigpipe. (which it should so it can bail)
            if previous_process:
                previous_process.stdout.close()

            if not self._quiet:
                print "command %s (%s) started... " % (command_string, process.pid)
            self._processes.append(process)
            previous_process = process

        self._last_process_in_pipe = process
        self._last_command_string = command_string

    # FIXME if one end of the pipeline completes but others have hung...
    # is this possible?  would we then be screwed?
    # FIXME it's a bit hackish to just close the save_file here and we don't say so.
    def set_return_codes(self):
        # wait for these in reverse order I guess! because..
        # it is possible the last one completed and we haven't gotten
        # the exit status.  then if we try to get the exit status from
        # the ones earlier in the pipe, they will be waiting for
        # it to have exited. and they will hange forever... and we
        # will hang forever in the wait() on them.
        self._processes.reverse()
        for proc in self._processes:
            print "DEBUG: trying to get return code for %s" %  proc.pid
            self._exit_values.append(proc.wait())
            retcode = proc.poll()
            print "DEBUG: return code %s for %s" % (retcode, proc.pid)
        self._exit_values.reverse()
        self._processes.reverse()
        if self.save_file():
            self.save_file().close()


    def is_running(self):
        """Check if process is running."""
        # Note that poll() returns None if the process
        # is not completed, or some value (may be 0) otherwise
        if self._last_process_in_pipe.poll() == None:
            return True
        else:
            return False

    def save_filename(self):
        return self._save_filename

    def exited_successfully(self):
        for value in self._exit_values:
            if value != 0:
                return False
        return True

    def exited_with_errors(self):
        if not self.exited_successfully():
            # we wil return the whole pipeline I guess, they might as well
            # see it in the error report instead of the specific issue in the pipe.
            return self.pipeline_string()
        return None

    # Checks the exit values of the individual commands in the
    # pipeline
    #
    # If each command exited with 0, None is returned.
    # Otherwise, a list is returned, whose entries are pairs
    # containing the error, and the command (as passed to the
    # constructor)
    def get_failed_commands_with_exit_value(self):
        """yields failed commands of a pipeline, along with exit values"""
        failed_commands = []
        for index, exit_value in enumerate(self._exit_values):
            if exit_value != 0:
                failed_commands.append([exit_value, self._commands[index]]);

        if len(failed_commands):
            return failed_commands

        return None

    def process_to_poll(self):
        return self._last_process_in_pipe

    def process_to_write_to(self):
        return self._first_process_in_pipe

    def set_poll_state(self, event):
        self._last_poll_state = event

    def check_poll_ready_for_read(self):
        if not self._last_poll_state:
            # this means we never had a poll return with activity on the current object... which counts as false
            return False
        if self._last_poll_state & (select.POLLIN|select.POLLPRI):
            return True
        else:
            return False

    def check_for_poll_errors(self):
        if not self._last_poll_state:
            # this means we never had a poll return with activity on the current object... which counts as false
            return False
        if self._last_poll_state & select.POLLHUP or self._last_poll_state & select.POLLNVAL or self._last_poll_state & select.POLLERR:
            return True
        else:
            return False

    def readline_alarm_handler(self, signum, frame):
        raise IOError("Hung in the middle of reading a line")

    def get_output_line_with_timeout(self, timeout=None):
        # if there is a save file you are not going to see any output.
        if self.save_file():
            return 0

        if not self._poller:
            self._poller = select.poll()
            self._poller.register(self._last_process_in_pipe.stdout, select.POLLIN|select.POLLPRI)

        # FIXME we should return something reasonable if we unregistered this the last time
        if timeout == None:
            fd_ready = self._poller.poll()
        else:
            # FIXME so poll doesn't take an arg :-P ...?
            fd_ready = self._poller.poll(timeout)

        if fd_ready:
            for (fdesc, event) in fd_ready:
                self.set_poll_state(event)
                if self.check_poll_ready_for_read():
                    signal.signal(signal.SIGALRM, self.readline_alarm_handler)
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
                    out = self._last_process_in_pipe.stdout.readline()

                    # DEBUG
#                    if (out):
#                        sys.stdout.write("DEBUG: got from %s out %s" % (self._lastCommandString, out))


                    signal.alarm(0)
                    return out
                elif self.check_for_poll_errors():
                    self._poller.unregister(fdesc)
                    return None
                else:
                    # it wasn't ready for read but no errors...
                    return 0
        # no poll events
        return 0

    # this returns "immediately" (after 1 millisecond) even if there is nothing to read
    def get_one_line_of_output_if_ready(self):
        # FIXME is waiting a millisecond and returning the best way to do this? Why isn't
        # there a genuine nonblicking poll()??
        return self.get_output_line_with_timeout(timeout=1)

    # this will block waiting for output.
    def get_one_line_of_output(self):
        return self.get_output_line_with_timeout()

    def output(self):
        return self._output

    def get_all_output(self):
        # gather output (from end of pipeline) and record array of exit values
        (stdout, stderr) = self.process_to_poll().communicate()
        self._output  = stdout

    def run_pipeline_get_output(self):
        """Run just the one pipeline, all output is concatenated and can be
        retrieved from self.output.  Redirection to an output file is honored.
        This function will block waiting for output"""
        self.start_commands()
        self.get_all_output()
        self.set_return_codes()

class CommandSeries(object):
    """Run a list of command pipelines in serial (e.g. tar cvfp distro/ distro.tar; chmod 644 distro.tar  )
    It takes as args: series of pipelines (each pipeline is a list of commands)"""
    def __init__(self, commandSeries, quiet=False, shell=False):
        self._command_series = commandSeries
        self._command_pipelines = []
        for pipeline in commandSeries:
            self._command_pipelines.append(CommandPipeline(pipeline, quiet, shell))
        self._in_progress_pipeline = None

    def start_commands(self, read_input_from_caller=False):
        self._command_pipelines[0].start_commands(read_input_from_caller)
        self._in_progress_pipeline = self._command_pipelines[0]

    # This checks only whether the particular pipeline in the series that was
    # running is still running
    def is_running(self):
        if not self._in_progress_pipeline:
            return False
        return self._in_progress_pipeline.is_running()

    def in_progress_pipeline(self):
        """Return which pipeline in the series of commands is running now"""
        return self._in_progress_pipeline

    def process_producing_output(self):
        """Get the last process in the pipeline that is currently running
        This is the one we would be collecting output from"""
        if self._in_progress_pipeline:
            return self._in_progress_pipeline.process_to_poll()
        else:
            return None

    def exited_successfully(self):
        for pipeline in self._command_pipelines:
            if not pipeline.exited_successfully():
                return False
        return True

    def exited_with_errors(self):
        """Return list of commands that exited with errors."""
        commands = []
        for pipeline in self._command_pipelines:
            if not pipeline.exited_successfully():
                command = pipeline.exited_with_errors()
                if command != None:
                    commands.append(command)
        return commands

    def all_output_read_from_pipeline_in_progress(self):
        if self._in_progress_pipeline.check_for_poll_errors() and not self._in_progress_pipeline.check_poll_ready_for_read():
            return True
        # there is no output to read, it's all going somewhere to a file.
        elif not self.in_progress_pipeline()._last_process_in_pipe.stdout:
            return True
        else:
            return False

    def continue_commands(self, get_output=False, read_input_from_caller=False):
        if self._in_progress_pipeline:
            # so we got all the output and the job's not running any more... get exit codes and run the next one
            if self.all_output_read_from_pipeline_in_progress() and not self._in_progress_pipeline.is_running():
                self._in_progress_pipeline.set_return_codes()
                # oohh ohhh start thenext one, w00t!
                index = self._command_pipelines.index(self._in_progress_pipeline)
                if index + 1 < len(self._command_pipelines):
                    self._in_progress_pipeline = self._command_pipelines[index + 1]
                    self._in_progress_pipeline.start_commands(read_input_from_caller)
                else:
                    self._in_progress_pipeline = None

    def get_one_line_of_output_if_ready(self):
        """This will retrieve one line of output from the end of the currently
        running pipeline, if there is something available"""
        return self._in_progress_pipeline.get_one_line_of_output_if_ready()

    def get_one_line_of_output(self):
        """This will retrieve one line of output from the end of the currently
        running pipeline, blocking if necessary"""
        return self._in_progress_pipeline.get_one_line_of_output()

    # FIXME this needs written, but for what use?
    # it also needs tested :-P
    def run_commands(self, read_input_from_caller=False):
        self.start_commands(read_input_from_caller)
        while True:
            self.get_one_line_of_output()
            self.continue_commands()
            if self.all_commands_completed() and not len(self._processes_to_poll):
                break

class ProcessMonitor(threading.Thread):
    def __init__(self, timeout, queue, output_queue, default_callback_interval,
                 callback_stderr, callbackStdout, callback_timed,
                 callback_stderr_arg, callbackStdoutArg, callback_timed_arg):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.queue = queue
        self.output_queue = output_queue
        self._default_callback_interval = default_callback_interval
        self._callback_stderr = callback_stderr
        self._callback_stdout = callbackStdout
        self._callback_timed = callback_timed
        self._callback_stderr_arg = callback_stderr_arg
        self._callback_stdout_arg = callbackStdoutArg
        self._callback_timed_arg = callback_timed_arg

    # one of these as a thread to monitor each command series.
    def run(self):
        series = self.queue.get()
        while series.process_producing_output():
            proc = series.process_producing_output()
            poller = select.poll()
            poller.register(proc.stderr, select.POLLIN|select.POLLPRI)
            fderr = proc.stderr.fileno()
            flerr = fcntl.fcntl(fderr, fcntl.F_GETFL)
            fcntl.fcntl(fderr, fcntl.F_SETFL, flerr | os.O_NONBLOCK)
            if proc.stdout:
                poller.register(proc.stdout, select.POLLIN|select.POLLPRI)
                fd_to_stream = { proc.stdout.fileno(): proc.stdout, proc.stderr.fileno(): proc.stderr }
                fdout = proc.stdout.fileno()
                flout = fcntl.fcntl(fdout, fcntl.F_GETFL)
                fcntl.fcntl(fdout, fcntl.F_SETFL, flout | os.O_NONBLOCK)
            else:
                fd_to_stream = { proc.stderr.fileno(): proc.stderr }

            command_completed = False

            waited = 0
            while not command_completed:
                waiting = poller.poll(self.timeout)
                if waiting:
                    for (filed, event) in waiting:
                        series.in_progress_pipeline().set_poll_state(event)
                        if series.in_progress_pipeline().check_poll_ready_for_read():
                            out = os.read(filed, 1024)
                            if out:
                                if filed == proc.stderr.fileno():
                                    self.output_queue.put(OutputQueueItem(OutputQueueItem.get_stderr_channel(), out))
                                elif filed == proc.stdout.fileno():
                                    self.output_queue.put(OutputQueueItem(OutputQueueItem.get_stdout_channel(), out))
                            else:
                                # possible eof? what would cause this?
                                pass
                        elif series.in_progress_pipeline().check_for_poll_errors():
                            poller.unregister(filed)
                            # FIXME if it closed prematurely and then runs for hours to completion
                            # we will get no updates here...
                            proc.wait()
                            # FIXME put the returncode someplace?
                            print "returned from %s with %s" % (proc.pid, proc.returncode)
                            command_completed = True

                waited = waited + self.timeout
                if waited > self._default_callback_interval and self._callback_timed:
                    if self._callback_timed_arg:
                        self._callback_timed(self._callback_timed_arg)
                    else:
                        self._callback_timed()
                    waited = 0

            # run next command in series, if any
            series.continue_commands()

        # completed the whole series. time to go home.
        self.queue.task_done()

class OutputQueueItem(object):
    def __init__(self, channel, contents):
        self.channel = channel
        self.contents = contents
        self.stdout_channel = OutputQueueItem.get_stdout_channel()
        self.stderr_channel = OutputQueueItem.get_stderr_channel()

    def get_stdout_channel():
        return 1

    def get_stderr_channel():
        return 2

    get_stdout_channel = staticmethod(get_stdout_channel)
    get_stderr_channel = staticmethod(get_stderr_channel)

class CommandsInParallel(object):
    """Run a pile of commandSeries in parallel (e.g. dump articles 1 to 100K,
    dump articles 100K+1 to 200K, ...).  This takes as arguments: a list of series
    of pipelines (each pipeline is a list of commands, each series is a list of
    pipelines), as well as a possible callback which is used to capture all output
    from the various commmand series.  If the callback takes an argument other than
    the line of output, it should be passed in the arg parameter (and it will be passed
    to the callback function first before the output line).  If no callback is provided
    and the individual pipelines are not provided with a file to save output,
    then output is written to stderr.
    Callbackinterval is in milliseconds, defaults is 20 seconds"""
    def __init__(self, command_series_list, callback_stderr=None, callbackStdout=None, callback_timed=None, callback_stderr_arg=None, callbackStdoutArg=None, callback_timed_arg=None, quiet=False, shell=False, callback_interval=20000):
        self._command_series_list = command_series_list
        self._command_serieses = []
        for series in self._command_series_list:
            self._command_serieses.append(CommandSeries(series, quiet, shell))
        # for each command series running in parallel,
        # in cases where a command pipeline in the series generates output, the callback
        # will be called with a line of output from the pipeline as it becomes available
        self._callback_stderr = callback_stderr
        self._callback_stdout = callbackStdout
        self._callback_timed = callback_timed
        self._callback_stderr_arg = callback_stderr_arg
        self._callback_stdout_arg = callbackStdoutArg
        self._callback_timed_arg = callback_timed_arg
        self._command_series_queue = Queue.Queue()
        self._output_queue = Queue.Queue()
        self._normal_thread_count = threading.activeCount()

        # number millisecs we will wait for select.poll()
        self._default_poll_time = 500

        # for programs that don't generate output, wait this many milliseconds between
        # invoking callback if there is one
        self._default_callback_interval = callback_interval

    def start_commands(self):
        for series in self._command_serieses:
            series.start_commands()

    def setup_output_monitoring(self):
        for series in self._command_serieses:
            self._command_series_queue.put(series)
            thrd = ProcessMonitor(500, self._command_series_queue, self._output_queue, self._default_callback_interval, self._callback_stderr, self._callback_stdout, self._callback_timed, self._callback_stderr_arg, self._callback_stdout_arg, self._callback_timed_arg)
            thrd.start()

    def all_commands_completed(self):
        """Check if all series have run to completion."""
        for series in self._command_serieses:
            if series.in_progress_pipeline():
                # something is still running
                return False
        return True

    def exited_successfully(self):
        for series in self._command_serieses:
            if not series.exited_successfully():
                return False
        return True

    def commands_with_errors(self):
        commands = []
        for series in self._command_serieses:
            if not series.exited_successfully():
                commands.extend(series.exited_with_errors())
        return commands

    def watch_output_queue(self):
        done = False
        while not done:
            # check the number of threads active, if they are all gone we are done
            if threading.activeCount() == self._normal_thread_count:
                done = True
            output = None
            try:
                output = self._output_queue.get(True, 1)
            except:
                pass
            if output:
                if output.channel == OutputQueueItem.get_stdout_channel():
                    if self._callback_stdout:
                        if self._callback_stdout_arg:
                            self._callback_stdout(self._callback_stdout_arg, output.contents)
                        else:
                            self._callback_stdout(output.contents)
                    else:
                        sys.stderr.write(output.contents)
                else: # output channel is stderr
                    if self._callback_stderr:
                        if self._callback_stderr_arg:
                            self._callback_stderr(self._callback_stderr_arg, output.contents)
                        else:
                            self._callback_stderr(output.contents)
                    else:
                        sys.stderr.write(output.contents)

    def run_commands(self):
        self.start_commands()
        self.setup_output_monitoring()
        self.watch_output_queue()
#        self._commandSeriesQueue.join()


def testcallback(output=None):
    output_file = open("/home/ariel/src/mediawiki/testing/outputsaved.txt", "a")
    if output == None:
        output_file.write("no output for me.\n")
    else:
        output_file.write(output)
    output_file.close()

def main():
    command1 = ["/usr/bin/vmstat", "1", "10"]
    command2 = ["/usr/sbin/lnstat", "-i", "7", "-c", "5", "-k", "arp_cache:entries,rt_cache:in_hit,arp_cache:destroys", ">", "/home/ariel/src/mediawiki/testing/savelnstat.txt"]
    command3 = ["/usr/bin/iostat", "9", "2"]
    command4 = ['/bin/touch', "/home/ariel/src/mediawiki/testing/touchfile"]
    command5 = ["/bin/grep", "write", "/home/ariel/src/mediawiki/testing/mysubsagain.py"]
    command6 = ["/bin/grep", "-v", "FIXME"]
    # this file does not end in a newline. let's see what happens.
    command7 = ["/bin/cat", "/home/ariel/src/mediawiki/testing/blob"]
    pipeline1 = [command1]
    pipeline2 = [command2]
    pipeline3 = [command3]
    pipeline4 = [command4]
    pipeline5 = [command5, command6]
    pipeline6 = [command7]
    series1 = [pipeline1, pipeline4]
    series2 = [pipeline2]
    series3 = [pipeline3]
    series4 = [pipeline5]
    series5 = [pipeline6]
    parallel = [series1, series2, series3, series4, series5]
    commands = CommandsInParallel(parallel, callbackStdout=testcallback)
    commands.run_commands()
    if commands.exited_successfully():
        print "w00t!"
    else:
        print "big bummer!"

if __name__ == "__main__":
    main()
