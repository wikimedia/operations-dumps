#!/usr/bin/python3
'''
sample methods illustrating the use and innards of some
classes from the command_management module
'''
import fcntl
import select
import sys
import os
sys.path.append(os.path.dirname(sys.path[0]))
# pylint: disable=wrong-import-position
from dumps.commandmanagement import CommandPipeline, CommandSeries


def run_pipeline(pipeline, quiet=False):
    '''
    run a pipeline of commands that produces output and display
    that output if there is any
    '''
    proc = CommandPipeline(pipeline, quiet=quiet)

    # This method expects output to be small; don't use this for any commands
    # that may produce megabytes of output
    proc.run_pipeline_get_output()

    return_ok = bool(proc.exited_successfully())
    if not return_ok:
        print("Some commands failed:", proc.get_failed_cmds_with_retcode())
    else:
        output = proc.output()
        if output:
            # output returned is in bytes; python supports bytes and unicode strings
            output = output.decode("utf-8").rstrip()

        print("Result:", output)


def setup_polling_for_process(proc):
    '''
    set up a poller for stdout, stderr for the specified process
    '''
    # this code is simplified from that in ProcessMonitor in the command_management module
    poller = select.poll()
    poller.register(proc.stderr, select.POLLIN | select.POLLPRI)
    fderr = proc.stderr.fileno()
    flerr = fcntl.fcntl(fderr, fcntl.F_GETFL)
    fcntl.fcntl(fderr, fcntl.F_SETFL, flerr | os.O_NONBLOCK)
    if proc.stdout:
        poller.register(proc.stdout, select.POLLIN | select.POLLPRI)
        fdout = proc.stdout.fileno()
        flout = fcntl.fcntl(fdout, fcntl.F_GETFL)
        fcntl.fcntl(fdout, fcntl.F_SETFL, flout | os.O_NONBLOCK)
    return poller


def handle_events(poller, waiting, series, proc, quiet):
    '''
    given a list of poll events, read from the appropriate
    file descriptors and return the accumulated stdout and
    error output, if any
    '''
    # this code is simplified from that in ProcessMonitor in the command_management module
    output = ""
    error_out = ""
    command_completed = False
    for (filed, event) in waiting:
        series.in_progress_pipeline().set_poll_state(event)
        # check_poll_ready_for_read checks if the event, which we have
        # stashed in an attribute, has one of the flags
        # select.POLLIN or select.POLLPRI set
        if series.in_progress_pipeline().check_poll_ready_for_read():
            out = os.read(filed, 1024)
            if out:
                if filed == proc.stderr.fileno():
                    error_out = error_out + out.decode("utf-8")
                elif filed == proc.stdout.fileno():
                    output = output + out.decode("utf-8")
            else:
                # possible eof? what would cause this?
                pass
        # check_for_poll_errors checks if the stashed event has one of
        # the flags select.POLLHUP, select.POLLNVAL or select.POLLERR set
        elif series.in_progress_pipeline().check_for_poll_errors():
            poller.unregister(filed)
            # Note: if the fd closed prematurely and the proc then runs for hours to
            # completion, we will get no updates here.
            proc.wait()
            if not quiet:
                print("returned from {pid} with {retcode}".format(
                    pid=proc.pid, retcode=proc.returncode))
            command_completed = True
    return output, error_out, command_completed


def get_series_output(series, quiet=False):
    '''
    run the pipelines in a command series, capture and display the output
    if any, along with any errors
    '''
    # is there some process running that might produce output from one of
    # the pipelines? remember we only run one pipeline at a time, and the
    # series object knows which pipeline is running at any given time
    # and which process is at the end of the pipeline to produce output
    while series.process_producing_output():
        proc = series.process_producing_output()

        # we need to be able to check when there is output to stdout
        # or stderr from the process, and capture it. we accumulate
        # all errors into one string and all output into another,
        # making the assumption that there are not megabytes of either.
        poller = setup_polling_for_process(proc)

        command_completed = False

        # this code is simplified from that in ProcessMonitor in the
        # command_management module, and it has been split up into the
        # smaller methods handle_events and setup_polling_for_process.
        output = ""
        error_out = ""
        while not command_completed:
            # time is in milliseconds
            waiting = poller.poll(500)
            if waiting:
                new_out, new_err, command_completed = handle_events(
                    poller, waiting, series, proc, quiet)
                if new_out:
                    output += new_out
                if new_err:
                    error_out += new_err
        if output:
            print("Result:", output)
        if error_out:
            print("Errors:", error_out)

        # run next command in series, if any
        # this checks to be sure that the current pipeline's last command
        # is indeed complete, before starting the next pipeline;
        # it calls check_for_poll_errors() (it expects to see one)
        # and check_poll_ready_for_read() (it expects this to be false)
        series.continue_commands()


def run_series(series, quiet=False, shell=False):
    '''
    run a command series the pipelines of which may or may not produce
    output, and display the output from each, if any
    '''
    procs = CommandSeries(series, quiet, shell)
    procs.start_commands()
    get_series_output(procs, quiet)

    if not procs.exited_successfully():
        print("Some commands failed:", procs.pipelines_with_errors())


def do_main():
    '''
    entry point
    '''
    # walk one dir back up the tree, heh
    repo_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    if not os.path.exists(os.path.join(repo_path, "dumps/commandmanagement.py")):
        print("Run the copy of this script that resides in the xml/sql dumps repository.")
        sys.exit(1)

    print("")

    # This is a command pipeline: two or more commands as lists, the output of each
    # to be piped to the next.
    # Note that the full path of each command (grep, wc) is given.
    pipeline = [["/bin/grep", "command", os.path.join(repo_path, "dumps/commandmanagement.py")],
                ["/usr/bin/wc", "-l"]]
    print("Running pipeline with default args:")
    print("----------------------")
    run_pipeline(pipeline)
    print("")
    print("Running pipeline with quiet:")
    print("----------------------")
    run_pipeline(pipeline, quiet=True)

    pipeline_one = [["/bin/grep", "command", os.path.join(repo_path, "dumps/commandmanagement.py")],
                    ["/usr/bin/wc", "-l"]]
    pipeline_two = [["/bin/grep", "command", os.path.join(repo_path, "dumps/commandmanagement.py")],
                    ["/usr/bin/wc", "-c"]]
    # This is a command series: two or more pipelines which are run one after the other,
    # waiting for one to complete before the next is started.
    series = [pipeline_one, pipeline_two]

    print("\n=====================\n")

    print("Running series with default args:")
    print("----------------------")
    run_series(series)
    print("")
    print("Running series with quiet:")
    print("----------------------")
    run_series(series, quiet=True)


if __name__ == '__main__':
    do_main()
