'''
run a set of commands in a given order,
given information about how many free slots
the host has for such processes and how many
slots each process takes.
'''
import os
import sys
import getopt
from subprocess import Popen
import logging
import time
import smtplib
import email.mime.text
import json
import traceback
import signal


LOG = logging.getLogger(__name__)


def line_to_entry(line, total_slots):
    '''
    convert a line of text representing a command to be run,
    into an entry describing the command info

    line has content: slots count onfailure errornotify command
    onfailure may be one of continue exit retry
    '''
    slots, count, onfailure, errornotify, command = line.split(' ', 4)

    if count == 'max':
        # figure out how many jobs can run at once given the
        # total slots available
        count = str(total_slots / int(slots))

    return {'slots': int(slots),
            'count': int(count),
            'command': command,
            'onfailure': onfailure,
            'errornotify': errornotify,
            'processes': [],
            'processids': [],
            'procidsfromcache': [],
            'done': 0}


def json_obj_dump(obj):
    '''
    catch-all json encoder for any objects that don't have a default method
    '''
    try:
        return obj.toJSON()
    except AttributeError:
        return obj.__dict__


class Scheduler(object):
    '''
    handle running a sequence of commands, each command possibly to
    be run multiple times, each command possibly using more than
    one 'slot' of resources (think cpu)
    '''

    def __init__(self, file_p, slots, cache, mailhost,
                 email_from, formatvars, restore, rerun):
        '''
        constructor
        also define a unique id that is set in the environment of every command
        run and can be used later if this script dies, to check to see if a command
        with the same pid and environment variable is still running
        '''

        signal.signal(signal.SIGHUP, self.handle_hup)

        self.input = file_p
        self.total_slots = slots
        self.commands = []
        self.free_slots = slots
        self.cache = cache
        self.mailhost = mailhost
        self.restore = restore
        self.rerun = rerun
        self.pid = os.getpid()
        self.my_id = "%s%d%s" % (time.strftime("%Y%m%d%H%M%S", time.gmtime()),
                                 self.pid, os.geteuid())
        self.my_prefix = 'PYMGR_ID'
        self.email_from = email_from
        self.formatvars = self.format_convert(formatvars)

    def handle_hup(self, signo_unused, frame_unused):
        """
        ignore any more hups
        shoot all children
        close all the fds except the big three
        re-exec ourselves
        """
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        for command in self.commands:
            if 'processids' in command:
                for pid in command['processids']:
                    os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
                for pid in command['processids']:
                    # no zombie apocalypse here
                    try:
                        os.waitpid(int(pid), 0)
                    except OSError:
                        pass

        for filedesc in reversed(range(os.sysconf('SC_OPEN_MAX'))):
            if filedesc not in [sys.__stdin__, sys.__stdout__, sys.__stderr__]:
                try:
                    filedesc.close()
                except IOError:
                    pass
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def format_convert(self, names_values):
        '''
        expect a string like name1=val1,name2=val2...
        return a dict {'name1': 'val1', 'name2': 'val2'...}
        suitable for use by format()
        '''
        if names_values is None:
            return names_values
        pairs = names_values.split(',')
        converted = {}
        for pair in pairs:
            name, val = pair.split('=')
            converted[name] = val
        return converted

    def run(self):
        '''
        run all commands in order.
        if this script died and was restarted with the option to
        restore from cache, it will restore its state and check the
        commands that were running at the time of interruption,
        rerunning them after they complete, if specified.
        this script has no way to tell if such processes ran to
        successful completion which is why a rerun might be a good choice.

        waits 30 seconds in between starting up new commands, assuming
        free slots (e.g. cpu resources) are available.
        '''

        if self.cache and self.restore:
            self.restore_from_cache()
            if self.rerun:
                for entry in self.commands:
                    entry['rerun'] = True
        else:
            self.read_commands()
        os.environ[self.my_prefix] = self.my_id

        while True:
            if self.start_command() is None:
                self.save_to_cache()
                LOG.info("all command sets completed.")
                break
            # do we want this configurable? meh
            # 30 seconds is fine, we're dealing with long running commands here
            time.sleep(30)

    def read_commands(self):
        '''
        read text entries describing each set of commands to be run
        '''

        for line in self.input:
            line = line.rstrip('\n')
            if line.startswith('#') or line.startswith(" ") or not line:
                continue
            if self.formatvars is not None:
                line = line.format(**self.formatvars)
            self.commands.append(line_to_entry(line, self.total_slots))
        if self.input != sys.stdin:
            self.input.close()

    def check_pid(self, pid):
        '''
        see if process with given pid is running
        and if we started it. if the process was running
        when this script was interrupted and we restored
        from cache, we check that the special environment
        variable is set for the process, to ensure the pid
        didn't get reused
        '''

        id_string = self.my_prefix + "=" + self.my_id
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        # it exists, is it the same command?
        try:
            process_environ = open("/proc/%s/environ" % pid, "r")
        except IOError:
            # permission or gone, anyways not us
            return False
        for line in process_environ:
            if line:
                fields = line.split("\x00")
                for field in fields:
                    if field == id_string:
                        process_environ.close()
                        return True
        process_environ.close()
        return False

    def check_process_done(self, process, pid):
        '''
        will return True, plus the process returncode if it is available
        or if process not done, will return False, None
        '''

        if process is not None:
            process.poll()
            if process.returncode is not None:
                return (True, process.returncode)
            else:
                return (False, None)
        else:
            return (self.check_pid(pid), None)

    def mark_process_done(self, process, pid, entry):
        '''
        remove process from list of running processes for this command set
        and add it to the done count for this command set
        '''

        LOG.info("freeing up %s slot(s)s for completed process %s",
                 str(entry['slots']), str(pid))
        if process is not None:
            entry['processes'].remove(process)
        entry['processids'].remove(pid)
        entry['done'] += 1
        self.free_slots += entry['slots']
        if self.free_slots > self.total_slots:
            self.free_slots = self.total_slots

    def notify_failure_email(self, entry, pid, retcode):
        '''
        send email if a command fails, if email host
        is set and email notification is requested for
        the command set
        '''

        if entry['errornotify'] == 'none':
            return
        if self.mailhost is None:
            return

        address = entry['errornotify']
        email_text = '''
        Notification from dumpstager:

        Command failed: {0}
        process id: {1} return code: {1}
'''
        text_formatted = email_text.format(
            entry['command'], pid, retcode if retcode is not None else "Unknown")
        message = email.mime.text.MIMEText(text_formatted)
        message["Subject"] = "Failure of command from dumpscheduler"
        message["From"] = self.email_from
        message["To"] = address
        try:
            server = smtplib.SMTP(self.mailhost)
            server.sendmail(message['From'], self.email_from,
                            message.as_string())
            server.close()
        except smtplib.SMTPException:
            LOG.error('problem sending mail to %s', address)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            except_message = repr(traceback.format_exception(
                exc_type, exc_value, exc_traceback))
            LOG.error(except_message)
            LOG.error(message.as_string())

    def handle_nonzero_retcode(self, process, pid, entry):
        '''
        a command has failed. email notification if needed,
        set up for retry if needed
        or mark as done and go on to the next command in the set
        or series
        '''

        if entry['errornotify']:
            self.notify_failure_email(
                entry, pid,
                process.returncode if process is not None else None)

        LOG.error("Command failed: %s", entry['command'])
        LOG.error("process id: %s, return code: %s", pid,
                  process.returncode if process is not None else "Unknown")

        if entry['onfailure'] == 'continue':
            self.mark_process_done(process, pid, entry)
        elif entry['onfailure'].startswith('retry='):
            retries = entry['onfailure'].split("=", 1)[1]
            if not retries.isdigit():
                LOG.error("number of retries must be an integer but "
                          "found '%s', continuing", retries)
                entry['onfailure'] = 'continue'
            else:
                if process is not None:
                    entry['processes'].remove(process)
                entry['processids'].remove(pid)
                if pid in entry['procidsfromcache']:
                    entry['procidsfromcache'].remove(pid)
                entry['count'] = entry['count'] + 1
                LOG.info("after failure, retry for %s scheduled", entry['command'])
                if retries != "0":
                    entry['onfailure'] = "retry=%d" % (int(retries) - 1)
                else:
                    entry['onfailure'] = "continue"
                    # next failure there will be no more retries

    def check_running(self):
        '''
        check every command in the series and see if any that have
        been started are completed.  This includes commands started
        before an untimely demise of this script and recorded as
        running in the cache, if any.
        for any that are completed, possibly set them up to rerun if
        they failed or mark as done.
        '''

        will_exit = False
        for entry in self.commands:
            if entry['processes']:
                processes_to_check = entry['processes'][:]
                for process in processes_to_check:
                    done, retcode = self.check_process_done(process, process.pid)
                    if done:
                        if retcode != 0:
                            self.handle_nonzero_retcode(process, process.pid, entry)
                            if entry['onfailure'] == 'exit':
                                will_exit = True
                        else:
                            self.mark_process_done(process, process.pid, entry)
            else:
                pids_to_check = entry['processids'][:]
                for pid in pids_to_check:
                    done, retcode = self.check_process_done(None, pid)
                    if 'rerun' in entry and pid in entry['procidsfromcache']:
                        self.handle_nonzero_retcode(None, pid, entry)
                        if entry['onfailure'] == 'exit':
                            will_exit = True
                    else:
                        self.mark_process_done(None, pid, entry)
        if will_exit:
            self.save_to_cache()
            LOG.error("exiting after command failure")
            sys.exit(1)

    def start_command(self):
        '''
        see if any command has completed, deal with failure or successful completion,
        (some commands may be requeued on failure)
        then go through the series of command sets in order and for the first set that
        has not completed, if there are slots available, start up a command
        '''

        self.check_running()
        entry = None
        for item in self.commands:
            if item['count'] > 0:
                entry = item
                break

        if entry is None:
            for item in self.commands:
                if item['processids']:
                    # no more commands left to run, waiting for completion
                    # depending on completion (errors etc) some may be requeued
                    return False

            # no more commands left to run, all completed
            return None

        LOG.debug("entry slots is %s and free is %s", str(entry['slots']),
                  str(self.free_slots))
        if entry['slots'] <= self.free_slots:
            LOG.info("using %s slot(s), starting command %s",
                     str(entry['slots']), entry['command'])
            # we need setpgrp here so that kills to our children will
            # propagate through to any subprocesses that might be forked
            process = Popen(entry['command'],
                            shell=True, bufsize=-1, preexec_fn=os.setpgrp)
            entry['processes'].append(process)
            entry['processids'].append(process.pid)
            self.free_slots = self.free_slots - entry['slots']
            entry['count'] -= 1
            self.save_to_cache()
        return True

    def save_to_cache(self):
        '''
        write a cache file recording all commands in series
        and their state, in case this script meets an untimely demise
        '''

        cache_p = open(self.cache + ".tmp", "w+")
        cache_p.write("id:%s\n" % self.my_id)
        for entry in self.commands:
            # dump if there are processes running or processes yet
            # to run for command set
            if entry['processids'] or entry['count'] > 0:
                cache_p.write(json.dumps(entry, default=json_obj_dump) + "\n")
        cache_p.close()
        os.rename(self.cache + ".tmp", self.cache)

    def restore_from_cache(self):
        '''
        if this script has been interrupted, restore command state information
        from a cache file, including the value of the variable set in the
        environment of all commands previously started by this script

        this is necessary because we won't have the Popen object for those
        processes now, if they are still running. We will only have the pid
        which could have been re-used. We check the environ for a variable
        with that value to make sure it was really our process.
        '''

        if not os.path.exists(self.cache):
            return

        cache_p = open(self.cache, "r")
        for line in cache_p:
            line = line.rstrip("\n")
            if line.startswith("id:"):
                self.my_id = line.split(":", 1)[1]
                continue

            entry = json.loads(line)
            entry['slots'] = int(entry['slots'])
            entry['count'] = int(entry['count'])
            entry['done'] = int(entry['done'])
            entry['procidsfromcache'] = entry['processids'][:]
            entry['processes'] = []
            self.commands.append(entry)


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: dumpscheduler.py --slots number [--commands path]
    [--cache path] [--directory path] [--mailhost hostname]
    [--email address] [--formatvars var1=val1,var2=val2...]
    [--restore] [--rerun] [--verbose] [--help]

Send a SIGHUP to this script to shoot all children and restart the script
from where it was interrupted, using the cache file.

Options:

  --slots     (-s):     how many 'slots' the host where this script runs is deemed to have
                        you can think of this as how many cores are free for running tasks
  --commands  (-c):     full path of file containing commands to be run
                        if this option is not specified, list will be read from stdin
  --cache     (-C):     full path of cache file for tracking running processes
                        if this option is not specified, "running_cache.txt"
                        in the current working directory will be used
  --directory (-d):     change to this directory as working directory before running
                        commands; if this is not specified, the commands will run
                        from the same working dir as this script
  --mailhost  (-m):     mailhost to which error reports will be sent, if indicated
                        in command list
  --email     (-e):     email failure notifications will be sent from this address
                        default: root
  --restore   (-r):     restore state from cache (for interrupted script)
  --rerun     (-R):     rerun any processes still in process (for interrupted script)
  --formatvars(-f):     comma-separated list of var names and values to be substituted
                        into the command list via format()
  --verbose   (-v):     display progress messages
  --debug     (-d):     display even more progress messages
  --help      (-h):     display this usage message

Command list format:

Each line of the file of commands or of the list from stdin is of the form:

    <numslots> <numcommands> <onfailure> <errornotify> <command>

where:

    numslots is the number of slots (free cores, perhaps) that one process takes,
    numcommands is the max number of copies of this command to run at once,
                if 'max' is given as the value, the value will be calculated from
                numslots the command uses vs slots the host has in total as specified
                in the --slots arg to the script
    onfailure: what to do if a command fails: continue, retry=numofretries, exit
               note that numofretries is the number of total retries for the entire
               count of that command. So if 5 copies of the command are run and
               retries are 3, that's 3 retries total, not 3 per each of 5 commands.
    errornotify: if set to an email address, notification will be sent for each failure
                 otherwise enter the string 'none'
    command is the whole command string as it would be run at the shell

Fields are space-separated.
Lines starting with space or # are skipped.

Example entry:

4 2 continue none bash ./worker --config confs/wikidump.conf.bigwikis \
      --job articlesdump --date last > /var/log/dumps/junk

This says that each process takes 4 slots, we want two of these to run (at most) at
once, and the full command to run is
  bash ./worker --config confs/wikidump.conf.bigwikis \
      --job articlesdump --date last > /var/log/dumps/junk

"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def main():
    'main entry point, does all the work'

    command_file = None
    cache = "running_cache.txt"
    slots = None
    working_dir = None
    mailhost = None
    email_from = "root"
    formatvars = None
    restore = False
    rerun = False
    verbose = False
    debug = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "c:C:d:e:m:s:f:rDRvh",
                                                 ["commands=", "cache=", "directory=",
                                                  "email=", "slots=", "formatvars=", "restore",
                                                  "rerun", "mailhost", "verbose", "debug",
                                                  "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--commands"]:
            command_file = val
        elif opt in ["-C", "--cache"]:
            cache = val
        elif opt in ["-d", "--directory"]:
            working_dir = val
        elif opt in ["-e", "--email"]:
            email_from = val
        elif opt in ["-s", "--slots"]:
            if not val.isdigit():
                usage("slots option requires a number")
            slots = int(val)
        elif opt in ["-r", "--restore"]:
            restore = True
        elif opt in ["-R", "--rerun"]:
            rerun = True
        elif opt in ["-f", "--formatvars"]:
            formatvars = val
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-m", "--mailhost"]:
            mailhost = val
        elif opt in ["-d", "--debug"]:
            debug = val
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if slots is None:
        usage("The mandatory slots option was not specified")

    if command_file is not None:
        if not os.path.exists(command_file):
            usage("no such file found: " + command_file)
        commands_in = open(command_file, "r")
    else:
        commands_in = sys.stdin

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.ERROR)

    if working_dir is not None:
        os.chdir(working_dir)

    scheduler = Scheduler(commands_in, slots, cache, mailhost,
                          email_from, formatvars, restore, rerun)
    scheduler.run()


if __name__ == '__main__':
    os.environ['DUMPS'] = "scheduler"
    main()
