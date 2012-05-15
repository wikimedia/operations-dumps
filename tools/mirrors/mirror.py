import os, sys

class MirrorError(Exception):
    pass

class MirrorMsg(object):
    def warn(message):
        # maybe this should go to stderr. eh for now...
        print "Warning:", os.getpid(), message
        sys.stdout.flush()

    def display(message, continuation = False):
        # caller must add newlines to messages as desired
        if continuation:
            print message,
        else:
            print "Info: (%d) %s" % (os.getpid(), message),
        sys.stdout.flush()

    warn = staticmethod(warn)
    display = staticmethod(display)
