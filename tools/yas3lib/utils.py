import os, sys

class Err(object):
    """
    Static methods for displayin error messages
    """

    @staticmethod
    def whine(message = None):
        """
        Display an error message to stdout.

        Arguments:
        message -- the message to be displayed. without trailing newline
        """
        if message:
            sys.stderr.write("Error encountered: %s\n" % message)
        else:
            sys.stderr.write("Unknown error encountered\n")
        raise ErrExcept

class ErrExcept(Exception):
    """
    Exception class which will one day do something more than the default
    """
    pass

class PPXML(object):
    """
    XML pretty printer
    """

    @staticmethod
    # FIXME if there is already indentation of some sort in the data, toss it and use ours
    def cheapPrettyPrintXML(data):
        """
        Pretty prints well-formed xml data to stdout; broken data will be handled badly

        Arguments:
        data -- the xml text to be pretty-printed
        """
        # do nothing special if there's no xml in here
        if data.find("<?xml ") < 0:
            data = data.strip('\r\n').strip()
            if data:
                print data
            return

        data = data.replace("><", ">\n<")
        dataList = data.split("\n")
        indent = 0
        prettify = False
        for d in dataList:
            # found xml doc marker
            if d.startswith("<?xml "):
                indent = 0
                prettify = True
            elif not prettify:
                print d
            # close tag on its own line
            elif d.startswith("</"):
                indent = indent - 1
                print "%s%s" % ("  "*indent, d)
            elif d.startswith("<"):
                # open-close in one tag on its own line
                if d.endswith("/>"):
                    print "%s%s" % ("  "*indent, d)
                # open tag, data, close tag on one line
                elif d.find("</") != -1:
                    print "%s%s" % ("  "*indent, d)
                # open tag, maybe data, on its own line
                else:
                    print "%s%s" % ("  "*indent, d)
                    indent = indent + 1
            # data followed by close tag
            elif d.find("</") != -1:
                print d
                indent = indent - 1
            # data
            else:
                print d
