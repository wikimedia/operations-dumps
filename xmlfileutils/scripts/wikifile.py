# -*- coding: utf-8 -*-
import re, gzip, bz2

class File(object):
    """open bz2, gz and uncompressed files"""

    @staticmethod
    def openInput(filename):
        """Open for input a file optionally gz or bz2 compressed,
        determined by existence of .gz or .bz2 suffix"""

        if (filename.endswith(".gz")):
            fd = gzip.open(filename, "rb")
        else:
            fd = open(filename, "r")
        return fd

    @staticmethod
    def openOutput(filename):
        """Open for output a file optionally gz or bz2 compressed,
        determined by existence of .gz or .bz2 suffix"""

        if (filename.endswith(".gz")):
            fd = gzip.open(filename, "wb")
        else:
            fd = open(filename, "w")
        return fd

    @staticmethod
    def combineXML(pathList, outputPath):
        """Combine multiple content or stub xml files into one,
        skipping extra headers (siteinfo etc) and footers
        There is a small risk here tht the site info is
        actually different between the files, if we were really
        paranoid we would check that
        Arguments:
        pathList   -- list of full paths to xml content or stub files
        outputPath -- full path to combined output file"""

        endHeaderPattern = "^\s*</siteinfo>"
        compiledEndHeaderPattern = re.compile(endHeaderPattern)
        endMediaWikiPattern = "^\s*</mediawiki>"
        compiledEndMediaWikiPattern = re.compile(endMediaWikiPattern)

        outFd = File.openOutput(outputPath)
        i = 0
        listLen = len(pathList)
        for f in pathList:
            inHeader = True
            inFd = File.openInput(f)
            for line in inFd:
                if (i +1 < listLen): # skip footer of all files but last one
                    if compiledEndMediaWikiPattern.match(line):
                        continue
                if i and inHeader:  # skip header of all files but first one
                    if compiledEndHeaderPattern.match(line):
                        inHeader = False
                else:
                    outFd.write(line)
            inFd.close()
            i = i +1

        outFd.close()
