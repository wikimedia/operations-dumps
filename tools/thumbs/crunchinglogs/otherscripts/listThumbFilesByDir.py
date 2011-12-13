# -*- coding: utf-8  -*-

import os
import re
import sys
import time

# given a list of thumb dirs, list the files in 
# each dir, not sorted in any fashion, for input
# to other scripts

def listFiles(dirName):
    for f in os.listdir(dirName):
        fName = dirName + "/" + f
        if os.path.isfile(fName):
            stat = os.stat(fName)
            fileDate = time.strftime("%Y-%m-%d  %H:%M:%S",time.gmtime(stat.st_mtime))
            fileSize = stat.st_size
            print fileDate, " ", fileSize, " ", fName

if __name__ == "__main__":
    count = 0
    for line in sys.stdin:
        dName = line.rstrip()
        if os.path.isdir(dName):
            listFiles(dName)
    sys.exit(0)

