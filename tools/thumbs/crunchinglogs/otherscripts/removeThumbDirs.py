# -*- coding: utf-8  -*-

import os
import re
import sys
import time

# given a list of thumb dirs for cleanup, 
# toss all files in them and also the dirs themselves

def rmDirAndFiles(dirName):
    for f in os.listdir(dirName):
        if os.path.isfile(dirName + "/" + f):
            os.remove(dirName + "/" + f)
    os.rmdir(dirName)

if __name__ == "__main__":
    count = 0
    for line in sys.stdin:
        dname = line.rstrip()
        if os.path.isdir(dname):
            rmDirAndFiles(dname)
        count = count + 1
        if count % 1000 == 0:
            print "count ", count, "removed dir",  dname
        if count % 100 == 0:
            time.sleep(5)
    sys.exit(0)

