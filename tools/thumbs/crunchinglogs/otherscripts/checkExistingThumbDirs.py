# -*- coding: utf-8  -*-

import os
import re
import sys
import time

# after a cleanup of some dirs, run this on the list of dirs to check
# how many of them have been recreated in the meantime

if __name__ == "__main__":
    count = 0
    numDirs = 0
    for line in sys.stdin:
        dname = line.rstrip()
        if os.path.isdir(dname):
            numDirs = numDirs + 1
        count = count + 1
        if count % 1000 == 0:
            print "count:", count, "reached dir:",  dname, "existing dirs:", numDirs
    sys.exit(0)

