# -*- coding: utf-8  -*-


'''
given a list of thumb dirs, list the files in
each dir, not sorted in any fashion, for input
to other scripts
'''


import os
import sys
import time


def list_files(dir_name):
    for fname in os.listdir(dir_name):
        path = dir_name + "/" + fname
        if os.path.isfile(path):
            stat = os.stat(path)
            file_date = time.strftime("%Y-%m-%d  %H:%M:%S",
                                      time.gmtime(stat.st_mtime))
            file_size = stat.st_size
            print file_date, " ", file_size, " ", path


def do_main():
    for line in sys.stdin:
        d_name = line.rstrip()
        if os.path.isdir(d_name):
            list_files(d_name)
    sys.exit(0)


if __name__ == "__main__":
    do_main()
