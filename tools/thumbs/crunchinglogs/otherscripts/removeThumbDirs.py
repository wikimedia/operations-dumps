# -*- coding: utf-8  -*-


'''
given a list of thumb dirs for cleanup,
toss all files in them and also the dirs themselves
'''


import os
import sys
import time


def rm_dir_and_files(dir_name):
    for fname in os.listdir(dir_name):
        if os.path.isfile(dir_name + "/" + fname):
            os.remove(dir_name + "/" + fname)
    os.rmdir(dir_name)


def do_main():
    count = 0
    for line in sys.stdin:
        dname = line.rstrip()
        if os.path.isdir(dname):
            rm_dir_and_files(dname)
        count = count + 1
        if count % 1000 == 0:
            print "count ", count, "removed dir", dname
        if count % 100 == 0:
            time.sleep(5)
    sys.exit(0)


if __name__ == "__main__":
    do_main()
