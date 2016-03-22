# -*- coding: utf-8  -*-

'''
convert image filenames (with _) to
full path with hash, for use by other scripts
'''

import sys
import hashlib


def get_hash_path_for_level(name, levels):
    if levels == 0:
        return ''
    else:
        summer = hashlib.md5()
        summer.update(name)
        md5_hash = summer.hexdigest()
        path = ''
        for i in range(1, levels+1):
            path = path + md5_hash[0:i] + '/'
        return path


def do_main():
    basedir = "/export/thumbs/wikipedia/commons/thumb/"
    for line in sys.stdin:
        fname = line.rstrip()
        hashpath = get_hash_path_for_level(fname, 2)
        result = basedir + hashpath + fname
        print result


if __name__ == "__main__":
    do_main()
