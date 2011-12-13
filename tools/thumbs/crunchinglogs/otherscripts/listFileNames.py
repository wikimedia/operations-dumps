# -*- coding: utf-8  -*-

import os
import re
import sys
import time
import hashlib

# convert image filenames (with _) to 
# full path with hash, for use by other scripts

def getHashPathForLevel( name, levels ):
    if levels == 0:
        return ''
    else:
        summer = hashlib.md5()
        summer.update( name )
        md5Hash = summer.hexdigest()        
        path = ''
        for i in range( 1,levels+1 ):
            path = path + md5Hash[0:i] + '/'
        return path

if __name__ == "__main__":
    basedir="/export/thumbs/wikipedia/commons/thumb/"
    for line in sys.stdin:
        fname = line.rstrip()
        hashpath = getHashPathForLevel(fname,2)
	result = basedir + hashpath + fname
	print result
