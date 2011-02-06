import getopt
import os
import re
import sys
import time
import bz2
import xml.sax
import Bzip2RandomAccess

from os.path import dirname, exists, getsize, join, realpath
from Bzip2RandomAccess import BzFile, BzConstants

class BzXmlFile(object):
    """Convenience functions that do things like find the last pageID
    from the last complete block in a possibly truncated bzip2 file, etc"""
    def __init__(self, fileName):
        self._fileName = fileName
        self._dataBlock = None
        self._pageID = None
        self._bzf = BzFile(self._fileName)

    def getFooter(self):
        block = self._bzf.findLastFullBzBlock()
        if (not block):
            return False
        uncompressedData = block.getUncompressedData()
        if (not uncompressedData ):
            return False
        footerPattern = re.compile('</mediawiki>\s*');
        result = footerPattern.search(uncompressedData)
        if (not result):
            return False
        return True

    # start from a fixed length back from the end of the file, 
    # read through blocks looking for page id info. goal is to 
    # find the *last one* in the file
    # if we don't find any in those blocks then we step back that same length
    # further back and try again...
    # repeat until we find one (in which case find the last one available)
    # or we run out of file
    def getLastPageIDFromFile(self):
        self._bzf._f.seek(0,os.SEEK_END)
        filesize = self._bzf._f.tell()

        # so because stuff isn't byte aligned etc, after
        # multiple iterations trying to get blocks and matches etc,
        # we could be a few bytes of in various length counts.
        # maybe I'm just being paranoid but that's what this is anyways.
        fudgefactor = 10

        offset = filesize
        # the pattern we are looking for could be perfectly positioned 
        # to be split across two blocks, so put leftover stuff in here
        # to be concatenated onto the beginning of the next block
        holdoverString = ""

        # blocks should always be smaller than this so if we start from
        # this far back in the file we should be guaranteed to find
        # a block. (unless the file is hopelessly corrupted)
        interval = BzConstants.getMaxCompressedBlockSize(self._bzf.getBlockSizeMultiplier())*2

        pageIDPattern = re.compile('</title>\n\s*<id>(?P<pageid>\d+?)</id>');
        
        # the position in the file after which we have already scanned
        didAlready = filesize

        # outer loop: jump back in the file from where we are,
        # far enough back we are guaranteed to find at least
        # one bzip2 block in ther if the file isn't garbage
        # hen read through it looking for pageID til we get to
        # ground we've already covered. 
        # found one? awesome, return it. otherwise,
        # jump back even further, repeat.
        while offset > 0:
            start = offset - interval
            if (start < 0):
                if (didAlready != fudgefactor):
                    start = 0
                else:
                    return currentPageID
            didAlready = offset + fudgefactor
            if didAlready > filesize:
                didAlready = filesize
            doingNow = start
            currentPageID = None
            while start < didAlready:
                # we need to walk through the blocks now going forward
                # looking for all pageIDs we encounter, if we find any
                # we return the last one. Only if we find none do
                # we fall through to the outer loop to back up further 
                # towards the beginning of the file.
                block = self._bzf.findBzBlockFromSeekPoint(start)
                if not block:
                    return currentPageID
                uncompressedData = block.getUncompressedData()
                text = holdoverString + uncompressedData
                pageID = pageIDPattern.search(uncompressedData)
                if (pageID):
                    currentPageID = pageID.group('pageid')
                start = start + block.getBlockLength() - fudgefactor
                holdoverString = uncompressedData[-1025:]

            if currentPageID:
                return currentPageID

            # no, we need to just add the block length I think, maybe subtract a few bytes
            offset = start - interval
            if offset < 0:
                return currentPageID
            holdoverString = ""

class PageInXml:
    """One page in XML, minus most of the content which we don't care about"""

    def __init__(self, title, id, revisionids):
        self.title = title
        self.id = id
        self.revisionids = revisionids

#
#
# the rest of this crap may get tossed... we shall see
#
#
class XmlFileChunk(object):
    """find pageID in last complete or partial bzip2 block before end of file,
    something like that."""
    def __init__(self, fileName):
        self._fileName = fileName
        self._dataBlock = None
        self._pageID = None
        self._f = BzFile(self._fileName)
        # not convinced I need this now, let's see
        self._seekOffset = None
                
    def getPageID(self, pageData):
        # there is the possibility that this chunk of data will have had the page tag
        # but not the page ID tag in it. 
        titleAndIDPattern = re.compile('<title>(?P<title>.+?)</title>\s*' + '<id>(?P<pageid>\d+?)</id>')
        result = titleAndIDPattern.search(pageData)
        if (not result):
            return None
        print result.group('title')
        return result.group('pageid')
    
    def uncompressedPageDataCount(self,page,pattern):
        """from one page (ie <page> through close tag)
        count the nuber of some given tag or other string"""
        return(page.count(pattern))
               
    def countRevisionsInData(self, uncompressedData):
        """return number of revisions in uncompressedPageData,
        looking at revision start tags only"""
        if not uncompressedPageData:
            return 0
        return(self.uncompressedPageDataCount(uncompressedPageData,"<revision>"))
               
    # FIXME used but incomplete, is this really the way to get revision counts for a
    # page is to get a whole page worth of data and count the revisions?
    # I doubt it, we really just set state = in page, count revisions
    # until we get to state not in page. something like that. 
    def getOnePage(self,data,offset=0):
        """get one page starting at offset specified from uncompressed data"""
        offsetPage = data[offset:]
        pageStart = offsetPage.find("<page>")
        if (not pageStart):
            return None
        pageEnd = offsetPage[pageStart:].find("</page>")
        if (not pageEnd):
            # FIXME we should go get more blocks or something?
            return None
        return offsetPage[pageStart:pageStart + pageEnd+len("</page>")]

    # FIXME too (unused and incomplete)
    def findPageInBlock(self, uncompressedData):
        # format: 
        # <page>
        #  <title>MediaWiki:Categories</title>
        #    <id>1</id>
        # etc.
        pageStartPattern = re.compile('<page>\s*');
        result = pageStartPattern.search(uncompressedData)
        if not result:
            return None
        # now we look for the end page marker. we will
        # put the uncompressed page someplace
        pageEndPattern = re.compile('</page>');
        result = pageEndPattern.search(uncompressedData)
        if not result:
            # need to grab the next block...
            # FIXME from here
            pass
        
    def findPageIDInBlock(self, uncompressedData):
        # format: 
        # <page>
        #  <title>MediaWiki:Categories</title>
        #    <id>1</id>
        # etc.

        pageStartPattern = re.compile('<page>\s*');
        result = pageStartPattern.search(uncompressedData)
        if not result:
            return None

        # we want the first page available in this block I guess
        # hmm, this block might have stuff from the previous pageID.
        # or some one much earlier than that, if some pages were deleted.
        # how can we tell? have to go find it??

        pages = uncompressedData[result.start():].split("<page>")
        for page in pages:
            ID = self.getPageID(page)
            if (ID):
                return(ID)
        return None

    def findPageIDFromSeekpoint(self, seek, maxBlocksToCheck = None, maxOffset = None):
        block = self._f.findBzBlockFromSeekPoint(seek)
        if not block:
            print "DEBUG: findPageIDFromSeekpoint: no block found, wtf"
            return (None, None)
        uncompressedData = block.getUncompressedData()
        if not uncompressedData:
            print "DEBUG: findPageIDFromSeekpoint: no bzip2 block found"
            return (None, None)
        # we got a block, we can look for a pageid in it (or in it plus the next
        # one, if there is a next one)
        pageID = self.findPageIDInBlock(uncompressedData)
        print "DEBUG: findPageIDFromSeekpoint: trying to find pageid in block"
        if (pageID):
            self._dataBlock = block
            self._pageID = pageID
            self._seekOffset = -1*seek
            return(pageID, uncompressedData)

        blockCount = 1
        pageID = None
        while (True):
            if (maxBlocksToCheck and (blockCount > maxBlocksToCheck)):
                break
            seek = seek + block.getBlockLength()
            if (maxOffset and (seek > maxOffset)):
                break
            block = self._f.findBzBlockFromSeekPoint(seek)
            # the n is length of <title> plus </title> plus <id> plus </id> plus <page> 
            # plus max title length plus a few for good measure.  so title length max is 255
            # let's future proof this a bit
            prevBytes = uncompressedData[-1050:]
            uncompressedData = block.getUncompressedData()
            if not uncompressedData:
                break
            uncompressedData = prevBytes + uncompressedData
            pageID = self.findPageIDInBlock(uncompressedData)
            if (pageID):
                self._dataBlock = block
                self._pageID = pageID
                self._seekOffset = -1*seek
                break
        return(pageID, uncompressedData)
    
    def close(self):
        self._f.close()

if __name__ == "__main__":
    myfile = BzXmlFile("/home/ariel/src/mediawiki/testing/dumps/public/elwikidb/20110205/elwikidb-20110205-pages-articles4.xml.bz2")
    pageID = myfile.getLastPageIDFromFile()
    if pageID:
        print "pageID", pageID
    else:
        print "no pageID for you!"


    result = myfile.getFooter()
    if (result):
        print "and this file has a footer"
    else:
        print "no footer on this baby"

    try:
#        f = XmlFileChunk("/home/ariel/elwikt/elwiktionary-20100305-pages-articles.xml.bz2")
        f = XmlFileChunk("/home/ariel/src/mediawiki/testing/enwiki-20100904-pages-meta-history9.xml.bz2")
#        f = XmlFileChunk("/mnt/dataset1/xmldatadumps/public/enwiki/20100904/enwiki-20100904-pages-meta-history11.xml.bz2")
        if not f:
            print "couldn't initialize file for searching"
            f.close()
            os.sys.exit()

        for i in range(1,100):
            (id,stuff) = f.findPageIDFromSeekpoint(1315000*i)
            if (id):
                print "page id:", id, " offset from eof:", f._seekOffset, "number of revisions: ", f.countRevisionsInData(f.getOnePage(stuff))
            
            else:
                print "no id found"
            
        f.close()
    except(IOError):
        print "there was no such file, you fool"


