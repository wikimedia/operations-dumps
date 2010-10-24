import getopt
import os
import re
import sys
import time
import bz2
import xml.sax

class ShiftedData(object):
    """This class manages strings of data that have been left-shifted 
    0 through 7 bits."""

    def __init__(self, data, n=0, padding = None): 
        """Arguments:
        data -- the data to shift
        n -- the number of bits to shift left
        padding -- whether to add  1's on upper end of new 
        leftmost byte and lower end of rightmost byte"""
        self._data = data
        self._bitsShiftedLeft = n % 8
        self._padding = padding
        self._shiftedData = self.shiftLeftNBits()

    def getShiftedData(self):
        return self._shiftedData

    def getData(self):
        return self._data

    def getBitsShifted(self):
        return(self._bitsShiftedLeft)

    def getlength(self):
        return len(self._data)

    def getShiftedLength(self):
        return len(self._shiftedData)

    def shiftLeftNBits(self):
        """shift a string of crap n bits left, pushing 
        overflow into a new leftmost byte"""
        return ByteAlignedDataMethods.shiftLeftNBits(self._data,self._bitsShiftedLeft,self._padding)

class ByteAlignedDataMethods(object):
    """Contains various methods for byte-aligned data"""

    def shiftLeftNBits(data, bits, padding = False):
        """shift a string of crap n bits left, pushing 
        overflow into a new leftmost byte, padding on right
        with 1s if requested, otherwise with 0s"""
        if (bits == 0):
            return data

        resultList = []

        # overflow from shift off left end, may be 0
        overflow = ord(data[0])>> (8 - bits)
        resultList.append(chr(overflow))

        if (padding):
            resultList[-1] = chr(ord(resultList[-1]) | (256 - 2**bits))
            
        for i in range(0,len(data)):
            c = ord(data[i])
            if i == len(data)-1:
                next = 0
            else:
                next = ord(data[i+1])

            # grab stuff shifted off the left end of the next byte
            resultList.append(chr((c<<bits) & 255 | (next >> (8 - bits))))

        if (padding):
            resultList[-1] = chr(ord(resultList[-1]) | (2**bits -1))

        resultString = "".join(resultList)
        return(resultString)

    def getByteAlignedData(data, startByte, startBit):
        """given a string of data, a starting byte number (from 0) 
        and a starting bit in that byte (counting 0 from the leftmost bit),
        return the string of bits starting from there and going to the end of the
        string of data. The last byte is 0-padded on the right if necessary."""
        if (startByte >= len(data)):
            return None
        startBit = startBit % 8
        shifted = ByteAlignedDataMethods.shiftLeftNBits(data[startByte:],startBit)
        if (startBit):
            # the new uppermost byte is the extra bits that we didn't want anyways
            return(shifted[1:])
        else:
            return(shifted)

    shiftLeftNBits = staticmethod(shiftLeftNBits)
    getByteAlignedData = staticmethod(getByteAlignedData)


class shiftedSearchString(object):
    """This class manages search strings that may searched for in 
    bit-shifted data."""

    def __init__(self, data): 
        """Arguments:
        data -- the data to shift"""
        self._searchStringShifted=[]
        for i in range(0,8):
            self._searchStringShifted.append(ShiftedData(data,i,padding=True))

    def getLength(self):
        return len(self._searchStringShifted[0].getData())

    def findAllMatchesOfStringNonaligned(self,data):
        """search for all matches of a given pattern in a given string of data
        not byte aligned.  returns an array n0, n1, n2..  where n0 = list of
        starting bytes where pattern was found with no bit padding, 
        n1 = with 1 bit shifted, etc."""

        # FIXME this might be with the bit pattern shifted and not the string, dunno.

        if (not data):
            return (None, None)
        matches = []
        for i in range(0,8):
            results = self.findAllMatchesOfStringFromLeft(data,self._searchStringShifted[i].getShiftedData())
            matches.append(results)
        return(matches)


    def findAllMatchesOfStringFromLeft(self,data,pattern):
        """byte aligned already"""
        if (not data):
            return (None, None)

        positions = []
        offset = 0
        while (offset < len(data)):
            # do all but the first and last byte (which may have padding)
            result = data[offset:].find(pattern[1:-1])
            if (result >= 0):
                if (offset+result-1 >= 0):
                    offset = offset -1
                    firstByte = data[offset+result]
                    if firstByte == chr(ord(pattern[0]) & ord(firstByte)):
                        lastByte = data[offset+result+len(pattern) -1]  
                        if lastByte == chr(ord(pattern[-1]) & ord(lastByte)):
                            positions.append(result)
                                        
                # that submatch isn't at a block boundary, false alarm
                offset = offset + result + 2  # +1 because we start match at byte 2 of pattern and we want to move up one also
            else:
                return(positions)

        
    def findStringInDataFromLeft(self,data):
        """Find first occurence of string in (bit-shifted) data
        (occurence may not be byte aligned)
        Arguments: a string of data or a ShiftedData object
        Returns: tuple consisting of
        -  the starting position of the first match
        -  number of bits data must be left-shifted in order to find 
           byte-aligned match"""
        
        if (not data):
            return (None, None)

        while(True):
            firstMatch = None
            shiftedBy = None
            for i in range (0,8):
                offset = 0
                # do all but the first and last byte (which may have padding)
                bytesShifted = self._searchStringShifted[i].getShiftedData()
                result = data[offset:].find(bytesShifted[1:-1])
                if (result >= 0):
                    if (offset+result-1 >= 0):
                        offset = offset -1
                        firstByte = data[offset+result]
                        if firstByte == chr(ord(bytesShifted[0]) & ord(firstByte)):
                            lastByte = data[offset+result+len(bytesShifted) -1]  
                            if lastByte == chr(ord(bytesShifted[-1]) & ord(lastByte)):
                                if (firstMatch == None or result+offset < firstMatch):
                                    firstMatch = result+offset
                                    shiftedBy = i
                    # that submatch isn't at a block boundary, false alarm
                    offset = offset + result + 2  # +1 because we start match at byte 2 of pattern and we want to move up one also
            if (firstMatch == None):
                return(None, None)
            else:
                return (firstMatch,(8 - shiftedBy)%8)

    def findStringInDataFromRight(self,data):
        """Find last occurence of string in (bit-shifted) data
        (occurence may not be byte aligned)
        Arguments: a string of data or a ShiftedData object
        Returns: tuple consisting of
        -  the starting position of the first match, starting from byte 0
        -  number of bits pattern was left-shifted in order to find match"""
        
        if (not data):
            return (None, None)

        if isinstance(data, ShiftedData):
            checkData = data.getShiftedData()
        else:
            checkData = data

        while(True):
            firstMatch = None
            shiftedBy = None
            for i in range (0,8):
                offset = 0
                # do all but the first and last byte (which may have padding)
                bytesShifted = self._searchStringShifted[i].getShiftedData()
                result = checkData[offset:].rfind(bytesShifted[1:-1])
                if (result >= 0):
                    if (offset+result-1 >= 0):
                        offset = offset -1
                        firstByte = checkData[offset+result]
                        if firstByte == chr(ord(bytesShifted[0]) & ord(firstByte)):
                            lastByte = checkData[offset+result+len(bytesShifted) -1]  
                            if lastByte == chr(ord(bytesShifted[-1]) & ord(lastByte)):
                                if (firstMatch == None or result+offset > firstMatch):
                                    firstMatch = result+offset
                                    shiftedBy = i
                    # that submatch isn't at a block boundary, false alarm
                    offset = offset - result + 2  # +1 because we start match at 1 byte before end pattern and we want to skip a byte also
                    
            if (firstMatch == None):
                return(None, None)
            else:
                return (firstMatch,(8 - shiftedBy)%8)

    def dumpSearchString(self):
        for i in range(0, len(self._searchStringShifted)):
            BzConstants.dumpstring(self._searchStringShifted[i].getShiftedData(),"search string shifted %s:" % i)

class BzConstants(object):
    """Contains various defines for bz2 data"""

    def getFooter():
        """Return string which is at the end of every bzip2 stream or file"""
        footer = [ '0x17', '0x72', '0x45', '0x38', '0x50', '0x90' ]
        for i in range(0,len(footer)):
            footer[i] = chr(int(footer[i],16))
        footerString = "".join(footer)
        return footerString

    def getBlockMarker():
        """Return string which  is at the beginning of every bzip2 compressed block"""
        return "1AY&SY" 

    def getMaxCompressedBlockSize(bzBlockSizeMultiplier):
        """Return the maximum compressed bzip2 block size based on the 
        block size multipler (from the bzip2 header) passed as an argument"""
        # max length of compressed data block given size of uncompressed data as specified in header
        # "To guarantee that the compressed data will fit in its buffer, allocate an output buffer of 
        # size 1% larger than the uncompressed data, plus six hundred extra bytes." (Plus paranoia :-P)
        return (bzBlockSizeMultiplier + bzBlockSizeMultiplier/100)*100000 + 650

    def dumpstring(string,message="dumping string:"):
        print message, 
        for i in range(0,len(string)):
            print hex(ord(string[i])),
        print 
    
    def isZeros(data):
        for i in range(0,len(data)):
            if (ord(data[i]) != 0):
                return False
        return True

    def checkForFooter(data):
        """See if data passed as argument ends in the bz2 footer
        We expect: 6 bytes of footer, 4 bytes of crc, 0 to 7 bits of padding"""
        footerSearchString = shiftedSearchString(BzConstants.getFooter())
        ( offset, bitsShifted ) = footerSearchString.findStringInDataFromRight(data[-30:])
        if (offset != None):
            if (bitsShifted > 0):
                paddingByte = 1
            else:
                paddingByte = 0
            # starts from 0
            # expect 0-filled bytes at the end, if there are any
            extraBytes = -30+(offset + 6 + 4 + paddingByte)
            # so this iszeros thing... see, this data we got passed may have not been byte aligned.
            # so maybe it got left-bit shifted for some block marker.  
            # then if we foudn another marker farther down, maybe it got shifted again
            # and so on... so there could be a bunch of extra zero bytes at the end
            # FIXME this is the wrong place to check for that but I don't know the 
            # right place yet.  And keeping track is absolutely out of the question.
            # we could instead of using the newly byte aligned data from the stream
            # and continually left shifting it, go back to the original stream each
            # time which would limit this some.  ? needs thought.
            if (extraBytes <= 0 or BzConstants.isZeros(data[-30+(offset + 6 + 4 + paddingByte):])):
                # starting byte of footer, counting from end. may not be byte aligned.
                    return (-30 + offset)
        return None

    def getDecompressedBlock(data, bzHeader):
        """takes string of data plus 4 byte bzip2 header, returns decompressed block"""
        block = bzHeader + data
        try:
            bz = bz2.BZ2Decompressor()
            out = bz.decompress(block)
            return(out)
        except Exception, ex:
            print ex
            return(None)

    getFooter = staticmethod(getFooter)
    getBlockMarker = staticmethod(getBlockMarker)
    getMaxCompressedBlockSize = staticmethod(getMaxCompressedBlockSize)
    dumpstring = staticmethod(dumpstring)
    checkForFooter = staticmethod(checkForFooter)
    getDecompressedBlock = staticmethod(getDecompressedBlock)
    isZeros = staticmethod(isZeros)

class BzBlockStart(object):
    """This class manages bzip2 block markers (which mark the start of bzip2 blocks) 
    in a blob of data.  Because the block marker may not be byte aligned, it includes 
    the number of bits shifted left for the block start marker to be byte aligned. 
    It also contains a copy of the byte aligned data beginning at the start of the block 
    and including this marker.
    Arguments:
    data -- a blob of compressed data within which to find a bzip2 block
    bzBlockSizeMultiplier -- 1 through 9.
    This number is typically retrieved from a bzip2 header; it indicates the bzip2 block 
    sizes used for the uncompressed data, in units of 100K.
    This function is a bit wasteful in that it attempts a decompression of the data and 
    throws away the results; it does this in order to verify that the block marker really
    is at the start of a block. 
    It will detect a footer in the appropriate place at the end of the data stream
    (it has to in order for uncompression to work correctly)
    Use this function for seeking into an arbitrary place in a bzip2 file and digging up 
    data, or finding the last whole block written out to an arbitrarily truncated bzip2 file,
    not for regular stream decompression."""

    def __init__(self, data, header):
        """Arguments: data, 4-byte header from bzip stream
        If the data does not contain a bzip2 block marker
        (that really begins a block, i.e. the data afterwards
        uncompresses properly), subsequent calls to 
        getBzBlockMarkerStart() will return None.
        Otherwise, getBzBlockMarkerStart() will return the byte in the 
        data where the marker starts, (counting from 0),
        getBitsShifted() will get the number of bits that the 
        data at that byte must be left-shifted in order for the
        block marker to be byte-aligned, and 
        getByteAlignedData() will return the byte-aligned data 
        including the block marker.
        Note that we test for the block marker to be a genuine start of
        block by trying decompression; this can fail sometimes if the 
        bzip2 end of stream footer is present, so we will attempt to 
        remove it and getByteAlignedData() returns the data with 
        that removed (FIXME should it?  Do we even need that function?)"""

        self._data = data
        # 4 byte bzip2 header at the beginning of every compressed file or stream
        self._bzHeader = header
        self._bzBlockSizeMultiplier = int(header[3])
        self._bzBlockSize = self._bzBlockSizeMultiplier * 100000
        # this byte string is at the start of every bzip2 compressed block
        self._bzBlockMarker = shiftedSearchString(BzConstants.getBlockMarker())  
        # index of byte in data where block marker found
        self._bzBlockMarkerStart = None
        # number of bits block was shifted in order to be byte aligned
        self._bitsShifted = None 
        # block marker + following data, byte aligned
        self._byteAlignedData = None 
        self.findBzBlockMarker()

    def findBzBlockMarker(self):
        data = self._data
        offset = 0

        while (True):
            ( bzBlockMarkerStart, bitsShifted ) = self._bzBlockMarker.findStringInDataFromLeft(data)
            if ( bzBlockMarkerStart == None):
                return None
            offset = offset + bzBlockMarkerStart

            data = ByteAlignedDataMethods.getByteAlignedData(data,bzBlockMarkerStart, bitsShifted)

            # try uncompression to see if it's a valid block.  since the block marker can
            # appear in the data stream randomly, we don't try to bound this possible block by the next
            # appearance of a block marker; the decompress routine may barf on a partial block 
            # (and we don't know how much truncation is allowed). So pass something guaranteed to be
            # a full block size and then some, for the test.
            toDO = BzConstants.getMaxCompressedBlockSize(self._bzBlockSizeMultiplier)

            dataToUncompress = data[:toDO]

            # if there is a footer we will toss it
            footerOffset = BzConstants.checkForFooter(dataToUncompress)
            if (footerOffset != None):
                # what do we think about this, give a partial footer? 
                dataToTry = dataToUncompress[:footerOffset+5]
            else:
                dataToTry = dataToUncompress

            uncompressedData = BzConstants.getDecompressedBlock(dataToTry, self._bzHeader)
            if (uncompressedData != None):
                # w00t! 
                self._byteAlignedData = dataToUncompress
                self._bzBlockMarkerStart = offset
                self._bitsShifted = bitsShifted
                return True

            # no possibilities left
            if (len(data) <= len(BzConstants.getBlockMarker())):
                self._bzBlockMarkerStart = None

                return None
            # not a real block. on to the next possibility
            else:
                data = data[len(BzConstants.getBlockMarker()):]

    def getBitsShifted(self):
        return self._bitsShifted

    def getBzBlockMarkerStart(self):
        return self._bzBlockMarkerStart

    def getByteAlignedData(self):
        return self._byteAlignedData

class BzBlock(object):
    """This class manipulates bzipped data blocks (which include the 
    block start marker).  Because the block may not have been byte
    aligned wthin the original compressed stream or file, it also
    includes the number of bits shifted left for the block start marker to
    be byte aligned, as well as the number of bits to shift for
    the start of the next block, it there is one, in the stream
    or file. 
    It may additionally include some or all of the uncompressed data.
    Takes as arguments: 
    data -- the stream of data in which to find a block
    header -- the 4 byte bzip2 header which tells us block size among other things"""

    def __init__(self, blockData, header):
        self._blockData = blockData
        self._compressedData = None
        self._bzHeader = header
        self._bzBlockStart = None
        self._uncompressedData = None
        self._bzBlockLength = None
        self.findAndUncompressFirstBlock()

    def getBitMask(self, bits, left = False):
        """return bitmask starting from left or right end, of specified number of bits.
        default is start from right end"""
        if bits < 0:
            return 0

        bits = bits % 8
        if (left):
            return 255 - 2**(8-bits) +1
            pass
        else:
            return 2**bits -1

    def getMasked(self, byte, bitCount, left = False):
        """return leftmost or rightmost bitCount bits from byte
        default is rightmost. expect byte to be ord, not char"""
        mask = self.getBitMask(bitCount,left)
        return  mask & byte

    def findAndUncompressFirstBlock(self):
        bzBlockStart = BzBlockStart(self._blockData, self._bzHeader)
        if (not bzBlockStart.getBzBlockMarkerStart()):
            return None

        dataToUncompress = bzBlockStart.getByteAlignedData()

        # ok now we want to get the start of the next block in here,
        nextBlockStart = BzBlockStart(bzBlockStart.getByteAlignedData()[1:],self._bzHeader)
        if (not nextBlockStart.getBzBlockMarkerStart()):
            footerOffset =  BzConstants.checkForFooter(dataToUncompress)
            if (footerOffset != None):
                # partial footer?
                endMarker = footerOffset+5
            else:
                return None
        else:
            footerOffset = None
            endMarker = nextBlockStart.getBzBlockMarkerStart() + 8

        # this is either an additional 4 or 7 characters. not 5 or 8. python is stupid that way.
        dataToUncompress = dataToUncompress[:endMarker]

        self._uncompressedData = BzConstants.getDecompressedBlock(dataToUncompress, self._bzHeader)

        if (self._uncompressedData == None):
            return None
        else:
            # fixme is this next line right?
            self._compressedData = dataToUncompress
            self._bzBlockLength = len(dataToUncompress) 
            # now set *real* block length, not the length of the block plus a 
            # a few bytes of the following block marker or footer.  
            # NOTE that truncating your data to this byte may be a bad idea since the next byte,
            # while it will have the start of the next block marker in it, may not be
            # byte aligned; i.e. the first so many bits of that byte may be the end of this block. :-P
            # also note that truncating your block here for purposes of decompression with the
            # python bindings *will not work*, it needs to see the footer or the beginning of
            # the next block or it will fail and complain. sorry dudes.
            if (footerOffset == None):
                self._bzBlockLength = self._bzBlockLength - 7
            else:
                self._bzBlockLength = self._bzBlockLength - 4
            self._bzBlockStart = bzBlockStart
            return bzBlockStart

    def getOffset(self):
        if (self._bzBlockStart):
            return self._bzBlockStart.getBzBlockMarkerStart()
        else:
            return None

    def getCompressedData(self):
        return self._compressedData

    def getUncompressedData(self):
        return self._uncompressedData

    def getBlockLength(self):
        """return length of the (compressed) bz2 block"""
        return(self._bzBlockLength)

class BzFile:
    """handle bzip2 files, which means we can seek to arbitrary places
    in the compressed data, find the next block, uncompress it, 
    uncompress the following n blocks, get the last complete block
    from before the eof, etc."""
    def __init__(self, fileName):
        self._fileName = fileName
        self._dataBlock = None
        self._seekOffset = None
        self._blocksize = None
        self._header = None  # bzip2 header, 4 bytes
        self._f = open(fileName,"r")
        self.readHeader()
        self._blockSizeMultiplier = int(self._header[3])
        self._footer = BzConstants.getFooter()
        self._filesize = None

    def getBlockSizeMultiplier(self):
        return(self._blockSizeMultiplier)

    def readHeader(self):
        if self._header == None:
            self._f.seek(0)
            # header is BZhn (n = multiplier of 100k for compression blocksize)
            self._header = self._f.read(4)

    def close(self):
        self._f.close()

    def findLastFullBzBlock(self):
        """find last full bzip2 block written out before eof (by seeking
        to near the eof).  This is useful in case you have a truncated XML dump 
        and you want to know where to restart the run from; for very large files,
        decompressing the blocks starting from the beginning of the file
        can be quite slow.
        Returns a pointer to the DataBlock object or None if there was no
        bzip2 block found."""

        if not self._filesize:
            self._f.seek(0,os.SEEK_END)
            self._filesize = self._f.tell()

        seekBackTo = BzConstants.getMaxCompressedBlockSize(self._blockSizeMultiplier)*2
        if self._filesize < seekBackTo:
            seekBackTo = self._filesize
        self._f.seek(seekBackTo * -1,os.SEEK_END)
        # we are guaranteed to have a full block in here (if the file isn't less than a block long)
        # so start walking through this data til we find the last full block before eof.
        data = self._f.read()
        previousBlock = None

        while True:
            blockFound = BzBlock(data, self._header)
            if not blockFound.getUncompressedData(): # truncated block?
                if previousBlock:
                    self._dataBlock = previousBlock
                    self._seekOffset = self._filesize - previousSeekBack
                    return previousBlock
                else:
                    return None
            previousBlock = blockFound
            offset = blockFound.getBlockLength()
            # otheroffset = where the fricking block started in the data we passed it
            otheroffset = blockFound.getOffset()

            previousSeekBack = seekBackTo - otheroffset 
            seekBackTo = seekBackTo - offset - otheroffset + 1
            data = data[seekBackTo * -1:]
        
    def findBzBlockFromSeekPoint(self,seek):
        """Seek to given offset in file, search for and return
        first bzip2 block found in file after seek point, or
        None if none was found"""
        self._f.seek(seek,os.SEEK_SET)
        data = self._f.read(BzConstants.getMaxCompressedBlockSize(self._blockSizeMultiplier)*2)
        
        blockFound = BzBlock(data, self._header)
        if not blockFound.getUncompressedData():
            return None
        self._dataBlock = blockFound
        self._seekOffset = seek + blockFound.getOffset()
        return blockFound

    def getOffset(self):
        return self._seekOffset

if __name__ == "__main__":
    try:
# works
        f = BzFile("/home/ariel/src/mediawiki/testing/enwiki-20100904-pages-meta-history9.xml.bz2")

# works 
#        f = BzFile("/home/ariel/elwiki-20100925-pages-meta-history.xml.bz2")

# works hmm for from certain point, fails, because seek point > end of file :-P
#        f = BzFile("/home/ariel/src/mediawiki/testing/sample-last-but-0.bz2")

# works 
#        f = BzFile("/home/ariel/sample-file.txt.bz2")

# works
#        f = BzFile("/home/ariel/sample-file-bz9.txt.bz2")

#        offset = f.getBlockSizeMultiplier()*100000 + 600
        offset = 14315000

        # in these all our results are byte-aligned block markers out of the box
        # maybe that indicates a little problem? check. yes it's a bug, should have something around
        # 14254438 + 61571  and don't. so where is it? only finding start block aligned and
        # end block at shifted by 7, that's really weird. this must be recent, have
        # this behavior for the other routine too. 

        block = f.findBzBlockFromSeekPoint(offset)

#        block = f.findLastFullBzBlock()
        offset = None
        if (block):
            print "found this block (at offset in file %s, original seek point was %s, length %s): " % ( f.getOffset(), offset, block.getBlockLength()), block.getUncompressedData()[-500:]
            print "doublecheck..."
            f._f.seek(f.getOffset(),os.SEEK_SET)
            datatemp = f._f.read(100)
            BzConstants.dumpstring(datatemp[0:30],"contents of file from that offset")
        else:
            print "no block found"
            
        f.close()
    except(IOError):
        print "there was no such file, you fool"
