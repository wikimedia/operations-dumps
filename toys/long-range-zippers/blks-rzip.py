#!/usr/bin/env python
# Randall Farmer, twotwotwo at gmail, 2013. Public domain. No warranty.
import sys, subprocess, tempfile, gzip as gzip_mod, array, os, getopt, bisect, ctypes, bz2
from struct import pack, unpack
from cStringIO import StringIO
from binascii import crc32

# pure-Python runzipper won't decompress more than this
MAX_SIZE = 1024*1024*300

# check our endianness
LITTLE_ENDIAN = array.array('L',[1]).tostring()[0] == '\x01'

MAGIC = 'BLKS'

MISSING_RZIP = os.system('rzip -V > /dev/null 2> /dev/null')

### MEMORY-TO-MEMORY RUNZIP FROM PYTHON

class RZFile:
    def __init__(self, f=None, s=None):
        self.infile = StringIO(s or f.read())
        file_header = self.infile.read(24)
        # we like rzip | gzip, too
        if file_header[0:2] == '\0x1f\0x8b':
            self.infile.seek(0)
            self.infile = gzip_mod.GzipFile(fileobj=self.infile)
            file_header = self.infile.read(24)
        self.initial_pos = 24
        if file_header[0:4] != 'RZIP':
            raise Exception('not rzip')
        self.outsize, = unpack("!i", file_header[6:10])
        if self.outsize > MAX_SIZE:
            raise Exception('won\'t expand >%d bytes into RAM, saw %d' % (MAX_SIZE, self.outsize))
        self.outbuf = ctypes.create_string_buffer(self.outsize)
        self.outoffs = 0
        self.instreams = [None, None]
        self.nexthdrs = [24,37]

    def read_stream(self,which,bytes):
        res = ''
        if self.instreams[which]:
            res = self.instreams[which].read(bytes)
            if len(res) == bytes:
                return res
        # we're just starting, or crossed a chunk boundary.
        while len(res) < bytes:
            self.infile.seek(self.nexthdrs[which])
            chunk_header = self.infile.read(13)
            if not chunk_header:
                raise Exception('no chunk header where expected')
            ctype,c_len,buflen,next = unpack('<bLLL', chunk_header)
            next += self.initial_pos
            data = None
            if ctype == 3: # uncompressed
                data = self.infile.read(c_len)
            elif ctype == 4: # bzip
                data = bz2.decompress(self.infile.read(c_len))
            else:
                raise Exception('bad chunk type--corrupt file? runzip bug?')
            self.instreams[which] = StringIO(data)
            self.nexthdrs[which] = next
            res = self.instreams[which].read(bytes)
            if len(res) == bytes:
                return res
    
    def decompress(self):
        while True:
            hdr = self.read_stream(0,3)
            if not hdr:
                return self.outbuf # WE'RE DONE
            type,len = unpack('<bH', hdr)
            if type == 0: # literal
                if not len:
                    return self.outbuf # WE'RE DONEISH
                self.outbuf[self.outoffs:self.outoffs+len] = self.read_stream(1,len)
            elif type == 1: # match
                offs, = unpack('<I', self.read_stream(0,4))
                while len > offs: # repeats
                    self.outbuf[self.outoffs:self.outoffs+offs] = \
                        self.outbuf[self.outoffs-offs:self.outoffs]
                    len -= offs
                    self.outoffs += offs
                self.outbuf[self.outoffs:self.outoffs+len] = \
                    self.outbuf[self.outoffs-offs:self.outoffs-offs+len]
            else:
                # rzip accepts >1 as a match, but only emits 1
                raise Exception('unexpected instruction %d' % type)
            self.outoffs += len

def runzip(s):
    rz = RZFile(s=s)
    buf = rz.decompress()
    return buf.raw

compressor = lambda f,g: ['rzip', f, '-1o', g]
decompressor = lambda f,g: ['rzip', '-d', f,'-o', g]
suffix='.rz'
mysuffix = '-blks'
infile = sys.stdin
outfile = sys.stdout
blksz = 10000000

skip = length = None
inputoffs = outputoffs = 0
blocks = array.array('L')

### PORTABLE I/O NONENSE FOR WRAPPER FORMAT
pack_num = lambda i: pack('>Q', i)
unpack_num = lambda s: unpack('>Q', s)[0]
read_num = lambda f: unpack_num(f.read(8))
write_num = lambda f,i: f.write(pack_num(i))

def read_chunk():
    l = read_num(infile)
    if l == 0:
        return ''
    
    if MISSING_RZIP:
        return runzip(s=infile.read(l))

    global decompressor
    chunk = None
    tmp = tempfile.NamedTemporaryFile(suffix=suffix,delete=False)
    tmp.write(infile.read(l))
    tmp.flush()
    proc = subprocess.Popen(
        decompressor(tmp.name, tmp.name[:-len(suffix)])
    )
    proc.wait()
    chunk = open(tmp.name[:-len(suffix)]).read()
    os.unlink(tmp.name[:-len(suffix)])
    # r(un)zip deletes tmp if it works, but if we fail (and crash)
    # we still should not leave tmp files around
    try:
        os.unlink(tmp.name)
    except OSError:
        # as expected
        pass
    return chunk

def gunzip(s):
    sio = StringIO(s)
    return gzip_mod.GzipFile(fileobj=sio).read()

def read_index():
    infile.seek(-8, os.SEEK_END)
    index_length = read_num(infile)
    infile.seek(-(8 + index_length), os.SEEK_END)
    gzipped_index = infile.read(index_length)
    index_str = gunzip(gzipped_index)
    # even indices are input offsets, odd are block offsets
    index = array.array('L')
    index.fromstring(index_str)
    if LITTLE_ENDIAN:
        index.byteswap()
    formatted_index = []
    for i in xrange(0, len(index), 2):
        formatted_index.append((index[i],index[i+1]))
    return formatted_index

def read_random(index, skip, length):
    # find block to start on, how much to skip
    i = bisect.bisect_right(index, (skip,0))
    inputstart,blockstart = index[i-1]
    infile.seek(blockstart)

    # output partial from the first chunk
    outbytes = 0
    blockskip = skip - inputstart
    block = read_chunk()
    if not block:
        raise Exception('out of file')
    truncated = block[blockskip:blockskip+length]
    outfile.write(truncated)
    outbytes += len(truncated)
    
    # until we're past the end offset...
    while outbytes < length:
        # ...output chunks, truncating if we have to
        block = read_chunk()
        if not block:
            raise Exception('out of file')
        truncated = block[:length - outbytes]
        outfile.write(truncated)
        outbytes += len(truncated)


def write_chunk():
    chunk = infile.read(blksz)
    if not chunk:
        return False

    global inputoffs, outputoffs, compressor
    # track position in stdin
    inputstart = inputoffs
    inputoffs += len(chunk)
    inputend = inputoffs
    
    result = None
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(chunk)
    tmp.flush()
    proc = subprocess.Popen(
        compressor(tmp.name, tmp.name + suffix)
    )
    proc.wait()
    result = open(tmp.name + suffix).read()
    os.unlink(tmp.name + suffix)
    # r(un)zip deletes tmp if it works, but if we fail (and crash)
    # we still should not leave tmp files around
    try:
        os.unlink(tmp.name)
    except OSError:
        # expected: r(un)zip deletes the original
        pass
    
    outputstart = outputoffs
    write_num(outfile,len(result))
    outputoffs += 8
    outfile.write(result)
    outputoffs += len(result)
    
    blocks.extend(
      (inputstart,outputstart)
    )
    return True

def gzip(s):
    sio = StringIO()
    gzipfile = gzip_mod.GzipFile(mode='w',fileobj=sio)
    gzipfile.write(s)
    gzipfile.close()
    return sio.getvalue()

def infile_to_outfile():
    outfile.write(MAGIC)
    global outputoffs
    outputoffs += len(MAGIC)
    while write_chunk():
        pass
    write_num(outfile,0)
    if LITTLE_ENDIAN:
        blocks.byteswap()
    zipped_index = gzip(blocks.tostring())
    outfile.write(zipped_index)
    write_num(outfile,len(zipped_index))
  
def main():
    global skip, length, infile, outfile, suffix
    global compressor_name, compressor, decompressor
    decompress = force = pipe = keep = False
    level = None
    opts, args = getopt.gnu_getopt(sys.argv[1:],'dcp:fk19',['skip=','length='])
    for o,v in opts:
        if o == '--skip':
            skip = int(v)
        elif o == '--length':
            length = int(v)
        elif o == '-d':
            decompress = True
        elif o == '-c':
            pipe = True
        elif o == '-f': # write to tty, overwrite
            force = True
        elif o == '-k': # keep original file
            keep = True
        else:
            raise Exception('%s unsupported' % o)
    
    if len(args) > 2:
        raise Exception('I only support infile and outfile as args')

    if decompress is None:
        if 'blkunz' in sys.argv[0]:
            decompress = True
        else:
            decompress = False
    
    if len(args) > 0:
        infile = open(args[0])
        if decompress is False and args[0].endswith(mysuffix) and not force:
            raise Exception('won\'t recompress %s without -f' % mysuffix)

    # pick a default filename if needed
    outfn = cmdline_outfn = None
    if len(args) > 1:
        if pipe:
            raise Exception('-c (pipe) doesn\'t make sense with an outfile')
        outfn = args[1]
    else:
        outfn = None
        if decompress and infile.name.endswith(suffix + mysuffix) and not length:
            outfn = infile.name[:-len(suffix + mysuffix)]
        if not decompress and len(args) == 1:
            outfn = args[0] + suffix + mysuffix
    
    # open it, unless we'd zip to a terminal or -c was passed in
    if outfn and not pipe:
        exists = False
        try:
            os.stat(outfn)
            if not force:
                raise Exception('won\'t overwrite %s without -f' % outfn)
        except OSError:
            # file not found, yay
            pass
        outfile = open(outfn, 'w')

    if outfile.isatty() and not decompress and not force:
        raise Exception('won\'t compress to terminal without -f')
 
    if decompress:
        magic = infile.read(4)
        if magic != MAGIC:
            raise Exception('sorry; the file is not from this program')
        if skip or length:
            if skip is None or length is None:
                raise Exception('--skip and --length go together')
            index = read_index()
            read_random(index,skip,length)
        else:
            chunk = read_chunk()
            while chunk:
                outfile.write(chunk)
                chunk = read_chunk()
    else:
        if MISSING_RZIP:
            sys.stderr.write(
                "You don't have rzip installed. Consider:\n"
                "  sudo apt-get install rzip\n"
                "  -or-\n"
                "  yum install rzip\n"
                "  -or-\n"
                "  http://rzip.samba.org/\n"
                "Decompression is still possible, but slow."
            )
            sys.exit(1)
        if skip or length:
            raise Exception('--skip and --length are decompress-only')
        infile_to_outfile()
    
    if outfn and not (pipe or keep):
        os.unlink(infile.name)

if __name__ == '__main__':
    main()
else:
    raise Exception('not a module')
    

