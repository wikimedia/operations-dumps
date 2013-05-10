#!/usr/bin/env python
# Randall Farmer, twotwotwo at gmail, 2013. Public domain. No warranty.
import sys, gzip as gzip_mod, array, os, getopt, bisect, ctypes, bz2
from struct import pack, unpack
from cStringIO import StringIO

LITTLE_ENDIAN = array.array('L',[1]).tostring()[0] == '\x01'
pack_num = lambda i: pack('>Q', i)
unpack_num = lambda s: unpack('>Q', s)[0]
read_num = lambda f: unpack_num(f.read(8))
write_num = lambda f,i: f.write(pack_num(i))

def gzip(s):
    sio = StringIO()
    gzipfile = gzip_mod.GzipFile(mode='w',fileobj=sio,compresslevel=6)
    gzipfile.write(s)
    gzipfile.close()
    return sio.getvalue()

def unzip(s):
    if s[:3] == 'BZh':
        return bz2.decompress(s)
    sio = StringIO(s)
    return gzip_mod.GzipFile(fileobj=sio).read()

nums_compressor = gzip
text_compressor = gzip

def compress(s):
    lines = s.split('\n')
    line_list = sorted(set(lines))
    # if the line numbers would overflow 16 bits, bail
    if len(line_list) > 65535:
        return pack_num(0) + text_compressor(s)
    line_dict = dict((line,i) for i,line in enumerate(line_list))
    line_numbers = array.array('I', (line_dict[line] for line in lines))
    if LITTLE_ENDIAN:
        line_numbers.byteswap()
    nums = nums_compressor(line_numbers.tostring())
    text = text_compressor('\n'.join(line_list))
    return pack_num(len(nums)) + nums + text

def decompress(s):
    nums_length = unpack_num(s[0:8])
    if nums_length == 0:
        return unzip(s[8:])
    line_numbers = array.array('I', unzip(s[8:8+nums_length]))
    if LITTLE_ENDIAN:
        line_numbers.byteswap()
    lines = unzip(s[8+nums_length:]).split('\n')
    return '\n'.join(lines[i] for i in line_numbers)

MAGIC = 'BLKS'

infile = sys.stdin
outfile = sys.stdout
blksz = 10000000

skip = length = None
inputoffs = outputoffs = 0
blocks = array.array('L')
mysuffix = '.test'

def read_chunk():
    l = read_num(infile)
    if l == 0:
        return ''
    return decompress(infile.read(l))

def read_index():
    infile.seek(-8, os.SEEK_END)
    index_length = read_num(infile)
    infile.seek(-(8 + index_length), os.SEEK_END)
    gzipped_index = infile.read(index_length)
    index_str = unzip(gzipped_index)
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
    
    global inputoffs, outputoffs
    inputstart = inputoffs
    outputstart = outputoffs

    inputoffs += len(chunk)
    result = compress(chunk)
    write_num(outfile,len(result))
    outputoffs += 8
    outfile.write(result)
    outputoffs += len(result)
    
    blocks.extend(
      (inputstart,outputstart)
    )
    return True

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
    global skip, length, infile, outfile
    global compressor_name, compressor, decompressor
    want_decompress = force = pipe = keep = False
    level = None
    opts, args = getopt.gnu_getopt(sys.argv[1:],'dcp:fk19',['skip=','length='])
    for o,v in opts:
        if o == '--skip':
            skip = int(v)
        elif o == '--length':
            length = int(v)
        elif o == '-d':
            want_decompress = True
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

    if want_decompress is None:
        if 'blkunz' in sys.argv[0]:
            want_decompress = True
        else:
            want_decompress = False
    
    if len(args) > 0:
        infile = open(args[0])
        if want_decompress is False and args[0].endswith(mysuffix) and not force:
            raise Exception('won\'t recompress %s without -f' % mysuffix)

    # pick a default filename if needed
    outfn = cmdline_outfn = None
    if len(args) > 1:
        if pipe:
            raise Exception('-c (pipe) doesn\'t make sense with an outfile')
        outfn = args[1]
    else:
        outfn = None
        if want_decompress and infile.name.endswith(mysuffix) and not length:
            outfn = infile.name[:-len(mysuffix)]
        if not want_decompress and len(args) == 1:
            outfn = args[0] + mysuffix
    
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

    if outfile.isatty() and not want_decompress and not force:
        raise Exception('won\'t compress to terminal without -f')
 
    if want_decompress:
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
        if skip or length:
            raise Exception('--skip and --length are decompress-only')
        infile_to_outfile()
    
    if outfn and not (pipe or keep):
        os.unlink(infile.name)

if __name__ == '__main__':
    main()
else:
    raise Exception('not a module')
    

