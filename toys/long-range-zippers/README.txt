These are a couple of scripts playing with alternate compressors for
full-history dumps.  The essential idea is that general-purpose compression
tools weren't made for files like history dumps that have many repetitions
of long chunks of content, sometimes spaced far apart.  Using tools made for
this sort of content can save you compression time or space.

Both of these scripts share a lot of boilerplate to split uncompressed
content into 10M chunks; that allows the script to stream input and output
even if the underlying compressor can't, and allows random read access to
the compressed content.


* blks-standalone.py preprocesses the file in Python, then runs gzip or
  bzip.

  Specifically, some Python code produces 1) a sorted list of all *distinct*
  lines in the input, and 2) indices into that array for every line of the
  input.  It gzips (or, with minor code changes, can bzip) both the indices
  and the lines.
  
  Here's a slightly simplified copy of the compression code to illustrate:
  
    lines = input.split('\n')
    line_list = sorted(set(lines))
    line_dict = dict((line,i) for i,line in enumerate(line_list))
    line_numbers = array.array('I', (line_dict[line] for line in lines))
    nums = zip(line_numbers.tostring())
    text = zip('\n'.join(line_list))
    return pack_num(len(nums)) + nums + text
  
  In a test on a c1.medium EC2 instance, it compressed 4.1G to 63M in 2m45s
  wall time. Using bzip instead of gzip on the text, it gets the file to 56M
  in 3m40s.


* blks-rzip.py runs rzip, from http://rzip.samba.org/, which internally uses
  a hashtable to find long repetitions (32+ chars) anywhere in the file,
  then bzips the 'instructions' and the remaining text.
  
  rzip's more efficient: in the saem test scenario above, it creates a 44M
  file in three minutes.  You can also run rzip -0 and gzip the output for
  faster results with worse compression.  rzip also works on all sorts of
  files with long-range redundancy, whereas the line-based trick in
  blks-standalone only works on text content.  People obviously need the
  rzip package installed to use it; there's a pure-Python decompressor in
  the script, but it's slow.


None of this is optimal.  If anything, the performance numbers from this
just indicate there may be gains from even blunt approaches to exploiting
the similarity between revisions.

Everything here is public domain.

Randall Farmer, twotwotwo at gmail, 2013