#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include "bzlib.h"
#include "findpageidinbz2xml.h"

/* return n ones either at left or right end */
int bitmask(int numbits, int end) {
  if (end == MASKRIGHT) {
    return((1<<numbits)-1);
  }
  else {
    return(((1<<numbits)-1) << (8-numbits));
  }
}

void shift_bytes_left(unsigned char *buffer, int buflen, int numbits) {
  int i;

  if (numbits == 0) {
    return;
  }

  for (i=0; i<buflen; i++) {
    /* left 1 */
    buffer[i] = (unsigned char) ((int) (buffer[i]) << numbits);
      
    /* grab leftmost from next byte */
    if (i < buflen-1) {
      buffer[i] = ( unsigned char ) ( (unsigned int) buffer[i] | ( ( ((unsigned int) buffer[i+1])  & bitmask(numbits,MASKLEFT) ) >> (8-numbits) ) );
    }
  }
}

void shift_bytes_right(unsigned char *buffer, int buflen, int numbits) {
  int i;

  for (i=buflen-1; i>=0; i--) {
    /* right 1 */
    buffer[i] = (unsigned char) ((int) (buffer[i]) >> numbits);
      
    /* grab rightmost from prev byte */
    if (i > 0) {
      buffer[i] = ( unsigned char ) ((unsigned int) buffer[i] | ( ((unsigned int) (buffer[i-1])<<(8-numbits))  & bitmask(numbits,MASKLEFT)));
    }
  }
}

unsigned char ** init_marker() {
  unsigned char **marker = malloc(8*sizeof(unsigned char *));
  int i;

  /* set up block marker plus its various right-shifted incarnations */
  for (i = 0; i< 8; i++) {
    marker[i] = malloc(sizeof(unsigned char)*7);
  }
  marker[0][0]= (unsigned char) 0x31;
  marker[0][1]= (unsigned char) 0x41;
  marker[0][2]= (unsigned char) 0x59;
  marker[0][3]= (unsigned char) 0x26;
  marker[0][4]= (unsigned char) 0x53;
  marker[0][5]= (unsigned char) 0x59;
  marker[0][6]= (unsigned char) 0x00;
  for (i = 1; i< 8; i++) {
    memcpy((char *)(marker[i]), (char *)(marker[i-1]),7);
    shift_bytes_right(marker[i],7,1);
  }
  return(marker);
}

/* buff1 is some random bytes, buff2 is some random bytes which we expect to start with the contents of buff1, 
 both buffers are  bit-shifted to the right "bitsrightshifted". this function compares the two and returns 1 if buff2
 matches and 0 otherwise. */
int bytes_compare(unsigned char *buff1, unsigned char *buff2, int numbytes, int bitsrightshifted) {
  int i;

  if (bitsrightshifted == 0) {
    for (i = 0; i< numbytes; i++) {
      if (buff1[i] != buff2[i]) {
	return(1);
      }
    }
    return(0);
  }
  else {
    for (i = 1; i< numbytes-2; i++) {
      if (buff1[i] != buff2[i]) {
	return(1);
      }
    }
    /* do leftmost byte */
    if ((buff1[0] & bitmask(8-bitsrightshifted,MASKRIGHT))  != (buff2[0] & bitmask(8-bitsrightshifted,MASKRIGHT)) ) {
      return(1);
    }
    /* do rightmost byte */
    if ((buff1[numbytes-1] & bitmask(bitsrightshifted,MASKLEFT))  != (buff2[numbytes-1] & bitmask(bitsrightshifted,MASKLEFT)) ) {
      return(1);
    }
    return(0);
  }
}


/* return -1 if no match
   return number of bits rightshifted otherwise */
int check_buffer_for_bz2_block_marker(bz_info_t *bfile) {
  int result, i;

  result = bytes_compare(bfile->marker[0],bfile->marker_buffer+1,6,0);
  if (!result) {
    return(0);
  }
  for (i=1; i<8; i++) {
    result = bytes_compare(bfile->marker[i],bfile->marker_buffer,7,i);
    if (!result) {
      return(i);
    }
  }
  return(-1);
}


/* return: 1 if found, 0 if not, -1 on error */
int find_next_bz2_block_marker(int fin, bz_info_t *bfile) {
  int result;

  bfile->bits_shifted = -1;
  result = read(fin, bfile->marker_buffer, 7);
  if (result == -1) {
    /* fprintf(stderr,"read of file failed\n"); */
    return(-1);
  }
  /* must be after 4 byte file header, and we add a leftmost byte to the buffer 
     of data read in case some bits have been shifted into it */
  while (bfile->position <= bfile->file_size - 6 && bfile->bits_shifted < 0) { 
    bfile->bits_shifted = check_buffer_for_bz2_block_marker(bfile);
    if (bfile->bits_shifted < 0) {
      bfile->position++;
      result = lseek(fin, (bfile->position), SEEK_SET);
      if (result == -1) {
	fprintf(stderr,"lseek of file to %ld failed (2)\n",(long int) bfile->position);
	return(-1);
      }
      result = read(fin, bfile->marker_buffer, 7);
      if (result < 7) {
	/* fprintf(stderr,"read of file failed\n"); */
	return(-1);
      }
    }
    else {
      bfile->block_start = bfile->position;
      return(1);
    }
  }
  return(0);
}

/*
  initializes the bz2 strm structure, 
  calls the BZ2 decompression library initializer

  returns:
    BZ_OK on success
    various BZ_ errors on failure (see bzlib.h)
*/
int init_decompress(bz_info_t *bfile) {
  int bz_verbosity = 0;
  int bz_small = 0;
  int ret;

  bfile->strm.bzalloc = NULL;
  bfile->strm.bzfree = NULL;
  bfile->strm.opaque = NULL;

  ret = BZ2_bzDecompressInit ( &(bfile->strm), bz_verbosity, bz_small );
  if (ret != BZ_OK) {
    fprintf(stderr,"uncompress failed, err %d\n", ret);
    exit(-1);
  }
  return(ret);
}

/*
  reads the first 4 bytes from a bz2 file (should be
  "BZh" followed by the block size indicator, typically "9")
  and passes them into the BZ2 decompression library.
  This must be done before decompression of any block of the 
  file is attempted.

  returns:
    BZ_OK if successful,
    various BZ_ errors on failure (see bzlib.h)
*/
int decompress_header(int fin, bz_info_t *bfile) {
  int ret, res;

  res = lseek(fin,0,SEEK_SET);
  if (res == -1) {
    fprintf(stderr,"lseek of file to 0 failed (3)\n");
    exit(-1);
  }
  bfile->bytes_read = read(fin, bfile->header_buffer, 4);
  if (bfile->bytes_read < 4) {
    fprintf(stderr,"failed to read 4 bytes of header, exiting\n");
    exit(-1);
  }
  bfile->strm.next_in = (char *)bfile->header_buffer;
  bfile->strm.avail_in = 4;

  ret = BZ2_bzDecompress ( &(bfile->strm) );
  if (BZ_OK != ret && BZ_STREAM_END != ret) {
    fprintf(stderr,"Corrupt bzip2 header, exiting\n");
    exit(-1);
  }
  return(ret);
}

/*
  seek to appropriate offset as specified in bfile,
  read compressed data into buffer indicated by bfile, 
  update the bfile structure accordingly,
  save the overflow byte (bit-shifted data = suck)
  this is for the *first* buffer of data in a stream,
  for subsequent buffers use fill_buffer_to_decompress()

  this will set bfile->eof on eof.  no other indicator
  will be provided. 

  returns:
    0 on success
    -1 on error
*/
int setup_first_buffer_to_decompress(int fin, bz_info_t *bfile) {
  int res;

  if (bfile->bits_shifted == 0) {
    res = lseek(fin,bfile->position+1,SEEK_SET);
    if (res == -1) {
      fprintf(stderr,"lseek of file to %ld failed (4)\n",(long int) bfile->position+1);
      return(-1);
    }
  }
  else {
    res = lseek(fin,bfile->position,SEEK_SET);
    if (res == -1) {
      fprintf(stderr,"lseek of file to %ld failed (5)\n",(long int) bfile->position);
      return(-1);
    }
  }
  bfile->bytes_read = read(fin, bfile->bufin, bfile->bufin_size);
  if (bfile->bytes_read > 0) {
    bfile->overflow = bfile->bufin[bfile->bytes_read-1];
    shift_bytes_left(bfile->bufin, bfile->bytes_read, bfile->bits_shifted);

    bfile->strm.next_in = (char *)(bfile->bufin);
    bfile->strm.avail_in = bfile->bytes_read-1;
  }
  if (bfile->bytes_read <=0) {
    bfile->eof++;
  }
  return(0);
}

/*
  read compressed data into buffer indicated by bfile, 
  from current position of file,
  stuffing the overflow byte in first.
  update the bfile structure accordingly
  save the new overflow byte (bit-shifted data = suck)
  this function is for decompression of buffers *after
  the first one*.  for the first one use
  setup_first_buffer_to_decompress()

  this will set bfile->eof on eof.  no other indicator
  will be provided. 

  returns:
    0 on success
    hmm, it really does not do anything about errors :-D
*/
int fill_buffer_to_decompress(int fin, bz_info_t *bfile, int ret) {
  if (bfile->strm.avail_in == 0) {
    bfile->strm.next_in = (char *)(bfile->bufin);
    bfile->bufin[0] = bfile->overflow;
    bfile->bytes_read = read(fin, bfile->bufin+1, bfile->bufin_size-1);
    if (bfile->bytes_read > 0) {
      bfile->overflow = bfile->bufin[bfile->bytes_read];
      shift_bytes_left(bfile->bufin,bfile->bytes_read+1,bfile->bits_shifted);
      bfile->strm.avail_in = bfile->bytes_read;
      bfile->position+=bfile->bytes_read;
    }
    else {
      bfile->strm.avail_in = 1; /* the overflow byte */
      bfile->eof++;
    }
  }
  return(0);
}

/* size of buffer is bytes usable. there will be a null byte at the end 

   what we do with the buffer:
   - read from front of buffer to end, 
   - fill from point where prev read did not fill buffer, or from where 
     move of data at end of buffer to beginning left room,
   - mark a string of bytes (starting from what's available to read) as "read"

*/
buf_info_t *init_buffer(int size) {
  buf_info_t *b;

  b = (buf_info_t *)malloc(sizeof(buf_info_t));
  b->buffer = malloc(sizeof(unsigned char)*(size+1));
  b->buffer[size]='\0';
  b->end = b->buffer + size;
  b->next_to_read = b->end; /* nothing available */
  b->bytes_avail = 0; /* bytes to read, nothing available */
  b->next_to_fill = b->buffer; /* empty */
  b->next_to_fill[0] = '\0';
  return(b);
}

/* check if buffer (used for decompressed data output) is empty,
   returns 1 if so and 0 if not */
int buffer_is_empty(buf_info_t *b) {
  if (b->bytes_avail == 0) {
    return(1);
  }
  else {
    return(0);
  }
}

/* check if buffer (used for decompressed data output) is full,

   returns 1 if so and 0 if not
   I'm not liking this function so well, fixme */
int buffer_is_full(buf_info_t *b) {
  if (b->next_to_fill == b->end) {
    return(1);
  }
  else {
    return(0);
  }
}

/* FIXME do this right. whatever. */
int get_file_size(int fin) {
  int res;

  res = lseek(fin, 0, SEEK_END);
  if (res == -1) {
    fprintf(stderr,"lseek of file to 0 failed (6)\n");
    exit(-1);
  }
  return(res);
}


/*
  look for the first bz2 block in the file after specified offset
  it tests that the block is valid by doing partial decompression.
  this function will update the bfile structure:
  bfile->position will contain the current position of the file (? will it?)
  bfile->bits_shifted will contain the number of bits that the block is rightshifted
  bfile->block_start will contain the offset from start of file to the block
  returns:
    position of next byte in file to be read, on success
    -1 if no marker or other error
*/
int find_first_bz2_block_after_offset(bz_info_t *bfile, int fin, int position) {
  int res;

  bfile->bufin_size = BUFINSIZE;
  bfile->marker = init_marker();
  bfile->position = position;
  bfile->block_start = -1;
  bfile->bytes_read = 0;
  bfile->bytes_written = 0;
  bfile->eof = 0;
  bfile->bits_shifted = -1;

  bfile->file_size = get_file_size(fin);

  while (bfile->bits_shifted < 0) {
    if (bfile->position > bfile->file_size) {
      return(-1);
    }
    res = lseek(fin, bfile->position, SEEK_SET);
    if (res == -1) {
      fprintf(stderr,"lseek of file to %ld failed (7)\n",(long int) bfile->position);
      exit(-1);
    }
    res = find_next_bz2_block_marker(fin, bfile);
    if (res == 1) {
      init_decompress(bfile);
      decompress_header(fin, bfile);
      res = setup_first_buffer_to_decompress(fin, bfile);
      if (res == -1) {
	fprintf(stderr,"couldn't get first buffer of data to uncompress\n");
	exit(-1);
      }
      bfile->strm.next_out = (char *)bfile->bufout;
      bfile->strm.avail_out = bfile->bufout_size;
      res = BZ2_bzDecompress ( &(bfile->strm) );
      /* this means we (probably) have a genuine marker */
      if (BZ_OK == res || BZ_STREAM_END == res) {
	res = BZ2_bzDecompressEnd ( &(bfile->strm) );
	bfile->bytes_read = 0;
	bfile->bytes_written = 0;
	bfile->eof = 0;
	/* leave the file at the right position */
	res = lseek(fin, bfile->block_start, SEEK_SET);
	if (res == -1) {
	  fprintf(stderr,"lseek of file to %ld failed (7)\n",(long int) bfile->position);
	  exit(-1);
	}
	return(0);
      }
      /* right bytes, but there by chance, skip and try again */
      else {
	bfile->position+=6;
	bfile->bits_shifted = -1;
	bfile->block_start = -1;
      }
    }
    else {
      return(-1);
    }
  }
  return(-1);
}

/* 
   find the first bz2 block marker in the file, 
   from its current position,
   then set up for decompression from that point 
   returns: 
     0 on success
     -1 if no marker or other error
*/
int init_bz2_file(bz_info_t *bfile, int fin) {
  int res;

  bfile->initialized++;

  res = find_next_bz2_block_marker(fin, bfile);
  if (res ==1) {
    init_decompress(bfile);
    decompress_header(fin, bfile);
    setup_first_buffer_to_decompress(fin, bfile);
    return(0);
  }
  return(-1);
}

/* return -1 if error */
int decompress_data(bz_info_t *bfile, int fin, unsigned char *bufferout, int bufout_size) {
  int ret;

  bfile->bufout = bufferout;
  bfile->bufout_size = bufout_size;
  bfile->bytes_written = 0;

  if (! bfile->initialized) {
    if (init_bz2_file(bfile, fin) == -1) {
      /* fprintf(stderr,"failed to find block in bz2file (2)\n"); */
      return(-1);
    };
    bfile->strm.next_out = (char *)bfile->bufout;
    bfile->strm.avail_out = bfile->bufout_size;
  }

  ret = BZ_OK;
  while (BZ_OK == ret && bfile->bytes_written == 0) {
    ret = BZ2_bzDecompress ( &(bfile->strm) );
    if (BZ_OK == ret || BZ_STREAM_END == ret) {
      bfile->bytes_written = (unsigned char *)(bfile->strm.next_out) - bfile->bufout;
    }
    else {
      /* fprintf(stderr,"error from BZ decompress %d\n",ret); */
      return(-1);
    }
    fill_buffer_to_decompress(fin, bfile, ret);
    /*
    if (bfile->eof && (BZ_OK == ret || BZ_STREAM_END == ret) ) {
      fprintf(stderr,"eof reached\n");
    }
    */
  }
  return(0);
}


/* 
   fill output buffer in b with uncompressed data from bfile
   if this is the first call to the function for this file,
   the file header will be read, and the first buffer of
   uncompressed data will be prepared.  bfile->position
   should be set to the offset (from the beginning of file) from 
   which to find the first bz2 block.
   
   returns: 
     on success, number of bytes read (may be 0)
     -1 on error
*/
int get_buffer_of_uncompressed_data(buf_info_t *b, int fin, bz_info_t *bfile) {
  int res;

  if (buffer_is_full(b)) {
    return(0);
  }

  if (buffer_is_empty(b)) {
    b->next_to_fill = b->buffer;
  }

  res = decompress_data(bfile, fin, b->next_to_fill, b->end - b->next_to_fill);
  if (res == -1) {
    return(res);
  }
  if (bfile->bytes_written < 0) {
    /* fprintf(stderr,"read of file failed\n"); */
    return(-1);
  }
  else {
    /* really?? FIXME check this */
    if (buffer_is_empty(b)) {
      b->next_to_read = b->next_to_fill; /* where we just read */
    }
    b->bytes_avail += bfile->bytes_written;
    b->next_to_fill += bfile->bytes_written;
    b->next_to_fill[0] = '\0';
    return(0);
  }
}

void dumpbuf_info_t(buf_info_t *b) {
  fprintf(stdout, "\n");
  fprintf(stdout, "b->buffer: %ld\n", (long int) b->buffer);
  fprintf(stdout, "b->end: %ld\n", (long int) b->end);
  fprintf(stdout, "b->next_to_read: %ld\n", (long int) b->next_to_read);
  fprintf(stdout, "b->next_to_fill: %ld\n", (long int) b->next_to_fill);
  fprintf(stdout, "b->bytes_avail: %ld\n", (long int) b->bytes_avail);
}


/* 
   copy text from end of buffer to the beginning, that we want to keep
   around for further processing (i.e. further regex matches)
   returns number of bytes copied 
*/
int  move_bytes_to_buffer_start(buf_info_t *b, unsigned char *from_where, int maxbytes) {
  int i, tocopy;

  if (from_where >= b->end) {
    return(0);
  }
  else {
    tocopy = b->end - from_where;
    if (maxbytes && (tocopy > maxbytes)) {
      tocopy = maxbytes;
    }
    for (i = 0; i < tocopy; i++) {
      b->buffer[i] = from_where[i];
    }
    b->next_to_fill = b->buffer + tocopy;
    b->next_to_fill[0] = '\0';
    b->next_to_read = b->buffer; 
    b->bytes_avail = tocopy;
    return(tocopy);
  }
}

/* 
   get the first page id after position in file 
   if a pageid is found, the structure pinfo will be updated accordingly
   returns:
      1 if a pageid found,
      0 if no pageid found,
      -1 on error
*/
int get_first_page_id_after_offset(int fin, int position, page_info_t *pinfo) {
  int res;
  regmatch_t *match_page, *match_page_id;
  regex_t compiled_page, compiled_page_id;
  int length=5000; /* output buffer size */
  char *page = "<page>";
  char *page_id = "<page>\n[ ]+<title>[^<]+</title>\n[ ]+<id>([0-9]+)</id>\n"; 

  buf_info_t *b;
  bz_info_t bfile;

  bfile.initialized = 0;

  res = regcomp(&compiled_page, page, REG_EXTENDED);
  res = regcomp(&compiled_page_id, page_id, REG_EXTENDED);

  match_page = (regmatch_t *)malloc(sizeof(regmatch_t)*1);
  match_page_id = (regmatch_t *)malloc(sizeof(regmatch_t)*2);

  b = init_buffer(length);

  pinfo->bits_shifted = -1;
  pinfo->position = -1;
  pinfo->page_id = -1;

  bfile.bytes_read = 0;

  if (find_first_bz2_block_after_offset(&bfile, fin, position) == -1) {
    /* fprintf(stderr,"failed to find block in bz2file (1)\n"); */
    return(-1);
  }

  while (!get_buffer_of_uncompressed_data(b, fin, &bfile) && (! bfile.eof)) {
    if (bfile.bytes_read) {
      while (regexec(&compiled_page_id, (char *)b->next_to_read,  2,  match_page_id, 0 ) == 0) {
	if (match_page_id[1].rm_so >=0) {
	  /* write page_id to stderr */
	  /*
	    fwrite(b->next_to_read+match_page_id[1].rm_so, sizeof(unsigned char), match_page_id[1].rm_eo - match_page_id[1].rm_so, stderr);
	    fwrite("\n",1,1,stderr);
	  */
	  pinfo->page_id = atoi((char *)(b->next_to_read+match_page_id[1].rm_so));
	  pinfo->position = bfile.block_start;
	  pinfo->bits_shifted = bfile.bits_shifted;
	  return(1);
	  /* write up to and including page id tag to stdout */
	  /*
	    fwrite(b->next_to_read,match_page_id[0].rm_eo,1,stdout);
	    b->next_to_read = b->next_to_read+match_page_id[0].rm_eo;
	    b->bytes_avail -= match_page_id[0].rm_eo;
	  */
	}
	else {
	  /* should never happen */
	  fprintf(stderr,"regex gone bad...\n"); 
	  exit(-1);
	}
      }
      if (regexec(&compiled_page, (char *)b->next_to_read,  1,  match_page, 0 ) == 0) {
	/* write everything up to but not including the page tag to stdout */
	/*
	fwrite(b->next_to_read,match_page[0].rm_eo - 6,1,stdout);
	*/
	move_bytes_to_buffer_start(b, b->next_to_read + match_page[0].rm_so, b->bytes_avail - match_page[0].rm_so);
	bfile.strm.next_out = (char *)b->next_to_fill;
	bfile.strm.avail_out = b->end - b->next_to_fill;
      }
      else {
	/* could have the first part of the page tag... so copy up enough bytes to cover that case */
	if (b->bytes_avail> 5) {
	  /* write everything that didn't match, but leave 5 bytes, to stdout */
	  /*
	  fwrite(b->next_to_read,b->bytes_avail - 5,1,stdout);
	  */
	  move_bytes_to_buffer_start(b, b->next_to_read + b->bytes_avail - 5, 5);
	  bfile.strm.next_out = (char *)b->next_to_fill;
	  bfile.strm.avail_out = b->end - b->next_to_fill;
	}
	else {
	  if (buffer_is_empty(b)) {
	    bfile.strm.next_out = (char *)b->buffer;
	    bfile.strm.avail_out = bfile.bufout_size;
	    b->next_to_fill = b->buffer; /* empty */
	  }
	  else {
	    /* there were only 5 or less bytes so just save em don't write em to stdout */
	    move_bytes_to_buffer_start(b, b->next_to_read, b->bytes_avail);
	    bfile.strm.next_out = (char *)b->next_to_fill;
	    bfile.strm.avail_out = b->end - b->next_to_fill;
	  }
	}
      }
    }
  }
  /*
  if (b->bytes_avail) {
    fwrite(b->next_to_read,b->bytes_avail,1,stdout);
  }
  */
  return(0);
}

/* search for pageid in a bz2 file, given start and end offsets
   to search for
   we guess by the most boring method possible (shrink the
   interval according to the value found on the last guess, 
   try midpoint of the new interval)
   multiple calls of this will get the job done.
   interval has left end = right end if search is complete.
   this function may return the previous guess and simply
   shrink the interval.
   note that a "match" means either that the pageid we find
   is smaller than the one the caller wants, or is equal.
   why? because then we can use the output for prefetch
   for xml dumps and be sure a specific page range is covered :-P

   return value from guess, or -1 on error. 
 */
int do_iteration(iter_info_t *iinfo, int fin, page_info_t *pinfo) {
  int res;
  int new_position;
  int interval;

  /* 
     last_position is somewhere in the interval, perhaps at an end 
     last_value is the value we had at that position
  */
  
  interval = (iinfo->right_end - iinfo->left_end)/2;
  if (interval == 0) {
    interval = 1;
  }
  /*  fprintf(stderr,"interval size is %ld, left end %ld, right end %ld, last val %d\n",interval, iinfo->left_end, iinfo->right_end, iinfo->last_value); */
  /* if we're this close, we'll check this value and be done with it */
  if (iinfo->right_end -iinfo->left_end < 2) {
    new_position = iinfo->left_end;
    iinfo->right_end = iinfo->left_end;
  }
  else {
    if (iinfo->last_value < iinfo->value_wanted) {
      /*      fprintf(stderr,"resetting left end\n"); */
      iinfo->left_end = iinfo->last_position;
      new_position = iinfo->last_position + interval;
    }
    /* iinfo->last_value > iinfo->value_wanted */
    else {
      /*      fprintf(stderr,"resetting right end\n"); */
      iinfo->right_end = iinfo->last_position;
      new_position = iinfo->last_position - interval;
    }
  }
  res = get_first_page_id_after_offset(fin, new_position, pinfo);
  if (res >0) {
    /* caller wants the new value */
    iinfo->last_value = pinfo->page_id;
    iinfo->last_position = new_position;
    return(pinfo->page_id);
  }
  else {
    /* here is the tough case, if we didn't find anything then we are prolly too close to the end, truncation or
       there's just no block here.
       set the right end, keep the last value and position and let the caller retry with the new interval */
    if (iinfo->last_value < iinfo->value_wanted) { /* we were moving towards eof */
      iinfo->right_end = new_position;
      return(iinfo->last_value);
    }
    /* in theory we were moving towards beginning of file, should not have issues, so bail here */
    else { 
      /* fprintf(stderr,"something very broken, giving up\n"); */
      return(-1);
    }
  }
}

/*
  given a bzipped and possibly truncated file, and a page id, 
  hunt for the page id in the file; this assume that the
  bz2 header is intact and that page ids are steadily increasing
  throughout the file. 

  writes the offset of the relevant block (from beginning of file) 
  and the first pageid found in that block, to stdout

  format of output:
     position:xxxxx pageid:nnn

  returns: 0 on success, -1 on error
*/
int main(int argc, char **argv) {
  int fin, position, res, interval, page_id, oldmarker, file_size;
  page_info_t pinfo;
  iter_info_t iinfo;

  if (argc != 3) {
    fprintf(stderr,"usage: %s infile id\n", argv[0]);
    exit(-1);
  }

  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  page_id = atoi(argv[2]);
  if (page_id <1) {
    fprintf(stderr,"please specify a page_id >= 1.\n");
    fprintf(stderr,"usage: %s infile page_id\n", argv[0]);
    exit(-1);
  }

  file_size = get_file_size(fin);

  interval = file_size;
  position = 0;
  oldmarker = -1;
  pinfo.bits_shifted = -1;
  pinfo.position = -1;
  pinfo.page_id = -1;

  iinfo.left_end = 0;
  file_size = get_file_size(fin);
  iinfo.right_end = file_size;
  iinfo.value_wanted = page_id;

  res = get_first_page_id_after_offset(fin, 0, &pinfo);
  if (res > 0) {
    iinfo.last_value = pinfo.page_id;
    iinfo.last_position = 0;
  }
  else {
    fprintf(stderr,"failed to get anything useful from the beginning of the file even, bailing.\n");
    exit(1);
  }
  if (pinfo.page_id == page_id) {
      fprintf(stdout,"position:%d page_id:%d\n",pinfo.position, pinfo.page_id);
      exit(0);
  }

  while (1) {
    res = do_iteration(&iinfo, fin, &pinfo);
    /* things to check: bad return? interval is 0 bytes long? */
    if (iinfo.left_end == iinfo.right_end) {
      fprintf(stdout,"position:%d page_id:%d\n",pinfo.position, pinfo.page_id);
      exit(0);
    }
    else if (res < 0) {
      fprintf(stderr,"broken and quitting\n");
      exit(-1);
    }
  }
  exit(0);
}
