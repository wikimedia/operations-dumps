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
int bit_mask(int numbits, int end) {
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
      buffer[i] = ( unsigned char ) ( (unsigned int) buffer[i] | ( ( ((unsigned int) buffer[i+1])  & bit_mask(numbits,MASKLEFT) ) >> (8-numbits) ) );
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
      buffer[i] = ( unsigned char ) ((unsigned int) buffer[i] | ( ((unsigned int) (buffer[i-1])<<(8-numbits))  & bit_mask(numbits,MASKLEFT)));
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
    if ((buff1[0] & bit_mask(8-bitsrightshifted,MASKRIGHT))  != (buff2[0] & bit_mask(8-bitsrightshifted,MASKRIGHT)) ) {
      return(1);
    }
    /* do rightmost byte */
    if ((buff1[numbytes-1] & bit_mask(bitsrightshifted,MASKLEFT))  != (buff2[numbytes-1] & bit_mask(bitsrightshifted,MASKLEFT)) ) {
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
  if (result < 0) {
    fprintf(stderr,"read of file failed\n");
    exit(-1);
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
	exit(-1);
      }
      result = read(fin, bfile->marker_buffer, 7);
      if (result < 7) {
	/* fprintf(stderr,"read of file failed\n"); */
	exit(-1);
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
  if (res < 0) {
    fprintf(stderr,"lseek of file to 0 failed (3)\n");
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
    if (res < 0) {
      fprintf(stderr,"lseek of file to %ld failed (4)\n",(long int) bfile->position+1);
      return(-1);
    }
  }
  else {
    res = lseek(fin,bfile->position,SEEK_SET);
    if (res < 0) {
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
      bfile->position+=bfile->bytes_read;
      bfile->overflow = bfile->bufin[bfile->bytes_read];
      shift_bytes_left(bfile->bufin,bfile->bytes_read+1,bfile->bits_shifted);
      bfile->strm.avail_in = bfile->bytes_read;
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
  if (res < 0) {
    fprintf(stderr,"lseek of file to 0 failed (6)\n");
    exit(-1);
  }
  return(res);
}


/* 
   set up the marker, seek to right place, get first
   buffer of compressed data for processing
   bfile->position must be set to desired offset first by caller.
   returns:
   -1 if no marker or other error, position of next read if ok 
*/
int init_bz2_file(bz_info_t *bfile, int fin) {
  int res;

  bfile->bufin_size = BUFINSIZE;
  bfile->marker = init_marker();
  bfile->bytes_read = 0;
  bfile->bytes_written = 0;
  bfile->eof = 0;

  bfile->initialized++;

  bfile->file_size = get_file_size(fin);
  if (bfile->position > bfile->file_size) {
    fprintf(stderr,"asked for position past end of file\n");
    exit(-1);
  }
  res = lseek(fin, bfile->position, SEEK_SET);
  if (res < 0) {
    fprintf(stderr,"lseek of file to %ld failed (7)\n",(long int) bfile->position);
    exit(-1);
  }

  find_next_bz2_block_marker(fin, bfile);
  if (bfile->bits_shifted >= 0) {
    /*    fprintf(stderr,"marker bits shifted by is %d\n",bfile->bits_shifted); */
    init_decompress(bfile);
    decompress_header(fin, bfile);
    setup_first_buffer_to_decompress(fin, bfile);
    return(0);
  }
  return(-1);
}

/* get the next buffer of uncompressed stuff */
int decompress_data(bz_info_t *bfile, int fin, unsigned char *bufferout, int bufout_size) {
  int ret;

  bfile->bufout = bufferout;
  bfile->bufout_size = bufout_size;
  bfile->bytes_written = 0;

  if (! bfile->initialized) {
    if (init_bz2_file(bfile, fin) == -1) {
      fprintf(stderr,"failed to initialize bz2file\n");
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
      fprintf(stderr,"error from BZ decompress %d\n",ret);
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
    fprintf(stdout,"DEBUG buffer full\n");
    return(0);
  }

  if (buffer_is_empty(b)) {
    b->next_to_fill = b->buffer;
  }

  res = decompress_data(bfile, fin, b->next_to_fill, b->end - b->next_to_fill);
  if (res <0 ) {
    return(res);
  }
  if (bfile->bytes_written < 0) {
    fprintf(stderr,"read of file failed\n");
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
int  move_bytes_to_buffer_start(buf_info_t *b, unsigned char *fromwhere, int maxbytes) {
  int i, tocopy;

  if (fromwhere >= b->end) {
    return(0);
  }
  else {
    tocopy = b->end - fromwhere;
    if (maxbytes && (tocopy > maxbytes)) {
      tocopy = maxbytes;
    }
    for (i = 0; i < tocopy; i++) {
      b->buffer[i] = fromwhere[i];
    }
    b->next_to_fill = b->buffer + tocopy;
    b->next_to_fill[0] = '\0';
    b->next_to_read = b->buffer; 
    b->bytes_avail = tocopy;
    return(tocopy);
  }
}

/* 
   dump the <meadiawiki> header (up through
   </siteinfo> close tag) found at the 
   beginning of xml dump files. 
   returns:
      0 on success,
      -1 on error
*/
int dump_mw_header(int fin) {
  int res;
  regmatch_t *match_siteinfo;
  regex_t compiled_siteinfo;
  int length=5000; /* output buffer size */
  char *siteinfo = "  </siteinfo>\n";

  buf_info_t *b;
  bz_info_t bfile;

  int firstpage = 1;
  int done = 0;
  bfile.initialized = 0;

  res = regcomp(&compiled_siteinfo, siteinfo, REG_EXTENDED);

  match_siteinfo = (regmatch_t *)malloc(sizeof(regmatch_t)*1);

  b = init_buffer(length);
  bfile.bytes_read = 0;
  bfile.position = 0;

  while ((get_buffer_of_uncompressed_data(b, fin, &bfile)>=0) && (! bfile.eof) && (!done)) {
    /* fixme either we don't check the return code right or we don't notice no bytes read or we don't clear the bytes read */
    if (bfile.bytes_read) {
      if (firstpage) {
	if (bfile.bytes_read >= 11 && !memcmp((char *)b->next_to_read,"<mediawiki ",11)) {
	  /* good, write it and loop and not firstpage any more */
	  if (b->bytes_avail) {
	    if (regexec(&compiled_siteinfo, (char *)b->next_to_read,  2,  match_siteinfo, 0 ) == 0) {
	      fwrite(b->next_to_read,match_siteinfo[0].rm_eo, 1, stdout);
	      b->next_to_read = b->end;
	      b->bytes_avail = 0;
	      b->next_to_fill = b->buffer; /* empty */
	      bfile.strm.next_out = (char *)b->next_to_fill;
	      bfile.strm.avail_out = b->end - b->next_to_fill;
	      done++;
	    }
	    else {
	      fwrite(b->next_to_read,b->bytes_avail,1,stdout);
	      b->next_to_read = b->end;
	      b->bytes_avail = 0;
	      b->next_to_fill = b->buffer; /* empty */
	      bfile.strm.next_out = (char *)b->next_to_fill;
	      bfile.strm.avail_out = b->end - b->next_to_fill;
	    }
	  }  
	}
	else {
	  fprintf(stderr,"missing mediawiki header from bz2 xml file\n");
	  return(-1);
	}
	firstpage = 0;
      }
      else { /* not firstpage */
	if (regexec(&compiled_siteinfo, (char *)b->next_to_read,  2,  match_siteinfo, 0 ) == 0) {
	  fwrite(b->next_to_read,match_siteinfo[0].rm_eo, 1, stdout);
	  b->next_to_read = b->end;
	  b->bytes_avail = 0;
	  b->next_to_fill = b->buffer; /* empty */
	  bfile.strm.next_out = (char *)b->next_to_fill;
	  bfile.strm.avail_out = b->end - b->next_to_fill;
	  done++;
	}
	else {
	  /* could have the first part of the siteinfo tag... so copy up enough bytes to cover that case */
	  if (b->bytes_avail> 12) {
	    /* write everything that didn't match, but leave 12 bytes, to stdout */
	    fwrite(b->next_to_read,b->bytes_avail - 12,1,stdout);
	    move_bytes_to_buffer_start(b, b->next_to_read + b->bytes_avail - 12, 12);
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
	      /* there were only 12 or less bytes so just save em don't write em to stdout */
	      move_bytes_to_buffer_start(b, b->next_to_read, b->bytes_avail);
	      bfile.strm.next_out = (char *)b->next_to_fill;
	      bfile.strm.avail_out = b->end - b->next_to_fill;
	    }
	  }
	}
      } /* end notfirstpage */
    }
  }
  if (!done) {
    fprintf(stderr,"incomplete or no mediawiki header found\n");
    return(-1);
  }
  else {
    return(0);
  }
}

/* 
   find the first page id after position in file 
   decompress and dump to stdout from that point on
   returns:
      0 on success,
      -1 on error
*/
int dump_from_first_page_id_after_offset(int fin, int position) {
  int res;
  regmatch_t *match_page;
  regex_t compiled_page;
  int length=5000; /* output buffer size */
  char *page = "  <page>";

  buf_info_t *b;
  bz_info_t bfile;

  int firstpage = 1;

  bfile.initialized = 0;

  res = regcomp(&compiled_page, page, REG_EXTENDED);

  match_page = (regmatch_t *)malloc(sizeof(regmatch_t)*1);

  b = init_buffer(length);
  bfile.bytes_read = 0;
  bfile.position = position;

  while ((get_buffer_of_uncompressed_data(b, fin, &bfile)>=0) && (! bfile.eof)) {
    /* fixme either we don't check the return code right or we don't notice no bytes read or we don't clear the bytes read */
    if (bfile.bytes_read) {
      if (firstpage) {
	if (regexec(&compiled_page, (char *)b->next_to_read,  2,  match_page, 0 ) == 0) {
	  fwrite(b->next_to_read+match_page[0].rm_so,b->next_to_fill - (b->next_to_read+match_page[0].rm_so), 1, stdout);
	  b->next_to_read = b->end;
	  b->bytes_avail = 0;
	  b->next_to_fill = b->buffer; /* empty */
	  bfile.strm.next_out = (char *)b->next_to_fill;
	  bfile.strm.avail_out = b->end - b->next_to_fill;
	  firstpage = 0;
	}
	else {
	  /* could have the first part of the page tag... so copy up enough bytes to cover that case */
	  if (b->bytes_avail> 7) {
	    /* write everything that didn't match, but leave 7 bytes, to stdout */
	    fwrite(b->next_to_read,b->bytes_avail - 7,1,stdout);
	    move_bytes_to_buffer_start(b, b->next_to_read + b->bytes_avail - 7, 7);
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
	      /* there were only 7 or less bytes so just save em don't write em to stdout */
	      move_bytes_to_buffer_start(b, b->next_to_read, b->bytes_avail);
	      bfile.strm.next_out = (char *)b->next_to_fill;
	      bfile.strm.avail_out = b->end - b->next_to_fill;
	    }
	  }
	}
      }
      else {
	if (b->bytes_avail) {
	  fwrite(b->next_to_read,b->bytes_avail,1,stdout);
	  b->next_to_read = b->end;
	  b->bytes_avail = 0;
	  b->next_to_fill = b->buffer; /* empty */
	  bfile.strm.next_out = (char *)b->next_to_fill;
	  bfile.strm.avail_out = b->end - b->next_to_fill;
	}
      }
    }
  }
  if (b->bytes_avail) {
    fwrite(b->next_to_read,b->bytes_avail,1,stdout);
    b->next_to_read = b->end;
    b->bytes_avail = 0;
    b->next_to_fill = b->buffer; /* empty */
    bfile.strm.next_out = (char *)b->next_to_fill;
    bfile.strm.avail_out = b->end - b->next_to_fill;
  }  
  return(0);
}

/*
  find the first bz2 block after the specified offset,
  uncompress from that point on, write out the
  contents starting with the first <page> tag,
  prefacing first with the <mediawiki> header from
  the beginning of the file, up through </siteinfo>.

  note that we may lose some bytes from the very last
  block if the blocks are bit shifted, because the
  bzip crc at end of file will be wrong.  (needs testing to
  find a workaround, simply not feeding in the crc doesn't
  suffice)

  for purposes of the XML dumps this is fine, since we use
  this tool to generate prefetch data starting from
  a given pageid, rather than needing to uncompress
  gigabytes of data to get to the point in the file
  we want.

  returns:
    BZ_OK on success, various BZ_ errors otherwise.
*/
int main(int argc, char **argv) {
  int fin, position, res;

  if (argc != 3) {
    fprintf(stderr,"usage: %s infile position\n", argv[0]);
    exit(-1);
  }

  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  position = atoi(argv[2]);
  if (position <0) {
    fprintf(stderr,"please specify a position >= 0.\n");
    fprintf(stderr,"usage: %s infile position\n", argv[0]);
    exit(-1);
  }
  /* input file, starting position in file, length of buffer for reading */
  res = dump_mw_header(fin);

  res = dump_from_first_page_id_after_offset(fin, position);
  exit(res);
}
