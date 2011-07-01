#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include "bzlib.h"

/* 
   Find the last bz2 block marker in a file
   and dump whatever can be decompressed after
   that point.  The header of the file must
   be intact in order for any output to be produced.
   This will produce output for truncated files as well,
   as long as there is "enough" data after the block 
   marker.

   Arguments: the name of the file to check, presumably 
   a bzipped file. 
   Outputs: the decompressed data at the end of the file.
   Exits with 0 if decompression of some data can be done,
   1 if decompression fails, and -1 on error.
*/

#define BUFSIZE 121072
typedef struct {
  unsigned char bufin[BUFSIZE];
  unsigned char bufout[BUFSIZE];
  int bufsize;
  bz_stream strm;
  unsigned char overflow;
  int bitsshifted;
  int position;
} bzinfo;

int read_footer(unsigned char *buffer, int fin) {
  int res;

  res = lseek(fin, -11, SEEK_END);
  if (res < 0) {
    fprintf(stderr,"lseek of file failed\n");
    exit(-1);
  }
  res = read(fin, buffer, 11);
  if (res < 0) {
    fprintf(stderr,"read of file failed\n");
    exit(-1);
  }
  return(0);
}

#define LEFT 0
#define RIGHT 1

/* return n ones either at left or right end */
int bitmask(int numbits, int end) {
  if (end == RIGHT) {
    return((1<<numbits)-1);
  }
  else {
    return(((1<<numbits)-1) << (8-numbits));
  }
}

void shiftbytesleft(unsigned char *buffer, int buflen, int numbits) {
    int i;

    if (numbits == 0) {
      return;
    }

    for (i=0; i<buflen; i++) {
      /* left 1 */
      buffer[i] = (unsigned char) ((int) (buffer[i]) << numbits);
      
      /* grab leftmost from next byte */
      if (i < buflen-1) {
	buffer[i] = ( unsigned char ) ( (unsigned int) buffer[i] | ( ( ((unsigned int) buffer[i+1])  & bitmask(numbits,LEFT) ) >> (8-numbits) ) );
      }
    }
}


void shiftbytesright(unsigned char *buffer, int buflen, int numbits) {
    int i;

    for (i=buflen-1; i>=0; i--) {
      /* right 1 */
      buffer[i] = (unsigned char) ((int) (buffer[i]) >> numbits);
      
      /* grab rightmost from prev byte */
      if (i > 0) {
	buffer[i] = ( unsigned char ) ((unsigned int) buffer[i] | ( ((unsigned int) (buffer[i-1])<<(8-numbits))  & bitmask(numbits,LEFT)));
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
    shiftbytesright(marker[i],7,1);
  }
  return(marker);
}

unsigned char ** init_footer() {
  unsigned char **footer = malloc(8*sizeof(unsigned char *));
  int i;

  /* set up footer plus its various right-shifted incarnations */
  /* dude why couldn't you have 0 padded each bzip2 block? seriously ... */
  for (i = 0; i< 8; i++) {
    footer[i] = malloc(sizeof(unsigned char)*7);
  }
  footer[0][0]= (unsigned char) 0x17;
  footer[0][1]= (unsigned char) 0x72;
  footer[0][2]= (unsigned char) 0x45;
  footer[0][3]= (unsigned char) 0x38;
  footer[0][4]= (unsigned char) 0x50;
  footer[0][5]= (unsigned char) 0x90;
  footer[0][6]= (unsigned char) 0x00;
  for (i = 1; i< 8; i++) {
    memcpy((char *)(footer[i]), (char *)(footer[i-1]),7);
    shiftbytesright(footer[i],7,1);
  }
  return(footer);
}


/* buff1 is some random bytes, buff2 is some random bytes which we expect to start with the contents of buff1, 
 both buffers are  bit-shifted to the right "bitsrightshifted". this function compares the two and returns 1 if buff2
 matches and 0 otherwise. */
int bytescompare(unsigned char *buff1, unsigned char *buff2, int numbytes, int bitsrightshifted) {
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
    if ((buff1[0] & bitmask(8-bitsrightshifted,RIGHT))  != (buff2[0] & bitmask(8-bitsrightshifted,RIGHT)) ) {
      return(1);
    }
    /* do rightmost byte */
    if ((buff1[numbytes-1] & bitmask(bitsrightshifted,LEFT))  != (buff2[numbytes-1] & bitmask(bitsrightshifted,LEFT)) ) {
      return(1);
    }
    return(0);
  }
}

/* return -1 if no match
   return number of bits rightshifted otherwise */
int checkfileforfooter(int fin, unsigned char **footer) {
  unsigned char buffer[11];
  int result, i;

  read_footer(buffer,fin);

  result = bytescompare(footer[0],buffer+1,6,0);
  if (!result) {
    return(0);
  }
  
  for (i=1; i<8; i++) {
    result = bytescompare(footer[i],buffer,7,i);
    if (!result) {
      return(i);
    }
  }
  return(-1);
}

/* return -1 if no match
   return number of bits rightshifted otherwise */
int checkbufferforblockmarker(unsigned char *buffer, unsigned char **marker) {
  int result, i;

  result = bytescompare(marker[0],buffer+1,6,0);
  if (!result) {
    return(0);
  }
  for (i=1; i<8; i++) {
    result = bytescompare(marker[i],buffer,7,i);
    if (!result) {
      return(i);
    }
  }
  return(-1);
}

void clearbuffer(unsigned char *buf, int length) {
  int i;

  for (i=0; i<length; i++) {
    buf[i]=0;
  }
  return;
}

int findnextmarker(int fin, int *start_at, int *position, unsigned char **marker, unsigned char *buffer ) {
  int bitsshifted = -1;
  int result;

  /* must be after 4 byte file header, and we add a leftmost byte to the buffer 
     of data read in case some bits have been shifted into it */
  while (*position >= 3 && bitsshifted < 0) { 
    bitsshifted = checkbufferforblockmarker(buffer, marker);
    if (bitsshifted < 0) {
      (*start_at)++;
      /*
      if (*start_at % 10000 == 0) {
	fprintf(stderr, "starting at %d, position %d\n", *start_at, *position);
      }
      */
      *position = lseek(fin, -1*(*start_at), SEEK_END);
      if (*position < 0) {
	fprintf(stderr,"lseek of file failed\n");
	exit(-1);
      }
      result = read(fin, buffer, 7);
      if (result < 0) {
	fprintf(stderr,"read of file failed\n");
	exit(-1);
      }
    }
    else {
      return(bitsshifted);
    }
  }
  return(bitsshifted);
}

int init_decompress(bzinfo *bfile) {
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

int decompress_header(int fin, bzinfo *bfile) {
  int bytesread, ret;
  unsigned char header[4];

  lseek(fin,0,SEEK_SET);
  bytesread = read(fin, header, 4);
  if (bytesread < 4) {
    fprintf(stderr,"failed to read 4 bytes of header, exiting\n");
    exit(-1);
  }
  bfile->strm.next_in = (char *)header;
  bfile->strm.avail_in = 4;

  bfile->strm.next_out = (char *)(bfile->bufout);
  bfile->strm.avail_out = bfile->bufsize;
  ret = BZ2_bzDecompress ( &(bfile->strm) );
  if (BZ_OK != ret && BZ_STREAM_END != ret) {
    fprintf(stderr,"Corrupt bzip2 header, exiting\n");
    exit(-1);
  }
  return(ret);
}

int setup_first_buffer(int fin, bzinfo *bfile) {
  int bytesread, eof=0;

  if (bfile->bitsshifted == 0) {
    lseek(fin,bfile->position+1,SEEK_SET);
  }
  else {
    lseek(fin,bfile->position,SEEK_SET);
  }
  bytesread = read(fin, bfile->bufin, bfile->bufsize);
  if (bytesread > 0) {
    bfile->overflow = bfile->bufin[bytesread-1];
    shiftbytesleft(bfile->bufin,bytesread,bfile->bitsshifted);

    bfile->strm.next_in = (char *)(bfile->bufin);
    bfile->strm.avail_in = bytesread-1;

    bfile->strm.next_out = (char *)(bfile->bufout);
    bfile->strm.avail_out = bfile->bufsize;
  }
  if (bytesread <=0) {
    eof++;
  }
  return(eof);
}

int do_last_byte(bzinfo *bfile) {
  int ret=BZ_OK;
  int written;

  if (bfile->strm.avail_in == 0) {
    bfile->strm.next_in = (char *)(bfile->bufin);
    bfile->bufin[0] = bfile->overflow;
    shiftbytesleft(bfile->bufin,1,bfile->bitsshifted);
    bfile->strm.avail_in = 1;
    bfile->strm.next_out = (char *)(bfile->bufout);
    bfile->strm.avail_out = bfile->bufsize;
    ret = BZ2_bzDecompress ( &(bfile->strm) );
    if (BZ_OK == ret || BZ_STREAM_END == ret) {
      written = fwrite(bfile->bufout, sizeof(unsigned char), (unsigned char *)bfile->strm.next_out - bfile->bufout, stdout);
    }
  }
  return(ret);
}

int read_next_buffer(int fin, bzinfo *bfile, int ret) {
  int bytesread, eof=0;

  /*	fprintf(stderr," got return from decompress of %d\n", ret); */
  
  if (bfile->strm.avail_in == 0) {
    bfile->strm.next_in = (char *)(bfile->bufin);
    bfile->bufin[0] = bfile->overflow;
    bytesread = read(fin, bfile->bufin+1, bfile->bufsize-1);
    if (bytesread > 0) {
      bfile->overflow = bfile->bufin[bytesread];
      shiftbytesleft(bfile->bufin,bytesread+1,bfile->bitsshifted);
      bfile->strm.avail_in = bytesread;
    }
    else {
      eof++;
      bfile->strm.avail_in = 0;
    }
  }
  bfile->strm.next_out = (char *)(bfile->bufout);
  bfile->strm.avail_out = bfile->bufsize;

  return(eof);
}


int main(int argc, char **argv) {

  bzinfo bfile;

  int fin;
  int result, ret;
  unsigned char buffer[8];

  unsigned char **footer;
  unsigned char **marker;

  int written=0;
  int start_at;

  int eof = 0;

  if (argc != 2) {
    fprintf(stderr,"usage: %s infile\n", argv[0]);
    exit(-1);
  }

  marker = init_marker();
  footer = init_footer();

  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  bfile.bufsize = BUFSIZE;

  result = checkfileforfooter(fin, footer);
  if (result == -1) {
    start_at = 0;
  }
  else {
    start_at = 11; /* size of footer, perhaps with 1 byte extra */
  }
  start_at +=6; /* size of marker */
  bfile.position = lseek(fin, -1*start_at, SEEK_END);
  if (bfile.position < 0) {
    fprintf(stderr,"lseek of file failed\n");
    exit(-1);
  }
  result = read(fin, buffer, 7);
  if (result < 0) {
    fprintf(stderr,"read of file failed\n");
    exit(-1);
  }

  while (1) {

    bfile.bitsshifted = findnextmarker(fin, &start_at, &bfile.position, marker, buffer);
    if (bfile.bitsshifted >= 0) {
      /*      fprintf(stderr, "found marker at pos %d and shifted %d, start_at is %d\n", bfile.position,  bfile.bitsshifted, start_at); */
      ret = init_decompress(&bfile);

      /* pass in the header */
      ret = decompress_header(fin,&bfile);

      eof = setup_first_buffer(fin, &bfile);

      while (BZ_OK == ret && !eof) {
	ret = BZ2_bzDecompress ( &(bfile.strm) );
	if (BZ_OK == ret || BZ_STREAM_END == ret) {
	  written += fwrite(bfile.bufout, sizeof(unsigned char), (unsigned char *)(bfile.strm.next_out) - bfile.bufout, stdout);
	}
	eof = read_next_buffer(fin, &bfile, ret);
      }
      if (BZ_OK == ret || BZ_STREAM_END == ret ) {
	/* so we read no bytes, process the last byte we held */
	do_last_byte(&bfile);
      }
      if (written == 0) {
	/* truncated block or other corruption, try going back one */
	start_at +=5; 
	clearbuffer(buffer,sizeof(buffer));
	continue;
      }
      else {
	break;
      }
    }
    else {
      fprintf(stderr,"no block marker in this file.\n");
      exit(-1);
    }
  }
  close(fin);
  exit(0);
}

