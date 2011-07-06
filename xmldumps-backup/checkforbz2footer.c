#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>

/* 
   Check to see whether a file ends with a bz2 footer or not
   (i.e. if it is truncated or corrupted). 
   This is a crude but fast test for integrity; we don't 
   check the CRC at the end of fthe stream, nor do we check the
   bit padding in the last byte of the file.

   Arguments: the name of the file to check, presumably 
   a bzipped file. 
   Outputs: none.
   Exits with 0 if the file contains the footer at the end, 
   1 if the file does not contain the footer, and -1 on error.
*/


int read_footer(unsigned char *buffer, int fin) {
  int res;

  res = lseek(fin, -11, SEEK_END);
  if (res == -1) {
    fprintf(stderr,"lseek of file failed\n");
    exit(-1);
  }
  res = read(fin, buffer, 11);
  if (res == -1) {
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

void shiftbytesright(unsigned char *buffer, int buflen, int numbits) {
    int i;

    for (i=buflen-1; i>=0; i--) {
      /* right 1 */
      buffer[i] = (unsigned char) ((int) (buffer[i]) >> numbits);
      
      /* grab rightmost from prev byte */
      if (i > 0) {
	buffer[i] = ( unsigned char ) ((unsigned int) buffer[i] | ( ((unsigned int) (buffer[i-1])<<(8-numbits))  & bitmask(1,LEFT)));
      }
    }
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

int checkfileforfooter(int fin) {
  unsigned char buffer[11];
  int result, i;
  unsigned char **footer = malloc(8*sizeof(unsigned char *));

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

  read_footer(buffer,fin);

  result = bytescompare(footer[0],buffer+1,6,0);
  if (!result) {
    return(0);
  }
  
  for (i=1; i<8; i++) {
    result = bytescompare(footer[i],buffer,7,i);
    if (!result) {
      return(0);
    }
  }
  return(1);
}

int main(int argc, char **argv) {

  int fin;
  int result;

  if (argc != 2) {
    fprintf(stderr,"usage: %s infile\n", argv[0]);
    exit(-1);
  }
  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }
  result = checkfileforfooter(fin);
  close(fin);
  exit(result);
}

