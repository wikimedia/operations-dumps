#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include "mwbzutils.h"

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
   -1 if the file does not contain the footer or there is an error.
*/


int main(int argc, char **argv) {

  int fin;
  int result;
  bz_info_t bfile;

  if (argc != 2) {
    fprintf(stderr,"usage: %s infile\n", argv[0]);
    exit(-1);
  }
  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  bfile.footer = init_footer();
  result = check_file_for_footer(fin, &bfile);
  close(fin);
  if (result == -1) {
    return(result);
  }
  else {
    return(0);
  }
}

