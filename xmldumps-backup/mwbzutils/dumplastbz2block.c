#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>
#include "mwbzutils.h"


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

int main(int argc, char **argv) {

  bz_info_t bfile;

  int fin;
  int result;
  buf_info_t *b;

  int firstblock = 1;
  int length = 5000; /* output buffer size */

  if (argc != 2) {
    fprintf(stderr,"usage: %s infile\n", argv[0]);
    exit(-1);
  }

  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  bfile.file_size = get_file_size(fin);
  bfile.footer = init_footer();
  result = check_file_for_footer(fin, &bfile);
  if (result == -1) {
    bfile.position = bfile.file_size;
  }
  else {
    bfile.position = bfile.file_size - (off_t)11; /* size of footer, perhaps with 1 byte extra */
  }
  bfile.position -=(off_t)6; /* size of marker */
  bfile.initialized = 0;
  b = init_buffer(length);
  bfile.bytes_read = 0;

  /*  init_bz2_file(&bfile, fin, BACKWARD); */
  firstblock = 1;

  if (find_first_bz2_block_from_offset(&bfile, fin, bfile.position, BACKWARD) <= (off_t)0) {
    fprintf(stderr,"failed to find block in bz2file\n");
    exit(-1);
  }
  while ((get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD)>=0) && (! bfile.eof) && (! bfile.position == (off_t)0)) {
    if (bfile.bytes_read) {
      fwrite(b->next_to_read,b->bytes_avail,1,stdout);
      b->next_to_read = b->end;
      b->bytes_avail = 0;
      b->next_to_fill = b->buffer; /* empty */
      bfile.strm.next_out = (char *)b->next_to_fill;
      bfile.strm.avail_out = b->end - b->next_to_fill;
      firstblock = 0;
    }
    else {
      /* should never happen */
      fprintf(stderr,"there was a block but now it's gone, giving up\n");
      exit(-1);
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
  close(fin);
  exit(0);
}

