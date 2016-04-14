#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>
#include <getopt.h>
#include "mwbzutils.h"

void usage(char *message) {
  char * help =
"Usage: dumplastbz2block [--version|--help]\n"
"   or: dumplastbz2block <infile>\n\n"
"Find the last bz2 block marker in a file and dump whatever can be\n"
"decompressed after that point.  The header of the file must be intact\n"
"in order for any output to be produced.\n"
"This will produce output for truncated files as well, as long as there\n"
"is 'enough' data after the block marker.\n"
"Exits with 0 if some decompressed data was written, 1 if no data could\n"
"be uncompressed and -1 on error.\n\n"
"Options:\n\n"
"Flags:\n\n"
"  -h, --help       Show this help message\n"
"  -v, --version    Display the version of this program and exit\n\n"
"Arguments:\n\n"
"  <infile>         Name of the file to process\n\n"
"Report bugs in dumplastbz2block to <https://phabricator.wikimedia.org/>.\n\n"
"See also checkforbz2footer(1), dumpbz2filefromoffset(1), findpageidinbz2xml(1),\n"
"recompressxml(1), writeuptopageid(1)\n\n";
  if (message) {
    fprintf(stderr,"%s\n\n",message);
  }
  fprintf(stderr,"%s",help);
  exit(-1);
}

void show_version(char *version_string) {
  char * copyright =
"Copyright (C) 2011, 2012, 2013 Ariel T. Glenn.  All rights reserved.\n\n"
"This program is free software: you can redistribute it and/or modify it\n"
"under the  terms of the GNU General Public License as published by the\n"
"Free Software Foundation, either version 2 of the License, or (at your\n"
"option) any later version.\n\n"
"This  program  is  distributed  in the hope that it will be useful, but\n"
"WITHOUT ANY WARRANTY; without even the implied warranty of \n"
"MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General\n"
"Public License for more details.\n\n"
"You should have received a copy of the GNU General Public License along\n"
"with this program.  If not, see <http://www.gnu.org/licenses/>\n\n"
    "Written by Ariel T. Glenn.\n";
  fprintf(stderr,"dumplastbz2block %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}


int main(int argc, char **argv) {

  bz_info_t bfile;

  int fin;
  int result;
  buf_info_t *b;

  int firstblock = 1;
  int length = 5000; /* output buffer size */

  int optc;
  int optindex=0;

  struct option optvalues[] = {
    {"help", 0, 0, 'h'},
    {"version", 0, 0, 'v'},
    {NULL, 0, NULL, 0}
  };

  if (argc != 2) {
    usage("Missing option or argument.");
    exit(-1);
  }

  while (1) {
    optc=getopt_long_only(argc,argv,"hv", optvalues, &optindex);
    if (optc=='h')
      usage(NULL);
    else if (optc=='v')
      show_version(VERSION);
    else if (optc==-1) break;
    else usage("Unknown option or other error\n");
  }

  if (optind >= argc) {
    usage("Missing filename argument.");
  }

  fin = open (argv[optind], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[optind]);
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
