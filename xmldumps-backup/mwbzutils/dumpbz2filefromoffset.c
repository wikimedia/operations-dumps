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
#include <getopt.h>
#include "mwbzutils.h"

void usage(char *message) {
  char * help =
"Usage: dumpbz2filefromoffset [--version|--help]\n"
"   or: dumpbz2filefromoffset <infile> <offset>\n\n"
"Find the first bz2 block in a file after the specified offset, uncompress\n"
"and write contents from that point on to stdout, starting with the first\n"
"<page> tag encountered.\n\n"
"The starting <mediawiki> tag and the <siteinfo> header from the file will\n"
"be written out first.\n\n"
"Note that some bytes from the very last block may be lost if the blocks are\n"
"not byte-aligned. This is due to the bzip2 crc at the eof being wrong.\n\n"
"Exits with BZ_OK on success, various BZ_ errors otherwise.\n\n"
"Options:\n\n"
"Flags:\n\n"
"  -h, --help       Show this help message\n"
"  -v, --version    Display the version of this program and exit\n\n"
"Arguments:\n\n"
"  <infile>         Name of the file to check\n"
"  <offset>         byte in the file from which to start processing\n\n"
"Report bugs in dumpbz2filefromoffset to <https://phabricator.wikimedia.org/>.\n\n"
"See also checkforbz2footer(1), dumplastbz2block(1), findpageidinbz2xml(1),\n"
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
  fprintf(stderr,"dumpbz2filefromoffset %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

/* 
   dump the <mediawiki> header (up through
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
  bfile.position = (off_t)0;

  while ((get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD)>=0) && (! bfile.eof) && (!done)) {
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
int dump_from_first_page_id_after_offset(int fin, off_t position) {
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

  while ((get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD)>=0) && (! bfile.eof)) {
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

int main(int argc, char **argv) {
  int fin, res;
  off_t position;

  int optc;
  int optindex=0;

  struct option optvalues[] = {
    {"help", 0, 0, 'h'},
    {"version", 0, 0, 'v'},
    {NULL, 0, NULL, 0}
  };

  if (argc < 2 || argc > 3) {
    usage("Missing or bad options/arguments");
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
  optind++;
  if (optind >= argc) {
    usage("Missing offset argument.");
  }
  position = atoll(argv[optind]);
  if (position <(off_t)0) {
    fprintf(stderr,"please specify an offset >= 0.\n");
    fprintf(stderr,"usage: %s infile offset\n", argv[0]);
    exit(-1);
  }
  /* input file, starting position in file, length of buffer for reading */
  res = dump_mw_header(fin);

  res = dump_from_first_page_id_after_offset(fin, position);
  exit(res);
}
