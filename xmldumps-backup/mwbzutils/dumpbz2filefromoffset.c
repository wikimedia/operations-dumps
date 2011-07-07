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
#include "mwbzutils.h"

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
  int fin, res;
  off_t position;

  if (argc != 3) {
    fprintf(stderr,"usage: %s infile position\n", argv[0]);
    exit(-1);
  }

  fin = open (argv[1], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[1]);
    exit(-1);
  }

  position = atoll(argv[2]);
  if (position <(off_t)0) {
    fprintf(stderr,"please specify a position >= 0.\n");
    fprintf(stderr,"usage: %s infile position\n", argv[0]);
    exit(-1);
  }
  /* input file, starting position in file, length of buffer for reading */
  res = dump_mw_header(fin);

  res = dump_from_first_page_id_after_offset(fin, position);
  exit(res);
}
