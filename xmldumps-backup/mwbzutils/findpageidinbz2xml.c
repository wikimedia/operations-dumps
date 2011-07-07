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
   find the first bz2 block marker in the file, 
   from its current position,
   then set up for decompression from that point 
   returns: 
     0 on success
     -1 if no marker or other error
*/
int init_and_read_first_buffer_bz2_file(bz_info_t *bfile, int fin) {
  int res;

  bfile->initialized++;

  res = find_next_bz2_block_marker(fin, bfile, FORWARD);
  if (res ==1) {
    init_decompress(bfile);
    decompress_header(fin, bfile);
    setup_first_buffer_to_decompress(fin, bfile);
    return(0);
  }
  return(-1);
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

  if (find_first_bz2_block_from_offset(&bfile, fin, position, FORWARD) <= 0) {
    /* fprintf(stderr,"failed to find block in bz2file (1)\n"); */
    return(-1);
  }

  while (!get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD) && (! bfile.eof)) {
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
