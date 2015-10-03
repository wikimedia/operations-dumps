#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include <inttypes.h>
#include <zlib.h>
#include "mwbzutils.h"

void usage(char *message) {
  char * help =
"Usage: findpageidinbz2xml --filename file --pageid id [--stubfile] [--useapi] [--verbose]\n"
"       [--help] [--version]\n\n"
"Show the offset of the bz2 block in the specified MediaWiki XML dump file\n"
"containing the given page id.  This assumes that the bz2 header of the file\n"
"is intact and that page ids are steadily increasing throughout the file.\n\n"
"If the page id is found, a line in the following format will be written to stdout:\n"
"    position:xxxxx pageid:nnn\n\n"
"where 'xxxxx' is the offset of the block from the beginning of the file, and\n"
"'nnn' is the id of the first page encountered in that block.\n\n"
"Note:\n"
"This program may use the MediaWiki api to find page ids from revision ids\n"
"if 'useapi' is specified.\n"
"It may use a stub file to find page ids from rev ids if 'stubfile' is specified.\n"
"It will only do one of the above if it has been reading from the file for some\n"
"large number of iterations without findind a page tag (some pages have > 500K\n"
"revisions and a heck of a lot of text).\n"
"If both 'useapi' and 'stubfile' are specified, the api will be used as it is faster.\n\n"
"Exits with 0 in success, -1 on error.\n\n"
"Options:\n\n"
"  -f, --filename   name of file to search\n"
"  -p, --pageid     page_id of page for which to search\n"
"  -s, --stubfile   name of MediaWiki XML stub file to fall back on (see 'Note' above)\n"
"  -a, --useapi     fall back to the api if stuck (see 'Note' above)\n"
"  -V, --verbose    show search process; specify multiple times for more output\n"
"  -h, --help       Show this help message\n"
"  -V, --version    Display the version of this program and exit\n\n"
"Report bugs in findpageidinbz2xml to <https://phabricator.wikimedia.org/>.\n\n"
"See also dumpbz2filefromoffset(1), dumplastbz2block(1), findpageidinbz2xml(1),\n"
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
  fprintf(stderr,"findpageidinbz2xml %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

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

  bfile->bufin_size = BUFINSIZE;
  bfile->marker = init_marker();
  bfile->bytes_read = 0;
  bfile->bytes_written = 0;
  bfile->eof = 0;
  bfile->file_size = get_file_size(fin);

  bfile->initialized++;

  res = find_next_bz2_block_marker(fin, bfile, FORWARD);
  if (res ==1) {
    init_decompress(bfile);
    decompress_header(fin, bfile);
    setup_first_buffer_to_decompress(fin, bfile);
    return(0);
  }
  else {
    fprintf(stderr,"Failed to find the next block marker\n");
    return(-1);
  }
}

extern char * geturl(char *hostname, int port, char *url);

char *get_hostname_from_xml_header(int fin) {
  int res;
  regmatch_t *match_base_expr;
  regex_t compiled_base_expr;
  /*	 <base>http://el.wiktionary.org/wiki/...</base> */
  /*  <base>http://trouble.localdomain/wiki/ */
  char *base_expr = "<base>http://([^/]+)/"; 
  int length=5000; /* output buffer size */

  buf_info_t *b;
  bz_info_t bfile;

  int hostname_length = 0;

  off_t old_position, seek_result;
  static char hostname[256];

  bfile.initialized = 0;

  res = regcomp(&compiled_base_expr, base_expr, REG_EXTENDED);
  match_base_expr = (regmatch_t *)malloc(sizeof(regmatch_t)*2);

  b = init_buffer(length);
  bfile.bytes_read = 0;

  bfile.position = (off_t)0;
  old_position = lseek(fin,(off_t)0,SEEK_CUR);
  seek_result = lseek(fin,(off_t)0,SEEK_SET);

  while ((get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD)>=0) && (! bfile.eof)) {
    /* so someday the header might grow enough that <base> isn't in the first 1000 characters but we'll ignore that for now */
    if (bfile.bytes_read && b->bytes_avail > 1000) {
      /* get project name and language name from the file header
	 format: 
	 <mediawiki xmlns="http://www.mediawiki.org/xml/export-0.5/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.5/ http://www.mediawiki.org/xml/export-0.5.xsd" version="0.5" xml:lang="el">
	 <siteinfo>
	 <sitename>Βικιλεξικό</sitename>
	 <base>http://el.wiktionary.org/wiki/...</base>
      */
      if (regexec(&compiled_base_expr, (char *)b->next_to_read,  2,  match_base_expr, 0 ) == 0) {
	if (match_base_expr[1].rm_so >=0) {
	  hostname_length = match_base_expr[1].rm_eo - match_base_expr[1].rm_so;
	  if (hostname_length > sizeof(hostname)) {
	    fprintf(stderr,"Very long hostname, giving up\n");
	    break;
	  }
	  else {
	    memcpy(hostname,(char *)b->next_to_read + match_base_expr[1].rm_so, hostname_length);
	    hostname[hostname_length] = '\0';
	    b->next_to_read = b->end;
	    b->bytes_avail = 0;
	    b->next_to_fill = b->buffer; /* empty */
	    bfile.strm.next_out = (char *)b->next_to_fill;
	    bfile.strm.avail_out = b->end - b->next_to_fill;
	    res = BZ2_bzDecompressEnd ( &(bfile.strm) );
	    seek_result = lseek(fin,old_position,SEEK_SET);
	    free_buffer(b);
	    return(hostname);
	  }
	}
      }
      else {
	break;
      }
    }
  }
  res = BZ2_bzDecompressEnd ( &(bfile.strm) );
  seek_result = lseek(fin,old_position,SEEK_SET);
  free_buffer(b);
  return(NULL);
}

int has_xml_tag(char *line, char *tag) {
  return(! strncmp(line,tag,strlen(tag)));
}

/* assumes the open tag, close tag and avlaue are all on the same line */
long int get_xml_elt_value(char *line, char *tag) {
  return(atol(line+strlen(tag)));
}

/* returns pageid, or -1 on error. this requires the name of a stub file
   which contains all page ids and revisions ids in our standard xml format.
   It scans through the entire file looking for the page id which corresponds
   to the revision id.  This can take up to 5 minutes for the larger
   stub history files; clearly we don't want to do this unless we
   have no other option. 
   we need this in the case where the page text is huge (eg en wp pageid 5137507
   which has a cumulative text length across all revisions of > 163 GB. 
   This can take over two hours to uncompress and scan through looking for
   the next page id, so we cheat */
long int get_page_id_from_rev_id_via_stub(long int rev_id, char *stubfile) {
  gzFile *gz;
  int page_id = -1;
  char buf[8192];
  char *bufp;
  enum States{WantPage,WantPageID,WantRevOrPage,WantRevID};
  int state;
  long int temp_rev_id;

  gz = gzopen(stubfile,"r");
  state = WantPage;
  while ((bufp = gzgets(gz,buf,8191)) != NULL) {
    while (*bufp == ' ') bufp++;
    if (state == WantPage) {
      if (has_xml_tag(bufp,"<page>")) {
	state = WantPageID;
      }
    }
    else if (state == WantPageID) {
      if (has_xml_tag(bufp,"<id>")) {
	page_id = get_xml_elt_value(bufp,"<id>");
	state = WantRevOrPage;
      }
    }
    else if (state == WantRevOrPage) {
      if (has_xml_tag(bufp,"<revision>")) {
	state = WantRevID;
      }
      else if (has_xml_tag(bufp,"<page>")) {
	state = WantPageID;
      }
    }
    else if (state == WantRevID) {
      if (has_xml_tag(bufp,"<id>")) {
	temp_rev_id = get_xml_elt_value(bufp,"<id>");
	if (temp_rev_id == rev_id) {
	  return(page_id);
	}
	/* this permits multiple revs in the page */
	state = WantRevOrPage;
      }
    }
  }
  return(-1);
}

/* returns pageid, or -1 on error. this requires network access,
 it does an api call to the appropriate server for the appropriate project 
 we need this in the case where the page text is huge (eg en wp pageid 5137507
 which has a cumulative text length across all revisions of > 163 GB. 
 This can take over two hours to uncompress and scan through looking for
 the next page id, so we cheat */
int get_page_id_from_rev_id_via_api(long int rev_id, int fin) {
  /* char hostname[80]; */
  char *hostname;
  char url[80];
  char *buffer;
  long int page_id = -1;
  char *api_call = "/w/api.php?action=query&format=xml&revids=";
  regmatch_t *match_page_id_expr;
  regex_t compiled_page_id_expr;
  char *page_id_expr = "<pages><page pageid=\"([0-9]+)\""; 
  int res;

  hostname = get_hostname_from_xml_header(fin);
  if (!hostname) {
    return(-1);
  }

  /*
  if (strlen(lang) + strlen(project) + strlen(".org") > sizeof(hostname)-2) {
    fprintf(stderr,"language code plus project name is huuuge string, giving up\n");
    return(-1);
  }
  sprintf(hostname,"%s.%s.org",lang,project);
  */
  sprintf(url,"%s%ld",api_call,rev_id);

  buffer = geturl(hostname, 80, url);
  if (buffer == NULL) {
    return(-1);
  }
  else {
    /* dig the page id out of the buffer 
       format: 
       <?xml version="1.0"?><api><query><pages><page pageid="6215" ns="0" title="hystérique" /></pages></query></api>
    */
    match_page_id_expr = (regmatch_t *)malloc(sizeof(regmatch_t)*3);
    res = regcomp(&compiled_page_id_expr, page_id_expr, REG_EXTENDED);

    if (regexec(&compiled_page_id_expr, buffer,  3,  match_page_id_expr, 0 ) == 0) {
      if (match_page_id_expr[2].rm_so >=0) {
	page_id = atol(buffer + match_page_id_expr[2].rm_so);
      }
    }
    return(page_id);
  }
}

/* 
   get the first page id after position in file 
   if a pageid is found, the structure pinfo will be updated accordingly
   use_api nonzero means that we will fallback to ask the api about a page
   that contains a given rev_id, in case we wind up with a huge page which
   has piles of revisions and we aren't seeing a page tag in a reasonable
   period of time.
   returns:
      1 if a pageid found,
      0 if no pageid found,
      -1 on error
*/
int get_first_page_id_after_offset(int fin, off_t position, page_info_t *pinfo, int use_api, int use_stub, char *stubfilename, int verbose) {
  int res;
  regmatch_t *match_page, *match_page_id, *match_rev, *match_rev_id;
  regex_t compiled_page, compiled_page_id, compiled_rev, compiled_rev_id;
  int length=5000; /* output buffer size */
  char *page = "<page>";
  char *page_id = "<page>\n[ ]+<title>[^<]+</title>\n([ ]+<ns>[0-9]+</ns>\n)?[ ]+<id>([0-9]+)</id>\n"; 
  char *rev = "<revision>";
  char *rev_id_expr = "<revision>\n[ ]+<id>([0-9]+)</id>\n";

  buf_info_t *b;
  bz_info_t bfile;
  long int rev_id=0;
  long int page_id_found=0;

  int buffer_count = 0;

  bfile.initialized = 0;

  res = regcomp(&compiled_page, page, REG_EXTENDED);
  res = regcomp(&compiled_page_id, page_id, REG_EXTENDED);
  res = regcomp(&compiled_rev, rev, REG_EXTENDED);
  res = regcomp(&compiled_rev_id, rev_id_expr, REG_EXTENDED);

  match_page = (regmatch_t *)malloc(sizeof(regmatch_t)*1);
  match_page_id = (regmatch_t *)malloc(sizeof(regmatch_t)*3);
  match_rev = (regmatch_t *)malloc(sizeof(regmatch_t)*1);
  match_rev_id = (regmatch_t *)malloc(sizeof(regmatch_t)*2);

  b = init_buffer(length);

  pinfo->bits_shifted = -1;
  pinfo->position = (off_t)-1;
  pinfo->page_id = -1;

  bfile.bytes_read = 0;

  if (find_first_bz2_block_from_offset(&bfile, fin, position, FORWARD) <= (off_t)0) {
    if (verbose) fprintf(stderr,"failed to find block in bz2file after offset %"PRId64" (1)\n", position);
    return(-1);
  }

  if (verbose) fprintf(stderr,"found first block in bz2file after offset %"PRId64"\n", position);

  while (!get_buffer_of_uncompressed_data(b, fin, &bfile, FORWARD) && (! bfile.eof)) {
    buffer_count++;
    if (verbose >=2) fprintf(stderr,"buffers read: %d\n", buffer_count);
    if (bfile.bytes_written) {
      while (regexec(&compiled_page_id, (char *)b->next_to_read,  3,  match_page_id, 0 ) == 0) {
	if (match_page_id[2].rm_so >=0) {
	  if (verbose){
	    fwrite(b->next_to_read+match_page_id[2].rm_so, sizeof(unsigned char), match_page_id[2].rm_eo - match_page_id[2].rm_so, stderr);
	    fwrite("\n",1,1,stderr);
	  }
	  pinfo->page_id = atoi((char *)(b->next_to_read+match_page_id[2].rm_so));
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

      if (use_api || use_stub) {
	if (!rev_id) {
	  if (regexec(&compiled_rev_id, (char *)b->next_to_read,  2,  match_rev_id, 0 ) == 0) {
	    if (match_rev_id[1].rm_so >=0) {
	      rev_id = atoi((char *)(b->next_to_read+match_rev_id[1].rm_so));
	    }
	  }
	}

	/* this needs to be called if we don't find a page by X tries, or Y buffers read, 
	   and we need to retrieve a page id from a revision id in the text instead 
	   where does this obscure figure come from? assume we get at least 2-1 compression ratio,
	   text revs are at most 10mb plus a little, then if we read this many buffers we should have
	   at least one rev id in there.  20 million / 5000 or whatever it is, is 4000 buffers full of crap
	   hopefully that doesn't take forever. 
	*/
	if (buffer_count>(20000000/BUFINSIZE) && rev_id) {
	  if (verbose) fprintf(stderr, "passed retries cutoff for using api\n");
	  if (use_api) {
	    page_id_found = get_page_id_from_rev_id_via_api(rev_id, fin);
	  }
	  else { /* use_stub */
	    page_id_found = get_page_id_from_rev_id_via_stub(rev_id, stubfilename);
	  }
	  pinfo->page_id = page_id_found +1; /* want the page after this offset, not the one we're in */
	  pinfo->position = bfile.block_start;
	  pinfo->bits_shifted = bfile.bits_shifted;
	  return(1);
	}
      }
      /* FIXME this is probably wrong */

      if (regexec(&compiled_page, (char *)b->next_to_read,  1,  match_page, 0 ) == 0) {
	/* write everything up to but not including the page tag to stdout */
	/*
	fwrite(b->next_to_read,match_page[0].rm_eo - 6,1,stdout);
	*/
	move_bytes_to_buffer_start(b, b->next_to_read + match_page[0].rm_so, b->bytes_avail - match_page[0].rm_so);
	bfile.strm.next_out = (char *)b->next_to_fill;
	bfile.strm.avail_out = b->end - b->next_to_fill;
      }
      else if ((use_api || use_stub) && (regexec(&compiled_rev, (char *)b->next_to_read,  1,  match_rev, 0 ) == 0)) {
	/* write everything up to but not including the rev tag to stdout */
	/*
	fwrite(b->next_to_read,match_page[0].rm_eo - 6,1,stdout);
	*/
	move_bytes_to_buffer_start(b, b->next_to_read + match_rev[0].rm_so, b->bytes_avail - match_rev[0].rm_so);
	bfile.strm.next_out = (char *)b->next_to_fill;
	bfile.strm.avail_out = b->end - b->next_to_fill;
      }
      else {
	/* could have the first part of the page or the rev tag... so copy up enough bytes to cover that case */
	if (b->bytes_avail> 10) {
	  /* write everything that didn't match, but leave 10 bytes, to stdout */
	  /*
	  fwrite(b->next_to_read,b->bytes_avail - 10,1,stdout);
	  */
	  move_bytes_to_buffer_start(b, b->next_to_read + b->bytes_avail - 10, 10);
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
	    /* there were only 10 or less bytes so just save em don't write em to stdout */
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
int do_iteration(iter_info_t *iinfo, int fin, page_info_t *pinfo, int use_api, int use_stub, char *stubfilename, int verbose) {
  int res;
  off_t new_position;
  off_t interval;

  /* 
     last_position is somewhere in the interval, perhaps at an end 
     last_value is the value we had at that position
  */
  
  interval = (iinfo->right_end - iinfo->left_end)/(off_t)2;
  if (interval == (off_t)0) {
    interval = (off_t)1;
  }
  if (verbose) 
    fprintf(stderr,"interval size is %"PRId64", left end %"PRId64", right end %"PRId64", last val %d\n",interval, iinfo->left_end, iinfo->right_end, iinfo->last_value);
  /* if we're this close, we'll check this value and be done with it */
  if (iinfo->right_end -iinfo->left_end < (off_t)2) {
    new_position = iinfo->left_end;
    if (verbose >= 2) fprintf(stderr," choosing new position (1) %"PRId64"\n",new_position);
    iinfo->right_end = iinfo->left_end;
  }
  else {
    if (iinfo->last_value < iinfo->value_wanted) {
      if (verbose >= 2) fprintf(stderr,"resetting left end\n");
      iinfo->left_end = iinfo->last_position;
      new_position = iinfo->last_position + interval;
      if (verbose >= 2) fprintf(stderr," choosing new position (2) %"PRId64"\n",new_position);
    }
    /* iinfo->last_value > iinfo->value_wanted */
    else {
      if (verbose >=2) fprintf(stderr,"resetting right end\n");
      iinfo->right_end = iinfo->last_position;
      new_position = iinfo->last_position - interval;
      if (new_position < 0) new_position = 0;
      if (verbose >= 2) fprintf(stderr," choosing new position (3) %"PRId64"\n",new_position);
    }
  }
  res = get_first_page_id_after_offset(fin, new_position, pinfo, use_api, use_stub, stubfilename, verbose);
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
      if (verbose) fprintf(stderr,"something very broken, giving up\n");
      return(-1);
    }
  }
}

int main(int argc, char **argv) {
  int fin, res, page_id=0;
  off_t position, interval, file_size;
  page_info_t pinfo;
  iter_info_t iinfo;
  char *filename = NULL;
  int optindex=0;
  int use_api = 0;
  int use_stub = 0;
  int verbose = 0;
  int optc;
  char *stubfile=NULL;

  struct option optvalues[] = {
    {"filename", 1, 0, 'f'},
    {"help", 0, 0, 'h'},
    {"pageid", 1, 0, 'p'},
    {"useapi", 0, 0, 'a'},
    {"stubfile", 1, 0, 's'},
    {"verbose", 0, 0, 'v'},
    {"version", 0, 0, 'V'},
    {NULL, 0, NULL, 0}
  };

  while (1) {
    optc=getopt_long_only(argc,argv,"f:hp:as:vV", optvalues, &optindex);
    if (optc=='f') {
     filename=optarg;
    }
    else if (optc=='p') {
      if (!(isdigit(optarg[0]))) usage(NULL);
      page_id=atoi(optarg);
    }
    else if (optc=='a')
      use_api=1;
    else if (optc=='s') {
      use_stub=1;
      stubfile = optarg;
    }
    else if (optc=='h')
      usage(NULL);
    else if (optc=='v')
      verbose++;
    else if (optc=='V')
      show_version(VERSION);
    else if (optc==-1) break;
    else usage("Unknown option or other error\n");
  }

  if (! filename || ! page_id) {
    usage(NULL);
  }

  if (page_id <1) {
    usage("Please specify a page_id >= 1.\n");
  }

  fin = open (filename, O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"Failed to open file %s for read\n", filename);
    exit(1);
  }

  file_size = get_file_size(fin);

  interval = file_size;
  position = (off_t)0;
  pinfo.bits_shifted = -1;
  pinfo.position = (off_t)-1;
  pinfo.page_id = -1;

  iinfo.left_end = (off_t)0;
  iinfo.right_end = file_size;
  iinfo.value_wanted = page_id;

  res = get_first_page_id_after_offset(fin, (off_t)0, &pinfo, use_api, use_stub, stubfile, verbose);
  if (res > 0) {
    iinfo.last_value = pinfo.page_id;
    iinfo.last_position = (off_t)0;
  }
  else {
    fprintf(stderr,"Failed to find any page from start of file, exiting\n");
    exit(1);
  }
  if (pinfo.page_id == page_id) {
    if (verbose) fprintf(stderr,"found the page id right away, no iterations needed.\n");
    fprintf(stdout,"position:%"PRId64" page_id:%d\n",pinfo.position, pinfo.page_id);
    exit(0);
  }
  if (pinfo.page_id > page_id) {
    fprintf(stderr,"Page requested is less than first page id in file\n");
    exit(-1);
  }
  while (1) {
    res = do_iteration(&iinfo, fin, &pinfo, use_api, use_stub, stubfile, verbose);
    if (res < 0) {
      fprintf(stderr,"Error encountered during search\n");
      exit(-1);
    }
    else if (iinfo.left_end == iinfo.right_end) {
      if ( pinfo.page_id <= page_id) {
	fprintf(stdout,"position:%"PRId64" page_id:%d\n",pinfo.position, pinfo.page_id);
	exit(0);
      }
      else {
	fprintf(stderr,"File does not contain requested page id\n");
	exit(-1);
      }
    }
  }
  exit(0);
}
