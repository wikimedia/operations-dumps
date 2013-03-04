#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include <ctype.h>
#include <inttypes.h>
#include <time.h>
#include <bzlib.h>
#include <zlib.h>
#include <stdarg.h>

#include "mwxml2sql.h"

int lines_done = 0;
int tuples_done = 0;
  
void show_error(char *message, ...) {
  va_list argptr;

  va_start(argptr,message);

  fprintf(stderr,"Error encountered: (%d:%d) ", lines_done, tuples_done);
  if (message)
    vfprintf(stderr,message, argptr);
  else
    fprintf(stderr,"unknown error");
  fprintf(stderr,"\n");

  va_end(argptr);

  return;
}

int write_field(output_file_t *f, char *start, char *end, int starting_quote, int ending_quote, int verbose) {
  char out_buf[TEXT_BUF_LEN*2 +7];
  char *ind;

  ind = out_buf;

  if (starting_quote) {
    *ind = '\'';
    ind++;
  }
  strncpy(ind, start, end-start);
  ind += end-start;
  if (ending_quote) {
    *ind = '\'';
    ind++;
  }
  *ind = '\0';
  if (verbose) fprintf(stderr,"put field: <%s>\n", out_buf);
  return(put_line(f, out_buf));
}

char *do_field(input_file_t *sql, output_file_t *text, char *start, int verbose) {
  /* should be at the beginning of a field. either a leading ' or the data.
     our job: read in data, til we get to ..
     - closing ' if we opened with one
     - , or ) if there was no open quote
     end of buffer first, in which case we write out what we have,
     saving a few chars in case of escapes I guess, move them in etc
     and refill buffer, then keep going with the above
     once we get to that or as we get to that we write what we have
     and put start at uh...the comma or the ) if there is one, else
     to NULL if we hit eof? blergh
  */
  int quoted = 0;
  char *ind = NULL;
  int first_write = 1;
  char load_data_escaped_buf[TEXT_BUF_LEN*2 + 6];
  int donulls = 1;

  while (*start == ' ') start++;

  if (*start == '\'') {
    quoted++;
    start+=1;
  }
  ind = start;
  while (1) {
    if (quoted && *ind == '\'') {
      load_data_escape(start, ind-start, load_data_escaped_buf, sizeof(load_data_escaped_buf), 0);
      write_field(text, load_data_escaped_buf, load_data_escaped_buf + strlen(load_data_escaped_buf), first_write&&quoted, 1&&quoted, verbose);
      start = ind+1;
      return(start);
    }
    else if (!quoted && (*ind == ',' || *ind == ')' )) {
      load_data_escape(start, ind-start, load_data_escaped_buf, sizeof(load_data_escaped_buf), donulls);
      write_field(text, load_data_escaped_buf, load_data_escaped_buf + strlen(load_data_escaped_buf), first_write&&quoted, 1&&quoted, verbose);
      first_write = 0;
      start = ind;
      return(start);
    }
    else if (!*ind) {
      load_data_escape(start, ind-start, load_data_escaped_buf, sizeof(load_data_escaped_buf), donulls);
      write_field(text, load_data_escaped_buf, load_data_escaped_buf + strlen(load_data_escaped_buf), first_write&&quoted, 0, verbose);
      first_write = 0;
      if (!get_line(sql)) {
	show_error("abrupt end to data after or in field %s\n", start);
	return(NULL);
      }
      start = sql->in_buf->content;
      ind = start;
    }
    else {
      /* move ind along, skipping over escaped crap etc. */
      if (*ind == '\\') {
	ind++;
	if (!*ind) {
	  sql->leftover[0] = '\\';
	  sql->leftover[1] = '\0';
	  load_data_escape(start, ind-start-1, load_data_escaped_buf, sizeof(load_data_escaped_buf), donulls);
	  write_field(text, load_data_escaped_buf, load_data_escaped_buf + strlen(load_data_escaped_buf), first_write&&quoted, 0, verbose);
	  first_write = 0;
	  if (!get_line(sql)) {
	    show_error("abrupt end to data after backslash in field %s\n", start);
	    return(NULL);
	  }
	  start = sql->in_buf->content;
	  ind = start;
	}
	else ind++;
      }
      else ind++;
    }
  }
  return(NULL);
}

/* we are at ) and we need to find ( */
char *find_next_tuple(input_file_t *sql, char *start, int verbose) {
  while (*start != '(') {
    if (!*start) return(NULL); /* end of full line */
    else start++;
  }
  return(start);
}

/* if we have a partial line we had better deal with it here, so
   that when we return to the caller an entire tuple has in fact been processed,
   with the next piece of the line preloaded into buffer
   expect *start to be '(' = start of tuple
*/
char *do_tuple(input_file_t *sql, output_file_t *text, char *start, int verbose) {
  int first = 1;
  char buf[2];

  buf[0] = '\t';
  buf[1] = '\0';
  while (*start == ' ') start++;

  if (*start == '(') start++;
  else {
    show_error("expected ( for beginning of tuple, got this: %s\n", start);
    return(NULL);
  }
  if (!*start) {
    if (get_line(sql) == NULL) return(NULL);
    start = sql->in_buf->content;
  }
  while (start && *start) {
    if (first) first = 0;
    else {
      put_line(text, buf);
    }
    start = do_field(sql, text, start, verbose);
    /* we should now be at either ')' or ',', we want to skip to:
       next ( if there is one, or .. .';' (which should indicate end of line,
       so expect that)
    */

    if (!start) {
      if (get_line(sql) == NULL) return(NULL);
      start = sql->in_buf->content;
    }

    while (*start == ' ') start++;

    /* if we ran out of data right after a tuple = (xx,yyy,...zzz) then refill the buffer
       if we run out in the middle of a field do_field will handle that case */
    if (!*start) {
      if (get_line(sql) == NULL) return(NULL);
      start = sql->in_buf->content;
    }
    if (*start == ')') {
      start = find_next_tuple(sql, start, verbose);
      return(start);
    }
    else if (*start == ',') {
      start++;
      if (!*start) { /* try to refill the buffer */
	if (get_line(sql) == NULL) return(NULL);
	start = sql->in_buf->content;
      }
    }
    else {
      show_error("tuple has unexpected data: <%s>", start);
      return(NULL);
    }
  }
  return NULL;
}

/* if we have a partial line we had better deal with it here, so
   that when we return to the caller an entire line has in fact been processed */
int do_line(input_file_t *sql, output_file_t *text, int verbose) {
  int skip = 0;
  char *start = NULL;
  char buf[2];

  if (verbose) fprintf(stderr,"processing line starting <%c%c%c>\n", sql->in_buf->content[0], sql->in_buf->content[1], sql->in_buf->content[2]);
  /* input may start with INSERT ... VALUES (
     or simply with with a leading (
     newline means end of tuple or tuples
     anything else doesn't have tuples so we ignore it
  */
  if (!strncmp(sql->in_buf->content, "INSERT ", 6)) {
    start = strstr(sql->in_buf->content, " VALUES (");
    if (!start) skip++;
    else start+=7;
  }
  else if (sql->in_buf->content[0] != '(') skip++;
  else start = sql->in_buf->content;

  if (skip) return(0); /* don't process this line, it doesn't have a data tuple */
  buf[0] = '\n';
  buf[1] = '\0';
  while (start) {
    start = do_tuple(sql, text, start, verbose);
    tuples_done++;
    put_line(text, buf);
  }

  /* fixme we should actually capture error returns from do_tuple and
     return with -1 here */
  return(0);
}

/*
   args:
     whoami    name of calling program
     message   message to print out before usage information, if any
               this should not end in a newline

   this function prints usage information for the program to stdout
*/
void usage(char *whoami, char *message) {
  if (message) {
    fprintf(stderr,"%s\n\n",message);
  }
  fprintf(stderr,"Usage: %s [--sqlfile filename] [--txtfile filename] [--verbose] [--help]\n", whoami);
  fprintf(stderr,"\n");
  fprintf(stderr,"Reads a possibly compressed stream of MySQL INSERT statements and converts\n");
  fprintf(stderr,"it to tab-separated output suitable for import via LOAD FILE\n");
  fprintf(stderr,"\n");
  fprintf(stderr,"Arguments:\n");
  fprintf(stderr,"\n");
  fprintf(stderr,"sqlfile   (s):   name of sqlfile from which to read INSERT statements; if none\n");
  fprintf(stderr,"                 is specified, data will be read from stdin.  If a filename is\n");
  fprintf(stderr,"                 specified that ends in .gz or .bz2, the file will silently be\n");
  fprintf(stderr,"                 decompressed.\n");
  fprintf(stderr,"txtfile   (t):   name of file to which to write output; if none is specified,\n");
  fprintf(stderr,"                 data will be written to stdout. If a filename is specified that\n");
  fprintf(stderr,"                 ends in .gz or .bz2, the file will be gz or bz2 compressed.\n");
  fprintf(stderr,"help      (h):   print this help message and exit\n");
  fprintf(stderr,"verbose   (v):   write progress information to stderr.\n");
  exit(-1);
}

int main(int argc, char **argv) {
  int optindex=0;
  int optc = 0;
  int result;

  int help = 0;
  int verbose = 0;

  char *sql_file = NULL;  /* contains mysql insert commands */
  char *text_file = NULL; /* output */

  input_file_t *sql = NULL;
  output_file_t *text = NULL;

  char *filebase = NULL;
  char *filesuffix = NULL;

  struct option optvalues[] = {
    {"sqlfile", required_argument, NULL, 'c'},
    {"textfile", required_argument, NULL, 'f'},
    {"help", no_argument, NULL, 'h'},
    {"verbose", no_argument, NULL, 'v'},
    {NULL, 0, NULL, 0}
  };

  while (1) {
    optc=getopt_long(argc,argv,"hs:t:v", optvalues, &optindex);
    if (optc==-1) break;

    switch(optc) {
    case 's':
      sql_file = optarg;
      break;
    case 't':
      text_file = optarg;
      break;
    case 'h':
      help++;
      break;
    case 'v':
      verbose++; 
      break;
    default:
      usage(argv[0],"unknown option or other error\n");
    }
  }

  if (help) usage(argv[0], NULL);

  sql = init_input_file(sql_file);
  if (!sql) exit(1);

  if (!text_file)
    text = init_output_file(NULL, NULL, NULL);
  else {
    /* take apart the name if needed and shove in the prefix, then the suffix */

    filebase = get_filebase(text_file, verbose);
    filesuffix = get_filesuffix(text_file, verbose);
    text = init_output_file(filebase, filesuffix, NULL);
  }

  if (verbose) fprintf(stderr,"Input and output files opened\n");

  while (1) {
    if (get_line(sql) == NULL) break;
    result = do_line(sql, text, verbose);
    if (result) {
      fprintf(stderr,"error encountered scanning sql file\n");
      exit(1);
    }
    lines_done++;
    if (verbose && !(lines_done%1000)) fprintf(stderr,"%d lines processed\n", lines_done);
  }

  if (verbose && (lines_done%1000)) fprintf(stderr,"%d lines processed\n", lines_done);

  close_input_file(sql);
  free_input_file(sql);

  close_output_file(text);
  free_output_file(text);

  exit(0);
}
