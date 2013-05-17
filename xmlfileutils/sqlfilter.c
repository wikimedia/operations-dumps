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

#include "uthash.h"
#include "mwxml2sql.h"

/* fill the file content buffer if it's empty */
#define RELOAD(inf, result) \
  if (get_line(inf) == NULL) result = NULL; \
  else result = inf->in_buf->content;

/* skip over whitespace in the content buffer,
   refilling it as needed */
#define SKIP_WS(f,s)		\
  if (!s) { RELOAD(f,s); }	\
  if (s) {			\
    while (!*s || *s == ' ') {	\
      while (*s == ' ') s++;	\
      if (!*s) {		\
	RELOAD(f,s);		\
	if (!s) break;		\
      }				\
    }				\
  }

/* globals for use in displaying
   error messages, progress messages etc */
int lines_done = 0;  /* number of lines processed (if a tuple is split across lines this count may be misleading) */
int tuples_done = 0; /* number of sql tuples (rows) processed */

#define HASHINT 1
#define HASHSTR 2

/* holds string filter values */
typedef struct filter_hash_int {
  int value;
  UT_hash_handle hh;
} filter_hash_int_t;

/* holds int filter values */
typedef struct filter_hash_str {
  char *value;
  UT_hash_handle hh;
} filter_hash_str_t;

/* hash of filter per column, so that for each tuple
   to be filtered, we can check each column to see
   if if has a filter hash that needs checked */
typedef struct filter_hoh {
  int colnum;
  int hashtype;
  filter_hash_int_t *fint;
  filter_hash_str_t *fstr;
  UT_hash_handle hh;
} filter_hoh_t;

/* holds fields from an sql tuple (row) */
typedef struct tuple_fields {
  string_t **f;
  int count;
  int used;
} tuple_fields_t;

/* hash of all filter int or string value hashes */
filter_hoh_t *fhoh = NULL;

/*
   args:
     message  -- error message to display, possibly a printf-style format string
     optionally arguments to use with the format string

   this function displays a message in stderr, with extra args if desired,
   with the number of lines and tuples from the sql input file shown
   at the beginning, for easier tracking down of the issue
*/
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

/* 
   args:
     start         -- pointer to start of string to append to existing field
     end           -- pointer to byte after end of string to append
     append_to_me  -- string_t to which bytes will be appended

   this function appends a string to the end of content in a
   string_t

   if append_to_me->content is null it will be allocated
   otherwise it will be realloced to fit the extra data
   plus some spare, so there are fewer realloc calls

   if end is NULL we copy the whole string from start til
   end of string, otherwise we copy from start up to but not
   including end
*/
void field_append(char *start, char *end, string_t *append_to_me) {
  int len_new_string = 0;
  int len_extended = 0;
  int len_realloc = 0;

  if (!append_to_me->length) { /* initial alloc */
    if (!end) {
      append_to_me->content = strdup(start);
      if (!append_to_me->content) {
	show_error("failed to get memory for appending to field\n");
	exit(-1);
      }
      append_to_me->length = strlen(start)+1;
    }
    else {
      append_to_me->content = strndup(start, end-start);
      if (!append_to_me->content) {
	show_error("failed to get memory for appending to field\n");
	exit(-1);
      }
      append_to_me->length = end-start+1;
    }
  }
  else { /* realloc */
    if (!end) len_new_string = strlen(start);
    else len_new_string = end - start;
    len_extended = strlen(append_to_me->content) + len_new_string + 1;
    len_realloc = len_extended + 1024;
    if (len_extended > append_to_me->length) {
      append_to_me->content = (char *)realloc(append_to_me->content, len_realloc);
      if (!append_to_me->content) {
	show_error("failed to get memory for appending to field\n");
	exit(-1);
      }
      append_to_me->length = len_realloc;
    }
    if (!end) {
      strcpy(append_to_me->content + strlen(append_to_me->content), start); /* copies trailing null */
    }
    else {
      strncpy(append_to_me->content + strlen(append_to_me->content), start, end - start);
      append_to_me->content[len_extended-1] = '\0'; /* and there's the null */
    }
  }
  return;
}

/*
  args:
    fields   -- pointer to array of string_t pointers

  this function makes sure there is an unused allocated field in the
  array; if none is available one will be allocated.
  if one is available, its content will be cleared.

  on error the function displays an error message on stderr and
  exits with a nonzero exit code.
 */
void expand_fields(tuple_fields_t *fields) {
  if ( !fields->count || fields->count == fields->used) {
    fields->f = (string_t **)realloc(fields->f, (fields->count + 1)*sizeof(string_t *));
    if (!fields->f) {
      show_error("failed to get memory for fields in tuple\n");
      exit(-1);
    }
    fields->f[fields->count] = (string_t *)malloc(sizeof(string_t));
    if (!fields->f[fields->count]) {
      show_error("failed to get memory for fields in tuple\n");
      exit(-1);
    }
    fields->f[fields->count]->content = NULL;
    fields->f[fields->count]->length = 0;
    fields->count++;
  }
  else { /* there's space in there, just leave it as empty string */
    fields->f[fields->used]->content[0] = '\0';
  }
  return;
}

/*
  args:
    sql      -- name of sql table dump file
    out      -- name of output file
    start    -- pointer to next content to be parsed
    fields   -- pointer to array which holds fields that have been parsed
    verbose  -- display progress messages on stderr

  this function retrieves one field from the content to be parsed and
  stores it in the next slot in fields

  returns: pointer to char (not spaces) after field, if we don't run out of file
  (if we do it's an error), so we mean either , or )
  on error, returns NULL

  start should be at the beginning of a field, which means either a leading ' or the data.
  we read in data, stripping extra spaces off of non quoted data, up to comma or )
  if we run out of buffer content during processing we will reload it
*/
char *do_field(input_file_t *sql, output_file_t *out, char *start, tuple_fields_t *fields, int verbose) {
  int quoted = 0;
  char *ind = NULL;

  string_t *field = NULL;

  ind = start;

  if (*ind == '\'') {
    quoted++;
    start = ind;
    ind++;
  }

  /* set up the slot in fields if needed */
  expand_fields(fields);
  field = fields->f[fields->used++];

  while (1) {
    if (!*ind) {
      field_append(start, ind, field);
      RELOAD(sql,start);
      if (!start) {
	show_error("abrupt end to data after or in field %s\n", start);
	return(NULL);
      }
      ind = start;
    }
    if (quoted && *ind == '\'') {
      ind++;
      field_append(start, ind, field);
      SKIP_WS(sql,ind);
      if (!ind) {
	show_error("abrupt end to data after or in field %s\n", start);
	return(NULL);
      }
      start = ind;
      if (*ind != ',' && *ind != ')') {
	show_error("unexpected data encountered after quoted field: <%s>\n",ind);
	return(NULL);
      }
      break; /* end of field with quote */
    }
    else if (!quoted && (*ind == ' ')) {
      field_append(start, ind, field);
      SKIP_WS(sql,ind);
      if (!ind) {
	show_error("abrupt end to data after field %s\n", start);
	return(NULL);
      }
      start = ind;
      if (*ind != ',' || *ind != ')') {
	show_error("unexpected data encountered after unquoted field: <%s>\n",ind);
	return(NULL);
      }
      break; /* end of field with space */
    }
    else if (!quoted && (*ind == ',' || *ind == ')' )) {
      field_append(start, ind, field);
      break; /* end of field */
    }
    else {
      /* move ind along, skipping over escaped crap etc. */
      if (*ind == '\\') {
	ind++;
	if (!*ind) {
	  sql->leftover[0] = '\\';
	  sql->leftover[1] = '\0';
	  field_append(start, ind-1, field); /* don't copy in the leftover backslash */
	  RELOAD(sql,ind);
	  if (!ind) {
	    show_error("abrupt end to data after backslash in field %s\n", start);
	    return(NULL);
	  }
	  start = ind;
	}
	else ind++;
      }
      else ind++;
    }
  }
  return(ind);
}

/*
  this function allocates a holder of field content.

  it returns the pointer to the holder; on error
  it displays a message on stderr and exits with
  nonzero exit code.
*/
tuple_fields_t *alloc_fields() {
  tuple_fields_t *f;

  f = (tuple_fields_t *)malloc(sizeof(tuple_fields_t));
  if (f) {
    f->count = 0;
    f->used = 0;
    f->f = NULL;
  }
  if (!f) {
    show_error("failed to get memory for field in tuple\n");
    exit(-1);
  }
  return(f);
}


/*
  args:
    sql      -- name of sql table dump file
    out      -- name of output file
    start    -- pointer to next content to be parsed
    verbose  -- display progress messages on stderr
    fields   -- pointer to array which holds fields that have been parsed

  this function parses one tuple (sql row) from the provided content.

  it returns a pointer to the first nonblank character after the tuple (should be
  comma or newline), or NULL on error or if we run out of file in the middle
  of the tuple.

   we expect *start to be '(' (i.e. start of tuple)

   if we run out of buffer content during processing we will reload it

   the character after the ending ')' (there is no way we can fail to have a next
   character if we don't run out of file)
*/
char *do_tuple(input_file_t *sql, output_file_t *out, char *start, int verbose, tuple_fields_t *fields) {
  int eot = 0;

  if (*start == '(') {
    start++;
  }
  else {
    show_error("expected ( for beginning of tuple, got this: %s\n", start);
    return(NULL);
  }
  if (!*start) {
    RELOAD(sql,start);
    if (!start) {
      show_error("unexpected end of data after beginning of tuple\n");
      return(NULL);
    }
  }
  eot = 0; /* end of tuple */

  while (!eot) {
    SKIP_WS(sql,start);
    if (!start) {
      show_error("unexpected end of data after beginning of tuple\n");
      return(NULL);
    }

    start = do_field(sql, out, start, fields, verbose);
    if (!start) return(NULL); /* some processing error or out of data */

    if (*start == ')') {
      eot++;
      start++;
    }
    else if (*start == ',') {
      start++;
      SKIP_WS(sql,start);
      if (!start) {
	show_error("unexpected end of data after beginning of tuple\n");
	return(NULL);
      }
    }
    else {
      show_error("tuple has unexpected data: <%s>", start);
      return(NULL);
    }
  }
  SKIP_WS(sql,start);
  if (!start) {
    show_error("unexpected end of data after end of tuple\n");
    return(NULL);
  }
  return(start);
}

void fields_free(tuple_fields_t *fields) {
  int i = 0;

  if (fields) {
    for (i=0; i<fields->count; i++) {
      if (fields->f[i]->content) free(fields->f[i]->content);
      free(fields->f[i]);
    }
    free(fields);
  }
  return;
}

/*
  args:
    out       -- file to write to
    fields    -- fields for a tuple to be written
    col_mask  -- mask of which fields in the tuple to write
    raw       -- strip sql escaping and commas and parens, just write space sep fields

  this function writes a tuple of fields to the designated output file,
  either keeping the format as an sql insert or writing the raw fields only
*/
void write_fields(output_file_t *out, tuple_fields_t * fields, int col_mask, int raw) {
  int i=0;

  if (!raw)
    put_line(out,"(");
  for (i=0; i<fields->used; i++) {
    /* col_mask of zero means write all fields */
    if (!col_mask || (1<<(i+1) & col_mask)) {
      put_line(out,fields->f[i]->content);
      /* either col mask is all 1's, print them all (and then
	 we had best make sure that there is another field left
	 in the array to print), or 
	 it selects certain fields, (and then we must make sure there is
	 a field left to be selected after this one */
      if ((i+1 < fields->used) && (!col_mask || ((1 << (i+2)) <= col_mask)) ) {
	if (!raw) put_line(out, ","); 
	else put_line(out, " ");
      }
    }
  }
  if (!raw)
    put_line(out,")");
  else
    put_line(out,"\n");
  return;
}

/* 
   args:
      sql      initialized structure for input file with sql insert statements
      out      initialized structure for output file
      fields   where to put the fields that will be written out
      col_mask 31 bit mask corresponding to the field numbers the caller wants written from each tuple
      raw      whether or not to write just the raw values with space sep or write all the sql markup and parens
      verbose  if greater than 0, various progress messages will be written

   returns:
      -1  on error
      0 on success

   this function processes one line (one INSERT statement) of data
   a tuple may be split across multiple lines

   we expect insert statements to be one per line, not multiple on a line
   we expect the line to start with INSERT and possibly have leading
     blanks before that, and end in ';' with possible trailing whitespace
   any other statements are taken to be unrelated sql directives and are written out unaltered
*/
int do_line(input_file_t *sql, output_file_t *out, tuple_fields_t *fields, int col_mask, int raw,  int verbose) {
  char *start = NULL;
  int eoi = 0;

  int eol = 0;
  int line_started = 0;
  int wrote_tuple = 0;

  int key = 0;
  char *keystr = NULL;

  string_t header;

  int filtered = 0;

  filter_hoh_t *fhoh_entry = NULL;

  filter_hash_int_t *found_int;
  filter_hash_str_t *found_str;

  header.content = NULL;
  header.length = 0;

  int col_num = 0;

  while (!eol) {
    if (get_line(sql) == NULL) {
      /* if we are in the middle of processing a line and we ran out, this is an error */
      if (line_started) {
	show_error("unexpected end of file in the middle of a line\n");
	return(-1);
      }
      else {
	return(1); /* normal end */
      }
    }
    line_started = 1;

    if (strchr(sql->in_buf->content, '\n')) eol++;
    if (strncmp(sql->in_buf->content, "INSERT ", 6)) {
      if (!raw)
	put_line(out, sql->in_buf->content); /* not an insert line, pass it through */
      while (!eol) {
	if (get_line(sql) == NULL) {
	  show_error("unexpected end of file in the middle of a line\n");
	  return(-1);
	}
	if (strchr(sql->in_buf->content, '\n')) eol++;
	if (!raw)
	  put_line(out, sql->in_buf->content);
      }
      return(0);
    }

    start = strstr(sql->in_buf->content, " VALUES (");
    if (!start) {
      if (!raw)
	put_line(out, sql->in_buf->content); /* not an insert line, pass it through */
      while (!eol) {
	if (get_line(sql) == NULL) {
	  show_error("unexpected end of file in the middle of a line\n");
	  return(-1);
	}
	if (strchr(sql->in_buf->content, '\n')) eol++;
	if (!raw)
	  put_line(out, sql->in_buf->content);
      }
      return(0);
    }

    start += 8;

    field_append(sql->in_buf->content, start, &header);
    eoi = 0; /* end of insert? not yet */

    while (! eoi) {
      if (! *start) {
	if (get_line(sql) == NULL) {
	  show_error("unexpected end of file in the middle of a line\n");
	  return(-1);
	}
	if (strchr(sql->in_buf->content, '\n')) eol++;
      }

      fields->used = 0;
      start = do_tuple(sql, out, start, verbose, fields);
      if (!start) {
	return(-1);
      }
      tuples_done++;
      if (strchr(sql->in_buf->content, '\n')) eol++;

      if (*start == ';') {
	eoi++;
      }
      else if (*start == ',') {
	start++;
	SKIP_WS(sql,start);
	if (!start) {
	  show_error("unexpected end of file in the middle of a line\n");
	  return(-1);
	}
	/* find start of next tuple */
	while (*start != '(') {
	  if (!*start) {
	    RELOAD(sql,start);
	    if (!start) break;
	  }
	  else start++;
	}
	if (!start) {
	  show_error("unexpected end of file when looking for tuple in the middle of a line\n");
	  return(-1);
	}
      }
      else {
	show_error("unexpected content in middle of line: <%s>\n", start);
	return(-1);
      }
      filtered = 0;

      if (fhoh != NULL) {
	if (fields->used < HASH_COUNT(fhoh)) {
	  /*  we have a spec for a field num > number of actual fields in tuple */
	  fprintf(stderr,"number of fields in tuple (%d) less than column required for filter (%d), giving up\n", fields->used, HASH_COUNT(fhoh));
	  exit(1);
	}
	for (col_num=1; col_num<= fields->used; col_num++) {
	  fhoh_entry = NULL;
	  /* check each field */
	  HASH_FIND_INT(fhoh, &col_num, fhoh_entry);
	  if (fhoh_entry) {
	    if (fhoh_entry->hashtype == HASHINT) {
	      key = atoi(fields->f[col_num -1]->content);
	      found_int = NULL;
	      HASH_FIND_INT(fhoh_entry->fint, &key, found_int);
	      if (!found_int) {
		filtered = 1;
		break;
	      }
	    }
	    else if (fhoh_entry->hashtype == HASHSTR) {
	      keystr = fields->f[col_num -1]->content;
	      /* remove enclosing quotes */
	      if (keystr[strlen(keystr)-1] != '\'' && strcmp(keystr,"NULL")) {
		/* should never happen but you never know etc */
		fprintf(stderr,"missing close quote for field, skipping <%s> in column %d\n", keystr, col_num);
		filtered = 1;
		break;
	      }
	      else {
		keystr[strlen(keystr)-1] = '\0';
		found_str = NULL;
		HASH_FIND_STR(fhoh_entry->fstr, keystr+1, found_str);
		keystr[strlen(keystr)] = '\'';	
		if (!found_str) {
		  filtered = 1;
		  break;
		}
	      }
	    }
	    else {
	      fprintf(stderr,"Unknown hash key type requested, giving up\n");
	      exit(1);
	    }
	  }
	}
      }
      if (!filtered) {
	if (header.length) { /* first write of tuple from this line */
	  if (!raw)
	    put_line(out, header.content);
	  free(header.content);
	  header.content = NULL;
	  header.length = 0;
	}
	else {
	  if (!raw)
	    put_line(out,","); /* second or later write of tuple from this line */
	}
	write_fields(out, fields, col_mask, raw);
	fields->used = 0;
	wrote_tuple++;
      }
    }
    /* we are left either with:
       end of file, errors in processing some tuple - returned
       end of file, ok all tuples but no other data left
       end of line, errors in processing some tuple - returned
       end of line, ok all tuples but no other data left online
       not end of line, errors in processing some tuple - returned
       not end of line, what's left must be something else, this is what we care about
    */
    if (eoi) {
      if (wrote_tuple)
	if (!raw)
	  put_line(out,";\n");
    }
  }
  return(0);
}

/*
   this function prints version information for the program to stderr
*/
void show_version(char *whoami, char *version_string) {
  char * copyright =
"Copyright (C) 2013 Ariel T. Glenn.  All rights reserved.\n\n"
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
  fprintf(stderr,"sqlfilter %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

/*
   args:
     whoami    name of calling program
     message   message to print out before usage information, if any
               this should not end in a newline

   this function prints usage information for the program to stderr
*/
void usage(char *whoami, char *message) {
  char * help =
"Usage: sqlfilter [OPTION]...\n\n"
"Sqlfilter reads a possibly compressed stream of MySQL INSERT statements,\n"
"compares the contents of specified fields against lists of values and\n"
"writes only those tuples (rows) for which the field values are found in\n"
"the lists, to a possibly compressed output file. It can also write only\n"
"specified columns (fields) from each tuple. Note that specifying matches\n"
"for multple columns is an AND operation and that only exact match is\n"
"supported. This is intended to be a very simple filter, not an SQL\n"
"replacement.\n\n"
"Options:\n\n"
"  -c, --cols column-number[,column-number]...\n"
"        Comma-separated list specifying columns to write out\n"
"        (column-numbers starting with 1).  If this option is specified, \n"
"        tuples (rows) must contain fewer than 32 fields. Default: write \n"
"        out all columns.\n"
"  -f, --filterfile filename\n"
"        Name of file with column-number:value pairs against which rows\n"
"        will be filtered. File must have one column-number:value pair per\n"
"        line (column-numbers starting from 1).  Format: column-number:value\n"
"        where the value should consist of either a string of digits, or a\n"
"        string enclosed in single quotes and SQL-escaped. In particular\n"
"        any single quotes in the string must be escaped with a backslash.\n"
"        Default: no filter file (i.e. do not filter, unless --cols\n"
"        argument is provided).\n"
"  -h, --help\n"
"        Show summary of options; and exit.\n"
"  -o, --outputfile filename\n"
"        Name of file to which output will be written. If none is\n"
"        specified, data will be written to stdout. If a filename is\n"
"        specified that ends in .gz or .bz2, the file will be gzip or.\n"
"        bzip2 compressed.\n"
"  -r, --raw\n"
"        Write raw output without INSERT markup or parens, but with a \n"
"        newline after each tuple; and do not write any other SQL\n"
"        statements. Default: off (write all SQL markup)\n"
"  -s, --sqlfile filename\n"
"        Name of SQL file from which INSERT statements will be read. If\n"
"        none is specified, data will be read from stdin.  If a filename is\n"
"        specified that ends in .gz or .bz2, the file will silently be\n"
"        decompressed.\n"
"  -V, --value column-number:value\n"
"        Column-number:value pair against which rows will be filtered\n"
"        (overridden if --filterfile provided). To specify more than one\n"
"        such pair give this option more than once. Format:\n"
"        column-number:value, where the value should consist of either a\n"
"        string of digits, or a string enclosed with single quotes and\n"
"        SQL-escaped. In particular any single quotes in the string must be\n"
"        escaped with a backslash. Default: none (i.e. do not filter,\n"
"        unless --filterfile argument is provided).\n"
"  -v, --verbose\n"
"        Write progress information to stderr.\n"
"  -w, --version\n"
"        Write version information to stderr.\n\n"
"Report bugs in sqlfilter to <https://bugzilla.wikimedia.org/>.\n\n"
"See also mwxml2sql(1), sql2txt(1).\n\n";
  if (message) {
    fprintf(stderr,"%s\n\n",message);
  }
  fprintf(stderr,"%s",help);
  exit(-1);
}

/* 
   args:
     firstchar  -- first character of field from a tuple

   returns:
     hashtype, one of HASHINT or HASHSTR
     -1 on error

   this function determines the hash type of a field, for filtering purposes
   if the first char in the field is a quote, we assume string hash
   if it's a digit, we assume int
   if i'ts something else that's a big fail
*/
int get_hashtype(char firstchar) {
  if (isdigit(firstchar)) return(HASHINT);
  else if (firstchar == '\'') return(HASHSTR);
  else return(-1);
}

/*
  args:
    s      --  string (either single quoted text or a string of digits) to be added to hash
    colno  --  column number of field in tuples which will be checked against this value

   this function adds filter information to a hash
   the actual hashed entry will not include the single quotes, for non-int hashtype

  returns 0 on success, -1 on hashtype error, exits with nonzero exit code
  on other errors
*/
int add_to_hash(char *s, int colno) {
  filter_hash_int_t *fv_int = NULL;
  filter_hash_str_t *fv_str = NULL;
  filter_hoh_t *fhoh_entry = NULL;
  int hashtype = 0;

  HASH_FIND_INT(fhoh, &colno, fhoh_entry);
  if (fhoh_entry) {
   hashtype = fhoh_entry->hashtype;
  }
  else {
    hashtype = get_hashtype(*s);
    if (hashtype < 0) {
      fprintf(stderr,"bad value encountered in value filters: <%s>\n", s);
      return(-1);
    }
    fhoh_entry = (filter_hoh_t *)malloc(sizeof(filter_hoh_t));
    if (fhoh_entry == NULL) {
      fprintf(stderr,"failed to allocate memory for filter values holders\n");
      exit(1);
    }
    fhoh_entry->hashtype = hashtype;
    fhoh_entry->colnum = colno;
    fhoh_entry->fint = NULL;
    fhoh_entry->fstr = NULL;
    HASH_ADD_KEYPTR(hh, fhoh, &(fhoh_entry->colnum), sizeof(int), fhoh_entry); 
  }

  if (hashtype == HASHSTR) {
    if (isdigit(*s)) {
      fprintf(stderr,"non-quoted value encountered in string value filters: <%s>\n", s);
      return(-1);
    }
    /* here get rid of enclosing quotes */
    if (strlen(s) < 2 || s[strlen(s) -1] != '\'') {
      fprintf(stderr,"no ending quote for filter value <%s>\n",s);
    }
    s[strlen(s)-1] = '\0';
    fv_str = (filter_hash_str_t *)malloc(sizeof(filter_hash_str_t));
    if (fv_str == NULL) {
      fprintf(stderr,"failed to allocate memory for filter values\n");
      exit(1);
    }
    fv_str->value = strdup(++s);
    HASH_ADD_KEYPTR(hh, fhoh_entry->fstr, fv_str->value, strlen(fv_str->value),fv_str); 
  }
  else if (hashtype == HASHINT) {
    if (*s == '\'') {
      fprintf(stderr,"string value encountered in numeric value filters: <%s>\n", s);
      return(-1);
    }
    fv_int = (filter_hash_int_t *)malloc(sizeof(filter_hash_int_t));
    if (fv_int == NULL) {
      fprintf(stderr,"failed to allocate memory for filter values\n");
      exit(1);
    }
    fv_int->value = atoi(s);
    HASH_ADD_KEYPTR(hh, fhoh_entry->fint, &(fv_int->value), sizeof(int),fv_int); 
  }
  else {
    fprintf(stderr,"bad hashtype encountered in filters value holders: <%d>\n", hashtype);
    return(-1);
  }
  return(0);
}

/*
  args:
    field  -- pointer to start of field from tuple
  
  this function returns a pointer to the first
  byte after the field, skipping over backslash-escaped
  characters in quoted strings, if any.

  in all cases the next byte after the end of the field
  should be '\0'

  on error NULL is returned
*/
char *find_field_end(char *field) {
  char *end = NULL;

  if (isdigit(*field)) {
    end = field;
    while (*end) {
      if (!isdigit(*end)) {
	fprintf(stderr,"bad value for field, non-digits in numerical value <%s>\n", field);
	return(NULL);
      }
      end++;
    }
    return(end);
  }
  else if (*field == '\'') {
    end = field+1;
    while (*end  && *end != '\'') {
      if (*end == '\\') { /* deal with escaped characters */
	end++;
	if (*end) end++; /* otherwise the error will be caught by not field ending in quote */
      }
      else end++; /* regular character */
    }
    if (*end != '\'') {
      fprintf(stderr,"bad value for field, string field does not end in quote <%s>\n", field);
      return(NULL);
    }
    else if (*(end+1)) {
      fprintf(stderr,"bad value for field, trailing garbade after end of string field <%s>\n", field);
      return(NULL);
    }
    return(end+1); /* point to null after the end quote */
  }
  else {
    fprintf(stderr,"bad value for field, unquoted string at <%s>\n", field);
    return(NULL);
  }
}

/*
  args:
    file  -- name of file containing filter information

  this function reads filter information from a file and adds it to
  hashes for filtering later

  the file should contain one filter pair per line consisting of
  colnum:value  where colnum is the number of the column in each tuple
  (row) to check, and the value is either a string of digits or a single
  quoted string of characters.

  on error it exits with nonzero exit code
*/
void setup_hashes_from_file(char *file) {
  input_file_t *filter = NULL;
  char *temp = NULL, *line = NULL, *sep = NULL, *field = NULL;
  int colnum = 0;

  filter = init_input_file(file);
  if (!filter) exit(1);

  while (1) {
    if (get_line(filter) == NULL) break;
    /* strip trailing newline if any */
    if ((temp = strchr(filter->in_buf->content, '\n'))) *temp = '\0';

    /*
      line format: colnum:value\n
      where colnum is a string of digits, value is either a
      string of digits or enclosed in single quotes and sql-escaped

      we'll skip blank lines and allow lines starting with # to be comments
    */
    line = filter->in_buf->content;
    if (!line) break;
    if (*line == '#') continue;
    sep = strchr(line, ':');
    if (!sep) {
      fprintf(stderr,"bad format for filter values, should be colunm:value at <%s>\n", line);
      continue;
    }
    *sep = '\0';
    colnum = atoi(line);
    *sep = ':';
    field = sep+1;
    sep = find_field_end(field);
    if (!sep) continue; /* whining will have been done in the subroutine */
    if (*sep) continue; /* shouldn't happen but just in case */
    if (add_to_hash(field, colnum)) {
      close_input_file(filter);
      exit(1);
    }
  }
  close_input_file(filter);
  return;
}

/*
  args:
    value  -- string with filter information

  this function adds filter information to hashes for filtering later

  the filter value string should contain one filter pair consisting of
  colnum:value  where colnum is the number of the column in each tuple
  (row) to check, and the value is either a string of digits or a single
  quoted string of characters.

  on error it exits with nonzero exit code
*/
void setup_hashes_from_valstring(char *value) {
  char *sep = NULL, *field = NULL;
  int colnum = 0;

  sep = strchr(value, ':');
  if (!sep) {
    fprintf(stderr,"bad format for filter value, should be column:value at <%s>\n", value);
    exit(1);
  }
  *sep = '\0';
  colnum = atoi(value);
  *sep = ':';
  field = sep+1;
  sep = find_field_end(field);
  if (!sep || *sep) exit(1); /* whining will have been done already */
  if (add_to_hash(field, colnum)) {
    exit(1);
  }
  return;
}

int main(int argc, char **argv) {
  int optindex=0;
  int optc = 0;
  int result;
  int raw = 0;
  int help = 0;
  int verbose = 0;
  int version = 0;

  char *sql_file = NULL;  /* contains mysql insert commands */
  char *output_file = NULL; /* output */
  char *filter_file = NULL; /* values to filter first field against, if any */

  input_file_t *sql = NULL;
  output_file_t *out = NULL;

  char *filebase = NULL;
  char *filesuffix = NULL;

  tuple_fields_t *fields = NULL;

  char *cols = NULL;
  int col_mask = 0; /* write all fields */

  char *start = NULL, *end = NULL, *temp = NULL;

  char *value = NULL;

  struct option optvalues[] = {
    {"cols", required_argument, NULL, 'c'},
    {"sqlfile", required_argument, NULL, 's'},
    {"outputfile", required_argument, NULL, 'o'},
    {"filterfile", required_argument, NULL, 'f'},
    {"value", required_argument, NULL, 'V'},
    {"raw", no_argument, NULL, 'r'},
    {"help", no_argument, NULL, 'h'},
    {"verbose", no_argument, NULL, 'v'},
    {"version", no_argument, NULL, 'w'},
    {NULL, 0, NULL, 0}
  };

  while (1) {
    optc=getopt_long(argc,argv,"c:f:ho:rs:vV:w", optvalues, &optindex);
    if (optc==-1) break;

    switch(optc) {
    case 'c':
      cols = optarg;
      break;
    case 'o':
      output_file = optarg;
      break;
    case 's':
      sql_file = optarg;
      break;
    case 'f':
      filter_file = optarg;
      break;
    case 'V':
      value = optarg;
      setup_hashes_from_valstring(value);
      break;
    case 'r':
      raw++;
      break;
    case 'h':
      help++;
      break;
    case 'v':
      verbose++; 
      break;
    case 'w':
      version++;
      break;
    default:
      usage(argv[0],"unknown option or other error\n");
    }
  }

  if (help) usage(argv[0], NULL);
  if (version) {
    show_version(argv[0], VERSION);
    exit(1);
  }

  if (cols) {
    start = cols;
    col_mask = 0;
    while (*start) {
      if ((end = strchr(start, ','))){
	*end = '\0';
	temp = start;
	while (*temp) {
	  if (! isdigit(*temp))
	    usage(argv[0],"cols option must be a comma separated list of positive numbers");
	  temp++;
	}
	col_mask |= 1<<atoi(start);
	start = end + 1;
      }
      else {
	temp = start;
	while (*temp) {
	  if (! isdigit(*temp))
	    usage(argv[0],"cols option must be a comma separated list of positive numbers");
	  temp++;
	}
	col_mask |= 1<<atoi(start);
	break;
      }
    }
  }

  sql = init_input_file(sql_file);
  if (!sql) exit(1);

  if (!output_file)
    out = init_output_file(NULL, NULL, NULL);
  else {
    /* take apart the name if needed and shove in the prefix, then the suffix */

    filebase = get_filebase(output_file, verbose);
    filesuffix = get_filesuffix(output_file, verbose);
    out = init_output_file(filebase, filesuffix, NULL);
  }
  if (verbose) fprintf(stderr,"Input and output files opened\n");

  if (filter_file) {
    setup_hashes_from_file(filter_file);
    if (verbose) fprintf(stderr,"filter values read from file\n");
  }
  else if (value) {
    if (verbose) fprintf(stderr,"filter values parsed from argument(s)\n");
  }
  else {
    if (verbose) fprintf(stderr,"no filtering by value\n");
  }

  fields = alloc_fields();

  while (1) {
    result = do_line(sql, out, fields, col_mask, raw, verbose);
    if (result < 0) {
      show_error("error encountered scanning sql file");
      exit(1);
    }
    lines_done++;
    if (result > 0) {
      break;
    }
    if (verbose && !(lines_done%1000)) fprintf(stderr,"%d lines processed\n", lines_done);
  }

  if (verbose && (lines_done%1000)) fprintf(stderr,"%d lines processed\n", lines_done);

  fields_free(fields);

  close_input_file(sql);
  free_input_file(sql);

  close_output_file(out);
  free_output_file(out);

  exit(0);
}
