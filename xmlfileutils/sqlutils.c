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
/*
  args:
    f          file opened for output
    field      value to print out
    isstring   1 if string, 0 if numeric
    islast     1 if this is the last item in the row, 0 if not

  this function will print to the specied file a field with quotes if string,
  without if not, with a trailing comma if last in the row and without if not

  if the field value is null then NULL will be printed, this is a feature
  not a bug
 */
void print_sql_field(FILE *f, char *field, int isstring, int islast) {
  if (isstring) {
    if (!field) fprintf(f,"NULL");
    else fprintf(f,"'%s'",field);
  }
  else fprintf(f,"%s", field);
  if (!islast) fprintf(f, ", ");
  return;
}

/*
  args:
    outbuf     buffer in which to place the formatted value
    field      value to print out
    isstring   1 if string, 0 if numeric
    islast     1 if this is the last item in the row, 0 if not

  this function will copy to outbuf a field with quotes if string,
  without if not, with a trailing comma if last in the row and without if not

  if the field value is null then NULL will be printed, this is a feature
  not a bug
 */
/* should do length check but instead the caller gets to do that :-P */
void copy_sql_field(char *outbuf, char *field, int isstring, int islast) {
  if (isstring) {
    if (field) {
      if (!islast) {
	sprintf(outbuf,"'%s', ",field);
      }
      else {
	sprintf(outbuf,"'%s'",field);
      }
    }
    else {
      if (!islast) {
	strcpy(outbuf,"NULL, ");
      }
      else {
	strcpy(outbuf,"NULL");
      }
    }
  }
  else if (!islast) {
    sprintf(outbuf,"%s, ",field);
  }
  else {
    sprintf(outbuf,"%s",field);
  }
  return;
}

/*
  args:
    value    character string to convert, null terminated
    output   holder for the result; if this is null conversion is
             done in place
    last     whether this string is the last one in a sequence or not
             (needed so that we can handle cases where an xml-escaped
             character is split across two strings, with e.g. '&am'
             at the end of one and 'p;' at the beginning of the next)

  returns:
    if 'last' is 0, and the beginning of an xml-escaped character is
    encountered near the end of the string, a pointer to the start
    of that data will be returned and unescaping will stop there

    it is expected that the caller would then copy those bytes
    to the beginning of another string with any additional data
    before calling this function again on that data

    in all other cases NULL is returned (i.e. no data was left
    unprocessed)

  this function converts a character string that had been escaped
  for writing out XML, to a plain string
  it handles the following escape sequences:
  &lt; &gt; &quot; &amp; &#039; which become  < > " & ' respectively
*/
char *un_xml_escape(char *value, char*output, int last) {
  char *scan_ind;
  char *copy_ind;
  if (output) copy_ind = output;
  else copy_ind = value;

  scan_ind = value;
  while (scan_ind[0]) {
    if (scan_ind[0] == '&') {
      scan_ind++;
      if (!strncmp(scan_ind, "lt;", 3)) {
	copy_ind[0] = '<';
	copy_ind++;
	scan_ind += 3;
      }
      else if (!strncmp(scan_ind, "gt;", 3)) {
	copy_ind[0] = '>';
	copy_ind++;
	scan_ind += 3;
      }
      else if (!strncmp(scan_ind, "quot;", 5)) {
	copy_ind[0] = '"';
	copy_ind++;
	scan_ind += 5;
      }
      else if (!strncmp(scan_ind, "amp;", 4)) {
	copy_ind[0] = '&';
	copy_ind++;
	scan_ind += 4;
      }
      else if (!strncmp(scan_ind, "#039;", 5)) {
	copy_ind[0] = '\'';
	copy_ind++;
	scan_ind += 5;
      }
      else {
	/* if we're near the end of the string  and supposed to keep
	 the last few chrs, do so */
	if (!last && strlen(value) - (scan_ind - value) < 5) {
	  copy_ind[0] = '\0';
	  return scan_ind-1;
	}
	else {
	  copy_ind[0] = '&';
	  copy_ind++;
	  copy_ind[0] = scan_ind[0];
	  copy_ind++;
	  scan_ind++;
	}
      }
    }
    else {
      copy_ind[0] = scan_ind[0];
      copy_ind++;
      scan_ind++;
    }
  }
  copy_ind[0] = '\0';
  return(NULL);
}

/*
  args:
     s           string to escape
     s_size      length of string to escape
     out         holder for result
     out_size    size of holder for result

   returns:
      pointer to the next byte in s to be processed, or to NULL if all
      bytes were processed

   this function escapes character strings for input to mysql,
   adding a trailing '\0' to the result
   characters that are escaped are the below:
   \x00, \n, \r, \, ', " and \x1a

   if s_size is -1, the string to escape mus be null terminated
   and its length is not checked.
*/
char *sql_escape(char *s, int s_size, char *out, int out_size) {
  char c;
  char *from ;
  char *to;
  int copied = 0;
  int ind = 0;

  from = s;
  to = out;
  while ((!s_size && *from) || ind < s_size) {
    if (copied +3 > out_size) {
      /* null terminate here and return index */
      *to = '\0';
      return(from);
    }
    switch (*from) {
    case '\0':
      c = '0';
      break;
    case '\n':
      c = 'n';
      break;
    case '\r':
      c= 'r';
      break;
    case '\\':
      c= '\\';
      break;
    case '\'':
      c= '\'';
      break;
    case '"':
      c= '"';
      break;
    case '\032':
      c= 'Z';
      break;
    default:
      c = 0;
      *to = *from;
      to++;
      copied++;
      from++;
      ind++;
    }
    if (c) {
      *to = '\\';
      to++;
      copied++;
      *to = c;
      to++;
      copied++;
      from++;
      ind++;
    }
  }
  *to = '\0';
  return(NULL);
}

/*
  args:
     s           string to escape
     s_size      length of string to escape
     out         holder for result
     out_size    size of holder for result

   returns:
      pointer to the next byte in s to be processed, or to NULL if all
      bytes were processed

   this function escapes tabs in character strings for input to LOAD FILE
   adding a trailing '\0' to the result (you should pass a string that
   already has the remainder of the mysql escapes applied)

   if s_size is -1, the string to escape must be null terminated
   and its length is not checked.
*/
char *tab_escape(char *s, int s_size, char *out, int out_size) {
  char c;
  char *from ;
  char *to;
  int copied = 0;
  int ind = 0;

  from = s;
  to = out;

  while ((s_size == -1 && *from) || ind < s_size) {
    if (copied +3 > out_size) {
      /* null terminate here and return index */
      *to = '\0';
      return(from);
    }
    switch (*from) {
    case '\t':
      c = 't';
      break;
    default:
      c = 0;
      *to = *from;
      to++;
      copied++;
      from++;
      ind++;
    }
    if (c) {
      *to = '\\';
      to++;
      copied++;
      *to = c;
      to++;
      copied++;
      from++;
      ind++;
    }
  }
  *to = '\0';
  return(NULL);
}

/*
  args:
    t    null-terminated title string to be converted

  this function converts the supplied page title to its canonical
  form for storage in the page table (spaces to underscores)
*/
void title_escape(char *t) {
  while (*t) {
    if (*t == ' ') *t = '_';
    t++;
  }
  return;
}

/*
  args:
     buf    character string

  on return, the buffer will have all characters removed from it that
  are not digits 0-9

  this function is used for example to clean up page->touched, which we
  fill in from the rev timestamp, which looks like 2006-09-08T04:15:52Z
  whereas page->touched should be 20130118111419
 */
void digits_only(char *buf) {
  int copied = 0;
  int checked = 0;
  while (buf[checked]) {
    if (isdigit((unsigned int)buf[checked])) {
      buf[copied] = buf[checked];
      copied++;
    }
    checked++;
  }
  buf[copied] = '\0';
  return;
}

/* args:
      f        output file structure
      schema   MediaWiki XML export schema (eg '0.8')
      s        structure containing site info (namespaces and such)


      this function writes site metadata as contained in the supplied
      siteinfo structure to the specified output file, formatted as
      comments for MySQL
*/
void write_metadata(output_file_t *f, char *schema, siteinfo_t *s) {
  char out_buf[256];
  namespace_t *n = NULL;

  while (f) {
    snprintf(out_buf, sizeof(out_buf), "-- MediaWiki XML dump converted to SQL by mwxml2sql version %s\n", VERSION);
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "-- MediaWiki XML dump schema %s\n", schema);
    put_line(f, out_buf);
    strcpy(out_buf,"--\n");
    put_line(f, out_buf);
    if (s) {
      snprintf(out_buf, sizeof(out_buf), "-- Sitename: %s\n", s->sitename);
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "-- Base url: %s\n", s->base);
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "-- XML dump generated by: %s\n", s->generator);
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "-- Case sensitivity: %s\n", s->s_case);
      put_line(f, out_buf);
      strcpy(out_buf,"--\n");
      put_line(f, out_buf);
      if (s->namespaces) {
	n = s->namespaces;
	while (n) {
	  snprintf(out_buf, sizeof(out_buf), "-- Namespace %s: %s\n", n->key, n->namespace);
	  put_line(f, out_buf);
	  n = n->next;
	}
      }
    }
    f = f->next;
  }
  return;
}

/*
  args:
    f         structure for output file
    nodrop    do not write 'DROP TABLE...' statements (but do write 'INSERT IGNORE' statements)
    t         structure with the names of the tables

  this function writes to the specified output file the sql required to create the
  page, revision and text tables for the MediaWiki version specified

 */
void write_createtables_file(output_file_t *f, int nodrop, tablenames_t *t) {
  char out_buf[256];
  mw_version_t *mwv;

  while (f) {

    mwv = f->mwv;
    if (!nodrop) {
      snprintf(out_buf, sizeof(out_buf), "DROP TABLE IF EXISTS `%s`;\n", t->text);
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), "CREATE TABLE `%s` (\n", t->text);
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10))
      snprintf(out_buf, sizeof(out_buf), "`old_id` int(8) unsigned NOT NULL AUTO_INCREMENT,\n");
    else   if (MWV_GREATER(mwv, 1, 9))
      snprintf(out_buf, sizeof(out_buf), "`old_id` int unsigned NOT NULL AUTO_INCREMENT,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,9))
      snprintf(out_buf, sizeof(out_buf), "`old_text` mediumblob NOT NULL default '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`old_text` mediumblob NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,9))
      snprintf(out_buf, sizeof(out_buf), "`old_flags` tinyblob NOT NULL default '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`old_flags` tinyblob NOT NULL,\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "PRIMARY KEY (`old_id`)\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), ") ENGINE=InnoDB DEFAULT CHARSET=binary\n");
    put_line(f, out_buf);

    snprintf(out_buf, sizeof(out_buf), "\n");
    put_line(f, out_buf);
    
    if (!nodrop) {
      snprintf(out_buf, sizeof(out_buf), "DROP TABLE IF EXISTS `%s`;\n", t->page);
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), "CREATE TABLE `%s` (\n", t->page);
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,\n");
    else   if (MWV_GREATER(mwv, 1, 9))
      snprintf(out_buf, sizeof(out_buf), "`page_id` int unsigned NOT NULL AUTO_INCREMENT,\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "`page_namespace` int NOT NULL,\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "`page_title` varchar(255) binary NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,9))
      snprintf(out_buf, sizeof(out_buf), "`page_restrictions` tinyblob NOT NULL default '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_restrictions` tinyblob NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_counter` bigint(20) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv,1,15))
      snprintf(out_buf, sizeof(out_buf), "`page_counter` bigint unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_counter` bigint unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv,1,15))
      snprintf(out_buf, sizeof(out_buf), "`page_is_redirect` tinyint unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_is_redirect` tinyint unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_is_new` tinyint(1) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv, 1, 15))
      snprintf(out_buf, sizeof(out_buf), "`page_is_new` tinyint unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_is_new` tinyint unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "`page_random` real unsigned NOT NULL,\n");

    put_line(f, out_buf);

    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_touched` char(14) binary NOT NULL DEFAULT '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_touched` binary(14) NOT NULL DEFAULT '',\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_latest` int(8) unsigned NOT NULL,\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_latest` int unsigned NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv,1,10))
      snprintf(out_buf, sizeof(out_buf), "`page_len` int(8) unsigned NOT NULL,\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`page_len` int unsigned NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_GREATER(mwv, 1, 20)) {
      snprintf(out_buf, sizeof(out_buf), "`page_content_model` varbinary(32) DEFAULT NULL,\n");
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), "PRIMARY KEY (`page_id`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "UNIQUE KEY `name_title` (`page_namespace`,`page_title`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "KEY `page_random` (`page_random`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "KEY `page_len` (`page_len`),\n");
    put_line(f, out_buf);
  
    if (MWV_GREATER(mwv,1,18)) {
      snprintf(out_buf, sizeof(out_buf), "KEY `page_redirect_namespace_len` (`page_is_redirect`,`page_namespace`,`page_len`)\n");
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), ") ENGINE=InnoDB DEFAULT CHARSET=binary\n");
    put_line(f, out_buf);

    /* auto_increment how does it work when we insert a bunch of crap into a table with fixed values
       for those indexes? */

    snprintf(out_buf, sizeof(out_buf), "\n");
    put_line(f, out_buf);
    
    if (!nodrop) {
      snprintf(out_buf, sizeof(out_buf), "DROP TABLE IF EXISTS `%s`;\n", t->revs);
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), "CREATE TABLE `%s` (\n", t->revs);
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10)) {
      snprintf(out_buf, sizeof(out_buf), "`rev_id` int(8) unsigned NOT NULL AUTO_INCREMENT,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_page` int(8) unsigned NOT NULL,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_text_id` int(8) unsigned NOT NULL,\n");
      put_line(f, out_buf);
    }
    else {
      snprintf(out_buf, sizeof(out_buf), "`rev_id` int unsigned NOT NULL AUTO_INCREMENT,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_page` int unsigned NOT NULL,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_text_id` int unsigned NOT NULL,\n");
      put_line(f, out_buf);
    }
    if (MWV_LESS(mwv, 1, 9))
      snprintf(out_buf, sizeof(out_buf), "`rev_comment` tinyblob NOT NULL default '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`rev_comment` tinyblob NOT NULL,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10))
      snprintf(out_buf, sizeof(out_buf), "`rev_user` int(5) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv, 1, 15))
      snprintf(out_buf, sizeof(out_buf), "`rev_user` int unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`rev_user` int unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "`rev_user_text` varchar(255) binary NOT NULL DEFAULT '',\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10))
      snprintf(out_buf, sizeof(out_buf), "`rev_timestamp` char(14) binary NOT NULL DEFAULT '',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`rev_timestamp` binary(14) NOT NULL DEFAULT '',\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10))
      snprintf(out_buf, sizeof(out_buf), "`rev_minor_edit` tinyint(1) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv, 1, 15))
      snprintf(out_buf, sizeof(out_buf), "`rev_minor_edit` tinyint unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`rev_minor_edit` tinyint unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    if (MWV_LESS(mwv, 1, 10))
      snprintf(out_buf, sizeof(out_buf), "`rev_deleted` tinyint(1) unsigned NOT NULL DEFAULT '0',\n");
    else if (MWV_LESS(mwv, 1, 15))
      snprintf(out_buf, sizeof(out_buf), "`rev_deleted` tinyint unsigned NOT NULL DEFAULT '0',\n");
    else
      snprintf(out_buf, sizeof(out_buf), "`rev_deleted` tinyint unsigned NOT NULL DEFAULT 0,\n");
    put_line(f, out_buf);
    if (MWV_GREATER(mwv, 1, 9)) {
      snprintf(out_buf, sizeof(out_buf), "`rev_len` int unsigned DEFAULT NULL,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_parent_id` int unsigned DEFAULT NULL,\n");
      put_line(f, out_buf);
    }
    if (MWV_GREATER(mwv, 1, 18)) {
      snprintf(out_buf, sizeof(out_buf), "`rev_sha1` varbinary(32) NOT NULL DEFAULT '',\n");
      put_line(f, out_buf);
    }
    if (MWV_GREATER(mwv, 1, 20)) {
      snprintf(out_buf, sizeof(out_buf), "`rev_content_model` varbinary(32) DEFAULT NULL,\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "`rev_content_format` varbinary(64) DEFAULT NULL,\n");
      put_line(f, out_buf);
    }

    if (MWV_LESS(mwv, 1, 15)) {
      snprintf(out_buf, sizeof(out_buf), "PRIMARY KEY `rev_page_id` (`rev_page`,`rev_id`),\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "UNIQUE KEY (`rev_id`),\n");
      put_line(f, out_buf);
    }
    else {
      snprintf(out_buf, sizeof(out_buf), "PRIMARY KEY (`rev_id`),\n");
      put_line(f, out_buf);
      snprintf(out_buf, sizeof(out_buf), "UNIQUE KEY `rev_page_id` (`rev_page`,`rev_id`),\n");
      put_line(f, out_buf);
    }
    snprintf(out_buf, sizeof(out_buf), "KEY `rev_timestamp` (`rev_timestamp`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "KEY `page_timestamp` (`rev_page`,`rev_timestamp`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "KEY `user_timestamp` (`rev_user`,`rev_timestamp`),\n");
    put_line(f, out_buf);
    snprintf(out_buf, sizeof(out_buf), "KEY `usertext_timestamp` (`rev_user_text`,`rev_timestamp`),\n");
    put_line(f, out_buf);

    if (MWV_GREATER(mwv, 1, 19)) {
      snprintf(out_buf, sizeof(out_buf), "KEY `page_user_timestamp` (`rev_page`,`rev_user`,`rev_timestamp`)\n");
      put_line(f, out_buf);
    }

    snprintf(out_buf, sizeof(out_buf), ") ENGINE=InnoDB DEFAULT CHARSET=binary\n");
    put_line(f, out_buf);
    f = f->next;
  }
  return;
}

/*
  args:
    prefix    string used as prefix for all MediaWiki table names
              (folks often set up local mw installations where all
	      the tables have some prefix like mw_ or what have you
	      instead of just the regular names)

  returns:
    filled in structure containing names for page, revision and
    text tables, with prefix if one was provided, or without if
    prefix was NULL
*/
tablenames_t *setup_table_names(char *prefix) {
  tablenames_t *t = NULL;
 
  t = (tablenames_t *)malloc(sizeof(tablenames_t));
  if (!t) return(NULL);
  
  if (prefix) {
    if (strlen(prefix) > (80 - strlen("revision") - 1)) {
      fprintf(stderr,"Table prefix longer than 80 characters.  Seriously??\n");
      free(t);
      return(NULL);
    }
  }
  sprintf(t->page, "%s%s", prefix?prefix:"", "page");
  sprintf(t->revs, "%s%s", prefix?prefix:"", "revision");
  sprintf(t->text, "%s%s", prefix?prefix:"", "text");
  return(t);
}

