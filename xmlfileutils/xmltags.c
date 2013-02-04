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
     f            file structure with file content buffer to check
     holder       preallocated memory into which to copy the tag name if
                  found, otherwise the first byte will be set to '\0'
                  If this argument is null no copy will be made
     holder_size  length of the tag name holder, if the tag name
                  plus a terminating null is longer than this, only
                  holder_size bytes will be copied and the result
		  will not be null-terminated

   stuffs name of tag into holder if found
   this function expects the tag to be on one line, not split across lines,
   but the tag need not start the line.

   returns:
     index to character after tag name in line if specified tag found
     (that followig characer might be space before attributes, might
     be / of a simple tag, might be >)
     -1 otherwise
*/
int find_first_tag(input_file_t *f, char *holder, int holder_size) {
  
  /* either: <tagname> or <tagname a="b" ... > or <tagname /> */
  int ind=0;
  int start_tag_name;
  int length = 0;
  char *buf;

  if (holder) holder[0] = '\0';

  buf = f->in_buf->content;

  while (buf[ind] && buf[ind] != '<') ind++;
  if (!buf[ind]) return(-1);

  ind++;

  while (buf[ind] && buf[ind] == ' ')ind++;
  if (!buf[ind]) return(-1); /* '<' and maybe spaces and nothing else. reject that. */
  start_tag_name = ind;
  
  while (buf[ind] && buf[ind] != ' ' && buf[ind] != '>' && !(buf[ind] == '/' && buf[ind+1] == '>')) ind++;
  if (holder) {
    length = ind-start_tag_name;
    if (length > holder_size -1) length = holder_size;
    strncpy(holder, buf+start_tag_name, length);
    if (length < holder_size) {
      holder[length] = '\0';
      holder[holder_size -1] = '\0'; /* so caller can check right away if there's a length problem */
    }
  }
  return(ind);
}

/*
   args:
     f            file structure with file content buffer to check
     start        index into file content buffer where attribute string starts
                  (leading spaces are allowed)
     holder       preallocated memory into which to copy the attribute string
                  if found, otherwise the first byte will be set to '\0'
                  If this argument is null no copy will be made
     holder_size  length of the attribute string holder, if the attribute string
                  plus a terminating null is longer than this, only holder_size
		  bytes will be copied and the result will not be null-terminated

   this function expects the tag to be on one line, not split across lines,
   the function does not check that there's actual attributes and not some
   garbage string in there, eg it could be <tagname sdlksdlghldshg >, caller
   is responsible for digging out names and values

   returns:
     index to character after atttributes in file content buffer, relative
     to start index
     (could be space before close tag char, or / from string />, or >)
     -1 otherwise
*/
int find_attrs(input_file_t *f, int start, char *holder, int holder_size) {
  int ind=0;
  int start_attr;
  int length = 0;
  char *buf;

  buf = f->in_buf->content + start;
  if (holder) holder[0] = '\0';
  while (buf[ind] && buf[ind] == ' ') ind++;
  if (!buf[ind] || (buf[ind] == '/' && buf[ind+1] == '>') || buf[ind] == '>') return(-1);

  start_attr = ind;
  while (buf[ind] && !(buf[ind] == '/' && buf[ind+1] == '>') && buf[ind] != '>') ind++;

  /* backtrack from any trailing spaces */
  while (buf[ind-1] == ' ') ind--;

  if (holder) {
    length = ind-start_attr;
    if (length > holder_size) length = holder_size;
    strncpy(holder, buf+start_attr, length);
    if (length < holder_size) {
      holder[length] = '\0';
      holder[holder_size -1] = '\0'; /* so caller can check right away if there's a length problem */
    }
  }
  return(ind);
}

/*
   args:
     f            file structure with file content buffer to check
     start_ind    index into file content buffer with first character
                  to be processed
     holder       preallocated memory into which to copy the element value
                  if found, otherwise the first byte will be set to '\0'
                  If this argument is null no copy will be made
     holder_size  length of the elt value holder, if the value
                  plus a terminating null is longer than this, only
                  holder_size bytes will be copied and the result
		  will not be null-terminated

   this function expects the value to start the line; leading spaces
   will be considered part of the value, and content will be read from 
   the file input buffer until a '<' is encountered. If the buffer does not
   contain '<' then lines will be read from the input stream f and added
   to the input byffer until such a lime is found, the input buffer is
   full, or end of file is encountered.

   the file input buffer is not huge (only TEXT_BUF_LEN bytes) so
   don't use this function for ginormous text content. Reading past
   one line is done because sometimes a literal newline sneaks into
   a comment element for example.

   returns:
     index to character after value in line, relative to start_ind
     (that following characer should be '<' but could also be null
     if we reached the end of file abruptly)
     -1 otherwise

  if value is longer than holder_size, only holder_size bytes will be copied,
  otherwise the whole value will be copied and then null terminated.

  s_ind is ind into s->content first char to be processed
*/
int find_value(input_file_t *f, int start_ind, char *holder, int holder_size) {
  int ind=0;
  int start_value;
  char *buf;
  int length = 0;

  if (holder) holder[0] = '\0';
  buf = (f->in_buf->content) + start_ind;
  while (buf[ind] && buf[ind] == ' ') ind++;
  if (!buf[ind]) return(-1);

  if (buf[ind] == '>') ind+=1;
  else return(-1);

  start_value = ind;
  while (buf[ind] && buf[ind] != '<') ind++;

  while (start_ind + ind  < f->in_buf->length && !buf[ind]) {
    buf[ind]='\0';
    if (get_line2buffer(f, f->in_buf->content+start_ind+ind, f->in_buf->length-(start_ind + ind +1)) == NULL) return(-1);
    while (buf[ind] && buf[ind] != '<') ind++;
  }
  if (!buf[ind]) return(-1);

  if (holder) {
    length = ind-start_value;
    if (length > holder_size) length = holder_size;
    strncpy(holder, buf+start_value, length);
    if (length < holder_size) {
      holder[length] = '\0';
      holder[holder_size -1] = '\0'; /* so caller can check right away if there's a length problem */
    }
  }
  return(ind);
}

/*
  args:
     f            file structure with file content buffer to check
     start        index into file content buffer with first character
                  to be processed
     holder       preallocated memory into which to copy the
                  tag name if found, otherwise the first
                  byte will be set to '\0'
                  If this argument is null no copy will be made
     holder_size  length of the tag name holder, if the tag name
                  plus a terminating null is longer than this, only
                  holder_size bytes will be copied and the result
		  will not be null-terminated

   this function expects the file content buffer to contain an xml
   close tag for an element (</blah>) possibly with leading spaces,
   at the start of a line

   returns:
     index to character after close tag in line if specified tag found,
     relative to start index
     -1 otherwise
*/
int find_close_tag(input_file_t *f, int start, char *holder, int holder_size) {
  int ind=0;
  int start_tagname;
  int length = 0;
  char *buf;

  buf = f->in_buf->content + start;
  if (holder) holder[0] = '\0';
  while (buf[ind] && buf[ind] == ' ') ind++;
  if (!buf[ind] || !(buf[ind] == '<' && buf[ind+1] == '/')) return(-1);
  ind+=2;
  while (buf[ind] && buf[ind] == ' ') ind++;
  if (!buf[ind]) return (-1);
  start_tagname = ind;
  while (buf[ind] && buf[ind] != '>') ind++;
  if (!buf[ind]) return (-1);

  if (holder) {
    length = ind-start_tagname;
    if (length > holder_size -1) length = holder_size;
    strncpy(holder, buf+start_tagname, length);
    if (length < holder_size) {
      holder[length] = '\0';
      holder[holder_size -1] = '\0'; /* so caller can check right away if there's a length problem */
    }
  }
  return(ind);
}

/*
   args:
     buf:         null-terminated buffer string of charcters expected to
                  contain the string '/>' which ends a closing xml tag
     start:       indexinto buffer to the clsing string '/>' (leading
                  spaces are ok)

   returns:
     index to character after the close tag string if found
     -1 otherwise
*/
int find_simple_close_tag(input_file_t *f, int start) {
  int ind=0;
  char *buf;

  buf = f->in_buf->content + start;

  while (buf[ind] && buf[ind] == ' ') ind++;
  if (!buf[ind] || !(buf[ind] == '/' && buf[ind+1] == '>')) return(-1);
  ind+=2;
  return(ind);
}

/* 
   args:
     f         file structure with file content buffer to check
     tag_name  name of xml tag to find

   returns:
     index to character after close tag in line if specified tag found
     0 otherwise

   this function expects the file content buffer to contain an xml
   start tag for an element (<blah>), with or without attributes
   (<blah a="stuff">), NOT a tag for a simple element (<blah />).
   It need not be at the start at the line.

   this function does not reload or otherwise alter the file content buffer
*/
int get_start_tag(input_file_t *f, char *tag_name) {
  int result;
  char tag[MAX_TAG_NAME_LEN];
  char attrs[MAX_ATTRS_STR_LEN];
  int ind=0;

  result = find_first_tag(f, tag, sizeof(tag));
  if (result == -1 || strncmp(tag,tag_name, sizeof(tag))) return(-1); /* no tag or not the right one */
  ind = find_attrs(f, result, attrs, MAX_ATTRS_STR_LEN);
  if (ind > 0) result += ind;
  /* now look for spaces and > */
  while ((f->in_buf->content)[result] && (f->in_buf->content)[result] == ' ') result++;
  if ((f->in_buf->content)[result] != '>') return(-1);
  else return(result);
}


/*
  <namespace key="-2" case="first-letter">Media</namespace>
  <redirect title="Computer accessibility" />
  <case>first-letter</case>
  <comment>Fixing reference errors and rescuing orphaned refs (&quot;Review09&quot; → &quot;RevieTITS
w09&quot; from rev 527035558)</comment>

   args:
     f            file structure with file content buffer to check
     tag_name     name of xml tag to find
     holder       preallocated memory into which to copy the
                  element value, if found, otherwise the first
                  byte will be set to '\0'
                  If this argument is null no copy will be made
     holder_size  length of the value holder, if the element value
                  plus a terminating null is longer than this, only
                  holder_size bytes will be copied and the result
		  will not be null-terminated
     attrs        preallocated memory into which to copy the
                  (unparsed) string of atributes, if found, otherwise
                  the first byte will be set to '\0'
                  If this argument is null no copy will be made
     attrs_size   length of the attrs arg, if the string of element
                  attributes plus a terminating null is longer than
                  this, only attr_size bytes will be copied and the
                  result will not be null-terminated

   returns:
     index to character after close tag in line if specified tag found
     0 otherwise

   elts found by this function should have start and end tags
   on the same line OR a single tag with />.  the element should
   be the first tag in the line but need not start the line.

   this function expects the file content buffer to contain an xml
   start tag for an element (<blah>), with or without attributes
   (<blah a="stuff">), or a tag for a simple element (<blah />)

   if a start tag is found, this function will read further lines
   from the file into the file content buffer as needed in order to
   find the first close tag in the content, and will return with the
   line with that clos tag in the file content buffer

   if a simple element is found (<blah />) the file content buffer
   will be unchanged.
*/
int get_elt_with_attrs(input_file_t *f, char *tag_name, char *holder, int holder_size, char *attrs, int attrs_size) {
  int result;
  char tag[MAX_TAG_NAME_LEN];
  int ind;

  /* 
     <blah attr = " stuff " attr2 = "stuff" ... >value in here\n more value <tag />
     <blah attr = " stuff " attr2 = "stuff" ... /> 
     <text id="382338088" bytes="57" />
     <redirect title="Bon Jovi/New Jersey" />
  */
  if (attrs) attrs[0] = '\0';
  if (holder) holder[0]='\0';
  result = find_first_tag(f, tag, sizeof(tag));
  if (result == -1 || strncmp(tag,tag_name, sizeof(tag))) return(-1); /* no tag or not the right one */
  ind = find_attrs(f, result, attrs, attrs_size);
  if (ind > 0) result += ind;
  ind = find_value(f, result, holder, holder_size);
  if (ind > 0) {
    result += ind;
    /* next see if there is a close tag or just /> */
    ind = find_close_tag(f, result, tag, sizeof(tag));
    if (ind > 0) {
      if (strcmp(tag, tag_name)) return(-1); /* found different close tag! */
      else result += ind;
    }
  }
  else {
    /* hunt for /> instead */
    ind = find_simple_close_tag(f, result);
    if (ind == -1) return(-1); /* no close tag, something broken */
    else result += ind;
  }
  return(ind);
}

/*
   args:
     f         file structure with file content buffer to check
     tag_name  name of xml tag to find

   returns:
     index to character after close tag in line if specified tag found
     -1 otherwise or on error

   this function expects the file content buffer to contain an xml
   close tag for an element (</blah>) possibly with leading spaces,
   at the start of a line

   this function does not reload or otherwise alter the file content buffer
*/
int get_end_tag(input_file_t *f, char *tag_name) {
  char tag[MAX_TAG_NAME_LEN];
  int ind = 0;

  ind = find_close_tag(f, 0, tag, sizeof(tag));
  if (ind == -1) return(-1);
  if (strcmp(tag, tag_name)) return(-1); /* found different close tag! */
  return(ind);
}
