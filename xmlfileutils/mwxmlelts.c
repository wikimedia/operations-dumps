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

#define SHA_DIGEST_LENGTH 20

char page_in_process[MAX_ID_LEN];
int page_rows_written;
int rev_rows_written;
int text_bytes_written;

/* 4M standard buffer size for mysql */
#define MAX_TEXT_PACKET 4000000
/* about 1000 inserts stacked up, don't want more than that */
#define MAX_PAGE_BATCH 1000
#define MAX_REV_BATCH 1000

/*
  this function should be called before calling
  any other xml element processing function;
  it initializes some values used for example
  in error reporting or in batching up INSERT
  statements
 */
void init_mwxml() {
  strcpy(page_in_process,"none");
  page_rows_written = 0;
  rev_rows_written = 0;
  text_bytes_written = 0;
  return;
}

/*
  args:
    sqlp   structure for sql file for page table inserts
    sqlr   structure for sql file for revision table inserts
    sqlt   structure for sql file for text table inserts

  this function should be called just before closing the
  various sql output files; it writes a final COMMIT
  for each table if needed
 */
void cleanup_mwxml(output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt) {
  char buf[12];

  if (page_rows_written) {
    strcpy(buf,";\nCOMMIT;\n");
    while (sqlp) {
      put_line(sqlp, buf);
      sqlp = sqlp->next;
    }
  }
  if (rev_rows_written) {
    strcpy(buf,";\nCOMMIT;\n");
    while (sqlr) {
      put_line(sqlr, buf);
      sqlr = sqlr->next;
    }
  }
  if (text_bytes_written) {
    strcpy(buf,";\nCOMMIT;\n");
    while (sqlt) {
      put_line(sqlt, buf);
      sqlt = sqlt->next;
    }
  }
}

void whine(char *message, ...) {
  va_list argptr;

  va_start(argptr,message);

  fprintf(stderr,"WHINE: (%s) ", page_in_process);
  if (message)
    vfprintf(stderr,message, argptr);
  else
    fprintf(stderr,"problem encountered");
  fprintf(stderr,"\n");

  va_end(argptr);

  return;
}

/*
   <contributor>
     <username>OlEnglish</username>
     <id>7181920</id>
   </contributor>

   this function expects content buffer of file to already be
   filled with the line containing the contributor start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, r->contributor will be filled in completely
       on success and partially or not filled in at all
       on error.  Fields not filled in will contain the
       empty string.
*/
int do_contributor(input_file_t *f, contributor_t *c, int verbose) {
  if (!c) return(-1);
  c->username[0] = c->ip[0] = c->id[0] = '\0';
  if (get_start_tag(f, CONTRIBUTOR) == -1) return(0);

  if (get_line(f) == NULL) {
    whine("abrupt end of contributor data");
    return(0);
  }
  if (get_elt_with_attrs(f, USERNAME, c->username, sizeof(c->username), NULL, 0) != -1) {
    if (get_line(f) == NULL) {
      whine("abrupt end of contributor data");
      return(0);
    }
  }
  else c->username[0]='\0';
  un_xml_escape(c->username, NULL, 1);

  if (get_elt_with_attrs(f, ID, c->id, sizeof(c->id), NULL, 0) != -1) {
    if (get_line(f) == NULL) {
      whine("abrupt end of contributor data");
      return(0);
    }
  }
  else c->id[0] = '\0';

  if (get_elt_with_attrs(f, IP, c->ip, sizeof(c->ip), NULL, 0) != -1) {
    if (get_line(f) == NULL) {
      whine("abrupt end of contributor data");
      return(0);
    }
  }
  else c->ip[0]='\0';

  if (get_end_tag(f, CONTRIBUTOR) == -1) {
    whine("no rev end tag");
    return(0);
  }
  /* can we really have none of these? I think not so let's check that */
  if (!c->ip[0] && !c->id[0] && !c->username[0]) {
    whine("no user id or ip or name, something's wrong");
    return(0);
  }
  if (verbose > 1) {
    fprintf(stderr,"contributor info: username %s, ip %s, id %s\n", c->username, c->ip, c->id );
  }
  return(1);
}

  /*
    <revision>
      <id>381202555</id>
      <parentid>381200179</parentid>
      <timestamp>2010-08-26T22:38:36Z</timestamp>
      <contributor>
        <username>OlEnglish</username>
        <id>7181920</id>
      </contributor>
      <minor />
      <comment>[[Help:Reverting|Reverted]] edits by [[Special:Contributions/76.28.186.133|76.28.186.133]] ([[User talk:76.28.186.133|talk]]) to last version by Gurch</comment>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text>
      <sha1>lo15ponaybcg2sf49sstw9gdjmdetnk</sha1>
      <model>wikitext</model>
      <format>text/x-wiki</format>
    </revision>

  args:
    f     structure for xml input file
    id    revision id (character string)

  returns:
    0 on success
    -1 if not found or on error
    
  this function scans from the current position in the
  xml file line by line looking for a revision with the
  specified revision id

  once the id element for the matching revision is read,
  the file is left with that line in the file content buffer
*/
int find_rev_with_id(input_file_t *f, char *id) {
  int match = 0;
  char found_id[MAX_ID_LEN];

  /* if we are not at a rev tag, find one */
  while (!match) {
    while (get_start_tag(f, REVISION) == -1) {
      if (get_line(f) == NULL) {
	whine("no revision tag in file for id %s", id);
	return(-1);
      }
    }
    if (get_line(f) == NULL) {
      whine("no id for revision in file for id %s", id);
      return(-1);
    }
    if (get_elt_with_attrs(f, ID, found_id, sizeof(found_id), NULL, 0) == -1) {
      whine("no id for revision in file for id %s", id);
	return(-1);
    }
    if (!strcmp(id, found_id)) return(0);

    if (get_line(f) == NULL) {
      whine("no id for revision in file for id %s", id);
      return(-1);
    }
  }
  return(-1);
}

/*
   <text xml:space="preserve">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text>

   args:
      f    structure for xml input file

   returns:
      0 if a text tag is found before a revision close tag
      -1 otherwise or on error

  this function scans from the current position in the
  xml file line by line looking for a text start tag
  it will stop its scan if it finds the tag or if it
  encounters a revision close tag

  on return, the input file content buffer will contain the
  last line read
*/
int find_text_in_rev(input_file_t *f) {
  int result;
  char tag[MAX_TAG_NAME_LEN];

  while (get_end_tag(f, REVISION) == -1) {
    result = find_first_tag(f, tag, sizeof(tag));
    if (result != -1 && !strncmp(tag,TEXT, sizeof(tag))) break;
    if (get_line(f) == NULL) {
      whine("no text tag for revision in file");
      return(-1);
    }
  }
  return(0);
}

/*
   <text xml:space="preserve">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text> (text content file)
   (most will be multiple lines)

   args:
     f                structure for page content XML file
     sqlt             structure for sql output file for text table
     r                structure with for the revision containing this text elt
     verbose          0 for quiet mode, 1 or greater to display info about the record as it is being written
     t                structure containing all the standard db table names with db table prefix if any
                         (folks often set up local mw installations where all the tables have some
                         prefix like mw_ or what have you instead of just the regular names)
     insert_ignore    0 to write ordinary INSERT statements, 1 to write INSERT IGNORE (causes
                         mysql to ignore the insert if a record with the same primary key already exists)
     get_sha1         do sha1 of text content, normally you would want to dig this out of the
                         xml for the text elt in the stubs file
     get_text_length  compute the length of the text content (without xml escaping), normally
                         you would want to dig this out of the xml for the text lt in the stubs file
     text_compress:   1 to gzip text revision content and mark i as such in sql output, 0 for plaintext

   this function expects content buffer of file to already be
   filled with the line containing the text start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, fields r->text_len and r->sha1 may be
   modified using data from the text content,
*/
/*
   FIXME we still don't handle if a tag or something comes
   back to us without null termination, better get ready for that
*/
int do_text(input_file_t *f,  output_file_t *sqlt, revision_t *r, int verbose, tablenames_t *t, int insert_ignore, int get_sha1, int get_text_length, int text_compress) {
  int result=0;
  char *ind;
  char *endtag;
  char buf[TEXT_BUF_LEN], raw[TEXT_BUF_LEN], esc_buf[TEXT_BUF_LEN];
  int todo_length;
  char *todo, *todo_new;
  int text_length = 0;
  sha1_context ctx;
  unsigned char sha1[SHA_DIGEST_LENGTH];
  unsigned char sha1_string[SHA_DIGEST_LENGTH*2 +1];
  int i=0;
  char *compressed_content = NULL;
  int compressed_length = 0;
  int text_field_len = 0;
  char *leftover = NULL;
  char compressed_buf[TEXT_BUF_LEN_PADDED];
  char *compressed_ptr = NULL;

  unsigned int sha1_copy[SHA_DIGEST_LENGTH*2 +1];
  unsigned int sha1_temp[SHA_DIGEST_LENGTH*2 +1];
  unsigned int sha1_num[SHA_DIGEST_LENGTH/3 +1];
  int sha1_num_len;
  unsigned int sha1_b36[SHA_DIGEST_LENGTH*8/5 + 6];
  int sha1_b36_len;

  if (get_sha1) sha1_starts(&ctx);

  ind = strstr(f->in_buf->content, "<text");
  if (!ind) return(0);
  ind += 5;
  /* must be: space, or > or /> */
  if (*ind == ' ') {
    while (*ind == ' ') ind++;
    while (*ind && !(*ind == '/' && *(ind+1) == '>') && *ind != '>') ind++;
  }
  if (*ind != '>') return(0); /* other options are: no close of tag on line, weird... or
				 tag ends in /> which means no content, probably deleted */

  ind++;  /* skip that closing '.' */

  /* text table row fields are the same across all MW versions so no check needed here... yet */
  if (text_bytes_written == 0) {
    strcpy(buf,"BEGIN;\n");
    put_line_all(sqlt, buf);
    snprintf(buf, sizeof(buf), "INSERT %s INTO %s (old_id, old_text, old_flags) VALUES\n", insert_ignore?"IGNORE":"", t->text);
    put_line_all(sqlt, buf);
  }
  else {
    strcpy(buf,",\n");
    put_line_all(sqlt, buf);
  }
  /* text: old_text old_flags */
  /* write the beginning piece */
  snprintf(buf, sizeof(buf),						\
	   "(%s, '",r->text_id);
  put_line_all(sqlt, buf);

  if (verbose > 1) fprintf(stderr,"text info: insert start of line written\n");

  text_field_len = 0; /* length of the text field in the db, as it is stored */
  while (1) {
    endtag = strstr(ind, "</text>");
    if (!endtag) {
      leftover = un_xml_escape(ind, raw, 0);
      if (get_text_length) text_length+= strlen(raw);
      if (get_sha1) sha1_update(&ctx, (unsigned char *)raw, strlen(raw));
      if (text_compress) {
	/* FIXME do something with this return value */
	compressed_ptr = gzipit(raw, &compressed_length, compressed_buf, sizeof(compressed_buf));
	/* this can be null terminated cause nulls get escaped, yay */
	text_field_len += compressed_length;
	todo = compressed_content;
	todo_length = compressed_length;
	while (1) {
	  todo_new = sql_escape(todo, todo_length, esc_buf, sizeof(esc_buf));
	  put_line_all(sqlt, esc_buf);
	  if (!todo_new) break;
	  todo_length = todo_length - (todo_new - todo);
	  todo = todo_new;
	}
      }
      else {
	text_field_len += strlen(raw);
	todo = raw;
	todo_length = strlen(raw);
	while (1) {
	  todo_new = sql_escape(todo, todo_length, esc_buf, sizeof(esc_buf));
	  put_line_all(sqlt, esc_buf);
	  if (!todo_new) break;
	  todo_length = todo_length - (todo_new - todo);
	  todo = todo_new;
	}
      }
      if (leftover) {
	/* keep the bytes at the end, maybe they are
	   the first part of an escaped character */
	strcpy(f->leftover, leftover);
      }
      /* this can mean we don't process the last few chrs of the
	 file in case the xml output is terminated in the middle,
	 that's not a disaster */
      if (get_line(f) == NULL) break;
      ind = f->in_buf->content;
    }
    else {
      *endtag = '\0'; /* cheap trick but works :-P */
      un_xml_escape(ind, raw, 1);
      *endtag = '<';
      if (get_text_length) text_length+= strlen(raw);
      if (get_sha1) sha1_update(&ctx, (unsigned char *)raw, strlen(raw));
      if (text_compress) {
	/* FIXME do something with this return value */
	compressed_ptr = gzipit(raw, &compressed_length, compressed_buf, sizeof(compressed_buf));
	text_field_len += compressed_length;
	text_field_len += compressed_length;
	todo = compressed_content;
	todo_length = compressed_length;
	while (1) {
	  todo_new = sql_escape(todo, todo_length, esc_buf, sizeof(esc_buf));
	  put_line_all(sqlt, esc_buf);
	  if (!todo_new) break;
	  todo_length = todo_length - (todo_new - todo);
	  todo = todo_new;
	}
      }
      else {
	text_field_len += strlen(raw);
	todo = raw;
	todo_length = strlen(raw);
	while (1) {
	  todo_new = sql_escape(todo, todo_length, esc_buf, sizeof(esc_buf));
	  put_line_all(sqlt, esc_buf);
	  if (!todo_new) break;
	  todo_length = todo_length - (todo_new - todo);
	  todo = todo_new;
	}
      }
      *endtag = '<';
      result = 1;
      break;
    }
  }
  /* write out the end piece */
  text_bytes_written += text_field_len;
  strcpy(buf,"', ");
    put_line_all(sqlt, buf);

  sprintf(buf,"'%s')", text_compress?"utf-8,gzip":"utf-8");
  put_line_all(sqlt, buf);

  if (text_bytes_written > MAX_TEXT_PACKET) {
    strcpy(buf,";\nCOMMIT;\n");
    put_line_all(sqlt, buf);
    text_bytes_written = 0;
  }

  
  /*
     for cases where we have to compute it ourselves.
     more recent schemas have bytes attr in the text tag of
     stubs dumps so we don't have to compute it.
  */
  if (get_text_length) sprintf(r->text_len, "%d", text_length);
  /*
     for cases where we have to compute it ourselves.
     more recent schemas have sha1 tag in the revision
     so we don't have to compute it.
  */
  if (get_sha1) {
    sha1_finish(&ctx, sha1);

    /* base36 conversion, blah */
    for (i=0; i < SHA_DIGEST_LENGTH; i++)
      sprintf((char*)&(sha1_string[i*2]), "%02x", sha1[i]);

    /*    sha1_num_len = hexstring2int((char *)sha1_string, SHA_DIGEST_LENGTH*2, sha1_num);*/
    sha1_num_len = hexstring2int((char *)sha1_string, SHA_DIGEST_LENGTH*2, sha1_num);
    sha1_b36_len = tobase36(sha1_num, sha1_copy, sha1_temp, sha1_num_len, sha1_b36);
    int2string(sha1_b36, sha1_b36_len, r->sha1);
  }

  if (verbose > 1) fprintf(stderr,"text info: insert end of line written\n");
  return(result);
}

/*
    <revision>
      <id>381202555</id>
      <parentid>381200179</parentid>
     <timestamp>2010-08-26T22:38:36Z</timestamp>
      <contributor>
        <username>OlEnglish</username>
        <id>7181920</id>
      </contributor>
      <minor />
     <comment>[[Help:Reverting|Reverted]] edits by [[Special:Contributions/xx.xx.xx.xx|xx.xx.xx.xx]] ([[User talk:xx.xx.xx.xx|talk]]) to last version by Gurch</comment>
     <text xml:space="preserve">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text>
      <sha1>lo15ponaybcg2sf49sstw9gdjmdetnk</sha1>
      <model>wikitext</model>
      <format>text/x-wiki</format>
    </revision>

   this function reads through the xml for a revision (from an xml stub file) and writes a mysql
   insert statement for the revision to the output file for the revision table
   it also reads the text for the revision (from an xml text content file) and writes a mysql
   insert statement for the text content to the output file for the text table
   while the tuple for the insert is written one per row, BEGIN/COMMIT and INSERT are written
   for every MAX_REV_BATCH lines
   
   args:
     stubs:           structure for stubs XML file
     text:            structure for page content XML file
     text_compress:   1 to gzip text revision content and mark i as such in sql output, 0 for plaintext
     sqlp:            structure for sql output file for page table
     sqlr:            structure for sql output file for revision table
     sqlt:            structure for sql output file for text table
     p:               structure for page which this revision is an element
     verbose:         0 for quiet mode, 1 or greater to display info about the record as it is being written
     t:               structure containing all the standard db table names with db table prefix if any
                         (folks often set up local mw installations where all the tables have some
                         prefix like mw_ or what have you instead of just the regular names)
     insert_ignore:   0 to write ordinary INSERT statements, 1 to write INSERT IGNORE (causes
                         mysql to ignore the insert if a record with the same primary key already exists)

   this function expects content buffer of the stubs file to already be
   filled with the line containing the revision start tag
   the text content file may be left at some random line; it will be scanned until
   the next revision is found and then checked for the text content

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, fields p->touched, p->len, p->latest
     and p->model may be modified using data from
     the corresponding revision fields.
*/
int do_revision(input_file_t *stubs, input_file_t *text, int text_compress, output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt, page_t *p, int verbose, tablenames_t *t, int insert_ignore) {
  char out_buf[TEXT_BUF_LEN*2];
  revision_t r;
  contributor_t c;
  int get_sha1 = 0;
  int get_text_len = 0;
  char escaped_comment[FIELD_LEN*2];
  char escaped_user[FIELD_LEN*2];

  char attrs[MAX_ATTRS_STR_LEN];
  char *attrs_ptr = NULL;
  char *todo = NULL;
  char name[400];
  char value[400];
  int result = 0;

  mw_version_t *mwv;

  mwv = sqlr->mwv;

  if (get_start_tag(stubs, REVISION) == -1) return(0);

  if (get_line(stubs) == NULL) {
    whine("abrupt end of revision data");
    return(0);
  }

  r.contributor = &c;
  c.username[0] = '\0';
  c.ip[0] = '\0';
  c.id[0] = '\0';

  r.text = NULL;

  get_elt_with_attrs(stubs, ID, r.id, sizeof(r.id), NULL, 0);
  if (get_line(stubs) == NULL) {
    whine("abrupt end of revision data");
    return(0);
  }

  /* this is optional and so compatible with schemas earlier than 0.7 */
  if (get_elt_with_attrs(stubs, PARENTID, r.parent_id, sizeof(r.parent_id), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
  }

  get_elt_with_attrs(stubs, TIMESTAMP, r.timestamp, sizeof(r.timestamp), NULL, 0);
  if (r.timestamp[0]) {
    /* fix up r.timestamp, it is in format 2006-09-08T04:15:52Z but needs to be in format 20130118111419 */
    digits_only(r.timestamp);
  }

  if (get_line(stubs) == NULL) {
    whine("abrupt end of revision data in rev id %s", r.id);
    return(0);
  }
  do_contributor(stubs, &c, verbose);
  if (get_line(stubs) == NULL) {
    whine("abrupt end of revision data in rev id %s", r.id);
    return(0);
  }
  if (get_elt_with_attrs(stubs, MINOR, NULL, 0, NULL, 0) != -1) {
    r.minor[0]='1';
    r.minor[1]='\0';
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
    if (verbose > 2) fprintf(stderr,"this is a minor revision\n");
  }
  else {
    r.minor[0]='0';
    r.minor[1]='\0';
  }

  r.comment[0] = '\0';
  if (get_elt_with_attrs(stubs, COMMENT, r.comment, sizeof(r.comment), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
  }
  un_xml_escape(r.comment, NULL, 1);
  r.text_id[0] = '\0';
  r.text_len[0] = '\0';

  /* schema 0.7 has sha1 then text, earlier schema don't have it at all so look for it here optionally */
  if (get_elt_with_attrs(stubs, SHA1, r.sha1, sizeof(r.sha1), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
  }

  /*       <text id="382338088" bytes="57" />  */
  get_elt_with_attrs(stubs, TEXT, NULL, 0, attrs, MAX_ATTRS_STR_LEN);

  if (verbose > 1) fprintf(stderr,"text tag found, %s\n", attrs);
  attrs_ptr = attrs;
  while (1) {
    result = get_attr(attrs_ptr, name, value, &todo);
    if (result == -1) {
      whine("bad attribute info in text tag");
      break;
    }
    else if (! result) break;
    if (!strcmp(name, "id"))
      strcpy(r.text_id, value);
    else if (!strcmp(name, "bytes"))
      strcpy(r.text_len, value);
    else {
      whine("unknown attribute in text tag");
      break;
    }
    if (!todo) break;
    else attrs_ptr = todo;
  }

  if (get_line(stubs) == NULL) {
    whine("abrupt end of revision data in rev id %s", r.id);
    return(0);
  }

  r.model[0] = '\0';
  r.format[0] = '\0';

  /* schema 0.8 and later have sha1 here after text */
  if (! r.sha1[0]) {
    if (get_elt_with_attrs(stubs, SHA1, r.sha1, sizeof(r.sha1), NULL, 0) != -1) {
      if (get_line(stubs) == NULL) {
	whine("abrupt end of revision data in rev id %s", r.id);
	return(0);
      }
    }
  }
  /* schema 0.8 and later have model and format */
  if (get_elt_with_attrs(stubs, MODEL, r.model, sizeof(r.model), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
  }
  if (get_elt_with_attrs(stubs, FORMAT, r.format, sizeof(r.format), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of revision data in rev id %s", r.id);
      return(0);
    }
  }

  if (get_end_tag(stubs, REVISION) == -1) {
    whine("no rev end tag for rev id %s", r.id);
    return(0);
  }

  /* 
     If schema is earlier than 0.5 or for some other reason we don't
     hve the bytes attr in the text tag, AND we aren't reading the
     text content file, rev_len will be blithely inserted as 0. You have
     been warned!
  */
  if (text) {
    if (find_rev_with_id(text, r.id) != -1) {
      if (!find_text_in_rev(text)) {
	/* even if this turns out bad we are committed to adding the revision at this point */

	/* if any version in our list is recent enough that we will write out the field, we need it */
	if (mwv_any_greater(mwv,1,18) && !r.sha1[0]) get_sha1 = 1;
	if (mwv_any_greater(mwv,1,8) && !r.text_len[0]) get_text_len = 1;

	do_text(text, sqlt, &r, verbose, t, insert_ignore, get_sha1, get_text_len, text_compress);
      }
    }
  }

  sql_escape(r.comment,-1,escaped_comment, sizeof(escaped_comment));
  if (c.username[0]) sql_escape(c.username,-1,escaped_user, sizeof(escaped_user));
  if (verbose > 1) {
    fprintf(stderr,"revision info: id %s, parentid %s, timestamp %s, minor %s, comment %s, sha1 %s, model %s, format %s, len %s, textid %s\n", r.id, r.parent_id, r.timestamp, r.minor, escaped_comment, r.sha1, r.model, r.format, r.text_len, r.text_id);
  }

  /* this must be done before we pass up the rev contents to the page */
  if (!strcmp(r.model,"wikitext")) r.model[0] = '\0';
  if (!strcmp(r.format,"text/x-wiki")) r.format[0] = '\0';

  if (strcmp(r.timestamp, p->touched) > 0) {
    strcpy(p->touched,r.timestamp);
    strcpy(p->len,r.text_len);
    strcpy(p->latest,r.id);
    strcpy(p->model, r.model);
  }

  /* fixme having a fixed size buffer kinda sucks here */

  /*
    MW 1.19+: rev_sha1
    MW 1.10+: rev_len, rev_parent_id
  */
  if (!rev_rows_written) {
    strcpy(out_buf,"BEGIN;\n");
    put_line_all(sqlr, out_buf);
    if (verbose > 2) fprintf(stderr,"(%s) %s",t->revs, out_buf);

    snprintf(out_buf, sizeof(out_buf), "INSERT %s INTO %s \
(rev_id, rev_page, rev_text_id, rev_comment, rev_user, \
rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted", \
	     insert_ignore?"IGNORE":"", t->revs);
    put_line_all(sqlr, out_buf);    
    if (verbose > 2) fprintf(stderr,"(%s) %s",t->revs, out_buf);

    strcpy(out_buf, ", rev_len, rev_parent_id");
    write_if_mwv(sqlr, 1,9,0,0,out_buf, verbose);    

    strcpy(out_buf, ", rev_sha1");
    write_if_mwv(sqlr, 1,18,0,0,out_buf, verbose);    

    strcpy(out_buf, ", rev_content_model, rev_content_format");
    write_if_mwv(sqlr, 1,20,0,0,out_buf, verbose);    

    strcpy(out_buf,") VALUES\n");
    put_line_all(sqlr, out_buf);    
    if (verbose > 2) fprintf(stderr,"(%s) %s",t->revs, out_buf);

  }
  else {
    strcpy(out_buf,",\n");
    put_line_all(sqlr, out_buf);
  }
  /* text: rev_comment rev_user_text rev_timestamp rev_sha1 rev_content_model rev_content_format */
  /* possible null: rev_content_model rev_content_format */

  snprintf(out_buf, sizeof(out_buf),		   \
      "(%s, %s, %s, '%s', %s, '%s', '%s', %s, %s", \
	   r.id, p->id, r.text_id, escaped_comment, c.id[0]?c.id:"0",	\
	   c.ip[0]?c.ip:escaped_user, \
	   r.timestamp, r.minor, "0");
  put_line_all(sqlr, out_buf);
  if (verbose > 2) fprintf(stderr,"(%s) %s",t->revs, out_buf);

  strcpy(out_buf, ", ");
  write_if_mwv(sqlr, 1, 9, 0, 0, out_buf, verbose);

  copy_sql_field(out_buf, r.text_len[0]?r.text_len:NULL, 1, 0);
  write_if_mwv(sqlr, 1, 9, 0, 0, out_buf, verbose);

  copy_sql_field(out_buf, r.parent_id[0]?r.parent_id:NULL, 1, 1);
  write_if_mwv(sqlr, 1, 9, 0, 0, out_buf, verbose);

  sprintf(out_buf, ", '%s'", r.sha1);
  write_if_mwv(sqlr, 1, 18, 0, 0, out_buf, verbose);

  strcpy(out_buf, ", ");
  write_if_mwv(sqlr, 1, 20, 0, 0, out_buf, verbose);

  copy_sql_field(out_buf, r.model[0]?r.model:NULL, 1, 0);
  write_if_mwv(sqlr, 1, 20, 0, 0, out_buf, verbose);

  copy_sql_field(out_buf, r.format[0]?r.format:NULL, 1, 1);
  write_if_mwv(sqlr, 1, 20, 0, 0, out_buf, verbose);

  if (rev_rows_written == MAX_REV_BATCH) {
    strcpy(out_buf,");\nCOMMIT;\n");
    put_line_all(sqlr, out_buf);
    if (verbose > 2) fprintf(stderr,"%s", out_buf);
    rev_rows_written = 0;
  }
  else {
    strcpy(out_buf,")");
    put_line_all(sqlr, out_buf);
    if (verbose > 2) fprintf(stderr,"%s,\n",out_buf);
    rev_rows_written++;
  }

  return(1);
}

/*
  args:
     f    structure for xml input file
     id   id of page to find (character string)

  returns:
    0 on success
    -1 if not found or on error
    
  this function scans from the current position in the
  xml file line by line looking for a page with the
  specified page id

  once the id element for the matching page is read,
  the file is left with that line in the file content buffer

  this function does not save information like the page title
  anywhere, it is intended for parsing through the page
  content xml file after information from the corresponding
  page from the stubs xml file has already been read and
  stored
 */
int find_page_with_id(input_file_t *f, char *id) {
  int match = 0;
  char found_id[MAX_ID_LEN];

  /* if we are not at a page tag, find one */
  while (!match) {
    while (get_start_tag(f, PAGE) == -1) {
      if (get_line(f) == NULL) {
	whine("no page tag in file");
	return(-1);
      }
    }
    if (get_line(f) == NULL) {
      whine("no id for page in file");
      return(-1);
    }
    if (get_elt_with_attrs(f, TITLE, NULL, 0, NULL, 0) != -1) {
      if (get_line(f) == NULL) {
	whine("no id for page in file");
	return(-1);
      }
    }
    if (get_elt_with_attrs(f, NS, NULL, 0, NULL, 0) != -1) {
      if (get_line(f) == NULL) {
	whine("no id for page in file");
	return(-1);
      }
    }
    if (get_elt_with_attrs(f, ID, found_id, sizeof(found_id), NULL, 0) == -1) {
	whine("no id for page in file");
	return(-1);
    }
    if (!strcmp(id, found_id)) return(0);

    if (get_line(f) == NULL) {
      whine("no id for page in file");
      return(-1);
    }
  }
  return(-1);
}


/*
  <page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
    ...
    </revision>
  </page>

   this function expects content buffer of file to already be
   filled with the line containing the page start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read
*/

int do_page(input_file_t *stubs, input_file_t *text, int text_compress, output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt, siteinfo_t *s, int verbose, tablenames_t *t, int insert_ignore, char*start_page_id) {
  page_t p;
  char out_buf[1024]; /* seriously how long can username plus title plus the rest of the cruft be? */
  int want_text = 0;
  char escaped_title[FIELD_LEN*2];
  int skip = 0;

  p.title[0] = '\0';
  p.ns[0] = '\0';
  p.id[0] = '\0';
  p.redirect[0] = '0';
  p.redirect[1] = '\0';
  p.restrictions[0] = '\0';
  p.touched[0] = '\0';
  p.latest[0] = '\0';
  p.model[0] = '\0';

  if (get_start_tag(stubs, PAGE) == -1) return(0);

  if (get_line(stubs) == NULL) {
    whine("no title tag");
    return(0);
  }
  if (get_elt_with_attrs(stubs, TITLE, p.title, sizeof(p.title), NULL, 0) == -1) {
    whine("no title tag");
  }
  else {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
  }
  un_xml_escape(p.title, NULL, 1);

  /* this is optional, thus supporting schemas earlier than 0.6 */
  if (get_elt_with_attrs(stubs, NS, p.ns, sizeof(p.ns), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
  }
  if (get_elt_with_attrs(stubs, ID, p.id, sizeof(p.id), NULL, 0) == -1) {
    whine("no page id");
  }
  else {
    strcpy(page_in_process,p.id);
    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
    if (start_page_id) {
      if (strlen(start_page_id) > strlen(p.id)) skip = 1;
      else if (strlen(start_page_id) < strlen(p.id)) skip=0;
      else if (strcmp(start_page_id, p.id) > 0) skip = 1;
    }
    if (skip) {
      if (verbose > 1) fprintf(stderr,"skipping page %s by user request\n", p.id);
      while (1) {
	if (get_end_tag(stubs, PAGE) == -1) {
	  if (get_line(stubs) == NULL) {
	    whine("abrupt end of page data");
	    return(0);
	  }
	}
	else break;
      }

      if (want_text) {
	/* also skip forward in the text file */
	if (find_page_with_id(text, p.id) == -1) {
	  return(0);
	}
      }
      return(1);
    }
  }

  /* because we don't check the attributes for the redirect elt, this
     supports schemas earlier than 0.6 */
  if (get_elt_with_attrs(stubs, REDIRECT, NULL, 0, NULL, 0) != -1) {
    p.redirect[0] = '1';
    p.redirect[1] = '\0';

    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
  }
  else {
    p.redirect[0] = '0';
    p.redirect[1] = '\0';
  }

  if (get_elt_with_attrs(stubs, RESTRICTIONS, p.restrictions, sizeof(p.restrictions), NULL, 0) != -1) {
    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
  }
  sql_escape(p.title,-1, escaped_title, sizeof(escaped_title));
  namespace_strip(escaped_title, s);
  title_escape(escaped_title);
  /* we also need blank to _, see what else happens, woops */
  if (verbose > 1) {
    fprintf(stderr,"page info: title %s, id %s, ns %s, redirect %s, restrictions %s\n", escaped_title, p.id, p.ns, p.redirect, p.restrictions);
  }
  if (p.id) {
    if (text) {
      if (find_page_with_id(text, p.id) != -1) {
	want_text++;
      }
      /* fixme error check */
      else {
	whine("couldn't find page with the right rev in text file, skipping rev\n");
      }
    }
  }

  while (1) {
    if (!do_revision(stubs, want_text?text:NULL, text_compress, sqlp, sqlr, sqlt, &p, verbose, t, insert_ignore)) break;
    if (get_line(stubs) == NULL) {
      whine("abrupt end of page data");
      return(0);
    }
  }
  if (!page_rows_written) {
    strcpy(out_buf,"BEGIN;\n");
    put_line_all(sqlp, out_buf);
    if (verbose > 2) fprintf(stderr,"(%s) %s",t->page, out_buf);

    snprintf(out_buf, sizeof(out_buf), "INSERT %s INTO %s \
(page_id, page_namespace, page_title, page_restrictions, \
page_counter, page_is_redirect, page_is_new, \
page_random, page_touched, page_latest, page_len", insert_ignore?"IGNORE":"", t->page);
    put_line_all(sqlp, out_buf);
    if (verbose > 2) fprintf(stderr,"(%s) %s",t->page, out_buf);

    snprintf(out_buf, sizeof(out_buf), ", page_content_model");
    write_if_mwv(sqlp, 1,20,0,0,out_buf, verbose);

    strcpy(out_buf, ") VALUES\n");
    put_line_all(sqlp, out_buf);

  }
  else {
    strcpy(out_buf,",\n");
    put_line_all(sqlp, out_buf);
  }

  /* fixme having a fixed size buffer kinda sucks here */
  /* text: page_title page_restrictions page_touched */
  snprintf(out_buf, sizeof(out_buf),				\
       "(%s, %s, '%s', '%s', %s, %s, %s, %.14f, '%s', %s, %s", \
	   p.id, p.ns, escaped_title, p.restrictions,		\
	   "0", p.redirect, "0", drand48(), p.touched, p.latest, p.len );
  put_line_all(sqlp, out_buf);
  if (verbose > 2) fprintf(stderr,"(%s) %s",t->page, out_buf);

  strcpy(out_buf, ", ");
  write_if_mwv(sqlp, 1, 20, 0, 0, out_buf, verbose);

  copy_sql_field(out_buf, p.model[0]?p.model:NULL, 1, 1);
  write_if_mwv(sqlp, 1, 20, 0, 0, out_buf, verbose);

  if (page_rows_written == MAX_PAGE_BATCH) {
    strcpy(out_buf,");\nCOMMIT;\n");
    put_line_all(sqlp, out_buf);
    if (verbose > 2) fprintf(stderr,"%s", out_buf);
    page_rows_written = 0;
  }
  else {
    strcpy(out_buf,")");
    put_line_all(sqlp, out_buf);
    if (verbose > 2) fprintf(stderr,"%s,\n",out_buf);
    page_rows_written++;
  }    

  if (get_end_tag(stubs, PAGE) == -1) {
    whine("no end page tag");
    return(0);
  }
  return(1);
}

/* 
   args:
      s      string for parsing
      name   holder for attr name
      value  holder for attr value
      todo   to be filled with pointer to remainder of string for parsing,
             or NULL if there is nothing left

   returns:  1 if found an attr, 0 if end of string, -1 on error

   FIXME length checks needed on those just in case
*/
int get_attr( char *s, char *name, char *value, char **todo) {
  char *name_start, *name_end, *value_start, *value_end;
  char saved;

  *todo = s;
  while (*s == ' ') s++;
  /* empty, we must be done */
  if (!*s) return(0);

  /* get up to first =  and stuff in name */
  name_start = s;
  while (*s && *s != '=' && *s != ' ') s++;
  name_end = s;

  saved = *name_end;
  while (*s == ' ') s++;
  if (*s != '=') {
    *todo = s;
    return(-1);
  }
  s++;
  while (*s == ' ') s++;

  value_start = s;
  while (*s && *s != ' ') s++;
  value_end = s;

  name_end[0] = '\0';
  strcpy(name, name_start);
  name_end[0] = saved;

  /* strip quotes on the value if we have them */
  if (*value_start == '"' && *(value_end-1) == '"') {
    strncpy(value, value_start+1, value_end - value_start -2);
    value[value_end - value_start -2] = '\0';
  }
  else {
    strncpy(value, value_start, value_end - value_start);
    value[value_end - value_start] = '\0';
  }
  *todo = s;
  return(1);
}

/* 
   <namespace key="-2" case="first-letter">Media</namespace>
   <namespace key="0" case="first-letter" />

   this function expects content buffer of file to already be
   filled with the line containing the namespace start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, fields in n will be filled in completely
       on success and partially or not filled in at all
       on error.  Fields not filled in will contain the
       empty string.
*/
int do_namespace(input_file_t *f, namespace_t *n, int verbose) {
  char attrs[MAX_ATTRS_STR_LEN];
  char *attrs_ptr = NULL;
  char *todo = NULL;
  char name[400];
  char value[400];
  int result = 0;

  if (!n) return(-1);
  n->namespace[0] = n->n_case[0] = n->key[0] = 0;
  n->next = NULL;

  if (get_elt_with_attrs(f, NAMESPACE, value, sizeof(value), attrs, MAX_ATTRS_STR_LEN) == -1) return(0);
  if (verbose > 1) fprintf(stderr,"namespace tag found '%s', %s\n", value, attrs);
  strcpy(n->namespace,value);
  /* process attribs and stuff them into n->key, n->n_case */
  attrs_ptr = attrs;
  while (1) {
    result = get_attr(attrs_ptr, name, value, &todo);
    if (result == -1) {
      whine("bad attribute info in namespace tag");
      break;
    }
    else if (! result) break;
    if (!strcmp(name, "key"))
      strcpy(n->key, value);
    else if (!strcmp(name, "case"))
      strcpy(n->n_case, value);
    else {
      whine("unknown attribute in namespace tag");
      break;
    }
    if (!todo) break;
    else attrs_ptr = todo;
  }

  /* hack, for main namespace display bleah */
  if (!strcmp(n->key, "\"0\"") && !strcmp(n->namespace,"")) strcpy(n->namespace,"Main");
  return(1);
}

/*
   this function expects content buffer of file to already be
   filled with the line containing the namespaces start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, s->namespaces will be filled in completely
       on success and partially or not filled in at all
       on error.  If not filled in it will be NULL.
*/
int do_namespaces(input_file_t *f, siteinfo_t *s, int verbose) {
  int result = 0;
  namespace_t *namespace_head, *n;

  s->namespaces = NULL;
  namespace_head = NULL;

  if (get_start_tag(f, NAMESPACES) == -1) return(0);
  if (verbose > 1) fprintf(stderr,"start namespaces tag found\n");

  if (get_line(f) == NULL) {
    whine("no end namespaces tag");
    return(0);
  }
  while (1) {
    n = (namespace_t *)malloc(sizeof(namespace_t));
    if (n == NULL) {
      fprintf(stderr,"Failed to get memory for namespace\n");
      exit(1);
    }
    result = do_namespace(f, n, verbose);
    if (!result) break;

    if (!namespace_head) {
      s->namespaces = n;
      namespace_head = s->namespaces;
    }
    else {
      namespace_head->next = n;
      namespace_head = namespace_head->next;
    }

    if (get_line(f) == NULL) {
      whine("no end namespaces tag");
      return(0);
    }
  }

  if (get_end_tag(f, NAMESPACES) == -1) {
    whine("no end namespaces tag");
    return(0);
  }
  if (verbose > 1) fprintf(stderr,"end namespaces tag found\n");
  return(1);
}

/*
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <base>http://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.21wmf6</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
    </namespaces>
  </siteinfo>

   this function expects content buffer of file to already be
   filled with the line containing the siteinfo start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, s_info will point to a completely filled in
       structure on success and partially or not filled in at all
       on error.  If not filled in at all, it will be NULL.
*/
int do_siteinfo(input_file_t *f, siteinfo_t **s_info, int verbose) {
  int result = 0;
  siteinfo_t *s = NULL;

  if (s_info) *s_info = NULL;

  s = (siteinfo_t *)malloc(sizeof(siteinfo_t));
  if (s == NULL) {
    fprintf(stderr,"Failed to get memory for siteinfo\n");
    exit(1);
  }
  s->sitename[0] = s->base[0] = s->generator[0] = s->s_case[0] = '\0';
  s->namespaces = NULL;

  if (s_info) *s_info = s;

  if (get_start_tag(f, SITEINFO) == -1) return(0);

  if (verbose > 1) fprintf(stderr,"siteinfo tag found\n");

  if (get_line(f) == NULL) {
    whine("abrupt end to siteinfo");
    return(0);
  }
  result = get_elt_with_attrs(f, SITENAME, s->sitename, sizeof(s->sitename), NULL, 0);

  if (get_line(f) == NULL) {
    whine("abrupt end to siteinfo");
    return(0);
  }
  result = get_elt_with_attrs(f, BASE, s->base, sizeof(s->base), NULL, 0);

  if (get_line(f) == NULL) {
    whine("abrupt end to siteinfo");
    return(0);
  }
  result = get_elt_with_attrs(f, GENERATOR, s->generator, sizeof(s->generator), NULL, 0);

  if (get_line(f) == NULL) {
    whine("abrupt end to siteinfo");
    return(0);
  }
  result = get_elt_with_attrs(f, CASE, s->s_case, sizeof(s->s_case), NULL, 0);

  if (get_line(f) == NULL) {
    whine("abrupt end to siteinfo");
    return(0);
  }
  result = do_namespaces(f, s, verbose);
  if (result) {
    if (get_line(f) == NULL) {
      whine("abrupt end to siteinfo");
      return(0);
    }
  }

  if (get_end_tag(f, SITEINFO) == -1) {
    whine("no end siteinfo tag\n");
    return(0);
  }

  return(1);
}

/*
    <mediawiki xmlns="http://www.mediawiki.org/xml/export-0.8/" \
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
               xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.8/ \
                                   http://www.mediawiki.org/xml/export-0.8.xsd" \
               version="0.8" \
               xml:lang="en">

   args:
     f            structure for stubs XML file
     skipschema   1 to look for schema info and store it, 0 to return as soon as
                  start tag <mediawiki is found
     schema       pointer to holder for string with schema version (e.g. '0.8')
     verbose      0 for quiet mode, 1 or greater to display info about the record
                  as it is being written

   this function expects content buffer of file to already be
   filled with the line containing the mediawiki start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, schema will point to a string containing 
   the export xsd version (e.g. '0.8') on success
   and will contain NULL on error.
*/
int do_mw_header(input_file_t *f, int skipschema, char **schema, int verbose) {
  char *start_schema, *end_schema;

  if (schema) *schema = NULL;
  if (get_start_tag(f, MEDIAWIKI) == -1) return(0);
  if (verbose > 1) fprintf(stderr,"mediawiki tag found\n");
  if (!skipschema && schema) {
    start_schema = strstr(f->in_buf->content, "version=\"");
    if (start_schema) {
      start_schema+= strlen("version=\"");
      end_schema = strchr(start_schema, '"');
      if (!end_schema) return(0);
      *end_schema = '\0';
      *schema = (char *)malloc(strlen(start_schema)+ 1);
      if (!*schema) return(0);
      strcpy(*schema, start_schema);
      *end_schema = '"';
    }
    if (verbose)
      fprintf(stderr,"schema in mw header: %s\n", *schema);
  }
  return(1);
}

/*                                                                                                                                        
   this function reads a MediaWiki XML input stream (either stubs or
   page content) and collects information from the <mediawiki> and
   <siteinfo> elements.  Once it has reached the end of those elements
   or encouontered an error, it returns.

   args:
     f            structure for stubs XML file
     skipschema   1 to look for schema info and store it, 0 to return as soon as
                  start tag <mediawiki is found
     schema       pointer to preallocated holder for string with schema version (e.g. '0.8')
     s            poiter to holder for site info
     verbose      0 for quiet mode, 1 or greater to display info about the record
                  as it is being written

   this function expects content buffer of file to already be
   filled with the line containing the mediawiki start tag

   returns 0 on error, 1 on success

   on return, buffer will contain:
     last read line if we hit eof during processing
     last read line that doesn't follow the above xml
       fragment, if there's a problem with the element
     next line available for scanning, if the element
       is successfully read

   on return, schema will point to a string containing 
       the export xsd version (e.g. '0.8') on success
       and will contain NULL on error.

   on return, s_info will point to a completely filled in
       structure on success and partially or not filled in at all
       on error.  If not filled in at all, it will be NULL.
*/
int do_file_header(input_file_t *f, int skipschema, char **schema, siteinfo_t **s, int verbose) {
  if (schema && *schema) *schema[0] = '\0';
  if (s) *s = NULL;
  /* make this header optional */
  if (do_mw_header(f, skipschema, schema, verbose)) {
    if (get_line(f) == NULL) {
      fprintf(stderr,"abrupt end to content\n");
      return(1);
    }
  }
  /* make this part optional */
  if (do_siteinfo(f, s, verbose)) {
    if (get_line(f) == NULL) {
      fprintf(stderr,"abrupt end to content\n");
      return(1);
    }
  }
  return(0);
}
