#ifndef _MWXML2SQL_H
#define _MWXML2SQL_H

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

#define VERSION "0.0.1"

#define MAX_TAG_NAME_LEN 256
#define MAX_ATTRS_STR_LEN 256
#define MAX_ID_LEN 15
#define TEXT_BUF_LEN 65536
/* #define TEXT_BUF_LEN 4096 */
/* length * 1.015  + 5.. plus a few for paranoia, used for gzip compression of TEXT_BUF_LEN data */
#define TEXT_BUF_LEN_PADDED 4200
#define FIELD_LEN 256

#define FILENAME_LEN 256

typedef struct {
  int major;
  int minor;
  char qualifier[20];
} mw_version_t;

/* holds string of characters, possibly binary data, which should be
   null-terminated
   any embedded nulls must be converted to the string "\0"
*/
typedef struct {
  char *content;
  int length;
} string_t;

typedef struct namespace {
  char key[FIELD_LEN];
  char n_case[FIELD_LEN];
  char namespace[FIELD_LEN];
  struct namespace *next;
} namespace_t;

typedef struct {
  char sitename[FIELD_LEN];
  char base[FIELD_LEN];
  char generator[FIELD_LEN];
  char s_case[FIELD_LEN];
  namespace_t *namespaces;
} siteinfo_t;

typedef struct {
  char username[FIELD_LEN];
  char ip[FIELD_LEN];
  char id[MAX_ID_LEN];
} contributor_t;

typedef struct {
  /* these fields are read from xml */
  char id[MAX_ID_LEN];
  char parent_id[MAX_ID_LEN];
  char timestamp[FIELD_LEN];
  contributor_t *contributor;
  char minor[2];
  char comment[FIELD_LEN*2];
  char *text;
  char sha1[FIELD_LEN];
  char model[FIELD_LEN];   /* if not present, set to NULL */
  char format[FIELD_LEN];  /* if not present, set to NULL */
  char text_len[FIELD_LEN];
  char text_id[MAX_ID_LEN];

  char rev_deleted[2]; /* always "0" */
} revision_t;

typedef struct {
  char title[FIELD_LEN*2]; /* could be lots of escaped chars in here */
  char ns[FIELD_LEN];
  char id[MAX_ID_LEN];
  char redirect[2];
  char restrictions[FIELD_LEN];
  char touched[FIELD_LEN]; /* from rev_timestamp */
  char latest[MAX_ID_LEN];  /* from rev_id */
  char len[FIELD_LEN];     /* from text_len */
  char model[FIELD_LEN];   /* if not present, set to NULL */
  revision_t ** revs;
} page_t;

typedef struct {
  char buf[TEXT_BUF_LEN];
  int nextin;   /* pointer to next byte available for reading stuff in from file */
  int nextout;  /* pointer to next byte available for consumption by caller */
  int bytes_avail; /* number of bytes avail for consumption */
} bz2buffer_t;

#define PLAINTEXT 0x00
#define GZCOMPRESSED 0x01
#define BZCOMPRESSED 0x02

#define BZSUFFIX ".bz2"
#define GZSUFFIX ".gz"
#define TXTSUFFIX ".txt"

typedef struct {
  char *filename; /* expect mem from assignment not from alloc, if caller
			    does otherwise then caller must arrange to free as well */
  int filetype; /* one of PLAINTEXT, GZCOMPRESSED, BZCOMPRESSED */
  FILE *fd;
  gzFile gzfd;
  BZFILE *bz2fd;
  string_t *in_buf;
  char leftover[TEXT_BUF_LEN];
  bz2buffer_t *xmlb;
} input_file_t;

typedef struct {
  char *filename; /* expect mem from assignment not from alloc, if caller
			    does otherwise then caller must arrange to free as well */
  int filetype; /* one of PLAINTEXT, GZCOMPRESSED, BZCOMPRESSED */
  FILE *fd;
  gzFile gzfd;
  BZFILE *bz2fd;
  mw_version_t *mwv;
} output_file_t;

typedef struct {
  char page[80];
  char revs[80];
  char text[80];
} tablenames_t;

/* tags we recognize */
#define BASE "base"
#define CASE "case"
#define COMMENT "comment"
#define CONTRIBUTOR "contributor"
#define FORMAT "format"
#define GENERATOR "generator"
#define ID "id"
#define IP "ip"
#define MEDIAWIKI "mediawiki"
#define MINOR "minor"
#define MODEL "model"
#define NAMESPACE "namespace"
#define NAMESPACES "namespaces"
#define NS "ns"
#define PAGE "page"
#define PARENTID "parentid"
#define REDIRECT "redirect"
#define REVISION "revision"
#define RESTRICTIONS "restrictions"
#define SHA1 "sha1"
#define SITEINFO "siteinfo"
#define SITENAME "sitename"
#define TEXT "text"
#define TIMESTAMP "timestamp"
#define TITLE "title"
#define USERNAME "username"

/* macros for comparing mediawiki version numbers */
#define MWV_LESS(mwv,maj,min) (mwv->major < maj || (mwv->major == maj && mwv->minor < min))
#define MWV_GREATER(mwv,maj,min) (mwv->major > maj || (mwv->major == maj && mwv->minor > min))
#define MWV_EQUAL(mwv,maj,min) (mwv->major == maj && mwv->major == maj)

contributor_t *alloc_contributor();
void free_contributor(contributor_t *c);
revision_t *alloc_revision();
void free_revision(revision_t *r);
page_t *alloc_page();
void free_page();

void whine(char *message, ...);
void print_sql_field(FILE *f, char *field, int isstring, int islast);
void copy_sql_field(char *outbuf, char *field, int isstring, int islast);
int isfull(bz2buffer_t *b);
int fill_buffer(bz2buffer_t *b, BZFILE *fd);
int has_newline(bz2buffer_t *b);
void dump_bz2buffer(bz2buffer_t *b);
char *bz2gets(BZFILE *fd, bz2buffer_t *b, char *out, int nbytes);
char *get_line2buffer(input_file_t *f, char *buf, int length);
char *get_line(input_file_t *f);
int put_line(output_file_t *f, char *line);
char *un_xml_escape(char *value, char *output, int last);
int find_first_tag(input_file_t *f, char *holder, int holder_size);
int find_attrs(input_file_t *f, int result, char *holder, int holder_size);
int find_value(input_file_t *f, int s_ind, char *holder, int holder_size);
int find_close_tag(input_file_t *f, int start, char *holder, int holder_size);
int find_simple_close_tag(input_file_t *f, int start);
int get_start_tag(input_file_t *f, char *tag_name);
int get_elt_with_attrs(input_file_t *f, char *tag_name, char *holder, int holder_size, char *attrs, int attrs_size);
int get_end_tag(input_file_t *f, char *tag_name);

void init_mwxml();
void cleanup_mwxml(output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt);
int do_contributor(input_file_t *f, contributor_t *c, int verbose);
int find_rev_with_id(input_file_t *f, char *id);
int find_text_in_rev(input_file_t *f);
char *sql_escape(char *s, int s_size, char *out, int out_size);
int do_text(input_file_t *f,  output_file_t *sqlt, revision_t *r, int verbose, tablenames_t *t, int insrt_ignore, int get_sha1, int get_text_len, int text_commpress);
int do_revision(input_file_t *stubs, input_file_t *text, int text_compress, output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt, page_t *p, int verbose, tablenames_t *t, int insert_ignore);
void digits_only(char *buf);
int find_page_with_id(input_file_t *f, char *id);
int do_page(input_file_t *stubs, input_file_t *text, int text_compress, output_file_t *sqlp, output_file_t *sqlr, output_file_t *sqlt, int verbose, tablenames_t *t, int insert_ignore, char *start_page_id);
int do_namespace(input_file_t *f, namespace_t *n, int verbose);
int do_namespaces(input_file_t *f, siteinfo_t *s, int verbose);
int do_siteinfo(input_file_t *f, siteinfo_t **s, int verbose);
int do_mw_header(input_file_t *f, int skipschema, char **schema, int verbose);
void write_createtables_file(output_file_t *f, int nodrop, tablenames_t *t);
tablenames_t *setup_table_names(char *prefix);

void free_input_buffer(string_t *b);
string_t *init_input_buffer();
void free_bz2buf(bz2buffer_t *b);
bz2buffer_t *init_bz2buf();
void free_input_file(input_file_t *f);
void free_output_file(output_file_t *f);
input_file_t *init_input_file(char *xml_file);
output_file_t *init_output_file(char *xml_file, mw_version_t *mwv);
void close_input_file(input_file_t *f);
void close_output_file(output_file_t *f);

void show_version(char *whoami, char *version_string);
void usage(char *whoami, char *message);
char *get_filebase(char *file_name, int verbose);
char *get_filesuffix(char *file_name, int verbose);
int do_file_header(input_file_t *f, int skipschema, char **schema, siteinfo_t **s, int verbose);

char *gzipit(char *contents, int *compressed_length, char *gz_buf, int gz_buf_length);

void write_metadata(output_file_t *f, char *schema, siteinfo_t *s);
int get_attr( char *s, char *name, char *value, char **todo);
void title_escape(char *t);

#endif
