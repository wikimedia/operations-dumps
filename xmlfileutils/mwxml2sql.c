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
   to be resolved:

    optimizations (is read/write per line the best way?
     have lots of dup code around 'read a line and whine or not')

    recommend gz and give size estimates, this seems to be
    reasonable speed

    normalize all struct field names

    find_value and find_attrs can now return non null terminated strings
    that fill the holder. this is good (otherwise we cn't tell if we got
    a partial value back) but caller doesn't handle this

    unicode char can get truncated if we read in chunks only of the line
    commnts are 256 char max so unicode chars here *may have already been
    truncated* and we must account for this, see mwdumper

    reasonable error recovery from various stages of the parsing
*/

/*
   args:
     whoami          name of calling program
     version_string  version of calling program

   this function displays version information for the calling program
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
  fprintf(stderr,"mwxml2sql %s\n", version_string);
  fprintf(stderr,"Supported input schema versions: 0.4 through 0.8.\n");
  fprintf(stderr,"Supported output MediaWiki versions: 1.5 through 1.21.\n\n");
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

/*
  args:
     mwv        list of structures with mw version info

  this function frees a list of mediawiki version information
*/
void free_mw_version(mw_version_t *mwv) {
  mw_version_t *next;

  while (mwv) {
    next = mwv->next;
    if (mwv->version) free(mwv->version);
    free(mwv);
    mwv = next;
  }
  return;
}

/*
  args:
     specified       comma-separated list of mw version numbers
                     example: 1.5,1.18,1.20

     returns:
         filled in list of structures representing those versions
         or NULL on error
*/
mw_version_t *check_mw_version(char *specified) {
  mw_version_t *mwv = NULL, *head = NULL, *current = NULL;
  char *comma = NULL;
  char *start = NULL;
  int last= 0;

  if (!specified) return(NULL);
  start = specified;

  while (!last) {
    mwv = (mw_version_t *)malloc(sizeof(mw_version_t));
    if (!mwv) {
      fprintf(stderr,"Failed to get memory for mediawiki version check\n");
      exit(1);
    }
    if (!head) head = mwv;  /* first structure in list */
    else current->next = mwv;  /* appending to list */
    mwv->major = 0;
    mwv->minor = 0;
    mwv->qualifier[0] = '\0';
    mwv->next = NULL;
    mwv->version = NULL;

    comma = strchr(start, ',');
    if (comma) *comma = '\0';
    else last++;
    /* we know MW 1.5 through MW 1.21 even though there is no MW 1.21 yet */
    sscanf(start, "%u.%u%20s", &mwv->major, &mwv->minor, mwv->qualifier);
    if (mwv->major != 1 || mwv->minor < 5 || mwv->minor > 21) {
      free_mw_version(mwv);
      return(NULL);
    }

    mwv->version = (char *)malloc(strlen(start) + 1);
    if (!mwv->version) {
      fprintf(stderr,"Failed to get memory for mediawiki version check\n");
      exit(1);
    }
    strcpy(mwv->version, start);

    if (comma) start = comma + 1; /* otherwise last is set and we'll be out */
    current = mwv;
  }
  /* FIXME we should find and complain about dup version strings */
  return(head);
}

/*
   args:
     whoami    name of calling program
     message   message to print out before usage information, if any
               this should not end in a newline

   this function prints usage information for the program to stdout
*/
void usage(char *whoami, char *message) {
  char * help =
"Usage: mwxml2sql [OPTIONS] <-m|--mediawiki versionstring> <-s|--stubs filename>\n\n"
"Mwxml2sql reads a possibily compressed stream of MediaWiki XML pages and\n"
"writes possibly compressed SQL files containing INSERT statements for\n"
"page, revision, and text tables.\n\n"
"Options:\n\n"
"Mandatory arguments:\n\n"
"  -m, --mediawiki version-string\n"
"        Version of MediaWiki for which to output SQL. Supported versions\n"
"        are shown by the --version option. The version-string is used to\n"
"        generate the names of the SQL files for the page, revision, and\n"
"        text content.\n"
"  -s, --stubs filename\n"
"        Name of `stub-articles' XML dump file. If a filename is specified\n"
"        that ends in .gz or .bz2, the file will silently be decompressed.\n\n"
"Optional arguments:\n\n"
"  -t, --text filename\n"
"        Name of `pages-articles' XML dump file. If a filename is specified\n"
"        that ends in .gz or .bz2, the file will silently be decompressed.\n"
"        If not specified, no pages-articles file will be read and no text\n"
"        SQL file will be written.\n"
"  -f, --mysqlfile filename-template\n"
"        Filename (possibly ending in .gz or .bz2 or .txt) which will be\n"
"        used as a template to generate the names of the SQL files for the\n"
"        page, revision, and text content.  If the filename-template is \n"
"        enwiki-yyyymmdd.sql, then the SQL output will be written to\n"
"        enwiki-yyyymmdd-page.sql-1.19, enwiki-yyyymmdd-revision.sql-1.19,\n"
"        and enwiki-yyyymmdd-text.sql-1.19.  If none is specified, all data\n"
"        will be written to stdout. Note however that because the INSERT\n"
"        statements are batched on the assumption that they will be put\n"
"        to three separate files, this might not be what you want.\n"
"        Use this if you want to keep the existing data and are importing\n"
"        changes that have been made to the original site since then.\n"
"  -p, --tableprefix string\n"
"        If your database has this prefix before all table names, it will\n"
"        be prepended to all table names in the SQL output.\n\n"
"Flags:\n\n"
"  -c, --compress\n"
"        Compress text revisions in the SQL output (requires the 'text'\n"
"        option). If this option is not set, the CREATE TABLE statement for\n"
"        the 'text' table will include parameters for InnoDB table-based\n"
"        compression.\n"
"  -h, --help\n"
"        Show summary of options; and exit\n"
"  -n, --nodrop\n"
"        Do not write DROP TABLE IF EXISTS statement before the CREATE\n"
"        TABLE statement in the SQL output; but do write INSERT IGNORE\n"
"        statements rather than plain INSERT statements.\n"
"  -v, --verbose\n"
"        Produce debugging output to stderr. This option can be used\n"
"        multiple times to increase verbosity.\n"
"  -V, --version\n"
"        Write version information to stderr; and exit.\n\n"
"Report bugs in mwxml2sql to <https://bugzilla.wikimedia.org/>.\n\n"
"See also sql2txt(1), sqlfilter(1).\n\n";
  if (message) {
    fprintf(stderr,"%s\n\n",message);
  }
  fprintf(stderr,"%s",help);
  exit(-1);
}

int main(int argc, char **argv) {
  int optindex=0;
  int optc = 0;
  int result;

  int help = 0;
  int version = 0;
  int nodrop = 0;
  int verbose = 0;
  int text_compress = 0;

  char *stubs_file = NULL; /* cntains stub xml */
  char *text_file = NULL; /* contains xml with revision content */

  char *mysql_file = NULL; /* base and suffix of the mysql output files */

  input_file_t *stubs = NULL;
  input_file_t *text = NULL;

  output_file_t *mysql_createtables = NULL;
  output_file_t *mysql_page = NULL;
  output_file_t *mysql_revs = NULL;
  output_file_t *mysql_text = NULL;

  char mysql_createtables_file[FILENAME_LEN];
  char mysql_page_file[FILENAME_LEN];
  char mysql_revs_file[FILENAME_LEN];
  char mysql_text_file[FILENAME_LEN];
  char *filebase = NULL;
  char *filesuffix = NULL;

  char *mw_version = NULL;
  mw_version_t *mwv = NULL;

  int pages_done = 0;
  int eof = 0;
  
  char *table_prefix =  NULL;
  tablenames_t *tables = NULL;

  char *start_page_id = NULL;

  char *stubs_schema = NULL;
  siteinfo_t *s_info = NULL;

  struct option optvalues[] = {
    {"compress", no_argument, NULL, 'c'},
    {"help", no_argument, NULL, 'h'},
    {"mysqlfile", required_argument, NULL, 'f'},
    {"mediawiki", required_argument, NULL, 'm'},
    {"nodrop", no_argument, NULL, 'n'},
    {"pageid", required_argument, NULL, 'i'},
    {"stubs", required_argument, NULL, 's'},
    {"tableprefix", required_argument, NULL, 'p'},
    {"text", required_argument, NULL, 't'},
    {"verbose", no_argument, NULL, 'v'},
    {"version", no_argument, NULL, 'V'},
    {NULL, 0, NULL, 0}
  };

  while (1) {
    optc=getopt_long(argc,argv,"cf:hi:m:np:s:t:vV", optvalues, &optindex);
    if (optc==-1) break;

    switch(optc) {
    case 'c':
      text_compress++;
      break;
    case 'f':
      mysql_file = optarg;
      break;
    case 'h':
      help++;
      break;
    case 'i':
      start_page_id = optarg;
      break;
    case 'm':
      mw_version = optarg;
      break;
    case 'n':
      nodrop++;
      break;
    case 'p':
      table_prefix = optarg;
      break;
    case 's':
      stubs_file = optarg;
      break;
    case 't':
      text_file = optarg;
      break;
    case 'v':
      verbose++; 
      break;
    case 'V':
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

  if (text_compress && !text_file) {
    usage(argv[0], "Compression of text revisions requires the text option be specified");
  }

  if (!stubs_file) {
    usage(argv[0], "stubs file not specified, this argument is mandatory.");
  }

  if (!mw_version) {
    usage(argv[0], "missing required 'mediawiki' option");
  }
  mwv = check_mw_version(mw_version);
  if (!mwv) {
    usage(argv[0], "bad 'mediawiki' option given");
  }

  stubs = init_input_file(stubs_file);
  if (!stubs) exit(1);

  if (text_file) {
    text = init_input_file(text_file);
    if (!text) exit(1);
  }

  if (mysql_file == NULL) {
    mysql_createtables = init_output_file(NULL, NULL, mwv);
    mysql_page = init_output_file(NULL, NULL, mwv);
    mysql_revs = init_output_file(NULL, NULL, mwv);
    if (text_file)
      mysql_text = init_output_file(NULL, NULL, mwv);
  }
  else {
    /* take apart the name if needed and shove in the prefix, then the suffix */
    filebase = get_filebase(mysql_file, verbose);
    filesuffix = get_filesuffix(mysql_file, verbose);

    sprintf(mysql_createtables_file, "%s-createtables.sql", filebase);
    sprintf(mysql_page_file, "%s-page.sql", filebase);
    sprintf(mysql_revs_file, "%s-revision.sql", filebase);

    mysql_createtables = init_output_file(mysql_createtables_file, filesuffix, mwv);
    mysql_page = init_output_file(mysql_page_file, filesuffix, mwv);
    mysql_revs = init_output_file(mysql_revs_file, filesuffix, mwv);

    if (text_file) {
      sprintf(mysql_text_file, "%s-text.sql", filebase);
      mysql_text = init_output_file(mysql_text_file, filesuffix, mwv);
    }
    
    if (verbose) fprintf(stderr,"opened sql output files\n");
  }

  if (verbose) fprintf(stderr,"Input and output files opened\n");

  tables = setup_table_names(table_prefix);
  if (!tables) {
    fprintf(stderr,"failed to set up table prefix\n");
    exit(1);
  };

  /* if we compress text blobs then don't request innodb table compression,
     otherwise we want it */
  write_createtables_file(mysql_createtables, nodrop, !text_compress, tables);
  close_output_file(mysql_createtables);
  if (verbose) fprintf(stderr,"Create tables sql file written, beginning scan of xml\n");

  srand48((long int)time(NULL)); /* need this for page_random */
  init_mwxml(); /* do this before any do_* calls */

  if (get_line(stubs) == NULL) {
    fprintf(stderr,"abrupt end to content\n");
    return(1);
  }
  result = do_file_header(stubs, 0, &stubs_schema, &s_info, verbose);
  if (result) {
    fprintf(stderr,"error encountered scanning stubs file header\n");
    exit(1);
  }

  if (text) {
    if (get_line(text) == NULL) {
      fprintf(stderr,"abrupt end to content\n");
      exit(1);
    }
    result = do_file_header(text, 1, NULL, NULL, verbose);
    if (result) {
      fprintf(stderr,"error encountered scanning text file header\n");
      exit(1);
    }
  }

  if (s_info) {
    write_metadata(mysql_page, stubs_schema, s_info);
    write_metadata(mysql_revs, stubs_schema, s_info);
    if (text)
      write_metadata(mysql_text, stubs_schema, s_info);
  }

  while (! eof) {
    result = do_page(stubs, text, text_compress, mysql_page, mysql_revs, mysql_text, s_info, verbose, tables, nodrop, start_page_id);
    if (!result) break;
    pages_done++;
    if (verbose && !(pages_done%1000)) fprintf(stderr,"%d pages processed\n", pages_done);
    if (get_line(stubs) == NULL) eof++;
  }

  if (verbose) {
    fprintf(stderr,"pages processed: %d\n", pages_done);
  }

  close_input_file(stubs);
  free_input_file(stubs);

  if (text) {
    close_input_file(text);
    free_input_file(text);
  }

  cleanup_mwxml(mysql_page, mysql_revs, mysql_text);

  close_output_file(mysql_page);
  close_output_file(mysql_revs);

  free_output_file(mysql_createtables);
  free_output_file(mysql_page);
  free_output_file(mysql_revs);

  if (text) {
    close_output_file(mysql_text);
    free_output_file(mysql_text);
  }

  free(tables);

  exit(0);
}
