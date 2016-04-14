#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>

typedef enum { None, StartHeader, EndHeader, StartPage, AtPageID, WriteMem, Write, EndPage, AtLastPageID } States;

/* assume the header is never going to be longer than 1000 x 80 4-byte characters... how many
   namespaces will one project want? */
#define MAXHEADERLEN 524289

void usage(char *message) {
  char * help =
"Usage: writeuptopageid [--version|--help]\n"
"   or: writeuptopageid <startpageid> <endpageid>\n\n"
"Reads a MediaWiki XML file from stdin anfd writes a range of pages from the file\n"
"to stdout, starting with and including the startpageid, up to but not including\n"
"the endpageid.\n"
"This program can be used in processing XML dump files that were only partially\n"
"written, as well as in writing partial stub files for reruns of those dump files.\n"
"If endPageID is ommitted, all pages starting from startPageID will be copied.\n\n"
"Options:\n\n"
"Flags:\n\n"
"  -h, --help       Show this help message\n"
"  -v, --version    Display the version of this program and exit\n\n"
"Arguments:\n\n"
"  <startpageid>   id of the first page to write\n"
"  <endpageid>     id of the page at which to stop writing; if omitted, all pages through eof\n"
"                   will be written\n\n"
"Report bugs in writeuptopageid to <https://phabricator.wikimedia.org/>.\n\n"
"See also checkforbz2footer(1), dumpbz2filefromoffset(1), dumplastbz2block(1),\n"
    "findpageidinbz2xml(1), recompressxml(1)\n\n";
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
  fprintf(stderr,"writeuptopageid %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

/* note that even if we have only read a partial line
   of text from the body of the page, (cause the text 
   is longer than our buffer), it's fine, since the 
   <> delimiters only mark xml, they can't appear
   in the page text. 

   returns new state */
States setState (char *line, States currentState, int startPageID, int endPageID) {
  int pageID = 0;

  if (currentState == EndHeader) {
    /* if we have junk after the header we don't write it.
     commands like dumpbz2filefromoffset can produce such streams. */
    if (strncmp(line,"<page>",6)) {
      return(None);
    }
  }

  if (!strncmp(line,"<mediawiki",10)) {
    return(StartHeader);
  }
  else if (!strncmp(line,"</siteinfo>",11)) {
    return(EndHeader);
  }
  else if (!strncmp(line,"<page>",6)) {
    return(StartPage);
  }
  /* there are also user ids, revision ids, etc... pageid will be the first one */
  else if (currentState == StartPage && (!strncmp(line, "<id>", 4))) {
    /* dig the id out, format is <id>num</id> */
    pageID = atoi(line+4);
    if (endPageID && (pageID >= endPageID)) {
      return(AtLastPageID);
    }
    else if (pageID >= startPageID) {
      return(WriteMem);
    }
    else {
      /* we don't write anything */
      return(None);
    }
  }
  else if (currentState == WriteMem) {
    return(Write);
  }
  else if (!strncmp(line, "</page>", 6)) {
    if (currentState == Write) {
      return(EndPage);
    }
    else {
      /* don't write anything */
      return(None);
    }
  }
  else if (!strncmp(line, "</mediawiki",11)) {
    return(None);
  }
  return(currentState);
}

/* returns 1 on success, 0 on error */
int writeMemoryIfNeeded(char *mem, States state) {
  int res = 0;

  if (state == WriteMem) {
    res = fwrite(mem,strlen(mem),1,stdout);
    return(res);
  }
}

void clearMemoryIfNeeded(char *mem, States state) {
  if (state == WriteMem || state == None) {
    mem[0]='\0';
  }
  return;
}

/* returns 1 on success, 0 on error */
int writeIfNeeded(char *line, States state) {
  if (state == StartHeader || state == EndHeader || state == WriteMem || state == Write || state == EndPage) {
    return(fwrite(line,strlen(line),1,stdout));
  }
}

/*  returns 1 on success, 0 on error */
int saveInMemIfNeeded(char *mem, char *line, States state) {
  if (state == StartPage) {
    if (strlen(mem) + strlen(line) < MAXHEADERLEN) {
      strcpy(mem + strlen(mem),line);
    }
    else {
      /* we actually ran out of room, who knew */
      return(0);
    }
  }
  return(1);
}

int main(int argc,char **argv) {
  long int startPageID = 0;
  long int endPageID = 0;
  char *nonNumeric = 0;
  States state = None;
  char *text;
  char line[4097];
  /* order of magnitude of 2K lines of 80 chrs each,
     no header of either a page nor the mw header should
     ever be longer than that. At least not for some good 
     length of time. */
  char mem[MAXHEADERLEN];

  int optc;
  int optindex=0;

  struct option optvalues[] = {
    {"help", 0, 0, 'h'},
    {"version", 0, 0, 'v'},
    {NULL, 0, NULL, 0}
  };

  if (argc < 2 || argc > 3) {
    usage(NULL);
    exit(-1);
  }

  while (1) {
    optc=getopt_long_only(argc,argv,"hv", optvalues, &optindex);
    if (optc=='h')
      usage(NULL);
    else if (optc=='v')
      show_version(VERSION);
    else if (optc==-1) break;
    else usage("Unknown option or other error\n");
  }

  if (optind >= argc) {
    usage("Missing filename argument.");
  }

  errno = 0;
  startPageID = strtol(argv[optind], &nonNumeric, 10);
  if (startPageID == 0 || 
      *nonNumeric != 0 ||
      nonNumeric == (char *) &startPageID || 
      errno != 0) {
    usage("The value you entered for startPageID must be a positive integer.");
    exit(-1);
  }
  optind++;
  if (optind < argc) {
    endPageID = strtol(argv[optind], &nonNumeric, 10);
    if (endPageID == 0 || 
	*nonNumeric != 0 ||
	nonNumeric == (char *) &endPageID || 
	errno != 0) {
      usage("The value you entered for endPageID must be a positive integer.\n");
      exit(-1);
    }
  }
  
  while (fgets(line, sizeof(line)-1, stdin) != NULL) {
    text=line;
    while (*text && isspace(*text))
      text++;
    state = setState(text, state, startPageID, endPageID);
    if (!saveInMemIfNeeded(mem,line,state)) {
      fprintf(stderr,"failed to save text in temp memory, bailing\n");
      exit(-1);
    };
    if (!writeMemoryIfNeeded(mem,state)) {
      fprintf(stderr,"failed to write text from memory, bailing\n");
      exit(-1);
    }
    clearMemoryIfNeeded(mem,state);
    if (!writeIfNeeded(line,state)) {
      fprintf(stderr,"failed to write text, bailing\n");
      exit(-1);
    }
    if (state == AtLastPageID) {
      /* we are done. */
      break;
    }
  }
  fwrite("</mediawiki>\n",13,1,stdout);
  exit(0);
}

