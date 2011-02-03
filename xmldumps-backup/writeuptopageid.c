#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>

typedef enum { None, StartHeader, StartPage, AtPageID, WriteMem, Write, EndPage, AtLastPageID } States;

/* assume the header is never going to be longer than 1000 x 80 4-byte characters... how many
   namespaces will one project want? */
#define MAXHEADERLEN 524289

void usage(char *me) {
  fprintf(stderr,"Usage: %s pageID\n",me);
  fprintf(stderr,"Copies the contents of an XML file up to but not including\n");
  fprintf(stderr,"the specified pageID. This program is used in processing XML\n");
  fprintf(stderr,"dump files that were only partially written.\n");
}

/* note that even if we have only read a partial line
   of text from the body of the page, (cause the text 
   is longer than our buffer), it's fine, since the 
   <> delimiters only mark xml, they can't appear
   in the page text. 

   returns new state */
States setState (char *line, States currentState, int endPageID) {
  int pageID = 0;

  if (!strncmp(line,"<mediawiki",10)) {
    return(StartHeader);
  }
  else if (!strncmp(line,"<page>",6)) {
    return(StartPage);
  }
  /* there are also user ids, revision ids, etc... pageid will be the first one */
  else if (currentState == StartPage && (!strncmp(line, "<id>", 4))) {
    /* dig the id out, format is <id>num</id> */
    pageID = atoi(line+4);
    if (pageID == endPageID) {
      return(AtLastPageID);
    }
    else {
      return(WriteMem);
    }
  }
  else if (currentState == WriteMem) {
    return(Write);
  }
  else if (!strncmp(line, "</page>", 6)) {
    return(EndPage);
  }
  return(currentState);
}

/* returns 1 on success, 0 on error */
int writeMemoryIfNeeded(char *mem, States state) {
  int res = 0;

  if (state == WriteMem) {
    res = fwrite(mem,strlen(mem),1,stdout);
    mem[0]='\0';
    return(res);
  }
}

/* returns 1 on success, 0 on error */
int writeIfNeeded(char *line, States state) {
  if (state == StartHeader || state == WriteMem || state == Write || state == EndPage) {
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
  long int pageID = 0;
  char *nonNumeric = 0;
  States state = None;
  char *text;
  char line[4097];
  /* order of magnitude of 2K lines of 80 chrs each,
     no header of either a page nor the mw header should
     ever be longer than that. At least not for some good 
     length of time. */
  char mem[MAXHEADERLEN];

  if (argc != 2) {
    usage(argv[0]);
    exit(-1);
  }

  errno = 0;
  pageID = strtol(argv[1], &nonNumeric, 10);
  if (pageID == 0 || 
      *nonNumeric != 0 ||
      nonNumeric == (char *) &pageID || 
      errno != 0) {
    fprintf (stderr,"The value you entered for pageID must be a positive integer.\n");
    usage(argv[0]);
    exit(-1);
  }

  while (fgets(line, sizeof(line)-1, stdin) != NULL) {
    text=line;
    while (*text && isspace(*text))
      text++;
    state = setState(text, state, pageID);
    if (!saveInMemIfNeeded(mem,line,state)) {
      fprintf(stderr,"failed to save text in temp memory, bailing\n");
      exit(-1);
    };
    if (!writeMemoryIfNeeded(mem,state)) {
      fprintf(stderr,"failed to write text from memory, bailing\n");
      exit(-1);
    }
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

