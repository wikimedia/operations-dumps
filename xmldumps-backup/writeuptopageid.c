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
  fprintf(stderr,"Usage: %s startPageID endPageID\n",me);
  fprintf(stderr,"Copies the contents of an XML file starting with and including startPageID\n");
  fprintf(stderr,"and up to but not including endPageID. This program is used in processing XML\n");
  fprintf(stderr,"dump files that were only partially written, as well as in writing partial\n");
  fprintf(stderr,"stub files for reruns of those dump files.\n");
}

/* note that even if we have only read a partial line
   of text from the body of the page, (cause the text 
   is longer than our buffer), it's fine, since the 
   <> delimiters only mark xml, they can't appear
   in the page text. 

   returns new state */
States setState (char *line, States currentState, int startPageID, int endPageID) {
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
    if (pageID >= endPageID) {
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

  if (argc != 3) {
    usage(argv[0]);
    exit(-1);
  }

  errno = 0;
  startPageID = strtol(argv[1], &nonNumeric, 10);
  if (startPageID == 0 || 
      *nonNumeric != 0 ||
      nonNumeric == (char *) &startPageID || 
      errno != 0) {
    fprintf (stderr,"The value you entered for startPageID must be a positive integer.\n");
    usage(argv[0]);
    exit(-1);
  }
  endPageID = strtol(argv[2], &nonNumeric, 10);
  if (endPageID == 0 || 
      *nonNumeric != 0 ||
      nonNumeric == (char *) &endPageID || 
      errno != 0) {
    fprintf (stderr,"The value you entered for endPageID must be a positive integer.\n");
    usage(argv[0]);
    exit(-1);
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

