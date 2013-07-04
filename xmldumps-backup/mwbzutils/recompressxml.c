#include <unistd.h>
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
#include "bzlib.h"

char inBuf[4096];
char outBuf[8192];

char inBuf_indx[4096];
char outBuf_indx[8192];

char *pageOpenTag = "<page>\n";

char *pageTitleExpr = "<title>(.+)</title>\n";
regmatch_t *matchPageTitleExpr;
regex_t compiledMatchPageTitleExpr;

char *idExpr = "<id>([0-9]+)</id>\n";
regmatch_t *matchIdExpr;
regex_t compiledMatchIdExpr;

bz_stream strm_indx;

void usage(char *message) {
  char * help =
"Usage: recompressxml --pagesperstream n [--buildindex filename] [--verbose]\n"
"   or: recompressxml [--version|--help]\n\n"
"Reads a stream of XML pages from stdin and writes to stdout the bz2 compressed\n"
"data, one bz2 stream (header, blocks, footer) per specified number of pages.\n\n"
"Options:\n\n"
"  -p, --pagesperstream:  Compress this number of pages in each complete\n"
"                         bz2stream before opening a new stream.  The siteinfo\n"
"                         header is written to a separate stream at the beginning\n"
"                         of all output, and the closing mediawiki tag is written\n"
"                         into a separate stream at the end.\n"
"  -b, --buildindex:      Generate a file containing an index of pages ids and titles\n"
"                         per stream.  Each line contains: offset-to-stream:pageid:pagetitle\n"
"                         If filename ends in '.bz2' the file will be written in bz2 format.\n"
"  -v, --verbose:         Write lots of debugging output to stderr.  This option can be used\n"
"                         multiple times to increase verbosity.\n";
"  -h, --help             Show this help message\n"
"  -V, --version          Display the version of this program and exit\n\n"
"Report bugs in checkforbz2footer to <https://bugzilla.wikimedia.org/>.\n\n"
"See also checkforbz2footer(1), dumpbz2filefromoffset(1), dumplastbz2block(1),\n"
"findpageidinbz2xml(1), writeuptopageid(1)\n\n";
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
  fprintf(stderr,"recompressxml %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

void setupIndexBz2Stream() {
  int bz_verbosity = 0;
  int bz_workFactor = 0;
  int bz_blockSize100k = 9;

  strm_indx.bzalloc = NULL;
  strm_indx.bzfree = NULL;
  strm_indx.opaque = NULL;

  /* init bzip compression stuff */
  BZ2_bzCompressInit(&(strm_indx), bz_blockSize100k, bz_verbosity, bz_workFactor);
}

void setupRegexps() {
  matchPageTitleExpr = (regmatch_t *)malloc(sizeof(regmatch_t)*2);
  regcomp(&compiledMatchPageTitleExpr, pageTitleExpr, REG_EXTENDED);
  matchIdExpr = (regmatch_t *)malloc(sizeof(regmatch_t)*2);
  regcomp(&compiledMatchIdExpr, idExpr, REG_EXTENDED);
  return;
}

int startsPage(char *buf) {
  while (*buf == ' ') buf++;

  if (!strcmp(buf,pageOpenTag)) return 1;
  else return 0;
}

char *hasPageTitle(char *buf) {
  static char pageTitle[513];
  int length = 0;

  pageTitle[0]='\0';

  while (*buf == ' ') buf++;

  if (regexec(&compiledMatchPageTitleExpr, buf,  2,  matchPageTitleExpr, 0 ) == 0) {
    if (matchPageTitleExpr[1].rm_so >=0) {
      length = matchPageTitleExpr[1].rm_eo - matchPageTitleExpr[1].rm_so;
      if (length > 512) {
	fprintf(stderr,"Page title length > 512 bytes... really? Bailing.\n");
	exit(1);
      }
      strncpy(pageTitle,buf+matchPageTitleExpr[1].rm_so, length);
      pageTitle[length] = '\0';
    }
  }
  return(pageTitle);
}

int hasId(char *buf) {
  int id = 0;

  while (*buf == ' ') buf++;

  if (regexec(&compiledMatchIdExpr, buf,  2,  matchIdExpr, 0 ) == 0) {
    if (matchIdExpr[1].rm_so >=0) {
      id = atoi(buf+matchIdExpr[1].rm_so);
    }
  }
  return(id);
}

int endsXmlBlock(char *buf, int header) {
  char *pageCloseTag = "</page>\n";
  char *mediawikiCloseTag = "</mediawiki>\n";
  char *siteinfoCloseTag = "</siteinfo>\n";

  while (*buf == ' ') buf++;

  /* if we are trying to process the header, check for that only */
  if (header) {
    if (!strcmp(buf,siteinfoCloseTag)) return 1;
    else return 0;
  }

  /* normal check for end of page, end of content */  
  if (!strcmp(buf,pageCloseTag) || !strcmp(buf,mediawikiCloseTag)) return 1;
  else return 0;
}

off_t endBz2Stream(bz_stream *strm, char *outBuf, int bufSize, FILE *fd) {
  int result;
  off_t offset;

  do {
    strm->avail_in = 0;
    result = BZ2_bzCompress ( strm, BZ_FINISH );
    fwrite(outBuf,bufSize-strm->avail_out,1,fd);
    strm->next_out = outBuf;
    strm->avail_out = 8192;
  } while (result != BZ_STREAM_END);
  offset = (off_t)strm->total_out_lo32 | ((off_t)strm->total_out_hi32 << 32);
  BZ2_bzCompressEnd(strm);
  return(offset);
}

void writeCompressedXmlBlock(int header, int count, off_t *fileOffset, FILE *indexfd, int indexcompressed, int verbose)
 {
  bz_stream strm;
  int bz_verbosity = 0;
  int bz_workFactor = 0;
  int bz_blockSize100k = 9;
  int wroteSomething = 0;
  int blocksDone = 0;

  strm.bzalloc = NULL;
  strm.bzfree = NULL;
  strm.opaque = NULL;

  char *pageTitle = NULL;
  int pageId = 0;
  enum States{WantPage,WantPageTitle,WantPageId};
  int state = WantPage;

  /* init bzip compression stuff */
  BZ2_bzCompressInit(&strm, bz_blockSize100k, bz_verbosity, bz_workFactor);

  while (fgets(inBuf, sizeof(inBuf), stdin) != NULL) {
    if (verbose > 1) {
      fprintf(stderr,"input buffer is: ");
      fprintf(stderr,"%s",inBuf);
    }

    wroteSomething = 1;
    /* add the buffer content to stuff to be compressed */
    strm.next_in = inBuf;
    strm.avail_in = strlen(inBuf);
    strm.next_out = outBuf;
    strm.avail_out = 8192;

    /* we are to build an index. */
    if (indexfd) {
      if (verbose > 2) {
	fprintf(stderr,"doing index check\n");
      }
      if (state == WantPage) {
	if (verbose > 2) {
	  fprintf(stderr,"checking for page tag\n");
	}
	if (startsPage(inBuf)) {
	  state = WantPageTitle;
	}
      }
      else if (state == WantPageTitle) {
	if (verbose > 1) {
	  fprintf(stderr,"checking for page title tag\n");
	}
	pageTitle = hasPageTitle(inBuf);
	if (pageTitle[0]) {
	  state = WantPageId;
	}
      }
      else if (state == WantPageId) {
	if (verbose > 1) {
	  fprintf(stderr,"checking for page id tag\n");
	}
	pageId = hasId(inBuf);
	if (pageId) {
	  state = WantPage;
	  if (indexcompressed) {
	    if (verbose) {
	      fprintf(stderr,"writing line to compressed index file\n");
	    }
	    sprintf(inBuf_indx,"%"PRId64":%d:%s\n",*fileOffset,pageId,pageTitle);
	    strm_indx.next_in = inBuf_indx;
	    strm_indx.avail_in = strlen(inBuf_indx);
	    do {
	      if (verbose > 2) {
		fprintf(stderr,"bytes left to read for index compression: %d\n",strm_indx.avail_in);
	      }
	      strm_indx.next_out = outBuf_indx;
	      strm_indx.avail_out = 8192;
	      BZ2_bzCompress ( &strm_indx, BZ_RUN );
	      fwrite(outBuf_indx,sizeof(outBuf_indx)-strm_indx.avail_out,1,indexfd);
	    } while (strm_indx.avail_in >0);
	  }
	  else {
	    if (verbose) {
	      fprintf(stderr,"writing line to index file\n");
	    }
	    fprintf(indexfd,"%"PRId64":%d:%s\n",*fileOffset,pageId,pageTitle);
	  }
	  pageId = 0;
	  pageTitle = NULL;
	}
      }
    }
    do {
      if (verbose > 2) {
	fprintf(stderr,"bytes left to read for text compression: %d\n",strm.avail_in);
      }
      strm.next_out = outBuf;
      strm.avail_out = 8192;
      BZ2_bzCompress ( &strm, BZ_RUN );
      fwrite(outBuf,sizeof(outBuf)-strm.avail_out,1,stdout);
    } while (strm.avail_in > 0);
    if (verbose > 1) fprintf(stderr,"avail_out is now: %d\n", strm.avail_out);

    if (endsXmlBlock(inBuf, header)) {
      /* special case: doing the siteinfo stuff at the beginning */
      if (verbose) {
	fprintf(stderr,"end of header found\n");
      }
      if (header) {
	*fileOffset += endBz2Stream(&strm, outBuf, sizeof(outBuf), stdout);
	return;
      }

      blocksDone++;
      if (blocksDone % count == 0) {
	if (verbose) fprintf(stderr, "end of xml block found\n");
	/* close down bzip stream, we are done with this block */
	*fileOffset += endBz2Stream(&strm, outBuf, sizeof(outBuf), stdout);
	return;
      }
    }
  }
  if (verbose) fprintf(stderr,"eof reached\n");
  if (wroteSomething) {
    /* close down bzip stream, we are done with this block */
    *fileOffset += endBz2Stream(&strm, outBuf, sizeof(outBuf), stdout);
  }
  return;
}

int main(int argc, char **argv) {
  int optindex=0;
  int optc;
  off_t offset;

  struct option optvalues[] = {
    {"buildindex", 1, 0, 'b'},
    {"help", 0, 0, 'h'},
    {"pagesperstream", 1, 0, 'p'},
    {"verbose", 0, 0, 'v'},
    {"version", 0, 0, 'V'},
    {NULL, 0, NULL, 0}
  };

  int count = 0;
  char *indexFilename = NULL;
  int verbose = 0;
  FILE *indexfd = NULL;
  int indexcompressed = 0;

  while (1) {
    optc=getopt_long_only(argc,argv,"p:b:v", optvalues, &optindex);
    if (optc=='b') {
      indexFilename = optarg;
    }
    else if (optc=='h')
      usage(NULL);
    else if (optc=='p') {
      if (!(isdigit(optarg[0]))) usage(NULL);
      count=atoi(optarg);
    }
    else if (optc=='v')
      verbose++;
    else if (optc=='V')
      show_version(VERSION);
    else if (optc==-1) break;
    else usage("unknown option or other error\n");
  }

  if (count <= 0) {
    usage("bad or no argument given for count.\n");
  }

  if (indexFilename) {
    if (verbose) {
      fprintf(stderr,"setting up index file creation.\n");
    }
    indexfd = fopen(indexFilename, "w");
    if (! indexfd) {
      usage("failed to open index file for write.\n");
    }
    if (!strcmp(indexFilename+(strlen(indexFilename)-4),".bz2")) {
      if (verbose) {
	fprintf(stderr,"index file will be bz2 compressed.\n");
      }
      indexcompressed++;
      setupIndexBz2Stream();
    }
  }

  setupRegexps();

  offset = (off_t)0;
  /* deal with the XML header */
  writeCompressedXmlBlock(1,count,&offset,indexfd,indexcompressed,verbose);

  while (!feof(stdin)) {
    writeCompressedXmlBlock(0,count,&offset,indexfd,indexcompressed,verbose);
  }

  if (indexFilename) {
    if (indexcompressed) {
      if (verbose) {
	fprintf(stderr,"closing bz2 index file stream.\n");
      }
      endBz2Stream(&strm_indx, outBuf_indx, sizeof(outBuf_indx), indexfd);
    }
    if (verbose) {
      fprintf(stderr,"closing index file.\n");
    }
    fclose(indexfd);
  }

  exit(0);

}
