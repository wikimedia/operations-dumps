#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include "mwbzutils.h"

void usage(char *message) {
  char * help =
"Usage: checkforbz2footer [--version|--help]\n"
"   or: checkforbz2footer <infile>\n\n"
"Check whether the specified bzip2 compressed file ends with a bz2 footer\n"
"or not ((i.e. if it is truncated or corrupted).\n"
"This is a crude but fast test for integrity; we don't check the CRC at\n"
"the end of the stream, nor do we check the bit padding in the last byte\n"
"of the file.\n\n"
"Exits with 0 if the file has the bz2 footer, 1 if the file does not have\n"
"the footer and -1 on error.\n\n"
"Options:\n\n"
"Flags:\n\n"
"  -h, --help       Show this help message\n"
"  -v, --version    Display the version of this program and exit\n\n"
"Arguments:\n\n"
"  <infile>         Name of the file to check\n\n"
"Report bugs in checkforbz2footer to <https://bugzilla.wikimedia.org/>.\n\n"
"See also:\n\n"
"  dumpbz2filefromoffset(1), dumplastbz2block(1), findpageidinbz2xml(1)\n"
"  recompressxml(1), writeuptopageid(1)\n\n";
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
  fprintf(stderr,"checkforbz2footer %s\n", version_string);
  fprintf(stderr,"%s",copyright);
  exit(-1);
}

int main(int argc, char **argv) {

  int fin;
  int result;
  bz_info_t bfile;

  int optc;
  int optindex=0;

  struct option optvalues[] = {
    {"help", 0, 0, 'h'},
    {"version", 0, 0, 'v'},
    {NULL, 0, NULL, 0}
  };

  if (argc != 2) {
    usage("Missing option or argument.");
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

  fin = open (argv[optind], O_RDONLY);
  if (fin < 0) {
    fprintf(stderr,"failed to open file %s for read\n", argv[optind]);
    exit(-1);
  }

  bfile.footer = init_footer();
  result = check_file_for_footer(fin, &bfile);
  close(fin);
  if (result == -1) {
    return(result);
  }
  else {
    return(0);
  }
}

