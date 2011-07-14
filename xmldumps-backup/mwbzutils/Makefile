# ------------------------------------------------------------------
# This Makefile builds binaries which rely on three source files
# from libbzip2 version 1.0.6. (See bz2libfuncs.c, bzlib.h and 
# bzlib_private.h; the first is slightly modified while the 
# second is unchanged from the library version.)
#
# The copyright for those two files  is as follows:
#
# bzip2/libbzip2 version 1.0.6 of 6 September 2010
# Copyright (C) 1996-2010 Julian Seward <jseward@bzip.org>
#
# Those files are released under the terms of the license contained
# in the file LICENSE_BZ.
#
# All other files are released under the GPL, copyright (C) Ariel T. Glenn
# 2010-2010: see the file COPYING for details.
# ------------------------------------------------------------------

CC=gcc
LDFLAGS=
BIGFILES=-D_FILE_OFFSET_BITS=64
CFLAGS=-Wall -Winline -O2 -g $(BIGFILES)
PREFIX=/usr/local

SHELL=/bin/sh

OBJSBZ= bzlibfuncs.o

all: checkforbz2footer \
	dumpbz2filefromoffset \
	dumplastbz2block \
	findpageidinbz2xml

dumplastbz2block: $(OBJSBZ) mwbzlib.o dumplastbz2block.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o dumplastbz2block dumplastbz2block.o mwbzlib.o  $(OBJSBZ) -lbz2

findpageidinbz2xml: $(OBJSBZ) mwbzlib.o httptiny.o findpageidinbz2xml.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o findpageidinbz2xml findpageidinbz2xml.o httptiny.o mwbzlib.o $(OBJSBZ) -lbz2 -lz

checkforbz2footer: $(OBJSBZ) mwbzlib.o checkforbz2footer.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o checkforbz2footer checkforbz2footer.o mwbzlib.o $(OBJSBZ) -lbz2

dumpbz2filefromoffset: $(OBJSBZ) mwbzlib.o dumpbz2filefromoffset.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o dumpbz2filefromoffset dumpbz2filefromoffset.o mwbzlib.o $(OBJSBZ) -lbz2

install: dumplastbz2block findpageidinbz2xml checkforbz2footer dumpbz2filefromoffset
	if ( test ! -d $(PREFIX)/bin ) ; then mkdir -p $(PREFIX)/bin ; fi
	cp -f dumplastbz2block $(PREFIX)/bin/dumplastbz2block
	cp -f findpageidinbz2xml $(PREFIX)/bin/findpageidinbz2xml
	cp -f checkforbz2footer $(PREFIX)/bin/checkforbz2footer
	cp -f dumpbz2filefromoffset $(PREFIX)/bin/dumpbz2filefromoffset
	chmod a+x $(PREFIX)/bin/dumplastbz2block
	chmod a+x $(PREFIX)/bin/findpageidinbz2xml
	chmod a+x $(PREFIX)/bin/checkforbz2footer
	chmod a+x $(PREFIX)/bin/dumpbz2filefromoffset

clean: 
	rm -f *.o *.a dumplastbz2block findpageidinbz2xml \
		checkforbz2footer dumpbz2filefromoffset

bzlibfuncs.o: bzlibfuncs.c bzlib.h bzlib_private.h
	$(CC) $(CFLAGS) -c bzlibfuncs.c
mwbzlib.o: mwbzlib.c bzlib.h bzlib_private.h mwbzutils.h
	$(CC) $(CFLAGS) -c mwbzlib.c
httptiny.o: httptiny.c
	$(CC) $(CFLAGS) -c httptiny.c
dumplastbz2block.o: dumplastbz2block.c
	$(CC) $(CFLAGS) -c dumplastbz2block.c
findpageidinbz2xml.o: findpageidinbz2xml.c
	$(CC) $(CFLAGS) -c findpageidinbz2xml.c
checkforbz2footer.o: checkforbz2footer.c
	$(CC) $(CFLAGS) -c checkforbz2footer.c
dumpbz2filefromoffset.o: dumpbz2filefromoffset.c
	$(CC) $(CFLAGS) -c dumpbz2filefromoffset.c

distclean: clean

DISTNAME=mwbzutils-0.0.2
dist: 
	rm -f $(DISTNAME)
	ln -s -f . $(DISTNAME)
	tar cvf $(DISTNAME).tar \
	   $(DISTNAME)/dumplastbz2block.c \
	   $(DISTNAME)/findpageidinbz2xml.c \
	   $(DISTNAME)/checkforbz2footer.c \
	   $(DISTNAME)/dumpbz2filefromoffset.c \
	   $(DISTNAME)/httptiny.c \
	   $(DISTNAME)/mwbzlib.c \
	   $(DISTNAME)/mwbzutils.h \
	   $(DISTNAME)/bzlibfuncs.c \
	   $(DISTNAME)/bzlib.h \
	   $(DISTNAME)/bzlib_private.h \
	   $(DISTNAME)/Makefile \
	   $(DISTNAME)/LICENSE_BZ \
	   $(DISTNAME)/COPYING \
	   $(DISTNAME)/README \
	   $(DISTNAME)/CHANGES
	gzip -v $(DISTNAME).tar

