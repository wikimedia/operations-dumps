# ------------------------------------------------------------------
# This project is released under the GPL, copyright (C) Ariel T. Glenn
# 2013: see the file COPYING for details.
# ------------------------------------------------------------------

CC=gcc
LDFLAGS=
BIGFILES=-D_FILE_OFFSET_BITS=64
CFLAGS=-Wall -Winline -O2 $(BIGFILES)
#CFLAGS=-Wall -Winline -g $(BIGFILES)
PREFIX=/usr/local

SHELL=/bin/sh

all: mwxml2sql

mwxml2sql: mwxml2sql.o filebuffers.o xmltags.o mwxmlelts.o sqlutils.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o mwxml2sql mwxml2sql.o filebuffers.o \
	    xmltags.o mwxmlelts.o sqlutils.o -lssl -lcrypto -lbz2 -lz

install: mwxml2sql
	if ( test ! -d $(PREFIX)/bin ) ; then mkdir -p $(PREFIX)/bin ; fi
	cp -f mwxml2sql $(PREFIX)/bin/mwxml2sql
	chmod a+x $(PREFIX)/bin/mwxml2sql

clean: 
	rm -f *.o *.a mwxml2sql

mwxml2sql.o: mwxml2sql.c mwxml2sql.h
	$(CC) $(CFLAGS) -c mwxml2sql.c

filebuffers.o: filebuffers.c mwxml2sql.h
	$(CC) $(CFLAGS) -c filebuffers.c

xmltags.o: xmltags.c mwxml2sql.h
	$(CC) $(CFLAGS) -c xmltags.c

mwxmlelts.o: mwxmlelts.c mwxml2sql.h
	$(CC) $(CFLAGS) -c mwxmlelts.c

sqlutils.o: sqlutils.c mwxml2sql.h
	$(CC) $(CFLAGS) -c sqlutils.c

distclean:
	rm -f $(DISTNAME)
	rm -f *.tar.gz

DISTNAME=mwxml2sql-0.0.1
dist: 
	rm -f $(DISTNAME)
	ln -s -f . $(DISTNAME)
	tar cvf $(DISTNAME).tar \
	   $(DISTNAME)/mwxml2sql.h \
	   $(DISTNAME)/filebuffers.c \
	   $(DISTNAME)/mwxml2sql.c \
	   $(DISTNAME)/sqlutils.c \
	   $(DISTNAME)/xmltags.c \
	   $(DISTNAME)/Makefile \
	   $(DISTNAME)/COPYING \
	   $(DISTNAME)/README \
	   $(DISTNAME)/CHANGELOG
	gzip -v $(DISTNAME).tar

