# ------------------------------------------------------------------
# This project is released under the GPL, copyright (C) Ariel T. Glenn
# 2013: see the file COPYING for details.
# ------------------------------------------------------------------

CC=gcc
LDFLAGS=
BIGFILES=-D_FILE_OFFSET_BITS=64
CFLAGS=-Wall  -O2 $(BIGFILES)
#CFLAGS=-Wall  -g $(BIGFILES)
PREFIX=/usr/local

SHELL=/bin/sh

all: mwxml2sql sql2txt

mwxml2sql: mwxml2sql.o filebuffers.o xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o mwxml2sql mwxml2sql.o filebuffers.o \
	    xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o -lbz2 -lz

sql2txt: sql2txt.o filebuffers.o sqlutils.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o sql2txt sql2txt.o filebuffers.o sqlutils.o \
	    -lbz2 -lz

install: mwxml2sql sql2txt
	if ( test ! -d $(PREFIX)/bin ) ; then mkdir -p $(PREFIX)/bin ; fi
	cp -f mwxml2sql $(PREFIX)/bin/mwxml2sql
	cp -f sql2txt $(PREFIX)/bin/sql2txt
	chmod a+x $(PREFIX)/bin/mwxml2sql
	chmod a+x $(PREFIX)/bin/sql2txt

clean: 
	rm -f *.o *.a mwxml2sql sql2txt

static: mwxml2sql_static sql2txt_static

mwxml2sql_static: mwxml2sql
	$(CC) $(CFLAGS) -static -static-libgcc -o mwxml2sql_static mwxml2sql.o filebuffers.o \
	    xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o -L. -lbz2 -lz

sql2txt_static: sql2txt
	$(CC) $(CFLAGS) -static -static-libgcc -o sql2txt_static sql2txt.o filebuffers.o sqlutils.o \
	    sha1.o -L. -lbz2 -lz

mwxml2sql.o: mwxml2sql.c mwxml2sql.h
	$(CC) $(CFLAGS) -c mwxml2sql.c

sql2txt.o: sql2txt.c mwxml2sql.h
	$(CC) $(CFLAGS) -c sql2txt.c

filebuffers.o: filebuffers.c mwxml2sql.h
	$(CC) $(CFLAGS) -c filebuffers.c

xmltags.o: xmltags.c mwxml2sql.h
	$(CC) $(CFLAGS) -c xmltags.c

mwxmlelts.o: mwxmlelts.c mwxml2sql.h
	$(CC) $(CFLAGS) -c mwxmlelts.c

sqlutils.o: sqlutils.c mwxml2sql.h
	$(CC) $(CFLAGS) -c sqlutils.c

base36.o: base36.c
	$(CC) $(CFLAGS) -c base36.c

sha1.o: sha1.c sha1.h
	$(CC) $(CFLAGS) -c sha1.c

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
	   $(DISTNAME)/sql2txt.c \
	   $(DISTNAME)/sqlutils.c \
	   $(DISTNAME)/xmltags.c \
	   $(DISTNAME)/mwxmlelts.c \
	   $(DISTNAME)/sha1.h \
	   $(DISTNAME)/sha1.c \
	   $(DISTNAME)/base36.c \
	   $(DISTNAME)/Makefile \
	   $(DISTNAME)/COPYING \
	   $(DISTNAME)/README \
	   $(DISTNAME)/CHANGELOG
	gzip -v $(DISTNAME).tar

