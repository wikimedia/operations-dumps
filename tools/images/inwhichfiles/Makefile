# ------------------------------------------------------------------
# This Makefile builds binaries which rely on an include file
# from the uthash package, namely uthash.h.
#
# See LICENSE.uthash for the license for that file, which is
# Copyright (c) 2005-2010, Troy D. Hanson    http://uthash.sourceforge.net
#
# All other files are released under the GPL, copyright (C) Ariel T. Glenn
# 2012: see the file COPYING for details.
# ------------------------------------------------------------------

CC=gcc
LDFLAGS=
BIGFILES=-D_FILE_OFFSET_BITS=64
CFLAGS=-Wall -Winline -O2 -g $(BIGFILES)
PREFIX=/usr/local

SHELL=/bin/sh

all: inwhichfiles

inwhichfiles: inwhichfiles.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o inwhichfiles inwhichfiles.o -lz

install: inwhichfiles
	if ( test ! -d $(PREFIX)/bin ) ; then mkdir -p $(PREFIX)/bin ; fi
	cp -f inwhichfiles $(PREFIX)/bin/inwhichfiles
	chmod a+x $(PREFIX)/bin/inwhichfiles

clean: 
	rm -f *.o *.a inwhichfiles
