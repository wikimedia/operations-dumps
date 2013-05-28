# ------------------------------------------------------------------
# This project is released under the GPL, copyright (C) Ariel T. Glenn
# 2013: see the file COPYING for details.
# ------------------------------------------------------------------

MWXML2SQLVERS  = "0.0.2"
PROGRAM        = mediawiki-mwxml2sql
DISTNAME       = $(PROGRAM)-$(MWXML2SQLVERS)
NAME_MWXML2SQL = "convert MediaWiki XML dump file to MySQL INSERT commands"
NAME_SQL2TXT   = "convert MySQL INSERT commands to tab-separated data"
NAME_SQLFILTER = "filter MySQL INSERT commands with fields matching values"

BINDIR         = $(DESTDIR)$(PREFIX)/usr/bin/
MANDIR         = $(DESTDIR)$(PREFIX)/usr/share/man/man1/

GZIP           = /bin/gzip
HELP2MAN       = /usr/bin/help2man
SHELL          = /bin/sh

CC=gcc
LDFLAGS=
#LDFLAGS= -g
BIGFILES=-D_FILE_OFFSET_BITS=64
CFLAGS=-Wall  -O2 $(BIGFILES) -DMWXML2SQLVERS=\"$(MWXML2SQLVERS)\"
#CFLAGS=-Wall  -g $(BIGFILES)

build: mwxml2sql sql2txt sqlfilter manpages

mwxml2sql: mwxml2sql.o filebuffers.o xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o mwxml2sql mwxml2sql.o filebuffers.o \
	    xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o -lbz2 -lz

sql2txt: sql2txt.o filebuffers.o sqlutils.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o sql2txt sql2txt.o filebuffers.o sqlutils.o \
	    -lbz2 -lz

sqlfilter: sqlfilter.o filebuffers.o sqlutils.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o sqlfilter sqlfilter.o filebuffers.o sqlutils.o \
	    -lbz2 -lz

manpages: mwxml2sql.1.gz sql2txt.1.gz sqlfilter.1.gz

mwxml2sql.1.gz: mwxml2sql
	$(HELP2MAN) --section 1 --no-info --name $(NAME_MWXML2SQL) \
	            --no-discard-stderr ./mwxml2sql | $(GZIP) > mwxml2sql.1.gz

sql2txt.1.gz: sql2txt
	$(HELP2MAN) --section 1 --no-info --name $(NAME_SQL2TXT) \
	            --no-discard-stderr ./sql2txt | $(GZIP) > sql2txt.1.gz

sqlfilter.1.gz: sqlfilter
	$(HELP2MAN) --section 1 --no-info --name $(NAME_SQLFILTER) \
	            --no-discard-stderr ./sqlfilter | $(GZIP) > sqlfilter.1.gz

install: mwxml2sql sql2txt sqlfilter
	install --directory                      $(BINDIR)
	install --mode=755   mwxml2sql           $(BINDIR)
	install --mode=755   sql2txt             $(BINDIR)
	install --mode=755   sqlfilter           $(BINDIR)
	install --directory                      $(MANDIR)
	install --mode=644   mwxml2sql.1.gz      $(MANDIR)
	install --mode=644   sqlfilter.1.gz      $(MANDIR)
	install --mode=644   sql2txt.1.gz        $(MANDIR)

deinstall:
	rm -f $(BINDIR)mwxml2sql
	rm -f $(BINDIR)sql2txt
	rm -f $(BINDIR)sqlfilter
	rm -f $(MANDIR)mwxml2sql.1.gz
	rm -f $(MANDIR)sqlfilter.1.gz
	rm -f $(MANDIR)sql2txt.1.gz

clean: 
	rm -f *.o *.a mwxml2sql sql2txt sqlfilter *~
	rm -f mwxml2sql.1.gz sql2txt.1.gz sqlfilter.1.gz

static: mwxml2sql_static sql2txt_static sqlfilter_static

mwxml2sql_static: mwxml2sql
	$(CC) $(CFLAGS) -static -static-libgcc -o mwxml2sql_static mwxml2sql.o filebuffers.o \
	    xmltags.o mwxmlelts.o sqlutils.o base36.o sha1.o -L. -lbz2 -lz

sql2txt_static: sql2txt
	$(CC) $(CFLAGS) -static -static-libgcc -o sql2txt_static sql2txt.o filebuffers.o sqlutils.o \
	    sha1.o -L. -lbz2 -lz

sqlfilter_static: sqlfilter
	$(CC) $(CFLAGS) -static -static-libgcc -o sqlfilter_static sqlfilter.o filebuffers.o sqlutils.o \
	    sha1.o -L. -lbz2 -lz

mwxml2sql.o: mwxml2sql.c mwxml2sql.h
	$(CC) $(CFLAGS) -c mwxml2sql.c

sql2txt.o: sql2txt.c mwxml2sql.h
	$(CC) $(CFLAGS) -c sql2txt.c

sqlfilter.o: sqlfilter.c mwxml2sql.h
	$(CC) $(CFLAGS) -c sqlfilter.c

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

dist: 
	rm -f $(DISTNAME)
	ln -s -f . $(DISTNAME)
	tar cvf $(DISTNAME).tar \
	   $(DISTNAME)/*.h \
	   $(DISTNAME)/*.c \
	   $(DISTNAME)/Makefile \
	   $(DISTNAME)/COPYING \
	   $(DISTNAME)/README \
	   $(DISTNAME)/CHANGELOG \
	   $(DISTNAME)/TODO.txt \
	   $(DISTNAME)/scripts/*.py \
	   $(DISTNAME)/scripts/*.pl \
	   $(DISTNAME)/scripts/README.scripts
	gzip -v $(DISTNAME).tar
