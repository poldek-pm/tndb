# $Id$

AUTOMAKE_OPTIONS = foreign
AM_CFLAGS = @AM_CFLAGS@

# use libtool versioning 
TNDB_VERINFO   = 1:0:1

lib_LTLIBRARIES  = libtndb.la
libtndb_la_LDFLAGS = -version-info $(TNDB_VERINFO) -export-symbols $(top_srcdir)/tndb.sym

libtndb_la_SOURCES = write.c read.c tndb.c tndb_int.h
pkgincludedir = $(includedir)/tndb
pkginclude_HEADERS = tndb.h

EXTRA_DIST = Makefile.extra tndb.sym

EXTRA_PROGRAMS = test_tndb tndb_dump
test_tndb_LDADD = libtndb.la
tndb_dump_LDADD = $(test_tndb_LDADD)

DIST_SUBDIRS = tests

MAINTAINERCLEANFILES =	mkinstalldirs install-sh missing *.tar.gz *.spec \
			config.h.in configure Makefile.in config.h \
			config.sub config.guess config.status config.log aclocal.m4 \
			libtool ltconfig ltmain.sh stamp-h* depcomp

DISTCLEANFILES = TAGS gmon.out tndb $(distdir)/.deps libtndb.a
CLEANFILES = $(EXTRA_PROGRAMS) core *.o *.bak *~ *% *\# \#* 

#symlink: 
#	@if [ ! -d tndb ]; then ln . tndb -s; fi;

all-local:
	@ln -sf .libs/libtndb.a libtndb.a

#dist-hook:
#	rm -rf $(distdir)/.deps

#clean-local:
#	-rm -f core *.o *.bak *~ *% *\# TAGS gmon.out tndb \#*
#	-rm -f $(EXTRA_PROGRAMS)

include Makefile.extra
