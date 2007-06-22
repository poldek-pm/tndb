TESTS = test_tndb
noinst_PROGRAMS = test_tndb
test_tndb_SOURCES = test_tndb.c test_base.c test.h

test_tndb_INCLUDES= @CHECK_CFLAGS@
test_tndb_LDADD = @CHECK_LIBS@ ../libtndb.la

clean-local:
	-rm -f *.tmp core *.o *.bak *~ *% *\# TAGS gmon.out \#*\# dupa* 

