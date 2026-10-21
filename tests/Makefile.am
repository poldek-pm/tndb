AM_CFLAGS = @AM_CFLAGS@
CPPFLAGS = @CPPFLAGS@ @CHECK_CFLAGS@
LDADD = @CHECK_LIBS@ $(top_builddir)/libtndb.la

check_PROGRAMS = test_basic test_lookup test_tndb test_compr
TESTS =	$(check_PROGRAMS)

# tndb_dopen() with descriptor 0 as an explicit argument
check_PROGRAMS += test_dopen

# a failed tndb_verify() must not mark the database as verified
check_PROGRAMS += test_verify_flag
