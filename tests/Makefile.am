AM_CFLAGS = @AM_CFLAGS@
CPPFLAGS = @CPPFLAGS@ @CHECK_CFLAGS@
LDADD = @CHECK_LIBS@ $(top_builddir)/libtndb.la

check_PROGRAMS = test_basic test_lookup test_open test_tndb test_compr
TESTS =	$(check_PROGRAMS)
check_PROGRAMS += test_verify

# tndb_dopen() with descriptor 0 as an explicit argument
check_PROGRAMS += test_dopen

# tndb_dopen() must close fd when opening the stream fails
check_PROGRAMS += test_dopen_failure
test_dopen_failure_LDFLAGS = -Wl,--wrap=n_stream_dopen \
	-Wl,-rpath,$(abs_top_builddir)/../trurlib/.libs
test_dopen_failure_LDADD = $(top_builddir)/.libs/libtndb.a @LIBS@
test_dopen_failure_DEPENDENCIES = $(top_builddir)/libtndb.la

# a failed tndb_verify() must not mark the database as verified
check_PROGRAMS += test_verify_flag
