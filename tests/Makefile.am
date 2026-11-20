AM_CFLAGS = @AM_CFLAGS@
CPPFLAGS = @CPPFLAGS@ @CHECK_CFLAGS@
LDADD = @CHECK_LIBS@ $(top_builddir)/libtndb.la

check_PROGRAMS = test_basic test_lookup test_open test_tndb test_compr \
	test_verify test_dopen test_dopen_failure test_verify_flag test_atomic \
	test_die_hook
TESTS =	$(check_PROGRAMS)

test_dopen_failure_LDFLAGS = -Wl,--wrap=n_stream_dopen \
	-Wl,-rpath,$(abs_top_builddir)/../trurlib/.libs
test_dopen_failure_LDADD = $(top_builddir)/.libs/libtndb.a @LIBS@
test_dopen_failure_DEPENDENCIES = $(top_builddir)/libtndb.la

test_atomic_LDFLAGS = -Wl,--wrap=n_stream_close \
	-Wl,--wrap=n_stream_dopen -Wl,--wrap=copy_file_range \
	-Wl,--wrap=write \
	-Wl,-rpath,$(abs_top_builddir)/../trurlib/.libs
test_atomic_LDADD = $(top_builddir)/.libs/libtndb.a @LIBS@ @CHECK_LIBS@
test_atomic_DEPENDENCIES = $(top_builddir)/libtndb.la
