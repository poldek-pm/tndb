AM_CFLAGS = @AM_CFLAGS@
CPPFLAGS = @CPPFLAGS@ @CHECK_CFLAGS@
LDADD = @CHECK_LIBS@ $(top_builddir)/libtndb.la

AM_LDFLAGS = -Wl,-rpath,$(abs_top_builddir)/.libs

check_PROGRAMS = test_basic test_lookup test_open test_tndb test_compr \
	test_verify test_dopen test_dopen_failure test_verify_flag test_sign \
	test_sign_extra
TESTS =	$(check_PROGRAMS)

test_sign_LDFLAGS = $(AM_LDFLAGS)
test_sign_extra_LDFLAGS = $(AM_LDFLAGS)

test_dopen_failure_LDFLAGS = $(AM_LDFLAGS) -Wl,--wrap=n_stream_dopen \
	-Wl,-rpath,$(abs_top_builddir)/../trurlib/.libs
test_dopen_failure_LDADD = $(top_builddir)/.libs/libtndb.a @LIBS@
test_dopen_failure_DEPENDENCIES = $(top_builddir)/libtndb.la
