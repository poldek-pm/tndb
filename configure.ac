# Process this file with autoconf to produce a configure script.

m4_define([tndb_major_version], [0])
m4_define([tndb_minor_version], [1])
m4_define([tndb_micro_version], [1])
m4_define([tndb_version],
          [tndb_major_version.tndb_minor_version.tndb_micro_version])

AC_PREREQ([2.63])
AC_INIT([tndb], [tndb_version], [http://bugs.launchpad.net/poldek])
AC_CONFIG_SRCDIR([tndb.h])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([1.11 foreign no-dist-gzip dist-xz])
AM_MAINTAINER_MODE([enable])
AM_SILENT_RULES([no])

AC_SYS_LARGEFILE

# Checks for programs.
m4_ifdef([LT_INIT], [LT_INIT], [AC_PROG_LIBTOOL])
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB

# libtool versioning
LT_CURRENT=1
LT_REVISION=0
LT_AGE=1
AC_SUBST(LT_CURRENT)
AC_SUBST(LT_REVISION)
AC_SUBST(LT_AGE)

AM_CFLAGS="-Wall -W"
AC_SUBST(AM_CFLAGS)

if test -n "$GCC"; then
        cflags_save="$CFLAGS"
        CFLAGS="$CFLAGS -fvisibility=hidden"
        AC_MSG_CHECKING([whether gcc supports visibility])
        AC_COMPILE_IFELSE(
    		[AC_LANG_PROGRAM(
    			[[extern __attribute__((visibility("hidden"))) int hiddenvar;
                	  extern __attribute__((visibility("default"))) int exportedvar;
                	  extern __attribute__((visibility("hidden"))) void hiddenfunc();
                	  extern __attribute__((visibility("default"))) void exportedfunc();]],
                	[[]]
                )],
    		[AC_MSG_RESULT([yes])],
    		[
            		AC_MSG_WARN([compiler does not support visibility])
                        CFLAGS="$cflags_save"
    		]
    	)
fi

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h limits.h stdint.h stdlib.h string.h sys/param.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_UINT8_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T

# Checks for library functions.
AC_FUNC_ALLOCA
AC_CHECK_FUNCS([gettimeofday memset mkstemp rmdir])

# Checks for libraries.
AC_CHECK_LIB(crypto, EVP_DigestInit, [], [AC_MSG_ERROR(["libcrypto is needed by $PACKAGE"])])
AC_CHECK_LIB(z, gzopen)

# Use local ../trurlib copy if it exists
AC_MSG_CHECKING([for local trurlib copy in ../trurlib])
if test -f "${srcdir}/../trurlib/include/trurl/trurl.h"; then
	AC_MSG_RESULT([yes])
	CPPFLAGS="$CPPFLAGS -I\$(top_srcdir)/../trurlib/include/"
	LDFLAGS="$LDFLAGS -L\$(top_builddir)/../trurlib"
	LIBS="$LIBS -ltrurl"
else
	AC_MSG_RESULT([no])
	AC_CHECK_HEADER([trurl/trurl.h],
			[],
			[AC_MSG_ERROR([trurl.h is missing])])
	AC_CHECK_LIB(trurl, n_cfree)
fi

PKG_CHECK_MODULES(CHECK, check >= 0.9.4, have_check=yes, have_check=no)

AC_CONFIG_FILES([
Makefile
data/Makefile
data/tndb.pc
tests/Makefile
])
AC_OUTPUT

echo "
		    tndb $VERSION
		  ============

	prefix:         ${prefix}
	compiler:       ${CC}
	cflags:         ${CFLAGS}
	cppflags:       ${CPPFLAGS}
	ldflags:        ${LDFLAGS}
"
