#!/bin/sh
# $Id$

FILE=tndb.h
PKG=tndb
 
if [ ! -f $FILE ]; then
    cd `dirname $0`;
    if [ ! -f $FILE ]; then
	echo "`pwd`: not a $PKG directory"
	exit 1
    fi 	
fi

runcmd () {
    echo "$@"
    $@
    test $? -eq 0 || exit 1
}

runcmd libtoolize --force --automake
runcmd aclocal
runcmd autoheader
runcmd automake --add-missing --no-force
runcmd autoconf

CONFOPTS="--enable-maintainer-mode --enable-compile-warnings $@"
runcmd ./configure $CONFOPTS
