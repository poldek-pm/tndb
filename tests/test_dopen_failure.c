/* tndb_dopen() owns fd even when opening the stream fails: the caller must
   not be left with a descriptor it cannot close. */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include "tndb.h"

static int dopen_called;

tn_stream *__wrap_n_stream_dopen(int fd, const char *mode, int type)
{
    (void)fd;
    (void)mode;
    (void)type;

    dopen_called = 1;
    return NULL;
}

int main(void)
{
    struct tndb *db;
    int fd;

    if ((fd = open("/dev/null", O_RDONLY)) < 0) {
        perror("open");
        return 2;
    }

    dopen_called = 0;
    db = tndb_dopen(fd, "tndb-dopen-failure.db");

    if (db != NULL) {
        fprintf(stderr, "tndb_dopen() succeeded unexpectedly\n");
        tndb_close(db);
        return 1;
    }

    if (!dopen_called) {
        fprintf(stderr, "tndb_dopen() did not try to open the stream\n");
        return 1;
    }

    errno = 0;
    if (fcntl(fd, F_GETFD) != -1 || errno != EBADF) {
        fprintf(stderr, "tndb_dopen() did not close fd %d on failure\n", fd);
        return 1;
    }

    return 0;
}
