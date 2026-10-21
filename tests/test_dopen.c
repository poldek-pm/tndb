/* tndb_dopen() must accept descriptor 0 for the database and tndb_close()
   must release it.  tndb_dopen() treated 0 as "no descriptor", re-opened the
   database by path and left descriptor 0 behind. */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <trurl/n_check.h>

#include "tndb.h"

START_TEST(test_dopen_descriptor_zero)
{
    const char *path = NTEST_TMPPATH("tndb_dopen_zero.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;
    char key[] = "key", val[] = "value";
    char buf[64];
    int fd, nread, was_closed;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    /* free descriptor 0 and hand the database's own descriptor in */
    close(0);
    fd = open(path, O_RDONLY);
    expect_int(fd, 0);

    db = tndb_dopen(fd, path);
    expect_notnull(db);

    /* the database is really read through descriptor 0 */
    nread = tndb_get(db, key, strlen(key), buf, sizeof(buf));
    expect_int(nread, (int)strlen(val));
    buf[nread] = '\0';
    expect_str(buf, val);

    expect_int(tndb_close(db), 1);

    /* the fix: the descriptor passed in was used and is closed now; the bug
       re-opened the database by path and left 0 open */
    was_closed = (fcntl(0, F_GETFD) == -1);

    if (!was_closed)                    /* the leftover from the bug */
        close(0);
    fd = open("/dev/null", O_RDONLY);   /* give the suite a usable stdin */
    expect_int(fd, 0);

    expect_int(was_closed, 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

NTEST_RUNNER("tndb-dopen",
             test_dopen_descriptor_zero
);
