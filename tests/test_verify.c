/* tndb_verify() takes the .md5 next to a database for granted only while
   the database keeps the size and mtime it had when the .md5 was written */
#include <config.h>

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include <trurl/n_check.h>

#include "tndb.h"

static void create_verified(const char *path)
{
    struct tndb *db;
    char md5path[PATH_MAX];

    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    for (int i = 0; i < 100; i++) {
        char key[32];
        int n = snprintf(key, sizeof(key), "key%d", i);
        expect_int(tndb_put(db, key, n, key, n), 1);
    }
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);     /* writes the .md5 */
    tndb_close(db);
}

static void remove_verified(const char *path)
{
    char md5path[PATH_MAX];

    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);
    unlink(path);
}

static int verify(const char *path)
{
    struct tndb *db = tndb_open(path);
    int rc;

    expect_notnull(db);
    rc = tndb_verify(db);
    tndb_close(db);
    return rc;
}

static void set_mtime(const char *path, struct timespec mtime)
{
    struct timespec ts[2] = { mtime, mtime };
    expect_int(utimensat(AT_FDCWD, path, ts, 0), 0);
}

START_TEST(test_changed_within_the_same_second)
{
    char *path = NTEST_TMPPATH("tndb_verify_second.db");
    struct stat st, st2;
    FILE *fp;

    create_verified(path);
    expect_int(stat(path, &st), 0);

    fp = fopen(path, "r+b");
    expect_notnull(fp);
    fseek(fp, st.st_size - 10, SEEK_SET);
    fputc(0xff, fp);
    fclose(fp);

    /* the same second as before, another nanosecond */
    st.st_mtim.tv_nsec = (st.st_mtim.tv_nsec + 1) % 1000000000;
    set_mtime(path, st.st_mtim);

    expect_int(stat(path, &st2), 0);
    if (st2.st_mtim.tv_nsec == st.st_mtim.tv_nsec)
        expect_int(verify(path), 0);
    else                        /* whole seconds: the change cannot be told */
        printf("%s: the filesystem keeps no nanoseconds, case skipped\n", path);
    remove_verified(path);
}
END_TEST

START_TEST(test_cut_with_its_mtime_kept)
{
    char *path = NTEST_TMPPATH("tndb_verify_cut.db");
    struct stat st;

    create_verified(path);
    expect_int(stat(path, &st), 0);

    /* what an interrupted download stamped with the server's mtime leaves */
    expect_int(truncate(path, st.st_size - 100), 0);
    set_mtime(path, st.st_mtim);

    expect_int(verify(path), 0);
    remove_verified(path);
}
END_TEST

START_TEST(test_md5_file_of_an_older_tndb)
{
    char *path = NTEST_TMPPATH("tndb_verify_old.db");
    char md5path[PATH_MAX];
    struct stat st;

    create_verified(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);

    /* an older tndb wrote the md5 alone */
    expect_int(truncate(md5path, 32), 0);

    expect_int(verify(path), 1);
    expect_int(stat(md5path, &st), 0);
    expect_int(st.st_size > 32, 1);     /* now with the size and mtime */
    remove_verified(path);
}
END_TEST

NTEST_RUNNER("tndb-verify",
             test_changed_within_the_same_second,
             test_cut_with_its_mtime_kept,
             test_md5_file_of_an_older_tndb
);
