#include <config.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include <trurl/narray.h>
#include <trurl/nmalloc.h>
#include <trurl/n_snprintf.h>
#include <trurl/n_check.h>

#include "tndb.h"

START_TEST(test_creat_open_close)
{
    struct tndb *db;
    char *path = NTEST_TMPPATH("tndb_basic.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);

    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_close(db), 1);

    unlink(path);
}
END_TEST

START_TEST(test_empty_database)
{
    struct tndb *db;
    char key[] = "nonexistent";
    char val[256];
    uint32_t voffs, vlen;
    struct tndb_it it;
    char *path = NTEST_TMPPATH("tndb_empty.db");

    unlink(path);

    /* Create empty database */
    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_close(db), 1);

    /* Open and verify */
    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_size(db), 0);
    expect_int(tndb_verify(db), 1);

    /* Lookup in empty db */
    expect_int(tndb_get(db, key, strlen(key), val, sizeof(val)), 0);
    expect_int(tndb_get_voff(db, key, strlen(key), &voffs, &vlen), 0);

    /* Iterate empty db */
    expect_int(tndb_it_start(db, &it), 1);
    expect_int(tndb_it_get(&it, NULL, NULL, val, &vlen), 0);

    /* Keys from empty db */
    tn_array *keys = tndb_keys(db);
    expect_notnull(keys);
    expect_int(n_array_size(keys), 0);
    n_array_free(keys);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_put_get_basic)
{
    struct tndb *db;
    char *key = "testkey";
    char *val = "testvalue";
    char buf[256];
    int nread;
    char *path = NTEST_TMPPATH("tndb_putget.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);

    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    nread = tndb_get(db, key, strlen(key), buf, sizeof(buf));
    expect_int(nread, (int)strlen(val));
    buf[nread] = '\0';
    expect_str(buf, val);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_multiple_records)
{
    struct tndb *db;
    char key[32], val[32];
    char buf[32];
    int i, nread;
    int nrec = 100;
    char *path = NTEST_TMPPATH("tndb_multi.db");

    unlink(path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);

    for (i = 0; i < nrec; i++) {
        snprintf(key, sizeof(key), "key%.3d", i);
        snprintf(val, sizeof(val), "val%.3d", i);
        expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    }
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_size(db), nrec);
    expect_int(tndb_verify(db), 1);

    /* Lookup all records */
    for (i = 0; i < nrec; i++) {
        snprintf(key, sizeof(key), "key%.3d", i);
        snprintf(val, sizeof(val), "val%.3d", i);

        nread = tndb_get(db, key, strlen(key), buf, sizeof(buf));
        expect_int(nread, (int)strlen(val));
        buf[nread] = '\0';
        expect_str(buf, val);
    }

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_signed_database)
{
    struct tndb *db;
    char *key = "signed_key";
    char *val = "signed_value";
    char buf[256];
    char *path = NTEST_TMPPATH("tndb_signed.db");

    unlink(path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_verify(db), 1);
    expect_int(tndb_get(db, key, strlen(key), buf, sizeof(buf)), (int)strlen(val));

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_corrupted_database)
{
    struct tndb *db;
    char *key = "signed_key";
    char *val = "signed_value";
    char *path = NTEST_TMPPATH("tndb_corrupted.db");
    FILE *fp;
    long filesize;

    unlink(path);

    /* Create signed database */
    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    /* Corrupt the database file */
    fp = fopen(path, "r+b");
    expect_notnull(fp);

    /* Get file size */
    fseek(fp, 0, SEEK_END);
    filesize = ftell(fp);

    /* Corrupt a byte in the middle of the file */
    if (filesize > 100) {
        fseek(fp, filesize / 2, SEEK_SET);
        fputc(0xFF, fp);  /* corrupt one byte */
    }
    fclose(fp);

    /* Open corrupted database - should succeed */
    db = tndb_open(path);
    expect_notnull(db);

    /* Verify should fail for corrupted database */
    expect_int(tndb_verify(db), 0);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_refcount)
{
    struct tndb *db;
    char *path = NTEST_TMPPATH("tndb_ref.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);

    /* Reference counting */
    expect_notnull(tndb_ref(db));
    expect_notnull(tndb_ref(db));

    /* Close should decrement refcount */
    expect_int(tndb_close(db), 1);  /* refcount-- */
    expect_int(tndb_close(db), 1);  /* refcount-- */
    expect_int(tndb_close(db), 1);  /* refcount == 0, actually close */

    unlink(path);
}
END_TEST

START_TEST(test_path_and_stream)
{
    struct tndb *db;
    tn_stream *st;
    const char *ret_path;
    char *path = NTEST_TMPPATH("tndb_path.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);

    ret_path = tndb_path(db);
    expect_notnull(ret_path);
    expect_str(ret_path, path);

    st = tndb_tn_stream(db);
    expect_notnull(st);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_keys_api)
{
    struct tndb *db;
    char key[32], val[32];
    int i;
    int nrec = 50;
    char *path = NTEST_TMPPATH("tndb_keys.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);

    for (i = 0; i < nrec; i++) {
        snprintf(key, sizeof(key), "key%.3d", i);
        snprintf(val, sizeof(val), "val%.3d", i);
        expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    }
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    tn_array *keys = tndb_keys(db);
    expect_notnull(keys);
    expect_int(n_array_size(keys), nrec);

    n_array_free(keys);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

NTEST_RUNNER("tndb-basic",
             test_creat_open_close,
             test_empty_database,
             test_put_get_basic,
             test_multiple_records,
             test_signed_database,
             test_corrupted_database,
             test_refcount,
             test_path_and_stream,
             test_keys_api
);
