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

START_TEST(test_get_all)
{
    struct tndb *db;
    char *key = "testkey";
    char *val = "testvalue that is longer";
    void *buf = NULL;
    size_t nread;
    char *path = NTEST_TMPPATH("tndb_getall.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    nread = tndb_get_all(db, key, strlen(key), &buf);
    expect_int((int)nread, (int)strlen(val));
    expect_notnull(buf);
    expect_int(memcmp(buf, val, nread), 0);

    free(buf);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_get_str)
{
    struct tndb *db;
    char *key = "strkey";
    char *val = "string value";
    unsigned char buf[256];
    int nread;
    char *path = NTEST_TMPPATH("tndb_getstr.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    nread = tndb_get_str(db, key, buf, sizeof(buf));
    expect_int(nread, (int)strlen(val));
    expect_str((char*)buf, val);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_key_not_found)
{
    struct tndb *db;
    char *key = "existing";
    char *missing = "missing";
    char *val = "value";
    char buf[256];
    uint32_t voffs, vlen;
    void *alloc_buf = NULL;
    char *path = NTEST_TMPPATH("tndb_notfound.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_get(db, missing, strlen(missing), buf, sizeof(buf)), 0);
    expect_int(tndb_get_voff(db, missing, strlen(missing), &voffs, &vlen), 0);
    expect_int((int)tndb_get_all(db, missing, strlen(missing), &alloc_buf), 0);
    expect_null(alloc_buf);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_binary_data)
{
    struct tndb *db;
    char key[] = "binary_key";
    unsigned char val[128];
    void *buf = NULL;
    int i;
    size_t nread;
    char *path = NTEST_TMPPATH("tndb_binary.db");

    unlink(path);

    /* Fill with binary data including nulls */
    for (i = 0; i < (int)sizeof(val); i++)
        val[i] = (unsigned char)(i & 0xff);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, sizeof(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    /* Use tndb_get_all which allocates buffer automatically */
    nread = tndb_get_all(db, key, strlen(key), &buf);
    expect_int((int)nread, (int)sizeof(val));
    expect_notnull(buf);
    expect_int(memcmp(buf, val, sizeof(val)), 0);

    free(buf);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_max_key_length)
{
    struct tndb *db;
    char key[TNDB_KEY_MAX + 1];
    char *val = "value";
    char buf[256];
    int i, nread;
    char *path = NTEST_TMPPATH("tndb_maxkey.db");

    /* Fill key to max length */
    for (i = 0; i < TNDB_KEY_MAX; i++)
        key[i] = 'a' + (i % 26);
    key[TNDB_KEY_MAX] = '\0';

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, TNDB_KEY_MAX, val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    nread = tndb_get(db, key, TNDB_KEY_MAX, buf, sizeof(buf));
    expect_int(nread, (int)strlen(val));
    buf[nread] = '\0';
    expect_str(buf, val);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_iterator_rget)
{
    struct tndb *db;
    char key[32], val[32];
    char iter_key[TNDB_KEY_MAX + 1];
    void *iter_val = NULL;
    unsigned int klen, vlen;
    struct tndb_it it;
    int i, count = 0;
    int nrec = 50;
    char *path = NTEST_TMPPATH("tndb_itrget.db");

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

    expect_int(tndb_it_start(db, &it), 1);

    while (tndb_it_rget(&it, iter_key, &klen, &iter_val, &vlen) > 0) {
        fail_unless(klen <= TNDB_KEY_MAX, "key length exceeds max");
        iter_key[klen] = '\0';
        ((char*)iter_val)[vlen] = '\0';

        /* Verify format */
        fail_unless(strncmp(iter_key, "key", 3) == 0, "unexpected key format");
        fail_unless(strncmp(iter_val, "val", 3) == 0, "unexpected value format");

        count++;
    }

    expect_int(count, nrec);

    free(iter_val);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_iterator_get)
{
    struct tndb *db;
    char key[32], val[32];
    char iter_key[TNDB_KEY_MAX + 1];
    char iter_val[512];
    unsigned int klen, vlen;
    struct tndb_it it;
    int i, count = 0;
    int nrec = 30;
    char *path = NTEST_TMPPATH("tndb_itget.db");

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

    expect_int(tndb_it_start(db, &it), 1);

    while (1) {
        klen = TNDB_KEY_MAX + 1;
        vlen = sizeof(iter_val);
        if (tndb_it_get(&it, iter_key, &klen, iter_val, &vlen) <= 0)
            break;

        iter_key[klen] = '\0';
        iter_val[vlen] = '\0';

        /* Verify format */
        fail_unless(strncmp(iter_key, "key", 3) == 0, "unexpected key format");
        fail_unless(strncmp(iter_val, "val", 3) == 0, "unexpected value format");

        count++;
    }

    expect_int(count, nrec);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

START_TEST(test_get_voff)
{
    struct tndb *db;
    char *key = "testkey";
    char *val = "testvalue";
    uint32_t voffs, vlen;
    char buf[256];
    char *path = NTEST_TMPPATH("tndb_voff.db");

    unlink(path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    /* Get offset and length */
    expect_int(tndb_get_voff(db, key, strlen(key), &voffs, &vlen), 1);
    expect_int((int)vlen, (int)strlen(val));

    /* Read value at offset */
    expect_int(tndb_read(db, voffs, buf, vlen), (int)vlen);
    buf[vlen] = '\0';
    expect_str(buf, val);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

NTEST_RUNNER("tndb-lookup",
             test_get_all,
             test_get_str,
             test_key_not_found,
             test_binary_data,
             test_max_key_length,
             test_iterator_rget,
             test_iterator_get,
             test_get_voff
);
