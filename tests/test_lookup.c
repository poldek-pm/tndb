#include <config.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <trurl/narray.h>
#include <trurl/nmalloc.h>
#include <trurl/n_snprintf.h>
#include <trurl/n_check.h>
#include <trurl/ndie.h>

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
    unsigned int klen, vlen = 0;    /* in: size of iter_val */
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

/* an empty value is a record like any other: iteration goes on past it */
START_TEST(test_iterator_empty_value)
{
    static const char *keys[] = { "a", "b", "c" }, *vals[] = { "x", "", "y" };
    char *path = NTEST_TMPPATH("tndb_it_empty.db");
    char key[TNDB_KEY_MAX + 1], buf[16];
    unsigned int klen, vlen;
    void *rbuf = NULL;
    struct tndb_it it;
    struct tndb *db;
    tn_array *dbkeys;
    uint32_t voff;
    int n;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    for (n = 0; n < 3; n++)
        expect_int(tndb_put(db, keys[n], 1, vals[n], strlen(vals[n])), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; tndb_it_get_voff(&it, key, &klen, &voff, &vlen) > 0; n++)
        expect_int(vlen, strlen(vals[n]));
    expect_int(n, 3);

    expect_int(tndb_it_start(db, &it), 1);
    n = 0;
    vlen = sizeof(buf);
    while (tndb_it_get(&it, key, &klen, buf, &vlen) > 0) {
        expect_str(buf, vals[n++]);
        vlen = sizeof(buf);
    }
    expect_int(n, 3);

    expect_int(tndb_it_start(db, &it), 1);
    vlen = 0;
    for (n = 0; tndb_it_rget(&it, key, &klen, &rbuf, &vlen) > 0; n++)
        expect_str(rbuf, vals[n]);
    expect_int(n, 3);
    free(rbuf);

    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; tndb_it_get_begin(&it, key, &klen, &vlen) > 0; n++) {
        expect_int(vlen, strlen(vals[n]));
        expect_int(tndb_it_get_end(&it), 1);
    }
    expect_int(n, 3);

    dbkeys = tndb_keys(db);
    expect_notnull(dbkeys);
    expect_int(n_array_size(dbkeys), 3);
    n_array_free(dbkeys);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

static int die_calls;

static void returning_die_hook(const char *msg)
{
    (void)msg;
    die_calls++;
}

/* a reader called after it returned -1 returns 0, with an empty key */
static void expect_dead(struct tndb_it *it)
{
    char key[TNDB_KEY_MAX + 1];
    unsigned int klen, vlen;
    t_fn_die_hook old_hook;
    uint32_t voff;
    int rc;

    old_hook = n_die_set_hook(returning_die_hook);
    die_calls = 0;
    key[0] = 'k';
    voff = vlen = 7;
    rc = tndb_it_get_voff(it, key, &klen, &voff, &vlen);
    n_die_set_hook(old_hook);
    expect_int(rc, 0);
    expect_int(die_calls, 0);
    expect_int(klen, 0);
    expect_str(key, "");
    expect_int(voff, 0);
    expect_int(vlen, 0);
}

/* A record is a key length, the key, a 32-bit value length and the value;
   offsets in the file are 32-bit too. A record that ends past them comes
   from a corrupted file and nothing is read for it: with a value length of
   0xffffffff, vlen + 1 wrapped to 0 and the readers that size the caller's
   buffer with it copied the rest of the file into it. */

#define REST_SIZE (64 * 1024)
/* records whose value length, or also key, runs past the last offset */
static const uint32_t hdr_offs[] = { UINT32_MAX - 2, UINT32_MAX };

/* check has no skip: a case the environment cannot run passes, so at least
   name the case and the reason in the log (NTEST_LOG prints only with -vv);
   errno is kept for a %m in the message */
#define SKIP(...)                                             \
    do {                                                      \
        int e_ = errno;                                       \
        fprintf(stderr, "[%s] skipped: ", tcase_name());      \
        errno = e_;                                           \
        fprintf(stderr, __VA_ARGS__);                         \
        fflush(stderr);                                       \
    } while (0)

/* the size of the sparse file of the cases past the last offset */
#define SPARSE_SIZE ((uint64_t)UINT32_MAX + 8)

/* writes n bytes at offs; 0 on failure, with errno of the step that failed,
   so that a case that needs an environment the file system cannot give
   skips rather than fails */
static int write_at(const char *path, off_t offs, const void *buf, size_t n)
{
    FILE *fp;
    int ok, e;

    if ((fp = fopen(path, "r+b")) == NULL)
        return 0;

    ok = fseeko(fp, offs, SEEK_SET) == 0 && fwrite(buf, 1, n, fp) == n;
    e = errno;
    if (fclose(fp) != 0) {
        if (ok)                 /* the write fails only at the flush */
            e = errno;
        ok = 0;
    }
    errno = e;
    return ok;
}

static void put_be32(unsigned char *b, uint32_t v)
{
    b[0] = v >> 24; b[1] = v >> 16; b[2] = v >> 8; b[3] = v;
}

static uint32_t get_be32(const unsigned char *b)
{
    return (uint32_t)b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
}

/* the first byte of the guard a reader changed, or -1; one assertion for
   the whole guard, not one per byte, which floods the test framework */
static int guard_at(const char *buf, size_t from, size_t to)
{
    for (size_t j = from; j < to; j++)
        if (buf[j] != 'g')
            return (int)j;
    return -1;
}

/* key "a" with the value "value", then key "b" with REST_SIZE bytes; the
   value length of "a" set to len unless that is 5; returns its offset */
static uint32_t creat_with_vlen(const char *path, uint32_t len)
{
    static char rest[REST_SIZE];
    unsigned char be[4];
    uint32_t voff = 0;
    unsigned int vlen = 0;
    struct tndb *db;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "value", 5), 1);
    memset(rest, 'x', sizeof(rest));
    expect_int(tndb_put(db, "b", 1, rest, sizeof(rest)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "a", 1, &voff, &vlen), 1);
    expect_int(tndb_close(db), 1);

    if (len != 5) {
        put_be32(be, len);
        fail_unless(write_at(path, voff - sizeof(be), be, sizeof(be)),
                    "cannot write the value length at %u",
                    voff - (uint32_t)sizeof(be));
    }
    return voff;
}

/* a sparse file up to past the last offset, with the key length and key
   "a" of a record at offs; 0 when the environment cannot hold such a file,
   which skips the case rather than failing the code under test */
static int extend_with_record_at(const char *path, uint32_t offs)
{
    static const unsigned char rec[] = { 1, 'a' };
    struct rlimit rl;

    /* a write or truncate past RLIMIT_FSIZE raises SIGXFSZ, whose default
       action ends the process, instead of failing the call: check it up
       front, for the size of the file, which ends past the record */
    if (getrlimit(RLIMIT_FSIZE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY &&
        SPARSE_SIZE > (uint64_t)rl.rlim_cur) {
        SKIP("RLIMIT_FSIZE is %llu, the file needs %llu bytes\n",
             (unsigned long long)rl.rlim_cur, (unsigned long long)SPARSE_SIZE);
        return 0;
    }

    if (truncate(path, (off_t)SPARSE_SIZE) != 0) {
        SKIP("cannot extend %s past the last offset: %m\n", path);
        return 0;
    }

    if (!write_at(path, offs, rec, sizeof(rec))) {
        SKIP("cannot write a record at %u: %m\n", offs);
        return 0;
    }

    return 1;
}

START_TEST(test_lookup_value_past_offsets)
{
    char *path = NTEST_TMPPATH("tndb_vlen_lookup.db");
    unsigned char buf[16];
    unsigned int vlen;
    uint32_t voff, voff0;
    struct tndb *db;
    void *val;

    /* runs to the last offset: still a record */
    voff0 = creat_with_vlen(path, 5);
    creat_with_vlen(path, UINT32_MAX - voff0);
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "a", 1, &voff, &vlen), 1);
    expect_int(voff, voff0);
    fail_unless(vlen == UINT32_MAX - voff0, "value length %u", vlen);
    expect_int(tndb_close(db), 1);

    /* 0xffffffff, and one byte past the last offset */
    uint32_t lens[] = { UINT32_MAX, UINT32_MAX - voff0 + 1 };
    for (unsigned i = 0; i < sizeof(lens) / sizeof(lens[0]); i++) {
        creat_with_vlen(path, lens[i]);
        db = tndb_open(path);
        expect_notnull(db);

        expect_int(tndb_get_voff(db, "a", 1, &voff, &vlen), -1);
        expect_int(tndb_get(db, "a", 1, buf, sizeof(buf)), 0);
        val = NULL;
        expect_int((int)tndb_get_all(db, "a", 1, &val), 0);
        expect_null(val);

        /* the record after it is still found */
        val = NULL;
        expect_int((int)tndb_get_all(db, "b", 1, &val), REST_SIZE);
        free(val);

        expect_int(tndb_close(db), 1);
    }
    unlink(path);
}
END_TEST

/* the hash table points "a" at a record whose offsets, added in 32 bits,
   wrap: those of its value, and at the last offset those of its key too,
   which then is compared with the start of the file */
START_TEST(test_lookup_header_past_offsets)
{
    char *path = NTEST_TMPPATH("tndb_hdr_lookup.db");
    unsigned char head[4096];
    uint32_t voff, recoff, htt;
    unsigned int vlen;
    struct tndb *db;
    FILE *fp;

    for (unsigned k = 0; k < sizeof(hdr_offs) / sizeof(hdr_offs[0]); k++) {
        int n = 0;

        voff = creat_with_vlen(path, 5);
        recoff = voff - sizeof(uint32_t) - 1 - 1;
        fail_unless(recoff <= sizeof(head), "first record at %u", recoff);

        fp = fopen(path, "rb");
        expect_notnull(fp);
        expect_int(fread(head, 1, recoff, fp), recoff);
        fclose(fp);

        /* "tndb1.0\n", flags, signature block (its size first), timestamp,
           number of records, offset of the first one, then the hash table:
           offsets of 256 buckets, each its size and (hash, offset) pairs */
        htt = 9 + (head[9] << 8 | head[10]) + 3 * sizeof(uint32_t);
        for (int i = 0; i < 256; i++) {
            uint32_t b = get_be32(head + htt + 4 * i);

            for (uint32_t j = 0; b && j < get_be32(head + b); j++) {
                uint32_t at = b + 4 + 8 * j + 4;

                if (get_be32(head + at) == recoff) {
                    unsigned char be[4];

                    put_be32(be, hdr_offs[k]);
                    fail_unless(write_at(path, at, be, sizeof(be)),
                                "cannot write the offset at %u", at);
                    n++;
                }
            }
        }
        expect_int(n, 1);

        if (!extend_with_record_at(path, hdr_offs[k])) {
            unlink(path);
            return;
        }

        db = tndb_open(path);
        expect_notnull(db);
        expect_int(tndb_get_voff(db, "a", 1, &voff, &vlen), -1);
        expect_int(tndb_close(db), 1);
    }
    unlink(path);
}
END_TEST

START_TEST(test_iterator_value_past_offsets)
{
    char *path = NTEST_TMPPATH("tndb_vlen_it.db");
    char key[TNDB_KEY_MAX + 1];
    unsigned int klen, vlen;
    uint32_t voff, voff0;
    struct tndb_it it;
    struct tndb *db;
    /* the readers are told the buffer is 16 bytes; the rest is a guard */
    enum { BUFSIZE = 16, GUARDSIZE = 2 * REST_SIZE };
    char *buf = n_malloc(BUFSIZE + GUARDSIZE), *rbuf;

    voff0 = creat_with_vlen(path, 5);
    /* the record as written, 0xffffffff, and one byte past the last offset */
    uint32_t lens[] = { 5, UINT32_MAX, UINT32_MAX - voff0 + 1 };
    for (unsigned i = 0; i < sizeof(lens) / sizeof(lens[0]); i++) {
        int found = lens[i] == 5, rc = found ? 1 : -1, j;

        creat_with_vlen(path, lens[i]);
        db = tndb_open(path);
        expect_notnull(db);

        memset(buf, 'g', BUFSIZE + GUARDSIZE);
        expect_int(tndb_it_start(db, &it), 1);
        vlen = BUFSIZE;
        errno = 0;
        expect_int(tndb_it_get(&it, key, &klen, buf, &vlen), rc);
        if (found)
            expect_str(buf, "value");
        else
            expect_int(errno, EBADMSG);
        j = guard_at(buf, BUFSIZE, BUFSIZE + GUARDSIZE);
        fail_unless(j < 0, "tndb_it_get() wrote past the buffer at %d", j);

        /* same block, so a write past the given size stays in it */
        memset(buf, 'g', BUFSIZE + GUARDSIZE);
        expect_int(tndb_it_start(db, &it), 1);
        rbuf = buf;
        vlen = BUFSIZE;
        expect_int(tndb_it_rget(&it, key, &klen, (void **)&rbuf, &vlen), rc);
        fail_unless(rbuf == buf, "tndb_it_rget() reallocated the buffer");
        if (found)
            expect_str(buf, "value");
        j = guard_at(buf, BUFSIZE, BUFSIZE + GUARDSIZE);
        fail_unless(j < 0, "tndb_it_rget() wrote past the buffer at %d", j);

        expect_int(tndb_it_start(db, &it), 1);
        vlen = 7;
        expect_int(tndb_it_get_begin(&it, key, &klen, &vlen), rc);
        if (found) {
            expect_int(vlen, 5);
            expect_int(tndb_it_get_end(&it), 1);
        } else {
            /* a caller that takes -1 for a record and calls
               tndb_it_get_end() gets 0, as for a value cut short */
            expect_int(vlen, 0);
            expect_int(tndb_it_get_end(&it), 0);
            expect_dead(&it);
        }

        expect_int(tndb_it_start(db, &it), 1);
        expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), rc);
        if (found) {
            expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), 1);
            expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), 0);
        } else {
            expect_dead(&it);
            expect_null(tndb_keys(db));
        }

        expect_int(tndb_close(db), 1);
    }
    free(buf);
    unlink(path);
}
END_TEST

/* "a" ends where the record after it has its offsets past the last one */
START_TEST(test_iterator_header_past_offsets)
{
    char *path = NTEST_TMPPATH("tndb_hdr_it.db");
    char key[TNDB_KEY_MAX + 1];
    unsigned int klen, vlen;
    uint32_t voff, voff0;
    struct tndb_it it;
    struct tndb *db;

    voff0 = creat_with_vlen(path, 5);
    for (unsigned k = 0; k < sizeof(hdr_offs) / sizeof(hdr_offs[0]); k++) {
        creat_with_vlen(path, hdr_offs[k] - voff0);
        if (!extend_with_record_at(path, hdr_offs[k])) {
            unlink(path);
            return;
        }

        db = tndb_open(path);
        expect_notnull(db);
        expect_int(tndb_it_start(db, &it), 1);
        expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), 1);
        expect_int(voff, voff0);
        expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), -1);
        /* the voff and vlen of the record before are not left behind */
        expect_int(voff, 0);
        expect_int(vlen, 0);
        expect_dead(&it);
        expect_null(tndb_keys(db));
        expect_int(tndb_close(db), 1);
    }
    unlink(path);
}
END_TEST

/* tndb_it_get_end() skips a value the caller did not read; in a file cut
   inside the last value the skip fails, though a seek past the end does not */
START_TEST(test_iterator_get_end_skip)
{
    char *path = NTEST_TMPPATH("tndb_it_end.db");
    char key[TNDB_KEY_MAX + 1];
    unsigned int klen, vlen;
    uint32_t cvoff;
    struct tndb_it it;
    struct tndb *db;
    int n, rc;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "x", 1), 1);
    expect_int(tndb_put(db, "b", 1, "y", 1), 1);
    expect_int(tndb_put(db, "c", 1, "zzzzzz", 6), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "c", 1, &cvoff, &vlen), 1);
    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; (rc = tndb_it_get_begin(&it, key, &klen, &vlen)) > 0; n++)
        expect_int(tndb_it_get_end(&it), 1);
    expect_int(rc, 0);
    expect_int(vlen, 0);        /* not the 6 of "c" */
    expect_int(n, 3);
    expect_int(tndb_close(db), 1);

    /* a failed skip stops the iteration as a broken record does */
    expect_int(truncate(path, cvoff + 3), 0);
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; tndb_it_get_begin(&it, key, &klen, &vlen) > 0; n++) {
        rc = tndb_it_get_end(&it);
        expect_int(rc, n < 2);
        if (!rc)
            break;
    }
    expect_int(n, 2);
    expect_str(key, "c");
    expect_dead(&it);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

/* a caller that read past the value has lost its place: tndb_it_get_end()
   reports it with n_die() and, when the die hook returns, the iteration
   stops (the next tndb_it_get_begin() hit its n_assert() there) */
START_TEST(test_iterator_get_end_past_value)
{
    char *path = NTEST_TMPPATH("tndb_it_getend_past.db");
    char key[TNDB_KEY_MAX + 1], buf[16];
    unsigned int klen, vlen;
    t_fn_die_hook old_hook;
    struct tndb_it it;
    struct tndb *db;
    tn_stream *st;
    int rc;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "value", 5), 1);
    expect_int(tndb_put(db, "b", 1, "bvalue", 6), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    st = tndb_tn_stream(db);
    expect_int(tndb_it_start(db, &it), 1);
    expect_int(tndb_it_get_begin(&it, key, &klen, &vlen), 1);
    expect_str(key, "a");
    expect_int(n_stream_read(st, buf, vlen + 1), (int)vlen + 1);

    old_hook = n_die_set_hook(returning_die_hook);
    die_calls = 0;
    errno = 0;
    rc = tndb_it_get_end(&it);
    n_die_set_hook(old_hook);
    expect_int(rc, 0);
    expect_int(die_calls, 1);
    expect_int(errno, EBADMSG);

    /* not "b": the iteration is over */
    vlen = 7;
    expect_int(tndb_it_get_begin(&it, key, &klen, &vlen), 0);
    expect_str(key, "");
    expect_int(klen, 0);
    expect_int(vlen, 0);
    expect_dead(&it);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

/* 0 only for a database read to its end: a reader that cannot read a
   record of a file cut short returns -1, which stops the iteration */
START_TEST(test_iterator_end)
{
    char *path = NTEST_TMPPATH("tndb_it_end_cut.db");
    char key[TNDB_KEY_MAX + 1], buf[16];
    unsigned int klen, vlen;
    uint32_t voff, cvoff;
    struct tndb_it it;
    struct tndb *db;
    tn_array *keys;
    void *rbuf;
    int n, rc;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "x", 1), 1);
    expect_int(tndb_put(db, "b", 1, "y", 1), 1);
    expect_int(tndb_put(db, "c", 1, "zzzzzz", 6), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "c", 1, &cvoff, &vlen), 1);
    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; (rc = tndb_it_get_voff(&it, key, &klen, &voff, &vlen)) > 0; n++)
        ;
    expect_int(rc, 0);
    expect_int(n, 3);
    /* and stays at the end */
    expect_int(tndb_it_get_voff(&it, key, &klen, &voff, &vlen), 0);
    keys = tndb_keys(db);
    expect_notnull(keys);
    expect_int(n_array_size(keys), 3);
    n_array_free(keys);
    expect_int(tndb_close(db), 1);

    /* cut in the value of "c": the readers of values stop at it, the
       others at the end, where the file does not hold it */
    expect_int(truncate(path, cvoff + 3), 0);
    db = tndb_open(path);
    expect_notnull(db);

    expect_int(tndb_it_start(db, &it), 1);
    rbuf = NULL;
    vlen = 0;
    for (n = 0; (rc = tndb_it_rget(&it, key, &klen, &rbuf, &vlen)) > 0; n++)
        ;
    free(rbuf);
    expect_int(rc, -1);
    expect_int(errno, EBADMSG);
    expect_int(n, 2);
    expect_dead(&it);

    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; (rc = tndb_it_get_voff(&it, key, &klen, &voff, &vlen)) > 0; n++)
        ;
    expect_int(rc, -1);
    expect_int(n, 3);
    expect_dead(&it);
    expect_null(tndb_keys(db));

    expect_int(tndb_it_start(db, &it), 1);
    n = 0;
    vlen = sizeof(buf);
    while ((rc = tndb_it_get(&it, key, &klen, buf, &vlen)) > 0) {
        n++;
        vlen = sizeof(buf);
    }
    expect_int(rc, -1);
    expect_int(n, 2);
    expect_dead(&it);

    /* tndb_it_start() begins again */
    expect_int(tndb_it_start(db, &it), 1);
    vlen = sizeof(buf);
    expect_int(tndb_it_get(&it, key, &klen, buf, &vlen), 1);
    expect_str(buf, "x");
    expect_int(tndb_close(db), 1);

    /* cut in the value length of "c" */
    expect_int(truncate(path, cvoff - 2), 0);
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; (rc = tndb_it_get_voff(&it, key, &klen, &voff, &vlen)) > 0; n++)
        ;
    expect_int(rc, -1);
    expect_int(n, 2);
    expect_null(tndb_keys(db));
    expect_int(tndb_close(db), 1);

    /* cut before the key length of "c": the header counts a record more */
    expect_int(truncate(path, cvoff - 4 - 1 - 1), 0);
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    for (n = 0; (rc = tndb_it_get_voff(&it, key, &klen, &voff, &vlen)) > 0; n++)
        ;
    expect_int(rc, -1);
    expect_int(n, 2);
    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

/* a value longer than the caller's buffer is -1 with ERANGE like any other
   error: no n_die(), the size of the buffer is kept, the iteration stops */
START_TEST(test_iterator_get_small_buffer)
{
    char *path = NTEST_TMPPATH("tndb_it_small.db");
    char key[TNDB_KEY_MAX + 1], buf[16];
    unsigned int klen, vlen;
    t_fn_die_hook old_hook;
    struct tndb_it it;
    struct tndb *db;
    int rc;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "x", 1), 1);
    expect_int(tndb_put(db, "b", 1, "yyyyyyyy", 8), 1);
    expect_int(tndb_put(db, "c", 1, "z", 1), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    vlen = sizeof(buf);
    expect_int(tndb_it_get(&it, key, &klen, buf, &vlen), 1);

    old_hook = n_die_set_hook(returning_die_hook);
    die_calls = 0;
    vlen = 8;                   /* no room for the '\0' */
    errno = 0;
    rc = tndb_it_get(&it, key, &klen, buf, &vlen);
    n_die_set_hook(old_hook);
    expect_int(rc, -1);
    expect_int(errno, ERANGE);
    expect_int(die_calls, 0);
    expect_int(vlen, 0);
    expect_str(buf, "");
    expect_str(key, "");
    expect_dead(&it);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

/* a value that does not fit must not be written: the ERANGE stops the
   iteration (vlen is left as the buffer size, so a caller that ignores the
   result cannot make the next call read into a buffer it does not have) */
START_TEST(test_iterator_get_small_buffer_untouched)
{
    char *path = NTEST_TMPPATH("tndb_it_small_w.db");
    /* the buffer the library is told about, and room behind it to look at */
    struct { char buf[8]; unsigned char guard[64]; } b;
    /* shorter than the guard: a write of it stays in b */
    char key[TNDB_KEY_MAX + 1], val[32];
    unsigned int klen, vlen;
    t_fn_die_hook old_hook;
    struct tndb_it it;
    struct tndb *db;
    int i, rc;

    memset(val, 'y', sizeof(val));
    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "a", 1, "x", 1), 1);
    expect_int(tndb_put(db, "b", 1, val, sizeof(val)), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    vlen = sizeof(b.buf);
    expect_int(tndb_it_get(&it, key, &klen, b.buf, &vlen), 1);

    memset(&b, 'g', sizeof(b));
    old_hook = n_die_set_hook(returning_die_hook);
    die_calls = 0;
    vlen = sizeof(b.buf);
    errno = 0;
    rc = tndb_it_get(&it, key, &klen, b.buf, &vlen);
    expect_int(rc, -1);
    expect_int(errno, ERANGE);
    expect_int(vlen, 0);                /* the size is not grown */
    expect_int(b.buf[0], '\0');         /* an empty value */
    b.buf[0] = 'g';

    /* the iteration stopped: the next call ends it */
    vlen = sizeof(b.buf);
    rc = tndb_it_get(&it, key, &klen, b.buf, &vlen);
    n_die_set_hook(old_hook);
    expect_int(rc, 0);
    expect_int(die_calls, 0);

    for (i = 0; i < (int)sizeof(b); i++)
        fail_unless(((const unsigned char *)&b)[i] == 'g',
                    "tndb_it_get() wrote into the buffer at %d", i);

    expect_int(tndb_close(db), 1);
    unlink(path);
}
END_TEST

/* the loops of poldek test the result for truth and use what the reader
   left behind; a value cut short gives them an empty record, then 0 */
START_TEST(test_iterator_truth_test_caller)
{
    char *path = NTEST_TMPPATH("tndb_it_truth.db");
    char key[TNDB_KEY_MAX + 1];
    unsigned int klen, vlen, vlen_max;
    uint32_t cvoff;
    t_fn_die_hook old_hook;
    struct tndb *db;
    struct tndb_it it;
    char *val;
    int n, empty;

    unlink(path);
    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "_1a", 3, "/usr/bin", 8), 1);
    expect_int(tndb_put(db, "_1b", 3, "/usr/share/doc/b", 16), 1);
    expect_int(tndb_close(db), 1);
    db = tndb_open(path);
    expect_int(tndb_get_voff(db, "_1b", 3, &cvoff, &vlen), 1);
    expect_int(tndb_close(db), 1);
    expect_int(truncate(path, cvoff + 3), 0);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_it_start(db, &it), 1);
    memset(key, 'k', sizeof(key));      /* no '\0' from an earlier record */
    vlen = vlen_max = 8;
    val = n_malloc(vlen);
    memset(val, 'v', vlen);
    n = empty = 0;
    old_hook = n_die_set_hook(returning_die_hook);
    while (tndb_it_rget(&it, key, &klen, (void **)&val, &vlen)) {
        fail_unless(n++ < 3, "the loop does not end");
        val[vlen] = '\0';               /* pkgdir_dirindex.c does this */
        fail_unless(klen == strlen(key), "klen %u, key %zu", klen, strlen(key));
        fail_unless(vlen == strlen(val), "vlen %u, val %zu", vlen, strlen(val));
        if (klen == 0 && vlen == 0)
            empty++;
        if (vlen < vlen_max)
            vlen = vlen_max;
        else
            vlen_max = vlen;
    }
    n_die_set_hook(old_hook);
    expect_int(n, 2);                   /* "_1a" and the empty one */
    expect_int(empty, 1);
    free(val);
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
             test_get_voff,
             test_iterator_empty_value,
             test_lookup_value_past_offsets,
             test_lookup_header_past_offsets,
             test_iterator_value_past_offsets,
             test_iterator_header_past_offsets,
             test_iterator_get_end_skip,
             test_iterator_get_end_past_value,
             test_iterator_end,
             test_iterator_get_small_buffer,
             test_iterator_get_small_buffer_untouched,
             test_iterator_truth_test_caller
);
