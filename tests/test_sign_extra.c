/*
  Additional tests for the OpenPGP signature slot: corruption, boundaries
  and digest-only fallback on a PGP database.
*/
#include <config.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "test_sign_common.h"

static int max_sign_fn(const unsigned char *digest, size_t digest_len,
                       void **sig, size_t *sig_len, void *data)
{
    unsigned char *p = malloc(TNDB_PGP_SIG_MAX);

    (void)digest;
    (void)digest_len;
    (void)data;
    if (p == NULL)
        return 0;
    memset(p, 'M', TNDB_PGP_SIG_MAX);
    *sig = p;
    *sig_len = TNDB_PGP_SIG_MAX;
    return 1;
}

static int oversized_sign_fn(const unsigned char *digest, size_t digest_len,
                             void **sig, size_t *sig_len, void *data)
{
    unsigned char *p = malloc(TNDB_PGP_SIG_MAX + 1);

    (void)digest;
    (void)digest_len;
    (void)data;
    if (p == NULL)
        return 0;
    *sig = p;
    *sig_len = TNDB_PGP_SIG_MAX + 1;
    return 1;
}

static int zero_sign_fn(const unsigned char *digest, size_t digest_len,
                        void **sig, size_t *sig_len, void *data)
{
    (void)digest;
    (void)digest_len;
    (void)data;
    *sig = NULL;
    *sig_len = 0;
    return 1;
}

START_TEST(test_digest_only_verify_ignores_pgp_records)
{
    char *path = NTEST_TMPPATH("tndb-sign-digest-only.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_signature_flags(db),
               TNDB_SIGN_DIGEST | TNDB_SIGN_SHA256 | TNDB_SIGN_OPENPGP);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_DIGEST, NULL, NULL), 1);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_data_corruption_fails)
{
    char *path = NTEST_TMPPATH("tndb-sign-corrupt-data.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;
    uint32_t voffs, vlen;
    FILE *fp;
    int c;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "key", 3, &voffs, &vlen), 1);
    expect_int(tndb_close(db), 1);

    unlink(md5path);

    fp = fopen(path, "r+b");
    expect_notnull(fp);
    expect_int(fseek(fp, (long)voffs, SEEK_SET), 0);
    c = fgetc(fp);
    expect_int(c != EOF, 1);
    expect_int(fseek(fp, (long)voffs, SEEK_SET), 0);
    fputc(c ^ 0xff, fp);
    fclose(fp);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 0);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_pgp_corruption_fails_signature_only)
{
    char *path = NTEST_TMPPATH("tndb-sign-corrupt-pgp.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;
    FILE *fp;
    int c;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    /* The data digest does not cover the signature records, so flipping a
       byte inside the pgp/pad area should leave digest verification intact
       while breaking the OpenPGP signature check. */
    fp = fopen(path, "r+b");
    expect_notnull(fp);
    expect_int(fseek(fp, 100, SEEK_SET), 0);
    c = fgetc(fp);
    expect_int(c != EOF, 1);
    expect_int(fseek(fp, 100, SEEK_SET), 0);
    fputc(c ^ 0xff, fp);
    fclose(fp);

    unlink(md5path);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

static void corrupt_byte_preserve_stat(const char *path, long off)
{
    int fd;
    unsigned char c;
    struct stat st;
    struct timespec times[2];

    fd = open(path, O_RDWR);
    expect_int(fd >= 0, 1);
    expect_int(fstat(fd, &st), 0);
    expect_int(pread(fd, &c, 1, off), 1);
    c ^= 0xff;
    expect_int(pwrite(fd, &c, 1, off), 1);
    times[0] = st.st_atim;
    times[1] = st.st_mtim;
    expect_int(futimens(fd, times), 0);
    close(fd);
}

START_TEST(test_pgp_fast_path_uses_md5)
{
    char *path = NTEST_TMPPATH("tndb-sign-fast-path.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;
    uint32_t voffs, vlen;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_get_voff(db, "key", 3, &voffs, &vlen), 1);
    expect_int(tndb_verify(db), 1); /* writes .md5 */
    expect_int(tndb_close(db), 1);

    /* corrupt data but keep size+mtime; .md5 fast path trusts the cache */
    corrupt_byte_preserve_stat(path, (long)voffs);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1); /* fast path */
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    /* drop the cache: embedded digest must now fail */
    unlink(md5path);
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_signature_max_size)
{
    char *path = NTEST_TMPPATH("tndb-sign-max.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = tndb_creat(path, -1, TNDB_SIGN_OPENPGP);
    expect_notnull(db);
    expect_int(tndb_set_sign_fn(db, max_sign_fn, NULL), 1);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_signature_flags(db) & TNDB_SIGN_OPENPGP,
               TNDB_SIGN_OPENPGP);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_signature_too_large)
{
    char *path = NTEST_TMPPATH("tndb-sign-oversized.db");
    struct tndb *db;

    unlink(path);

    db = tndb_creat(path, -1, TNDB_SIGN_OPENPGP);
    expect_notnull(db);
    expect_int(tndb_set_sign_fn(db, oversized_sign_fn, NULL), 1);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 0);

    unlink(path);
}
END_TEST

START_TEST(test_zero_length_signature)
{
    char *path = NTEST_TMPPATH("tndb-sign-zero.db");
    struct tndb *db;

    unlink(path);

    db = tndb_creat(path, -1, TNDB_SIGN_OPENPGP);
    expect_notnull(db);
    expect_int(tndb_set_sign_fn(db, zero_sign_fn, NULL), 1);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 0);

    unlink(path);
}
END_TEST

NTEST_RUNNER("tndb sign extra",
             test_digest_only_verify_ignores_pgp_records,
             test_data_corruption_fails,
             test_pgp_corruption_fails_signature_only,
             test_pgp_fast_path_uses_md5,
             test_signature_max_size,
             test_signature_too_large,
             test_zero_length_signature);
