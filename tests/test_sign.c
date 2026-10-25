/*
  Tests for the OpenPGP signature slot in the tndb header.

  The real signing/verification is done by the caller; these tests use a
  deterministic stand-in that signs the SHA-256 digest.
*/
#include <config.h>

#include "test_sign_common.h"

static int test_bad_verify_fn(const unsigned char *digest,
                              size_t digest_len, const void *sig,
                              size_t sig_len, void *data)
{
    (void)digest;
    (void)digest_len;
    (void)sig;
    (void)sig_len;
    (void)data;
    return 0;
}

START_TEST(test_signed_header)
{
    char *path = NTEST_TMPPATH("tndb-sign.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_DIGEST | TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_signature_flags(db) & TNDB_SIGN_OPENPGP,
               TNDB_SIGN_OPENPGP);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 1);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_signed_header_requires_verifier)
{
    char *path = NTEST_TMPPATH("tndb-sign-noverify.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_DIGEST | TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP, NULL, NULL), 0);
    expect_int(tndb_verify_signature(db, 0, test_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_signed_header_rejects_bad_signature)
{
    char *path = NTEST_TMPPATH("tndb-sign-bad.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_DIGEST | TNDB_SIGN_OPENPGP, 1);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_bad_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_signed_header_requires_signer)
{
    char *path = NTEST_TMPPATH("tndb-sign-nosigner.db");
    struct tndb *db;

    unlink(path);

    db = create_db(path, TNDB_SIGN_DIGEST | TNDB_SIGN_OPENPGP, 0);
    expect_int(tndb_close(db), 0);
    unlink(path);
}
END_TEST

START_TEST(test_old_signed_db_still_verifies)
{
    char *path = NTEST_TMPPATH("tndb-sign-old.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_DIGEST, 0);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_signature_flags(db), TNDB_SIGN_DIGEST);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 0);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_sha256_only_implies_digest)
{
    char *path = NTEST_TMPPATH("tndb-sign-sha256-only.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = create_db(path, TNDB_SIGN_SHA256, 0);
    expect_int(tndb_close(db), 1);

    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_signature_flags(db),
               TNDB_SIGN_DIGEST | TNDB_SIGN_SHA256);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_SHA256, NULL, NULL), 1);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_openpgp_only_implies_digest_and_sha256)
{
    char *path = NTEST_TMPPATH("tndb-sign-openpgp-only.db");
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
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_verify_signature(db, TNDB_SIGN_OPENPGP,
                                     test_verify_fn, NULL), 1);
    expect_int(tndb_close(db), 1);

    unlink(path);
    unlink(md5path);
}
END_TEST

NTEST_RUNNER("tndb sign",
             test_signed_header,
             test_signed_header_requires_verifier,
             test_signed_header_rejects_bad_signature,
             test_signed_header_requires_signer,
             test_old_signed_db_still_verifies,
             test_sha256_only_implies_digest,
             test_openpgp_only_implies_digest_and_sha256);
