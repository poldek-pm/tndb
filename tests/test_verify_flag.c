/* A failed tndb_verify() must not leave the database marked as verified:
   the next lookup would then skip verification and read a corrupted value. */
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <trurl/n_check.h>

#include "tndb.h"

START_TEST(test_failed_verify_does_not_trust_database)
{
    struct tndb *db;
    char *key = "signed_key";
    char *val = "signed_value";
    char *path = NTEST_TMPPATH("tndb_verify_flag.db");
    char md5path[PATH_MAX + 4];
    char buf[64];
    uint32_t voffs, vlen;
    FILE *fp;
    int c;

    unlink(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);
    unlink(md5path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_put(db, key, strlen(key), val, strlen(val)), 1);
    expect_int(tndb_close(db), 1);

    /* verify once to learn where the value lives, then close */
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_get_voff(db, key, strlen(key), &voffs, &vlen), 1);
    expect_int(vlen, (int)strlen(val));
    expect_int(tndb_close(db), 1);

    /* corrupt the value in place and drop the .md5 shortcut; corrupting the
       middle of the file instead could hit the hash table, and then the
       lookup below would return 0 even without the bug */
    fp = fopen(path, "r+b");
    expect_notnull(fp);
    expect_int(fseek(fp, (long)voffs, SEEK_SET), 0);
    c = fgetc(fp);
    expect_int(c != EOF, 1);
    expect_int(fseek(fp, (long)voffs, SEEK_SET), 0);
    fputc(c ^ 0xff, fp);
    fclose(fp);
    unlink(md5path);

    db = tndb_open(path);
    expect_notnull(db);

    /* the database is corrupt ... */
    expect_int(tndb_verify(db), 0);

    /* ... and a failed verification must not leave it marked as verified:
       the next lookup has to refuse to read the corrupted value */
    expect_int(tndb_get(db, key, strlen(key), buf, sizeof(buf)), 0);

    expect_int(tndb_close(db), 1);
    unlink(path);
    unlink(md5path);
}
END_TEST

NTEST_RUNNER("tndb-verify-flag",
             test_failed_verify_does_not_trust_database
);
