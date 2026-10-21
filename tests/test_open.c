/* tndb_open() of a file that is not an intact database fails instead of
   aborting, so the caller can report it and fetch the file again */
#include <config.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include <trurl/n_check.h>

#include "tndb.h"

START_TEST(test_not_a_database)
{
    const char *html = "<html><head><title>Not Found</title></head></html>\n";
    char *path = NTEST_TMPPATH("tndb_open_html.db");
    FILE *fp;

    fp = fopen(path, "wb");
    expect_notnull(fp);
    fputs(html, fp);
    fclose(fp);

    errno = 0;
    expect_null(tndb_open(path));
    expect_int(errno, EINVAL);
    unlink(path);
}
END_TEST

/* signed database: "tndb1.0\n", flags (offset 8), signature block size (9),
   then the "md" record: name length (11), name (12), digest length (14) */
START_TEST(test_corrupted_signature_block)
{
    static const struct { long offs; const char *bytes; size_t n; } cases[] = {
        { 8,  "\x00", 1 },         /* flags without TNDB_SIGN_DIGEST */
        { 11, "\xff", 1 },         /* name of 255 bytes */
        { 14, "\xff\xff", 2 },     /* digest length 65535 */
        /* an empty digest, then a record that keeps the block size */
        { 11, "\x02" "md" "\x00\x00" "\x02" "xx" "\x00\x0f" "123456789012345", 25 },
    };
    char *path = NTEST_TMPPATH("tndb_open_sign.db");
    struct tndb *db;
    FILE *fp;

    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
        expect_notnull(db);
        expect_int(tndb_put(db, "key", 3, "value", 5), 1);
        expect_int(tndb_close(db), 1);

        fp = fopen(path, "r+b");
        expect_notnull(fp);
        fseek(fp, cases[i].offs, SEEK_SET);
        expect_int(fwrite(cases[i].bytes, 1, cases[i].n, fp), cases[i].n);
        fclose(fp);

        errno = 0;
        expect_null(tndb_open(path));
        expect_int(errno, EINVAL);
    }
    unlink(path);
}
END_TEST

/* A signature block whose declared size is smaller than the 2-byte size
   field itself: size16 -= sizeof(size16) used to underflow uint16_t and
   the parser read the rest of the file as signature records. */
START_TEST(test_short_signature_block)
{
    static const unsigned bad_sizes[] = { 0, 1 };
    char *path = NTEST_TMPPATH("tndb_open_short_sign.db");
    struct tndb *db;
    unsigned char sz[2];
    FILE *fp;

    for (unsigned i = 0; i < sizeof(bad_sizes) / sizeof(bad_sizes[0]); i++) {
        db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
        expect_notnull(db);
        expect_int(tndb_put(db, "key", 3, "value", 5), 1);
        expect_int(tndb_close(db), 1);

        fp = fopen(path, "r+b");
        expect_notnull(fp);

        /* cut the file right after the signature block, so that the
           underflowed parser hits EOF instead of a bogus record */
        expect_int(fseek(fp, 9, SEEK_SET), 0);
        expect_int(fread(sz, 1, 2, fp), 2);
        expect_int(ftruncate(fileno(fp), 9 + ((sz[0] << 8) | sz[1])), 0);

        sz[0] = (unsigned char)(bad_sizes[i] >> 8);
        sz[1] = (unsigned char)(bad_sizes[i] & 0xff);
        expect_int(fseek(fp, 9, SEEK_SET), 0);
        expect_int(fwrite(sz, 1, 2, fp), 2);
        fclose(fp);

        errno = 0;
        expect_null(tndb_open(path));
        expect_int(errno, EINVAL);
    }
    unlink(path);
}
END_TEST

/* A database that claims TNDB_SIGN_DIGEST but has an empty signature
   block has no "md" record.  It used to open and only fail later, in
   tndb_verify(). */
START_TEST(test_digest_without_md)
{
    char *path = NTEST_TMPPATH("tndb_open_no_md.db");
    struct tndb *db;
    unsigned char flags = TNDB_SIGN_DIGEST;
    FILE *fp;

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 1);

    /* flags are at offset 8; an unsigned database has an empty signature
       block (size16 == 2), so there is no "md" record to keep */
    fp = fopen(path, "r+b");
    expect_notnull(fp);
    expect_int(fseek(fp, 8, SEEK_SET), 0);
    expect_int(fwrite(&flags, 1, 1, fp), 1);
    fclose(fp);

    errno = 0;
    expect_null(tndb_open(path));
    expect_int(errno, EINVAL);
    unlink(path);
}
END_TEST

NTEST_RUNNER("tndb-open",
             test_not_a_database,
             test_corrupted_signature_block,
             test_short_signature_block,
             test_digest_without_md
);
