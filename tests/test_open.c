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

NTEST_RUNNER("tndb-open",
             test_not_a_database,
             test_corrupted_signature_block
);
