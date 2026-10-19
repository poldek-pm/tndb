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

NTEST_RUNNER("tndb-open",
             test_not_a_database
);
