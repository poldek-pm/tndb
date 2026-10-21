/* tndb_verify() takes the .md5 next to a database for granted only while
   the database keeps the size and mtime it had when the .md5 was written */
#include <config.h>

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <trurl/n_check.h>

#include "tndb.h"
#include "tndb_int.h"

static void create_verified(const char *path)
{
    struct tndb *db;
    char md5path[PATH_MAX + 4];

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
    char md5path[PATH_MAX + 4];

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
    char md5path[PATH_MAX + 4];
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

/* tndb_verify() trusts a .md5 whose key matches without comparing the hash
   or reading the database.  Corrupting the hash and moving the header's data
   offset past EOF makes both fallbacks fail; verify_digest() would also leave
   hdr->sign zeroed (it returns before restoring it), so an unchanged
   hdr->sign proves it was not called. */
START_TEST(test_md5_key_match_skips_verification)
{
    char *path = NTEST_TMPPATH("tndb_verify_key_match.db");
    char md5path[PATH_MAX + 4];
    struct tndb *db;
    unsigned char sig[sizeof(((struct tndb_hdr *)0)->sign.md)];
    char line[256];
    struct stat st;
    uint32_t doffs;
    FILE *fp;

    create_verified(path);
    snprintf(md5path, sizeof(md5path), "%s.md5", path);

    /* leave the key alone: if verify_md5() compared the hash it would fail */
    fp = fopen(md5path, "r+");
    expect_notnull(fp);
    expect_notnull(fgets(line, sizeof(line), fp));
    line[0] = (line[0] == '0') ? '1' : '0';
    rewind(fp);
    expect_int(fputs(line, fp) >= 0, 1);
    expect_int(fclose(fp), 0);

    db = tndb_open(path);
    expect_notnull(db);
    memcpy(sig, db->hdr.sign.md, sizeof(sig));

    /* if verify_digest() is called, the hash table read runs past EOF and
       returns before hdr->sign is restored */
    expect_int(stat(path, &st), 0);
    doffs = db->hdr.doffs;
    db->hdr.doffs = st.st_size + 4096;

    expect_int(tndb_verify(db), 1);
    expect_int(memcmp(sig, db->hdr.sign.md, sizeof(sig)), 0);

    db->hdr.doffs = doffs;
    expect_int(tndb_close(db), 1);
    remove_verified(path);
}
END_TEST

/* Force a change between stat_key() and the hash: the database path is a
   FIFO when stat_key() runs, and fopen() blocks until the test puts a
   regular file back at that path.  The hash is fed the old content, so
   only the second stat_key() can tell that the file changed. */
static volatile sig_atomic_t race_child = -1;

static void race_alarm(int sig)
{
    (void)sig;
    if (race_child > 0)
        kill((pid_t)race_child, SIGKILL);
    _exit(2);
}

static int verify_with_path_swap(struct tndb *db, const char *path,
                                 const void *feed, size_t feed_size)
{
    char orig[PATH_MAX];
    pid_t pid;
    int fd, status;

    snprintf(orig, sizeof(orig), "%s.orig", path);

    expect_int(rename(path, orig), 0);
    expect_int(mkfifo(path, 0600), 0);

    pid = fork();
    expect_int(pid >= 0, 1);
    if (pid == 0) {
        int rc = tndb_verify(db);
        _exit(rc == 0 ? 0 : 1);
    }

    race_child = pid;
    signal(SIGALRM, race_alarm);
    alarm(10);

    /* returns once the child has opened the FIFO -- after its stat_key() */
    fd = open(path, O_WRONLY);
    alarm(0);
    if (fd < 0) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    expect_int(fd >= 0, 1);

    /* the second stat_key() must see a regular file with another key */
    expect_int(unlink(path), 0);
    expect_int(rename(orig, path), 0);

    expect_int(write(fd, feed, feed_size), (int)feed_size);
    close(fd);

    expect_int(waitpid(pid, &status, 0), pid);
    expect_int(WIFEXITED(status), 1);
    return WEXITSTATUS(status);
}

START_TEST(test_make_md5_stat_race)
{
    char path[PATH_MAX], md5path[PATH_MAX + 5];
    struct tndb *db;
    unsigned char *buf;
    long size;
    FILE *fp;

    snprintf(path, sizeof(path), "%s", NTEST_TMPPATH("tndb_verify_race.db"));
    snprintf(md5path, sizeof(md5path), "%s.md5", path);

    unlink(path);
    unlink(md5path);

    db = tndb_creat(path, -1, 0);
    expect_notnull(db);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 1);

    /* keep the content to feed through the FIFO */
    fp = fopen(path, "rb");
    expect_notnull(fp);
    expect_int(fseek(fp, 0, SEEK_END), 0);
    size = ftell(fp);
    expect_int(size > 0, 1);
    expect_int(fseek(fp, 0, SEEK_SET), 0);
    buf = malloc(size);
    expect_notnull(buf);
    expect_int(fread(buf, 1, size, fp), (int)size);
    fclose(fp);

    db = tndb_open(path);
    expect_notnull(db);

    expect_int(verify_with_path_swap(db, path, buf, size), 0);

    /* a .md5 keyed by the FIFO state must not be left behind */
    expect_int(access(md5path, F_OK), -1);

    tndb_close(db);
    free(buf);
    unlink(path);
    unlink(md5path);
}
END_TEST

START_TEST(test_verify_md5_stat_race)
{
    char path[PATH_MAX], md5path[PATH_MAX + 5];
    struct tndb *db;
    unsigned char *buf, c;
    long size;
    FILE *fp;
    int fd;

    snprintf(path, sizeof(path), "%s", NTEST_TMPPATH("tndb_verify_md5_race.db"));
    snprintf(md5path, sizeof(md5path), "%s.md5", path);

    unlink(path);
    unlink(md5path);

    db = tndb_creat(path, -1, TNDB_SIGN_DIGEST);
    expect_notnull(db);
    expect_int(tndb_put(db, "key", 3, "value", 5), 1);
    expect_int(tndb_close(db), 1);

    /* verify once: this writes the .md5 of the intact database */
    db = tndb_open(path);
    expect_notnull(db);
    expect_int(tndb_verify(db), 1);
    expect_int(tndb_close(db), 1);

    /* keep the intact content for the FIFO */
    fp = fopen(path, "rb");
    expect_notnull(fp);
    expect_int(fseek(fp, 0, SEEK_END), 0);
    size = ftell(fp);
    expect_int(size > 1, 1);
    expect_int(fseek(fp, 0, SEEK_SET), 0);
    buf = malloc(size);
    expect_notnull(buf);
    expect_int(fread(buf, 1, size, fp), (int)size);
    fclose(fp);

    /* corrupt a byte covered by the digest, keeping the header intact */
    fd = open(path, O_RDWR);
    expect_int(fd >= 0, 1);
    expect_int(lseek(fd, -1, SEEK_END) >= 0, 1);
    expect_int(read(fd, &c, 1), 1);
    expect_int(lseek(fd, -1, SEEK_END) >= 0, 1);
    c ^= 0xff;
    expect_int(write(fd, &c, 1), 1);
    close(fd);

    /* keep the key different even on a filesystem with 1s mtime */
    set_mtime(path, (struct timespec){ .tv_sec = 1, .tv_nsec = 0 });

    db = tndb_open(path);
    expect_notnull(db);

    /* the hash of the intact content still matches the .md5, but the file
       changed between stat_key() and hashing: not verified */
    expect_int(verify_with_path_swap(db, path, buf, size), 0);

    tndb_close(db);
    free(buf);
    unlink(path);
    unlink(md5path);
}
END_TEST

NTEST_RUNNER("tndb-verify",
             test_changed_within_the_same_second,
             test_cut_with_its_mtime_kept,
             test_md5_file_of_an_older_tndb,
             test_md5_key_match_skips_verification,
             test_make_md5_stat_race,
             test_verify_md5_stat_race
);
