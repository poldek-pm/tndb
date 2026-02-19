/* $Id$ */

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/time.h>
#include <trurl/narray.h>
#include <trurl/nmalloc.h>
#include <trurl/n_snprintf.h>
#include <trurl/n_check.h>

#include "compiler.h"
#include "tndb.h"

tn_array *KEYS = NULL;

struct str {
    int len;
    char s[];
};

void load_KEYS(int limit) {
    char buf[1024];

    KEYS = n_array_new(5 * 4096, free, NULL);

    FILE *stream = popen("rpm -qaR --provides -l | cut -f1 -d' ' | sort -u", "r");


    while (fgets(buf, sizeof(buf), stream)) {
        if (limit > 0 && n_array_size(KEYS) > limit) {
            break;
        }

        int len = strlen(buf);
        if (len < 3) {
            continue;
        }
        struct str *st = malloc(sizeof(*st) + len + 1);
        st->len = len;
        strcpy(st->s, buf);
        n_array_push(KEYS, st);
    }
    //fclose(stream);

    printf("Loaded %d keys\n", n_array_size(KEYS));
}

void *timethis_begin(void)
{
    struct timeval *tv;

    tv = n_malloc(sizeof(*tv));
    gettimeofday(tv, NULL);
    return tv;
}

void timethis_end(void *tvp, const char *prefix, const char *suffix)
{
    struct timeval tv, *tv0 = (struct timeval *)tvp;

    (void)prefix;
    (void)suffix;

    gettimeofday(&tv, NULL);

    tv.tv_sec -= tv0->tv_sec;
    tv.tv_usec -= tv0->tv_usec;
    if (tv.tv_usec < 0) {
        tv.tv_sec--;
        tv.tv_usec = 1000000 + tv.tv_usec;
    }

    printf("%ld.%06lds\n", tv.tv_sec, tv.tv_usec);
    free(tvp);
}

int test_creat(const char *name)
{
    int i = 0;
    struct tndb *db;

    printf("[%d] creat %-50s", n_array_size(KEYS), name);
    fflush(stdout);

    if ((db = tndb_creat(name, -1, TNDB_SIGN_DIGEST)) == NULL) {
        perror("tndb_creat");
        n_die("%s: tndb_creat failed", name);
    }

    for (i = 0; i < n_array_size(KEYS); i++) {
        struct str *st = n_array_nth(KEYS, i);

        if (!tndb_put(db, st->s, st->len, st->s, st->len)) {
            perror("tndb_add");
            n_die("%s: tndb_add failed", name);
            return -1;
        }
        //if (i % (items / 5) == 0) {
        //    printf("%d..", i);
        //    fflush(stdout);
        // }
    }

    if (!tndb_close(db)) {
        perror("tndb_close");
        n_die("tndb_close %s failed", name);
    }

    db = tndb_open(name);
    n_assert(db != NULL);
    tndb_close(db);

    return 0;
}

int test_walk(const char *name, int with_keys)
{
    uint32_t vlen;
    uint32_t voffs;
    struct tndb *db;
    struct tndb_it it;
    char key[TNDB_KEY_MAX + 1], val[1024 * 32];
    char *keyp = NULL;
    int rc, nrec;
    unsigned int klen;

    if (with_keys)
        keyp = key;

    if ((db = tndb_open(name)) == NULL) {
        perror("Can't open the database");
        n_die("%s: tndb_open failed", name);
        return -1;
    }

    if (!tndb_verify(db)) {
        tndb_close(db);
        n_die("%s: broken database", name);
        return -1;
    }

    if (!tndb_it_start(db, &it)) {
        tndb_close(db);
        perror("tndb_it_start");
        n_die("%s: tndb_it_start failed", name);
        return -1;
    }


    printf("[%d] scan  %-50s", tndb_size(db), name);
    fflush(stdout);

    nrec = 0;
    tn_array *keys = n_array_new(tndb_size(db), free, NULL);

    while ((rc = tndb_it_get_voff(&it, keyp, &klen, &voffs, &vlen))) {
        char buf[1024 * 32];

        nrec++;

        if (rc < 0) {
            perror("tndb_it_get");
            n_die("%s: tndb_it_get %d failed", name, nrec);
            return -1;
        }

        if (keyp == NULL)
            continue;

        if (klen < 3) {
            n_die("%s: tndb_it_get key error (len %d): %m", name, klen);
            return -1;
        }

        n_array_push(keys, n_strdupl(keyp, klen));

        //printf("key %s %lld\n", key, voffs);
        if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
            perror("Error while reading data\n");
            n_die("%s: tndb_read at %lld failed", name, voffs);
            return -1;
        }

        n_assert(vlen == klen);
        buf[vlen] = '\0';

        if (strncmp(keyp, buf, klen) != 0) {
            n_die("%s: wrong data '%s' != '%s'!", name, val, buf);
            return -1;
        }
    }

    tndb_close(db);
    return 0;
}

int test_lookup(const char *name)
{
    int i;
    uint32_t vlen;
    uint32_t voffs;
    struct tndb *db;

    if ((db = tndb_open(name)) == NULL) {
        perror("Can't open the database");
        n_die("%s: tndb_open failed", name);
        return -1;
    }

    printf("[%d] lookup %-50s", n_array_size(KEYS), name);
    fflush(stdout);

    for (i = 0; i < n_array_size(KEYS); i++) {
        struct str *st = n_array_nth(KEYS, i);
        int rc;

        if ((rc = tndb_get_voff(db, st->s, st->len, &voffs, &vlen)) < 0) {
            n_die("%s: error while reading key %s (%d): %m", name, st->s, rc);
            return -1;

        } else if (rc == 0) {
            n_die("%s: key [%s] not found (rc=%d)", name, st->s, rc);
            return -1;

        } else {
            char buf[1024 * 32];
            if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
                perror("Error while reading data\n");
                n_die("%s: tndb_read at %lld failed", name, voffs);
                return -1;
            }
            buf[vlen] = '\0';
            //printf(" %s\n", buf);

            if (strcmp(buf, st->s) != 0) {
                n_die("%s: wrong data '%s' != '%s'!", name, st->s, buf);
                return -1;
            }
        }
    }

    tndb_close(db);
    return 0;
}


int test_keys(const char *name)
{
    struct tndb *db;

    if ((db = tndb_open(name)) == NULL) {
        perror("Can't open the database");
        n_die("%s: tndb_open failed", name);
        return -1;
    }
    printf("[%d] keys %-50s", n_array_size(KEYS), name);
    tn_array *keys = tndb_keys(db);
    n_assert(keys);
    if (n_array_size(keys) != n_array_size(KEYS)) {
        n_die("loaded %d keys, %d expected\n", n_array_size(keys), n_array_size(KEYS));
    }

    tndb_close(db);
    n_array_free(keys);
    return 0;
}

extern const char *trurl_default_libz(void);
int main(int argc, char *argv[])
{
    char *exts[] = { "none", "gz", "ngz", "zst", 0 };
    if (strcmp(trurl_default_libz(), "zlib-ng") == 0) {
        exts[0] = "none";
        exts[1] = "gz";
        exts[2] = "zst";
        exts[3] = 0;
    }

    char filename[1024];
    int limit = 4 * 4096;

    if (argc > 1 && *argv[1]) {
        int n = 0;
        if (sscanf(argv[1], "%d", &n) == 1) { /* limit */
            limit = n;
        }

        if (argc > 2 && *argv[2]) { /* single compression type */
            exts[0] = argv[1];
            exts[1] = 0;
        }
    }

    load_KEYS(limit);

    int i = 0;

    printf("create\n");
    while (exts[i]) {
        const char *ext = exts[i++];
        n_snprintf(filename, sizeof(filename), "tndb.%s", ext);
        const char *path = NTEST_TMPPATH(filename);

        void *tt = timethis_begin();
        test_creat(path);
        timethis_end(tt, "creat", ext);
        }

    printf("scan\n");
    i = 0;
    while (exts[i]) {
        const char *ext = exts[i++];
        n_snprintf(filename, sizeof(filename), "tndb.%s", ext);
        const char *path = NTEST_TMPPATH(filename);

        void *tt = timethis_begin();
        test_walk(path, 1);
        timethis_end(tt, "creat", ext);
    }

    i = 0;
    printf("lookup\n");
    while (exts[i]) {
        const char *ext = exts[i++];
        n_snprintf(filename, sizeof(filename), "tndb.%s", ext);
        const char *path = NTEST_TMPPATH(filename);

        void *tt = timethis_begin();
        test_lookup(path);
        timethis_end(tt, "lookup", ext);
    }

    i = 0;
    printf("keys\n");
    while (exts[i]) {
        const char *ext = exts[i++];
        n_snprintf(filename, sizeof(filename), "tndb.%s", ext);
        const char *path = NTEST_TMPPATH(filename);

        void *tt = timethis_begin();
        test_keys(path);
        timethis_end(tt, "lookup", ext);
    }

    return 0;
}
