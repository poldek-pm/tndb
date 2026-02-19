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
#include <trurl/nmalloc.h>
#include <trurl/n_snprintf.h>
#include <trurl/n_check.h>

#include "compiler.h"
#include "tndb.h"

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

    gettimeofday(&tv, NULL);

    tv.tv_sec -= tv0->tv_sec;
    tv.tv_usec -= tv0->tv_usec;
    if (tv.tv_usec < 0) {
        tv.tv_sec--;
        tv.tv_usec = 1000000 + tv.tv_usec;
    }

    printf("time [%s.%s] %ld.%ld\n", prefix, suffix, tv.tv_sec, tv.tv_usec);
    free(tvp);
}

int formatted_key(char *val, int vsize, int v)
{
    return n_snprintf(val, vsize, "key%.8d", v);
}


int formatted_value(char *val, int vsize, int v)
{
    char *fmt = "val%%.%dd", valfmt[256];
    snprintf(valfmt, sizeof(valfmt), fmt, v * 2);
    memset(val, 0, vsize);
    return n_snprintf(val, vsize, valfmt, v);
}

int test_creat(const char *name, int items)
{
    int i, data_size = 0;
    struct tndb *db;

    printf("\nCreating %s with %d records...", name, items);
    fflush(stdout);

    if ((db = tndb_creat(name, -1, TNDB_SIGN_DIGEST)) == NULL) {
        perror("tndb_creat");
        n_die("%s: tndb_creat failed", name);
    }

    for (i = 0; i < items; i++) {
	char key[40], val[1024 * 32];
        int kn, vn;

	kn = formatted_key(key, sizeof(key), i);
        vn = formatted_value(val, sizeof(val), i);
        data_size += vn;

        if (!tndb_put(db, key, kn, val, vn)) {
            perror("tndb_add");
            n_die("%s: tndb_add failed", name);
            return -1;
        }
        //if (i % (items / 5) == 0) {
        //    printf("%d..", i);
        //    fflush(stdout);
        // }
    }
    printf("%d (%d KB)\n", i, data_size/1024);

    if (!tndb_close(db)) {
        perror("tndb_close");
        n_die("tndb_close %s failed", name);
    }

    db = tndb_open(name);
    n_assert(db != NULL);
    tndb_close(db);

    return 0;
}

int test_walk(const char *name, int items, int with_keys)
{
    uint32_t vlen;
    uint32_t voffs;
    struct tndb *db;
    struct tndb_it it;
    char key[TNDB_KEY_MAX + 1], val[1024 * 32];
    char *keyp = NULL;
    int rc, nrec;
    unsigned int kn;

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


    printf("Scanning %s...", name);
    fflush(stdout);

    nrec = 0;
    while ((rc = tndb_it_get_voff(&it, keyp, &kn, &voffs, &vlen))) {
        char buf[1024 * 32];
        int i;

        if (items > 5 && 0)
            if (nrec % (items / 5) == 0) {
                printf("%d..", nrec);
                fflush(stdout);
            }
        nrec++;

        if (rc < 0) {
            perror("tndb_it_get");
            n_die("%s: tndb_it_get %d failed", name, nrec);
            return -1;
        }

        if (keyp == NULL)
            continue;

        if (kn < 3) {
            n_die("%s: tndb_it_get key error (len %d): %m", name, kn);
            return -1;
        }

        if (sscanf(key + 3, "%d", &i) != 1) {
            n_die("%s: tndb_it_get key error '%s'\n", name, key);
            return -1;
        }

        //printf("key %s %lld\n", key, voffs);
        if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
            perror("Error while reading data\n");
            n_die("%s: tndb_read at %lld failed", name, voffs);
            return -1;
        }
        buf[vlen] = '\0';

        formatted_value(val, sizeof(val), i);
        if (strcmp(val, buf) != 0) {
            n_die("%s: wrong data '%s' != '%s'!", name, val, buf);
            return -1;
        }
    }

    tndb_close(db);
    printf("%d\n", nrec);
    return 0;
}


int test_lookup(const char *name, int items)
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

    printf("Lookup %s...", name);
    fflush(stdout);
    for (i = 0; i < items + (items/2); i++) {
        //for (i = items + (items/2); i > -1; i--) {
        char key[40], val[1024 * 32];
        int kn, rc;

        if (i % (items / 5) == 0) {
            printf("%d..", i);
            fflush(stdout);
        }

        kn = formatted_key(key, sizeof(key), i);
        formatted_value(val, sizeof(val), i);

        if ((rc = tndb_get_voff(db, key, kn, &voffs, &vlen)) < 0) {
            n_die("%s: error while reading key %s (%d): %m", name, key, rc);
            return -1;


        } else if (rc == 0) {
            if (i < items) {
                n_die("%s: key [%s] not found (rc=%d)", name, key, rc);
                return -1;
            }


        } else {
            char buf[1024 * 32];
            //printf("found %s at %lld\n", key, voffs);

            if (i >= items) {
                n_die("%s: 'ghost' record ('%s') detected!", name, key);
                return -1;
            }

            if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
                perror("Error while reading data\n");
                n_die("%s: tndb_read at %lld failed", name, voffs);
                return -1;
            }
            buf[vlen] = '\0';
            //printf(" %s\n", buf);

            if (strcmp(val, buf) != 0) {
                n_die("%s: wrong data '%s' != '%s'!", name, val, buf);
                return -1;
            }
        }
    }

    printf("%d\n", i);
    tndb_close(db);
    return 0;
}

int test_filedb(const char *name)
{
    FILE *stream;
    char buf[1024];
    int i = 0;
    struct tndb *db;

    stream = popen("rpm -qla", "r");

    printf("\nCreating %s with rpm -qla...", name);
    fflush(stdout);

    if ((db = tndb_creat(name, -1, 0)) == NULL) {
        n_die("%s: tndb_creat failed", name);
        return -1;
    }

    while (fgets(buf, sizeof(buf), stream)) {
        char key[40], val[1024 * 32];
        int kn, vn;

        kn = snprintf(key, sizeof(key), "%d", i);
        vn = strlen(buf);

        if (!tndb_put(db, key, kn, val, vn)) {
            perror("tndb_add");
            n_die("%s: tndb_add failed", name);
            return -1;
        }
        i++;
        if (i % 1000 == 0 && 0) {
            printf("%d..", i);
            fflush(stdout);
        }
    }
    printf("%d\n", i);

    if (!tndb_close(db)) {
        n_die("%s: tndb_close failed", name);
        perror("tndb_close");
    }

    return 0;
}

int test_keys(const char *path, int items)
{
    struct tndb *db;

    if ((db = tndb_open(path)) == NULL) {
        perror("Can't open the database");
        n_die("%s: tndb_open failed", path);
        return -1;
    }


    tn_array *keys = tndb_keys(db);
    n_assert(keys);
    if (n_array_size(keys) != items) {
        n_die("loaded %d keys, %d expected\n", n_array_size(keys), items);
    }

    tndb_close(db);
    n_array_free(keys);
    return 0;
}

extern const char *trurl_default_libz(void);
int main(void)
{
    char *exts[] = { "none", "gz", "zst", 0 };

    int ns[] = { 0, 1000, 10000, -1 };
    char filename[1024];

    int ni = 0;
    while (ns[ni] >= 0) {
        int n = ns[ni++];
        int i = 0;

        while (exts[i]) {
            const char *ext = exts[i++];
            n_snprintf(filename, sizeof(filename), "tndb.%s", ext);
            const char *path = NTEST_TMPPATH(filename);

            void *tt = timethis_begin();
            test_creat(path, n);
            timethis_end(tt, "creat", ext);

            tt = timethis_begin();
            test_walk(path, n, 1);
            timethis_end(tt, "walk", ext);

            tt = timethis_begin();
            test_lookup(path, n);
            timethis_end(tt, "lookup", ext);

            tt = timethis_begin();
            test_keys(path, n);
            timethis_end(tt, "keys", ext);
        }
    }

    return 0;
}
