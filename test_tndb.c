/* $Id$ */
   
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <trurl/nmalloc.h>

#include "tndb.h"

#define NAME  "/tmp/tndb"
#define NAMEZ "/tmp/tndb.gz"

void *timethis_begin(void)
{
    struct timeval *tv;

    tv = n_malloc(sizeof(*tv));
    gettimeofday(tv, NULL);
    return tv;
}

void timethis_end(void *tvp, const char *prefix)
{
    struct timeval tv, *tv0 = (struct timeval *)tvp;

    gettimeofday(&tv, NULL);

    tv.tv_sec -= tv0->tv_sec;
    tv.tv_usec -= tv0->tv_usec;
    if (tv.tv_usec < 0) {
        tv.tv_sec--;
        tv.tv_usec = 1000000 + tv.tv_usec;
    }

    printf("time [%s] %ld.%ld\n", prefix, tv.tv_sec, tv.tv_usec);
    free(tvp);
}


int test_creat(const char *name, int items) 
{
    int i;
    struct tndb *db;
    
    printf("\n\nCreating %s with %d records...", name, items);
    fflush(stdout);
    
    if ((db = tndb_creat(name, TNDB_SIGN_DIGEST)) == NULL) {
        perror("tndb_creat");
        return -1;
    }

    for (i = 0; i < items; i++) {
	char key[40], val[1024 * 32], *fmt = "val%%.%dd", valfmt[256];
        int kn, vn;
        
	kn = snprintf(key, sizeof(key), "key%.8d", i);
        snprintf(valfmt, sizeof(valfmt), fmt, i);
        
        vn = snprintf(val, sizeof(val), valfmt, i);

        if (!tndb_put(db, key, kn, val, vn)) {
            perror("tndb_add");
            return -1;
        }
        if (i % (items / 5) == 0) {
            printf("%d..", i);
            fflush(stdout);
        }
    }
    printf("%d\n", i);
    
    if (!tndb_close(db))
        perror("tndb_close");

    return 0;
}


int test_creat_bigdata(const char *name, int items) 
{
    int i;
    struct tndb *db;
    void *tt;
    
    printf("\n\nCreating %s with %d records...", name, items);
    fflush(stdout);
    
    if ((db = tndb_creat(name, TNDB_SIGN_DIGEST)) == NULL) {
        perror("tndb_creat");
        return -1;
    }

    tt = timethis_begin();
    for (i = 0; i < items; i++) {
        char key[40], val[10 * 1024],
            *fmt = "val%%.%dd", valfmt[256];
        int kn, vn;
        
        kn = snprintf(key, sizeof(key), "key%.8d", i);
        memset(val, 0, sizeof(val));
        snprintf(valfmt, sizeof(valfmt), fmt, i);
        
        vn = snprintf(val, sizeof(val), valfmt, i);

        if (!tndb_put(db, key, kn, val, sizeof(val))) {
            perror("tndb_add");
            return -1;
        }
        if (items > 5)
            if (i % (items / 5) == 0) {
                printf("%d..", i);
                fflush(stdout);
            }
    }
    printf("%d\n", i);
    timethis_end(tt, "write-data");

    tt = timethis_begin();
    if (!tndb_close(db))
        perror("tndb_close");
    timethis_end(tt, "tndb_close");

    return 0;
}


int test_lookup(const char *name, int items) 
{
    int i;
    uint32_t vlen;
    off_t voffs;
    struct tndb *db;
    
    if ((db = tndb_open(name)) == NULL) {
        perror("Can't open the database");
        return -1;
    }
    
    printf("Lookup %s...", name);
    fflush(stdout);
    for (i = 0; i < items + (items/2); i++) {
        //for (i = items + (items/2); i > -1; i--) {
        char key[40], val[1024 * 32], *fmt = "val%%.%dd", valfmt[256];
        int kn, vn, rc;

        if (i % (items / 5) == 0) {
            printf("%d..", i);
            fflush(stdout);
        }
            
        kn = snprintf(key, sizeof(key), "key%.8d", i);
        snprintf(valfmt, sizeof(valfmt), fmt, i);
            
        vn = snprintf(val, sizeof(val), valfmt, i);
            
        if ((rc = tndb_get_voff(db, key, kn, &voffs, &vlen)) < 0) {
            printf("Error while reading key %s (%d): %m\n", key, rc);
            return -1;
                
                
        } else if (rc == 0) {
            if (i < items) {
                printf("Key %s not found (%d)\n", key, rc);
                return -1;
            }
                
                
        } else {
            char buf[1024 * 32];
            //printf("found %s %d %d -> ", str, retpos, retlen);

            if (i >= items) {
                printf("Ghost record '%s' detected!\n", key);
                return -1;
            }
                
            if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
                perror("Error while reading data\n");
                return -1;
            }
            buf[vlen] = '\0';
            //printf(" %s\n", buf);
                
            if (strcmp(val, buf) != 0) {
                printf("Wrong data %s %s!\n", val, buf);
                return -1;
            }
        }
    }
    
    printf("%d\n", i);
    return 0;
}

int test_walk(const char *name, int items, int with_keys) 
{
    uint32_t vlen;
    off_t voffs;
    struct tndb *db;
    struct tndb_it it;
    char key[TNDB_KEY_MAX + 1], val[1024 * 32], *fmt = "val%%.%dd", valfmt[256];
    char *keyp = NULL;
    int kn, vn, rc, nrec;

    if (with_keys)
        keyp = key;
    
    if ((db = tndb_open(name)) == NULL) {
        perror("Can't open the database");
        return -1;
    }

    if (!tndb_verify(db)) {
        printf("%s: BROKEN", name);
        return -1;
    }
    

    if (!tndb_it_start(db, &it)) {
        perror("tndb_it_start");
        return -1;
    }
    
    
    printf("Scanning %s...", name);
    fflush(stdout);
    
    nrec = 0;
    while ((rc = tndb_it_get_voff(&it, keyp, &kn, &voffs, &vlen))) {
        char buf[1024 * 32];
        int i;

        if (items > 5)
            if (nrec % (items / 5) == 0) {
                printf("%d..", nrec);
                fflush(stdout);
            }
        nrec++;
        
        if (rc < 0) {
            perror("tndb_it_get");
            return -1;
        }
        
        if (keyp == NULL)
            continue;
        
        if (kn < 3) {
            printf("tndb_it_get key error (len %d): %m\n", kn);
            return -1;
        }
        
        if (sscanf(key + 3, "%d", &i) != 1) {
            printf("tndb_it_get key error '%s'\n", key);
            return -1;
        }
        
        snprintf(valfmt, sizeof(valfmt), fmt, i);
        vn = snprintf(val, sizeof(val), valfmt, i);
        
        if (tndb_read(db, voffs, buf, vlen) != (int)vlen) {
            perror("Error while reading data\n");
            return -1;
        }
        buf[vlen] = '\0';
                
        if (strcmp(val, buf) != 0) {
            printf("Wrong data %s %s!\n", val, buf);
            return -1;
        }
    }
    
    printf("%d\n", nrec);
    return 0;
}


int test_filedb(const char *name)
{
    FILE *stream;
    char buf[1024];
    int i = 0;
    struct tndb *db;
        
    stream = popen("rpm -qla", "r");


    
    printf("\n\nCreating %s with rpm -qla...", name);
    fflush(stdout);
    
    if ((db = tndb_creat(name, 0)) == NULL) {
        perror("tndb_creat");
        return -1;
    }
    
    while (fgets(buf, sizeof(buf), stream)) {
        char key[40], val[1024 * 32], *fmt = "val%%.%dd", valfmt[256];
        int kn, vn;
        
        kn = snprintf(key, sizeof(key), "%d", i);
        vn = strlen(buf);

        if (!tndb_put(db, key, kn, val, vn)) {
            perror("tndb_add");
            return -1;
        }
        i++;
        if (i % 1000 == 0) {
            printf("%d..", i);
            fflush(stdout);
        }
    }
    printf("%d\n", i);
    
    if (!tndb_close(db))
        perror("tndb_close");

    return 0;
}

        
    
        

int main(void)
{
    int i, n = 1000;
    void *tt;

#if TEST_FILEDB    
    tt = timethis_begin();
    test_filedb(NAMEZ);
    timethis_end(tt, "creat");

    tt = timethis_begin();
    test_walk(NAMEZ, n, 0);
    timethis_end(tt, "walk");
    exit(0);
#endif

#if 0
    n = 200;
    tt = timethis_begin();
    test_creat_bigdata(NAMEZ, n);
    timethis_end(tt, "creat");
#endif    
#if 0    
    tt = timethis_begin();
    test_walk(NAMEZ, n, 0);
    timethis_end(tt, "walk");
    exit(0);
#endif    

    n = 200;
    for (i=0; i<1 ; i++) {
        n += (n * i);
        if (i < 1) {             /* too much space */
            tt = timethis_begin();
            test_creat(NAMEZ, n);
            timethis_end(tt, "creat");

            tt = timethis_begin();
            test_lookup(NAMEZ, n);
            timethis_end(tt, "lookup");
            exit(0);
            tt = timethis_begin();
            test_walk(NAME, n, 1);
            timethis_end(tt, "walk");

            tt = timethis_begin();
            test_walk(NAME, n, 0);
            timethis_end(tt, "walk(no keys)");
        }
        exit(0);
        tt = timethis_begin();
        test_creat(NAMEZ, n);
        timethis_end(tt, "creat");

        tt = timethis_begin();
        test_lookup(NAMEZ, n);
        timethis_end(tt, "lookup");

        tt = timethis_begin();
        test_walk(NAMEZ, n, 1);
        timethis_end(tt, "walk");

        tt = timethis_begin();
        test_walk(NAMEZ, n, 0);
        timethis_end(tt, "walk(no keys)");
    }

    return 0;
}
