/* $Id$ */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include <trurl/nmalloc.h>


#include "test.h"

#define DBNAME "tmp.tndb"
#define DBNAMEZ "tmp.tndb.gz"

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

void unlink_test_db(void) 
{
    unlink(DBNAME);
    unlink(DBNAMEZ);
}

int do_test_empty(const char *name)
{
    struct tndb *db;
    
    db = tndb_creat(name, -1, TNDB_SIGN_DIGEST);
    fail_if(db == NULL, "database open failed %s",  name);

    fail_if(!tndb_close(db), "database close failed");

    db = tndb_open(name);
    fail_if(db == NULL, "cannot open created empty database %s",  name);
    
    tndb_close(db);
    return 1;
}

START_TEST (test_empty)
{
    unlink_test_db();
    do_test_empty(DBNAME);
    unlink_test_db();
    do_test_empty(DBNAMEZ);
    unlink_test_db();
}
END_TEST


int do_test_creat(const char *name, int items, int size) 
{
    struct tndb *db;
    char *value;
    int i, valsize = 0;

    if (size == 0)
        size = 256;
    else
        valsize = size;
    
    value = n_malloc(size);
    
    //printf("\n\nCreating %s with %d records...", name, items);
    //fflush(stdout);

    unlink(name);
    db = tndb_creat(name, -1, TNDB_SIGN_DIGEST);
    fail_if(db == NULL, "database open failed %s",  name);

    for (i = 0; i < items; i++) {
        char key[40], *fmt = "val%%.%dd", valfmt[256];
        int kn, vn;
        
        kn = snprintf(key, sizeof(key), "key%.8d", i);
        snprintf(valfmt, sizeof(valfmt), fmt, i);
        
        vn = snprintf(value, size, valfmt, i);
        if (valsize == 0)
            valsize = vn;       /* size == 0 ? use real length */

        if (!tndb_put(db, key, kn, value, valsize))
            fail("tndb_put failed");

        if (i % (items / 5) == 0) {
            //printf("%d..", i);
            fflush(stdout);
        }
    }
    //printf("%d\n", i);
    
    fail_if(!tndb_close(db), "database close failed");
    return 1;
}

START_TEST (test_creat)
{
    do_test_creat(DBNAME, 1000, 0);
    do_test_creat(DBNAMEZ, 1000, 0);

    do_test_creat(DBNAME, 10, 1024 * 10);
    do_test_creat(DBNAMEZ, 10, 1024 * 10);

    unlink_test_db();
    
}
END_TEST


int test_lookup(const char *name, int items) 
{
    int i;
    uint32_t vlen;
    off_t voffs;
    struct tndb *db;

    do_test_creat(name, 1025, 128);
    
    
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




struct test_suite test_suite_base = {
    "base", 
    {
        { "empty", test_empty },
        { "creat", test_creat },
        { NULL, NULL }
    }
};

