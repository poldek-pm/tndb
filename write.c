/*
  Copyright (C) 2002 Pawel A. Gajda <mis@pld.org.pl>

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU Library General Public License, version 2
  as published by the Free Software Foundation (see file COPYING for details).

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <trurl/nmalloc.h>
#include <trurl/narray.h>
#include <trurl/nassert.h>

#define ENABLE_TRACE 0
#include "tndb_int.h"
#include "tndb.h"


static int st_write_hook_write(const void *buf, size_t size, void *arg) 
{
    struct tndb_sign *sign = arg;
    tndb_sign_update(sign, buf, size);
    return 1;
}

static int st_write_hook_nowrite(const void *buf, size_t size, void *arg) 
{
    struct tndb_sign *sign = arg;
    tndb_sign_update(sign, buf, size);
    return 0;
}

struct tndb *tndb_creat(const char *name, int comprlevel, unsigned flags)
{
    char                path[PATH_MAX], mode[32] = "w+b";
    tn_stream           *st;
    struct tndb         *db = NULL;
    int                 fd, n, type = TN_STREAM_STDIO;
    
    snprintf(path, sizeof(path), "%s.tmpXXXXXX", name);

#ifdef HAVE_MKSTEMP
    fd = mkstemp(path);
#else
    fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
#endif
    if (fd < 0)
        return NULL;
    
    rmdir(path);
    unlink(path);

    n = strlen(name);
    if (n > 3 && strcmp(&name[n - 3], ".gz") == 0) {
        type = TN_STREAM_GZIO;
        if (comprlevel >= 0 && comprlevel < 10)
            snprintf(mode, sizeof(mode), "w+b%d", comprlevel);
    }
    
    if ((st = n_stream_dopen(fd, mode, type)) == NULL)
        return NULL;
    
    db = tndb_new(flags);
    db->rtflags |= TNDB_R_MODE_W;
    db->st = st;
    db->path = n_strdupl(path, n);
    
    if (db->hdr.flags & TNDB_SIGN_DIGEST)
        n_stream_set_write_hook(st, st_write_hook_write, &db->hdr.sign);
    
    return db;
}

static inline int put_key(struct tndb *db, const char *key, size_t klen_)
{
    uint32_t               hv, hv_i;
    tn_array               *ht;
    struct tndb_hent       *he;
    uint8_t                klen;

    n_assert(db->rtflags & TNDB_R_MODE_W);

    if (klen_ > UINT8_MAX)
        n_die("Key is too long (max is %d)\n", UINT8_MAX);
    klen = klen_;

    if ((db->hdr.flags & TNDB_NOHASH) == 0) {
        hv = tndb_hash(key, klen);
        hv_i = hv & 0xff;
        ht = db->htt[hv_i];
        
        if (ht == NULL) {
            ht = n_array_new(128, tndb_hent_free, (tn_fn_cmp)tndb_hent_cmp_store);
            db->htt[hv_i] = ht;
        }
    
        he = tndb_hent_new(db, hv, db->offs.current);
        if (hv_i == 50)
            DBGF("addh[%d][%d] %s %u %u\n", hv_i, n_array_size(ht),
                 key, he->val, he->offs);
        n_array_push(ht, he);
    }
    
    n_assert(sizeof(klen) == 1);
    db->offs.current += sizeof(klen) + klen;

    if (n_stream_write(db->st, &klen, 1) != 1)
        return 0;

    if (n_stream_write(db->st, key, klen) != klen)
        return 0;

    return 1;
}


int tndb_put(struct tndb *db, const char *key, size_t klen_,
             const void *val, size_t vlen)
{

    if (!put_key(db, key, klen_))
        return 0;

    if (!n_stream_write_uint32(db->st, vlen))
        return 0;

    if (n_stream_write(db->st, val, vlen) != (int)vlen)
        return 0;

    db->offs.current += sizeof(vlen) + vlen;
    db->hdr.nrec++;
    return 1;
}


static uint32_t htt_store_size(struct tndb *db) 
{
    uint32_t size;
    int i;

    if (db->hdr.flags & TNDB_NOHASH)
        return 0;
    
    size = TNDB_HTBYTESIZE; 
    
    for (i=0; i < TNDB_HTSIZE; i++) {
        tn_array *hash0 = db->htt[i];

        if (hash0 == NULL || n_array_size(hash0) == 0)
            size += sizeof(uint32_t); /* 0 */
            
        else {
            size += sizeof(uint32_t); /* table size */
            /* val + offs */
            size += n_array_size(hash0) * (2 * sizeof(uint32_t)); 
        }
    }
    
    return size;
}


static int htt_write(struct tndb *db)
{
    int i, j;
    uint32_t data_offs, htt_size, ht_offs;

    n_assert((db->hdr.flags & TNDB_NOHASH) == 0);

    htt_size = htt_store_size(db);
    data_offs = tndb_hdr_store_sizeof(&db->hdr) + htt_size;
    
    ht_offs = tndb_hdr_store_sizeof(&db->hdr) + TNDB_HTBYTESIZE;
    //printf("data_offset %x\n", data_offs);
    DBGF("start at %ld, data_offs %d, ht_offs %d\n",
         n_stream_tell(db->st), data_offs, ht_offs);
    
    for (i=0; i < TNDB_HTSIZE; i++) {
        tn_array *ht = db->htt[i];
        
        if (ht == NULL) {
            if (!n_stream_write_uint32(db->st, 0))
                return 0;

            ht_offs += sizeof(uint32_t);
            
        } else {
            n_assert(n_array_size(ht) > 0);
            if (!n_stream_write_uint32(db->st, ht_offs))
                return 0;
            
            DBGF("w[%d] %d\n", i, ht_offs);
            ht_offs += sizeof(uint32_t); /* table size */
            ht_offs += n_array_size(ht) * (2 * sizeof(uint32_t));
        }
    }

    n_assert(ht_offs == data_offs);
    //DBGF("data_offset = %u\n", data_offs);
    

    for (i=0; i < TNDB_HTSIZE; i++) {
        tn_array *ht = db->htt[i];
        
        if (ht == NULL || n_array_size(ht) == 0) {
            if (!n_stream_write_uint32(db->st, 0))
                return 0;
            
        } else {
            n_array_sort(ht);
            if (!n_stream_write_uint32(db->st, n_array_size(ht)))
                return 0;
            
            for (j = 0; j < n_array_size(ht); j++) {
                struct tndb_hent *he = n_array_nth(ht, j);
                
                
                //if (i == 50)
                DBGF("at %ld h0[%d].h1[%d](%u) (%d+) %d\n",
                     n_stream_tell(db->st), 
                     i, j,
                     he->val, data_offs, he->offs);

                if (!n_stream_write_uint32(db->st, he->val))
                    return 0;
                
                if (!n_stream_write_uint32(db->st, he->offs + data_offs))
                    return 0;
            }
        }
    }

    return 1;
}

static int htt_compute(struct tndb *db)
{
    int rc;
    
    n_assert(db->hdr.flags & TNDB_SIGN_DIGEST);
    n_stream_set_write_hook(db->st, st_write_hook_nowrite, &db->hdr.sign);
    rc = htt_write(db);
    n_stream_set_write_hook(db->st, st_write_hook_write, &db->hdr.sign);
    return rc;
}

static int tndbw_close(struct tndb *db)
{
    size_t nread, ntotal;
    char   buf[1024 * 16];
    int    fdin = -1, fdout = -1, type, rc;

    rc = 0;
    n_assert(db->rtflags & TNDB_R_MODE_W);
    
    n_stream_flush(db->st);
    type = db->st->type;
    
    if ((fdin = dup(db->st->fd)) == -1) 
        return 0;
    
    n_stream_close(db->st);
    db->st = NULL;

    if ((fdout = open(db->path, O_RDWR | O_CREAT | O_TRUNC, 0644)) == -1)
        goto l_end;
    
    if ((db->st = n_stream_dopen(fdout, "wb", type)) == NULL)
        goto l_end;
    
    db->hdr.doffs = tndb_hdr_store_sizeof(&db->hdr) + htt_store_size(db);
    //printf("headers = %d\n", db->hdr.doffs);

    if (db->hdr.flags & TNDB_SIGN_DIGEST) {
        tndb_hdr_compute_digest(&db->hdr);
    
        if ((db->hdr.flags & TNDB_NOHASH) == 0)
            if (!htt_compute(db))
                goto l_end;
    
        
        n_stream_set_write_hook(db->st, NULL, NULL);
        tndb_sign_final(&db->hdr.sign);
    }
    
    if (!tndb_hdr_store(&db->hdr, db->st))
        goto l_end;
    
    if ((db->hdr.flags & TNDB_NOHASH) == 0) {
        if (!htt_write(db))
            goto l_end;
    }
    
    n_stream_flush(db->st);
    if ((fdout = dup(db->st->fd)) == -1) 
        goto l_end;
    
    n_stream_close(db->st);
    db->st = NULL;
    
    if (lseek(fdin, 0, SEEK_SET) == -1)
        goto l_end;
    
    if (lseek(fdout, 0, SEEK_END) == -1)
        goto l_end;
    
    ntotal = 0;
    while ((nread = read(fdin, buf, sizeof(buf))) > 0) {
        if (write(fdout, buf, nread) != (int)nread)
            goto l_end;
        ntotal += nread;
    }

    rc = 1;
    
 l_end:
    tndb_free(db);
    
    if (fdin > 0)
        close(fdin);

    if (fdout > 0)
        close(fdout);
    
    return rc;
}


int tndb_close(struct tndb *db)
{
    if (db->_refcnt > 0) {
        db->_refcnt--;
        return 1;
    }

    /* do not save created file if unlinked */
    if (db->rtflags & TNDB_R_MODE_W) {
        if ((db->rtflags & TNDB_R_UNLINKED) == 0)
            return tndbw_close(db);
        
        else if (db->hdr.flags & TNDB_SIGN_DIGEST)
            tndb_sign_final(&db->hdr.sign);
    }

    tndb_free(db);
    return 1;
}


int tndb_unlink(struct tndb *db)
{
    db->rtflags |= TNDB_R_UNLINKED;
    if ((db->rtflags & TNDB_R_MODE_R) && db->path)
        unlink(db->path);

    return 1;
}
