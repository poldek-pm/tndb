/*
  Copyright (C) 2002 Pawel A. Gajda <mis@pld.org.pl>

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU Library General Public License, version 2
  as published by the Free Software Foundation (see file COPYING for details).

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

/*
  $Id$
*/

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <openssl/evp.h>

#include <trurl/nmalloc.h>
#include <trurl/nassert.h>
#include <trurl/n_snprintf.h>

#include "tndb.h"
#define ENABLE_TRACE 0
#include "tndb_int.h"

//static char *msg_not_verified = "tndb: %s: refusing read unchecked file\n";

inline int verify_db(struct tndb *db)
{
    if (db->rflags & TNDB_R_SIGN_VRFIED)
        return 1;
    
    if (db->hdr.flags & TNDB_SIGNED)
        return tndb_verify(db);

    n_assert(0);
    db->rflags |= TNDB_R_SIGN_VRFIED;
    return 1;
}


static
int md5(FILE *stream, unsigned char *md, int *md_size)
{
    unsigned char buf[8*1024];
    EVP_MD_CTX ctx;
    int n, nn = 0;


    n_assert(md_size && *md_size);

    EVP_DigestInit(&ctx, EVP_md5());

    while ((n = fread(buf, 1, sizeof(buf), stream)) > 0) {
        EVP_DigestUpdate(&ctx, buf, n);
        nn += n; 
    }
    
    EVP_DigestFinal(&ctx, buf, &n);

    if (n > *md_size) {
        *md = '\0';
        *md_size = 0;
    } else {
        memcpy(md, buf, n);
        *md_size = n;
    }
    
    return *md_size;
}

static
int md5hex(FILE *stream, unsigned char *mdhex, int *mdhex_size)
{
    unsigned char md[128];
    int  md_size = sizeof(md);

    
    if (md5(stream, md, &md_size)) {
        int i, n = 0, nn = 0;
        
        for (i=0; i < md_size; i++) {
            n = n_snprintf(mdhex + nn, *mdhex_size - nn, "%02x", md[i]);
            nn += n;
        }
        *mdhex_size = nn;
        
    } else {
        *mdhex = '\0';
        *mdhex_size = 0;
    }

    return *mdhex_size;
}


#define DIGEST_SIZE_MD5  32

static
int make_md5(const char *pathname) 
{
    FILE            *stream;
    unsigned char   md[128];
    char            path[PATH_MAX];
    int             md_size = sizeof(md);

    
    if ((stream = fopen(pathname, "r")) == NULL)
        return 0;
    
    snprintf(path, sizeof(path), "%s.md5", pathname);

    md5hex(stream, md, &md_size);
    fclose(stream);
    
    if (md_size == DIGEST_SIZE_MD5) {
        FILE *f;
        
        if ((f = fopen(path, "w")) == NULL)
            return 0;
        fprintf(f, "%s", md);
        fclose(f);
    }

    return md_size;
}


int verify_md5(const char *pathname) 
{
    FILE            *stream;
    unsigned char   md1[DIGEST_SIZE_MD5 + 1], md2[DIGEST_SIZE_MD5 + 1];
    int             fd, md1_size, md2_size;
    char            path[PATH_MAX];

    snprintf(path, sizeof(path), "%s.md5", pathname);
    if ((fd = open(path, O_RDONLY)) < 0)
        return 0;
     
    md2_size = read(fd, md2, sizeof(md2));
    close(fd);

    if ((stream = fopen(pathname, "r")) == NULL)
        return 0;
    
    if (md2_size != DIGEST_SIZE_MD5)
        return 0;
    
    
    md1_size = sizeof(md1);
    md5hex(stream, md1, &md1_size);
    fclose(stream);
    
    return (md1_size == DIGEST_SIZE_MD5 && md1_size == md2_size &&
            memcmp(md1, md2, DIGEST_SIZE_MD5) == 0);
}


static
int read_eq(const struct tndb *db, const uint32_t offs, 
            const unsigned char *str, const uint32_t len)
{
    unsigned char *buf;
    
    if ((buf = alloca(len + 1)) == NULL)
        return -1;

    if (nn_stream_read_offs(db->st, buf, len, offs) != (int)len)
        return -1;
    
    buf[len] = '\0';
    DBGF("memcmp[%d] %s == %s ? %d\n", offs, buf, str,
         memcmp(buf, str, len));
    
    return memcmp(buf, str, len) == 0;
}


static int htt_read(struct tndb *db)
{
    int i, j, hdr_stsize;

    hdr_stsize = tndb_hdr_store_size(&db->hdr);
    
    for (i=0; i < TNDB_HTSIZE; i++)
        db->htt[i] = NULL;
    
    for (i=0; i < TNDB_HTSIZE; i++) {
        tn_array *ht = db->htt[i];
        uint32_t ht_offs, ht_size, offs;


        db->htt[i] = NULL;

        offs = hdr_stsize + (sizeof(uint32_t) * i); 
        
        if (!nn_stream_read_uint32_offs(db->st, &ht_offs, offs))
            return 0;
        
        if (ht_offs == 0) {
            //DBGF("empty %d\n", i);
            continue;
        }

        DBGF("r[%d] %d\n", i, ht_offs);
        
        if (!nn_stream_read_uint32_offs(db->st, &ht_size, ht_offs))
            return 0;
        
        if (ht_size == 0) {
            DBGF("empty!? %d\n", i);
            n_assert(0);
            continue;
        }
        
        if (ht == NULL) {
            ht = n_array_new(ht_size, tndb_hent_free, (tn_fn_cmp)tndb_hent_cmp);
            db->htt[i] = ht;
        }
        
        for (j=0; j < (int)ht_size; j++) {
            uint32_t val, offs;
            struct tndb_hent *he;
            
            if (!n_stream_read_uint32(db->st, &val))
                return 0;
            
            if (!n_stream_read_uint32(db->st, &offs))
                return 0;
            
            DBGF("at %ld h0[%d].h1[%d](%d) %d\n",
                 n_stream_tell(db->st) - (2 * sizeof(uint32_t)), 
                 i, j, val, offs);
            he = tndb_hent_new(db, val, offs);
            n_array_push(ht, he);
        }
    }

    return 1;
}

static
int verify_digest(struct tndb_hdr *hdr, tn_stream *st)
{
    unsigned char buf[4096];
    struct tndb_sign sign;
    int n, nread;
    long off;
    

    sign = hdr->sign;
    tndb_sign_init(&hdr->sign);

    off = n_stream_tell(st);
    n_stream_seek(st, hdr->doffs, SEEK_SET);
    
    while ((nread = n_stream_read(st, buf, sizeof(buf))) > 0)
        tndb_sign_update(&hdr->sign, buf, nread);

    tndb_hdr_compute_digest(hdr);
    
    if ((hdr->flags & TNDB_NOHASH) == 0) {
        int to_read;
        uint32_t htt_offs;
        
        
        htt_offs = tndb_hdr_store_size(hdr);
        n_stream_seek(st, htt_offs, SEEK_SET);
        to_read = hdr->doffs - htt_offs;
        
        while (to_read > 0) {
            int n = sizeof(buf);
            if (to_read < n)
                n = to_read;
            to_read -= n;
            
            if (n_stream_read(st, buf, n) != n)
                return 0;

            tndb_sign_update(&hdr->sign, buf, n);
        }
    }

    tndb_sign_final(&hdr->sign);
    n = 0;

    DBGF("md_compute =\n %s\n %s\n %d\n",
         (char*)tndb_bin2hex_s(hdr->sign.md, sizeof(sign.md)),
         (char*)tndb_bin2hex_s(sign.md, sizeof(sign.md)),
         memcmp(sign.md, hdr->sign.md, sizeof(sign.md)));
    
    if (memcmp(sign.md, hdr->sign.md, sizeof(sign.md)) == 0)
        n = 1;

    hdr->sign = sign;
    return n;
}

static
struct tndb *do_tndb_open(int fd, const char *path)
{
    struct tndb_hdr  hdr;
    tn_stream        *st;
    struct tndb      *db;
    int              n, type = TN_STREAM_STDIO;
    
    n = strlen(path);
    if (n > 3 && strcmp(&path[n - 3], ".gz") == 0)
        type = TN_STREAM_GZIO;

    if (fd > 0) 
        st = n_stream_dopen(fd, "rb", type);
    else 
        st = n_stream_open(path, "rb", type);
            
    if (st == NULL)
        return NULL;

    if (!tndb_hdr_restore(&hdr, st)) {
        n_stream_close(st);
        return NULL;
    }

    if (hdr.nrec <= 0) {
        n_stream_close(st);
        return NULL;
    }
    
    db = tndb_new(0);
    db->path = n_strdupl(path, n);
    db->st = st;
    db->rflags = TNDB_R_MODE_R;
    db->hdr = hdr;
    
    DBGF("nrec %u, doffs %u\n", hdr.nrec, hdr.doffs);
    
#if 0
    if ((hdr.flags & TNDB_NOHASH) == 0 && (db->rflags & TNDB_R_HTT_LOADED) == 0) {
        if (!htt_read(db))
            n_die("htt_read failed\n");
        db->rflags |= TNDB_R_HTT_LOADED;
    }
#endif

    if ((db->hdr.flags & TNDB_SIGNED) == 0)
        db->rflags |= TNDB_R_SIGN_VRFIED;
    
    return db;
}


struct tndb *tndb_open(const char *path) 
{
    return do_tndb_open(-1, path);
}


struct tndb *tndb_dopen(int fd, const char *path) 
{
    
    return do_tndb_open(fd, path);
}


int tndb_verify(struct tndb *db)
{
    int rc = 0;

    db->rflags |= TNDB_R_SIGN_VRFIED;
    
    if (verify_md5(db->path))
        rc = 1;
    
    else if (verify_digest(&db->hdr, db->st)) {
        make_md5(db->path);
        rc = 1;
    }
    DBGF("tndb_verify %s %d\n", db->path, rc);
    return rc;
}

struct tndb *tndb_incref(struct tndb *db)
{
    db->_refcnt++;
    return db;
}

tn_stream *tndb_tn_stream(const struct tndb *db)
{
    return db->st;
}

int tndb_get_voff(struct tndb *db, const void *key,
                  size_t klen_, off_t *voffs, size_t *vlen)
{
    uint32_t                 hv, hv_i;
    tn_array                 *ht;
    struct tndb_hent         he_tmp, *he;
    uint8_t                  klen;
    int                      n, found = 0;


    if (!verify_db(db))
        return 0;

    if (db->hdr.flags & TNDB_NOHASH) 
        n_die("tndb: method not allowed on hash-disabled file\n");  

    
    if ((db->rflags & TNDB_R_HTT_LOADED) == 0) {
        if (!htt_read(db))
            n_die("tndb: %p, htt_read failed\n", db);
        //printf("tndb: %p, htt_read OK\n", db);
        db->rflags |= TNDB_R_HTT_LOADED;
    }

    *voffs = (off_t) -1;
    *vlen = 0;

    if (klen_ > UINT8_MAX)
        n_die("tndb: key too long (max is %d)\n", UINT8_MAX);
    klen = klen_;
    
    hv = tndb_hash(key, klen);
    hv_i = hv & 0xff;
    ht = db->htt[hv_i];

    if (ht == NULL)
        return 0;

    he_tmp.val = hv;
    he_tmp.offs = 0;
    
    DBGF("search[%u] %u\n", hv_i, hv);
    if ((n = n_array_bsearch_idx(ht, &he_tmp)) == -1)
        return 0;

    DBGF("search[%u] %u -> %d\n", hv_i, hv, n);
    while (n < n_array_size(ht)) {
        uint8_t db_klen = 0;
        
        he = n_array_nth(ht, n++);

        DBGF("search[%u] %u: %u, %u\n", hv_i, hv, he->val, he->offs);
        if (he->val != hv) 
            break;

        if (nn_stream_read_offs(db->st, &db_klen, 1, he->offs) != 1) {
            found = -1;
            break;
        }
        
        if (db_klen != klen) {
            DBGF("db_klen %d, klen %d\n", db_klen, klen);
            continue;
        }
        
        if (read_eq(db, he->offs + sizeof(klen), key, klen)) {
            found = 1;
            
            *voffs = he->offs + sizeof(uint8_t) + klen;
            if (!nn_stream_read_uint32_offs(db->st, vlen, *voffs))
                found = -1;
            *voffs += sizeof(uint32_t);
        }
    }
    
    return found;                     
}


int tndb_get(struct tndb *db, const void *key,
             size_t klen, void *val, size_t size)
{
    off_t voffs;
    size_t vlen;
    int nread = 0;

    if (tndb_get_voff(db, key, klen, &voffs, &vlen) && vlen < size) {
        nread = nn_stream_read_offs(db->st, val, vlen, voffs);
        if (nread != (int)vlen)
            nread = 0;
    }
    
    return nread;
}


int tndb_get_str(struct tndb *db, const char *key,
                 unsigned char *val, size_t size)
{
    int nread = 0;
    
    if ((nread = tndb_get(db, key, strlen(key), val, size - 1)))
        val[nread] = '\0';
    
    return nread;
}


tn_array *tndb_keys(struct tndb *db) 
{
    struct tndb_it  it;
    char            key[TNDB_KEY_MAX + 1];
    int             klen, vlen, rc;
    tn_array        *keys;
    off_t           voffs;

    if (!verify_db(db))
        return NULL;
    
    if (!tndb_it_start(db, &it))
        return NULL;

    keys = n_array_new(db->hdr.nrec, free, (tn_fn_cmp)strcmp);
    while ((rc = tndb_it_get_voff(&it, key, &klen, &voffs, &vlen))) {
        if (rc < 0) {
            n_array_free(keys);
            return NULL;
        }
        
        n_array_push(keys, n_strdupl(key, klen));
    }

    return keys;
}


int tndb_it_start(struct tndb *db, struct tndb_it *it)
{
    n_assert(db->rflags & TNDB_R_MODE_R);

    if (!verify_db(db))
        return 0;

    it->_db = db;
    it->st = db->st;
    it->_nrec = 0;
    it->_off = db->hdr.doffs;

    it->_get_flag = 0;
    
    return 1;
}


/*
  key size must be at least 256 + 1 bytes (maximum tndb key length)
  if key is NULL then keys are not retrieved 
 */
int tndb_it_get_voff(struct tndb_it *it, void *key, size_t *klen,
                     off_t *voff, size_t *vlen)
{
    uint8_t db_klen = 0;
    tn_stream *st;

    n_assert(it->_get_flag == 0);

    if (key)
        *klen = 0;

    if (it->_nrec == it->_db->hdr.nrec)
        return 0;
    
    st = it->_db->st;

    if (nn_stream_read_offs(st, &db_klen, 1, it->_off) != 1)
        return 0;

    if (klen)
        *klen = db_klen;

    DBGF("\nget %d of %d\n", it->_nrec, it->_db->hdr.nrec);
    if (key) {
        if (n_stream_read(st, key, db_klen) != db_klen)
            return 0;
        
        ((unsigned char *)key)[db_klen] = '\0';
        DBGF("key[%d] %s(%d)\n", it->_off, key, db_klen);
    }
    
    it->_off += db_klen + 1;
    *voff = it->_off + sizeof(uint32_t);
    
    if (!nn_stream_read_uint32_offs(st, vlen, it->_off))
        return 0;

    //DBGF("val[%d] (%d)\n", it->_offs, *vlen);
    it->_off += *vlen + sizeof(uint32_t);
    it->_nrec++;
    
    return *vlen;
}


int tndb_it_get(struct tndb_it *it, void *key, size_t *klen,
                void *val, size_t *vlen_)
{
    off_t  voff;
    size_t vlen;
    int rc = 0;

    if (tndb_it_get_voff(it, key, klen, &voff, &vlen) && (vlen + 1) < *vlen_) {
        *vlen_ = vlen;
        rc = (n_stream_read(it->_db->st, val, vlen) == (int)vlen);
        if (rc)
            ((char*)val)[vlen] = '\0';
    }

    return rc;
}


int tndb_it_get_begin(struct tndb_it *it, 
                      void *key, size_t *klen, 
                      size_t *vlen_)
{
    off_t    voff = 0;
    uint32_t vlen = 0;

    n_assert(it->_get_flag == 0);

    if (tndb_it_get_voff(it, key, klen, &voff, &vlen)) {
        n_stream_seek(it->_db->st, voff, SEEK_SET);
        it->_get_flag = 1;
        if (vlen_)
            *vlen_ = vlen;
        
        //printf("tndb_it_get_begin %lu + %lu => %lu\n", voff, vlen, it->_off);
        return 1;
        
    }
    
    return 0;
}


int tndb_it_get_end(struct tndb_it *it)
{
    off_t    off = 0;

    off = n_stream_tell(it->_db->st);

    if (off > (int)it->_off)
        n_die("tndb_it_get_end: off %lu, expected %lu\n", off, it->_off);
    
    if (off < (int)it->_off)
        n_stream_seek(it->_db->st, it->_off, SEEK_SET);

    n_assert(it->_get_flag > 0);
    it->_get_flag = 0;

    return 1;
}


int tndb_read(struct tndb *db, long offs, void *buf, size_t size)
{
    if (n_stream_seek(db->st, offs, SEEK_SET) == -1)
        return -1;
    
    return n_stream_read(db->st, buf, size);
}


