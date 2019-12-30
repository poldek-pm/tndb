/*
  Copyright (C) 2002 Pawel A. Gajda <mis@k2.net.pl>

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU Library General Public License, version 2
  as published by the Free Software Foundation (see file COPYING for details).

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

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

#include <openssl/evp.h>

#include <trurl/nmalloc.h>
#include <trurl/nassert.h>
#include <trurl/n_snprintf.h>

#include "compiler.h"
#include "tndb.h"
#include "tndb_int.h"

//static char *msg_not_verified = "tndb: %s: refusing read unchecked file\n";

static inline int verify_db(struct tndb *db)
{
    if (db->rtflags & TNDB_R_SIGN_VRFIED)
        return 1;

    if (db->hdr.flags & TNDB_SIGNED)
        return tndb_verify(db);

    n_assert(0);
    db->rtflags |= TNDB_R_SIGN_VRFIED;
    return 1;
}


static
int md5(FILE *stream, unsigned char *md, unsigned *md_size)
{
    unsigned char buf[8*1024];
    EVP_MD_CTX *ctx;
    unsigned n, nn = 0;


    n_assert(md_size && *md_size);

    ctx = EVP_MD_CTX_create();
    if (!EVP_DigestInit(ctx, EVP_md5()))
        return 0;

    while ((n = fread(buf, 1, sizeof(buf), stream)) > 0) {
        EVP_DigestUpdate(ctx, buf, n);
        nn += n;
    }

    EVP_DigestFinal(ctx, buf, &n);

    if (n > *md_size) {
        *md = '\0';
        *md_size = 0;
    } else {
        memcpy(md, buf, n);
        *md_size = n;
    }

    EVP_MD_CTX_destroy(ctx);

    return *md_size;
}

static
int md5hex(FILE *stream, unsigned char *mdhex, unsigned *mdhex_size)
{
    unsigned char md[128];
    unsigned md_size = sizeof(md);

    if (md5(stream, md, &md_size)) {
        int i, n = 0, nn = 0;

        for (i=0; i < (int)md_size; i++) {
            n = n_snprintf((char*)mdhex + nn, *mdhex_size - nn, "%02x", md[i]);
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
    unsigned        md_size = sizeof(md);

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
    unsigned        md1_size, md2_size;
    char            path[PATH_MAX];
    int             fd;

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

    DBGF("md5 =\n %s\n %s\n => %d\n",
         (char*)tndb_debug_bin2hex_s(md1, md1_size),
         (char*)tndb_debug_bin2hex_s(md2, md2_size),
         memcmp(md1, md2, md1_size));

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
    int i, j;
    off_t ht_offsets[TNDB_HTSIZE];

    /* read ht entries offsets first to avoid backward file seek */

    for (i=0; i < TNDB_HTSIZE; i++) {
        uint32_t ht_offs, offs;

        db->htt[i] = NULL;
        ht_offsets[i] = 0;

        offs = db->offs.htt + (sizeof(uint32_t) * i);
        DBGF("h[%d] %d\n", i, offs);
        if (!nn_stream_read_uint32_offs(db->st, &ht_offs, offs))
            return 0;

        ht_offsets[i] = ht_offs;
    }

    for (i=0; i < TNDB_HTSIZE; i++) {
        tn_array *ht = db->htt[i];
        uint32_t ht_size;
        uint32_t ht_offs = ht_offsets[i];

        if (ht_offs == 0)
            continue;

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
            uint32_t val, hoffs;
            struct tndb_hent *he;

            if (!n_stream_read_uint32(db->st, &val))
                return 0;

            if (!n_stream_read_uint32(db->st, &hoffs))
                return 0;

            DBGF("at %ld h0[%d].h1[%d](%d) %d\n",
                 n_stream_tell(db->st) - (2 * sizeof(uint32_t)),
                 i, j, val, hoffs);

            he = tndb_hent_new(db, val, hoffs);
            n_array_push(ht, he);
        }
    }

    DBGF("htt_read DONE\n");
    return 1;
}

static
int verify_digest(struct tndb_hdr *hdr, uint32_t htt_offset, tn_stream *st)
{
    unsigned char buf[4096];
    struct tndb_sign sign;
    int rc, nread;

    sign = hdr->sign;
    tndb_sign_init(&hdr->sign);

    n_stream_seek(st, hdr->doffs, SEEK_SET);

    while ((nread = n_stream_read(st, buf, sizeof(buf))) > 0)
        tndb_sign_update(&hdr->sign, buf, nread);

    tndb_hdr_compute_digest(hdr);

    if ((hdr->flags & TNDB_NOHASH) == 0) { /* process hash table if any */
        int to_read;

        n_stream_seek(st, htt_offset, SEEK_SET);
        to_read = hdr->doffs - htt_offset;

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

    DBGF("md_compute =\n %s\n %s\n => %d\n",
         (char*)tndb_debug_bin2hex_s(hdr->sign.md, sizeof(sign.md)),
         (char*)tndb_debug_bin2hex_s(sign.md, sizeof(sign.md)),
         memcmp(sign.md, hdr->sign.md, 1));

    rc = 0;
    if (memcmp(sign.md, hdr->sign.md, sizeof(sign.md)) == 0)
        rc = 1;

    hdr->sign = sign;
    return rc;
}

static
struct tndb *do_tndb_open(int fd, const char *path)
{
    struct tndb_hdr  hdr;
    tn_stream        *st;
    struct tndb      *db;
    int              type;

    type = tndb_detect_stream_type(path);

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
    db->offs.htt = n_stream_tell(st); /* just after the hdr */
    db->path = n_strdup(path);
    db->st = st;
    db->rtflags = TNDB_R_MODE_R;
    db->hdr = hdr;

    DBGF("nrec %u, doffs %u\n", hdr.nrec, hdr.doffs);

#if 0                           /* do lazy loading */
    if ((hdr.flags & TNDB_NOHASH) == 0 &&
        (db->rtflags & TNDB_R_HTT_LOADED) == 0) {
        if (!htt_read(db))
            n_die("htt_read failed\n");
        db->rtflags |= TNDB_R_HTT_LOADED;
    }
#endif

    if ((db->hdr.flags & TNDB_SIGNED) == 0)
        db->rtflags |= TNDB_R_SIGN_VRFIED;

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

    if ((db->hdr.flags & TNDB_SIGN_DIGEST) == 0) { /* created w/o digest */
        make_md5(db->path);
    }

    db->rtflags |= TNDB_R_SIGN_VRFIED;

    if (verify_md5(db->path)) {
        rc = 1;
    } else if (verify_digest(&db->hdr, db->offs.htt, db->st)) {
        make_md5(db->path);
        rc = 1;
    }

    return rc;
}

struct tndb *tndb_ref(struct tndb *db)
{
    db->_refcnt++;
    return db;
}

tn_stream *tndb_tn_stream(const struct tndb *db)
{
    return db->st;
}

int tndb_get_voff(struct tndb *db, const void *key, unsigned int aklen,
                  off_t *voffs, unsigned int *vlen)
{
    uint32_t                 hv, hv_i;
    tn_array                 *ht;
    struct tndb_hent         he_tmp, *he;
    uint8_t                  klen;
    int                      n, found = 0;


    if (!verify_db(db))
        return 0;

    if (db->hdr.flags & TNDB_NOHASH)
        n_die("tndb: method not allowed on file without hash table\n");


    if ((db->rtflags & TNDB_R_HTT_LOADED) == 0) {
        if (!htt_read(db))
            n_die("tndb: %p, htt_read failed\n", db);
        //printf("tndb: %p, htt_read OK\n", db);
        db->rtflags |= TNDB_R_HTT_LOADED;
    }

    *voffs = (off_t) -1;
    *vlen = 0;

    if (aklen > UINT8_MAX)
        n_die("tndb: key too long (max is %d)\n", UINT8_MAX);

    klen = aklen;

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
            uint32_t len;
            found = 1;

            *voffs = he->offs + sizeof(uint8_t) + klen;
            if (nn_stream_read_uint32_offs(db->st, &len, *voffs))
                *vlen = len;
            else
                found = -1;
            *voffs += sizeof(uint32_t);
        }
    }

    return found;
}

int tndb_get(struct tndb *db, const void *key, unsigned int klen,
             void *val, unsigned int valsize)
{
    off_t        voffs;
    unsigned int vlen;
    int          nread = 0;

    if (tndb_get_voff(db, key, klen, &voffs, &vlen) && vlen < valsize) {
        nread = nn_stream_read_offs(db->st, val, vlen, voffs);
        if (nread != (int)vlen)
            nread = 0;
    }

    return nread;
}

size_t tndb_get_all(struct tndb *db, const void *key, size_t klen,
		    void **val)
{
    off_t  voffs;
    size_t nread = 0;
    unsigned int vlen;

    if (tndb_get_voff(db, key, klen, &voffs, &vlen)) {
	*val = n_malloc(vlen + 1); /* extra byte for \0 */

	nread = nn_stream_read_offs(db->st, *val, vlen, voffs);

	if (nread != vlen) {
	    nread = 0;
	    n_cfree(val);
	}
    }

    return nread;
}

int tndb_get_str(struct tndb *db, const char *key,
                 unsigned char *val, unsigned int valsize)
{
    int nread = 0;

    if ((nread = tndb_get(db, key, strlen(key), val, valsize - 1)))
        val[nread] = '\0';

    return nread;
}


tn_array *tndb_keys(struct tndb *db)
{
    struct tndb_it  it;
    char            key[TNDB_KEY_MAX + 1];
    unsigned        klen, vlen;
    tn_array        *keys;
    off_t           voffs;
    int             rc;


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
    n_assert(db->rtflags & TNDB_R_MODE_R);

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
int tndb_it_get_voff(struct tndb_it *it, void *key, unsigned int *klen,
                     off_t *voff, unsigned int *vlen)
{
    uint8_t db_klen = 0;
    uint32_t vlen32 = 0;
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

    DBGF("get %d of %d\n", it->_nrec, it->_db->hdr.nrec);
    if (key) {
        if (n_stream_read(st, key, db_klen) != db_klen)
            return 0;

        ((unsigned char *)key)[db_klen] = '\0';
        DBGF("key[%d] %s(%d)\n", it->_off, key, db_klen);
    }

    it->_off += db_klen + 1;
    *voff = it->_off + sizeof(uint32_t);

    if (!nn_stream_read_uint32_offs(st, &vlen32, it->_off))
        return 0;

    DBGF("vlen of key %s = %d\n", key ? key : "(null)", vlen32);

    *vlen = vlen32;
    it->_off += *vlen + sizeof(uint32_t);
    it->_nrec++;
    //DBGF("val[%d] (%d)\n", it->_offs, *vlen);
    return *vlen;
}


int tndb_it_get(struct tndb_it *it, void *key, unsigned int *klen,
                void *val, unsigned int *avlen)
{
    off_t        voff;
    unsigned int vlen;
    int          rc = 0;

    if (!tndb_it_get_voff(it, key, klen, &voff, &vlen))
        return 0;

    if ((vlen + 1) > *avlen) {
        n_die("tndb: not enough space for data (%d > %d)\n", vlen, *avlen);
        return 0;
    }

    *avlen = vlen;
    rc = (n_stream_read(it->_db->st, val, vlen) == (int)vlen);
    if (rc)
        ((char*)val)[vlen] = '\0';

    return rc;
}

int tndb_it_rget(struct tndb_it *it, void *key, unsigned int *klen,
                 void **val, unsigned int *avlen)
{
    off_t        voff;
    unsigned int vlen;
    int          rc = 0;


    if (!tndb_it_get_voff(it, key, klen, &voff, &vlen))
        return 0;

    DBGF("%s avlen %d\n", key, *avlen);
    if ((vlen + 1) > *avlen) {
        DBGF("realloc avlen=%d, vlen=%d\n", *avlen, vlen);
        *val = n_realloc(*val, vlen + 1);
    }

    *avlen = vlen;
    rc = (n_stream_read(it->_db->st, *val, vlen) == (int)vlen);
    if (rc)
        ((char*)*val)[vlen] = '\0';

    return rc;
}



int tndb_it_get_begin(struct tndb_it *it, void *key, unsigned int *klen,
                      unsigned int *avlen)
{
    off_t        voff = 0;
    unsigned int vlen = 0;

    n_assert(it->_get_flag == 0);

    if (!tndb_it_get_voff(it, key, klen, &voff, &vlen))
        return 0;

    n_stream_seek(it->_db->st, voff, SEEK_SET);
    it->_get_flag = 1;

    if (avlen)
        *avlen = vlen;
    //printf("tndb_it_get_begin %lu + %lu => %lu\n", voff, vlen, it->_off);
    return 1;
}


int tndb_it_get_end(struct tndb_it *it)
{
    off_t    off = 0;

    off = n_stream_tell(it->_db->st);

    if (off > (int)it->_off) {
        n_die("tndb_it_get_end: current offset is %lu, expected %u\n", off,
               it->_off);
        return 0;
    }

    if (off < (int)it->_off)
        n_stream_seek(it->_db->st, it->_off, SEEK_SET);

    n_assert(it->_get_flag > 0);
    it->_get_flag = 0;

    return 1;
}


int tndb_read(struct tndb *db, long offs, void *buf, unsigned int size)
{
    if (n_stream_seek(db->st, offs, SEEK_SET) == -1)
        return -1;

    return n_stream_read(db->st, buf, size);
}
