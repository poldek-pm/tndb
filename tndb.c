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

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <openssl/evp.h>

#include <trurl/n_snprintf.h>
#include <trurl/nassert.h>
#include <trurl/nmalloc.h>
#include <trurl/n2h.h>

#include "compiler.h"
#include "tndb_int.h"
#include "tndb.h"


void tndb_seterr(struct tndb *db, const char *fmt, ...)
{
    va_list   args;
    
    va_start(args, fmt);
    vsnprintf(db->errmsg, sizeof(db->errmsg), fmt, args);
    va_end(args);
}


uint32_t tndb_hash(const void *d, register uint8_t size)
{
    register uint32_t j = (uint32_t) 5381U;
    const unsigned char *p = d;
    
    while (size != 0) {
        size--;
        j += (j << 5);
        j ^= *p++;
    }

    j &= ~(uint32_t)0U;
    
    return j;
}

int tndb_bin2hex(char *hex, int hex_size, const unsigned char *bin, int bin_size)
{
    int i, n = 0, nn = 0;

    n_assert(hex_size > 2 * bin_size); /* with end '\0' */
    
    for (i=0; i < bin_size; i++) {
        n = snprintf(hex + nn, hex_size - nn, "%02x", bin[i]);
        nn += n;
        if (nn >= hex_size)
            break;
    }
    return nn;
}

char *tndb_bin2hex_s(const unsigned char *bin, int bin_size)
{
    static char hex[4 * 1024];
    
    tndb_bin2hex(hex, sizeof(hex), bin, bin_size);
    return hex;
}

//static
void tndb_sign_init(struct tndb_sign *sign) 
{
    EVP_MD_CTX *ctx;
    
    memset(sign, 0, sizeof(*sign));
    
    ctx = EVP_MD_CTX_create();
    EVP_DigestInit(ctx, EVP_sha1());
    sign->ctx = ctx;
    //printf("%p %p >> INIT\n", sign, sign->ctx);
}

void tndb_sign_update(struct tndb_sign *sign, const void *buf, unsigned int size) 
{
    n_assert(sign->ctx);
    EVP_DigestUpdate(sign->ctx, buf, size);
    //printf(" >> UPDATE %d (%s)\n", size, tndb_bin2hex_s(buf, size));
}


void tndb_sign_update_int32(struct tndb_sign *sign, uint32_t v)
{
    uint32_t vv = n_hton32(v);
    tndb_sign_update(sign, &vv, sizeof(vv)); 
}


void tndb_sign_final(struct tndb_sign *sign) 
{
    unsigned char buf[1024];
    unsigned n;

    //printf("%p %p >> FINAL\n", sign, sign->ctx);
    EVP_DigestFinal(sign->ctx, buf, &n);
    
    if (n > (int)sizeof(sign->md)) 
        *sign->md = '\0';
    else
        memcpy(sign->md, buf, n);
   
    EVP_MD_CTX_destroy((EVP_MD_CTX *)sign->ctx); 
    sign->ctx = NULL;
    
}


/* every sig is stored as: [name size(1byte)]name[sig size(2bytes)]sig */
static
int tndb_sign_store_sizeof(struct tndb_sign *sign, uint32_t flags) 
{
    int size = sizeof(uint16_t); /* tndb_sign size */
    
    if (flags & TNDB_SIGN_DIGEST)
        size += sizeof(uint8_t) + strlen("md") +
            sizeof(uint16_t) + sizeof(sign->md);
#define DUMMY_TEST_SIGN 0
#if DUMMY_TEST_SIGN
    size += sizeof(uint8_t) + strlen("dupa") +
            sizeof(uint16_t) + (2 * sizeof(sign->md));
#endif    
    return size;
}


static 
int store_sig(tn_stream *st, const char *name, void *sig, int size)
{
    uint16_t ssize;
    uint8_t  nsize;
    int stsize = 0, len;

    len = strlen(name); 
    n_assert(len < UINT8_MAX);
    nsize = len;
    if (!n_stream_write_uint8(st, nsize))
        return 0;
    
    if (n_stream_write(st, name, len) != len)
        return 0;

    stsize += sizeof(nsize) + len;

    n_assert(size < UINT16_MAX);
    ssize = size;
    if (!n_stream_write_uint16(st, ssize))
        return 0;

    if (n_stream_write(st, sig, size) != size)
        return 0;
    
    stsize += sizeof(ssize) + size;
    return stsize;
}

static 
int restore_sig(tn_stream *st, char *name, int namesize, void *sig, unsigned int size)
{
    uint16_t ssize;
    uint8_t  nsize;
    int stsize = 0;

    if (!n_stream_read_uint8(st, &nsize))
        return 0;
    
    n_assert(namesize > nsize);
    if (n_stream_read(st, name, nsize) != nsize)
        return 0;
    name[nsize] = '\0';
    
    stsize += sizeof(nsize) + nsize;

    if (!n_stream_read_uint16(st, &ssize))
        return 0;
    
    n_assert(size > ssize);
    if (n_stream_read(st, sig, ssize) != ssize)
        return 0;
    
    stsize += sizeof(ssize) + ssize;
    return stsize;
}


int tndb_sign_store(struct tndb_sign *sign, tn_stream *st, uint32_t flags) 
{
    int size, stsize;
    
    n_assert(n_stream_tell(st) == TNDBSIGN_OFFSET);

    size = tndb_sign_store_sizeof(sign, flags); 
    n_assert(size < UINT16_MAX);
    
    if (!n_stream_write_uint16(st, size))
        return 0;

    stsize = 0;
    if (flags & TNDB_SIGN_DIGEST) {
        int n;
        if ((n = store_sig(st, "md", sign->md, sizeof(sign->md))) == 0)
            return 0;
        stsize += n;
    }
#if DUMMY_TEST_SIGN
    {
        char buf[1024] = "ala ma kota xxxxxxxxxxxxxxxxxxxt";
        int n;
        
        if ((n = store_sig(st, "dupa", buf, 2 * sizeof(sign->md))) == 0)
            return 0;
        stsize += n;
    }
#endif    
    n_assert((int)sizeof(uint16_t) + stsize == size);
    return 1;
}


int tndb_sign_restore(tn_stream *st, struct tndb_sign *sign, uint32_t flags)
{
    int rc = 1, size;
    uint16_t size16;
    char name[32], sig[UINT16_MAX];
    
    n_assert(n_stream_tell(st) == TNDBSIGN_OFFSET);
    if (!n_stream_read_uint16(st, &size16))
        return 0;

    size16 -= sizeof(size16);
    size = size16;

    while (size > 0) {
        int n;
        n = restore_sig(st, name, sizeof(name), sig, sizeof(sig));
        if (n == 0) {
            rc = 0;
            break;
        }
        size -= n;
        if (size < 0 || size > size16) {  /* overrun */
            rc = 0;
            errno = EINVAL;
            break;
        }
        DBGF("%s, length=%d\n", name, n);
        if (strcmp(name, "md") == 0) {
            n_assert (flags & TNDB_SIGN_DIGEST);
            if (!memcpy(sign->md, sig, sizeof(sign->md))) {
                rc = 0;
                break;
            }
        }
    }
    return rc;
}


void tndb_hdr_init(struct tndb_hdr *hdr, unsigned flags)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->flags |= flags;
    
    n_snprintf((char*)hdr->hdr, sizeof(hdr->hdr), "tndb%d.%d\n",
               TNDB_FILEFMT_MAJOR, TNDB_FILEFMT_MINOR);
    
    if (flags & TNDB_SIGN_DIGEST)
        tndb_sign_init(&hdr->sign);
}

static int hdr_write_uint32(struct tndb_hdr *hdr, tn_stream *st, uint32_t v, int write)
{
    if (write)
        return n_stream_write_uint32(st, v);
    else
        tndb_sign_update_int32(&hdr->sign, v);
    
    return 1;
}


static 
int tndb_hdr_store_(struct tndb_hdr *hdr, tn_stream *st, int writeit)
{
    int nerr = 0, size;

    if (writeit)
        if (n_stream_seek(st, 0, SEEK_SET) == -1)
            return 0;

    size = sizeof(hdr->hdr);

    if (writeit) {
        nerr += n_stream_write(st, hdr->hdr, size) != size;
        n_stream_write_uint8(st, hdr->flags);
        if (!tndb_sign_store(&hdr->sign, st, hdr->flags))
            nerr++;
        
    } else {
        //printf("hdr = %s\n", hdr->hdr);
        tndb_sign_update(&hdr->sign, hdr->hdr, size);
        tndb_sign_update(&hdr->sign, &hdr->flags, sizeof(hdr->flags));
    }

    if (!hdr_write_uint32(hdr, st, hdr->ts, writeit))
        nerr++;

    if (!hdr_write_uint32(hdr, st, hdr->nrec, writeit))
        nerr++;

    if (!hdr_write_uint32(hdr, st, hdr->doffs, writeit))
        nerr++;


    DBGF("nrec %u, doffs %u\n", hdr->nrec, hdr->doffs);
    return nerr == 0;
}

int tndb_hdr_store(struct tndb_hdr *hdr, tn_stream *st)
{
    return tndb_hdr_store_(hdr, st, 1);
}

int tndb_hdr_compute_digest(struct tndb_hdr *hdr)
{
    return tndb_hdr_store_(hdr, NULL, 0);
}


int tndb_hdr_store_sizeof(struct tndb_hdr *hdr)
{
    int size = 0;
    
    size += sizeof(hdr->hdr) + sizeof(hdr->flags);
    size += tndb_sign_store_sizeof(&hdr->sign, hdr->flags);
    size += sizeof(hdr->nrec) + sizeof(hdr->doffs) +
        sizeof(hdr->ts);
    return size;
}



int tndb_hdr_restore(struct tndb_hdr *hdr, tn_stream *st)
{
    int nerr = 0, size;

    if (n_stream_seek(st, 0, SEEK_SET) == -1)
        return 0;

    size = sizeof(hdr->hdr);
    nerr += n_stream_read(st, hdr->hdr, size) != size;
    
    if (nerr == 0 && !n_stream_read_uint8(st, &hdr->flags))
        nerr++;

    if (nerr == 0 && !tndb_sign_restore(st, &hdr->sign, hdr->flags))
        nerr++;

    if (nerr == 0 && !n_stream_read_uint32(st, &hdr->ts))
        nerr++;
    
    if (nerr == 0 && !n_stream_read_uint32(st, &hdr->nrec))
        nerr++;
    
    if (nerr == 0 && !n_stream_read_uint32(st, &hdr->doffs))
        nerr++;

    DBGF("nrec %u, doffs %u\n", hdr->nrec, hdr->doffs);
    
    return nerr == 0;
}

struct tndb_hent *tndb_hent_new(struct tndb *db, uint32_t val, uint32_t offs)
{
    struct tndb_hent *h = NULL;

    n_assert(db);
    n_assert(db->na);
    h = db->na->na_malloc(db->na, sizeof(*h));
    h->val = val;
    h->offs = offs;
    
    return h;
}

void tndb_hent_free(void *ptr)
{
    ptr = ptr;                  /* do nothing, obstack is used */
}


int tndb_hent_cmp_store(const struct tndb_hent *h1, struct tndb_hent *h2)
{
    if (h1->val < h2->val)
        return -1;

    if (h2->val < h1->val)
        return 1;

    if (h1->offs < h2->offs)
        return -1;

    if (h2->offs < h1->offs)
        return 1;
    
    return 0;
}


int tndb_hent_cmp(const struct tndb_hent *h1, struct tndb_hent *h2)
{
    if (h1->val < h2->val)
        return -1;

    if (h2->val < h1->val)
        return 1;

    return 0;
}


struct tndb *tndb_new(unsigned flags) 
{
    struct tndb *db;
    int i;
    
    db = n_calloc(1, sizeof(*db));
    db->st = NULL;
    db->path = NULL;

    tndb_hdr_init(&db->hdr, flags);
    
    for (i=0; i < TNDB_HTSIZE; i++)
        db->htt[i] = NULL;
    *db->errmsg = '\0';

    db->na = n_alloc_new(64, TN_ALLOC_OBSTACK);
    return db;
}

        
void tndb_free(struct tndb *db) 
{
    int i;

    for (i=0; i < TNDB_HTSIZE; i++)
        if (db->htt[i] != NULL) {
            n_array_free(db->htt[i]);
            db->htt[i] = NULL;
        }

    if (db->st != NULL) {
        n_stream_close(db->st);
        db->st = NULL;
    }

    if (db->path != NULL) {
        free(db->path);
        db->path = NULL;
    }

    if (db->na) {
        n_alloc_free(db->na);
        db->na = NULL;
    }
    free(db);
}

const char *tndb_path(const struct tndb *db)
{
    return db->path;
}

