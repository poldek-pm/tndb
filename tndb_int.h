/* $Id$ */

#ifndef TNDB_INTERNAL_H
#define TNDB_INTERNAL_H

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <stdint.h>
#include <stdio.h>

#include <trurl/narray.h>
#include <trurl/nstream.h>

#define TNDB_FILEFMT_MAJOR     1
#define TNDB_FILEFMT_MINOR     0


uint32_t tndb_hash(const void *d, register uint8_t size);

#define TNDBSIGN_OFFSET       9 /* hdr[8] + sizeof(flsgs) */
struct tndb_sign {
    void           *ctx;
    unsigned char  md[20];      /* sha */
};

void tndb_sign_init(struct tndb_sign *sign);
void tndb_sign_update(struct tndb_sign *sign, const void *buf, size_t size);
void tndb_sign_update_int32(struct tndb_sign *sign, uint32_t v);
void tndb_sign_final(struct tndb_sign *sign);
int  tndb_sign_store(struct tndb_sign *sign, tn_stream *st, uint32_t flags);

                            
struct tndb_hdr {
    unsigned char      hdr[8];
    uint8_t            flags; 
    struct tndb_sign   sign;   
    uint32_t           ts;          /*  */
    uint32_t           nrec;        /* number of records */
    uint32_t           doffs;       /* offset of first data record */
};

void tndb_hdr_init(struct tndb_hdr *hdr, unsigned flags);
int tndb_hdr_store(struct tndb_hdr *hdr, tn_stream *st);
int tndb_hdr_compute_digest(struct tndb_hdr *hdr);
int tndb_hdr_store_size(struct tndb_hdr *hdr);
int tndb_hdr_restore(struct tndb_hdr *hdr, tn_stream *st);

#define tndb_hdr_upsign(hdr, buf, size)                \
      do { if (hdr->flags & TNDBHDR_SIGN)              \
               tndb_sign_update(hdr->sign, buf, size); \
      } while(0);   




struct tndb_hent {
    uint32_t val;
    uint32_t offs;
};

struct tndb;

struct tndb_hent *tndb_hent_new(struct tndb *db, uint32_t val, uint32_t offs);
void tndb_hent_free(void *ptr);
int tndb_hent_cmp(const struct tndb_hent *h1, struct tndb_hent *h2);
int tndb_hent_cmp_store(const struct tndb_hent *h1, struct tndb_hent *h2);

#define TNDB_HTSIZE       256
#define TNDB_HTBYTESIZE   (TNDB_HTSIZE * sizeof(uint32_t))

#define TNDB_R_MODE_R      (1 << 0)
#define TNDB_R_MODE_W      (1 << 1)
#define TNDB_R_HTT_LOADED  (1 << 2)
#define TNDB_R_SIGN_VRFIED (1 << 3)

struct tndb {
    unsigned                 rflags; /* runtime flags */
    char                     *path;
    tn_stream                *st;
    struct tndb_hdr          hdr;

    uint32_t                 current_doffs; /* used in rw mode only */
    
    tn_array                 *htt[TNDB_HTSIZE];  /* arary of tn_array ptr of
                                                    tndb_hent */
    char                     errmsg[128];
    void                     *obstack;
    int                      _refcnt;
};

struct tndb *tndb_new(unsigned flags);
void tndb_free(struct tndb *db);


static inline
int nn_stream_read_offs(tn_stream *st, void *buf, size_t size, uint32_t offs)
{
    if (st->seek(st->stream, offs, SEEK_SET) == -1)
        return -1;
    
    return st->read(st->stream, buf, size);
}

static inline
int nn_stream_read_uint32_offs(tn_stream *st, uint32_t *val, uint32_t offs)
{
    int rc;
    
    *val = 0;
    if (st->seek(st->stream, offs, SEEK_SET) == -1) 
        return 0;

    rc = n_stream_read_uint32(st, val);
    return rc;
}


#ifndef ENABLE_TRACE
# define ENABLE_TRACE 0
#endif

#if ENABLE_TRACE
# define DBGF(fmt, args...)  fprintf(stdout, "%-18s: " fmt, __FUNCTION__ , ## args)
# define DBG(fmt, args...)   fprintf(stdout, fmt, ## args)
#else 
# define DBGF(fmt, args...)  ((void) 0)
# define DBG(fmt, args...)    ((void) 0)
#endif

#define DBGMSG_F DBGF
#define DBGMSG   DBG

#define DBGF_NULL(fmt, args...) ((void) 0)
#define DBGF_F(fmt, args...) fprintf(stdout, "%-18s: " fmt, __FUNCTION__ , ## args)

#endif 


