/*
  Copyright (C) 2002 - 2007 Pawel A. Gajda <mis@pld-linux.org>

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

#ifdef HAVE_COPY_FILE_RANGE
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1 /* copy_file_range() is declared only with _GNU_SOURCE */
#endif
#define COPY_CHUNK_MAX 0x7ffff000 /* the most one copy_file_range() call moves */
#endif

#include <errno.h>
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
#include <trurl/natomic.h>

#define TN_STREAM_USE_GZIO_NG 1
#include "compiler.h"
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

/* <dir>/.<name>.tmp.XXXXXX, close-on-exec: it is open for the whole build
   of the database, and a child forked meanwhile must not keep it */
static int open_tmpfile(char *tmpath, size_t size, const char *path)
{
    const char *base = strrchr(path, '/');
    int fd, n;

    if (base != NULL)
        n = snprintf(tmpath, size, "%.*s.%s.tmp.XXXXXX",
                     (int)(base - path + 1), path, base + 1);
    else
        n = snprintf(tmpath, size, ".%s.tmp.XXXXXX", path);

    if (n < 0 || (size_t)n >= size) {
        errno = ENAMETOOLONG;
        *tmpath = '\0';
        return -1;
    }

#ifdef HAVE_MKSTEMP
    fd = mkstemp(tmpath);
#else
    fd = open(tmpath, O_RDWR | O_CREAT | O_TRUNC | O_EXCL, 0600);
#endif
    if (fd == -1) {
        *tmpath = '\0';
        return -1;
    }

    if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
        int err = errno;

        close(fd);
        unlink(tmpath);
        *tmpath = '\0';
        errno = err;
        return -1;
    }

    return fd;
}

struct tndb *tndb_creat(const char *name, int comprlevel, unsigned flags)
{
    char                path[PATH_MAX], mode[32] = "wb";
    tn_stream           *st;
    struct tndb         *db = NULL;
    int                 fd, type = TN_STREAM_STDIO;

    fd = open_tmpfile(path, sizeof(path), name);
    if (fd < 0)
        return NULL;

    fchmod(fd, 0600);

    unlink(path); /* unlink just after create, it's temporary file */

    type = tndb_detect_stream_type(name);
    if (type == TN_STREAM_GZIO || type ==  TN_STREAM_ZSTDIO || type == TN_STREAM_GZIO_NG) {
        if (comprlevel >= 0 && comprlevel < 10)
            snprintf(mode, sizeof(mode), "wb%d", comprlevel);
    }

    if ((st = n_stream_dopen(fd, mode, type)) == NULL) {
        close(fd);
        return NULL;
    }

    db = tndb_new(flags);
    db->rtflags |= TNDB_R_MODE_W;
    db->st = st;
    db->path = n_strdupl(name, strlen(name));
    if (db->hdr.flags & TNDB_SIGN_DIGEST)
        n_stream_set_write_hook(st, st_write_hook_write, &db->hdr.sign);

    return db;
}

static inline int put_key(struct tndb *db, const char *key, unsigned int aklen)
{
    uint32_t               hv, hv_i;
    tn_array               *ht;
    struct tndb_hent       *he;
    uint8_t                klen;

    n_assert(db->rtflags & TNDB_R_MODE_W);

    if (aklen > UINT8_MAX) {
        n_die("Key is too long (max is %d)\n", UINT8_MAX);
        return 0;
    }
    klen = aklen;

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


int tndb_put(struct tndb *db, const char *key, unsigned int aklen,
             const void *val, unsigned int vlen)
{

    if (!put_key(db, key, aklen))
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
    unsigned int i;
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

            for (int j = 0; j < n_array_size(ht); j++) {
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

/* computes and writes htt's digest  */
static int htt_compute_digest(struct tndb *db)
{
    int rc;

    n_assert(db->hdr.flags & TNDB_SIGN_DIGEST);
    n_stream_set_write_hook(db->st, st_write_hook_nowrite, &db->hdr.sign);
    rc = htt_write(db);
    n_stream_set_write_hook(db->st, st_write_hook_write, &db->hdr.sign);
    return rc;
}

/* copies the whole fdin, from offset 0, to the end of fdout */
static int append_body(int fdin, int fdout)
{
    off_t in_off = 0;

    if (lseek(fdout, 0, SEEK_END) == -1)
        return -1;

#ifdef HAVE_COPY_FILE_RANGE
    for (;;) {
        ssize_t n;

        n = copy_file_range(fdin, &in_off, fdout, NULL, COPY_CHUNK_MAX, 0);
        if (n == 0)             /* EOF */
            return 0;

        if (n < 0) {
            if (errno == EINTR)
                continue;

            /* no kernel copy here; read/write from where it stopped.  EPERM
               is what an older seccomp filter gives for a syscall it does
               not know; a real one comes back from write() */
            if (errno == ENOSYS || errno == EXDEV || errno == EINVAL ||
                errno == EOPNOTSUPP || errno == EPERM)
                break;

            return -1;
        }
    }
#endif

    if (lseek(fdin, in_off, SEEK_SET) == -1)
        return -1;

    {
        char buf[1024 * 16];
        ssize_t nread;

        while ((nread = read(fdin, buf, sizeof(buf))) != 0) {
            ssize_t nwritten = 0;

            if (nread < 0) {
                if (errno == EINTR)
                    continue;
                return -1;
            }

            while (nwritten < nread) {
                ssize_t n;

                n = write(fdout, buf + nwritten,
                          (size_t)(nread - nwritten));
                if (n < 0) {
                    if (errno == EINTR)
                        continue;
                    return -1;
                }

                if (n == 0) {
                    errno = EIO;
                    return -1;
                }

                nwritten += n;
            }
        }
    }

    return 0;
}


static int tndbw_close(struct tndb *db)
{
    tn_atomic *af = NULL;
    int    fdin = -1, fdout, type, rc;

    rc = 0;
    n_assert(db->rtflags & TNDB_R_MODE_W);

    if (n_stream_flush(db->st) != 0)
        goto l_end;
    type = db->st->type;

    if ((fdin = fcntl(db->st->fd, F_DUPFD_CLOEXEC, 0)) == -1)
        goto l_end;

    /* the trailer belongs to the body: a failure here cuts it */
    if (n_stream_close(db->st) != 0) {
        db->st = NULL;
        goto l_end;
    }
    db->st = NULL;

    /* the header, the hash table and the body go to a temporary file that
       the commit renames into place; synced, so that a crash leaves either
       the old database or the new one */
    if ((af = n_atomic_open(db->path, TN_ATOMIC_SYNC)) == NULL)
        goto l_end;

    /* the stream closes its own descriptor, the commit closes af's */
    if ((fdout = fcntl(n_atomic_fd(af), F_DUPFD_CLOEXEC, 0)) == -1)
        goto l_end;

    if ((db->st = n_stream_dopen(fdout, "wb", type)) == NULL) {
        close(fdout);
        goto l_end;
    }

    db->hdr.doffs = tndb_hdr_store_sizeof(&db->hdr) + htt_store_size(db);
    //printf("headers = %d\n", db->hdr.doffs);

    if (db->hdr.flags & TNDB_SIGN_DIGEST) {
        tndb_hdr_compute_digest(&db->hdr);

        if ((db->hdr.flags & TNDB_NOHASH) == 0)
            if (!htt_compute_digest(db))
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

    if (n_stream_flush(db->st) != 0)
        goto l_end;

    if (n_stream_close(db->st) != 0) {
        db->st = NULL;
        goto l_end;
    }
    db->st = NULL;

    if (append_body(fdin, n_atomic_fd(af)) != 0)
        goto l_end;

    /* a failed sync of the directory is reported after the rename */
    rc = n_atomic_commit(af) == 0;
    af = NULL;

 l_end:
    tndb_free(db);

    if (fdin >= 0)
        close(fdin);

    if (af != NULL)
        n_atomic_abort(af);

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
