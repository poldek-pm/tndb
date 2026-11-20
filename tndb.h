/*
  Copyright (C) 2002 Pawel A. Gajda <mis@k2.net.pl>

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU Library General Public License, version 2
  as published by the Free Software Foundation (see file COPYING for details).

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#ifndef TNDB_H_
#define TNDB_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "version.h"

#define TNDB_KEY_MAX 255

#include <trurl/nstream.h>
#include <trurl/narray.h>

#ifndef EXPORT
# define EXPORT extern
#endif

struct tndb;

#define TNDB_SIGN_DIGEST  (1 << 0)

#define TNDB_NOHASH       (1 << 7)         /* build db without hash table */
#define TNDB_SIGNED       TNDB_SIGN_DIGEST /* build signed db */

/* creates new database; tndb_close() writes it with trurlib's n_atomic_*()
   (see <trurl/natomic.h>): as the hidden .<name>.tmp.XXXXXX next to it,
   synced and renamed into place, so a failed close leaves an existing
   file as it was and a crash leaves either the old file or the new one.
   A failed sync of the directory is reported although the file is
   already replaced.  A symlink is followed to its target, a hard link is
   split, a directory or a special file is refused. */
EXPORT struct tndb *tndb_creat(const char *name, int comprlevel, unsigned flags);

EXPORT int tndb_put(struct tndb *db, const char *key, unsigned int klen,
		    const void *val, unsigned int vlen);

/* opens *existing* database

   tndb_dopen() takes ownership of fd: tndb_close() closes it on success and
   tndb_dopen() itself closes it on failure. */
EXPORT struct tndb *tndb_open(const char *path);
EXPORT struct tndb *tndb_dopen(int fd, const char *path);

EXPORT int tndb_close(struct tndb *db);

/* unlinks && _closes_ db */
EXPORT int tndb_unlink(struct tndb *db);

EXPORT int tndb_verify(struct tndb *db);

EXPORT struct tndb *tndb_ref(struct tndb *db);
EXPORT tn_stream *tndb_tn_stream(const struct tndb *db);
EXPORT const char *tndb_path(const struct tndb *db);

/**
* Reads value associated with key. Buffer (val) must be long enough to hold the whole value.
* Returns number of bytes that was read into buffer.
*/
EXPORT int tndb_get(struct tndb *db, const void *key, unsigned int klen,
        	    void *val, unsigned int valsize);

/**
* This function is similar to tndb_get(), except it uses dynamically allocated buffer, which
* can hold the whole value regardless of its length and should be freed when no
* longer needed.
*/
EXPORT size_t tndb_get_all(struct tndb *db, const void *key, size_t klen,
        	           void **val);

EXPORT int tndb_get_str(struct tndb *db, const char *key,
			unsigned char *val, unsigned int valsize);

/* 1 if found, 0 if not, -1 for a record that cannot be read or that ends
   past the 32-bit offsets of the file */
EXPORT int tndb_get_voff(struct tndb *db, const void *key, unsigned int aklen,
			 uint32_t *voffs, unsigned int *vlen);

EXPORT int tndb_read(struct tndb *db, long offs, void *buf, unsigned int size);


/* iterator */
struct tndb_it {
    tn_stream    *st;
    struct tndb  *_db;
    uint32_t     _nrec;
    uint32_t     _off;
    int          _state;        /* on a record, in its value, or broken */
};

#define tndb_it_stream(db) ((db)->st)


EXPORT int tndb_it_start(struct tndb *db, struct tndb_it *it);

/*
  readers: 1 for a record, 0 at the end, -1 for a broken record (EBADMSG);
  -1 ends the iteration, a reader called after it returns 0; with -1 the
  key, and the value of tndb_it_get() and tndb_it_rget(), are empty
  (*klen and *vlen are 0), so the broken record is named by the one before
  it. tndb_it_get_voff() reports 0 in *klen, *voff and *vlen unless it
  returns 1, tndb_it_get_begin() in *klen and *vlen; tndb_it_get() and
  tndb_it_rget() zero *vlen only with -1 and leave it as it was on 0
 */

/*
  key size must be at least TNDB_KEY_MAX + 1 bytes
  if key is NULL then keys are not retrieved
  *vlen is the size of val, then the length of the value;
  -1 with ERANGE if the value does not fit (see tndb_it_rget())
 */
EXPORT int tndb_it_get(struct tndb_it *it, void *key, unsigned int *klen,
		       void *val, unsigned int *vlen);

/* same as tndb_it_get() but, *val is realloced() if needed */
EXPORT int tndb_it_rget(struct tndb_it *it, void *key, unsigned int *klen,
	                void **val, unsigned int *vlen);

EXPORT int tndb_it_get_voff(struct tndb_it *it, void *key, unsigned int *klen,
                	    uint32_t *voff, unsigned int *vlen);

/* for reading directly from db's stream */
EXPORT int tndb_it_get_begin(struct tndb_it *it, void *key, unsigned int *klen,
            		     unsigned int *vlen);
/* skips the rest of the value; 0 if the file ends before it, which ends
   the iteration */
EXPORT int tndb_it_get_end(struct tndb_it *it);


/* NULL for a broken database */
EXPORT tn_array *tndb_keys(struct tndb *db);

/* number of records */
EXPORT uint32_t tndb_size(const struct tndb *db);

#endif
