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
#include <sys/types.h>          /* for off_t */

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

/* creates new database */
EXPORT struct tndb *tndb_creat(const char *name, int comprlevel, unsigned flags);

EXPORT int tndb_put(struct tndb *db, const char *key, unsigned int klen,
		    const void *val, unsigned int vlen);

/* opens *existing* database */
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

EXPORT int tndb_get_voff(struct tndb *db, const void *key, unsigned int aklen,
			 off_t *voffs, unsigned int *vlen);

EXPORT int tndb_read(struct tndb *db, long offs, void *buf, unsigned int size);


/* iterator */
struct tndb_it {
    tn_stream    *st;
    struct tndb  *_db;
    uint32_t     _nrec;
    uint32_t     _off;
    int          _get_flag;     /* for get_(begin/end) */
};

#define tndb_it_stream(db) ((db)->st)


EXPORT int tndb_it_start(struct tndb *db, struct tndb_it *it);

/*
  key size must be at least TNDB_KEY_MAX + 1 bytes
  if key is NULL then keys are not retrieved 
 */
EXPORT int tndb_it_get(struct tndb_it *it, void *key, unsigned int *klen,
		       void *val, unsigned int *vlen);

/* same as tndb_it_get() but, *val is realloced() if needed */
EXPORT int tndb_it_rget(struct tndb_it *it, void *key, unsigned int *klen,
	                void **val, unsigned int *vlen);

EXPORT int tndb_it_get_voff(struct tndb_it *it, void *key, unsigned int *klen,
                	    off_t *voff, unsigned int *vlen);

/* for reading directly from db's stream */
EXPORT int tndb_it_get_begin(struct tndb_it *it, void *key, unsigned int *klen,
            		     unsigned int *vlen);
EXPORT int tndb_it_get_end(struct tndb_it *it);


EXPORT tn_array *tndb_keys(struct tndb *db);

#endif
