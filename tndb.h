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

#ifndef TNDB_H_
#define TNDB_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define TNDB_KEY_MAX 255

#include <trurl/nstream.h>
#include <trurl/narray.h>

struct tndb;


#define TNDB_SIGN_DIGEST  (1 << 0)

#define TNDB_NOHASH       (1 << 7)         /* build db without hash table */
#define TNDB_SIGNED       TNDB_SIGN_DIGEST /* build signed db */

/* creates new database */
struct tndb *tndb_creat(const char *name, unsigned flags);

int tndb_put(struct tndb *db, const char *key, size_t klen,
             const void *val, size_t vlen);

/* opens *existing* database */
struct tndb *tndb_open(const char *path);
struct tndb *tndb_dopen(int fd, const char *path);

int tndb_close(struct tndb *db);

/* unlinks && _closes_ db */
int tndb_unlink(struct tndb *db);

int tndb_verify(struct tndb *db);

struct tndb *tndb_ref(struct tndb *db);
tn_stream *tndb_tn_stream(const struct tndb *db);
const char *tndb_path(const struct tndb *db);


int tndb_get(struct tndb *db, const void *key, size_t klen,
             void *val, size_t size);

int tndb_get_str(struct tndb *db, const char *key,
                 unsigned char *val, size_t size);

int tndb_get_voff(struct tndb *db, const void *key, size_t klen,
                  off_t *voffset, size_t *vlen);


int tndb_read(struct tndb *db, long offs, void *buf, size_t size);


/* iterator */
struct tndb_it {
    tn_stream    *st;
    struct tndb  *_db;
    uint32_t     _nrec;
    uint32_t     _off;
    int          _get_flag;     /* for get_(begin/end) */
};

#define tndb_it_stream(db) ((db)->st)


int tndb_it_start(struct tndb *db, struct tndb_it *it);

/*
  key size must be at least TNDB_KEY_MAX + 1 bytes
  if key is NULL then keys are not retrieved 
 */
int tndb_it_get(struct tndb_it *it, void *key, size_t *klen,
                void *val, size_t *vlen);

int tndb_it_get_voff(struct tndb_it *it, void *key, size_t *klen,
                     off_t *voff, size_t *vlen);

/* for reading directly from db's stream */
int tndb_it_get_begin(struct tndb_it *it, void *key, size_t *klen,
                      size_t *vlen);
int tndb_it_get_end(struct tndb_it *it);



tn_array *tndb_keys(struct tndb *db);

#endif
