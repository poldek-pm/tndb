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

#define TNDB_KEY_MAX 255

#include <trurl/nstream.h>
#include <trurl/narray.h>

#ifndef EXPORT
# define EXPORT extern
#endif

struct tndb;

#define TNDB_SIGN_DIGEST  (1 << 0)
#define TNDB_SIGN_OPENPGP (1 << 1)         /* detached OpenPGP signature */
#define TNDB_SIGN_SHA256  (1 << 2)         /* sha256 digest record */

#define TNDB_NOHASH       (1 << 7)         /* build db without hash table */
#define TNDB_SIGNED       TNDB_SIGN_DIGEST /* build signed db */

/* Maximum size of a raw OpenPGP signature that fits in the tndb header.
   Signer callbacks must not return a signature larger than this. */
#define TNDB_PGP_SIG_MAX  8192

/* Signer receives the SHA-256 digest of the database and returns the raw
   OpenPGP signature.  It is called by tndb_close() after the digest is
   complete and before the header is written.

   On success the signer must allocate *sig with a malloc-compatible allocator
   and set *sig_len.  Ownership of *sig transfers to tndb (it is freed on
   success and also on most failure paths).  *sig_len must be > 0 and not
   exceed TNDB_PGP_SIG_MAX.  Return 1 on success, 0 on failure. */
typedef int (*tndb_sign_fn)(const unsigned char *digest, size_t digest_len,
                            void **sig, size_t *sig_len, void *data);

/* Verifier receives the computed SHA-256 digest and the stored signature.
   Return 1 if the signature is valid, 0 otherwise. */
typedef int (*tndb_verify_sign_fn)(const unsigned char *digest,
                                   size_t digest_len,
                                   const void *sig, size_t sig_len,
                                   void *data);

/* creates new database */
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

/* Verify embedded digests only.  This uses the optional per-database .md5
   sidecar as a fast path when it matches the file's size and mtime.  It does
   NOT verify an OpenPGP signature; use tndb_verify_signature() for that. */
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
			 uint32_t *voffs, unsigned int *vlen);

EXPORT int tndb_read(struct tndb *db, long offs, void *buf, unsigned int size);


/* iterator */
struct tndb_it {
    tn_stream    *st;
    struct tndb  *_db;
    uint32_t     _nrec;
    uint32_t     _off;
    int          _get_flag;     /* for get_(begin_end) */
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
                	    uint32_t *voff, unsigned int *vlen);

/* for reading directly from db's stream */
EXPORT int tndb_it_get_begin(struct tndb_it *it, void *key, unsigned int *klen,
            		     unsigned int *vlen);
EXPORT int tndb_it_get_end(struct tndb_it *it);


EXPORT tn_array *tndb_keys(struct tndb *db);

/* Set the OpenPGP signer used for TNDB_SIGN_OPENPGP databases.
   Must be called before tndb_close() for a database being created. */
EXPORT int tndb_set_sign_fn(struct tndb *db, tndb_sign_fn fn, void *data);

/* Return TNDB_SIGN_* flags that describe what the database carries. */
EXPORT unsigned tndb_signature_flags(const struct tndb *db);

/* Verify the components required by require_flags.  require_flags must be
   non-zero.  TNDB_SIGN_OPENPGP requires a verifier callback and implies
   TNDB_SIGN_SHA256.  A return value of 0 means the requested verification
   failed or the request was invalid; the caller is responsible for enforcing
   its security policy. */
EXPORT int tndb_verify_signature(struct tndb *db, unsigned require_flags,
                                 tndb_verify_sign_fn fn, void *data);

/* number of records */
EXPORT uint32_t tndb_size(const struct tndb *db);

#endif
