/*
** 2010 April 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file implements an example of a simple VFS implementation that
** omits complex features often not required or not possible on embedded
** platforms.  Code is included to buffer writes to the journal file,
** which can be a significant performance improvement on some embedded
** platforms.
**
** OVERVIEW
**
**   The code in this file implements a minimal SQLite VFS that can be
**   used on Linux and other posix-like operating systems. The following
**   system calls are used:
**
**    File-system: access(), unlink(), getcwd()
**    File IO:     open(), read(), write(), fsync(), close(), fstat()
**    Other:       sleep(), usleep(), time()
**
**   The following VFS features are omitted:
**
**     1. File locking. The user must ensure that there is at most one
**        connection to each database when using this VFS. Multiple
**        connections to a single shared-cache count as a single connection
**        for the purposes of the previous statement.
**
**     2. The loading of dynamic extensions (shared libraries).
**
**     3. Temporary files. The user must configure SQLite to use in-memory
**        temp files when using this VFS. The easiest way to do this is to
**        compile with:
**
**          -DSQLITE_TEMP_STORE=3
**
**     4. File truncation. As of version 3.6.24, SQLite may run without
**        a working xTruncate() call, providing the user does not configure
**        SQLite to use "journal_mode=truncate", or use both
**        "journal_mode=persist" and ATTACHed databases.
**
**   It is assumed that the system uses UNIX-like path-names. Specifically,
**   that '/' characters are used to separate path components and that
**   a path-name is a relative path unless it begins with a '/'. And that
**   no UTF-8 encoded paths are greater than 512 bytes in length.
**
** JOURNAL WRITE-BUFFERING
**
**   To commit a transaction to the database, SQLite first writes rollback
**   information into the journal file. This usually consists of 4 steps:
**
**     1. The rollback information is sequentially written into the journal
**        file, starting at the start of the file.
**     2. The journal file is synced to disk.
**     3. A modification is made to the first few bytes of the journal file.
**     4. The journal file is synced to disk again.
**
**   Most of the data is written in step 1 using a series of calls to the
**   VFS xWrite() method. The buffers passed to the xWrite() calls are of
**   various sizes. For example, as of version 3.6.24, when committing a
**   transaction that modifies 3 pages of a database file that uses 4096
**   byte pages residing on a media with 512 byte sectors, SQLite makes
**   eleven calls to the xWrite() method to create the rollback journal,
**   as follows:
**
**             Write offset | Bytes written
**             ----------------------------
**                        0            512
**                      512              4
**                      516           4096
**                     4612              4
**                     4616              4
**                     4620           4096
**                     8716              4
**                     8720              4
**                     8724           4096
**                    12820              4
**             ++++++++++++SYNC+++++++++++
**                        0             12
**             ++++++++++++SYNC+++++++++++
**
**   On many operating systems, this is an efficient way to write to a file.
**   However, on some embedded systems that do not cache writes in OS
**   buffers it is much more efficient to write data in blocks that are
**   an integer multiple of the sector-size in size and aligned at the
**   start of a sector.
**
**   To work around this, the code in this file allocates a fixed size
**   buffer of SQLITE_DEMOVFS_BUFFERSZ using sqlite3_malloc() whenever a
**   journal file is opened. It uses the buffer to coalesce sequential
**   writes into aligned SQLITE_DEMOVFS_BUFFERSZ blocks. When SQLite
**   invokes the xSync() method to sync the contents of the file to disk,
**   all accumulated data is written out, even if it does not constitute
**   a complete block. This means the actual IO to create the rollback
**   journal for the example transaction above is this:
**
**             Write offset | Bytes written
**             ----------------------------
**                        0           8192
**                     8192           4632
**             ++++++++++++SYNC+++++++++++
**                        0             12
**             ++++++++++++SYNC+++++++++++
**
**   Much more efficient if the underlying OS is not caching write
**   operations.
*/

#if !defined(SQLITE_TEST) || SQLITE_OS_UNIX

#include <alloca.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <time.h>
#include <unistd.h>

#include "md5.h"
#include "sqlite3.h"
#include <ramcloud/CRamCloud.h>

/*
** Size of the write buffer used by journal files in bytes.
*/
#ifndef SQLITE_RCVFS_BUFFERSZ
# define SQLITE_RCVFS_BUFFERSZ 8192
#endif

#ifndef SQLITE_RCVFS_BLOCKSZ
# define SQLITE_RCVFS_BLOCKSZ 4096
#endif

#ifndef DPRINTF
//# define DPRINTF(...) printf(__VA_ARGS__)
# define DPRINTF(...) (0)
#endif

/*
** The maximum pathname length supported by this VFS.
*/
#define MAXPATHNAME 512


static void hex_dump(const char *buf, size_t size) {
  printf("\n HEXDUMP");
  unsigned i;
  for (i = 0; i < size; ++i) {
    if (i % 32 == 0)
      printf("\n");
    printf("%2x ", buf[i] & 0xff);
  }
  printf("\n");
}


/**
 * RAMCloud client sessions
 */
typedef struct sqlite3_rc_session RAMCLOUD_SESSION;
struct sqlite3_rc_session {
  struct rc_client *client;
  uint64_t tbl_dbs;
  uint64_t tbl_blocks;
};
RAMCLOUD_SESSION *g_sqlite3_ramcloud = NULL;  // TODO: TLS
// TODO: TLS
static RAMCLOUD_SESSION *get_rc_session() {
  return g_sqlite3_ramcloud;
}


/**
 * Database locks
 */
typedef struct sqlite3_rc_dblock RAMCLOUD_DBLOCK;
struct sqlite3_rc_dblock {
  unsigned char digest[16];  // Random 128bits token (0: unlocked)
  time_t deadline;           // Lock automatically expires once dealine passed
};
static int is_locked(RAMCLOUD_DBLOCK *lock) {
  unsigned i;
  for (i = 0; i < sizeof(lock->digest); ++i) {
    if (lock->digest[i])
      return 1;
  }
  return 0;
}


/**
 * SQlite database registry and open handles
 */
typedef struct sqlite3_rc_fileid RAMCLOUD_FILEID;
struct sqlite3_rc_fileid {  // Md5 hash over path name
  unsigned char digest[16];
};
typedef struct sqlite3_rc_db RAMCLOUD_DB;
struct sqlite3_rc_db {
  RAMCLOUD_FILEID fileid;
  RAMCLOUD_DBLOCK lock;  // non-zero objects owned by whoever has the lock token
  uint64_t size;         // Size in bytes
  uint64_t refctr;       // Counts open file handles
  unsigned blocksz;      // Size of the chunks in the block table
};
static void mk_fileid(const char *path, RAMCLOUD_FILEID *fileid) {
  md5_state_t pms;
  md5_init(&pms);
  md5_append(&pms, (const md5_byte_t *)path, strlen(path));
  md5_finish(&pms, fileid->digest);
}


/**
 * Block storage
 */
typedef struct sqlite3_rc_blockid RAMCLOUD_BLOCKID;
struct sqlite3_rc_blockid {  // Md5 hash over path name
  RAMCLOUD_FILEID fileid;
  uint64_t blockno;
};
void mk_blockid(
 const uint64_t offset,
 const unsigned blocksz,
 const RAMCLOUD_FILEID *fileid,
 RAMCLOUD_BLOCKID *blockid
) {
  blockid->fileid = *fileid;
  blockid->blockno = offset / blocksz;
}


//------------------------------------------------------------------------------


/**
 * Establishes the connection to a RAMCloud cluster
 */
int sqlite3_rcvfs_startup(const char *locator) {
  RAMCLOUD_SESSION *rc =
    (RAMCLOUD_SESSION *)sqlite3_malloc(sizeof(RAMCLOUD_SESSION));
  if (!rc) return 1;
  memset(rc, 0, sizeof(RAMCLOUD_SESSION));
  Status status;

  status = rc_connect(locator, "main", &rc->client);
  if (status != STATUS_OK)
    goto sqlite3_rcvfs_create_fail;

  status = rc_createTable(rc->client, "sqlite3_dbs", 1);
  if ((status != STATUS_OK) && (status != STATUS_OBJECT_EXISTS))
    goto sqlite3_rcvfs_create_fail;
  status = rc_getTableId(rc->client, "sqlite3_dbs", &rc->tbl_dbs);
  if (status != STATUS_OK)
    goto sqlite3_rcvfs_create_fail;

  status = rc_createTable(rc->client, "sqlite3_blocks", 1);
  if ((status != STATUS_OK) && (status != STATUS_OBJECT_EXISTS))
    goto sqlite3_rcvfs_create_fail;
  status = rc_getTableId(rc->client, "sqlite3_blocks", &rc->tbl_blocks);
  if (status != STATUS_OK)
    goto sqlite3_rcvfs_create_fail;

  g_sqlite3_ramcloud = rc;
  return 0;
 sqlite3_rcvfs_create_fail:
  free(rc);
  return 1;
}


/**
 * Disconnects from the RAMCloud cluster and frees resources
 */
void sqlite3_rcvfs_shutdown() {
  rc_disconnect(g_sqlite3_ramcloud->client);
  sqlite3_free(g_sqlite3_ramcloud);
  g_sqlite3_ramcloud = NULL;
}

typedef struct RcFile RcFile;
struct RcFile {
  sqlite3_file base;              /* Base class. Must be first. */
  RAMCLOUD_DB db;
  int flags;                      /* Open flags */
  char *aBuffer;                  /* Pointer to malloc'd buffer */
  int nBuffer;                    /* Valid bytes of data in zBuffer */
  sqlite3_int64 iBufferOfst;      /* Offset in file of zBuffer[0] */
};


/*
 * Write directly to the file passed as the first argument. Even if the
 * file has a write-buffer (RcFile.aBuffer), ignore it.
 */
static int rcDirectWrite(
  RcFile *p,                    /* File handle */
  const void *zBuf,             /* Buffer containing data to write */
  unsigned iAmt,                /* Size of data to write in bytes */
  sqlite_int64 iOfst            /* File offset to write to */
){
  DPRINTF("write direct %d %lld\n", iAmt, iOfst);
  //hex_dump(zBuf, iAmt);
  if (p->flags & SQLITE_OPEN_READONLY) return SQLITE_READONLY;

  RAMCLOUD_BLOCKID blockid;
  mk_blockid(iOfst, p->db.blocksz, &p->db.fileid, &blockid);

  // Write block-wise
  unsigned char *block = (unsigned char *)alloca(p->db.blocksz);
  unsigned remaining = iAmt;
  unsigned pos_in_block = iOfst % p->db.blocksz;
  RAMCLOUD_SESSION *rc = get_rc_session();
  while (remaining > 0) {
    memset(block, 0, p->db.blocksz);
    unsigned free_in_block = p->db.blocksz - pos_in_block;
    unsigned nbytes = (remaining > free_in_block) ? free_in_block : remaining;
    // Read-modify-write
    Status status;
    do {
      uint64_t version;
      uint32_t this_blocksz;
      status = rc_read(rc->client, rc->tbl_blocks,
                       &blockid, sizeof(blockid), NULL, &version,
                       block, p->db.blocksz, &this_blocksz);
      int new_block = 0;
      if (status == STATUS_OBJECT_DOESNT_EXIST) {
        new_block = 1;
        this_blocksz = 0;
      } else if (status != STATUS_OK) {
        return SQLITE_IOERR_WRITE;
      }

      memcpy(block + pos_in_block, (const char *)zBuf+(iAmt-remaining),
             nbytes);
      // The last block of a file can be enlarged
      if ((pos_in_block + nbytes) > this_blocksz)
        this_blocksz = pos_in_block + nbytes;

      struct RejectRules rrules;
      memset(&rrules, 0, sizeof(rrules));
      if (new_block) {
        rrules.exists = 1;
      } else {
        rrules.givenVersion = version;
      }
      status = rc_write(rc->client, rc->tbl_blocks,
                        &blockid, sizeof(blockid), block, this_blocksz,
                        &rrules, NULL);
    } while ((status == STATUS_WRONG_VERSION) ||
             (status == STATUS_OBJECT_EXISTS));
    if (status != STATUS_OK) return SQLITE_IOERR_WRITE;

    remaining -= nbytes;
    pos_in_block = 0;
    blockid.blockno++;
  }

  p->db.size = (p->db.size > iOfst + iAmt) ? p->db.size : iOfst + iAmt;
  DPRINTF("direct write OK, file size %lu\n", p->db.size);
  return SQLITE_OK;
}


/**
 * Flush the contents of the RcFile.aBuffer buffer to RAMCloud. This is a
 * no-op if this particular file does not have a buffer (i.e. it is not
 * a journal file) or if the buffer is currently empty.
 */
static int rcFlushBuffer(RcFile *p){
  DPRINTF("flushing buffer\n");
  int result = SQLITE_OK;
  if (p->nBuffer) {
    result = rcDirectWrite(p, p->aBuffer, p->nBuffer, p->iBufferOfst);
    p->nBuffer = 0;
  }
  return result;
}


/*
 * Close a file.
 */
static int rcClose(sqlite3_file *pFile) {
  DPRINTF("close\n");
  int result;
  RcFile *p = (RcFile*)pFile;
  result = rcFlushBuffer(p);
  sqlite3_free(p->aBuffer);

  RAMCLOUD_DB db;
  RAMCLOUD_SESSION *rc = get_rc_session();
  struct RejectRules rrules;
  memset(&rrules, 0, sizeof(rrules));
  Status status;
  do {  // Read-modify-write
    uint64_t version;
    uint32_t nbytes;
    status = rc_read(rc->client, rc->tbl_dbs,
                     &(p->db.fileid), sizeof(p->db.fileid), NULL, &version,
                     &db, sizeof(db), &nbytes);
    if (status != STATUS_OK) return SQLITE_IOERR;
    if (nbytes != sizeof(db)) return SQLITE_CORRUPT;
    db.refctr--;
    db.size = p->db.size;
    memset(&db.lock, 0, sizeof(db.lock));
    rrules.givenVersion = version;
    status = rc_write(rc->client, rc->tbl_dbs,
                      &(p->db.fileid), sizeof(p->db.fileid), &db, sizeof(db),
                      &rrules, NULL);
  } while (status == STATUS_WRONG_VERSION);
  if (status != STATUS_OK) return SQLITE_IOERR;
  DPRINTF("close status is %d\n", status);
  return SQLITE_OK;
}


/*
** Read data from a file.
*/
static int rcRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  DPRINTF("read %d %lld\n", iAmt, iOfst);
  RcFile *p = (RcFile*)pFile;

  /* Flush any data in the write buffer to disk in case this operation
  ** is trying to read data the file-region currently cached in the buffer.
  ** It would be possible to detect this case and possibly save an
  ** unnecessary write here, but in practice SQLite will rarely read from
  ** a journal file when there is data cached in the write-buffer.
  */
  int retval = rcFlushBuffer(p);
  if (retval != SQLITE_OK) return retval;

  // Read block-wise
  unsigned char *block = (unsigned char *)alloca(p->db.blocksz);
  RAMCLOUD_BLOCKID blockid;
  mk_blockid(iOfst, p->db.blocksz, &(p->db.fileid), &blockid);
  uint64_t written = 0;
  uint64_t remaining = iAmt;
  unsigned pos_in_block = iOfst % p->db.blocksz;
  RAMCLOUD_SESSION *rc = get_rc_session();
  while (written < iAmt) {
    uint32_t size_of_block;
    Status status = rc_read(rc->client, rc->tbl_blocks,
                            &blockid, sizeof(blockid), NULL, NULL,
                            block, p->db.blocksz, &size_of_block);
    if ((status == STATUS_OBJECT_DOESNT_EXIST) &&
        ((iOfst == 0) || (written > 0)))
    {
      return SQLITE_IOERR_SHORT_READ;
    }
    DPRINTF("read block returned %d\n", status);
    if (status != STATUS_OK) return SQLITE_IOERR_READ;
    if (size_of_block <= pos_in_block) return SQLITE_IOERR_SHORT_READ;

    size_of_block -= pos_in_block;
    const unsigned nbytes =
      (remaining > size_of_block) ? size_of_block : remaining;
    memcpy((char *)zBuf + written, block + pos_in_block, nbytes);
    blockid.blockno++;
    written += nbytes;
    remaining -= nbytes;
    pos_in_block = 0;
  }

  DPRINTF("read was fine\n");
  //hex_dump(zBuf, iAmt);
  return SQLITE_OK;
}

/*
** Write data to a crash-file.
*/
static int rcWrite(
  sqlite3_file *pFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  DPRINTF("write %d %lld\n", iAmt, iOfst);
  //hex_dump(zBuf, iAmt);
  RcFile *p = (RcFile*)pFile;

  if (p->aBuffer) {
    char *z = (char *)zBuf;       /* Pointer to remaining data to write */
    int n = iAmt;                 /* Number of bytes at z */
    sqlite3_int64 i = iOfst;      /* File offset to write to */

    while (n > 0) {
      int nCopy;                  /* Number of bytes to copy into buffer */

      /* If the buffer is full, or if this data is not being written directly
      ** following the data already buffered, flush the buffer. Flushing
      ** the buffer is a no-op if it is empty.
      */
      if ((p->nBuffer == SQLITE_RCVFS_BUFFERSZ) ||
          (p->iBufferOfst + p->nBuffer != i))
      {
        int retval = rcFlushBuffer(p);
        if (retval != SQLITE_OK) return retval;
      }
      assert((p->nBuffer == 0) || (p->iBufferOfst + p->nBuffer == i));
      p->iBufferOfst = i - p->nBuffer;

      /* Copy as much data as possible into the buffer. */
      nCopy = SQLITE_RCVFS_BUFFERSZ - p->nBuffer;
      if (nCopy > n) {
        nCopy = n;
      }
      memcpy(&p->aBuffer[p->nBuffer], z, nCopy);
      p->nBuffer += nCopy;

      n -= nCopy;
      i += nCopy;
      z += nCopy;
    }
  } else {
    return rcDirectWrite(p, zBuf, iAmt, iOfst);
  }

  return SQLITE_OK;
}


/*
** Truncate a file.  This is a no-op for this VFS (see header comments at
** the top of the file).
*/
// TODO
static int rcTruncate(sqlite3_file *pFile, sqlite_int64 size){
  DPRINTF("truncate\n");
#if 0
  if( ftruncate(((DemoFile *)pFile)->fd, size) ) return SQLITE_IOERR_TRUNCATE;
#endif
  return SQLITE_OK;
}


/*
** Sync the contents of the file to the persistent media.
*/
static int rcSync(sqlite3_file *pFile, int flags){
  DPRINTF("syncing\n");
  RcFile *p = (RcFile*)pFile;
  int retval;
  retval = rcFlushBuffer(p);
  if (retval != SQLITE_OK) return retval;

  // Write modified file size
  RAMCLOUD_DB db;
  RAMCLOUD_SESSION *rc = get_rc_session();
  struct RejectRules rrules;
  memset(&rrules, 0, sizeof(rrules));
  Status status;
  do {  // Read-modify-write
    uint64_t version;
    uint32_t nbytes;
    status = rc_read(rc->client, rc->tbl_dbs,
                     &(p->db.fileid), sizeof(p->db.fileid), NULL, &version,
                     &db, sizeof(db), &nbytes);
    if (status != STATUS_OK) return SQLITE_IOERR_FSYNC;
    if (nbytes != sizeof(db)) return SQLITE_CORRUPT;
    db.size = p->db.size;
    rrules.givenVersion = version;
    status = rc_write(rc->client, rc->tbl_dbs,
                      &(p->db.fileid), sizeof(p->db.fileid), &db, sizeof(db),
                      &rrules, NULL);
  } while (status == STATUS_WRONG_VERSION);
  if (status != STATUS_OK) return SQLITE_IOERR_FSYNC;
  return SQLITE_OK;
}

/*
** Write the size of the file in bytes to *pSize.
*/
static int rcFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  DPRINTF("file size\n");
  RcFile *p = (RcFile*)pFile;

  /* Flush the contents of the buffer to disk. As with the flush in the
  ** rcRead() method, it would be possible to avoid this and save a write
  ** here and there. But in practice this comes up so infrequently it is
  ** not worth the trouble.
  */
  int retval = rcFlushBuffer(p);
  if (retval != SQLITE_OK) return retval;

  *pSize = p->db.size;
  DPRINTF("return file size %lld\n", *pSize);
  return SQLITE_OK;
}

/*
** Locking functions. The xLock() and xUnlock() methods are both no-ops.
** The xCheckReservedLock() always indicates that no other process holds
** a reserved lock on the database file. This ensures that if a hot-journal
** file is found in the file-system it is rolled back.
*/
static int rcLock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}
static int rcUnlock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}
static int rcCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static int rcFileControl(sqlite3_file *pFile, int op, void *pArg){
  return SQLITE_OK;
}

/*
** The xSectorSize() and xDeviceCharacteristics() methods. These two
** may return special values allowing SQLite to optimize file-system
** access to some extent. But it is also safe to simply return 0.
*/
static int rcSectorSize(sqlite3_file *pFile){
  return 0;
}
static int rcDeviceCharacteristics(sqlite3_file *pFile){
  return 0;
}

/*
** Delete the file identified by argument zPath. If the dirSync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static int rcDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync) {
  DPRINTF("delete %s\n", zPath);

  RAMCLOUD_FILEID fileid;
  mk_fileid(zPath, &fileid);
  RAMCLOUD_SESSION *rc = get_rc_session();
  RAMCLOUD_DB db;
  Status status;
  uint32_t nbytes;
  status = rc_read(rc->client, rc->tbl_dbs,
                   &fileid, sizeof(fileid), NULL, NULL,
                   &db, sizeof(db), &nbytes);
  switch (status) {
    case STATUS_OK:
      break;
    case STATUS_OBJECT_DOESNT_EXIST:
      return SQLITE_OK;
    default:
      return SQLITE_IOERR_DELETE;
  }
  if (nbytes != sizeof(db)) return SQLITE_IOERR_DELETE;

  status = rc_remove(rc->client, rc->tbl_dbs, &fileid, sizeof(fileid),
                     NULL, NULL);
  if (status != STATUS_OK) return SQLITE_IOERR_DELETE;

  if (db.size == 0) return SQLITE_OK;
  unsigned nblocks = ((db.size-1) / db.blocksz) + 1;
  RAMCLOUD_BLOCKID blockid;
  blockid.fileid = fileid;
  unsigned i;
  for (i = 0; i < nblocks; ++i) {
    blockid.blockno = i;
    status = rc_remove(rc->client, rc->tbl_blocks, &blockid, sizeof(blockid),
                       NULL, NULL);
    if (status != STATUS_OK) return SQLITE_IOERR_DELETE;
  }

  DPRINTF("delete Ok\n");
  return SQLITE_OK;
}

#ifndef F_OK
# define F_OK 0
#endif
#ifndef R_OK
# define R_OK 4
#endif
#ifndef W_OK
# define W_OK 2
#endif

/*
** Query the file-system to see if the named file exists, is readable or
** is both readable and writable.
*/
static int rcAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
  DPRINTF("access %s\n", zPath);
  RAMCLOUD_FILEID fileid;
  mk_fileid(zPath, &fileid);

  RAMCLOUD_SESSION *rc = get_rc_session();
  RAMCLOUD_DB db;
  uint32_t nbytes;
  Status status = rc_read(rc->client, rc->tbl_dbs,
                          &fileid, sizeof(fileid), NULL, NULL,
                          &db, sizeof(db), &nbytes);
  switch (status) {
    case STATUS_OK:
      *pResOut = 1;
      DPRINTF("access yes\n");
      return SQLITE_OK;
    case STATUS_OBJECT_DOESNT_EXIST:
      DPRINTF("access no\n");
      *pResOut = 0;
      return SQLITE_OK;
    default:
      DPRINTF("access error\n");
      return SQLITE_IOERR;
  }
}


/*
** Argument zPath points to a nul-terminated string containing a file path.
** If zPath is an absolute path, then it is copied as is into the output
** buffer. Otherwise, if it is a relative path, then the equivalent full
** path is written to the output buffer.
**
** This function assumes that paths are UNIX style. Specifically, that:
**
**   1. Path components are separated by a '/'. and
**   2. Full paths begin with a '/' character.
*/
static int rcFullPathname(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zPath,              /* Input path (possibly a relative path) */
  int nPathOut,                   /* Size of output buffer in bytes */
  char *zPathOut                  /* Pointer to output buffer */
){
  DPRINTF("full path name %s\n", zPath);
  char zDir[MAXPATHNAME+1];
  if (zPath[0] == '/') {
    zDir[0] = '\0';
  } else {
    if (getcwd(zDir, sizeof(zDir)) == 0) return SQLITE_IOERR;
  }
  zDir[MAXPATHNAME] = '\0';

  sqlite3_snprintf(nPathOut, zPathOut, "%s/%s", zDir, zPath);
  zPathOut[nPathOut-1] = '\0';

  DPRINTF("full path name OK\n");
  return SQLITE_OK;
}

/*
** The following four VFS methods:
**
**   xDlOpen
**   xDlError
**   xDlSym
**   xDlClose
**
** are supposed to implement the functionality needed by SQLite to load
** extensions compiled as shared objects. This simple VFS does not support
** this functionality, so the following functions are no-ops.
*/
static void *rcDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}
static void rcDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}
static void (*rcDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void){
  return 0;
}
static void rcDlClose(sqlite3_vfs *pVfs, void *pHandle){
  return;
}

/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
static int rcRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte){
  return SQLITE_OK;
}

/*
** Sleep for at least nMicro microseconds. Return the (approximate) number
** of microseconds slept for.
*/
static int rcSleep(sqlite3_vfs *pVfs, int nMicro){
  sleep(nMicro / 1000000);
  usleep(nMicro % 1000000);
  return nMicro;
}

/*
** Set *pTime to the current UTC time expressed as a Julian day. Return
** SQLITE_OK if successful, or an error code otherwise.
**
**   http://en.wikipedia.org/wiki/Julian_day
**
** This implementation is not very good. The current time is rounded to
** an integer number of seconds. Also, assuming time_t is a signed 32-bit
** value, it will stop working some time in the year 2038 AD (the so-called
** "year 2038" problem that afflicts systems that store time this way).
*/
static int rcCurrentTime(sqlite3_vfs *pVfs, double *pTime){
  time_t t = time(0);
  *pTime = t/86400.0 + 2440587.5;
  return SQLITE_OK;
}


/*
** Open a file handle.
*/
static int rcOpen(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zName,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to RcFile struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
){
  static const sqlite3_io_methods rcio = {
    1,                          /* iVersion */
    rcClose,                    /* xClose */
    rcRead,                     /* xRead */
    rcWrite,                    /* xWrite */
    rcTruncate,                 /* xTruncate */
    rcSync,                     /* xSync */
    rcFileSize,                 /* xFileSize */
    rcLock,                     /* xLock */
    rcUnlock,                   /* xUnlock */
    rcCheckReservedLock,        /* xCheckReservedLock */
    rcFileControl,              /* xFileControl */
    rcSectorSize,               /* xSectorSize */
    rcDeviceCharacteristics     /* xDeviceCharacteristics */
  };

  DPRINTF("open %s\n", zName);
  RcFile *p = (RcFile*)pFile;  /* Populate this structure */

  // No support for temporary files yet (TODO: use random Md5 hash)
  if (zName == 0)
    return SQLITE_IOERR;
  RAMCLOUD_FILEID fileid;
  mk_fileid(zName, &fileid);
  RAMCLOUD_DB db;
  memset(&db, 0, sizeof(db));

  Status status;
  int new_db = 0;
  RAMCLOUD_SESSION *rc = get_rc_session();
  if (flags & SQLITE_OPEN_CREATE) {
    DPRINTF("creating db\n");
    db.fileid = fileid;
    db.refctr = 1;
    db.blocksz = SQLITE_RCVFS_BLOCKSZ;
    // TODO: locking

    struct RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.exists = 1;
    status = rc_write(rc->client, rc->tbl_dbs,
                      &fileid, sizeof(fileid), &db, sizeof(db),
                      &rrules, NULL);
    switch (status) {
      case STATUS_OK:
        new_db = 1;
        break;
      case STATUS_OBJECT_EXISTS:
        break;
      default:
        DPRINTF("STATUS %d\n", status);
        return SQLITE_CANTOPEN;
    }
  }
  if (!new_db) {
    DPRINTF("existing db\n");
    struct RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));

    do {  // Read-modify-write
      uint64_t version;
      uint32_t nbytes;
      status = rc_read(rc->client, rc->tbl_dbs,
                       &fileid, sizeof(fileid), NULL, &version,
                       &db, sizeof(db), &nbytes);
      if (status != STATUS_OK) return SQLITE_CANTOPEN;
      if (nbytes != sizeof(db)) return SQLITE_CORRUPT;
      if (is_locked(&db.lock)) return SQLITE_LOCKED;
      db.refctr++;
      // TODO: locking  ... if( flags&SQLITE_OPEN_EXCLUSIVE ) oflags |= O_EXCL;
      rrules.givenVersion = version;
      status = rc_write(rc->client, rc->tbl_dbs,
                        &fileid, sizeof(fileid), &db, sizeof(db),
                        &rrules, NULL);
    } while (status == STATUS_WRONG_VERSION);
    DPRINTF("read-modify-write status %d\n", status);
    if (status != STATUS_OK) return SQLITE_CANTOPEN;
  }

  memset(p, 0, sizeof(RcFile));
  p->db = db;
  p->flags = flags;
  if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
    p->aBuffer = (char *)sqlite3_malloc(SQLITE_RCVFS_BUFFERSZ);
    if (!p->aBuffer)
      return SQLITE_NOMEM;
  }
  if (pOutFlags) *pOutFlags = flags;
  p->base.pMethods = &rcio;
  DPRINTF("all went fine\n");
  return SQLITE_OK;
}


/*
** This function returns a pointer to the VFS implemented in this file.
** To make the VFS available to SQLite:
**
**   sqlite3_vfs_register(sqlite3_demovfs(), 0);
*/
sqlite3_vfs *sqlite3_rcvfs(void) {
  static sqlite3_vfs rcvfs = {
    1,                            /* iVersion */
    sizeof(RcFile),               /* szOsFile */
    MAXPATHNAME,                  /* mxPathname */
    0,                            /* pNext */
    "ramcloud",                   /* zName */
    0,                            /* pAppData */
    rcOpen,                       /* xOpen */
    rcDelete,                     /* xDelete */
    rcAccess,                     /* xAccess */
    rcFullPathname,               /* xFullPathname */
    rcDlOpen,                     /* xDlOpen */
    rcDlError,                    /* xDlError */
    rcDlSym,                      /* xDlSym */
    rcDlClose,                    /* xDlClose */
    rcRandomness,                 /* xRandomness */
    rcSleep,                      /* xSleep */
    rcCurrentTime,                /* xCurrentTime */
  };
  return &rcvfs;
}

#endif /* !defined(SQLITE_TEST) || SQLITE_OS_UNIX */
