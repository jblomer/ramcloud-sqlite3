
#if !defined(SQLITE_TEST) || SQLITE_OS_UNIX

#include <alloca.h>
#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include "md5.h"
#include "sqlite3.h"
#include "vfs-ramcloud-stats.h"
#include <ramcloud/CRamCloud.h>

#ifdef __cplusplus
extern "C" {
#endif

// Size of the write buffer used by journal files in bytes.
#ifndef SQLITE_RCVFS_BUFFERSZ
# define SQLITE_RCVFS_BUFFERSZ 8192
#endif

// Default page size
#ifndef SQLITE_RCVFS_BLOCKSZ
# define SQLITE_RCVFS_BLOCKSZ 1024
#endif

#ifndef DPRINTF
#  ifdef DEBUG
#    define DPRINTF(...) printf(__VA_ARGS__)
#  else
#    define DPRINTF(...) (0)
#  endif
#endif

#define SQLITE_RCVFS_TIMESKEW 2  // 2 seconds maximum time de-syncronization
// Allocate so many leases on stack and only use malloc if this is not enough
#define SQLITE_RCVFS_STACKLEASES 8
#define SQLITE_RCVFS_LEASETIME 20000  // 20 seconds lease time
#define SQLITE_RCVFS_WBUF_NBLOCKS 128

/**
 * The maximum pathname length supported by this VFS.
 */
#define MAXPATHNAME 4096


/*static void hex_dump(const char *buf, size_t size) {
  printf("\n HEXDUMP");
  unsigned i;
  for (i = 0; i < size; ++i) {
    if (i % 32 == 0)
      printf("\n");
    printf("%2x ", buf[i] & 0xff);
  }
  printf("\n");
}*/

static int rcRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte);


typedef struct sqlite3_rcvfs_connection SQLITE_RCVFS_CONNECTION;
struct sqlite3_rcvfs_connection {
  char *locator;
  char *cluster_name;
  char *table_name;
  uint64_t tblid;
  pthread_key_t tls;  // Points to a SQLITE_RCVFS_SESSION
};


/**
 * RAMCloud client sessions
 */
typedef struct sqlite3_rcvfs_session SQLITE_RCVFS_SESSION;
struct sqlite3_rcvfs_session {
  SQLITE_RCVFS_CONNECTION *conn;
  struct rc_client *client;
};
static SQLITE_RCVFS_SESSION *get_rc_session(SQLITE_RCVFS_CONNECTION *conn) {
  SQLITE_RCVFS_SESSION *rcs = pthread_getspecific(conn->tls);
  if (!rcs) {
    rcs = (SQLITE_RCVFS_SESSION *)sqlite3_malloc(sizeof(SQLITE_RCVFS_SESSION));
    if (!rcs) return NULL;
    memset(rcs, 0, sizeof(SQLITE_RCVFS_SESSION));
    rcs->conn = conn;
    Status status = rc_connect(conn->locator, conn->cluster_name, &rcs->client);
    if (status != STATUS_OK) {
      sqlite3_free(rcs);
      return NULL;
    }
    int retval = pthread_setspecific(conn->tls, rcs);
    if (retval != 0) {
      rc_disconnect(rcs->client);
      sqlite3_free(rcs);
      return NULL;
    }
  }
  return rcs;
}


/**
 * Database locks
 */
typedef struct sqlite3_rcvfs_token SQLITE_RCVFS_TOKEN;
struct sqlite3_rcvfs_token {
  unsigned char digest[19];  // Random 152bits token (0: unlocked)
};
static SQLITE_RCVFS_TOKEN mk_token() {
  SQLITE_RCVFS_TOKEN result;
  int retval = rcRandomness(NULL, 19, (char *)result.digest);
  assert(retval == 19);
  return result;
}
typedef struct sqlite3_rcvfs_lease SQLITE_RCVFS_LEASE;
struct sqlite3_rcvfs_lease {
  char lease_type;
  SQLITE_RCVFS_TOKEN token;
  time_t deadline;           // Lease expires once dealine passed
};
static int is_owned(
  const SQLITE_RCVFS_LEASE *lease,
  const SQLITE_RCVFS_TOKEN *my_token
){
  unsigned i;
  for (i = 0; i < sizeof(lease->token.digest); ++i) {
    if (lease->token.digest[i] != my_token->digest[i])
      return 0;
  }
  return 1;
}
static int is_locked(const SQLITE_RCVFS_LEASE *lease) {
  unsigned i;
  for (i = 0; i < sizeof(lease->token.digest); ++i) {
    if (lease->token.digest[i])
      return lease->deadline + SQLITE_RCVFS_TIMESKEW > time(NULL);
  }
  return 0;
}


/**
 * SQlite database registry and open handles
 */
typedef struct sqlite3_rcvfs_dbid SQLITE_RCVFS_DBID;
struct sqlite3_rcvfs_dbid {
  unsigned char digest[16];
};
static SQLITE_RCVFS_DBID mk_dbid(const char *path) {
  SQLITE_RCVFS_DBID result;
  md5_state_t pms;
  if (path) {
    md5_init(&pms);
    md5_append(&pms, (const md5_byte_t *)path, strlen(path));
    md5_finish(&pms, result.digest);
  } else {
    int retval = rcRandomness(NULL, 16, (char *)result.digest);
    assert(retval == 16);
  }
  /*unsigned i;
  for (i = 0; i < 16; ++i) {
    char dgt1 = (unsigned)result.digest[i] / 16;
    char dgt2 = (unsigned)result.digest[i] % 16;
    dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
    dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
    result.table_name[i*2] = dgt1;
    result.table_name[i*2+1] = dgt2;
  }
  result.table_name[32] = '\0';*/
  return result;
}
typedef struct sqlite3_rcvfs_dbheader SQLITE_RCVFS_DBHEADER;
struct sqlite3_rcvfs_dbheader {
  int version;           // Currently 1
  uint64_t size;         // Size in bytes
  uint64_t blocksz;      // Size of the chunks in the block table
};
typedef struct sqlite3_rcvfs_handle SQLITE_RCVFS_HANDLE;
struct sqlite3_rcvfs_handle {
  SQLITE_RCVFS_CONNECTION *conn;
  uint64_t tblid;
  uint64_t size;
  uint64_t blocksz;
  SQLITE_RCVFS_DBID dbid;
  SQLITE_RCVFS_TOKEN token;
  SQLITE_RCVFS_LEASE *leases;  // cache of leases
  unsigned nLeases;
  unsigned capacityLeases;
  uint64_t versionLeases;
};


/**
 * Block storage
 */
typedef struct sqlite3_rcvfs_blockid SQLITE_RCVFS_BLOCKID;
struct sqlite3_rcvfs_blockid {
  uint64_t blockno;
};
SQLITE_RCVFS_BLOCKID SQLITE_RCVFS_HEADERBLOCK  = { (uint64_t)(-1) };
SQLITE_RCVFS_BLOCKID SQLITE_RCVFS_LCBLOCK      = { (uint64_t)(-2) };
SQLITE_RCVFS_BLOCKID SQLITE_RCVFS_INVALIDBLOCK = { (uint64_t)(-4) };
static inline int isInvalidBlock(SQLITE_RCVFS_BLOCKID *blockid) {
  return blockid->blockno == SQLITE_RCVFS_INVALIDBLOCK.blockno;
}
typedef struct sqlite3_rcvfs_blockkey SQLITE_RCVFS_BLOCKKEY;
struct sqlite3_rcvfs_blockkey {
  SQLITE_RCVFS_DBID dbid;
  SQLITE_RCVFS_BLOCKID blockid;
};

typedef struct sqlite3_rcvfs_wbuffer SQLITE_RCVFS_WBUFFER;
struct sqlite3_rcvfs_wbuffer {
  SQLITE_RCVFS_BLOCKID blockIds[SQLITE_RCVFS_WBUF_NBLOCKS];
  uint16_t blockSizes[SQLITE_RCVFS_WBUF_NBLOCKS];
  unsigned char buf[SQLITE_RCVFS_WBUF_NBLOCKS][SQLITE_RCVFS_BLOCKSZ];
};
static inline void clearBufferBlock(SQLITE_RCVFS_WBUFFER *buf, unsigned idx) {
  buf->blockIds[idx] = SQLITE_RCVFS_INVALIDBLOCK;
  buf->blockSizes[idx] = 0;
  memset(buf->buf[idx], 0, SQLITE_RCVFS_BLOCKSZ);
}


typedef struct RcFile RcFile;
struct RcFile {
  sqlite3_file base;              /* Base class. Must be first. */
  SQLITE_RCVFS_HANDLE handle;
  int flags;                      /* Open flags */
  SQLITE_RCVFS_WBUFFER *blockBuffer;
  uint64_t permanentSize;
};

//------------------------------------------------------------------------------

typedef int64_t atomic_int64;

static void inline __attribute__((used)) atomic_init64(atomic_int64 *a) {
  *a = 0;
}

static int64_t inline __attribute__((used)) atomic_read64(atomic_int64 *a) {
  return __sync_fetch_and_add(a, 0);
}

static void inline __attribute__((used)) atomic_inc64(atomic_int64 *a) {
  (void) __sync_fetch_and_add(a, 1);
}

static int64_t inline __attribute__((used)) atomic_xadd64(atomic_int64 *a,
                                                          int64_t offset)
{
  if (offset < 0)
    return __sync_fetch_and_sub(a, -offset);
  return __sync_fetch_and_add(a, offset);
}


atomic_int64 sqlite_rcvfs_nread = 0;
atomic_int64 sqlite_rcvfs_nwrite = 0;
atomic_int64 sqlite_rcvfs_nmwrite = 0;
atomic_int64 sqlite_rcvfs_nremove = 0;
atomic_int64 sqlite_rcvfs_nmremove = 0;
atomic_int64 sqlite_rcvfs_szread = 0;
atomic_int64 sqlite_rcvfs_szwrite = 0;


//------------------------------------------------------------------------------


static char *sqlite3_strdup(const char *str) {
  size_t len = strlen(str) + 1;
  char *result = sqlite3_malloc(len);
  if (!result) return NULL;
  memcpy(result, str, len);
  return result;
}


static void sqlite3_rcvfs_tls_destructor(void *data) {
  SQLITE_RCVFS_SESSION *rcs = (SQLITE_RCVFS_SESSION *)data;
  if (rcs->client) rc_disconnect(rcs->client);
  sqlite3_free(rcs);
}


void sqlite3_rcvfs_get_stats(SQLITE_RCVFS_STATS *stats) {
  stats->nread = atomic_read64(&sqlite_rcvfs_nread);
  stats->nwrite = atomic_read64(&sqlite_rcvfs_nwrite);
  stats->nmwrite = atomic_read64(&sqlite_rcvfs_nmwrite);
  stats->nremove = atomic_read64(&sqlite_rcvfs_nremove);
  stats->nmremove = atomic_read64(&sqlite_rcvfs_nmremove);
  stats->szread = atomic_read64(&sqlite_rcvfs_szread);
  stats->szwrite = atomic_read64(&sqlite_rcvfs_szwrite);
}


/**
 * Establishes the connection to a RAMCloud cluster
 */
SQLITE_RCVFS_CONNECTION *sqlite3_rcvfs_connect(
  const char *locator,
  const char *cluster_name,
  const char *table_name
){
  int retval;
  SQLITE_RCVFS_CONNECTION *conn = NULL;
  SQLITE_RCVFS_SESSION *rcs = NULL;

  conn =
    (SQLITE_RCVFS_CONNECTION *)sqlite3_malloc(sizeof(SQLITE_RCVFS_CONNECTION));
  if (!conn) goto sqlite3_rcvfs_connect_fail;
  memset(conn, 0, sizeof(SQLITE_RCVFS_CONNECTION));
  conn->locator = sqlite3_strdup(locator);
  conn->cluster_name = sqlite3_strdup(cluster_name);
  conn->table_name = sqlite3_strdup(table_name);
  if (!conn->locator || !conn->cluster_name || !conn->table_name)
    goto sqlite3_rcvfs_connect_fail;

  rcs = (SQLITE_RCVFS_SESSION *)sqlite3_malloc(sizeof(SQLITE_RCVFS_SESSION));
  if (!rcs) goto sqlite3_rcvfs_connect_fail;
  memset(rcs, 0, sizeof(SQLITE_RCVFS_SESSION));
  rcs->conn = conn;
  Status status = rc_connect(locator, cluster_name, &rcs->client);
  if (status != STATUS_OK) goto sqlite3_rcvfs_connect_fail;

  status = rc_createTable(rcs->client, conn->table_name, 1);
  if (status != STATUS_OK) goto sqlite3_rcvfs_connect_fail;
  status = rc_getTableId(rcs->client, conn->table_name, &(conn->tblid));
  if (status != STATUS_OK) goto sqlite3_rcvfs_connect_fail;

  retval = pthread_key_create(&conn->tls, sqlite3_rcvfs_tls_destructor);
  if (retval != 0) goto sqlite3_rcvfs_connect_fail;
  retval = pthread_setspecific(conn->tls, rcs);
  if (retval != 0) {
    pthread_key_delete(conn->tls);
    goto sqlite3_rcvfs_connect_fail;
  }

  return conn;

 sqlite3_rcvfs_connect_fail:
  if (conn) {
    if (conn->locator) sqlite3_free(conn->locator);
    if (conn->cluster_name) sqlite3_free(conn->cluster_name);
    if (conn->table_name) sqlite3_free(conn->table_name);
    sqlite3_free(conn);
  }
  if (rcs) {
    if (rcs->client) rc_disconnect(rcs->client);
    sqlite3_free(rcs);
  }
  return NULL;
}


void sqlite3_rcvfs_disconnect(SQLITE_RCVFS_CONNECTION *conn) {
  SQLITE_RCVFS_SESSION *rcs = pthread_getspecific(conn->tls);
  if (rcs) {
    sqlite3_rcvfs_tls_destructor((void *)rcs);
  }
  pthread_key_delete(conn->tls);
  sqlite3_free(conn->locator);
  sqlite3_free(conn->cluster_name);
  sqlite3_free(conn->table_name);
  sqlite3_free(conn);
}


//------------------------------------------------------------------------------


static int rcFlushBlockBuffer(
  SQLITE_RCVFS_SESSION *rcs,
  RcFile *p
){
  DPRINTF("flush block buffer\n");
  if (!p->blockBuffer) return SQLITE_OK;

  uint16_t N = SQLITE_RCVFS_WBUF_NBLOCKS;
  uint16_t szMultiOpWrite = rc_multiOpSizeOf(MULTI_OP_WRITE);
  SQLITE_RCVFS_BLOCKKEY *block_keys = (SQLITE_RCVFS_BLOCKKEY *)
    alloca(N * sizeof(SQLITE_RCVFS_BLOCKKEY));
  unsigned char *mWriteObjects = (unsigned char *) alloca(N * szMultiOpWrite);
  void **pmWriteObjects = (void **)alloca(N * sizeof(void *));

  unsigned num_requests = 0;
  unsigned i;
  for (i = 0; i < SQLITE_RCVFS_WBUF_NBLOCKS; ++i) {
    if (!isInvalidBlock(&(p->blockBuffer->blockIds[i]))) {
      block_keys[num_requests].dbid = p->handle.dbid;
      block_keys[num_requests].blockid = p->blockBuffer->blockIds[i];
      pmWriteObjects[num_requests] =
        mWriteObjects + (num_requests * szMultiOpWrite);
      rc_multiWriteCreate(rcs->conn->tblid,
                          &(block_keys[num_requests]), sizeof(SQLITE_RCVFS_BLOCKKEY),
                          p->blockBuffer->buf[num_requests],
                          p->blockBuffer->blockSizes[num_requests],
                          NULL, pmWriteObjects[num_requests]);
      atomic_xadd64(&sqlite_rcvfs_szwrite,
                    p->blockBuffer->blockSizes[num_requests]);
      num_requests++;
    }
  }
  if (num_requests == 0) return SQLITE_OK;

  atomic_inc64(&sqlite_rcvfs_nmwrite);
  rc_multiWrite(rcs->client, pmWriteObjects, num_requests);
  memset(p->blockBuffer, 0, sizeof(p->blockBuffer));
  for (i = 0; i < SQLITE_RCVFS_WBUF_NBLOCKS; ++i)
    clearBufferBlock(p->blockBuffer, i);

  int result = SQLITE_OK;
  for (i = 0; i < num_requests; ++i) {
    Status status = rc_multiOpStatus(pmWriteObjects[i], MULTI_OP_WRITE);
    if (status != STATUS_OK) result = SQLITE_IOERR;
    rc_multiOpDestroy(pmWriteObjects[i], MULTI_OP_WRITE);
  }

  return result;
}


/**
 * Write directly to the file passed as the first argument.
 */
static int rcBufferedWrite(
  SQLITE_RCVFS_SESSION *rcs,
  RcFile *p,                    /* File handle */
  const void *zBuf,             /* Buffer containing data to write */
  unsigned iAmt,                /* Size of data to write in bytes */
  sqlite_int64 iOfst            /* File offset to write to */
){
  DPRINTF("write into block buffer %d %lld\n", iAmt, iOfst);
  //hex_dump(zBuf, iAmt);
  if (p->flags & SQLITE_OPEN_READONLY) return SQLITE_READONLY;

  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = p->handle.dbid;
  block_key.blockid.blockno = iOfst / p->handle.blocksz;

  // Write block-wise
  Status status;
  unsigned char *block = NULL;
  unsigned remaining = iAmt;
  unsigned pos_in_block = iOfst % p->handle.blocksz;
  while (remaining > 0) {
    block = NULL;
    unsigned free_in_block = p->handle.blocksz - pos_in_block;
    unsigned nbytes = (remaining > free_in_block) ? free_in_block : remaining;
    uint64_t absolute_offset = iOfst + iAmt - remaining;
    uint32_t this_blocksz = 0;

    // Check in buffer
    int in_cache = 0;
    unsigned i;
    int idx_free_block = -1;
    for (i = 0; i < SQLITE_RCVFS_WBUF_NBLOCKS; ++i) {
      if (p->blockBuffer->blockIds[i].blockno == block_key.blockid.blockno) {
        block = p->blockBuffer->buf[i];
        this_blocksz = p->blockBuffer->blockSizes[i];
        in_cache = 1;
        break;
      } else if ((idx_free_block == -1) &&
                 isInvalidBlock(&(p->blockBuffer->blockIds[i])))
      {
        idx_free_block = i;
      }
    }

    // If not already in buffer, occupy a new block.  Flush if necessary.
    if (!in_cache) {
      if (idx_free_block == -1) {
        int retval = rcFlushBlockBuffer(rcs, p);
        if (retval != SQLITE_OK) return retval;
        idx_free_block = 0;
      }
      block = p->blockBuffer->buf[idx_free_block];
      p->blockBuffer->blockIds[idx_free_block] = block_key.blockid;
      if ((pos_in_block + nbytes) > p->blockBuffer->blockSizes[idx_free_block])
        p->blockBuffer->blockSizes[idx_free_block] = pos_in_block + nbytes;
    }

    // Read only if this is not a full block and not in cache and if there
    // is a block for it in RAMCloud
    if (!in_cache &&
        (absolute_offset < p->permanentSize) &&
        ((pos_in_block != 0) || (remaining < p->handle.blocksz)))
    {
      atomic_inc64(&sqlite_rcvfs_nread);
      status = rc_read(rcs->client, p->handle.tblid,
                       &block_key, sizeof(block_key), NULL, NULL,
                       block, p->handle.blocksz, &this_blocksz);
      atomic_xadd64(&sqlite_rcvfs_szread, this_blocksz);
      if ((status != STATUS_OK) && (status != STATUS_OBJECT_DOESNT_EXIST))
        return SQLITE_IOERR_WRITE;
    }

    memcpy(block + pos_in_block, (const char *)zBuf+(iAmt-remaining), nbytes);
    if ((pos_in_block + nbytes) > this_blocksz)
      this_blocksz = pos_in_block + nbytes;

    remaining -= nbytes;
    pos_in_block = 0;
    block_key.blockid.blockno++;
  }

  p->handle.size = (p->handle.size > iOfst + iAmt) ?
                   p->handle.size : iOfst + iAmt;
  DPRINTF("buffered write OK, file size %lu\n", p->handle.size);
  return SQLITE_OK;
}


static int rcDeleteInternal(
  SQLITE_RCVFS_SESSION *rcs,
  SQLITE_RCVFS_DBID dbid,
  uint64_t blocksz,
  uint64_t size
){
  uint64_t max_block = size / blocksz;
  unsigned nbatch = (max_block + 2) > 1024 ? 1024 : max_block + 2;
  uint16_t szMultiOpRemove = rc_multiOpSizeOf(MULTI_OP_REMOVE);
  DPRINTF("delete internal %lu blocks\n", max_block);

  SQLITE_RCVFS_BLOCKKEY *block_keys = (SQLITE_RCVFS_BLOCKKEY *)
    alloca(nbatch * sizeof(SQLITE_RCVFS_BLOCKKEY));
  unsigned char *mRemoveObjects = (unsigned char *)
    alloca(nbatch * szMultiOpRemove);
  void **pmRemoveObjects = (void **)alloca(nbatch * sizeof(void *));

  uint64_t nremoved = 0;
  while (nremoved < max_block + 2) {
    unsigned i;
    for (i = 0; (i < nbatch) && (nremoved < (max_block + 2)); ++i, ++nremoved) {
      block_keys[i].dbid = dbid;
      block_keys[i].blockid.blockno = nremoved - 2;
      pmRemoveObjects[i] = mRemoveObjects + (i * szMultiOpRemove);
      rc_multiRemoveCreate(rcs->conn->tblid,
                           &(block_keys[i]), sizeof(SQLITE_RCVFS_BLOCKKEY),
                           NULL,
                           pmRemoveObjects[i]);
    }
    atomic_inc64(&sqlite_rcvfs_nmremove);
    rc_multiRemove(rcs->client, pmRemoveObjects, i);
    unsigned j;
    for (j = 0; j < i; ++j)
      rc_multiOpDestroy(pmRemoveObjects[j], MULTI_OP_REMOVE);
    for (j = 0; j < i; ++j) {
      Status status = rc_multiOpStatus(pmRemoveObjects[j], MULTI_OP_REMOVE);
      if ((status != STATUS_OK) && (status != STATUS_OBJECT_DOESNT_EXIST))
        return SQLITE_IOERR;
    }
  }
  return SQLITE_OK;
}


/**
 * Close a database.  All changes must be already committed.
 */
static int rcClose(sqlite3_file *pFile) {
  RcFile *p = (RcFile*)pFile;
  DPRINTF("close (my token %d)\n", p->handle.token.digest[0] & 0xff);
  sqlite3_free(p->blockBuffer);
  sqlite3_free(p->handle.leases);

  //int retval = p->base.pMethods->xUnlock(pFile, 0);
  //assert(retval == SQLITE_OK);

  if (p->flags & SQLITE_OPEN_DELETEONCLOSE) {
    SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
    if (!rcs) return SQLITE_IOERR;
    return rcDeleteInternal(rcs,
                            p->handle.dbid, p->handle.blocksz, p->permanentSize);
  }

  DPRINTF("RETURN close\n");
  return SQLITE_OK;
}


/**
 * Read data from a file.
 */
static int rcRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  //printf("R %d %lld\n", iAmt, iOfst);
  DPRINTF("read %d %lld\n", iAmt, iOfst);
  RcFile *p = (RcFile*)pFile;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_READ;
  if (iOfst >= p->handle.size) return SQLITE_IOERR_SHORT_READ;

  // Read block-wise
  unsigned char *block = (unsigned char *)alloca(p->handle.blocksz);
  unsigned char *block_on_stack = block;
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = p->handle.dbid;
  block_key.blockid.blockno = iOfst / p->handle.blocksz;
  uint64_t written = 0;
  uint64_t remaining = iAmt;
  unsigned pos_in_block = iOfst % p->handle.blocksz;
  while (written < iAmt) {
    block = block_on_stack;
    uint32_t size_of_block = 0;

    // Check in blockBuffer
    if (p->blockBuffer) {
      unsigned i;
      for (i = 0; i < SQLITE_RCVFS_WBUF_NBLOCKS; ++i) {
        if (p->blockBuffer->blockIds[i].blockno == block_key.blockid.blockno) {
          block = p->blockBuffer->buf[i];
          size_of_block = p->blockBuffer->blockSizes[i];
          break;
        }
      }
    }
    // Not in buffer, fetch from RAMCloud
    if (block == block_on_stack) {
      atomic_inc64(&sqlite_rcvfs_nread);
      Status status = rc_read(rcs->client, p->handle.tblid,
                              &block_key, sizeof(block_key), NULL, NULL,
                              block, p->handle.blocksz, &size_of_block);
      atomic_xadd64(&sqlite_rcvfs_szread, size_of_block);
      if ((status == STATUS_OBJECT_DOESNT_EXIST) &&
          ((iOfst == 0) || (written > 0)))
      {
        return SQLITE_IOERR_SHORT_READ;
      }
      //DPRINTF("read block returned %d\n", status);
      if (status != STATUS_OK) return SQLITE_IOERR_READ;
    }
    if (size_of_block <= pos_in_block) return SQLITE_IOERR_READ;

    size_of_block -= pos_in_block;
    const unsigned nbytes =
      (remaining > size_of_block) ? size_of_block : remaining;
    memcpy((char *)zBuf + written, block + pos_in_block, nbytes);
    block_key.blockid.blockno++;
    written += nbytes;
    remaining -= nbytes;
    pos_in_block = 0;
  }

  DPRINTF("RETURN read was fine\n");
  //hex_dump(zBuf, iAmt);
  return SQLITE_OK;
}


/**
 * Write data to a file.
 */
static int rcWrite(
  sqlite3_file *pFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  //printf("W %d %lld\n", iAmt, iOfst);
  DPRINTF("write %d %lld\n", iAmt, iOfst);
  //hex_dump(zBuf, iAmt);
  RcFile *p = (RcFile*)pFile;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_WRITE;

  return rcBufferedWrite(rcs, p, zBuf, iAmt, iOfst);
}


/**
 * Truncate a file.  This is a no-op for this VFS (see header comments at
 * the top of the file).
 */
// TODO
static int rcTruncate(sqlite3_file *pFile, sqlite_int64 size){
  printf("truncate\n");
#if 0
  if( ftruncate(((DemoFile *)pFile)->fd, size) ) return SQLITE_IOERR_TRUNCATE;
#endif
  return SQLITE_OK;
}


/**
 * Sync the contents of the file to the persistent media.
 */
static int rcSync(sqlite3_file *pFile, int flags){
  DPRINTF("syncing\n");
  RcFile *p = (RcFile*)pFile;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_FSYNC;

  int retval;
  retval = rcFlushBlockBuffer(rcs, p);
  if (retval != SQLITE_OK) return retval;

  // Write modified file size
  SQLITE_RCVFS_DBHEADER dbheader;
  memset(&dbheader, 0, sizeof(dbheader));
  dbheader.version = 1;
  dbheader.size = p->handle.size;
  dbheader.blocksz = p->handle.blocksz;
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = p->handle.dbid;
  block_key.blockid = SQLITE_RCVFS_HEADERBLOCK;
  Status status;
  atomic_inc64(&sqlite_rcvfs_nwrite);
  status = rc_write(rcs->client, p->handle.tblid,
                    &block_key, sizeof(block_key),
                    &dbheader, sizeof(dbheader), NULL, NULL);
  if (status != STATUS_OK) {
    DPRINTF("syncing failed %d\n", status);
    return SQLITE_IOERR_FSYNC;
  }
  atomic_xadd64(&sqlite_rcvfs_szwrite, sizeof(dbheader));
  p->permanentSize = p->handle.size;
  DPRINTF("syncing ok\n");
  return SQLITE_OK;
}


/**
 * Write the size of the file in bytes to *pSize.
 */
static int rcFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  DPRINTF("file size\n");
  RcFile *p = (RcFile*)pFile;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR;

  *pSize = p->handle.size;
  DPRINTF("return file size %lld\n", *pSize);
  return SQLITE_OK;
}

#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

/**
 * Reads the leases from the lock control block.  The leases array has
 * room for at least one more lock upon return.
 * Returns the version number of the lock control block in lcbVersion.
 */
static int get_lockcb(
  SQLITE_RCVFS_SESSION *rcs,
  SQLITE_RCVFS_DBID dbid,
  uint64_t tblid,
  SQLITE_RCVFS_HANDLE *handle,
  uint64_t *lcbVersion
){
  Status status;
  uint32_t nbytes = 0;
  int short_read = 0;
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = dbid;
  block_key.blockid = SQLITE_RCVFS_LCBLOCK;
  do {
    nbytes = 0;
    atomic_inc64(&sqlite_rcvfs_nread);
    status =
      rc_read(rcs->client, tblid,
              &block_key, sizeof(block_key),
              NULL, lcbVersion,
              handle->leases, handle->capacityLeases*sizeof(SQLITE_RCVFS_LEASE),
              &nbytes);
    atomic_xadd64(&sqlite_rcvfs_szread, nbytes);
    switch (status) {
      case STATUS_OBJECT_DOESNT_EXIST:
        nbytes = 0;
        // Fall through
      case STATUS_OK:
        handle->nLeases = nbytes / sizeof(SQLITE_RCVFS_LEASE);
        if (handle->nLeases >= handle->capacityLeases) {
          short_read = 1;
          handle->capacityLeases = handle->nLeases + 1;
          handle->leases = (SQLITE_RCVFS_LEASE *)
            sqlite3_realloc(handle->leases,
              handle->capacityLeases * sizeof(SQLITE_RCVFS_LEASE));
        } else {
          short_read = 0;
        }
        break;
      default:
        return SQLITE_IOERR_LOCK;
    }
  } while (short_read);

  DPRINTF("retrieved lock control block of size %d\n", *nLeasesOut);
  handle->versionLeases = *lcbVersion;
  return SQLITE_OK;
}

static void cleanup_lockcb(SQLITE_RCVFS_HANDLE *handle, char max_lease_type) {
  uint32_t i;
  for (i = 0; i < handle->nLeases; ) {
    int owned = is_owned(&(handle->leases[i]), &(handle->token));
    int valid_lease = is_locked(&(handle->leases[i]));
    if (owned && (handle->leases[i].lease_type == PENDING_LOCK))
      valid_lease = 0;
    if (owned && (handle->leases[i].lease_type > max_lease_type))
      valid_lease = 0;

    if (!valid_lease) {
      DPRINTF("removing a lock (mylock: %d)\n",
              is_owned(&(handle->leases[i]), &(handle->token)));
      // Shrink array
      uint32_t j;
      for (j = i+1; j < handle->nLeases; ++j)
        handle->leases[j-1] = handle->leases[j];
      handle->nLeases--;
    } else {
      ++i;
    }
  }
}


/**
 * Locking.
 */
static int rcLock(sqlite3_file *pFile, int eLock){
  RcFile *p = (RcFile *)pFile;
  DPRINTF("lock %d  (mytoken %d) (%p)\n",
          eLock, p->handle.token.digest[0] & 0xff, pFile);
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_LOCK;
  uint64_t tblid = p->handle.tblid;
  SQLITE_RCVFS_LEASE new_lease;
  memset(&new_lease, 0, sizeof(new_lease));
  new_lease.lease_type = eLock;
  new_lease.token = p->handle.token;
  new_lease.deadline = time(NULL) + SQLITE_RCVFS_LEASETIME;

  int result;
  uint64_t lcbVersion = 0;
  do {
    result = -1;
    if ((p->handle.versionLeases == 0) || (lcbVersion > 0)) {
      int retval;
      retval = get_lockcb(rcs, p->handle.dbid, tblid, &(p->handle), &lcbVersion);
      if (retval != SQLITE_OK) return retval;
    } else {
      lcbVersion = p->handle.versionLeases;
    }

    struct RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    if (lcbVersion > 0) {
      rrules.givenVersion = lcbVersion;
      rrules.versionNeGiven = 1;
    }

    cleanup_lockcb(&(p->handle), EXCLUSIVE_LOCK);
    int other_shared = 0;
    int other_reserved = 0;
    int other_pending = 0;
    int other_exclusive = 0;
    unsigned i;
    for (i = 0; i < p->handle.nLeases; ++i) {
      DPRINTF("found a lock of type %d, token %d (mytoken %d) (%p)\n",
              p->handle.leases[i].lease_type & 0xff,
              p->handle.leases[i].token.digest[0] & 0xff,
              p->handle.token.digest[0] & 0xff,
              p);
      if (is_owned(&(p->handle.leases[i]), &(p->handle.token))) {
        // Do I have already a lock of the required type or better?
        if (p->handle.leases[i].lease_type >= eLock) return SQLITE_OK;
      } else {
        // Not my locks
        if (p->handle.leases[i].lease_type == SHARED_LOCK) other_shared = 1;
        if (p->handle.leases[i].lease_type == RESERVED_LOCK) other_reserved = 1;
        if (p->handle.leases[i].lease_type == PENDING_LOCK) other_pending = 1;
        if (p->handle.leases[i].lease_type == EXCLUSIVE_LOCK)
          other_exclusive = 1;
      }
    }
    switch (eLock) {
      case SHARED_LOCK:
        if (!other_pending && !other_exclusive) result = SQLITE_OK;
        else result = SQLITE_BUSY;
        break;
      case RESERVED_LOCK:
        if (!other_reserved && !other_pending && !other_exclusive)
          result = SQLITE_OK;
        else
          result = SQLITE_BUSY;
        break;
      case EXCLUSIVE_LOCK:
        if (other_shared || other_reserved || other_pending || other_exclusive)
          result = SQLITE_BUSY;
        else
          result = SQLITE_OK;
        break;
      default:
        result = SQLITE_ERROR;
    }

    int new_lockcb = 0;
    if (result == SQLITE_OK) new_lockcb = 1;
    if ((result == SQLITE_BUSY) && (eLock == EXCLUSIVE_LOCK) &&
        !other_pending && !other_reserved)
    {
      new_lockcb = 1;
      new_lease.lease_type = PENDING_LOCK;
    }
    if (new_lockcb) {
      DPRINTF("writing new lockcb of size %d (%p)\n", p->handle.nLeases+1, p);
      p->handle.leases[p->handle.nLeases] = new_lease;
      p->handle.nLeases++;
      SQLITE_RCVFS_BLOCKKEY block_key;
      block_key.dbid = p->handle.dbid;
      block_key.blockid = SQLITE_RCVFS_LCBLOCK;
      atomic_inc64(&sqlite_rcvfs_nwrite);
      Status status =
        rc_write(rcs->client, tblid,
                 &block_key, sizeof(block_key),
                 p->handle.leases, p->handle.nLeases*sizeof(SQLITE_RCVFS_LEASE),
                 &rrules, &lcbVersion);
      switch (status) {
        case STATUS_OK:
          atomic_xadd64(&sqlite_rcvfs_szwrite,
                        p->handle.nLeases * sizeof(SQLITE_RCVFS_LEASE));
          p->handle.versionLeases = lcbVersion;
          break;
        case STATUS_OBJECT_EXISTS:
        case STATUS_OBJECT_DOESNT_EXIST:
        case STATUS_WRONG_VERSION:
          result = -1;
          break;
        default:
          result = SQLITE_IOERR_LOCK;
      }
    }
  } while (result < 0);

  DPRINTF("lock result %d (%p)\n", result, p);
  return result;
}


static int rcUnlock(sqlite3_file *pFile, int eLock) {
  RcFile *p = (RcFile *)pFile;
  DPRINTF("unlock to new level %d (mytoken %d) (%p)\n",
          eLock, p->handle.token.digest[0] & 0xff, pFile);
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_LOCK;
  uint64_t tblid = p->handle.tblid;

  int result;
  uint64_t lcbVersion = 0;
  do {
    if (lcbVersion > 0) {
      int retval;
      retval = get_lockcb(rcs, p->handle.dbid, tblid, &(p->handle), &lcbVersion);
      if (retval != SQLITE_OK) return retval;
      DPRINTF("retrieved %d leases (%p)\n", p->handle.nLeases, p);
      if (p->handle.nLeases == 0) return SQLITE_OK;
    } else {
      lcbVersion = p->handle.versionLeases;
    }

    struct RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.givenVersion = lcbVersion;
    rrules.versionNeGiven = 1;

    cleanup_lockcb(&(p->handle), NO_LOCK);
    if (eLock == SHARED_LOCK) {
      SQLITE_RCVFS_LEASE new_lease;
      memset(&new_lease, 0, sizeof(new_lease));
      new_lease.lease_type = SHARED_LOCK;
      new_lease.token = p->handle.token;
      new_lease.deadline = time(NULL) + SQLITE_RCVFS_LEASETIME;
      p->handle.leases[p->handle.nLeases] = new_lease;
      p->handle.nLeases++;
    }
    DPRINTF("cleanup+mod: now %d leases (%p)\n", p->handle.nLeases, p);

    SQLITE_RCVFS_BLOCKKEY block_key;
    block_key.dbid = p->handle.dbid;
    block_key.blockid = SQLITE_RCVFS_LCBLOCK;
    Status status;
    uint32_t nbytes = p->handle.nLeases * sizeof(SQLITE_RCVFS_LEASE);
    if (nbytes == 0) nbytes = 1;
    atomic_inc64(&sqlite_rcvfs_nwrite);
    status = rc_write(rcs->client, tblid,
                      &block_key, sizeof(block_key),
                      p->handle.leases, nbytes,
                      &rrules, &lcbVersion);
    switch (status) {
      case STATUS_OK:
        atomic_xadd64(&sqlite_rcvfs_szwrite, nbytes);
        p->handle.versionLeases = lcbVersion;
        result = SQLITE_OK;
        break;
      case STATUS_WRONG_VERSION:
      case STATUS_OBJECT_DOESNT_EXIST:
        result = -1;
        break;
      default:
        result = SQLITE_IOERR_LOCK;
    }
  } while (result < 0);

  DPRINTF("unlock result %d (%p)\n", result, p);
  return result;
}


static int rcCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  //printf("check reserve lock (%p)\n", pFile);
  RcFile *p = (RcFile *)pFile;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(p->handle.conn);
  if (!rcs) return SQLITE_IOERR_LOCK;

  uint64_t tblid = p->handle.tblid;
  uint64_t lcbVersion = 0;
  int retval;
  retval = get_lockcb(rcs, p->handle.dbid, tblid, &(p->handle), &lcbVersion);
  if (retval != SQLITE_OK) return retval;

  *pResOut = 0;
  unsigned i;
  for (i = 0; i < p->handle.nLeases; ++i) {
    if (is_locked(&(p->handle.leases[i])) &&
        (p->handle.leases[i].lease_type == RESERVED_LOCK))
    {
      *pResOut = 1;
      break;
    }
  }
  return SQLITE_OK;
}


/**
 * No xFileControl() verbs are implemented by this VFS.
 */
static int rcFileControl(sqlite3_file *pFile, int op, void *pArg){
  return SQLITE_OK;
}

/**
 * The xSectorSize() and xDeviceCharacteristics() methods. These two
 * may return special values allowing SQLite to optimize file-system
 * access to some extent. But it is also safe to simply return 0.
 */
static int rcSectorSize(sqlite3_file *pFile) {
  return 1024;
}

static int rcDeviceCharacteristics(sqlite3_file *pFile) {
  return
    SQLITE_IOCAP_ATOMIC1K |
    SQLITE_IOCAP_SAFE_APPEND |
    SQLITE_IOCAP_SEQUENTIAL |
    SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
}

/**
 * Delete the file identified by argument zPath. If the dirSync parameter
 * is non-zero, then ensure the file-system modification to delete the
 * file has been synced to disk before returning.  For RAMCloud, there is
 * no extra syncronization required.
 */
static int rcDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync) {
  DPRINTF("delete %s\n", zPath);

  SQLITE_RCVFS_CONNECTION *conn = (SQLITE_RCVFS_CONNECTION *)pVfs->pAppData;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(conn);
  if (!rcs) return SQLITE_IOERR_DELETE;

  Status status;
  SQLITE_RCVFS_DBHEADER dbheader;
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = mk_dbid(zPath);
  block_key.blockid = SQLITE_RCVFS_HEADERBLOCK;
  uint32_t nbytes = 0;
  atomic_inc64(&sqlite_rcvfs_nread);
  status = rc_read(rcs->client, rcs->conn->tblid, &block_key, sizeof(block_key),
                   NULL, NULL, &dbheader, sizeof(dbheader), &nbytes);
  atomic_xadd64(&sqlite_rcvfs_szread, nbytes);
  if (status == STATUS_OBJECT_DOESNT_EXIST) return SQLITE_OK;
  else if (status != STATUS_OK) return SQLITE_IOERR_DELETE;

  int retval =
    rcDeleteInternal(rcs, block_key.dbid, dbheader.blocksz, dbheader.size);
  DPRINTF("delete returns %d\n", retval);
  return retval;
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

/**
 * Query the file-system to see if the named file exists, is readable or
 * is both readable and writable.
 */
static int rcAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
  DPRINTF("access %s\n", zPath);

  SQLITE_RCVFS_CONNECTION *conn = (SQLITE_RCVFS_CONNECTION *)pVfs->pAppData;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(conn);
  if (!rcs) return SQLITE_IOERR;

  Status status;
  SQLITE_RCVFS_DBHEADER dbheader;
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = mk_dbid(zPath);
  block_key.blockid = SQLITE_RCVFS_HEADERBLOCK;
  uint32_t nbytes = 0;
  atomic_inc64(&sqlite_rcvfs_nread);
  status = rc_read(rcs->client, rcs->conn->tblid, &block_key, sizeof(block_key),
                   NULL, NULL, &dbheader, sizeof(dbheader), &nbytes);
  atomic_xadd64(&sqlite_rcvfs_szread, nbytes);
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

/**
 * The following four VFS methods:
 *
 *   xDlOpen
 *   xDlError
 *   xDlSym
 *   xDlClose
 *
 * are supposed to implement the functionality needed by SQLite to load
 * extensions compiled as shared objects. Taken from unixVfs
 */
static void *rcDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return dlopen(zPath, RTLD_NOW | RTLD_GLOBAL);
}

static void rcDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "rcDlError not supported");
  zErrMsg[nByte-1] = '\0';
}

static void (*rcDlSym(sqlite3_vfs *pVfs, void *pH, const char *zSym))(void){
  return dlsym(pH, zSym);
}

static void rcDlClose(sqlite3_vfs *pVfs, void *pHandle){
  dlclose(pHandle);
}

/**
 * Parameter zByte points to a buffer nByte bytes in size. Populate this
 * buffer with pseudo-random data.
 * Taken from default implementation.
 */
static int rcRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte) {
  assert((size_t)nByte >= (sizeof(time_t) + sizeof(int)));
  memset(zByte, 0, nByte);
  pid_t randomnessPid = getpid();
  int fd, got;
  fd = open("/dev/urandom", O_RDONLY);
  if(fd < 0) {
    time_t t;
    time(&t);
    memcpy(zByte, &t, sizeof(t));
    memcpy(&zByte[sizeof(t)], &randomnessPid, sizeof(randomnessPid));
    assert(sizeof(t) + sizeof(randomnessPid) <= (size_t)nByte);
    nByte = sizeof(t) + sizeof(randomnessPid);
  } else {
    do {
      got = read(fd, zByte, nByte);
    } while (got<0 && errno==EINTR);
    close(fd);
  }
  return nByte;
}

/*
** Sleep for at least nMicro microseconds. Return the (approximate) number
** of microseconds slept for.
*/
static int rcSleep(sqlite3_vfs *pVfs, int nMicro){
  struct timeval wait_for;
  wait_for.tv_sec = nMicro / 1000000;
  wait_for.tv_usec = nMicro % 1000000;
  select(0, NULL, NULL, NULL, &wait_for);
  return nMicro;
}

/**
 * Set *pTime to the current UTC time expressed as a Julian day. Return
 * SQLITE_OK if successful, or an error code otherwise.
 *
 *   http://en.wikipedia.org/wiki/Julian_day
 *
 * This implementation is not very good. The current time is rounded to
 * an integer number of seconds. Also, assuming time_t is a signed 32-bit
 * value, it will stop working some time in the year 2038 AD (the so-called
 * "year 2038" problem that afflicts systems that store time this way).
 */
static int rcCurrentTime(sqlite3_vfs *pVfs, double *pTime){
  time_t t = time(0);
  *pTime = t/86400.0 + 2440587.5;
  return SQLITE_OK;
}


/**
 * Open a file handle.
 */
static int rcOpen(
  sqlite3_vfs *pVfs,              // VFS
  const char *zName,              // File to open, or 0 for a temp file
  sqlite3_file *pFile,            // Pointer to RcFile struct to populate
  int flags,                      // Input SQLITE_OPEN_XXX flags
  int *pOutFlags                  // Output SQLITE_OPEN_XXX flags (or NULL)
){
  static const sqlite3_io_methods rcio = {
    1,                          // iVersion
    rcClose,                    // xClose
    rcRead,                     // xRead
    rcWrite,                    // xWrite
    rcTruncate,                 // xTruncate
    rcSync,                     // xSync
    rcFileSize,                 // xFileSize
    rcLock,                     // xLock
    rcUnlock,                   // xUnlock
    rcCheckReservedLock,        // xCheckReservedLock
    rcFileControl,              // xFileControl
    rcSectorSize,               // xSectorSize
    rcDeviceCharacteristics     // xDeviceCharacteristics
  };

  DPRINTF("open %s\n", zName);

  SQLITE_RCVFS_CONNECTION *conn = (SQLITE_RCVFS_CONNECTION *)pVfs->pAppData;
  SQLITE_RCVFS_SESSION *rcs = get_rc_session(conn);
  if (!rcs) return SQLITE_IOERR;

  RcFile *p = (RcFile*)pFile;  // Populate this structure

  // For temporary files (zName == 0) random name is used
  SQLITE_RCVFS_DBID dbid = mk_dbid(zName);
  SQLITE_RCVFS_BLOCKKEY block_key;
  block_key.dbid = dbid;
  block_key.blockid = SQLITE_RCVFS_HEADERBLOCK;
  SQLITE_RCVFS_DBHEADER dbheader;
  memset(&dbheader, 0, sizeof(dbheader));

  Status status;
  int new_db = 0;
  uint64_t tblid = conn->tblid;
  if (flags & SQLITE_OPEN_CREATE) {
    DPRINTF("creating db\n");
    dbheader.version = 1;
    dbheader.blocksz = SQLITE_RCVFS_BLOCKSZ;

    struct RejectRules rrules;
    memset(&rrules, 0, sizeof(rrules));
    rrules.exists = 1;
    atomic_inc64(&sqlite_rcvfs_nwrite);
    status = rc_write(rcs->client, tblid,
                      &block_key, sizeof(block_key),
                      &dbheader, sizeof(dbheader), &rrules, NULL);
    switch (status) {
      case STATUS_OBJECT_EXISTS:
        if (flags & SQLITE_OPEN_EXCLUSIVE) return SQLITE_CANTOPEN;
        break;
      case STATUS_OK:
        atomic_xadd64(&sqlite_rcvfs_szwrite, sizeof(dbheader));
        new_db = 1;
        break;
      default:
        return SQLITE_CANTOPEN;
    }
  }

  if (!new_db) {
    DPRINTF("reading header\n");
    uint32_t nbytes = 0;
    atomic_inc64(&sqlite_rcvfs_nread);
    status = rc_read(rcs->client, tblid,
                     &block_key, sizeof(block_key),
                     NULL, NULL, &dbheader, sizeof(dbheader), &nbytes);
    atomic_xadd64(&sqlite_rcvfs_szread, nbytes);
    if (status != STATUS_OK) return SQLITE_CANTOPEN;
  }

  memset(p, 0, sizeof(RcFile));
  p->handle.token = mk_token();
  p->handle.conn = conn;
  p->handle.dbid = dbid;
  p->handle.tblid = tblid;
  p->handle.size = dbheader.size;
  p->handle.blocksz = dbheader.blocksz;
  p->handle.versionLeases = 0;
  p->handle.nLeases = 0;
  p->handle.capacityLeases = SQLITE_RCVFS_STACKLEASES;
  p->handle.leases = (SQLITE_RCVFS_LEASE *)
    sqlite3_malloc(p->handle.capacityLeases * sizeof(SQLITE_RCVFS_LEASE));
  p->flags = flags;
  p->permanentSize = p->handle.size;
  p->blockBuffer = (SQLITE_RCVFS_WBUFFER *)
    sqlite3_malloc(sizeof(SQLITE_RCVFS_WBUFFER));
  unsigned i;
  for (i = 0; i < SQLITE_RCVFS_WBUF_NBLOCKS; ++i)
    clearBufferBlock(p->blockBuffer, i);
  if (pOutFlags) *pOutFlags = flags;
  p->base.pMethods = &rcio;
  DPRINTF("all went fine\n");
  return SQLITE_OK;
}


/**
 * This function returns a pointer to the VFS implemented in this file.
 * To make the VFS available to SQLite:
 *
 *   RAMCLOUD_CONNECTON *conn =
 *     sqlite3_rcvfs_connect("zk:localhost:2181", "main");
 *   sqlite3_vfs_register("ramcloud", sqlite3_rcvfs(conn), 0);
 */
sqlite3_vfs *sqlite3_rcvfs(
  const char *vfs_name,
  SQLITE_RCVFS_CONNECTION *conn
){
  static sqlite3_vfs rcvfs = {
    1,                            // iVersion
    sizeof(RcFile),               // szOsFile
    MAXPATHNAME,                  // mxPathname
    0,                            // pNext
    NULL,                         // zName
    NULL,                         // pAppData
    rcOpen,                       // xOpen
    rcDelete,                     // xDelete
    rcAccess,                     // xAccess
    rcFullPathname,               // xFullPathname
    rcDlOpen,                     // xDlOpen
    rcDlError,                    // xDlError
    rcDlSym,                      // xDlSym
    rcDlClose,                    // xDlClose
    rcRandomness,                 // xRandomness
    rcSleep,                      // xSleep
    rcCurrentTime,                // xCurrentTime
  };
  rcvfs.zName = vfs_name;
  rcvfs.pAppData = conn;
  return &rcvfs;
}

#ifdef __cplusplus
}
#endif

#endif /* !defined(SQLITE_TEST) || SQLITE_OS_UNIX */
