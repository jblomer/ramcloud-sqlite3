#ifndef VFS_RAMCLOUD_STATS_H_
#define VFS_RAMCLOUD_STATS_H_

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sqlite3_rcvfs_stats SQLITE_RCVFS_STATS;
struct sqlite3_rcvfs_stats {
  uint64_t nread;
  uint64_t nwrite;
  uint64_t nmwrite;
  uint64_t nremove;
  uint64_t nmremove;
  uint64_t szread;
  uint64_t szwrite;
};

#ifdef __cplusplus
}
#endif

#endif
