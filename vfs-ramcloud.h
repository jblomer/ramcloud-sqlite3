#ifndef VFS_RAMCLOUD_H_
#define VFS_RAMCLOUD_H_

#include <inttypes.h>
#include "sqlite3.h"
#include "vfs-ramcloud-stats.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void SQLITE_RCVFS_CONNECTION;
SQLITE_RCVFS_CONNECTION *sqlite3_rcvfs_connect(const char *locator,
                                               const char *cluster_name,
                                               const char *table_name);
void sqlite3_rcvfs_disconnect(SQLITE_RCVFS_CONNECTION *conn);
void sqlite3_rcvfs_get_stats(SQLITE_RCVFS_STATS *stats);
int sqlite3_rcvfs_upload(SQLITE_RCVFS_CONNECTION *conn, const char *path);
int sqlite3_rcvfs_download(SQLITE_RCVFS_CONNECTION *conn, const char *path);
int sqlite3_rcvfs_delete(SQLITE_RCVFS_CONNECTION *conn, const char *path);

sqlite3_vfs *sqlite3_rcvfs(const char *vfs_name, SQLITE_RCVFS_CONNECTION *conn);

// TODO
void sqlite3_rcvfs_refresh_leases(SQLITE_RCVFS_CONNECTION *conn);

#ifdef __cplusplus
}
#endif

#endif
