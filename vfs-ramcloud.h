#ifndef VFS_RAMCLOUD_H_
#define VFS_RAMCLOUD_H_

#include <pthread.h>
#include "sqlite3.h"

typedef void SQLITE_RCVFS_CONNECTION;
SQLITE_RCVFS_CONNECTION *sqlite3_rcvfs_connect(const char *locator,
                                               const char *cluster_name);
void sqlite3_rcvfs_disconnect(SQLITE_RCVFS_CONNECTION *conn);
char *sqlite3_rcvfs_table_name(const char *path);

sqlite3_vfs *sqlite3_rcvfs(const char *vfs_name, SQLITE_RCVFS_CONNECTION *conn);

#endif
