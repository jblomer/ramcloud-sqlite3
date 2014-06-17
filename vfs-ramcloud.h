#ifndef VFS_RAMCLOUD_H_
#define VFS_RAMCLOUD_H_

#include "sqlite3.h"

int sqlite3_rcvfs_startup(const char *locator);
void sqlite3_rcvfs_shutdown();

sqlite3_vfs *sqlite3_rcvfs(void);

#endif
