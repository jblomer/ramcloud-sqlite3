#include "sqlite3.h"
#include "vfs-ramcloud.h"

#include <inttypes.h>
#include <assert.h>
#include <stdio.h>

int main(int argc, char **argv) {
  int retval;

  if (argc < 4) {
    printf("usage: %s <file> <ramcloud locator> <db universe>\n", argv[0]);
    return 1;
  }

  char *dbfile = argv[1];
  char *locator = argv[2];
  char *universe = argv[3];

  SQLITE_RCVFS_CONNECTION *conn =
    sqlite3_rcvfs_connect(locator, "main", universe);
  assert(conn != NULL);

  retval = sqlite3_rcvfs_download(conn, dbfile);
  sqlite3_rcvfs_disconnect(conn);
  printf("download result %d\n", retval);

  return retval;
}
