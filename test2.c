#include "sqlite3.h"
#include "vfs-ramcloud.h"

#include <inttypes.h>
#include <assert.h>
#include <stdio.h>

#define ROWS 4
#define THREADS 2


char *dbname;

static void *mainThread(void *data) {
  uint64_t threadno = (uint64_t)(data);
  int retval;
  sqlite3 *db;
  retval = sqlite3_open_v2(dbname, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  assert(retval == 0);

  unsigned i;
  for (i = 0; i < ROWS; ++i) {
    do {
      retval = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    } while (retval == SQLITE_BUSY);
    printf("[%lu] begin transaction (%u)\n", threadno, i);
    //printf("retval is %d (error %s)\n", retval, sqlite3_errmsg(db));
    assert(retval == 0);
    sqlite3_stmt *sql_insert;
    do {
      retval = sqlite3_prepare(db, "INSERT INTO test (value) VALUES (:v)", -1, &sql_insert, NULL);
    } while (retval == SQLITE_BUSY);
    assert(retval == 0);
    printf("[%lu] prepare stmt (%u)\n", threadno, i);
    retval = sqlite3_bind_int64(sql_insert, 1, i);
    assert(retval == 0);
    do {
      retval = sqlite3_step(sql_insert);
    } while (retval == SQLITE_BUSY);
    assert(retval == SQLITE_DONE);
    printf("[%lu] step (%u)\n", threadno, i);
    retval = sqlite3_reset(sql_insert);
    assert(retval == 0);
    do {
      retval = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    } while (retval == SQLITE_BUSY);
    printf("[%lu] COMMIT retval is %d (error %s) (i is %d)\n", threadno, retval, sqlite3_errmsg(db), i);
    assert(retval == 0);
  }

  retval = sqlite3_close_v2(db);
  assert(retval == 0);

  return NULL;
}


int main(int argc, char **argv) {
  int retval;

  dbname = "./my-db";
  if (argc > 1)
    dbname = argv[1];

  //sqlite3_vfs *vfs = sqlite3_vfs_find("unix");
  //assert(vfs != NULL);
  //retval = sqlite3_vfs_register(vfs, 1);
  SQLITE_RCVFS_CONNECTION *conn =
    sqlite3_rcvfs_connect("zk:localhost:2181", "main");
  assert(conn != NULL);
  retval = sqlite3_vfs_register(sqlite3_rcvfs("ramcloud", conn), 1);
  assert(retval == 0);

  sqlite3 *db;
  retval = sqlite3_open_v2(dbname, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  assert(retval == 0);
  printf("file opened\n");

  do {
    retval = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS test (value INTEGER);", NULL, NULL, NULL);
  } while (retval == SQLITE_BUSY);
  printf("retval is %d\n", retval);
  assert(retval == 0);

  pthread_t threads[THREADS];
  uint64_t i;
  for (i = 0; i < THREADS; ++i) {
    retval = pthread_create(&threads[i], NULL, mainThread, (void *)i);
    assert(retval == 0);
  }
  for (i = 0; i < THREADS; ++i) {
    retval = pthread_join(threads[i], NULL);
    assert(retval == 0);
  }

  retval = sqlite3_close_v2(db);
  assert(retval == 0);

  retval = sqlite3_open_v2(dbname, &db, SQLITE_OPEN_READONLY, NULL);
  assert(retval == 0);

  sqlite3_stmt *sql_sum;
  retval = sqlite3_prepare(db, "SELECT SUM(value) FROM test;", -1, &sql_sum, NULL);
  assert(retval == 0);
  retval = sqlite3_step(sql_sum);
  assert(retval = SQLITE_ROW);
  uint64_t sum = sqlite3_column_int64(sql_sum, 0);
  retval = sqlite3_reset(sql_sum);
  assert(retval == 0);

  retval = sqlite3_close_v2(db);
  assert(retval == 0);

  sqlite3_rcvfs_disconnect(conn);

  printf("sum is %lu\n", sum);
  return 0;
}

