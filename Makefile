all: libsqlite3.a libvfs-ramcloud.a test test2

test2: test2.c vfs-ramcloud.h libvfs-ramcloud.a libsqlite3.a
	gcc -pthread -D_REENTRANT -std=c99 -O2 -g -Wall -o test2 test2.c libsqlite3.a libvfs-ramcloud.a -ldl -lramcloud

test: test.c vfs-ramcloud.h libvfs-ramcloud.a libsqlite3.a
	gcc -pthread -std=c99 -O2 -g -Wall -o test test.c libsqlite3.a libvfs-ramcloud.a -ldl -lramcloud

libvfs-ramcloud.a: vfs-ramcloud.c vfs-ramcloud.h md5.c md5.h
	gcc -D_REENTRANT -I. -fPIC -g -O2 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -Wall -MT vfs-ramcloud.o -MD -MP -c -o vfs-ramcloud.o vfs-ramcloud.c
	gcc -D_REENTRANT -I. -fPIC -g -O2 -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -Wall -MT md5.o -MD -MP -c -o md5.o md5.c
	ar cru libvfs-ramcloud.a vfs-ramcloud.o md5.o
	ranlib libvfs-ramcloud.a

libsqlite3.a: sqlite3.c sqlite3.h
	gcc -DHAVE_CONFIG_H -I. -I../.. -w -fPIC -DSQLITE_DEBUG=1 -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_MEMORY_MANAGEMENT -DSQLITE_ENABLE_MEMSYS5 -g -O2 -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -Wall -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES -D_BUILT_IN_LIBCURL -D_BUILT_IN_SQLITE3 -D_BUILTIN_IN_ZLIB -MT sqlite3.o -MD -MP -c -o sqlite3.o sqlite3.c
	ar cru libsqlite3.a sqlite3.o
	ranlib libsqlite3.a

clean:
	rm -f *.o *.d
	rm -f libsqlite3.a
	rm -f libvfs-ramcloud.a
	rm -f test test2
