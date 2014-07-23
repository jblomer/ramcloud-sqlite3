# Sqlite3 VFS driver on RAMCloud (_rcvfs_)
This provides a [VFS driver for sqlite3](https://www.sqlite.org/vfs.html) using RAMcloud as a backing store for databases.

## Open Issues
Locks are translated in leases with a deadline.  Currently, leases expire after 2 minutes, which allows to recover from deadlocks caused by crashed clients.  Automatic (or manual) refreshing of open leases remains to be done.

Currently the driver assumes that RAMCloud supports ordered batch operations.  This remains to be implemented in RAMCloud.

## Mapping to RAMCloud

The mapping implements a block storage (1kB blocks) as RAMCloud objects.

  * File (database) is split in a number of blocks
  * Writes are buffered by rcvfs
  * _All blocks_ of _all files_ are in the same table (the _sqlite db universe_)
  * Important I/O capabilities: `SQLITE_IOCAP_ATOMIC1K`, `SQLITE_IOCAP_SAFE_APPEND`, `SQLITE_IOCAP_SEQUENTIAL`
  
## RAMCloud Keys
  * (Full) Paths are mapped into an ID by their MD5 hash (128bit integer)
  * Block key: `ID || 64bit block number`
  
## Database Header Block
  * Block number `uint64_t(-1)`
  * Contains file size and block size (currently constant 1kB)
  * Cached (`get_file_size` is a noop)
  
## Lock Control Block
  * Block number `uint64_t(-2)`
  * Array of leases
  * Lease has a
    * type (shared, reserved, exclusive)
    * token (random bits), needed for ownership
    * deadline for safetiness in presence of client faults
  * Lock Control Block is cached, so under low concurrency it only needs to be written
  
## rcvfs interface

    typedef void SQLITE_RCVFS_CONNECTION;
    SQLITE_RCVFS_CONNECTION *sqlite3_rcvfs_connect(const char *locator,
                                                   const char *cluster_name,
                                                   const char *table_name);
    void sqlite3_rcvfs_disconnect(SQLITE_RCVFS_CONNECTION *conn);
    
    // Essential for sqlite3_register
    sqlite3_vfs *sqlite3_rcvfs(const char *vfs_name, SQLITE_RCVFS_CONNECTION *conn);
    
    // Helpers
    void sqlite3_rcvfs_get_stats(SQLITE_RCVFS_STATS *stats);
    int sqlite3_rcvfs_upload(SQLITE_RCVFS_CONNECTION *conn, const char *path);
    int sqlite3_rcvfs_download(SQLITE_RCVFS_CONNECTION *conn, const char *path);
    int sqlite3_rcvfs_delete(SQLITE_RCVFS_CONNECTION *conn, const char *path);

Example use:

    SQLITE_RCVFS_CONNECTION *conn =
        sqlite3_rcvfs_connect("zk:headnode.example.com:2181", "main", "sqlite3");
      retval = sqlite3_vfs_register(sqlite3_rcvfs("ramcloud", conn), 1);
      ... Normal use of sqlite ...
      sqlite3_rcvfs_disconnect(conn);

  * Thread-safe, RAMCloud object is in TLS
  
## SQL Operations to RPCs

### Read
    shared lock             1 write
    check for hot journal   1 read
    read header             1 read
    <block reads>
    drop lock               1 write
                            ---------------
                            2 write, 2 read

### Write
    shared lock             1 write		
    check for hot journal   1 read
    read header             1 read
    reserved lock           1 write
    Open/create journal 	1 write
    Write to journal write  buffered
    exclusive lock		    1 write
    Write main database     buffered
    flush buffer			1 multi-write					
    delete journal          1 multi-remove				
    shared lock             1 write
    drop lock               1 write
    				        ----------------------------------------------
    				        2 read, 6 write, 1 multi-write, 1 multi-remove

# Benchmarks

## LevelDB Micro Benchmarks

### RAM disk
    fillseq      :       14.245 micros/op;    7.8 MB/s     
    fillseqsync  :       15.680 micros/op;    7.1 MB/s (10000 ops)
    fillseqbatch :        4.902 micros/op;   22.6 MB/s     
    fillrandom   :       18.484 micros/op;    6.0 MB/s     
    fillrandsync :       17.490 micros/op;    6.3 MB/s (10000 ops)
    fillrandbatch :      13.224 micros/op;    8.4 MB/s    
    overwrite    :       21.960 micros/op;    5.0 MB/s     
    overwritebatch :     18.948 micros/op;    5.8 MB/s   
    readrandom   :        7.331 micros/op;                 
    readseq      :        2.590 micros/op;   36.8 MB/s    
    fillrand100K :      238.141 micros/op;  400.5 MB/s (1000 ops)
    fillseq100K  :      153.330 micros/op;  622.1 MB/s (1000 ops)
    readseq100K  :       81.320 micros/op; 1172.7 MB/s  
    readrand100K :       85.977 micros/op;          

### Local hard disk
    fillseq      :       18.565 micros/op;    6.0 MB/s     
    fillseqsync  :    10421.571 micros/op;    0.0 MB/s 
    fillseqbatch :        8.913 micros/op;   12.4 MB/s     
    fillrandom   :       26.957 micros/op;    4.1 MB/s     
    fillrandsync :     8512.323 micros/op;    0.0 MB/s 
    fillrandbatch :      19.263 micros/op;    5.7 MB/s    
    overwrite    :       38.198 micros/op;    2.9 MB/s     
    overwritebatch :     29.409 micros/op;    3.8 MB/s   
    readrandom   :       10.764 micros/op;                 
    readseq      :        3.276 micros/op;   29.1 MB/s    
    fillrand100K :      512.701 micros/op;  186.0 MB/s 
    fillseq100K  :      489.528 micros/op;  194.8 MB/s 
    readseq100K  :       85.419 micros/op; 1116.5 MB/s  
    readrand100K :       91.924 micros/op;

### rcvfs
    fillseq      :      144.845 micros/op;    0.8 MB/s    
    fillseqsync  :      142.400 micros/op;    0.8 MB/s 
    fillseqbatch :        5.293 micros/op;   20.9 MB/s    
    fillrandom   :      150.835 micros/op;    0.7 MB/s    
    fillrandsync :      140.658 micros/op;    0.8 MB/s 
    fillrandbatch :      17.571 micros/op;    6.3 MB/s   
    overwrite    :      165.058 micros/op;    0.7 MB/s    
    overwritebatch :     40.365 micros/op;    2.7 MB/s  
    readrandom   :       58.374 micros/op;                
    readseq      :        9.567 micros/op;   10.0 MB/s   
    fillrand100K :      616.751 micros/op;  154.7 MB/s 
    fillseq100K  :      384.462 micros/op;  248.1 MB/s 
    readseq100K  :      800.731 micros/op;  119.1 MB/s 
    readrand100K :      651.948 micros/op;             


## TPC-C

### RAM disk
                             Response Time (s)
     Transaction      %    Average :    90th %        Total        Rollbacks      %
    ------------  -----  ---------------------  -----------  ---------------  -----
        Delivery   4.09      0.010 :     0.012         5187                0   0.00
       New Order  45.27      0.009 :     0.011        57403              593   1.03
    Order Status   3.94      0.008 :     0.010         4990                0   0.00
         Payment  42.70      0.008 :     0.010        54145                0   0.00
     Stock Level   4.00      0.009 :     0.011         5069                0   0.00
    ------------  -----  ---------------------  -----------  ---------------  -----
    
    31310.73 new-order transactions per minute (NOTPM)

### Local disk (rc18)
                             Response Time (s)
     Transaction      %    Average :    90th %        Total        Rollbacks      %
    ------------  -----  ---------------------  -----------  ---------------  -----
        Delivery   3.89      0.302 :     0.367          151                0   0.00
       New Order  44.54      0.287 :     0.350         1731               18   1.04
    Order Status   4.55      0.257 :     0.321          177                0   0.00
         Payment  42.61      0.280 :     0.341         1656                0   0.00
     Stock Level   4.14      0.251 :     0.311          161                0   0.00
    ------------  -----  ---------------------  -----------  ---------------  -----
    
    944.18 new-order transactions per minute (NOTPM)

### Local disk, no fsync
                             Response Time (s)
     Transaction      %    Average :    90th %        Total        Rollbacks      %
    ------------  -----  ---------------------  -----------  ---------------  -----
        Delivery   4.05      0.011 :     0.013         4569                0   0.00
       New Order  45.30      0.010 :     0.012        51095              533   1.04
    Order Status   3.96      0.009 :     0.011         4467                0   0.00
         Payment  42.68      0.009 :     0.011        48141                0   0.00
     Stock Level   4.00      0.009 :     0.012         4506                0   0.00
    ------------  -----  ---------------------  -----------  ---------------  -----
    
    27870.00 new-order transactions per minute (NOTPM)

### rcvfs
                             Response Time (s)
     Transaction      %    Average :    90th %        Total        Rollbacks      %
    ------------  -----  ---------------------  -----------  ---------------  -----
        Delivery   3.94      0.015 :     0.018         3316                0   0.00
       New Order  45.37      0.013 :     0.017        38176              371   0.97
    Order Status   3.96      0.012 :     0.016         3334                0   0.00
         Payment  42.76      0.012 :     0.016        35976                0   0.00
     Stock Level   3.96      0.015 :     0.018         3332                0   0.00
    ------------  -----  ---------------------  -----------  ---------------  -----
    
    20823.27 new-order transactions per minute (NOTPM)

