

# Metric Introduction

PolarDB-NodeAgent stores the collected information in database tables, including information of wait events, IO calls and latency distribution, resource usage of each backend processes, etc. Each type of information is stored in one table, the columns of which include Dimension and Metric.


## dbmetrics

***dbmetrics*** displays the resource usage of the database. The metrics are as follows:

| Metric | Period | Description |
| --- | --- | --- |
| **Connection Status** |  |  |
| active_connections | 1s | Number of connections in active status. |
| waiting_connections | 1s | Number of connections in waiting status. |
| idle_connections | 1s | Number of connections in idle status. |
| **Wait Event** |  |  |
| client_waits | 1s | Number of processes that are waiting for some activity from user applications. |
| lwlock_waits | 1s | Number of processes that are waiting for a lightweight lock. |
| io_waits | 1s | Number of processes that are waiting for a IO to complete. |
| lock_waits | 1s | Number of processes that are waiting for a heavyweight lock. |
| extension_waits | 1s | Number of processes that are waiting for activity in an extension module. |
| ipc_waits | 1s | Number of processes that are waiting for some activity from another process. |
| timeout_waits | 1s | Number of processes that are waiting for a timeout to expire. |
| bufferpin_waits | 1s | Number of processes that are waiting to access to a data buffer. |
| cpu_waits | 1s | Number of processes that are using CPU. |
| activity_waits | 1s | Number of idle processes that are waiting for activity. |
| **Number of Transactions** |  |  |
| commits_delta | 1s | Number of transactions that have been committed. |
| rollbacks_delta | 1s | Number of transactions that have been rolled back. |
| **Transaction Status** |  |  |
| active_transactions | 1s | Number of transactions in active status. |
| idle_transactions | 1s | Number of transactions in idle status. |
| waiting_transactions | 1s | Number of transactions in waiting status. |
| one_second_transactions | 1s | Number of transactions that have run for longer than 1 second. |
| three_second_transactions | 1s | Number of transactions that have run for longer than 3 seconds. |
| five_seconds_long_transactions | 1s | Number of long-running transactions that have run for longer than 5 seconds. |
| one_second_idle_transactions | 1s | Number of transactions that have been idle for longer than 1 second. |
| three_seconds_idle_transactions | 1s | Number of transactions that have been idle for longer than 3 seconds. |
| five_seconds_idle_transactions | 1s | Number of transactions that have been idle for longer than 5 seconds. |
| two_pc_transactions | 1s | Number of two phase state transactions. |
| one_second_two_pc_transactions | 1s | Number of two phase state transactions which have run for longer than 1 second. |
| three_seconds_two_pc_transactions | 1s | Number of two phase state transactions which have run for longer than 3 seconds. |
| five_seconds_two_pc_transactions | 1s | Number of two phase state transactions which have run for longer than 5 seconds. |
| swell_time | 1s | The longest time for which the transaction runs currently. Unit: Second. |
| **SQL** |  |  |
| one_second_executing_sqls | 1s | Number of slow SQL queries that have been executed for longer than 1 second. |
| three_seconds_executing_sqls | 1s | Number of slow SQL queries that have been executed for longer than 3 seconds. |
| fibe_seconds_executing_sqls | 1s | Number of slow SQL queries that have been executed for longer than 5 seconds. |
| long_executing_sqls | 1s | Number of slow SQL queries that have been executed for longer than 7200 seconds. |
| deadlocks_delta | 1s | Number of deadlocks detected. |
| conflicts_delta | 1s | Number of queries canceled due to conflicts with recovery. |
| **Rows Processed** |  |  |
| tup_returned_delta | 1s | Number of rows returned by queries. |
| tup_fetched_delta | 1s | Number of rows fetched by queries. |
| tup_inserted_delta | 1s | Number of rows inserted by queries. |
| tup_updated_delta | 1s | Number of rows updated by queries. |
| tup_deleted_delta | 1s | Number of rows deleted by queries. |
| **Temporary File** |  |  |
| temp_files_delta | 1s | Number of temporary files created by queries. |
| temp_bytes_delta | 1s | Amount of data written to temporary files by queries. Unit: Byte. |
| **Buffer** |  |  |
| blks_hit_delta | 1s | Number of times disk blocks were found already in the buffer cache. |
| blks_read_delta | 1s | Number of disk blocks read. |
| buffers_backend_delta | 1s | Number of buffers written directly by a backend. |
| buffers_alloc_delta | 1s | Number of buffers allocated. |
| buffers_backend_fsync_delta | 1s | Number of times a backend had to execute its own `fsync` call. |
| buffers_checkpoint_delta | 1s | Number of buffers written during checkpoints. |
| buffers_clean_delta | 1s | Number of buffers written by the background writer. |
| maxwritten_clean_delta | 1s | Number of times the background writer stopped a cleaning scan because of too many buffers written. |
| polar_dirtypage_size | 1s | Number of dirty pages written in the buffer. |
| polar_copybuffer_used_size | 1s | Number of bufferes copied. |
| polar_copybuffer_isfull | 1s | Whether the copied buffer is full. |
| **Checkpoint** |  |  |
| checkpoint_sync_time_delta | 1s | Amount of time spent on checkpoint processing where files are synchronized to disk. Unit: Second. |
| checkpoints_timed_delta | 1s | Number of scheduled checkpoints. |
| checkpoint_write_time_delta | 1s | Amount of time spent on checkpoint processing where files are written to disk. Unit: Second. |
| checkpoints_req_delta | 1s | Number of requested checkpoints. |
| logindex_mem_tbl_size | 1s | Number of tables in LogIndex. |
| **Database Age** |  |  |
| db_age | 1s | The age of the database.  Type: XID. |
| **IO of Block Device** |  |  |
| local_iops_read | 1s | IO of reading locally per second. Unit: IOPS. |
| local_iops_write | 1s | IO of writing locally per second. Unit: IOPS. |
| local_throughput_read | 1s | Throughput of reading per second. Unit: MB/s. |
| local_throughput_write | 1s | Throughput of writing per second. Unit: MB/s. |
| **Replication** |  |  |
| replay_latency_in_mb | 1s | The latency between replay LSN on standby server and write LSN on master.Unit: MB. |
| send_latency_in_mb | 1s | The latency between sending and writing on master.Unit: MB. |
| ap_cp_latency_mb | 1s | The difference between LSN where WAL applied and consistent LSN. Unit: MB. |
| wp_ap_latency_mb | 1s | The difference between the LSN where WAL written and the one where WAL applied. Unit: MB.|
| wp_cp_latency_mb | 1s | The difference between LSN where WAL written and consistent LSN. Unit: MB. |

## polar_stat_aas_history
***polar_stat_aas_history*** displays the collected information of wait events, from the following dimensions:

| Dimension | Description |
| --- | --- |
| wait_event_type | Type of wait event. |
| wait_event | Name of wait event. |
| queryid | ID of SQL query. |

The metrics are as follows:

| Metric | Period | Description |
| --- | --- | --- |
| wait_count | 1s | Number of sessions for certain queryid in certain wait event. |

## polar_stat_io_info

***polar_stat_io_info*** displays the collected information of IO calls (`falloc`, `fsync`, `read`, `write`, `creat`, `seek`, `open`, `close`) of the database, from the following dimensions:

| Dimension | Description |
| --- | --- |
| fileloc | File storage location. <br>local: On local file system.<br/>pfs: PolarDB File System. |
| filetype | File type. |

The metrics are as follows:

| Metric | Period | Description |
| --- | --- | --- |
| falloc_latency_us | 1s | Total time spent on `falloc` call per second. Unit: us. |
| falloc_count | 1s | Times of `falloc` call per second.|
| creat_latency_us | 1s | Total time spent on `creat` call per second. Unit: us. |
| creat_count | 1s | Times of `creat` call per second.|
| read_throughput | 1s | Throughput of `read` call per second. Unit: MB/s.   |
| read_latency_us | 1s | Total time spent on `read` call per second. Unit: us. |
| read_count | 1s | Times of `read` call per second.|
| write_throughput | 1s | Throughput of `write` call per second. Unit: MB/s.|
| write_latency_us | 1s | Total time spent on `write` call per second. Unit: us. |
| write_count | 1s | Times of `write` call per second.|
| fsync_latency_us | 1s | Total time spent on `fsync` call per second. Unit: us.|
| fsync_count | 1s | Times of `fsync` call per second. |
| seek_latency_us | 1s | Total time spent on `seek` call per second. Unit: us.|
| seek_count | 1s | Times of `seek` call per second. |
| open_latency_us | 1s | Total time spent on `open` call per second. Unit: us. |
| open_count | 1s | Times of `open` call per second. |
| close_count | 1s | Times of `close` call per second.|

## polar_stat_io_latency
***polar_stat_io_latency*** displays the latency distribution of IO calls ( `fsync`, `read`, `write`, `seek`, `open`) of the database, from the following dimensions: 

| **Dimension** | **Description** |
| --- | --- |
| latency | Time range of latency. |

The metrics are as follows:

| **Metric** | Period | **Description** |
| --- | --- | --- |
| seek | 1s | Number of `seek` calls the latency of which are in certain time range. |
| fsync | 1s | Number of `fsync` calls the latency of which are in certain time range. |
| read | 1s | Number of `read` calls the latency of which are in certain time range. |
| write | 1s | Number of `write` calls the latency of which are in certain time range. |
| open | 1s | Number of `open` calls the latency of which are in certain time range. |

### polar_stat_process

polar_stat_process displays resource usage of the processes, from the following dimensions: 

| **Dimension** | **Description** |
| --- | --- |
| backend_type | Process Type |

The metrics are as follows:

| Metric | Period | Description |
| --- | --- | --- |
| cpu_user | 1s | CPU usage rate in the user mode. |
| cpu_sys | 1s | CPU usage rate in the kernel mode. |
| rss | 1s | RSS (Resident Set Size). Unit: MB. |
| pfs_read_ps | 1s | Times of reading on PFS. |
| pfs_read_throughput | 1s | Throughput of reading on PFS. Unit: MB/s. |
| pfs_read_latency_ms | 1s | Time delay of reading on PFS. Unit: ms. |
| pfs_write_ps | 1s | Times of writing on PFS. |
| pfs_write_throughput | 1s | Throughput of writing on PFS. Unit: MB/s. |
| pfs_write_latency_ms | 1s | Time delay of writing on PFS. Unit: ms. |



