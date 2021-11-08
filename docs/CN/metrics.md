# 采集指标说明

PolarDB-NodeAgent
将采集到的监控信息保存在数据库的表内，包括等待事件统计、数据库IO及延迟分布、各后端进程的资源统计等信息。每一类监控信息（数据模型）对应一张数据表，每张数据表包含维度列和指标列。


## dbmetrics

dbmetrics展示了采集到的数据库系统资源消耗情况及基本监控项，其指标和描述如下：

| 指标 | 采集周期 | 单位 | 说明 |
| --- | --- | --- | --- |
| **连接状态** |  |  |  |
| active_connections | 1s | 个 | 活跃连接数 |
| waiting_connections | 1s | 个 | 处于等待状态的连接数 |
| idle_connections | 1s | 个 | 空闲连接数 |
| **等待事件** |  |  |  |
| client_waits | 1s | 个 | 等待客户端进程数 |
| lwlock_waits | 1s | 个 | lwlock等待进程数 |
| io_waits | 1s | 个 | 等待IO进程数 |
| lock_waits | 1s | 个 | lock等待进程数 |
| extension_waits | 1s | 个 | 插件等待进程数 |
| ipc_waits | 1s | 个 | 处于进程间通信的进程数 |
| timeout_waits | 1s | 个 | 等待超时的进程数量 |
| bufferpin_waits | 1s | 个 | 等待bufferpin的进程数量 |
| cpu_waits | 1s | 个 | 使用CPU的进程数量 |
| activity_waits | 1s | 个 | 当前处于空闲状态，等待变成活跃状态的进程数量 |
| **事务数** |  |  |  |
| commits_delta | 1s | 个 | 提交的事务数 |
| rollbacks_delta | 1s | 个 | 回滚的事务数 |
| **事务状态** |  |  |  |
| active_transactions | 1s | 个 | 活跃事务数 |
| idle_transactions | 1s | 个 | 空闲事务数 |
| waiting_transactions | 1s | 个 | 等待状态的事务数 |
| one_second_transactions | 1s | 个 | 执行超过1秒的长事务数 |
| three_second_transactions | 1s | 个 | 执行超过3秒的长事务数 |
| five_seconds_long_transactions | 1s | 个 | 执行超过5秒的长事务数 |
| one_second_idle_transactions | 1s | 个 | 空闲超过1秒的事务数 |
| three_seconds_idle_transactions | 1s | 个 | 空闲超过3秒的事务数 |
| five_seconds_idle_transactions | 1s | 个 | 空闲超过5秒的事务数 |
| two_pc_transactions | 1s | 个 | 两阶段事务数 |
| one_second_two_pc_transactions | 1s | 个 | 执行超过1秒的两阶段事务数 |
| three_seconds_two_pc_transactions | 1s | 个 | 执行超过3秒的两阶段事务数 |
| five_seconds_two_pc_transactions | 1s | 个 | 执行超过5秒的两阶段长事务数 |
| swell_time | 1s | 秒 | 当前最长事务持续时间 |
| **SQL** |  |  |  |
| one_second_executing_sqls | 1s | 个 | 执行超过1s的慢SQL数量 |
| three_seconds_executing_sqls | 1s | 个 | 执行超过3秒的慢SQL数量 |
| fibe_seconds_executing_sqls | 1s | 个 | 执行超过5秒的慢SQL数量 |
| long_executing_sqls | 1s | 个 | 执行超过7200秒的慢SQL数量 |
| deadlocks_delta | 1s | 个 | 死锁数量 |
| conflicts_delta | 1s | 个 | 由于恢复冲突导致取消的查询数量 |
| **数据库处理行数** |  |  |  |
| tup_returned_delta | 1s | 行 | 扫描行数 |
| tup_fetched_delta | 1s | 行 | 返回行数 |
| tup_inserted_delta | 1s | 行 | 插入行数 |
| tup_updated_delta | 1s | 行 | 更新行数 |
| tup_deleted_delta | 1s | 行 | 删除行数 |
| **临时文件** |  |  |  |
| temp_files_delta | 1s | 个 | 临时文件个数 |
| temp_bytes_delta | 1s | 字节 | 临时文件字节数 |
| **数据库buffer** |  |  |  |
| blks_hit_delta | 1s | 个 | 命中缓存block数量 |
| blks_read_delta | 1s | 个 | 物理读次数 |
| buffers_backend_delta | 1s | 个 | backend写buffer数量 |
| buffers_alloc_delta | 1s | 个 | buffer分配数量 |
| buffers_backend_fsync_delta | 1s | 个 | backend fsync buffer数 |
| buffers_checkpoint_delta | 1s | 个 | checkpoint写buffer数量 |
| buffers_clean_delta | 1s | 个 | bgwriter写buffer数量 |
| maxwritten_clean_delta | 1s | 次 | bgwriter由于写了过多buffer而停止扫描的次数 |
| polar_dirtypage_size | 1s | 个 | buffer脏页数量 |
| polar_copybuffer_used_size | 1s | 个 | copy buffer使用数量 |
| polar_copybuffer_isfull | 1s | bool | copy buffer是否满 |
| **checkpoint** |  |  |  |
| checkpoint_sync_time_delta | 1s | 秒 | checkpoint sync时间 |
| checkpoints_timed_delta | 1s | 次 | 定时checkpoint次数 |
| checkpoint_write_time_delta | 1s | 秒 | checkpoint write时间 |
| checkpoints_req_delta | 1s | 个 | 主动请求checkpoint次数 |
| logindex_mem_tbl_size | 1s | 个 | logindex table个数 |
| **数据库年龄** |  |  |  |
| db_age | 1s | xid | 数据库年龄 |
| **块设备IO** |  |  |  |
| local_iops_read | 1s | IOPS | 每秒本地读IO |
| local_iops_write | 1s | IOPS | 每秒本地写IO |
| local_throughput_read | 1s | MB/s | 每秒读吞吐 |
| local_throughput_write | 1s | MB/s | 每秒写吞吐 |
| **复制** |  |  |  |
| replay_latency_in_mb | 1s | MB | 备库回放延迟 |
| send_latency_in_mb | 1s | MB | 主库发送延迟 |
| ap_cp_latency_mb | 1s | MB | 回放位点与一致性位点差距 |
| wp_ap_latency_mb | 1s | MB | 写入位点与回放位点差距 |
| wp_cp_latency_mb | 1s | MB | 写入位点与一致性位点差距 |

## polar_stat_aas_history
polar_stat_aas_history展示了采集到的等待事件统计信息，展示的维度包括：

| 维度 | 说明 |
| --- | --- |
| wait_event_type | 等待事件类型 |
| wait_event | 等待事件的名称 |
| queryid | queryid |

| 指标 | 采集周期 | 单位 | 说明 |
| --- | --- | --- | --- |
| wait_count | 1s | 个 | 同一queryid处于同一等待事件的会话数量 |

## polar_stat_io_info

polar_stat_io_info展示了采集到的数据库IO调用（例如`falloc`、`fsync`、 `read`、 `write`、 `creat`、 `seek`、 `open`、 `close`）信息，展示的维度包括：

| 维度 | 说明 |
| --- | --- |
| fileloc | 文件存储位置 (local: 本地, pfs: pfs) |
| filetype | 文件类型 |

展示的数据库IO信息的指标和描述如下：（按照调用类型排序）

| 指标 | 采集周期 | 单位 | 说明 |
| --- | --- | --- | --- |
| falloc_latency_us | 1s | us | 一秒内falloc调用时间的累计值 |
| falloc_count | 1s | 次/s | 一秒内falloc调用的次数 |
| creat_latency_us | 1s | us | 一秒内creat调用时间的累计值 |
| creat_count | 1s | 次/s | 一秒内creat调用的次数 |
| read_throughput | 1s | MB/s | 一秒内read调用的吞吐量 |
| read_latency_us | 1s | us | 一秒内read调用时间的累计值 |
| read_count | 1s | 次/s | 一秒内read调用的次数 |
| write_throughput | 1s | MB | 一秒内write调用的吞吐量 |
| write_latency_us | 1s | us | 一秒内write调用时间的累计值 |
| write_count | 1s | 次/s | 一秒内write调用的次数 |
| fsync_latency_us | 1s | us | 一秒内fsync调用时间的累计值 |
| fsync_count | 1s | 次/s | 一秒内fsync调用的次数 |
| seek_latency_us | 1s | us | 一秒内seek调用时间的累计值 |
| seek_count | 1s | 次/s | 一秒内seek调用的次数 |
| open_latency_us | 1s | us | 一秒内open调用时间的累计值 |
| open_count | 1s | 次/s | 一秒内open调用的次数 |
| close_count | 1s | 次/s | 一秒内close调用的次数 |

## polar_stat_io_latency
polar_stat_io_latency展示了采集到的数据库IO调用（例如`fsync`、 `read`、 `write`、  `seek`、 `open`）的延迟时间的分布信息，展示的维度包括：

| **维度** | **说明** |
| --- | --- |
| latency | 延迟时间区间 |

数据库IO延迟分布信息的指标和描述如下：

| **指标** | **采集周期** | **单位** | **说明** |
| --- | --- | --- | --- |
| seek | 1s | 个 | 处于延迟区间内的seek调用的个数 |
| fsync | 1s | 个 | 处于延迟区间内的fsync调用的个数 |
| read | 1s | 个 | 处于延迟区间内的read调用的个数 |
| write | 1s | 个 | 处于延迟区间内的write调用的个数 |
| open | 1s | 个 | 处于延迟区间内的open调用的个数 |

### polar_stat_process

polar_stat_process展示了采集到的backend的系统资源消耗统计信息，展示的维度包括：

| **维度** | **说明** | 
| --- | --- |
| backend_type | 进程类型 | 

backend资源统计信息的指标和描述如下：

| 指标 | 采集周期 | 单位 | 说明 |
| --- | --- | --- | --- |
| cpu_user | 1s | % | 用户态CPU |
| cpu_sys | 1s | % | 系统态CPU |
| rss | 1s | MB | RSS（实际内存占用大小） |
| pfs_read_ps | 1s | 次 | pfs读次数 |
| pfs_read_throughput | 1s | MB/s | pfs读吞吐 |
| pfs_read_latency_ms | 1s | ms | pfs读延迟 |
| pfs_write_ps | 1s | 次 | pfs写次数 |
| pfs_write_throughput | 1s | MB/s | pfs写吞吐 |
| pfs_write_latency_ms | 1s | ms | pfs写延迟 |



