# 配置

*以下配置均为数据库和PolarDB-NodeAgent
的默认配置, 如无特殊需求可以略过*

## 数据库发现
由于`PolarDB-NodeAgent`部署于每一台数据库实例运行的主机, 每台主机上`PolarDB-NodeAgent`只采集本节点数据库实例的监控信息, 为了减少外部因素对于监控采集的干扰, `PolarDB-NodeAgent`将通过unix domain socket与数据库进行通信。`PolarDB-NodeAgent`与数据库之间推荐使用 peer authentication 的免密认证方式，免密范围可以限定在以指定操作系统账户启动的进程使用指定数据库账号通过 unix domain socket 的方式访问指定数据库。
为了简化配置，建议每台主机上所有数据库实例共用一个unix domain socket目录，统一提供相同的采集专用数据库账号, `PolarDB-NodeAgent`会周期性地遍历unix domain socket目录进行实例发现。

### 数据库配置
#### 连接配置
在`postgresql.conf`中对`unix_socket_directories`配置项进行设置, 如果主机上存在多个数据库实例,`unix_socket_directories`需保持一致
```
unix_socket_directories = '/tmp/'
```

#### 认证配置
在`pg_hba.conf`中确认是否存在如下配置, 如果有, 可以跳过本节的其它设置:
```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
```

在`pg_ident.conf`中添加如下内容, 配置一个专供db-mointor采集的映射关系(MAPNAME), 该映射关系关联启动UE的操作系统账户(SYSTEM-USERNAME)及db-mointor使用的数据库账户(PG-USERNAME)
```
# MAPNAME       SYSTEM-USERNAME         PG-USERNAME
collector       polardb                 polardb
```
在`pg_hba.conf`中添加如下内容, 描述上述映射关系的认证方式:
```
# TYPE  DATABASE        USER            ADDRESS                METHOD
# "local" 表示仅能够使用Unix domain socket进行连接
local   postgres        polardb                                peer map=collector
```
* TYPE: local : local表示仅能够使用Unix domain socket进行连接
* DATABASE: postgres : 指定连接的数据库
* USER: polardb : 指定数据库连接账户
* ADDRESS:  : 使用unix domain socket连接的方式无需填写此项配置
* METHOD: peer map=ue_collector :
    * peer表示使用peer authentication认证方式
    * map=collector表示使用 pg_ident.conf 中对应的映射关系, 当前的配置表示只有操作系统上polardb账户启动的进程, 才能以polardb的数据库账户使用unix domain socket的方式免密连接数据库实例的postgres数据库.

上述配置变更完成后需要重启数据库实例生效.

### PolarDB-NodeAgent
在/opt/db-mointor/conf/monitor.yaml文件中, 需要根据实际情况进行以下配置, 以便`db-monitor`能顺利发现当前节点启动的数据库实例.
```
collector:
    database:
        socketpath: "/tmp"                         # polardb unix domain socket所在路径, 由数据库参数unix_socket_directories决定
        username: "postgres"                       # 采集的数据库实例账户
        database: "postgres"                       # 采集连接的数据库
```
上述配置变更完成后需要重启`db-monitor`生效

## 采集
### 说明
为了监控数据能够更灵活的在数据库(`polardb`)和时序数据库中进行存储, 我们对监控数据模型进行了抽象, 和数据仓库的基本概念一样, 会为每一个主题的数据(`datamodel`)单独建立一张表(`数据模型`), 该表具有三种类型的列: `维度列``指标列`和`时间列`. `时间列`顾名思义, 为性能数据的采集时间;`指标列`即为采集指标, 为数值类型;`维度列`是对应指标列的描述, 一般为字符串类型, 在时序数据库中其表现形式是`tag`或`label`字段.
以`prometheus`为例, 上述指标列将作为`metric`, 上述`datamodel`字段及所有的`维度列`都将作为`metric`对应的`label`.

### 采集SQL规范
在抽象好监控数据模型之后, 我们希望仅通过采集SQL的别名就能描述数据模型的schema, 而无需再做额外定义,  因此有如下SQL的别名规范:
* 每个字段别名可以拥有特殊标识的前缀, 支持以下特殊标识:

|字段前缀名称|含义|
|  ----  | ----  |
|delta_|     指标列标识, 表示该指标在数据库中为累计值, 需要计算其在两个采集周期之间的差值|
|rate_    |指标列标识, 表示该指标在数据库中为累计值, 需要计算其在两个采集周期之间的差值相对时间的变化率|
|rt_     |指标列标识, 表示该指标是实时值|
|dim_     |维度列标识, 非必须|

* 字段别名去掉字段前缀后将映射为后端存储的实际字段名
* 时间列不用指定, UE会做处理

示例如下:
```
SELECT
        CASE WHEN wait_event IS NULL THEN 'CPU' ELSE wait_event END AS dim_wait_event,
    CASE WHEN wait_event_type IS NULL THEN 'CPU' ELSE wait_event_type END AS dim_wait_event_type,
    query AS dim_query,
    COUNT(*) AS rt_wait_count
FROM
        polar_stat_activity
WHERE
        state='active' AND backend_type='client backend'
GROUP BY
        dim_wait_event, dim_wait_event_type, dim_query
```


### 采集配置
为了让采集SQL配置生效, 我们在采集数据库中定义了如下的function方便配置:

#### 查看采集
```
polar_gawr_collection.list_collections() RETURNS TABLE (
    name TEXT,
    enable BOOLEAN,
    cycle INT8,
    role TEXT,
    datamodel TEXT,
    query TEXT,
    comment TEXT
)
```
 * 返回值: 如下schema的Table:
   * name: 采集名称
   * enable: 是否启用
   * cycle: 采集周期, 单位为秒
   * role: 采集数据库的角色, 目前可选值为(RW, RO, All)
   * datamodel: 数据模型名称
   * query: 采集SQL
   * comment: 注释

####添加采集
```
polar_gawr_collection.add_collection(
    IN _name TEXT,
  IN _query TEXT,
  IN _cycle INT8 DEFAULT 10,
  IN _datamodel TEXT DEFAULT '',
  IN _comment TEXT DEFAULT ''
) RETURNS BIGINT
```
 * 参数_name: 采集名称
 * 参数_query: 采集SQL
 * 参数_cycle: 采集周期, 非必填参数, 单位为秒, 默认为10
 * 参数_datamodel: 后端存储表名, 非必填参数, 默认与_name一致
 * 参数_comment: 注释, 非必填参数, 默认为空字符串
 * 返回值: 生效数量

#### 更新采集SQL
```
polar_gawr_collection.update_collection_query(
    IN _name TEXT,
    IN _query TEXT
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 参数_query: 采集SQL
* 返回值: 生效数量

#### 更新采集周期
```
polar_gawr_collection.set_collection_cycle(
    IN _name TEXT,
    IN _cycle INT8
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 参数_cycle: 采集周期, 单位为秒
* 返回值: 生效数量

#### 设置采集数据库的角色
```
polar_gawr_collection.set_collection_role(
    IN _name TEXT,
    IN _role TEXT
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 参数_role: 采集数据库角色, 当前支持如下设置:
* RW: 只采集RW节点
* RO: 只采集RO节点
* All: 全部节点都采集
* 返回值: 生效数量
* 说明: add_collection后, 默认值为All

#### 启用指定采集
```
polar_gawr_collection.enable_collection(
    IN _name TEXT
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 返回值: 生效数量
* 说明: add_collection后, 采集默认为启用状态

#### 禁用指定采集
```
polar_gawr_collection.disable_collection(
    IN _name TEXT
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 返回值: 生效数量

#### 回滚指定采集至默认配置
```
polar_gawr_collection.rollback_collection_to_default(
    IN _name TEXT
) RETURNS BIGINT
```
* 参数_name: 采集名称
* 返回值: 生效数量
* 说明: 如果默认无此采集, 将禁用该采集.

## 存储
监控数据在采集后支持写入指定存储后端, 具体配置如下
### PolarDB-NodeAgent plugin配置
目前仅支持在UE中设置, 每一个采集插件均可对应一个或多个存储后端, 当前因为历史原因有两个数据库采集插件, 后续会合二为一:
|插件|    说明    |配置文件|
|  ----  | ----  | ---- |
|polardb_pg    |数据库资源与无维度监控数据采集|    conf/plugin/plugin_golang_polardb_pg.conf|
|polardb_pg_multidimension    |数据库多维度监控数据采集|    conf/plugin/plugin_golang_polardb_pg_multidimension.conf|
在每个配置文件的multi_backends可以配置存储监控数据的存储后端, 存储后端可以配置多个. 需要注意的是multi_backends里出现的backend必须要存在于dependencies中.
```
# conf/plugin/plugin_golang_polardb_pg.conf
# conf/plugin/plugin_golang_polardb_pg_multidimension.conf

{
  "multi_backends": ["golang-backend-db"],
  "dependencies": ["golang-backend-db"]
}
```

### PolarDB-NodeAgent backend配置
目前支持以下三种backend, backend目前配置较为繁琐, 后续会做简化:
|backend|    说明    |详细配置|
|  ----  | ----  | ---- |
|golang-backend-db    |写入polardb|    plugin/db_backend/db_backend.conf|
|golang-backend-prometheus    |写入prometheus    |plugin/prometheus_backend/prometheus_backend.conf|
|golang-backend-influxdb    |写入influxdb    |plugin/influxdb_backend/influxdb_backend.conf|
每一种backend都可进行详细配置, 具体如下

#### db backend
* datatype_backend: backends中支持local和remote两种模式, 其个数与顺序和插件配置文件内multi_backends中名为golang-backend-db的项一一对应
```
# conf/plugin/db_backend/db_backend.conf

"datatype_backend": [
{
    "name": "polardb-o",
    "backends": ["local"]
}
]
```

* backends_conf: 可以配置local和remote的更详细信息
```
# plugin/db_backend/db_backend.conf

"backends_conf": [
    {
        "name": "local",
        "host": "/tmp",
        "port": 5432,
        "username": "postgres",
        "database": "postgres",
        "schema": "polar_awr"
    },
    {
        "name": "remote",
        "host": "localhost",
        "port": 1521,
        "username": "aurora",
        "database": "polardb_admin",
        "schema": "polar_awr_global"
    }
]
```

#### prometheus backend
目前支持作为prometheus的exporter, 在配置中可以指定端口
```
# conf/plugin/prometheus_backend/prometheus_backend.conf

{
    "exporter_port": 9974
}
```
* exporter_port: exporter监听端口


对应prometheus 的推荐 scrape_config:
```
scrape_configs:
  - job_name: 'polardb_o'

    static_configs:
    - targets: ['0.0.0.0:9974']

    honor_labels: true

    scrape_interval: 20s
    scrape_timeout: 20s
```

需要重启PolarDB-NodeAgent
生效, 重启方式见前文.

#### influxdb backend
支持直接写入influxdb, endpoint可在配置中指定
```
# conf/plugin/influxdb_backend/influxdb_backend.conf
{
    "endpoint": "localhost:8086",
    "database": "polardb_o",
    "username": "",
    "password": "",
    "log_outdict": false,
    "timeout_ms": 5000
}
```

需要重启PolarDB-NodeAgent
生效, 重启方式见前文.
