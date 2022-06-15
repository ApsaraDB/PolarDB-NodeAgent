# Configuration

> All of the following are the default configurations of the database and PolarDB-NodeAgent. You can skip them if there are no special requirements.

## Discover the Database
As `PolarDB-NodeAgent` is deployed on each host where the database instances run, `PolarDB-NodeAgent` on each host only collects the monitoring information of the database instances in this node. To reduce the external factors' interference with data monitoring and collection, `PolarDB-NodeAgent` communicates with the database through the unix domain socket. The passwordless authentication method of peer authentication is recommended between `PolarDB-NodeAgent` and the database. The passwordless authentication can be limited to the situation where a process started by a specified operating system account uses a specified database account to access a specified database through the unix domain socket.

To simplify the configuration, it is suggested that all database instances on each host share a unix domain socket directory and use the same dedicated database account provided for collection. `PolarDB-NodeAgent` will periodically traverse the unix domain socket directory to discover instances.

### Configure the Database
#### Configure Connection
Configure `unix_socket_directories` in the file `postgresql.conf`. If there are many database instances on the host, the `unix_socket_directories` should be the same.

```
unix_socket_directories = '/tmp/'
```

#### Configure Authentication
Check whether the file `pg_hba.conf` contains the following configurations. If so, you can skip other configurations in this section.

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
```

Add the following content to `pg_ident.conf` to configure the mapping relationship (MAPNAME) dedicated for the collection of PolarDB-NodeAgent. The operating system account (SYSTEM-USERNAME) for starting PolarDB-NodeAgent is associated with the database account (PG-USERNAME) used by PolarDB-NodeAgent in the mapping relationship.

```
# MAPNAME       SYSTEM-USERNAME         PG-USERNAME
collector       polardb                 polardb
```
Add the following content to `pg_hba.conf` to describe the authentication method of the mapping relationship mentioned above:
```
# TYPE  DATABASE        USER            ADDRESS                METHOD
# "local" indicates that only the Unix domain socket can be used for the connection.
local   postgres        polardb                                peer map=collector
```
* `TYPE: local `: "local" indicates that only the Unix domain socket can be used for the connection.
* `DATABASE: postgres`: specifies the connected database.
* `USER: polardb`: specifies the account used to connect to the database.
* `ADDRESS:`: You do not have to set this parameter if you use the Unix domain socket for connection.
* METHOD: peer map=ue_collector :
    * "peer" indicates using peer authentication as the authentication method.
    * "map=collector" indicates using the mapping relationship in pg_ident.conf. The current configuration indicates that only the processes started by the polardb account in the operating system can use the polardb database account to connect to the postgres database of the database instance through the Unix domain socket without a password.

After changing the above configurations, you need to restart the database instance to make them take effect.

### PolarDB-NodeAgent
In the /opt/db-monitor/conf/monitor.yaml, you can configure the following parameters according to your actual needs so that `db-monitor` can successfully discover the database instances started on the current node.

```
collector:
    database:
        socketpath: "/tmp"                         # the path of polardb unix domain socket, which is determined by the database parameter unix_socket_directories
        username: "postgres"                       # the account of the database instance used for collection
        database: "postgres"                       # the database connected for collection
```
After changing the above configurations, you need to restart `db-monitor` to make them take effect.

## Collect Monitoring Data
### Introduction
To store the monitoring data more flexibly in the database (`polardb`) and the time-series database, we abstract the models of the monitoring data. Like the basic concept of the data warehouse, we create a separate table (`data model`) for the data (`datamode`) of each topic. This table contains three types of columns: `dimension`, `metric`, and `time`. The `time` column shows the collection time of the performance data. The `metric` column shows the collection metrics which are of numeric types. The `dimension` column shows the description of the corresponding `metric` column which is of type string in general. In the time-series database, the description of the corresponding `metric` is in the `tag` or `label` field. Take the time series database `prometheus` as an example. The metrics will be listed in the `metric` column, the `datamodel` field and all `dimension` columns will be in the corresponding `label` of `metric`.

### Define Specifications of Collection SQL
After abstracting models of the monitoring data, we hope to describe the schema of the data model only by the alias of the collection SQL without extra definitions. Therefore, we define the following SQL alias specifications:

* The alias of each field can be prefixed with a specific identifier. Supported special identifiers are shown as follows:

|Field Prefix|Description|
|  ----  | ----  |
|delta_| Identifier of the `metric` column. It indicates that the value of this metric is a cumulative value in the database and it is required to calculate the difference between two collection periods. |
|rate_    |Identifier of the `metric` column. It indicates that the value of this metric is a cumulative value in the database and it is required to calculate the rate of change of the difference between two collection periods to the time.|
|rt_     |Identifier of the `metric` column. It indicates that the value of the metric is a real-time value.|
|dim_     |Identifier of the `dimension` column. It is not required.|

* After the field prefix is removed from the field alias, the result will be mapped to the actual field name stored in the backend.
* The `time` column does not need to be specified and it will be processed by PolarDB-NodeAgent.

Example:
```sql
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


### Configure Collection
To make the collection SQL take effect, we define the following functions in the collection database for convenient configuration:

#### View Collection
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
 * Return values: the table of the following schema:
   * name: collection name
   * enable: whether to enable
   * cycle: collection period, unit: second
   * role: role of the database for the collection, currently it can be RW, RO, or All.
   * datamodel: data model name
   * query: collection SQL
   * comment: annotation

#### Add Collection

```
polar_gawr_collection.add_collection(
    IN _name TEXT,
  IN _query TEXT,
  IN _cycle INT8 DEFAULT 10,
  IN _datamodel TEXT DEFAULT '',
  IN _comment TEXT DEFAULT ''
) RETURNS BIGINT
```
 * Parameter `_name`: collection name
 * Parameter `_query`: collection SQL
 * Parameter `_cycle`: collection period, optional parameter, default value: 10, unit: second.
 * Parameter `_datamodel`: table name stored in the backend. It is not required and is the same as `_name` by default.
 * Parameter `_comment`: annotation, it is not required and is an empty string by default.
 * Return value: number of collection SQL statements that take effect

#### Update the Collection SQL
```
polar_gawr_collection.update_collection_query(
    IN _name TEXT,
    IN _query TEXT
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Parameter `_query`: collection SQL
* Return value: number of collection SQL statements that take effect

#### Update the Collection Period
```
polar_gawr_collection.set_collection_cycle(
    IN _name TEXT,
    IN _cycle INT8
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Parameter `_cycle`: collection period, unit: second
* Return value: number of collection SQL statements that take effect

#### Set the Database Role for Collection
```
polar_gawr_collection.set_collection_role(
    IN _name TEXT,
    IN _role TEXT
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Parameter `_role`: role of the database for the collection. Currently, it can be set to one of the following values:
  * RW: only collect data from the RW node
  * RO: only collect data from the RO nodes
  * All: collect data from all nodes

* Return value: number of collection SQL statements that take effect
* Notes: After the function add_collection is called, the default value of parameter `_role` is All.

#### Enable Specified Collection
```
polar_gawr_collection.enable_collection(
    IN _name TEXT
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Return value: number of collection SQL statements that take effect
* Note: After the function add_collection is called, the collection is enabled by default.

#### Disable Specified Collection
```
polar_gawr_collection.disable_collection(
    IN _name TEXT
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Return value: number of collection SQL statements that take effect

#### Roll Back Specified Collection to Default Settings
```
polar_gawr_collection.rollback_collection_to_default(
    IN _name TEXT
) RETURNS BIGINT
```
* Parameter `_name`: collection name
* Return value: number of collection SQL statements that take effect
* Note: If the specified collection does not exist by default, it will be disabled by default.

## Store Monitoring Data
The collected monitoring data can be written to the specified backend storage. The specific configuration is as follows:

### PolarDB-NodeAgent Plug-in Configuration
Currently, the plug-in configuration can only be set in PolarDB-NodeAgent. Each collection plug-in can correspond to one or more storage backends.

At present, there are two plug-ins for database collection. We will merge them in the future.

|Plug-in|    Description    |Configuration File|
|  ----  | ----  | ---- |
|polardb_pg    |Collect the resource and dimensionless monitoring data of the database|    conf/plugin/plugin_golang_polardb_pg.conf|
|polardb_pg_multidimension    |Collect the multidimensional monitoring data of the database|    conf/plugin/plugin_golang_polardb_pg_multidimension.conf|

In each configuration file, you can set multi-backends to the backend storage for storing the monitoring data. It is supported to configure multiple storage backends. Note that backends configured in multi-backends must exist in dependencies.

```
# conf/plugin/plugin_golang_polardb_pg.conf
# conf/plugin/plugin_golang_polardb_pg_multidimension.conf

{
  "multi_backends": ["golang-backend-db"],
  "dependencies": ["golang-backend-db"]
}
```

### PolarDB-NodeAgent backend Configuration
Currently, the following three types of backend are supported.

> Now the backend configuration is complex and will be simplified later.

|backend|    Description    |Detailed Configuration|
|  ----  | ----  | ---- |
|golang-backend-db    |Write data to polardb|    plugin/db_backend/db_backend.conf|
|golang-backend-prometheus    |Write data to prometheus    |plugin/prometheus_backend/prometheus_backend.conf|
|golang-backend-influxdb |Write data to InfluxDB |plugin/influxdb_backend/influxdb_backend.conf|

Each type of backend can be configured in detail as shown below:

#### db backend

* datatype_backend: Two modes can be set for "backends": local and remote. The number and order of elements in the array datatype_backend correspond to the item golang-backend-db of multi_backends in the plug-in configuration file.
```
# conf/plugin/db_backend/db_backend.conf

"datatype_backend": [
{
    "name": "polardb-o",
    "backends": ["local"]
}
]
```

* backends_conf: You can configure detailed information about local and remote here.
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
Currently, it supports using the prometheus backend as the exporter of prometheus. You can specify the port in the configuration.

```
# conf/plugin/prometheus_backend/prometheus_backend.conf

{
    "exporter_port": 9974
}
```
* exporter_port: listening port of exporter


The recommended scrape_config of the corresponding prometheus is as follows:
```
scrape_configs:
  - job_name: 'polardb_o'

    static_configs:
    - targets: ['0.0.0.0:9974']

    honor_labels: true

    scrape_interval: 20s
    scrape_timeout: 20s
```

You need to restart PolarDB-NodeAgent to make the configuration take effect. For details about how to restart PolarDB-NodeAgent, refer to [Quick Start](quickstart.md).

#### influxdb backend

The collected data can be directly written to the InfluxDB. You can specify the endpoint in the configuration.

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

You need to restart PolarDB-NodeAgent to make the configuration take effect. For details about how to restart PolarDB-NodeAgent, refer to [Quick Start](quickstart.md).
