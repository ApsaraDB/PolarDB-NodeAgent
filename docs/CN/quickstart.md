# 快速入门

本文档介绍了如何安装和运行PolarDB-NodeAgent，并查看采集到的监控数据。

对于PolarDB-for-PostgreSQL, PolarDB-NodeAgent默认把将监控数据写回数据库, 并在数据库内提供视图以供查询, 也可通过配置将监控数据推送给prometheus pushgateway, 具体配置可见[配置文档](configuration.md).
此外, 对于监控数据存入数据库的场景, 还配有grafana dashboard方便展示.

## 安装

目前提供两种安装部署的方式：源码编译安装及RPM包安装，两种方式均需安装相关依赖。

### 安装依赖
  [安装golang](https://golang.org/doc/install)

### 源码编译 & 安装
1. 源码下载

   ```
   git clone git@github.com:ApsaraDB/PolarDB-NodeAgent.git
   ```

2. 编译

   ```
   go mod tidy
   go mod vendor
   make clean && make all
   ```

   或

   ```
   sh build.sh
   ```

3. 安装, 默认安装路径为`/opt/db-monitor`

   ```
   make install
   ```



### RPM包构建 & 安装

1. 源码下载

   ```
   git clone git@github.com:ApsaraDB/PolarDB-NodeAgent.git
   ```

2. 在源码目录下进入到 `rpm` 子目录，构建RPM包, 完成后RPM包在`$HOME/rpmbuild/RPMS/`路径下。

   ```
   rpmbuild -bb polardb-monitor.spec
   ```

3. 执行`yum install`或者`rpm -ivh`对RPM包进行安装。



## 运行

进入`/opt/db-monitor/`目录，执行如下命令进行启动、停止和重启操作：
```
# 启动
sh bin/service.sh start
# 停止
sh bin/service.sh stop
# 重启
sh bin/service.sh restart
```



## 查看监控数据

### 数据库

所有采集到的监控数据，默认均存放于数据库实例的`polar_gawr_collection` schema下，可以通过视图进行查看。

例如：`view_fact_dbmetrics`保存数据库资源消耗情况及基础监控指标。
所有监控指标可见[metrics说明](metrics.md)文档，采集及指标保留配置情况可见[配置说明](configuration.md)文档。

### Grafana

除数据库视图之外，目前还提供更直观的grafana展示，可以通过导入grafana dashboard配置的形式进行查看。
推荐使用最新版本的grafana 8.2.1，安装部署可见官方文档。

#### 前置条件

* 认证: 需要合理设置数据库的访问配置, 即`pg_hba.conf`, 以便外部访问
* 鉴权: 对于非超级用户, 需要赋予 `polar_gawr_user` 权限:
```
GRANT polar_gawr_user TO [用户名]
```

#### 添加数据源

PolarDB使用`PostgreSQL`数据源，数据源配置请根据实际情况填写。

![添加数据源](grafana_add_datasource.png)

#### 导入Dashboard

PolarDB提供配置好的报表样例，在`grafana`目录下。

![导入Dashboard](grafana_import_dashboard.png)
