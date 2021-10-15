# Quick Start

This document describes how to install and run db-monitor and view the collected performance data.

## Installation

There are two ways to install db-monitor: Installing by compiling source code and by by building RPM package.

### Install Dependence
Install Go. For details about how to install golang, refer to https://golang.org/doc/install.

### Compile Source Code and Install
1. Download the source code of db-monitor.

   ```
   git clone git@github.com:ApsaraDB/db-monitor.git

2. Compile.

   ```
   go mod tidy
   go mod vendor
   make clean && make all
   ```

   Or

   ```
   sh build.sh

3. Install, and it will be installed in `/opt/db-monitor` by default.

   ```
   make install
   ```



### Build RPM Package and Install

1. Download the source code of db-monitor.

   ```
   git clone git@github.com:ApsaraDB/db-monitor.git

2. Enter the `rpm` directory and build RPM package, and the RPM package is in `$HOME/rpmbuild/RPMS/`.

   ```
   rpmbuild -bb polardb-monitor.spec
   ```

3. Run `yum install` or `rpm -ivh` to install the RPM package.



### Start/Stop/Restart

Enter the directory `/opt/db-monitor/`, and run the following commands to start/stop/restart db-monitor.

```
# Start
sh bin/service.sh start
# Stop
sh bin/service.sh stop
# Restart
sh bin/service.sh restart
```



## View Performance Data

### Database

All the collected data about the database performance are saved in the schema `polar_gawr_collection`. You can view the related views to check the data.

For example, view  `view_fact_dbmetrics` to check the resource usage about the database and related metrics.

For all the metrics and data description, refer to [Metrics Introduction](metrics.md).


### Grafana

In addition to the database view, you can also get a more intuitive view with grafana by importing the configuration of grafana dashboard. It is recommended to use the latest version of grafana 8.2.1. See the official documentation for installation and deployment.

#### Add Data Source

PolarDB uses `PostgreSQL` as data source, fill in the data source configuration according to the actual situation.

![add datasource](grafana_add_datasource.png)

#### Import Dashboard

PolarDB provides configured report samples in the 'grafana' directory.

![import Dashboard](grafana_import_dashboard.png)
