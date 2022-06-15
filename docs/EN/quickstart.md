# Quick Start

This document describes how to install and run PolarDB-NodeAgent and view the collected monitoring data.

For PolarDB for PostgreSQL, PolarDB-NodeAgent provides the exporter service on port 9974 by default for Prometheus to collect data. For the specific configurations, refer to [Configuration](configuration.md).

In addition, PolarDB-NodeAgent also provides you Grafana Dashboard configurations for easy display.

## Installation

There are two ways to install PolarDB-NodeAgent: install by compiling the source code and install by building the RPM package. Both require installing related dependencies.

### Install Dependencies
Install Go. For details about how to install Go, refer to <https://golang.org/doc/install>.

### Compile the Source Code and Install
1. Download the source code of PolarDB-NodeAgent.

   ```bash
   git clone git@github.com:ApsaraDB/PolarDB-NodeAgent.git

2. Compile the source code.

   ```bash
   go mod tidy
   go mod vendor
   make clean && make all
   ```

   Or

   ```bash
   sh build.sh
   ```

3. Install. It will be installed in `/opt/db-monitor` by default.

   ```bash
   make install
   ```

### Build the RPM Package and Install

1. Download the source code of PolarDB-NodeAgent.

   ```bash
   git clone git@github.com:ApsaraDB/PolarDB-NodeAgent.git
   ```

2. Enter the `rpm` directory and build the RPM package. Then the RPM package is in `$HOME/rpmbuild/RPMS/`.

   ```bash
   rpmbuild -bb polardb-monitor.spec
   ```

3. Execute `yum install` or `rpm -ivh` to install the RPM package.

### Start/Stop/Restart

Enter the directory `/opt/db-monitor/` and execute the following commands to start/stop/restart PolarDB-NodeAgent.

```bash
# Start
sh bin/service.sh start
# Stop
sh bin/service.sh stop
# Restart
sh bin/service.sh restart
```

## View Performance Data

### Prometheus Configuration

Add the following configurations to `scrape_conigs` in the configuration file of Prometheus.

```
  - job_name: 'polardb_o'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
    - targets: ['0.0.0.0:9974']

    honor_labels: true

    scrape_interval: 20s
    scrape_timeout: 20s
```

For the specific monitoring metrics, refer to [Metrics Introduction](metrics.md). Note that all metrics imported to Prometheus are prefixed with `polar_`.


### Grafana

In addition to the database view, you can also get a more intuitive view with Grafana by importing the configuration of Grafana Dashboard. It is recommended to use the latest version of Grafana 8.2.1. See the official documentation for installation and deployment.


#### Add the Data Source

Fill in the data source configuration of Prometheus according to the actual situation.

![add datasource](grafana_add_datasource.png)

#### Import Dashboard

PolarDB provides configured report samples in the directory `grafana` of the code repository.

![import Dashboard](grafana_import_dashboard.png)
