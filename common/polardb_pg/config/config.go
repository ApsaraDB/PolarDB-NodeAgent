/*-------------------------------------------------------------------------
 *
 * config.go
 *    metric.conf extractor
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *           common/polardb_pg/config/config.go
 *-------------------------------------------------------------------------
 */

package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"

	"gopkg.in/yaml.v2"
)

var (
	MonitorConfInfo *MonitorConf
	hasInit         bool
)

const (
	MonitorConfPath = "conf/monitor.yaml"
)

type MonitorConf struct {
	Collector struct {
		Database struct {
			Socketpath string `yaml:"socketpath"`
			Username   string `yaml:"username"`
			Database   string `yaml:"database"`
			ListenCIDR string `yaml:"cidr"`
			ListenAddr *net.IPNet
		} `yaml:"database"`
		Maxscale struct {
			Path       string `yaml:"path"`
			ListenCIDR string `yaml:"cidr"`
			ListenAddr *net.IPNet
		}
		ClusterManager struct {
			LogPath    string `yaml:"logpath"`
			ListenCIDR string `yaml:"cidr"`
			ListenAddr *net.IPNet
		} `yaml:"clustermanager"`
	} `yaml:"collector"`
	Service struct {
		Netdev string `yaml:"netdev"`
		Port   int    `yaml:"port"`
	} `yaml:"service"`
}

func GetMonitorConf() (*MonitorConf, error) {
	return MonitorConfInfo, nil
}

func GetDatabaseCIDR() (*net.IPNet, error) {
	if MonitorConfInfo.Collector.Database.ListenAddr == nil {
		return nil, errors.New("no listen cidr config")
	}

	return MonitorConfInfo.Collector.Database.ListenAddr, nil
}

func GetClusterManagerCIDR() (*net.IPNet, error) {
	if MonitorConfInfo.Collector.ClusterManager.ListenAddr == nil {
		return nil, errors.New("no listen cidr config")
	}

	return MonitorConfInfo.Collector.ClusterManager.ListenAddr, nil
}

func GetMaxscaleCIDR() (*net.IPNet, error) {
	if MonitorConfInfo.Collector.Maxscale.ListenAddr == nil {
		return nil, errors.New("no listen cidr config")
	}

	return MonitorConfInfo.Collector.Maxscale.ListenAddr, nil
}

func init() {
	hasInit = false
	MonitorConfInfo = &MonitorConf{}

	confstr, err := ioutil.ReadFile(MonitorConfPath)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(confstr, MonitorConfInfo)
	if err != nil {
		panic(fmt.Sprintf("yaml unmarshal conf file failed: %s", err.Error()))
	}

	if MonitorConfInfo.Collector.Database.ListenCIDR != "" {
		_, MonitorConfInfo.Collector.Database.ListenAddr, err = net.ParseCIDR(MonitorConfInfo.Collector.Database.ListenCIDR)
		if err != nil {
			panic(fmt.Sprintf("parse cidr[%s] for database conf failed: %s",
				MonitorConfInfo.Collector.Database.ListenCIDR, err.Error()))
		}
	} else {
		MonitorConfInfo.Collector.Database.ListenAddr = nil
	}

	if MonitorConfInfo.Collector.Maxscale.ListenCIDR != "" {
		_, MonitorConfInfo.Collector.Maxscale.ListenAddr, err = net.ParseCIDR(MonitorConfInfo.Collector.Maxscale.ListenCIDR)
		if err != nil {
			panic(fmt.Sprintf("parse cidr[%s] for maxscale conf failed: %s",
				MonitorConfInfo.Collector.Maxscale.ListenCIDR, err.Error()))
		}
	} else {
		MonitorConfInfo.Collector.Maxscale.ListenAddr = nil
	}

	if MonitorConfInfo.Collector.ClusterManager.ListenCIDR != "" {
		_, MonitorConfInfo.Collector.ClusterManager.ListenAddr, err = net.ParseCIDR(MonitorConfInfo.Collector.ClusterManager.ListenCIDR)
		if err != nil {
			panic(fmt.Sprintf("parse cidr[%s] for cluster manager conf failed: %s",
				MonitorConfInfo.Collector.ClusterManager.ListenCIDR, err.Error()))
		}
	} else {
		MonitorConfInfo.Collector.ClusterManager.ListenAddr = nil
	}
}
