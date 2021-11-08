/*-------------------------------------------------------------------------
 *
 * control_service.go
 *    Metrics provider for cluster manager
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
 *           common/polardb_pg/control_service/control_service.go
 *-------------------------------------------------------------------------
 */
package control_service

import (
	context "context"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	sync "sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/control_service/polardb"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/db_backend/dao"
	grpc "google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

type server struct {
	polardb.UnimplementedControlServiceServer
}

var once sync.Once

func GetServerInstance() error {

	once.Do(func() {
		go initGrpcServer()
	})

	return nil
}

const DefaultNetDev = "*"
const DefaultPort = 819

const (
	MonitorConfPath = "conf/monitor.yaml"
)

type MonitorConf struct {
	Collector struct {
		Database struct {
			Socketpath string `yaml:"socketpath"`
			Username   string `yaml:"username"`
			Database   string `yaml:"database"`
		} `yaml:"database"`
	} `yaml:"collector"`
	Service struct {
		Netdev string `yaml:"netdev"`
		Port   int    `yaml:"port"`
	} `yaml:"service"`
}

func initConf(confPath string) (*MonitorConf, error) {
	monitorConf := &MonitorConf{}
	monitorConf.Service.Netdev = DefaultNetDev
	monitorConf.Service.Port = DefaultPort

	confstr, err := ioutil.ReadFile(MonitorConfPath)
	if err != nil {
		log.Warn("[control_service] read conf file failed, we use default info",
			log.String("error", err.Error()),
			log.String("monitor conf", fmt.Sprintf("%+v", monitorConf)))
	} else {
		log.Info("[control_service] read conf result", log.String("conf", string(confstr)))

		err = yaml.Unmarshal(confstr, monitorConf)
		if err != nil {
			log.Error("[control_service] yaml unmarshal conf file failed",
				log.String("error", err.Error()), log.String("conf", string(confstr)))
			return nil, err
		}
	}

	return monitorConf, nil
}

func initGrpcServer() error {

	log.Info("[control_service] init grpc server")

	conf, err := initConf(MonitorConfPath)
	if err != nil {
		log.Error("[control_service] init conf failed",
			log.String("err", err.Error()), log.String("path", MonitorConfPath))
		return err
	}

	hostIP, err := getIPByNetInterface(conf.Service.Netdev)
	if err != nil {
		log.Error("[control_service] get host ip fail",
			log.String("err", err.Error()), log.String("NetInterface", conf.Service.Netdev))
		return err
	}

	address := hostIP + ":" + strconv.Itoa(conf.Service.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Error("[control_service] listen fail",
			log.String("err", err.Error()), log.String("address", address))
		return err
	}
	s := grpc.NewServer()

	log.Info("[control_service] start control service", log.String("address", address))

	polardb.RegisterControlServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Error("[control_service] init server fail", log.String("err", err.Error()))
		return err
	}

	return nil
}

func getIPByNetInterface(netInterName string) (string, error) {
	ipStr := "127.0.0.1"
	if netInterName == "0.0.0.0" || netInterName == "*" || netInterName == "" {
		return "0.0.0.0", nil
	}

	byName, err := net.InterfaceByName(netInterName)
	if err != nil {
		log.Error("[control_service] get net interface failed",
			log.String("name", netInterName), log.String("error", err.Error()))
		return ipStr, err
	}

	addresses, err := byName.Addrs()
	if err != nil {
		log.Error("[control_service] get address failed",
			log.String("name", netInterName), log.String("error", err.Error()))
		return ipStr, err
	}

	for _, addr := range addresses {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr = ipnet.IP.String()
				break
			}
		}
	}

	return ipStr, nil
}

func ConvertToDBInfo(ins *polardb.SyncInstanceRequest) *dao.DBInfo {
	dbinfo := &dao.DBInfo{
		InsName:  ins.LogicalInsname,
		Host:     ins.Rw.Host,
		Port:     int(ins.Rw.Port),
		UserName: ins.Rw.Username,
		Password: ins.Rw.Password,
		DBName:   ins.Rw.Database,
		Schema:   "polar_gawr_collection",
	}

	return dbinfo
}

func (s *server) SyncInstance(ctx context.Context,
	in *polardb.SyncInstanceRequest) (*polardb.SyncInstanceResponse, error) {

	defer TimeTrack("SyncInstance", time.Now())

	log.Info("[control_service] SyncInstance request",
		log.String("instype", in.Ins.Instype),
		log.String("local", fmt.Sprintf("%s:%d", in.Ins.Host, in.Ins.Port)),
		log.String("rw", fmt.Sprintf("%s:%d", in.Rw.Host, in.Rw.Port)))

	newdbinfo := ConvertToDBInfo(in)
	metaService := meta.GetMetaService()
	existdb, exist := metaService.GetInterface("topology", strconv.Itoa(int(in.Ins.Port)))
	if exist {
		// we don't modify logic insname here
		existdbinfo := existdb.(*dao.DBInfo)
		if !(newdbinfo.Host == existdbinfo.Host &&
			newdbinfo.Port == existdbinfo.Port &&
			newdbinfo.UserName == existdbinfo.UserName &&
			newdbinfo.Password == existdbinfo.Password &&
			newdbinfo.DBName == existdbinfo.DBName &&
			newdbinfo.Schema == existdbinfo.Schema) {
			existdbinfo.Host = newdbinfo.Host
			existdbinfo.Port = newdbinfo.Port
			existdbinfo.UserName = newdbinfo.UserName
			existdbinfo.Password = newdbinfo.Password
			existdbinfo.DBName = newdbinfo.DBName
			existdbinfo.Schema = newdbinfo.Schema
		}
	} else {
		metaService.SetInterface("topology", strconv.Itoa(int(in.Ins.Port)), newdbinfo)
	}

	result := polardb.SyncInstanceResponse{}
	result.Code = 0
	result.Msg = ""

	log.Debug("[control_service] SyncInstance",
		log.String("response", fmt.Sprintf("%+v", result)))

	return &result, nil
}

func TimeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	log.Debug("[control_service] rpc cost in ms.",
		log.String("function", key), log.Int64("elapsed", elapsed.Milliseconds()))
}
