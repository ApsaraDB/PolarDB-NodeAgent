/*-------------------------------------------------------------------------
 *
 * db_backend.go
 *    Database backend plugin
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
 *           plugins/db_backend/db_backend.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/ApsaraDB/db-monitor/common/consts"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/control_service"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/db_config"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/meta"
	"github.com/ApsaraDB/db-monitor/common/utils"
	"github.com/ApsaraDB/db-monitor/plugins/db_backend/dao"
)

const (
	defaultPort    = "30051"
	DEFAULT_SCHEMA = "polar_gawr_collection"
	DBConnTimeout  = 10
	DBQueryTimeout = 10
)

var (
	sqlite_dir = "/opt/pdb_ue/data/sqlite"
)

var dbmanager *dao.DBManager
var once sync.Once

type DatatypeBackend struct {
	Name     string   `json:"name"`
	Backends []string `json:"backends"`
}

type BackendsConf struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
}

type DBBackendConf struct {
	DbBackends struct {
		BackendsConf    []BackendsConf    `json:"backends_conf"`
		DatatypeBackend []DatatypeBackend `json:"datatype_backend"`
	} `json:"db_backends"`
	DefaultRetentionTime     int                  `json:"default_retention_time"`
	DefaultRetentionInterval int                  `json:"default_retention_interval"`
	InitSQLs                 []db_config.QueryCtx `json:"init_sqls"`
}

type DBConf struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
}

type InsDesc struct {
	logical_ins_name  string
	physical_ins_name string
	host              string
	port              string
	role              string
	version           string
}

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("[sqlite_backend] invalid ctx")
	}
	extern := m[consts.PluginExternKey].(string)

	log.Info("[db_backend] plugin init", log.String("m", fmt.Sprintf("%+v", m)))
	content, err := ioutil.ReadFile(extern)
	if err != nil {
		log.Error("[db_backend] read sqlite conf fail", log.String("err", err.Error()))
		return nil, err
	}

	once.Do(func() {
		dbmanager = dao.NewDBManager()
		err = dbmanager.Init(string(content), sqlite_dir)
		if err != nil {
			log.Error("[db_backend] init sqlite fail", log.String("err", err.Error()))
			panic(err)
		}

		// start service to get topology from cluster manager
		control_service.GetServerInstance()
	})

	err = initContext(content, m)
	if err != nil {
		log.Error("[db_backend] init context failed", log.String("err", err.Error()))
		return nil, err
	}

	m["configmap"] = nil

	return ctx, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return errors.New("[db_backend] invalid ctx")
	}

	backendCtx, ok := m["backend"].(map[string]interface{})
	if !ok {
		return errors.New("[db_backend] no backends ctx")
	}

	// prepare meta info
	metaService := meta.GetMetaService()
	logical_ins_name := m["logical_ins_name"].(string)
	physical_ins_name := m["physical_ins_name"].(string)
	host := m["backend_host"].(string)
	port := m["backend_port"].(string)
	username := ""
	portint, _ := strconv.Atoi(port)
	endpoint := "/tmp"
	role := "RW"
	version := "others"
	var configmap map[string]interface{}
	if backendCtx["configmap"] == nil {
		configmap = nil
	} else {
		configmap = backendCtx["configmap"].(map[string]interface{})
	}
	plugin := m["plugin"].(string)
	data_type := m["datatype"].(string)
	collect_type := m["collect_type"].(string)
	initsqls := backendCtx["init_sqls"].([]db_config.QueryCtx)

	log.Debug("[sqlite_backend] get endpoint ", log.String("endpoint", endpoint))
	if data_type == "polardb-o" {
		configmap = metaService.GetSingleMap("configmap", port)
		endpoint = metaService.GetSingleString("endpoint", port)
		username = metaService.GetSingleString("username", port)
		role = metaService.GetSingleString("role", port)
		if metaService.GetSingleString("polar_release_date", port) == "20190101" {
			version = fmt.Sprintf("polar version: %s",
				metaService.GetSingleString("polar_version", port))
		} else {
			version = fmt.Sprintf("release date: %s, polar version: %s",
				metaService.GetSingleString("polar_release_date", port),
				metaService.GetSingleString("polar_version", port))
		}
	} else if data_type == "host" {
		role = "Host"
	}

	// get logical ins name for non polardb o
	if data_type != "polardb-o" {
		if data_type != "host" {
			if x, ok := metaService.GetInterface("topology", port); ok {
				dbinfo := x.(*dao.DBInfo)
				if dbinfo.InsName != "" {
					log.Debug("[db_backend] use meta dbinfo",
						log.String("ins", dbinfo.InsName))
					logical_ins_name = dbinfo.InsName
				}

				// TODO(wormhole.gl): get ins name from db
			} else {
				log.Warn("[sqlite_backend] get topology failed",
					log.String("data type", data_type), log.String("port", port))
				return nil
			}
		}
	} else {
		if r, ok := metaService.GetString("role", port); !ok {
			log.Warn("[db_backend] get db role failed",
				log.String("data type", data_type), log.String("port", port))
			return nil
		} else {
			role = r
		}

		if polar_release_date, ok := metaService.GetString("polar_release_date", port); !ok {
			log.Warn("[db_backend] get db version failed",
				log.String("data type", data_type), log.String("port", port))
			return nil
		} else {
			if polar_release_date == "20190101" {
				version = fmt.Sprintf("polar version: %s",
					metaService.GetSingleString("polar_version", port))
			} else {
				version = fmt.Sprintf("release date: %s, polar version: %s",
					metaService.GetSingleString("polar_release_date", port),
					metaService.GetSingleString("polar_version", port))
			}
		}

		log.Debug("[db_backend] get db info success",
			log.String("plugin", plugin), log.String("role", role),
			log.String("port", port), log.String("version", version))
	}

	// default location
	backendDBType := "Postgres"
	backendLocation := "remote"

	if datatypeConf, ok := backendCtx["datatype_conf"]; ok {
		if datatype, ok := datatypeConf.(map[string]DatatypeBackend)[m["datatype"].(string)]; ok {
			if m["backend_idx"].(int) < len(datatype.Backends) {
				backendLocation = datatype.Backends[m["backend_idx"].(int)]
			} else {
				log.Info("[db_backend] no such backend location config, we will return directly",
					log.String("collecttype", collect_type),
					log.String("dbtype", backendDBType),
					log.String("location", backendLocation),
					log.Int("idx", m["backend_idx"].(int)),
					log.Int("backend location length", len(datatype.Backends)),
					log.String("datatype", m["datatype"].(string)))
				return nil
			}
			if backendLocation == "sqlite" {
				backendDBType = "SQLITE"
			}
		}
	}

	insname := host

	if data_type != "host" {
		insname = logical_ins_name
	}

	insID := insname + "_" + port + "_" + backendLocation

	var dbinfo *dao.DBInfo

	if backendLocation == "local" {
		enable_localdb_backend := GetConfigMapValue(configmap,
			"enable_write_localdb_backend",
			"integer", 1).(int)
		if enable_localdb_backend == 0 {
			log.Debug("[db_backend] local db backend is disabled")
			return nil
		}

		enable_ro_write_localdb_backend := GetConfigMapValue(configmap,
			"enable_ro_write_localdb_backend",
			"integer", 0).(int)
		if role == "RO" && enable_ro_write_localdb_backend == 0 {
			log.Debug("[db_backend] ro write local db backend is disabled")
			return nil
		}

		localdb_backend_dbname := GetConfigMapValue(configmap, "localdb_backend_dbname",
			"string", "postgres").(string)
		dbinfo = &dao.DBInfo{
			Host:     endpoint,
			Port:     portint,
			UserName: username,
			Password: "",
			DBName:   localdb_backend_dbname,
			Schema:   DEFAULT_SCHEMA,
			DBType:   backendDBType,
			DBPath:   "/opt/pdb_ue/data/sqlite/",
		}

		if username == "" {
			if dbconf, ok := backendCtx["backends_conf"].(map[string]BackendsConf)["local"]; ok {
				dbinfo.UserName = dbconf.Username
				dbinfo.Password = dbconf.Password
				dbinfo.DBName = dbconf.Database
				dbinfo.Schema = dbconf.Schema

				log.Debug("[sqlite_backend] dbinfo",
					log.String("collecttype", collect_type),
					log.String("dbinfo", fmt.Sprintf("%+v", dbinfo)))
			}
		}

		if x, ok := metaService.GetInterface("topology", port); ok {
			tmpdbinfo := x.(*dao.DBInfo)
			dbinfo.Host = tmpdbinfo.Host
			dbinfo.Port = tmpdbinfo.Port
			dbinfo.UserName = tmpdbinfo.UserName
			dbinfo.Password = tmpdbinfo.Password
			dbinfo.Schema = tmpdbinfo.Schema
			dbinfo.DBName = localdb_backend_dbname
			log.Debug("[db_backend] get rw by ins port",
				log.String("local ins port", port),
				log.String("rw", fmt.Sprintf("%s:%d", dbinfo.Host, dbinfo.Port)),
				log.String("database", dbinfo.DBName))
		} else {
			log.Debug("[db_backend] using origin dbinfo",
				log.String("local ins port", port),
				log.String("endpoint", fmt.Sprintf("%s:%d", endpoint, portint)))

			if data_type != "polardb-o" {
				log.Warn("[db_backend] cannot find related backend",
					log.String("ins port", port),
					log.String("data type", data_type))
				return nil
			} else {
				if role != "RW" {
					log.Info("[db_backend] we are not RW and we don't get RW's topology, do nothing")
					return nil
				}
			}
		}
	} else if backendLocation == "remote" {
		enable_remotedb_backend := GetConfigMapValue(configmap,
			"enable_write_remotedb_backend", "integer", 1).(int)
		if enable_remotedb_backend == 0 {
			log.Warn("[sqlite_backend] remote db backend is disabled")
			return nil
		}

		dbtype := m["datatype"].(string)
		if dbtype == "polardb-o" {
			dbtype = "db"
		}

		schemaName := strings.Replace(insname, ".", "_", -1)
		schemaName = strings.Replace(schemaName, "-", "$", -1)
		schemaName = "polar_gawr_collection_" + dbtype + "_" + schemaName
		// must cut to 64 here
		if len(schemaName) > 63 {
			schemaName = schemaName[0:63]
		}

		var dbconf BackendsConf
		if dbconf, ok = backendCtx["backends_conf"].(map[string]BackendsConf)["remote"]; ok {
			dbinfo = &dao.DBInfo{
				Host:     dbconf.Host,
				Port:     dbconf.Port,
				UserName: dbconf.Username,
				Password: dbconf.Password,
				DBName:   dbconf.Database,
				Schema:   schemaName,
				DBType:   backendDBType,
			}

			log.Debug("[db_backend] dbinfo",
				log.String("collecttype", collect_type),
				log.String("dbinfo", fmt.Sprintf("%+v", dbinfo)))
		}
	} else {
		log.Debug("[db_backend] neither local or remote, we use sqlite as default")
		enable_sqlite_backend := GetConfigMapValue(configmap,
			"enable_write_sqlite_backend", "integer", 1).(int)
		if enable_sqlite_backend == 0 {
			log.Debug("[sqlite_backend] sqlite backend is disabled")
			return nil
		}

		dbtype := m["datatype"].(string)
		if dbtype == "polardb-o" {
			dbtype = "db"
		}

		schemaName := strings.Replace(insname, ".", "_", -1)
		schemaName = strings.Replace(schemaName, "-", "_", -1)
		schemaName = "polar_gawr_collection_" + dbtype + "_" + schemaName

		dbinfo = &dao.DBInfo{
			Host:     "/tmp",
			Port:     portint,
			UserName: "postgres",
			Password: "",
			DBName:   "postgres",
			Schema:   schemaName,
			DBType:   backendDBType,
			DBPath:   "/opt/pdb_ue/data/sqlite/",
		}
	}

	log.Debug("[db_backend] dbtype and location",
		log.String("collecttype", collect_type),
		log.String("dbtype", backendDBType),
		log.String("location", backendLocation),
		log.Int("idx", m["backend_idx"].(int)),
		log.String("datatype", m["datatype"].(string)),
		log.String("ins id", insID),
		log.String("dbinfo", fmt.Sprintf("%+v", dbinfo)))

	t := int64(0)
	finalmap := param.(map[string]interface{})

	if collect_type == "perf" {
		// we only generate time for perf collect
		// because time can be parsed from log
		time := m["time"].(int64)
		t = int64(time) / 1000
	}

	insinfo := &dao.InsInfo{
		LogicalInsName:  logical_ins_name,
		PhysicalInsName: physical_ins_name,
	}

	insdesc := &InsDesc{
		logical_ins_name:  logical_ins_name,
		physical_ins_name: physical_ins_name,
		host:              host,
		port:              port,
		role:              role,
		version:           version,
	}

	ins, err := dbmanager.GetOrInitInstance(m["datatype"].(string), insID, insinfo, dbinfo, initsqls)
	if err != nil {
		log.Error("[db_backend] get db fail",
			log.String("collecttype", collect_type),
			log.String("err", err.Error()))
		return err
	}

	for k, v := range finalmap {
		if _, ok := v.([]map[string]interface{}); !ok {
			log.Warn("[db_backend] final map key type wrong", log.String("key", k))
			continue
		}

		if err := storeCollectData(ins, insdesc,
			k, v.([]map[string]interface{}), t); err != nil {
			log.Error("[db_backend] insert content failed",
				log.String("ins name", ins.Name),
				log.String("datamodel", k),
				log.String("error", err.Error()))
		}
	}

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[db_backend] PluginExit, only stop specific instance")

	m, ok := ctx.(map[string]interface{})
	if !ok {
		return errors.New("[sqlite_backend] invalid ctx")
	}

	backendCtx, ok := m["backend"].(map[string]interface{})
	if !ok {
		return errors.New("[sqlite_backend] no backends ctx")
	}

	backendLocation := "remote"
	if datatypeConf, ok := backendCtx["datatype_conf"]; ok {
		if datatype, ok := datatypeConf.(map[string]DatatypeBackend)[m["datatype"].(string)]; ok {
			if m["backend_idx"].(int) < len(datatype.Backends) {
				backendLocation = datatype.Backends[m["backend_idx"].(int)]
			}
		}
	}

	logical_ins_name := m["logical_ins_name"].(string)
	port := m["backend_port"].(string)

	insID := logical_ins_name + "_" + port + "_" + backendLocation
	err := dbmanager.StopInstance(m["datatype"].(string), insID)
	if err != nil {
		log.Error("[db_backend] stop db ins dao failed",
			log.String("error", err.Error()),
			log.String("instance", insID))
		return err
	}

	log.Info("[db_backend] stop backend successfully", log.String("instance", insID))

	return nil
}

func initContext(content []byte, m map[string]interface{}) error {
	var backendConf DBBackendConf

	err := json.Unmarshal(content, &backendConf)
	if err != nil {
		log.Warn("[db_backend] unmarshal content failed", log.String("err", err.Error()))
		return err
	}

	backends := make(map[string]BackendsConf)
	for _, v := range backendConf.DbBackends.BackendsConf {
		backends[v.Name] = v
	}
	m["backends_conf"] = backends

	datatypes := make(map[string]DatatypeBackend)
	for _, v := range backendConf.DbBackends.DatatypeBackend {
		datatypes[v.Name] = v
	}
	m["datatype_conf"] = datatypes

	m["init_sqls"] = backendConf.InitSQLs

	if backendConf.DefaultRetentionInterval > 0 {
		dao.RETENTION_INTERVAL = backendConf.DefaultRetentionInterval
	}

	if backendConf.DefaultRetentionTime > 0 {
		dao.RETENTION_TIME = backendConf.DefaultRetentionTime
	}

	log.Debug("[db_backend] conf content", log.String("content", string(content)))

	return nil
}

func storeCollectData(ins *dao.Instance, insdesc *InsDesc,
	datamodel string, datacontent []map[string]interface{}, time int64) error {

	var data [][]interface{}
	var schema []string

	getScheme := false
	for _, ref := range datacontent {
		if !getScheme {
			for key, _ := range ref {
				if key == "time" {
					continue
				}

				schema = append(schema, key)
				getScheme = true
			}

			// make result ordered,
			// otherwise queryid may be distinct for the same insert sql in database
			sort.Strings(schema)
		}

		var rowdata []interface{}
		if len(schema) == 0 {
			log.Debug("[db_backend] empty result",
				log.String("ins name", ins.Name),
				log.String("data model", datamodel))
			continue
		}

		for _, rows := range schema {
			rowdata = append(rowdata, ref[rows])
		}

		rowdata = append(rowdata, insdesc.logical_ins_name)
		rowdata = append(rowdata, insdesc.physical_ins_name)
		rowdata = append(rowdata, insdesc.host)
		rowdata = append(rowdata, insdesc.port)
		rowdata = append(rowdata, insdesc.role)
		rowdata = append(rowdata, insdesc.version)

		if time == int64(0) {
			rowdata = append(rowdata, ref["time"].(int64))
		} else {
			rowdata = append(rowdata, time)
		}

		data = append(data, rowdata)
	}

	schema = append(schema, "dim_logical_ins_name")
	schema = append(schema, "dim_physical_ins_name")
	schema = append(schema, "dim_host")
	schema = append(schema, "dim_port")
	schema = append(schema, "dim_role")
	schema = append(schema, "dim_version")
	schema = append(schema, "time")

	err := ins.AsyncInsert(datamodel, schema, data)
	if err != nil {
		log.Error("[db_backend] table insert",
			log.String("ins name", ins.Name),
			log.String("err", err.Error()),
			log.String("datamodel", datamodel))
	}

	return err

}

func GetConfigMapValue(m map[string]interface{}, key string,
	valueType string, defValue interface{}) interface{} {

	if v, ok := m[key]; ok {
		value, ok := v.(string)
		if !ok {
			log.Debug("[db_backend] config value is not string",
				log.String("key", key), log.String("value", fmt.Sprintf("%+v", v)))
			return defValue
		}

		switch valueType {
		case "string":
			return value
		case "integer":
			if intv, err := strconv.Atoi(value); err == nil {
				return intv
			} else {
				log.Warn("[db_backend] config value type is not integer",
					log.String("key", key),
					log.String("error", err.Error()),
					log.String("value", fmt.Sprintf("%+v", intv)))
				return defValue
			}
		default:
			log.Debug("[db_backend] cannot recognize this value type",
				log.String("type", valueType))
			return defValue
		}
	}

	log.Debug("[db_backend] cannot get config map key", log.String("key", key))
	return defValue
}

func init() {
	sqlite_dir = utils.GetBasePath()
}
