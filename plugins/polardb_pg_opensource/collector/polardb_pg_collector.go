/*-------------------------------------------------------------------------
 *
 * polardb_pg_collector.go
 *    Polardb pg metrics collector
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
 *           plugins/polardb_pg_opensource/collector/polardb_pg_collector.go
 *-------------------------------------------------------------------------
 */
package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/db_config"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/polardb_pg_opensource/environment"

	_ "github.com/lib/pq"
)

const (
	PolarMinReleaseDate = int64(20190101)
	MinDBVersion        = -1 // means no limit
	MaxDBVersion        = -1 // means no limit
	CollectMinDBVersion = 90200
	CollectMaxDBVersion = MaxDBVersion
	DBConnTimeout       = 10
	DBQueryTimeout      = 60
	CollectTotalTimeout = 5
	DBPrepareTimeout    = 10
	DBBatchQueryTimeout = 60

	DefaultLocalDiskCollectInterval = 15
	DefaultPFSCollectInterval       = 1
	DBConfigCheckVersionInterval    = 5
	DefaultLCMForAllIntervals       = 60

	basePath    = "base_path"
	rwoDataPath = "rwo_data_path"

	KEY_SEND_TO_MULTIBACKEND = "send_to_multibackend"
	KEY_MULTIDIMENSION       = "multidimension"
	KEY_SYS_INFO             = "sysinfo"

	DefaultCollectLongIntervalThreshold = 600
	MIN_TRACKLOG_TIME                   = 500

	DB_STAT_ROUTINE_KEY       = "dbstat"
	RESOURCE_STAT_ROUTINE_KEY = "resource"
	SYSINFO_ROUTINE_KEY       = "sysinfo"

	Splitter = "^^^"

	INTERNAL_MARK          = "/* rds internal mark */"
	SET_CLIENT_MIN_MESSAGE = "SET log_min_messages=FATAL"
)

const (
	All     = 0
	RW      = 1
	RO      = 2
	StandBy = 3
	DataMax = 4
	User    = 5
)

var DefaultCollectCycle int64
var EL_REG = regexp.MustCompile(`\\$\\{.*?\\}`)

type DBInfo struct {
	username        string
	password        string
	database        string
	db              *sql.DB
	configdb        *sql.DB
	role            int
	extensions      []string
	applicationName string
	version         int64
	releaseDate     int64
	polarVersion    string
	configCenterdb  *sql.DB
	connections     map[string]*sql.DB
}

type CollectorInfo struct {
	querymap     map[string]db_config.QueryCtx
	intervalNano int64
	interval     int64
	cycle        int64

	localDiskCollectInterval int64

	timeNow                   time.Time
	count                     int64
	realTick                  int64 // count * interval
	enablePFS                 bool
	enableDBConfig            bool
	enableDBConfigReleaseDate string
	enableDBConfigCenter      bool
	dbConfigCenterHost        string
	dbConfigCenterPort        int
	dbConfigCenterUser        string
	dbConfigCenterPass        string
	dbConfigCenterDatabase    string

	dbConfigVersion       int64
	dbConfigCenterVersion int64

	dbConfigNeedUpdate bool
	dbConfigSchema     string
	dbSnapshotNo       int64
	dbNeedSnapshot     bool
	useFullOutdict     bool

	slowCollectionLogThreshold   int64
	longCollectIntervalThreshold int
	commandInfos                 []interface{}

	cacheout map[string]interface{}
}

type PreValue struct {
	LastTime int64
	Value    float64
}

type PolarDBPgCollector struct {
	DataDir          string
	Endpoint         string
	Port             int
	InsName          string
	InsId            int
	PhyInsId         int
	HostInsId        int
	HostInsIdStr     string
	HostInsIdStrList []string
	Role             string
	PolarVersion     string
	PolarReleaseDate string
	SystemIdentifier string
	UserName         string
	Database         string
	dbType           string
	environment      string
	isOnEcs          bool
	rawPre           map[string]float64 // raw value last time
	preValueMap      map[string]PreValue
	preNSValueMap    map[string]map[string]PreValue

	dbInfo *DBInfo
	cInfo  *CollectorInfo

	envCollector     environment.EnvironmentCollectorInterface
	logger           *log.PluginLogger
	enableOutDictLog bool
	dbConfig         *db_config.DBConfig

	DbPath      string
	ClusterName string
	Envs        map[string]string
	ConfigMap   map[string]interface{}

	configCollectQueryContext      []interface{}
	configCollectInitContext       []interface{}
	configCollectConfigInitContext []interface{}

	mutex                        *sync.Mutex
	asyncCollectionRunChannel    map[string]chan int
	asyncCollectionResultChannel map[string]chan error
}

func New() *PolarDBPgCollector {
	c := &PolarDBPgCollector{
		asyncCollectionResultChannel: make(map[string]chan error),
		asyncCollectionRunChannel:    make(map[string]chan int),
		cInfo:                        &CollectorInfo{},
		dbInfo:                       &DBInfo{},
		dbConfig:                     db_config.NewDBConfig(),
		logger:                       log.NewPluginLogger("polardb_pg", nil),
		mutex:                        &sync.Mutex{},
	}

	c.asyncCollectionRunChannel[RESOURCE_STAT_ROUTINE_KEY] = make(chan int, 1)
	c.asyncCollectionRunChannel[DB_STAT_ROUTINE_KEY] = make(chan int, 1)
	c.asyncCollectionRunChannel[SYSINFO_ROUTINE_KEY] = make(chan int, 1)
	c.asyncCollectionResultChannel[RESOURCE_STAT_ROUTINE_KEY] = make(chan error, 1)
	c.asyncCollectionResultChannel[DB_STAT_ROUTINE_KEY] = make(chan error, 1)
	c.asyncCollectionResultChannel[SYSINFO_ROUTINE_KEY] = make(chan error, 1)

	return c
}

func (c *PolarDBPgCollector) getDataDir(basepath string, env map[string]string) string {
	if path, ok := env["host-polardata-dir"]; ok {
		// for polardb on dbstack: local fs mode
		if path != "" {
			return env["host-polardata-dir"]
		}
	}

	if path, ok := env["host_polardata_dir"]; ok {
		if path != "" {
			return env["host_polardata_dir"]
		}
	}

	if path, ok := env["host_data_dir"]; ok {
		if path != "" {
			return env["host_data_dir"]
		}
	}

	if path, ok := env["host-data-dir"]; ok {
		if path != "" {
			return "/" + strings.Replace(env["host-data-dir"], ".", "/", 5)
		}
	}

	return fmt.Sprintf("%s/%d/data", basepath, c.HostInsId)
}

func parseEL(path string, envs map[string]string) string {
	path = EL_REG.ReplaceAllStringFunc(path, func(part string) string {
		return envs[part[2:len(part)-1]]
	})
	return path
}

func (c *PolarDBPgCollector) initEndpoint(
	m map[string]interface{}, envs map[string]string) string {
	// for polarbox
	if host, ok := envs["annotation.polarbox.ppas.ipAddress"]; ok {
		return host
	}
	if envs["apsara.ins.cluster.mode"] == "WriteReadMore" ||
		envs["apsara.ins.cluster.mode"] == "LocalEnterprise" {
		c.DbPath = fmt.Sprintf("%s/%s", m[rwoDataPath].(string), envs["apsara.metric.ins_id"])
		if _, ok := envs["host_data_dir"]; ok {
			return envs["host_data_dir"]
		}

		if _, ok := envs["host-data-dir"]; ok {
			return envs["host-data-dir"]
		}
	}

	hostVal := m["host"]
	if hostVal != nil {
		if host, ok := m["host"].(string); ok {
			return host
		}
	}
	if _, ok := envs["apsara.metric.ins_name"]; ok {
		return fmt.Sprintf("%s/data", c.DbPath)
	} else {
		return c.DbPath + "/"
	}
}

func (c *PolarDBPgCollector) initHostInsIdStrList() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				c.HostInsIdStrList = append(c.HostInsIdStrList,
					fmt.Sprintf("%s:%d", ipnet.IP.String(), c.Port))
			}
		}
	}

	c.logger.Info("init endpoint list",
		log.String("endpoint list", fmt.Sprintf("%+v", c.HostInsIdStrList)))
}

func (c *PolarDBPgCollector) initLogger(m map[string]interface{}) {
	logName := c.getMapValue(m, "logger_name", "polardb_pg").(string)
	logIdentifier := map[string]string{
		"ins_name": c.InsName,
		"port":     strconv.Itoa(c.Port),
	}

	c.logger.SetTag(logName)
	c.logger.SetIdentifier(logIdentifier)
}

func (c *PolarDBPgCollector) initParams(m map[string]interface{}) error {
	c.dbType = m["dbtype"].(string)
	c.environment = m["environment"].(string)

	envs := m["env"].(map[string]string)
	c.Envs = envs

	c.enableOutDictLog = c.getMapValue(m, "enable_outdict_log", false).(bool)
	c.ConfigMap =
		c.getMapValue(m, "configmap", make(map[string]interface{})).(map[string]interface{})

	// flags
	c.cInfo.interval = int64(m[consts.PluginIntervalKey].(int))
	c.cInfo.enablePFS = m["enable_pfs"].(bool)
	c.cInfo.enableDBConfig = c.getMapValue(m, "enable_dbconfig", false).(bool)
	c.cInfo.enableDBConfigReleaseDate =
		c.getMapValue(m, "enable_dbconfig_releasedate", "20211230").(string)
	c.cInfo.enableDBConfigCenter =
		c.getMapValue(m, "enable_dbconfig_center", false).(bool)
	c.cInfo.dbConfigCenterHost = c.getMapValue(m, "dbconfig_center_host", "").(string)
	c.cInfo.dbConfigCenterPort =
		int(c.getMapValue(m, "dbconfig_center_port", float64(5432)).(float64))
	c.cInfo.dbConfigCenterUser = c.getMapValue(m, "dbconfig_center_user", "postgres").(string)
	c.cInfo.dbConfigCenterPass = c.getMapValue(m, "dbconfig_center_pass", "").(string)
	c.cInfo.dbConfigCenterDatabase =
		c.getMapValue(m, "dbconfig_center_database", "postgres").(string)
	c.cInfo.slowCollectionLogThreshold =
		int64(c.getMapValue(m, "slow_collection_log_thres", float64(MIN_TRACKLOG_TIME)).(float64))
	c.cInfo.useFullOutdict = c.getMapValue(m, "use_full_outdict", false).(bool)

	c.cInfo.dbConfigSchema = c.getMapValue(m, "dbconfig_schema", "polar_gawr_collection").(string)
	c.cInfo.commandInfos = c.getMapValue(m, "commands", make([]interface{}, 0)).([]interface{})
	c.cInfo.longCollectIntervalThreshold = int(c.getMapValue(m, "long_collect_interval_threshold",
		float64(DefaultCollectLongIntervalThreshold)).(float64))
	if c.environment == "polarbox" {
		if envs["apsara.ins.cluster.mode"] == "WriteReadMore" {
			c.cInfo.enablePFS = true
		} else {
			c.cInfo.enablePFS = false
		}
	}
	c.isOnEcs = m["is_on_ecs"].(bool)

	// env & dbtype related params
	if c.environment == "polarbox" || c.environment == "enterprise" {
		c.DbPath = parseEL(m[basePath].(string), envs)
		c.ClusterName = envs["apsara.metric.clusterName"]
	}

	c.HostInsIdStrList = make([]string, 0)
	c.Endpoint = c.initEndpoint(m, envs)
	if _, ok := envs["apsara.metric.ins_name"]; c.environment != "dbstack" && ok {
		// in docker
		c.InsId, _ = strconv.Atoi(envs["apsara.metric.logic_custins_id"])
		c.HostInsId, _ = strconv.Atoi(envs["apsara.metric.ins_id"])
		c.PhyInsId, _ = strconv.Atoi(envs["apsara.metric.physical_custins_id"])
		c.HostInsIdStr = envs["apsara.metric.ins_id"]
		c.logger.Info("All IDs",
			log.Int("ins id:", c.InsId),
			log.Int("host ins id:", c.HostInsId),
			log.Int("phy ins id:", c.PhyInsId))
		c.DataDir = c.getDataDir(m[basePath].(string), envs)
		if c.environment == "public_cloud" {
			c.Endpoint = c.DataDir
		}
		c.logger.Info("endpoint", log.String("endpoint", c.Endpoint))
	} else {
		// rds pg
		c.InsId, _ = strconv.Atoi(m[consts.PluginContextKeyInsId].(string))
		c.DataDir = c.getDataDir(m[basePath].(string), envs)
		if c.environment == "dbstack" {
			c.initHostInsIdStrList()
		} else {
			c.HostInsIdStr = envs["host_ins_id"]
			if c.environment == "software" {
				if socketpath, ok := envs["socket_path"]; ok {
					c.Endpoint = socketpath
				} else {
					c.Endpoint = fmt.Sprintf("%s/", m[basePath].(string))
				}
			} else {
				c.Endpoint = fmt.Sprintf("%s/", m[basePath].(string))
			}
		}
	}

	m["datadir"] = c.DataDir

	// collect info
	c.cInfo.intervalNano = int64(0)
	if _, ok := m["local_disk_collect_interval"]; ok {
		c.cInfo.localDiskCollectInterval = int64(m["local_disk_collect_interval"].(float64))
	} else {
		c.cInfo.localDiskCollectInterval = DefaultLocalDiskCollectInterval
	}
	c.cInfo.count = 0
	c.cInfo.realTick = 0
	c.cInfo.cacheout = make(map[string]interface{})
	c.cInfo.cycle = int64(m["cycle"].(float64))

	// other init
	DefaultCollectCycle = c.cInfo.cycle
	c.rawPre = make(map[string]float64, 1024)
	c.preValueMap = make(map[string]PreValue, 1024)
	c.preNSValueMap = make(map[string]map[string]PreValue)

	c.logger.Info("init params done",
		log.String("c", fmt.Sprintf("%+v", c)),
		log.String("cInfo", fmt.Sprintf("%+v", c.cInfo)))
	return nil
}

func (c *PolarDBPgCollector) initDBInfo(m map[string]interface{}) error {
	if err := c.initCollectDB(m); err != nil {
		c.logger.Error("init collect db failed", err)
		return err
	}

	if err := c.initConfigDB(m); err != nil {
		c.logger.Error("init config db failed", err)
		return err
	}

	if c.cInfo.enableDBConfigCenter {
		if err := c.initConfigCenterDB(m); err != nil {
			c.logger.Error("init config center db failed", err)
			return err
		}
	}

	return nil
}

func (c *PolarDBPgCollector) initCollectDB(m map[string]interface{}) error {
	var err error

	if _, ok := c.Envs[consts.PluginContextKeyUserName]; ok {
		c.dbInfo.username = c.Envs[consts.PluginContextKeyUserName]
		c.dbInfo.database = c.Envs[consts.PluginContextKeyDatabase]
	} else if _, ok = m[consts.PluginContextKeyUserName]; ok {
		c.dbInfo.username = m[consts.PluginContextKeyUserName].(string)
		c.dbInfo.database = m[consts.PluginContextKeyDatabase].(string)
	} else {
		c.dbInfo.username = "postgres"
		c.dbInfo.database = "postgres"
	}

	c.UserName = c.dbInfo.username
	c.Database = c.dbInfo.database
	c.dbInfo.password = m[consts.PluginContextKeyPassword].(string)
	c.dbInfo.applicationName = m["application_name"].(string)
	c.dbInfo.extensions = make([]string, 0)
	for _, extension := range m["extensions"].([]interface{}) {
		c.dbInfo.extensions = append(c.dbInfo.extensions, extension.(string))
	}

	dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d fallback_application_name=%s "+
		"sslmode=disable connect_timeout=%d",
		c.Endpoint, c.dbInfo.username, c.dbInfo.database, c.Port, c.dbInfo.applicationName,
		DBConnTimeout)

	c.dbInfo.connections = make(map[string]*sql.DB)
	if c.dbInfo.db, err = sql.Open("postgres", dbUrl); err != nil {
		c.logger.Error("connect to db failed", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()
	if err = c.dbInfo.db.PingContext(ctx); err != nil {
		c.logger.Error("ping db failed", err)
		return err
	}

	c.dbInfo.db.Exec("SET log_min_messages=FATAL")
	c.dbInfo.db.Exec("SET polar_comp_redwood_greatest_least=off")

	if c.dbInfo.version, err =
		c.getDBVersion(); err != nil {
		c.logger.Error("get db version failed", err)
		return err
	}

	c.dbInfo.releaseDate = PolarMinReleaseDate
	if c.isPolarDB() {
		if enablePFS, err := c.getEnablePFS(); err != nil {
			c.logger.Warn("get pfs mode failed", err)
		} else {
			c.cInfo.enablePFS = enablePFS
		}

		if c.dbInfo.releaseDate, err =
			c.getPolarReleaseDate(); err != nil {
			c.logger.Warn("get polardb release date failed", err)
			c.dbInfo.releaseDate = PolarMinReleaseDate
		}
		c.PolarReleaseDate = strconv.FormatInt(c.dbInfo.releaseDate, 10)

		if c.PolarReleaseDate < c.cInfo.enableDBConfigReleaseDate {
			c.logger.Info("db config is disabled for this release_date",
				log.String("polar_release_date", c.PolarReleaseDate),
				log.String("dbconfig_release_date", c.cInfo.enableDBConfigReleaseDate))
			c.cInfo.enableDBConfig = false
		}

		if c.cInfo.enableDBConfig {
			c.logger.Info("db config is enable for this release_date",
				log.String("polar_release_date", c.PolarReleaseDate))
		}

		if c.dbInfo.polarVersion, err = c.getValueFromDB("SHOW polar_version"); err != nil {
			c.logger.Warn("get polar version failed", err)
		}
		c.PolarVersion = c.dbInfo.polarVersion
	}

	if c.dbInfo.role, err = c.getDBRole(); err != nil {
		c.logger.Error("get db role failed.", err)
		return err
	}

	if c.SystemIdentifier, err =
		c.getValueFromDB("SELECT system_identifier FROM pg_control_system()"); err != nil {
		c.logger.Warn("get system identifier from db failed", err)
	}

	if c.dbInfo.role == RW {
		if err = c.prepareExtensions(); err != nil {
			c.logger.Error("prepare db collect failed.", err)
		}

		if err = c.prepareObjects(); err != nil {
			c.logger.Warn("prepare db object failed", err)
		}
	}

	return nil
}

func (c *PolarDBPgCollector) initConfigDB(m map[string]interface{}) error {
	var err error

	configdb := "postgres"
	if _, ok := m["configdb"]; ok {
		configdb = m["configdb"].(string)
	}

	if c.dbInfo.database != configdb {
		configDBUrl := fmt.Sprintf("host=%s user=%s dbname=postgres port=%d "+
			"fallback_application_name=%s sslmode=disable connect_timeout=%d",
			c.Endpoint, c.dbInfo.username, c.Port, "ue_configdb", DBConnTimeout)

		if c.dbInfo.configdb, err = sql.Open("postgres", configDBUrl); err != nil {
			c.logger.Warn("connect to config db failed, use origin database for config", err,
				log.String("database", c.dbInfo.database))
			c.dbInfo.configdb = c.dbInfo.db
		} else {
			c.dbInfo.configdb.Exec("SET log_min_messages=FATAL")
			c.logger.Info("use configdb", log.String("database", configdb))
		}
	} else {
		c.logger.Info("use origin database for config", log.String("database", c.dbInfo.database))
		c.dbInfo.configdb = c.dbInfo.db
	}

	return nil

}

func (c *PolarDBPgCollector) initConfigCenterDB(m map[string]interface{}) error {
	var err error

	configCenterDBURL := fmt.Sprintf("host=%s user=%s dbname=%s port=%d password=%s "+
		"fallback_application_name=%s sslmode=disable connect_timeout=%d",
		c.cInfo.dbConfigCenterHost,
		c.cInfo.dbConfigCenterUser,
		c.cInfo.dbConfigCenterDatabase,
		c.cInfo.dbConfigCenterPort,
		c.cInfo.dbConfigCenterPass,
		"ue_configcenter",
		DBConnTimeout)

	if c.dbInfo.configCenterdb, err = sql.Open("postgres", configCenterDBURL); err != nil {
		c.logger.Warn("connect to config center db failed", err,
			log.String("database", c.cInfo.dbConfigCenterDatabase))
	} else {
		c.dbInfo.configCenterdb.SetMaxIdleConns(1)
		c.dbInfo.configCenterdb.Exec("SET log_min_messages=FATAL")
		c.logger.Info("connect config center db success", log.String("url", configCenterDBURL))
	}

	return nil
}

func (c *PolarDBPgCollector) initQueries(queryContexts []interface{}) error {
	defaultQueryCtx := db_config.QueryCtx{
		Name:               "Default",
		Query:              "",
		DataModel:          "dbmetrics",
		MinVersion:         CollectMinDBVersion,
		MaxVersion:         CollectMaxDBVersion,
		PolarReleaseDate:   PolarMinReleaseDate,
		DBRole:             All,
		Cycle:              DefaultCollectCycle,
		SendToMultiBackend: 0,
		Enable:             1,
		QueryAllDB:         false,
		Offset:             0,
		WriteIntoLog:       0,
	}

	c.cInfo.querymap = make(map[string]db_config.QueryCtx)
	for i, query := range queryContexts {
		queryCtx := defaultQueryCtx
		queryMap := query.(map[string]interface{})
		confStr, err := json.Marshal(queryMap)
		if err != nil {
			c.logger.Warn("marshal query conf to string failed.", err)
			continue
		}

		if err = json.Unmarshal(confStr, &queryCtx); err != nil {
			c.logger.Warn("unmarshal conf string failed", err)
			continue
		}

		if queryCtx.Enable == 0 {
			c.logger.Info("sql is disabled", log.String("name", queryCtx.Name))
		}

		if queryCtx.MinVersion != MinDBVersion && c.dbInfo.version < queryCtx.MinVersion {
			c.logger.Info("instance version is too low",
				log.Int64("conf_version", queryCtx.MinVersion),
				log.Int64("ins_version", c.dbInfo.version),
				log.String("name", queryCtx.Name))
			queryCtx.Enable = 0
		}

		if queryCtx.MaxVersion != MaxDBVersion && c.dbInfo.version > queryCtx.MaxVersion {
			c.logger.Info("instance version is high",
				log.Int64("conf_version", queryCtx.MaxVersion),
				log.Int64("ins_version", c.dbInfo.version),
				log.String("name", queryCtx.Name))
			queryCtx.Enable = 0
		}

		// queryCtx.Cycle = queryCtx.Cycle / c.cInfo.interval
		// if queryCtx.Cycle == 0 {
		// 	queryCtx.Cycle = int64(1)
		// }

		if c.isPolarDB() {
			if c.dbInfo.releaseDate < queryCtx.PolarReleaseDate {
				c.logger.Info("instance release date is older",
					log.Int64("conf_release_date", queryCtx.PolarReleaseDate),
					log.Int64("ins_release_date", c.dbInfo.releaseDate),
					log.String("name", queryCtx.Name))
				queryCtx.Enable = 0
			}
		}

		if c.dbInfo.role == DataMax {
			c.logger.Info("this db is DataMax, would not execute any sql")
			queryCtx.Enable = 0
		}

		if queryCtx.DBRole != All && queryCtx.DBRole != User && queryCtx.DBRole != c.dbInfo.role {
			c.logger.Info("conf dbrole is not match to instance dbrole",
				log.Int("conf_role", int(queryCtx.DBRole)),
				log.Int("ins_role", int(c.dbInfo.role)),
				log.String("name", queryCtx.Name))
			queryCtx.Enable = 0
		}

		if queryCtx.DBRole == User {
			if !(c.dbInfo.role == RW || c.dbInfo.role == RO) {
				c.logger.Info("db role is not RW or RO",
					log.Int("conf_role", int(queryCtx.DBRole)),
					log.Int("ins_role", int(c.dbInfo.role)),
					log.String("name", queryCtx.Name))
				queryCtx.Enable = 0
			}
		}

		if queryCtx.Name == "Default" {
			queryCtx.Name = strconv.Itoa(i)
			c.logger.Warn("query ctx does not have a name, "+
				"this may cause problem when using db config, "+
				"we use index as its name here",
				errors.New("query ctx does not have a name"),
				log.String("query", fmt.Sprintf("%+v", queryCtx)))
		}
		c.cInfo.querymap[queryCtx.Name] = queryCtx
	}

	if c.cInfo.enableDBConfig {
		var err error
		if c.dbInfo.role != DataMax {
			if err = c.dbConfig.InitFromDBConfig(c.ConfigMap,
				c.cInfo.querymap, c.dbInfo.role == RW); err != nil {
				if c.dbInfo.role != RW {
					c.cInfo.dbConfigVersion = -1
					c.logger.Info("we may need to wait for RW init")
				} else {
					c.logger.Error("init from db config failed", err)
					return err
				}
			}

			if err == nil {
				if c.cInfo.dbConfigVersion, err = c.dbConfig.GetDBConfigVersion(); err != nil {
					c.logger.Error("init from db config version failed", err)
					return err
				}

				if c.cInfo.enableDBConfigCenter {
					if c.cInfo.dbConfigCenterVersion, err =
						c.dbConfig.GetConfigCenterDBConfigVersion(); err != nil {
						c.logger.Warn("init from config center db config version failed", err)
					}
				}

				if c.cInfo.dbSnapshotNo, err = c.dbConfig.GetDBSnapshotNo(); err != nil {
					c.logger.Error("init db snapshot no failed", err)
					return err
				}
			}
		}

		c.cInfo.dbConfigNeedUpdate = false
		c.cInfo.dbNeedSnapshot = false
	}

	for _, qctx := range c.cInfo.querymap {
		c.logger.Debug("query context",
			log.String("name", qctx.Name),
			log.String("context", fmt.Sprintf("%+v", qctx)))
	}

	return nil
}

func (c *PolarDBPgCollector) initDBCollector(m map[string]interface{}) error {
	var err error

	c.configCollectInitContext = c.getMapValue(m, "collect_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectConfigInitContext = c.getMapValue(m, "config_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectQueryContext = c.getMapValue(m, "queries",
		make([]interface{}, 0)).([]interface{})

	if err = c.initDBInfo(m); err != nil {
		c.logger.Error("prepare db collector failed.", err)
		return err
	}

	if c.cInfo.enableDBConfig {
		err = c.dbConfig.Init(c.InsName, "polardb_pg",
			c.cInfo.dbConfigSchema,
			c.dbInfo.configdb,
			c.dbInfo.configCenterdb,
			c.configCollectConfigInitContext,
			c.logger)
		if err != nil {
			c.logger.Error("init db config failed", err)
			return err
		}
	}

	if err = c.initQueries(c.configCollectQueryContext); err != nil {
		c.logger.Warn("init collect queries failed", err)
		return err
	}

	c.logger.Debug("ConfigMap", log.String("configmap", fmt.Sprintf("%+v", c.ConfigMap)))

	return nil
}

func (c *PolarDBPgCollector) initResourceCollector(m map[string]interface{}) error {
	if c.getConfigMapValue(c.ConfigMap, "enable_db_resource_collect", "integer", 1).(int) == 0 {
		c.logger.Info("not initialize resource collection")
		return nil
	}

	c.envCollector = environment.CreateEnvironmentCollector(m, c.logger)
	if err := c.envCollector.Init(m, c.logger); err != nil {
		c.logger.Warn("environment collector init failed", err)
		return err
	}

	return nil
}

func (c *PolarDBPgCollector) initMetaService() error {
	metaService := meta.GetMetaService()
	metaService.SetInterfaceMap("configmap", strconv.Itoa(c.Port), c.ConfigMap)
	metaService.SetStringMap("envs", strconv.Itoa(c.Port), c.Envs)
	metaService.SetString("polar_release_date", strconv.Itoa(c.Port), c.PolarReleaseDate)
	metaService.SetString("polar_version", strconv.Itoa(c.Port), c.PolarVersion)
	metaService.SetString("username", strconv.Itoa(c.Port), c.UserName)
	metaService.SetString("endpoint", strconv.Itoa(c.Port), c.Endpoint)
	metaService.SetString("role", strconv.Itoa(c.Port), c.Role)

	return nil
}

func (c *PolarDBPgCollector) Init(m map[string]interface{}) error {
	var err error

	c.InsName = m[consts.PluginContextKeyInsName].(string)
	c.Port = m[consts.PluginContextKeyPort].(int)

	c.initLogger(m)
	c.logger.Info("init with info.", log.String("info", fmt.Sprintf("%+v", m)))

	if err = c.initParams(m); err != nil {
		c.logger.Error("init param failed", err)
		goto ERROR
	}

	if err = c.initDBCollector(m); err != nil {
		c.logger.Error("init collector failed", err)
		goto ERROR
	}

	if err = c.initResourceCollector(m); err != nil {
		c.logger.Error("init resource collector failed", err)
		goto ERROR
	}

	if err = c.initMetaService(); err != nil {
		c.logger.Error("init meta service failed", err)
		goto ERROR
	}

ERROR:
	if err != nil {
		c.logger.Error("init error, we must release all resources here", err)
		if c.dbInfo.db != nil {
			c.dbInfo.db.Close()
		}

		if c.dbInfo.configdb != nil {
			c.dbInfo.configdb.Close()
			c.dbInfo.configdb = nil
		}

		if c.dbInfo.configCenterdb != nil {
			c.dbInfo.configCenterdb.Close()
			c.dbInfo.configCenterdb = nil
		}

		return err
	}

	return nil
}

func (c *PolarDBPgCollector) prepareExtensions() error {
	ctx, cancel := context.WithTimeout(context.Background(), DBPrepareTimeout*time.Second)
	defer cancel()

	for _, extension := range c.dbInfo.extensions {
		// not using ALTER EXTENSION here because some extensions are not backward compatibility
		err := c.execDBCtx(ctx, fmt.Sprintf("DROP EXTENSION IF EXISTS %s CASCADE", extension))
		if err != nil {
			c.logger.Warn("drop extension failed.", err, log.String("extension", extension))
		}

		err = c.execDBCtx(ctx, fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s CASCADE", extension))
		if err != nil {
			c.logger.Warn("create extension failed.", err, log.String("extension", extension))
		}
	}

	return nil
}

func (c *PolarDBPgCollector) prepareObjects() error {
	ctx, cancel := context.WithTimeout(context.Background(), DBPrepareTimeout*time.Second)
	defer cancel()

	defaultQueryCtx := db_config.QueryCtx{
		Name:             "Default",
		Query:            "",
		MinVersion:       CollectMinDBVersion,
		MaxVersion:       CollectMaxDBVersion,
		PolarReleaseDate: PolarMinReleaseDate,
		Cycle:            1,
		Offset:           0,
		Enable:           1,
		DataModel:        "default_data_model",
		QueryAllDB:       false,
		OriginQuery:      "",
	}

	for _, query := range c.configCollectInitContext {
		queryCtx := defaultQueryCtx
		queryMap := query.(map[string]interface{})
		confStr, err := json.Marshal(queryMap)
		if err != nil {
			c.logger.Warn("marshal query conf to string failed", err,
				log.String("query", fmt.Sprintf("%+v", queryMap)))
			continue
		}

		err = json.Unmarshal(confStr, &queryCtx)
		if err != nil {
			c.logger.Warn("unmarshal conf string failed", err,
				log.String("query", fmt.Sprintf("%+v", queryMap)))
			continue
		}

		if queryCtx.Enable == 0 {
			c.logger.Info("sql is disabled", log.String("query", queryCtx.Query))
			continue
		}

		if err := c.execDBCtx(ctx, queryCtx.Query); err != nil {
			c.logger.Warn("exec collect init sql failed", err, log.String("query", queryCtx.Query))
		} else {
			c.logger.Info("exec collect init sql succeed", log.String("query", queryCtx.Query))
		}
	}

	return nil
}

func (c *PolarDBPgCollector) getDBRole() (int, error) {
	var inRecoveryMode bool
	var err error

	if inRecoveryMode, err = c.isInRecoveryMode(); err != nil {
		c.Role = "Standby"
		c.logger.Error("check with recovery mode failed.", err)
		return StandBy, err
	}

	if nodetype, err := c.getValueFromDB("SELECT * FROM polar_node_type()"); err == nil {
		switch nodetype {
		case "master":
			if inRecoveryMode {
				c.Role = "RO"
				return RO, nil
			} else {
				c.Role = "RW"
				return RW, nil
			}
		case "replica":
			c.Role = "RO"
			return RO, nil
		case "standby":
			c.Role = "Standby"
			return StandBy, nil
		case "standalone_datamax":
			c.Role = "Logger"
			return DataMax, nil
		}
	}

	if inRecoveryMode {
		c.Role = "RO"
		return RO, nil
	}

	c.Role = "RW"
	return RW, nil
}

func (c *PolarDBPgCollector) isInRecoveryMode() (bool, error) {
	v, err := c.getValueFromDB("SELECT pg_is_in_recovery()")
	if err != nil {
		c.logger.Error("get recovery mode failed", err)
		return true, err
	}

	if v == "false" {
		return false, nil
	}

	return true, nil
}

func (c *PolarDBPgCollector) getDBVersion() (int64, error) {
	v, err := c.getValueFromDB("SHOW server_version_num")
	if err != nil {
		c.logger.Error("get db version failed", err)
		return 0, err
	}

	version, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		c.logger.Error("parse db version value to int64 failed", err)
		return 0, err
	}

	c.logger.Info("DB Info", log.Int64("Postgres Version", version))

	return version, nil
}

func (c *PolarDBPgCollector) getEnablePFS() (bool, error) {
	v, err := c.getValueFromDB("SHOW polar_vfs.localfs_mode")
	if err != nil {
		c.logger.Error("get pfs enable flag failed", err)
		return false, err
	}

	if v == "on" {
		return false, nil
	}

	return true, nil
}

func (c *PolarDBPgCollector) getPolarReleaseDate() (int64, error) {
	v, err := c.getValueFromDB("SHOW polar_release_date")
	if err != nil {
		c.logger.Error("get polar release date failed", err)
		return 0, err
	}

	if len(v) < 8 {
		err := errors.New("version value length is invalid")
		c.logger.Error("version value length is invalid", err,
			log.String("version value", v))
		return 0, err
	}

	value := v[0:8]
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		c.logger.Error("parse version value failed", err, log.String("version", value))
		return 0, err
	}

	c.logger.Info("DB Info", log.Int64("Polar Release Date", version))

	return version, nil
}

func (c *PolarDBPgCollector) getValueFromDB(query string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := c.queryDBCtx(ctx, query)
	if err != nil {
		c.logger.Error("query db failed", err, log.String("sql", query))
		return "", err
	}
	defer rows.Close()

	var value string

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return "", err
		}

		c.logger.Debug("get db result", log.String("query", query), log.String("value", value))

		return value, nil
	}

	return "", errors.New("cannot get anything from db")
}

func (c *PolarDBPgCollector) checkIfNeedRestart() error {
	// when role changed, we need to restart
	if dbrole, err := c.getDBRole(); err != nil {
		c.logger.Warn("get db type when collect failed.", err)
	} else {
		if dbrole != c.dbInfo.role {
			c.logger.Info("db role changed",
				log.Int("old role", c.dbInfo.role), log.Int("new role", dbrole))
			return errors.New("db role changed, we must reinit the plugin")
		}
	}

	if c.dbInfo.role == DataMax {
		return nil
	}

	if c.cInfo.dbConfigNeedUpdate {
		c.logger.Info("db config version update")
		return errors.New("db config version update, we must reinit to reload")
	}

	if c.cInfo.realTick%DBConfigCheckVersionInterval != 0 {
		return nil
	}

	// when config version changed, we need to reload config
	if !c.cInfo.enableDBConfig {
		return nil
	}

	version, err := c.dbConfig.GetDBConfigVersion()
	if err != nil {
		c.logger.Warn("get db config version failed", err)
		return nil
	}

	if version > c.cInfo.dbConfigVersion {
		c.cInfo.dbConfigNeedUpdate = true
		if err = c.initQueries(c.configCollectQueryContext); err != nil {
			c.logger.Warn("reinit queries failed", err,
				log.Int64("old version", c.cInfo.dbConfigVersion),
				log.Int64("new version", version))
			return err
		}
		if err = c.initMetaService(); err != nil {
			c.logger.Warn("reinit meta service failed", err)
			return err
		}
		c.cInfo.dbConfigNeedUpdate = false
		c.logger.Info("db config version update",
			log.Int64("old version", c.cInfo.dbConfigVersion),
			log.Int64("new version", version))
		c.cInfo.dbConfigVersion = version
	}

	if c.cInfo.enableDBConfigCenter {
		version, err = c.dbConfig.GetConfigCenterDBConfigVersion()
		if err != nil {
			c.logger.Warn("get db config center version failed", err)
			return nil
		}

		if version > c.cInfo.dbConfigCenterVersion {
			c.cInfo.dbConfigNeedUpdate = true
			if err = c.initQueries(c.configCollectQueryContext); err != nil {
				c.logger.Warn("reinit queries failed", err,
					log.Int64("old version", c.cInfo.dbConfigVersion),
					log.Int64("new version", version))
				return err
			}
			if err = c.initMetaService(); err != nil {
				c.logger.Warn("reinit meta service failed", err)
				return err
			}
			c.cInfo.dbConfigNeedUpdate = false
			c.logger.Info("db config center version update",
				log.Int64("old version", c.cInfo.dbConfigCenterVersion),
				log.Int64("new version", version))
			c.cInfo.dbConfigCenterVersion = version
		}
	}

	snapshotno, err := c.dbConfig.GetDBSnapshotNo()
	if err != nil {
		c.logger.Warn("get db snapshot failed", err)
		return nil
	}

	if snapshotno > c.cInfo.dbSnapshotNo {
		c.cInfo.dbNeedSnapshot = true
		c.cInfo.dbSnapshotNo = snapshotno
		c.logger.Info("db snapshot needed", log.Int64("snapshot no", c.cInfo.dbSnapshotNo))
	}

	return nil
}

func (c *PolarDBPgCollector) Collect(out map[string]interface{}) error {
	now := time.Now()
	// not right in first time
	c.cInfo.intervalNano = now.UnixNano() - c.cInfo.timeNow.UnixNano()

	c.cInfo.timeNow = now
	c.cInfo.count += 1
	c.cInfo.realTick += c.cInfo.interval

	if c.dbInfo.db == nil {
		c.logger.Warn("db is nil", fmt.Errorf("db is nil"))
		return nil
	}

	if err := c.asyncCollect(out); err != nil {
		c.logger.Warn("async collection failed", err)
		return err
	}

	c.cacheCollectResult(out)
	c.prepareCollectResult(out)

	out["collect_timestamp"] = now.Unix()
	if c.enableOutDictLog {
		c.logger.Info("out data dict",
			log.Int64("count", c.cInfo.count),
			log.String("out", fmt.Sprintf("%+v", out)))
	}

	return nil
}

func (c *PolarDBPgCollector) asyncCollect(out map[string]interface{}) error {
	out1 := make(map[string]interface{})
	go c.asyncCollector(DB_STAT_ROUTINE_KEY, out1, func(out map[string]interface{}) error {
		if err := c.checkIfNeedRestart(); err != nil {
			c.logger.Info("we need to restart", log.String("info", err.Error()))
			return err
		}

		if c.getConfigMapValue(c.ConfigMap, "enable_db_metric_collect", "integer", 1).(int) == 1 {
			c.dbInfo.db.Exec("SET log_min_messages=FATAL")

			if err := c.collectDBStat(out1); err != nil {
				c.logger.Warn("collect db stat failed.", err)
			}
		}

		return nil
	})

	out2 := make(map[string]interface{})
	go c.asyncCollector(RESOURCE_STAT_ROUTINE_KEY, out2, func(out map[string]interface{}) error {
		if c.getConfigMapValue(c.ConfigMap, "enable_db_resource_collect", "integer", 1).(int) == 1 {
			if err := c.envCollector.Collect(out2); err != nil {
				c.logger.Warn("collect resource stat failed.", err)
			}
		}

		return nil
	})

	out3 := make(map[string]interface{})
	go c.asyncCollector(SYSINFO_ROUTINE_KEY, out3, func(out map[string]interface{}) error {
		if c.getConfigMapValue(c.ConfigMap, "enable_db_sysinfo_collect", "integer", 1).(int) == 1 {
			if !(c.cInfo.count == 5 || c.cInfo.realTick%3600 == 10) {
				return nil
			}

			if err := c.collectSysInfo(out3); err != nil {
				c.logger.Warn("collect sys info failed", err)
			}
		}

		return nil
	})

	// get result
	var collectionErr error
	dbstatCollectDone := false
	resourceCollectDone := false
	sysinfoCollectDone := false
	timeout := false
	for {
		select {
		case err, ok := <-c.asyncCollectionResultChannel[DB_STAT_ROUTINE_KEY]:
			if !ok {
				c.logger.Info("collector has been stopped",
					log.String("channel key", DB_STAT_ROUTINE_KEY))
			} else {
				if err != nil {
					collectionErr = err
				}
			}
			dbstatCollectDone = true
		case err, ok := <-c.asyncCollectionResultChannel[RESOURCE_STAT_ROUTINE_KEY]:
			if !ok {
				c.logger.Info("collector has been stopped",
					log.String("channel key", RESOURCE_STAT_ROUTINE_KEY))
			} else {
				if err != nil {
					collectionErr = err
				}
			}
			resourceCollectDone = true
		case err, ok := <-c.asyncCollectionResultChannel[SYSINFO_ROUTINE_KEY]:
			if !ok {
				c.logger.Info("collector has been stopped",
					log.String("channel key", RESOURCE_STAT_ROUTINE_KEY))
			} else {
				if err != nil {
					collectionErr = err
				}
			}
			sysinfoCollectDone = true
		case <-time.After(CollectTotalTimeout * time.Second):
			c.logger.Warn("collect timeout", nil,
				log.Bool("dbstat", dbstatCollectDone),
				log.Bool("resource", resourceCollectDone),
				log.Bool("sysinfo", sysinfoCollectDone))
			timeout = true
		}

		if (dbstatCollectDone && resourceCollectDone && sysinfoCollectDone) || timeout {
			if collectionErr != nil {
				return collectionErr
			}
			c.mergeMultidimensionResult(out, out1, out2, out3)
			break
		}
	}

	return nil
}

func (c *PolarDBPgCollector) mergeMultidimensionResult(
	out map[string]interface{},
	merge ...map[string]interface{}) {

	for _, mout := range merge {
		for k, v := range mout {
			if k == KEY_SEND_TO_MULTIBACKEND {
				if _, ok := out[KEY_SEND_TO_MULTIBACKEND]; !ok {
					out[KEY_SEND_TO_MULTIBACKEND] = make(map[string]interface{})
				}

				for k, v := range mout[KEY_SEND_TO_MULTIBACKEND].(map[string]interface{}) {
					out[KEY_SEND_TO_MULTIBACKEND].(map[string]interface{})[k] = v
				}
			} else if k == KEY_MULTIDIMENSION {
				if _, ok := out[KEY_MULTIDIMENSION]; !ok {
					out[KEY_MULTIDIMENSION] = make(map[string]interface{})
				}

				for model, res := range mout[KEY_MULTIDIMENSION].(map[string]interface{}) {
					out[KEY_MULTIDIMENSION].(map[string]interface{})[model] = res
				}
			} else if k == KEY_SYS_INFO {
				if _, ok := out[KEY_MULTIDIMENSION]; !ok {
					out[KEY_MULTIDIMENSION] = make(map[string]interface{})
				}

				out[KEY_MULTIDIMENSION].(map[string]interface{})[KEY_SYS_INFO] = mout[KEY_SYS_INFO]
			} else {
				out[k] = v
			}
		}
	}
}

func (c *PolarDBPgCollector) collectDBStat(out map[string]interface{}) error {
	var err error
	if err = c.collectSQLStat(c.cInfo.querymap, out); err != nil {
		c.logger.Error("collect sql stat failed", err)
	}

	if c.environment == "polarbox" || c.environment == "dbstack" {
		if c.getConfigMapValue(c.ConfigMap, "enable_db_engine_status_collect",
			"integer", 0).(int) == 1 {
			if err = c.collectEngineStatus(out); err != nil {
				c.logger.Error("collect engine status failed", err)
			}
		}
	}

	if _, ok := out["shared_memory_size_mb"]; ok {
		meta.GetMetaService().SetFloat("shared_memory_size_mb", strconv.Itoa(c.Port),
			out["shared_memory_size_mb"].(float64))
	}

	return nil
}

func (c *PolarDBPgCollector) collectSQLStat(
	queries map[string]db_config.QueryCtx,
	out map[string]interface{}) error {

	ctx, cancel := context.WithTimeout(context.Background(), DBBatchQueryTimeout*time.Second)
	defer cancel()

	for _, queryCtx := range queries {
		query := queryCtx.Query
		c.logger.Debug("executing query", log.String("query", query),
			log.Int("use snapshot", queryCtx.UseSnapshot),
			log.Bool("need snapshot", c.cInfo.dbNeedSnapshot))

		if queryCtx.Enable == 0 {
			continue
		}

		if c.cInfo.realTick%queryCtx.Cycle != 0 {
			if queryCtx.UseSnapshot == 0 {
				continue
			}

			if !c.cInfo.dbNeedSnapshot {
				continue
			}
		}

		if int(queryCtx.Cycle) >= c.cInfo.longCollectIntervalThreshold {
			c.logger.Info("collect long interval metrics",
				log.String("name", queryCtx.Name),
				log.Int64("cycle", queryCtx.Cycle))
		}

		collectTime := time.Now().UnixNano() / 1e6

		results, err := c.collectSQL(ctx, queryCtx)
		if err != nil {
			c.logger.Warn("collect failed", err, log.String("sql", queryCtx.Query))
			continue
		}

		duration := time.Now().UnixNano()/1e6 - collectTime
		if duration >= 1500 {
			c.logger.Info("slow collection query",
				log.String("name", queryCtx.Name),
				log.Int64("duration", duration))
		}

		if len(*results) == 0 {
			continue
		}

		if queryCtx.DataModel == "dbmetrics" {
			for _, line := range *results {
				for k, v := range line {
					out[k] = v
					if queryCtx.SendToMultiBackend != 0 {
						c.buildOutDictSendToMultiBackend(out, k)
					}
				}
			}
		} else {
			if _, ok := out[KEY_MULTIDIMENSION]; !ok {
				out[KEY_MULTIDIMENSION] = make(map[string]interface{})
			}
			out[KEY_MULTIDIMENSION].(map[string]interface{})[queryCtx.DataModel] = *results
		}
	}

	if c.cInfo.dbNeedSnapshot {
		out["snapshot_no"] = uint64(c.cInfo.dbSnapshotNo)
		c.cInfo.dbNeedSnapshot = false
	}

	return nil
}

func (c *PolarDBPgCollector) collectSQL(
	ctx context.Context,
	query db_config.QueryCtx) (*[]map[string]interface{}, error) {
	var msgContext []map[string]interface{}
	var DBList []string
	var err error
	if query.QueryAllDB {
		DBList, err = c.getAllDBName()
		if err != nil {
			return nil, err
		}
	} else {
		DBList = append(DBList, c.dbInfo.database)
	}

	for _, dbName := range DBList {
		c.logger.Debug("query db for",
			log.String("db", dbName),
			log.String("name", query.Name),
			log.String("dblist", fmt.Sprintf("%+v", DBList)))

		if query.QueryAllDB && dbName == "polardb_admin" {
			continue
		}

		singleMsgContext, err := c.collectSingleSQL(ctx, query, dbName)
		if err != nil {
			c.logger.Warn("collect single SQL error", err,
				log.String("dbName", dbName), log.String("query.Query", query.Query))
		}
		msgContext = append(msgContext, singleMsgContext...)
	}

	return &msgContext, nil

}

func (c *PolarDBPgCollector) getAllDBName() ([]string, error) {
	var DBList []string
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	querySQL := "SELECT datname FROM pg_database WHERE datname NOT LIKE 'template%'"
	rows, err := c.queryDBCtx(ctx, querySQL)
	if err != nil {
		c.logger.Warn("get all db name, exec sql error", err, log.String("querySQL", querySQL))
		return DBList, err
	}
	defer rows.Close()
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			c.logger.Warn("get all db name, rows scan error", err,
				log.String("querySQL", querySQL))
			return DBList, err
		}
		DBList = append(DBList, dbName)
	}

	return DBList, nil
}

func (c *PolarDBPgCollector) collectSingleSQL(
	ctx context.Context,
	qctx db_config.QueryCtx,
	dbName string) ([]map[string]interface{}, error) {

	var valRows *sql.Rows
	var msgContext []map[string]interface{}
	var err error

	c.logger.Debug("exec query", log.String("db", dbName), log.String("query", qctx.Query))

	if !qctx.QueryAllDB {
		valRows, err = c.queryDBCtx(ctx, qctx.Query)
		if err != nil {
			if strings.Contains(err.Error(), "pq: target backend may not exists") {
				return msgContext, nil
			} else {
				c.logger.Warn("exec queryDB failed", err, log.String("query", qctx.Query))
				return msgContext, err
			}
		}
		defer valRows.Close()
	} else {
		valRows, err = c.execQuerySQL(ctx, qctx.Query, dbName)

		if err != nil {
			c.logger.Warn("exec execQuerySQl failed", err, log.String("query", qctx.Query))
			return msgContext, err
		}
		defer valRows.Close()
	}

	cols, err := valRows.ColumnTypes()
	if err != nil {
		c.logger.Warn("get columns name failed.", err,
			log.String("sql", qctx.Query))
		return msgContext, err
	}

	colinfos, err := ParseCols(cols)
	if err != nil {
		c.logger.Warn("parse columns failed.", err,
			log.String("sql", qctx.Query))
		return msgContext, err
	}

	columnsPtr := make([]interface{}, len(cols))
	for i := range columnsPtr {
		col := cols[i]
		if qctx.WriteIntoLog != 0 {
			columnsPtr[i] = &sql.NullString{}
			continue
		}

		if strings.HasPrefix(col.Name(), "dim_") {
			columnsPtr[i] = &sql.NullString{}
			continue
		}

		columnsPtr[i] = &sql.NullFloat64{}
	}

	rowcount := 0
	for valRows.Next() {
		rowcount += 1

		if err = valRows.Scan(columnsPtr...); err != nil {
			c.logger.Warn("scan fields error", err)
			continue
		}

		// write query result into log
		if qctx.WriteIntoLog != 0 {
			resultlist := make([]string, len(cols))
			for i, col := range cols {
				ptrString, ok := columnsPtr[i].(*sql.NullString)
				if ok {
					resultlist[i] = fmt.Sprintf("%s: %s", col.Name(), ptrString.String)
				} else {
					resultlist[i] = ""
					c.logger.Info("result may not be string", log.String("name", qctx.Name))
				}
			}
			c.logger.Info("db collect info", log.Int("line", rowcount),
				log.String("name", qctx.Name),
				log.String("result", strings.Join(resultlist, ", ")))
			continue
		}

		ns := qctx.DataModel + "_" + dbName
		dimensionKey := ""
		kv := map[string]interface{}{}
		for i, col := range cols {
			// 维度数据
			if !strings.HasPrefix(col.Name(), "dim_") {
				continue
			}

			ptrString, ok := columnsPtr[i].(*sql.NullString)
			if ok {
				kv[NormalizeColumn(col.Name())] = ptrString.String
				dimensionKey += (ptrString.String + "_")
			}
			continue
		}

		dimensionKey = strings.ToLower(dimensionKey)

		for i, col := range cols {
			// dimemsion column, we don't calculate the value
			if strings.HasPrefix(col.Name(), "dim_") {
				continue
			}

			colNameWithDim := col.Name()
			if dimensionKey != "" {
				colNameWithDim = dimensionKey + colNameWithDim
			}

			ptrInt, ok := columnsPtr[i].(*sql.NullFloat64)
			if !ok {
				c.logger.Warn("parse to float error", errors.New("parse int error"))
				continue
			}

			if qctx.DataModel == "dbmetrics" {
				if strings.HasSuffix(colNameWithDim, "_delta") {
					c.calDeltaData(ns, colNameWithDim, kv, colNameWithDim,
						time.Now().Unix(), ptrInt.Float64, true, false)
				} else if strings.HasSuffix(colNameWithDim, "_delta_without_suffix") {
					c.calDeltaData(ns, colNameWithDim, kv,
						strings.TrimSuffix(colNameWithDim, "_delta_without_suffix"),
						time.Now().Unix(), ptrInt.Float64, true, false)
				} else {
					kv[colNameWithDim] = ptrInt.Float64
				}
			} else {
				// 增量数据
				if strings.HasPrefix(col.Name(), "delta_") {
					c.calDeltaData(ns, colNameWithDim, kv,
						NormalizeColumn(col.Name()), time.Now().Unix(),
						ptrInt.Float64, false,
						qctx.Cycle >= int64(c.cInfo.longCollectIntervalThreshold))
					continue
				}

				if strings.HasPrefix(col.Name(), "rate_") {
					c.calDeltaData(ns, colNameWithDim, kv,
						NormalizeColumn(col.Name()), time.Now().Unix(),
						ptrInt.Float64, true,
						qctx.Cycle >= int64(c.cInfo.longCollectIntervalThreshold))
					continue
				}

				// 实时数据
				if strings.HasPrefix(col.Name(), "rt_") {
					kv[NormalizeColumn(col.Name())] = ptrInt.Float64
					continue
				}

				kv[NormalizeColumn(col.Name())] = ptrInt.Float64
			}
		}
		msgContext = append(msgContext, kv)
	}

	if rowcount > 1000 {
		c.logger.Info("row count may be too large",
			log.String("db", dbName),
			log.String("query", qctx.Query),
			log.Int("rowcount", rowcount))
	}

	for _, process := range colinfos.process {
		var err error
		msgContext, err = process(msgContext, colinfos)
		if err != nil {
			c.logger.Warn("process failed", err)
			return msgContext, err
		}
		colinfos.step++
	}

	return msgContext, nil
}

func (c *PolarDBPgCollector) execQuerySQL(
	ctx context.Context, query string, dbName string) (*sql.Rows, error) {
	var conn *sql.DB

	conn, err := c.getConnection(dbName)
	if err != nil {
		c.logger.Error("get db connection failed", err)
		return nil, err
	}

	defer c.timeTrack(dbName+": "+query, time.Now())

	rows, err := c.queryDBWithConnection(ctx, conn, c.buildQuery(query))
	if err != nil {
		c.logger.Error("query db failed", err)
		return rows, err
	}

	return rows, nil
}

func (c *PolarDBPgCollector) getConnection(dbName string) (*sql.DB, error) {
	var ok bool
	var conn *sql.DB

	if conn, ok = c.dbInfo.connections[dbName]; ok {
		return conn, nil
	}

	dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d "+
		"fallback_application_name=%s sslmode=disable connect_timeout=%d",
		c.Endpoint, c.dbInfo.username, dbName, c.Port,
		"ue_multidimension_multidb_collector", DBConnTimeout)

	tempDb, err := sql.Open("postgres", dbUrl)
	if err != nil {
		c.logger.Error("connect to db failed", err)
		return nil, err
	}

	c.logger.Info("create multi db connection ok", log.String("dbname", dbName))

	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	err = tempDb.PingContext(ctx)
	defer cancel()
	if err != nil {
		c.logger.Error("ping db failed before exec query", err)
		return nil, err
	}

	conn = tempDb
	c.dbInfo.connections[dbName] = conn

	conn.Exec("SET log_min_messages=FATAL")
	conn.SetMaxIdleConns(0) // no idle connection retain

	return conn, nil
}

func (c *PolarDBPgCollector) collectEngineStatus(out map[string]interface{}) error {
	detectSQL := `
		/* rds internal mark */
		do $$
			begin
			update ha_health_check set id = id + 1 where type = 1;
			if not found then
				begin
					insert into ha_health_check(type, id) values (1, 1);
				exception when unique_violation then
				end;
			end if;
		exception when undefined_table then
			create table if not exists ha_health_check(type int, id bigint, primary key(type));
			begin
				insert into ha_health_check(type, id) values (1, 1);
			exception when unique_violation then
			end;
		end $$;
		select id from ha_health_check;`

	key := "polarbox_engine_status"

	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	if c.dbInfo.role == RW {
		rows, err := c.queryDBCtx(ctx, detectSQL)
		if err != nil {
			out[key] = err.Error()
			return nil
		}
		rows.Close()
		out[key] = "alive"
	}
	return nil
}

func (c *PolarDBPgCollector) collectSysInfo(out map[string]interface{}) error {
	sysinfos := make([]map[string]interface{}, 0)

	c.logger.Debug("commands",
		log.String("command info", fmt.Sprintf("%+v", c.cInfo.commandInfos)))
	for _, icommand := range c.cInfo.commandInfos {
		command, ok := icommand.(map[string]interface{})
		if !ok {
			c.logger.Debug("command is not map[string]string",
				log.String("command", fmt.Sprintf("%+v", icommand)))
			continue
		}

		info, err := exec.Command("sh", "-c", command["command"].(string)).Output()
		if err != nil {
			c.logger.Warn("exec failed", err, log.String("command", command["command"].(string)))
		} else {
			if strings.Trim(string(info), " \n") == "" {
				c.logger.Debug("exec result is empty",
					log.String("command", command["command"].(string)))
			} else {
				sysinfo := make(map[string]interface{})
				if name, ok := command["name"]; ok {
					sysinfo["dim_name"] = name.(string)
				} else {
					sysinfo["dim_name"] = command["command"].(string)
				}
				sysinfo["dim_value"] = strings.Trim(string(info), " \n")
				sysinfo["dim_type"] = command["type"].(string)
				sysinfo["count"] = 1 // just mock a value field
				sysinfos = append(sysinfos, sysinfo)
			}
		}
	}

	out[KEY_SYS_INFO] = sysinfos

	c.logger.Info("sys info", log.String("sys info", fmt.Sprintf("%+v", out)))

	return nil
}

func (c *PolarDBPgCollector) cacheCollectResult(out map[string]interface{}) {
	// cache out dict
	if c.cInfo.count == 1 || c.cInfo.realTick%DefaultLCMForAllIntervals == 0 {
		c.cInfo.cacheout = make(map[string]interface{})
		for k, v := range out {
			if k != KEY_SEND_TO_MULTIBACKEND && k != KEY_MULTIDIMENSION {
				c.cInfo.cacheout[k] = v
			}
		}
		c.logger.Debug("refresh cacheout", log.Int64("count", c.cInfo.count))
	} else {
		for k, v := range out {
			if k != KEY_SEND_TO_MULTIBACKEND && k != KEY_MULTIDIMENSION {
				c.cInfo.cacheout[k] = v
			}
		}

		for k, v := range c.cInfo.cacheout {
			if _, ok := out[k]; !ok {
				out[k] = v
			}
		}
	}
}

func (c *PolarDBPgCollector) prepareCollectResult(out map[string]interface{}) {
	for k, v := range out {
		if strings.Contains(k, "-") {
			continue
		}

		_, ok := v.(string)
		if ok {
			continue
		}

		_, ok = v.(map[string]interface{})
		if ok {
			if k != KEY_SEND_TO_MULTIBACKEND && k != KEY_MULTIDIMENSION {
				c.logger.Warn("must be send_to_multibackend, but now it isn't",
					nil, log.String("key", k))
			}
			continue
		}

		if c.cInfo.useFullOutdict {
			if _, ok := out[KEY_SEND_TO_MULTIBACKEND]; !ok {
				out[KEY_SEND_TO_MULTIBACKEND] = make(map[string]interface{})
			}
			out[KEY_SEND_TO_MULTIBACKEND].(map[string]interface{})[k] = v
		}

		vfloat, ok := v.(float64)
		if ok {
			out[k] = strconv.FormatUint(uint64(vfloat), 10)
			continue
		}

		vint, ok := v.(int64)
		if ok {
			out[k] = strconv.FormatUint(uint64(vint), 10)
			continue
		}

		vuint, ok := v.(uint64)
		if ok {
			out[k] = strconv.FormatUint(vuint, 10)
			continue
		}

		c.logger.Warn("value can neither be converted to string nor float64/int64/uint64",
			errors.New("value convert failed"), log.String("key", k))
	}
}

// stop this collector, release resources
func (c *PolarDBPgCollector) Stop() error {
	c.logger.Info("collector stop")

	if c.getConfigMapValue(c.ConfigMap, "enable_db_resource_collect", "integer", 1).(int) == 1 {
		c.envCollector.Stop()
	}

	for _, c := range c.asyncCollectionRunChannel {
		close(c)
	}

	for _, c := range c.asyncCollectionResultChannel {
		close(c)
	}

	if c.dbInfo.db != nil {
		c.dbInfo.db.Close()
		c.dbInfo.db = nil
	}

	if c.dbInfo.configdb != nil {
		c.dbInfo.configdb.Close()
		c.dbInfo.configdb = nil
	}

	if c.dbInfo.configCenterdb != nil {
		c.dbInfo.configCenterdb.Close()
		c.dbInfo.configCenterdb = nil
	}

	for _, conn := range c.dbInfo.connections {
		conn.Close()
	}

	return nil
}

func (c *PolarDBPgCollector) asyncCollector(name string, out map[string]interface{},
	collect func(map[string]interface{}) error) {
	select {
	case c.asyncCollectionRunChannel[name] <- 1:
	default:
		c.logger.Warn("async collector already in running state",
			nil, log.String("name", name))
		return
	}
	defer func() { <-c.asyncCollectionRunChannel[name] }()

	// try to pop out stale result
	select {
	case <-c.asyncCollectionResultChannel[name]:
	default:
	}

	c.asyncCollectionResultChannel[name] <- collect(out)
}

func (c *PolarDBPgCollector) execDBCtx(ctx context.Context, sql string) error {
	collectTime := time.Now().UnixNano() / 1e6
	_, err := c.dbInfo.db.ExecContext(ctx, c.buildQuery(sql))
	c.logger.Debug("collect ins done.", log.String("sql", sql),
		log.Int64("duration", time.Now().UnixNano()/1e6-collectTime))

	if err != nil {
		c.logger.Error("exec db failed", err, log.String("sql", sql))
		return err
	}
	return nil
}

func (c *PolarDBPgCollector) queryDBCtx(ctx context.Context, sql string) (*sql.Rows, error) {
	return c.queryDBWithConnection(ctx, c.dbInfo.db, sql)
}

func (c *PolarDBPgCollector) queryDBWithConnection(
	ctx context.Context, db *sql.DB, sql string) (*sql.Rows, error) {
	collectTime := time.Now().UnixNano() / 1e6
	rows, err := db.QueryContext(ctx, c.buildQuery(sql))
	c.logger.Debug("collect ins done.", log.String("sql", sql),
		log.Int64("duration", time.Now().UnixNano()/1e6-collectTime))

	if err != nil {
		c.logger.Error("query db failed", err, log.String("sql", sql))
		return nil, err
	}
	return rows, nil
}

func (c *PolarDBPgCollector) buildQuery(sql string) string {
	return fmt.Sprintf("%s %s; %s %s", INTERNAL_MARK, SET_CLIENT_MIN_MESSAGE, INTERNAL_MARK, sql)
}

func (c *PolarDBPgCollector) isPolarDB() bool {
	return c.dbType == "polardb_pg"
}

func (c *PolarDBPgCollector) buildOutDictSendToMultiBackend(
	out map[string]interface{}, keys ...string) error {

	if c.cInfo.useFullOutdict {
		return nil
	}

	for _, name := range keys {
		if o, ok := out[name]; ok {
			if r, xok := out[KEY_SEND_TO_MULTIBACKEND]; xok {
				r.(map[string]interface{})[name] = o
			} else {
				sendmap := make(map[string]interface{})
				sendmap[name] = o
				out[KEY_SEND_TO_MULTIBACKEND] = sendmap
			}
		}
	}

	return nil
}

func (c *PolarDBPgCollector) getMapValue(
	m map[string]interface{}, key string, defValue interface{}) interface{} {
	if v, ok := m[key]; ok {
		mapv, ok := v.(map[string]interface{})
		if ok {
			dupmap := make(map[string]interface{})
			for tmpk, tmpv := range mapv {
				dupmap[tmpk] = tmpv
			}
			return dupmap
		}
		return v
	}

	return defValue
}

func (c *PolarDBPgCollector) getConfigMapValue(
	m map[string]interface{}, key string, valueType string, defValue interface{}) interface{} {
	if v, ok := m[key]; ok {
		value, ok := v.(string)
		if !ok {
			c.logger.Debug("[polardb_pg] config value is not string",
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
				c.logger.Warn("[polardb_pg] config value type is not integer", err,
					log.String("key", key), log.String("value", fmt.Sprintf("%+v", intv)))
				return defValue
			}
		default:
			c.logger.Debug("[polardb_pg] cannot recognize this value type",
				log.String("type", valueType))
			return defValue
		}
	}

	c.logger.Debug("[polardb_pg] cannot get config map key", log.String("key", key))
	return defValue
}

func (c *PolarDBPgCollector) timeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() >= c.cInfo.slowCollectionLogThreshold {
		c.logger.Info("slow collection",
			log.String("function", key),
			log.Int64("thres", c.cInfo.slowCollectionLogThreshold),
			log.Int64("elapsed", elapsed.Milliseconds()))
	}
}

func (p *PolarDBPgCollector) calDeltaData(ns string,
	deltaName string, kv map[string]interface{},
	key string, timestamp int64, value float64, do_rate, do_record_start bool) float64 {

	var result float64

	if nsmap, ok := p.preNSValueMap[ns]; ok {
		if oldValue, ok := nsmap[deltaName]; ok {
			if do_rate {
				if value >= oldValue.Value && timestamp-oldValue.LastTime > 0 {
					result = (value - oldValue.Value) / float64(timestamp-oldValue.LastTime)
					kv[key] = result
					if do_record_start {
						kv["begin_snapshot_time"] = strconv.FormatInt(oldValue.LastTime, 10)
					}
				} else {
					result = float64(0)
				}
			} else {
				if value >= oldValue.Value {
					result = (value - oldValue.Value)
					kv[key] = result
					if do_record_start {
						kv["begin_snapshot_time"] = strconv.FormatInt(oldValue.LastTime, 10)
					}
				} else {
					result = float64(0)
				}
			}
		}
		nsmap[deltaName] = PreValue{LastTime: timestamp, Value: value}
	} else {
		nsmap := make(map[string]PreValue)
		nsmap[deltaName] = PreValue{LastTime: timestamp, Value: value}
		p.preNSValueMap[ns] = nsmap
	}

	return result
}

type DimensionPropertyType int

const (
	Reserve DimensionPropertyType = iota
	Ignore
)

type MetricPropertyType int

const (
	Top MetricPropertyType = iota
	Sum
)

type PostProcess func([]map[string]interface{}, *SQLCollectInfo) ([]map[string]interface{}, error)

type Metric struct {
	name string
	// value      float64
	properties []MetricPropertyType // same length as PostProcess steps
	param      []map[string]interface{}
}

type Dimension struct {
	name       string
	properties []DimensionPropertyType // same length as PostProcess steps
}

type SQLCollectInfo struct {
	// cols map[string]Column
	dimensions map[string]Dimension // same length as dimension cols
	metrics    map[string]Metric    // same length as metric cols
	process    []PostProcess        // same length as PostProcess steps
	step       int                  // step index
}

var topre = regexp.MustCompile(`_top(\d+)$`)

func ParseCols(cols []*sql.ColumnType) (*SQLCollectInfo, error) {
	var hasPostProcess bool
	var s string

	hasPostProcess = false
	info := &SQLCollectInfo{}
	info.metrics = make(map[string]Metric)
	info.dimensions = make(map[string]Dimension)
	info.process = make([]PostProcess, 0, 1)
	info.step = 0

	for _, col := range cols {
		if strings.HasPrefix(col.Name(), "dim_") {
			var dimension Dimension
			dimension.properties = make([]DimensionPropertyType, 0, 1)
			s = col.Name()

			if strings.HasSuffix(s, "_ignore") {
				s = strings.TrimSuffix(s, "_ignore")
				dimension.properties = append(dimension.properties, Ignore)
			} else if strings.HasSuffix(s, "_reserve") {
				s = strings.TrimSuffix(s, "_reserve")
				dimension.properties = append(dimension.properties, Reserve)
			}
			dimension.name = s
			info.dimensions[dimension.name] = dimension
		} else if strings.HasPrefix(col.Name(), "rt_") ||
			strings.HasPrefix(col.Name(), "delta_") ||
			strings.HasPrefix(col.Name(), "rate_") {

			var metric Metric
			metric.properties = make([]MetricPropertyType, 0, 1)
			metric.param = make([]map[string]interface{}, 0)
			s = col.Name()

			if strings.HasPrefix(s, "rt_") {
				s = strings.TrimPrefix(s, "rt_")
			} else if strings.HasPrefix(s, "delta_") {
				s = strings.TrimPrefix(s, "delta_")
			} else {
				s = strings.TrimPrefix(s, "rate_")
			}

			if match := topre.FindStringSubmatch(s); match != nil {
				s = strings.TrimSuffix(s, match[0])
				top, _ := strconv.Atoi(match[1])
				metric.properties = append(metric.properties, Top)
				param := map[string]interface{}{
					"top": top,
				}
				metric.param = append(metric.param, param)
				if !hasPostProcess {
					info.process = append(info.process, PostProcessTop)
					hasPostProcess = true
				}
			} else if strings.HasSuffix(s, "_sum") {
				s = strings.TrimSuffix(s, "_sum")
				metric.properties = append(metric.properties, Sum)
				if !hasPostProcess {
					info.process = append(info.process, PostProcessGroupBy)
					hasPostProcess = true
				}
			}
			metric.name = s
			info.metrics[metric.name] = metric
		}
	}

	return info, nil
}

func PostProcessTop(
	resultset []map[string]interface{}, info *SQLCollectInfo) ([]map[string]interface{}, error) {

	returnset := []map[string]interface{}{}
	groupmap := make(map[string]float64)

	for _, metric := range info.metrics {
		if !(len(metric.properties) > 0 && metric.properties[0] == Top) {
			continue
		}

		sort.SliceStable(resultset, func(i, j int) bool {
			vi, _ := resultset[i][metric.name].(float64)
			vj, _ := resultset[j][metric.name].(float64)
			return vi > vj
		})

		for i, result := range resultset {
			dimlist := make([]string, 0)
			for _, dim := range info.dimensions {
				if dim.name == "dim_query" {
					continue
				}
				dimlist = append(dimlist, result[dim.name].(string))
			}
			sort.Strings(dimlist)

			groupkey := strings.Join(dimlist, Splitter)
			if _, ok := groupmap[groupkey]; !ok {
				value, ok := result[metric.name].(float64)
				if !ok {
					// maybe nil here in the first time
					continue
				}
				if value < 0.01 {
					break
				}

				returnset = append(returnset, result)
				groupmap[groupkey] = float64(0)
			}

			var ok bool
			var topvalue int
			if topvalue, ok = metric.param[0]["top"].(int); !ok {
				topvalue = 5
			}

			if i >= topvalue {
				break
			}
		}
	}

	return returnset, nil
}

func PostProcessGroupBy(
	resultset []map[string]interface{}, info *SQLCollectInfo) ([]map[string]interface{}, error) {

	returnset := []map[string]interface{}{}
	groupmap := make(map[string]map[string]float64)

	dimnames := make([]string, 0)
	for _, dim := range info.dimensions {
		if dim.properties[0] != Ignore {
			dimnames = append(dimnames, dim.name)
		}
	}

	// group by dim list
	for _, record := range resultset {
		dimlist := make([]string, 0)
		for _, name := range dimnames {
			dimlist = append(dimlist, record[name].(string))
		}

		groupkey := strings.Join(dimlist, Splitter)

		if r, ok := groupmap[groupkey]; ok {
			for _, metric := range info.metrics {
				value, _ := record[metric.name].(float64)
				// check metric property:
				r[metric.name] += value
			}
		} else {
			resultmap := make(map[string]float64)
			for _, metric := range info.metrics {
				value, _ := record[metric.name].(float64)
				resultmap[metric.name] = value
			}
			groupmap[groupkey] = resultmap
		}
	}

	// reconstruct group by result
	for groupkey, metrics := range groupmap {
		kv := make(map[string]interface{})

		dimlist := strings.Split(groupkey, Splitter)
		for i, name := range dimnames {
			kv[name] = dimlist[i]
		}

		for metricname, metric := range metrics {
			kv[metricname] = metric
		}

		returnset = append(returnset, kv)
	}

	return returnset, nil
}

func NormalizeColumn(name string) string {
	s := name
	s = strings.TrimPrefix(s, "rt_")
	s = strings.TrimPrefix(s, "delta_")
	s = strings.TrimPrefix(s, "rate_")
	s = strings.TrimSuffix(s, "_reserve")
	if match := topre.FindStringSubmatch(s); match != nil {
		s = strings.TrimSuffix(s, match[0])
	}
	s = strings.TrimSuffix(s, "_top")
	s = strings.TrimSuffix(s, "_sum")
	return s
}
