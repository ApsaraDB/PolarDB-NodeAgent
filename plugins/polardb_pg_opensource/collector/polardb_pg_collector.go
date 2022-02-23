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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/cgroup"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/db_config"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/logger"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/system"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/utils"

	_ "github.com/lib/pq"
)

const (
	PolarMinReleaseDate   = int64(20190101)
	MinDBVersion          = -1 // means no limit
	MaxDBVersion          = -1 // means no limit
	CollectMinDBVersion   = 90200
	CollectMaxDBVersion   = MaxDBVersion
	DBConnTimeout         = 10
	DBQueryTimeout        = 60
	CollectTotalTimeout   = 5
	plutoPluginIdentifier = "golang-collector-pluto"

	DefaultLocalDiskCollectInterval = 15
	DefaultPFSCollectInterval       = 1
	DBConfigCheckVersionInterval    = 5
	DefaultLCMForAllIntervals       = 60

	basePath    = "base_path"
	rwoDataPath = "rwo_data_path"

	cpuSetCpusFile = "cpuset.cpus"
	cpuShareFile   = "cpu.shares"

	cpuCfsQuotaUs  = "cpu.cfs_quota_us"
	cpuCfsPeriodUs = "cpu.cfs_period_us"

	KEY_SEND_TO_MULTIBACKEND = "send_to_multibackend"
	MIN_TRACKLOG_TIME        = 500

	DB_STAT_ROUTINE_KEY       = "dbstat"
	RESOURCE_STAT_ROUTINE_KEY = "resource"
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

type DirInfo struct {
	dataDir   string
	newlogDir string
	newWalDir string
	baseDir   string
}

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
}

type CollectorInfo struct {
	querymap     map[string]db_config.QueryCtx
	intervalNano int64
	interval     int64
	cycle        int64

	localDiskCollectInterval int64

	timeNow                   time.Time
	count                     int64
	enablePFS                 bool
	enableLocalDisk           bool
	enableHugePage            bool
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
	localDiskPath      string
	useFullOutdict     bool

	slowCollectionLogThreshold int64

	cacheout map[string]interface{}
}

type ImportedCollector struct {
	newInstance    func(ctx interface{}, home, regex string, dirs ...string) error
	removeInstance func(ctx interface{}, home string, dirs ...string) error
	dirSize        func(interface{}, string) (int64, error)
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
	buf              bytes.Buffer
	rawPre           map[string]float64 // raw value last time
	preValueMap      map[string]PreValue
	cpumem           *cgroup.CPUMem
	pfsdcpumem       *cgroup.CPUMem
	podmem           *cgroup.CPUMem
	cpuacct          *cgroup.CPUMem
	podCpuacct       *cgroup.CPUMem
	cgroupio         *cgroup.Io

	pfsdToolCpumem *cgroup.CPUMem
	managerCpumem  *cgroup.CPUMem
	pauseCpumem    *cgroup.CPUMem
	podCpumem      *cgroup.CPUMem

	importedCollector *ImportedCollector

	dbInfo  *DBInfo
	cInfo   *CollectorInfo
	dirInfo *DirInfo

	plutoCtx         interface{}
	pfsCollector     *PfsCollector
	prCollector      *PgProcessResourceCollector
	logger           *logger.PluginLogger
	enableOutDictLog bool
	dbConfig         *db_config.DBConfig

	DbPath      string
	ClusterName string
	// localhost          *SSHClient
	Envs               map[string]string
	isNodeCpuSetAssign bool
	ConfigMap          map[string]interface{}

	configCollectQueryContext      []interface{}
	configCollectInitContext       []interface{}
	configCollectConfigInitContext []interface{}

	mutex                        *sync.Mutex
	asyncCollectionRunChannel    map[string]chan int
	asyncCollectionResultChannel map[string]chan error
}

func New() *PolarDBPgCollector {
	c := &PolarDBPgCollector{
		dbInfo:            &DBInfo{},
		cInfo:             &CollectorInfo{},
		importedCollector: &ImportedCollector{},
	}
	c.cpumem = cgroup.New(&c.buf)
	c.pfsdcpumem = cgroup.New(&c.buf)
	c.podmem = cgroup.New(&c.buf)
	c.cpuacct = cgroup.New(&c.buf)
	c.podCpuacct = cgroup.New(&c.buf)
	c.mutex = &sync.Mutex{}
	c.asyncCollectionRunChannel = make(map[string]chan int)
	c.asyncCollectionResultChannel = make(map[string]chan error)
	c.asyncCollectionRunChannel[RESOURCE_STAT_ROUTINE_KEY] = make(chan int, 1)
	c.asyncCollectionRunChannel[DB_STAT_ROUTINE_KEY] = make(chan int, 1)
	c.asyncCollectionResultChannel[RESOURCE_STAT_ROUTINE_KEY] = make(chan error, 1)
	c.asyncCollectionResultChannel[DB_STAT_ROUTINE_KEY] = make(chan error, 1)

	c.cgroupio = cgroup.NewIo(&c.buf)
	// c.pfsdCpumem = cgroup.New(&c.buf)
	// c.pfsdPodCpumem = cgroup.New(&c.buf)

	// c.pfsdCpumem = cgroup.New(&c.buf)
	c.pfsdToolCpumem = cgroup.New(&c.buf)
	c.managerCpumem = cgroup.New(&c.buf)
	c.pauseCpumem = cgroup.New(&c.buf)
	c.podCpumem = cgroup.New(&c.buf)

	c.pfsCollector = NewPfsCollector()
	c.logger = logger.NewPluginLogger("polardb_pg", nil)
	c.prCollector = NewPgProcessResourceCollector()
	c.dbConfig = db_config.NewDBConfig()
	return c
}

func (c *PolarDBPgCollector) getDataDir(basepath string, env map[string]string) string {
	if path, ok := env["host_data_dir"]; ok {
		if path != "" {
			return env["host_data_dir"]
		}
	}

	if path, ok := env["host-data-dir"]; ok {
		if path != "" {
			return "/" + strings.Replace(env["host-data-dir"], ".", "/", 5)
			// 	return env["host-data-dir"]
		}
	}

	return fmt.Sprintf("%s/%d/data", basepath, c.HostInsId)
}

func (c *PolarDBPgCollector) initEndpoint(m map[string]interface{}, envs map[string]string) string {
	if host, ok := envs["annotation.polarbox.ppas.ipAddress"]; ok {
		return host
	}
	if envs["apsara.ins.cluster.mode"] == "WriteReadMore" {
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

func (c *PolarDBPgCollector) initParams(m map[string]interface{}) error {
	c.InsName = m[consts.PluginContextKeyInsName].(string)
	c.Port = m[consts.PluginContextKeyPort].(int)

	c.dbType = m["dbtype"].(string)
	c.environment = m["environment"].(string)

	envs := m["env"].(map[string]string)
	c.Envs = envs

	// init logger first
	logIdentifier := map[string]string{
		"ins_name": c.InsName,
		"port":     strconv.Itoa(c.Port),
	}

	c.logger.SetIdentifier(logIdentifier)
	c.logger.Info("init")

	c.enableOutDictLog = c.GetMapValue(m, "enable_outdict_log", false).(bool)
	c.ConfigMap = c.GetMapValue(m, "configmap", make(map[string]interface{})).(map[string]interface{})

	// flags
	c.cInfo.interval = int64(m[consts.PluginIntervalKey].(int))
	c.cInfo.enablePFS = m["enable_pfs"].(bool)
	c.cInfo.enableDBConfig = c.GetMapValue(m, "enable_dbconfig", false).(bool)
	c.cInfo.enableDBConfigReleaseDate = c.GetMapValue(m, "enable_dbconfig_releasedate", "20211230").(string)
	c.cInfo.enableDBConfigCenter = c.GetMapValue(m, "enable_dbconfig_center", false).(bool)
	c.cInfo.dbConfigCenterHost = c.GetMapValue(m, "dbconfig_center_host", "").(string)
	c.cInfo.dbConfigCenterPort = int(c.GetMapValue(m, "dbconfig_center_port", float64(5432)).(float64))
	c.cInfo.dbConfigCenterUser = c.GetMapValue(m, "dbconfig_center_user", "postgres").(string)
	c.cInfo.dbConfigCenterPass = c.GetMapValue(m, "dbconfig_center_pass", "").(string)
	c.cInfo.dbConfigCenterDatabase = c.GetMapValue(m, "dbconfig_center_database", "postgres").(string)
	c.cInfo.slowCollectionLogThreshold = int64(c.GetMapValue(m, "slow_collection_log_thres", float64(MIN_TRACKLOG_TIME)).(float64))
	c.cInfo.useFullOutdict = c.GetMapValue(m, "use_full_outdict", false).(bool)

	c.cInfo.dbConfigSchema = c.GetMapValue(m, "dbconfig_schema", "polar_gawr_collection").(string)
	if c.environment == "polarbox" {
		if envs["apsara.ins.cluster.mode"] == "WriteReadMore" {
			c.cInfo.enablePFS = true
		} else {
			c.cInfo.enablePFS = false
		}
	}
	c.isOnEcs = m["is_on_ecs"].(bool)

	c.HostInsIdStrList = make([]string, 0)
	c.Endpoint = c.initEndpoint(m, envs)
	// need to simplify this logic
	if _, ok := envs["apsara.metric.ins_name"]; ok {
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
		c.initHostInsIdStrList()
		c.InsId, _ = strconv.Atoi(m[consts.PluginContextKeyInsId].(string))
		c.DataDir = c.getDataDir(m[basePath].(string), envs)
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

	// collect info
	c.cInfo.intervalNano = int64(0)
	if _, ok := m["local_disk_collect_interval"]; ok {
		c.cInfo.localDiskCollectInterval = int64(m["local_disk_collect_interval"].(float64))
	} else {
		c.cInfo.localDiskCollectInterval = DefaultLocalDiskCollectInterval
	}
	c.cInfo.count = 0
	c.cInfo.cacheout = make(map[string]interface{})
	c.cInfo.cycle = int64(m["cycle"].(float64))

	// other init
	DefaultCollectCycle = c.cInfo.cycle
	c.rawPre = make(map[string]float64, 1024)
	c.preValueMap = make(map[string]PreValue, 1024)

	c.logger.Debug("init params done", log.String("c", fmt.Sprintf("%+v", c)))
	return nil
}

func (c *PolarDBPgCollector) initImportedCollector(m map[string]interface{}) error {
	importsMap, ok := m[consts.PluginContextKeyImportsMap].(map[string]interface{})
	if !ok {
		return fmt.Errorf("init: imports not found: %v", m)
	}

	newInstanceFn, ok := importsMap[consts.PlutoNewInstanceIdentifier]
	if !ok {
		return fmt.Errorf("newInstance func not found: %s", consts.PlutoNewInstanceIdentifier)
	}
	c.importedCollector.newInstance, _ =
		newInstanceFn.(func(ctx interface{}, home, regex string, dirs ...string) error)

	removeInstanceFn, ok := importsMap[consts.PlutoRemoveInstanceIdentifier]
	if !ok {
		return fmt.Errorf("removeInstance func not found: %s", consts.PlutoRemoveInstanceIdentifier)
	}
	c.importedCollector.removeInstance =
		removeInstanceFn.(func(ctx interface{}, home string, dirs ...string) error)

	dirSizeFn, ok := importsMap[consts.PlutoDirSizeCollectorIdentifier]
	if !ok {
		return fmt.Errorf("dirSize func not found: %s", consts.PlutoDirSizeCollectorIdentifier)
	}
	c.importedCollector.dirSize = dirSizeFn.(func(ctx interface{}, path string) (int64, error))

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
		c.getDBVersion("Postgres Version", "SHOW server_version_num"); err != nil {
		c.logger.Error("get db version failed", err)
		return err
	}

	c.dbInfo.releaseDate = PolarMinReleaseDate
	if c.isPolarDB() {
		if enablePFS, err := c.getEnablePFS("SHOW polar_vfs.localfs_mode"); err != nil {
			c.logger.Warn("get pfs mode failed", err)
		} else {
			c.cInfo.enablePFS = enablePFS
		}

		if c.dbInfo.releaseDate, err =
			c.getPolarVersion("PolarDB Release Date", "SHOW polar_release_date"); err != nil {
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

		queryCtx.Cycle = queryCtx.Cycle / c.cInfo.interval
		if queryCtx.Cycle == 0 {
			queryCtx.Cycle = int64(1)
		}

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

	c.configCollectInitContext = c.GetMapValue(m, "collect_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectConfigInitContext = c.GetMapValue(m, "config_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectQueryContext = c.GetMapValue(m, "queries",
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

func (c *PolarDBPgCollector) getContainerCgroupPath(path string, name string, envs map[string]string) string {
	return path + "/../" + envs[fmt.Sprintf("pod_container_%s_containerid", name)]
}

func (c *PolarDBPgCollector) initCgroupCollector(m map[string]interface{}) error {
	envs := m["env"].(map[string]string)

	c.cpumem.InitCpu(envs[consts.CGroupCpuPath])
	c.cpumem.InitMemory(envs[consts.CGroupMemPath])
	c.podmem.InitMemory(filepath.Dir(envs[consts.CGroupMemPath]))
	c.cgroupio.InitPath(envs[consts.CGroupBlkioPath], c.isOnEcs)

	if c.environment != "software" {
		c.podCpuacct.InitCpu(filepath.Dir(envs["cgroup_cpu_path"]))

		// c.pfsdCpumem.InitCpu(envs["pfsd_cgroup_cpu_path"])
		// c.pfsdCpumem.InitMemory(envs["pfsd_cgroup_mem_path"])
		// c.pfsdPodCpumem.InitCpu(filepath.Dir(envs["pfsd_cgroup_cpu_path"]))

		c.logger.Info("podcpuset filepath",
			log.String("cgroup_cpuset_path", filepath.Dir(envs["cgroup_cpuset_path"])),
			log.String("pfsd_cgroup_cpuset_path", filepath.Dir(envs["pfsd_cgroup_cpuset_path"])))

	}

	if _, ok := envs["cgroup_cpuacct_path"]; ok {
		c.cpuacct.InitCpu(envs["cgroup_cpuacct_path"])
	} else {
		c.cpuacct.InitCpu(envs["cgroup_cpu_path"])
	}

	if c.isPolarDB() {
		c.initCgroupCollectObj(c.pfsdcpumem, envs, "pfsd")
		c.initCgroupCollectObj(c.pfsdToolCpumem, envs, "pfsd-tool")
		c.initCgroupCollectObj(c.managerCpumem, envs, "manager")
		c.initCgroupCollectObj(c.pauseCpumem, envs, "POD")

		c.podCpumem.InitMemory(filepath.Dir(envs[consts.CGroupMemPath]))
		if _, ok := envs[consts.CGroupCpuAcctPath]; ok {
			c.podCpumem.InitCpu(filepath.Dir(envs[consts.CGroupCpuAcctPath]))
		} else {
			c.podCpumem.InitCpu(filepath.Dir(envs[consts.CGroupCpuPath]))
		}
		c.logger.Info("pod cgroup path",
			log.String("cpu", filepath.Dir(envs[consts.CGroupCpuPath])),
			log.String("memory", filepath.Dir(envs[consts.CGroupMemPath])))
	}

	if _, ok := envs[consts.CGroupHugeMemPath]; ok {
		c.cpumem.InitHugePageMemory(envs[consts.CGroupHugeMemPath], "2M")
		c.podmem.InitHugePageMemory(filepath.Dir(envs[consts.CGroupHugeMemPath]), "2M")
		c.cInfo.enableHugePage = true
	} else {
		c.cInfo.enableHugePage = false
	}

	return nil
}

func (c *PolarDBPgCollector) initLocalDiskCollector(m map[string]interface{}) error {
	var newlogDir, baseDir, newWalDir string
	datadir := c.DataDir

	c.cInfo.enableLocalDisk = false
	envs := m["env"].(map[string]string)
	if !c.cInfo.enablePFS {
		// for polarflex
		if pvdatadir, ok := envs["/disk1"]; ok {
			c.cInfo.enableLocalDisk = true
			c.cInfo.localDiskPath = pvdatadir
			datadir = pvdatadir
		}

		if _, ok := envs["host_data_local_dir"]; ok {
			c.cInfo.enableLocalDisk = true
			c.cInfo.localDiskPath = envs["host_data_dir"]
		}
	}

	if waldir, ok := envs["host_wal_dir"]; ok {
		newWalDir = fmt.Sprintf("%s/pg_wal", waldir)
	} else {
		newWalDir = fmt.Sprintf("%s/pg_wal", datadir)
	}

	if waldir, ok := envs["host_wal_full_path"]; ok {
		newWalDir = waldir
	}

	if logdir, ok := envs["host_log_full_path"]; ok {
		newlogDir = logdir
	} else {
		newlogDir = fmt.Sprintf("%s/%s", datadir, "log")
	}

	baseDir = fmt.Sprintf("%s/base", datadir)

	if c.cInfo.enablePFS {
		c.dirInfo = &DirInfo{
			dataDir:   datadir,
			newlogDir: newlogDir,
			baseDir:   baseDir,
		}
	} else {
		c.dirInfo = &DirInfo{
			dataDir:   datadir,
			newlogDir: newlogDir,
			baseDir:   baseDir,
			newWalDir: newWalDir,
		}
	}

	c.logger.Info("dir info", log.String("dirinfo", fmt.Sprintf("%+v", c.dirInfo)))

	plutoCtx, ok := m[consts.PlutoPluginIdentifier]
	if !ok {
		return fmt.Errorf("context not found: %s", plutoPluginIdentifier)
	}
	c.plutoCtx = plutoCtx

	if c.cInfo.enablePFS {
		c.importedCollector.newInstance(plutoCtx, "", "",
			c.dirInfo.dataDir, c.dirInfo.newlogDir,
			c.dirInfo.baseDir)
	} else {
		c.importedCollector.newInstance(plutoCtx, "", "",
			c.dirInfo.dataDir, c.dirInfo.newlogDir,
			c.dirInfo.baseDir, c.dirInfo.newWalDir)
	}

	return nil
}

func (c *PolarDBPgCollector) initDiskCollector(m map[string]interface{}) error {
	var err error

	if err = c.initLocalDiskCollector(m); err != nil {
		c.logger.Error("init local disk collector failed", err)
		return err
	}

	if c.cInfo.enablePFS {
		if err = c.pfsCollector.Init(m, c.logger); err != nil {
			c.logger.Error("init pfs collector failed", err)
			return err

		}
	}

	return nil
}

func (c *PolarDBPgCollector) initResourceCollector(m map[string]interface{}) error {
	var err error
	if c.environment != "software" {
		if c.isPolarDB() {
			if err = c.initOOMScore(m); err != nil {
				c.logger.Warn("change oom score failed", err)
			}
		}
		if err = c.initCgroupCollector(m); err != nil {
			c.logger.Error("init cgroup collector failed", err)
			return err
		}

	}

	if err = c.prCollector.Init(m, c.logger); err != nil {
		c.logger.Error("init process resource collector failed", err)
		return err
	}

	if err = c.initDiskCollector(m); err != nil {
		c.logger.Error("init disk collector failed", err)
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

func (c *PolarDBPgCollector) changeOOMScore(name string, pid int64) error {
	if err := ioutil.WriteFile(fmt.Sprintf("/proc/%d/oom_score_adj", int(pid)), []byte("-1000"), 0644); err != nil {
		c.logger.Warn("write oom adj failed", err,
			log.String("name", name),
			log.String("file", fmt.Sprintf("/proc/%d/oom_adj", int(pid))))
		return err
	}

	c.logger.Info("change oom score success", log.String("name", name), log.Int64("pid", pid))

	return nil
}

func (c *PolarDBPgCollector) initOOMScore(m map[string]interface{}) error {
	envs := m["env"].(map[string]string)

	// 1. pause
	pausePath := c.getContainerCgroupPath(envs[consts.CGroupCpuPath], "POD", envs)

	content, err := ioutil.ReadFile(pausePath + "/tasks")
	if err != nil {
		c.logger.Warn("read pause tasks file failed", err)
	} else {
		if pid, err := strconv.ParseInt(strings.TrimSuffix(string(content), "\n"), 10, 64); err != nil {
			c.logger.Warn("tasks file content cannot be parsed to int64", err, log.String("content", string(content)))
		} else {
			if err := c.changeOOMScore("pause", pid); err != nil {
				c.logger.Warn("change puase oom score failed", err,
					log.Int64("pid", pid))
			}
		}
	}

	// 2. postmaster
	// content, err = ioutil.ReadFile(c.DataDir + "/postmaster.pid")
	// if err != nil {
	// 	c.logger.warn("read postmaster pid failed", err)
	// } else {
	// 	for _, line := range(strings.Split(string(content), "\n")) {
	// 		if pid, err := strconv.ParseInt(line, 10, 64); err != nil {
	// 			c.logger.warn("postmaster pid content cannot be parsed to int64", err, log.String("content", string(content)))
	// 		} else {
	// 			if err := c.changeOOMScore("postmaster", pid); err != nil {
	// 				c.logger.warn("change postmaster oom score failed", err,
	// 						log.Int64("pid", pid))
	// 			}
	// 		}
	// 		break
	// 	}
	// }

	return nil
}

func (c *PolarDBPgCollector) Init(m map[string]interface{}) error {
	var err error

	log.Debug("[polardb_pg] init with info.", log.String("info", fmt.Sprintf("%+v", m)))

	if err = c.initParams(m); err != nil {
		c.logger.Error("init param failed", err)
		goto ERROR
	}

	if err = c.initImportedCollector(m); err != nil {
		c.logger.Error("init imported collector failed", err)
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
	for _, extension := range c.dbInfo.extensions {
		err := c.execDB(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", extension))
		if err != nil {
			c.logger.Warn("create extension failed.", err, log.String("extension", extension))
		}

		err = c.execDB(fmt.Sprintf("ALTER EXTENSION %s UPDATE", extension))
		if err != nil {
			c.logger.Warn("update extension failed.", err, log.String("extension", extension))
		}
	}

	return nil
}

func (c *PolarDBPgCollector) prepareObjects() error {
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

		if err := c.execDB(queryCtx.Query); err != nil {
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

	if nodetype, err := c.getNodeType(); err != nil {
		c.logger.Warn("get node type failed", err)
	} else {
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

func (c *PolarDBPgCollector) getNodeType() (string, error) {
	var nodetype string

	rows, cancel, err := c.queryDB("SELECT * FROM polar_node_type()")
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get node type", err)
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&nodetype); err != nil {
			c.logger.Error("get node type failed", err)
			return "", nil
		}

		return nodetype, nil
	}

	return "", errors.New("nothing get from polar_node_type")
}

func (c *PolarDBPgCollector) isInRecoveryMode() (bool, error) {
	rows, cancel, err := c.queryDB("SELECT pg_is_in_recovery();")
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("Can't get pg recovery mode.", err)
		return true, err
	}
	defer rows.Close()

	// Value
	var isRecoveryMode sql.NullBool

	if rows.Next() {
		if err := rows.Scan(&isRecoveryMode); err != nil {
			c.logger.Error("scan for values failed", err)
			return true, err
		}

		if !isRecoveryMode.Valid {
			err := errors.New("recovery mode value is invalid")
			c.logger.Error("recovery mode value is invalid", err)
			return true, err
		}

		value := isRecoveryMode.Bool
		return value, nil
	}

	return false, nil
}

func (c *PolarDBPgCollector) getDBVersion(prefix string, query string) (int64, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get db version", err)
		return 0, err
	}
	defer rows.Close()

	var value sql.NullInt64

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return 0, err
		}

		if !value.Valid {
			c.logger.Error("version value is invalid", errors.New("version value is invalid"))
			return 0, errors.New("version value is invalid")
		}

		version := int64(value.Int64)
		c.logger.Info(prefix, log.Int64("version", version))

		return version, nil
	}

	return 0, nil
}

func (c *PolarDBPgCollector) getEnablePFS(query string) (bool, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get pfs mode", err)
		return false, err
	}
	defer rows.Close()

	var value string

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return false, err
		}

		c.logger.Info("PolarDB Check File System", log.String("local fs", value))

		if value == "on" {
			return false, nil
		}

		return true, nil
	}

	return false, errors.New("no enable pfs get from db")
}

func (c *PolarDBPgCollector) getPolarVersion(prefix string, query string) (int64, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get polar version", err)
		return 0, err
	}
	defer rows.Close()

	var value string

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return 0, err
		}

		if len(value) < 8 {
			err := errors.New("version value length is invalid")
			c.logger.Error("version value length is invalid", err,
				log.String("version value", value))
			return 0, err
		}

		value := value[0:8]
		version, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			c.logger.Error("parse version value failed", err, log.String("version", value))
			return 0, err
		}

		c.logger.Info(prefix, log.Int64("version", version))

		return version, nil
	}

	return 0, nil
}

func (c *PolarDBPgCollector) getValueFromDB(query string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := c.dbInfo.db.QueryContext(ctx, "/* rds internal mark */ "+query)
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

	if c.cInfo.count%DBConfigCheckVersionInterval != 0 {
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
				log.Int64("old version", c.cInfo.dbConfigVersion), log.Int64("new version", version))
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

func (c *PolarDBPgCollector) collectCgroupStat(out map[string]interface{}) error {
	var err error

	if err = c.collectCgroupCPU(out); err != nil {
		c.logger.Warn("collect cgroup CPU failed.", err)
	}

	if err = c.collectCgroupMemory(out); err != nil {
		c.logger.Warn("collect cgroup memory failed.", err)
	}

	if err = c.collectCgroupIO(out); err != nil {
		c.logger.Warn("collect cgroup IO failed.", err)
	}

	return nil
}

func (c *PolarDBPgCollector) getMemTotal() (uint64, error) {
	memfile, err := os.Open("/proc/meminfo")
	if err != nil {
		return uint64(0), err
	}
	defer memfile.Close()

	buf := make([]byte, 10*1024)

	num, err := memfile.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return uint64(0), err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.FieldsFunc(buf[:index], func(r rune) bool {
			return r == ':' || unicode.IsSpace(r)
		})
		buf = buf[index+1:]

		if len(fields) < 2 {
			continue
		}

		size, err := strconv.ParseUint(string(fields[1]), 10, 64)
		return size / 1024, err
	}

	return uint64(0), nil
}

func (c *PolarDBPgCollector) collectResourceStat(out map[string]interface{}) error {

	if c.environment != "software" {
		if err := c.collectCgroupStat(out); err != nil {
			c.logger.Error("collect cgroup stat failed.", err)
		}
	}

	if c.environment == "software" {
		cpuCores, _ := system.GetCPUCount()
		c.setPrCollectCpuCore(float64(cpuCores))
		memTotal, _ := c.getMemTotal()

		if err := c.prCollector.Collect(out, c.cInfo.intervalNano); err != nil {
			c.logger.Error("collect process resource failed", err)
		} else {
			if out["procs_cpu_user_sum"] != nil && out["procs_cpu_sys_sum"] != nil {
				out["cpu_user"] = out["procs_cpu_user_sum"]
				out["cpu_sys"] = out["procs_cpu_sys_sum"]
				out["cpu_total"] =
					out["procs_cpu_user_sum"].(float64) + out["procs_cpu_sys_sum"].(float64)
			} else {
				out["cpu_user"] = float64(0)
				out["cpu_sys"] = float64(0)
				out["cpu_total"] = float64(0)
			}

			out["cpu_user_usage"] = out["cpu_user"].(float64) / float64(cpuCores)
			out["cpu_sys_usage"] = out["cpu_sys"].(float64) / float64(cpuCores)
			out["cpu_total_usage"] = out["cpu_total"].(float64) / float64(cpuCores)

			out["cpu_cores"] = uint64(cpuCores)
			// include shared memory
			if share, ok := meta.GetMetaService().GetFloat("shared_memory_size_mb",
				strconv.Itoa(c.Port)); ok {
				out["mem_total_usage"] =
					(out["procs_mem_rss_sum"].(float64) +
						share) * 100 / float64(memTotal)
				out["mem_total_used"] = (out["procs_mem_rss_sum"].(float64) + share)
			} else {
				out["mem_total_usage"] =
					out["procs_mem_rss_sum"].(float64) * 100 / float64(memTotal)
				out["mem_total_used"] = out["procs_mem_rss_sum"]
			}

			out["mem_total"] = memTotal

			// CPU and Memory formalize
			c.buildOutDictSendToMultiBackend(out, "cpu_cores",
				"cpu_user_usage", "cpu_sys_usage", "cpu_total_usage",
				"mem_total_usage", "mem_total_used", "mem_total")
		}
	} else {
		if err := c.prCollector.Collect(out, c.cInfo.intervalNano); err != nil {
			c.logger.Error("collect process resource failed", err)
		}
	}

	if err := c.collectDiskSpace(out); err != nil {
		c.logger.Warn("collect disk space failed", err)
	}

	return nil
}

func (c *PolarDBPgCollector) collectDiskSpace(out map[string]interface{}) error {
	out["enable_pfs"] = uint64(0)
	if c.cInfo.enablePFS {
		if c.cInfo.count%DefaultPFSCollectInterval == 0 {
			out["enable_pfs"] = uint64(1)
			if err := c.pfsCollector.Collect(out); err != nil {
				c.logger.Error("collect pfs stat failed", err)
			} else {
				if _, ok := out["pls_base_dir_size"]; ok {
					out["polar_base_dir_size"] = out["pls_base_dir_size"]
					out["polar_wal_dir_size"] = out["pls_pg_wal_dir_size"]
				}
			}
		}
	} else {
		if c.cInfo.count%c.cInfo.localDiskCollectInterval == 0 {
			if c.cInfo.enableLocalDisk {
				if err := c.collectLocalVolumeCapacityWithDf(out); err != nil {
					c.logger.Error("collect volume capacity with df failed", err)
				}
			}

			if err := c.collectLocalDirSize(out); err != nil {
				c.logger.Error("collect dir size failed", err)
			} else {
				if _, ok := out["local_base_dir_size"]; ok {
					out["polar_base_dir_size"], _ =
						strconv.ParseUint(out["local_base_dir_size"].(string), 10, 64)
				}
				if _, ok := out["local_pg_wal_dir_size"]; ok {
					out["polar_wal_dir_size"], _ =
						strconv.ParseUint(out["local_pg_wal_dir_size"].(string), 10, 64)
				}
			}
		}
	}

	// FS normalize
	c.buildOutDictSendToMultiBackend(out, "enable_pfs",
		"fs_inodes_total", "fs_inodes_used", "fs_inodes_usage",
		"fs_blocks_total", "fs_blocks_used", "fs_blocks_usage",
		"fs_size_total", "fs_size_used", "fs_size_usage")
	c.buildOutDictSendToMultiBackend(out, "pls_iops",
		"pls_iops_read", "pls_iops_write", "pls_throughput",
		"pls_throughput_read", "pls_throughput_write", "pls_latency_read",
		"pls_latency_write")
	// dir formalize
	c.buildOutDictSendToMultiBackend(out, "polar_base_dir_size", "polar_wal_dir_size")

	return nil
}

func (c *PolarDBPgCollector) Collect(out map[string]interface{}) error {
	now := time.Now()
	// not right in first time
	c.cInfo.intervalNano = now.UnixNano() - c.cInfo.timeNow.UnixNano()

	c.cInfo.timeNow = now
	c.cInfo.count += 1

	if c.dbInfo.db == nil {
		c.logger.Warn("db is nil", fmt.Errorf("db is nil"))
		return nil
	}

	// async collection
	asyncCollector := func(name string, out map[string]interface{},
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

	out1 := make(map[string]interface{})
	go asyncCollector(DB_STAT_ROUTINE_KEY, out1, func(out map[string]interface{}) error {
		if err := c.checkIfNeedRestart(); err != nil {
			c.logger.Info("we need to restart", log.String("info", err.Error()))
			return err
		}

		// these two order is important
		if c.GetConfigMapValue(c.ConfigMap, "enable_db_metric_collect", "integer", 1).(int) == 1 {
			c.dbInfo.db.Exec("SET log_min_messages=FATAL")

			if err := c.collectDBStat(out1); err != nil {
				c.logger.Warn("collect db stat failed.", err)
			}
		}

		return nil
	})

	out2 := make(map[string]interface{})
	go asyncCollector(RESOURCE_STAT_ROUTINE_KEY, out2, func(out map[string]interface{}) error {
		if c.GetConfigMapValue(c.ConfigMap, "enable_db_resource_collect", "integer", 1).(int) == 1 {
			if err := c.collectResourceStat(out2); err != nil {
				c.logger.Warn("collect resource stat failed.", err)
			}
		}

		return nil
	})

	// get result
	dbstatCollectDone := false
	resourceCollectDone := false
	timeout := false
	for {
		select {
		case err := <-c.asyncCollectionResultChannel[DB_STAT_ROUTINE_KEY]:
			if err != nil {
				return err
			}
			dbstatCollectDone = true
		case err := <-c.asyncCollectionResultChannel[RESOURCE_STAT_ROUTINE_KEY]:
			if err != nil {
				return err
			}
			resourceCollectDone = true
		case <-time.After(CollectTotalTimeout * time.Second):
			c.logger.Warn("collect timeout", nil,
				log.Bool("dbstat", dbstatCollectDone),
				log.Bool("resource", resourceCollectDone))
			timeout = true
		}

		if (dbstatCollectDone && resourceCollectDone) || timeout {
			break
		}
	}

	// merge async collection results
	mergeResult := func(merge ...map[string]interface{}) {
		for _, mout := range merge {
			for k, v := range mout {
				if k != KEY_SEND_TO_MULTIBACKEND {
					out[k] = v
				} else {
					if _, ok := out[KEY_SEND_TO_MULTIBACKEND]; !ok {
						out[KEY_SEND_TO_MULTIBACKEND] = make(map[string]interface{})
					}

					for k, v := range mout[KEY_SEND_TO_MULTIBACKEND].(map[string]interface{}) {
						out[KEY_SEND_TO_MULTIBACKEND].(map[string]interface{})[k] = v
					}
				}
			}
		}
	}

	if dbstatCollectDone {
		mergeResult(out1)
	}

	if resourceCollectDone {
		mergeResult(out2)
	}

	if c.cInfo.useFullOutdict {
		out[KEY_SEND_TO_MULTIBACKEND] = make(map[string]interface{})
	}

	// cache out dict
	if c.cInfo.count == 1 || c.cInfo.count%DefaultLCMForAllIntervals == 0 {
		c.cInfo.cacheout = make(map[string]interface{})
		for k, v := range out {
			if k != KEY_SEND_TO_MULTIBACKEND {
				c.cInfo.cacheout[k] = v
			}
		}
		c.logger.Debug("refresh cacheout", log.Int64("count", c.cInfo.count))
	} else {
		for k, v := range out {
			if k != KEY_SEND_TO_MULTIBACKEND {
				c.cInfo.cacheout[k] = v
			}
		}

		for k, v := range c.cInfo.cacheout {
			if _, ok := out[k]; !ok {
				out[k] = v
			}
		}
	}

	// turn out from map[string]uint64 to map[string]string
	for k, v := range out {
		_, ok := v.(string)
		if ok {
			continue
		}

		_, ok = v.(map[string]interface{})
		if ok {
			if k != KEY_SEND_TO_MULTIBACKEND {
				c.logger.Warn("must be send_to_multibackend, but now it isn't",
					nil, log.String("key", k))
			}
			continue
		}

		if c.cInfo.useFullOutdict {
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

	out["collect_timestamp"] = time.Now().Unix()

	if c.enableOutDictLog {
		c.logger.Info("out data dict",
			log.Int64("count", c.cInfo.count),
			log.String("out", fmt.Sprintf("%+v", out)))
	}

	return nil
}

func (c *PolarDBPgCollector) collectLocalDirSize(out map[string]interface{}) error {
	if dataSize, err := c.importedCollector.dirSize(c.plutoCtx, c.dirInfo.dataDir); err != nil {
		c.logger.Debug("get local data dir size failed",
			log.String("error", err.Error()), log.String("dir", c.dirInfo.dataDir))
	} else {
		utils.AddNonNegativeValue(out, "local_data_dir_size", dataSize/1024/1024)
	}

	newlogSize, err := c.importedCollector.dirSize(c.plutoCtx, c.dirInfo.newlogDir)
	if err != nil {
		c.logger.Debug("get new pg_log dir size failed",
			log.String("error", err.Error()),
			log.String("logdir", c.dirInfo.newlogDir))
	} else {
		utils.AddNonNegativeValue(out, "local_pg_log_dir_size", (newlogSize)/1024/1024)
	}

	baseSize, err := c.importedCollector.dirSize(c.plutoCtx, c.dirInfo.baseDir)
	if err != nil {
		c.logger.Debug("get base dir size failed",
			log.String("error", err.Error()),
			log.String("basedir", c.dirInfo.baseDir))
	} else {
		utils.AddNonNegativeValue(out, "local_base_dir_size", baseSize/1024/1024)
	}

	if !c.cInfo.enablePFS {
		newWalSize, err := c.importedCollector.dirSize(c.plutoCtx, c.dirInfo.newWalDir)
		if err != nil {
			c.logger.Debug("get pg_wal dir size failed",
				log.String("error", err.Error()),
				log.String("waldir", c.dirInfo.newWalDir))
		} else {
			utils.AddNonNegativeValue(out, "local_pg_wal_dir_size", (newWalSize)/1024/1024)
		}
	}

	return nil
}

func (c *PolarDBPgCollector) getIntValue(filepath string) (int64, error) {
	f, err := os.Open(filepath)
	if err != nil {
		c.logger.Error("read file fail", err, log.String("filepath", filepath))
		return 0, err
	}
	defer f.Close()

	bufBytes, err := ioutil.ReadAll(f)
	if err != nil {
		c.logger.Error("read to fd fail", err, log.String("filepath", filepath))
		return 0, err
	}

	// 去掉最后一个换行符
	bufBytes = bufBytes[0 : len(bufBytes)-1]

	value, err := strconv.ParseInt(string(bufBytes), 10, 64)
	if err != nil {
		c.logger.Error("parser int fail", err, log.String("bufBytes", string(bufBytes)))
		return 0, err
	}

	return value, nil

}

func (c *PolarDBPgCollector) getCpuSetCores(filepath string) (uint64, error) {
	f, err := os.Open(filepath)
	if err != nil {
		c.logger.Error("read file fail", err, log.String("filepath", filepath))
		return 0, err
	}
	defer f.Close()

	bufBytes, err := ioutil.ReadAll(f)
	if err != nil {
		c.logger.Error("read to fd fail", err, log.String("filepath", filepath))
		return 0, err
	}

	// 去掉最后一个换行符
	bufBytes = bufBytes[0 : len(bufBytes)-1]

	fields := bytes.FieldsFunc(bufBytes, func(r rune) bool {
		return r == ','
	})
	var cores int64 = 0
	for _, field := range fields {
		if bytes.ContainsRune(field, '-') {
			coreSet := bytes.FieldsFunc(field, func(r rune) bool {
				return r == '-'
			})
			if len(coreSet) == 2 {
				start, err := strconv.ParseInt(string(coreSet[0]), 10, 64)
				if err != nil {
					return 0, err
				}
				end, err := strconv.ParseInt(string(coreSet[1]), 10, 64)
				if err != nil {
					return 0, err
				}
				cores += end - start + 1
			} else {
				return 0, fmt.Errorf("wrong format for cpuset,path:%s", filepath)
			}
		} else {
			cores++
		}
	}
	return uint64(cores), nil
}

func (c *PolarDBPgCollector) getCgroupCpucoresLimit() (float64, error) {

	var err error
	// 绑核情况，才去获取cpuset
	if c.isNodeCpuSetAssign {
		// 获取pod的CPUSet
		cpuSetCores, err :=
			c.getCpuSetCores(filepath.Dir(c.Envs["cgroup_cpuset_path"]) + "/" + cpuSetCpusFile)
		if err == nil && cpuSetCores != 0 {
			return float64(cpuSetCores), err
		} else if err != nil {
			c.logger.Warn("podCpuset GetCpuSetCores fail", err)
		}
	}

	// 获取pod的CPU quota
	cpuQuota, err := c.getIntValue(filepath.Dir(c.Envs["cgroup_cpu_path"]) + "/" + cpuCfsQuotaUs)
	if err == nil && cpuQuota != -1 {
		cpuPeriods, err := c.getIntValue(filepath.Dir(c.Envs["cgroup_cpu_path"]) + "/" + cpuCfsPeriodUs)
		if err == nil && cpuPeriods != 0 {
			return float64(cpuQuota) / float64(cpuPeriods), err
		}
	} else if err != nil {
		c.logger.Warn("pfsd pfsdPodCpumem GetCpuLimit", err)
	}

	// 获取pod的CPUshare
	cpuShare, err := c.getIntValue(filepath.Dir(c.Envs["cgroup_cpu_path"]) + "/" + cpuShareFile)
	if err == nil && cpuShare != 0 && cpuShare != -1 {
		return float64(cpuShare) / 1024, err
	} else if err != nil {
		c.logger.Warn("pfsd get CPUshare fail", err)
	}

	return 0.0, nil
}

func (c *PolarDBPgCollector) setPrCollectCpuCore(cpuCores float64) error {

	c.prCollector.cpuCoreNumber = cpuCores
	return nil
}

func (c *PolarDBPgCollector) collectCgroupCPU(out map[string]interface{}) error {
	calculateCPUUsage := func(key string, out map[string]interface{}, used uint64) {
		c.calcDeltaWithoutSuffix(key, out, float64(used))
		if x, ok := out[key]; ok {
			out[key] = (x.(float64)) * 100 / float64(c.cInfo.intervalNano)
		}
	}

	calculateTotalCpu := func(out map[string]interface{}, finalkey string, key ...string) {
		for _, k := range key {
			if _, ok := out[k]; ok {
				if _, xok := out[finalkey]; xok {
					out[finalkey] = out[finalkey].(float64) + out[k].(float64)
				} else {
					out[finalkey] = out[k].(float64)
				}
			}
		}
	}

	var cpuCoresLimit float64 = 1
	cpuCoresLimit, _ = c.getCgroupCpucoresLimit()
	c.setPrCollectCpuCore(cpuCoresLimit)
	out["cpu_cores"] = uint64(cpuCoresLimit)

	userCpu, sysCpu, totalCpu, err := c.cpuacct.GetCpuUsage()
	if err != nil {
		c.logger.Warn("get cpu metrics failed.", err)
	} else {
		calculateCPUUsage("engine_cpu_user", out, userCpu)
		calculateCPUUsage("engine_cpu_sys", out, sysCpu)
		calculateCPUUsage("engine_cpu_total", out, totalCpu)
		calculateTotalCpu(out, "cpu_user", "engine_cpu_user")
		calculateTotalCpu(out, "cpu_sys", "engine_cpu_sys")
		calculateTotalCpu(out, "cpu_total", "engine_cpu_total")

		if c.isPolarDB() {
			cgroupCpuCollect := func(cg *cgroup.CPUMem, name string) {
				if user, sys, total, err := cg.GetCpuUsage(); err != nil {
					c.logger.Warn("get cpu usage failed", err, log.String("container", name))
				} else {
					calculateCPUUsage(name+"_cpu_user", out, user)
					calculateCPUUsage(name+"_cpu_sys", out, sys)
					calculateCPUUsage(name+"_cpu_total", out, total)
				}
			}

			cgroupCpuCollect(c.pfsdcpumem, "pfsd")
			cgroupCpuCollect(c.pfsdToolCpumem, "pfsd_tool")
			cgroupCpuCollect(c.managerCpumem, "manager")
			cgroupCpuCollect(c.pauseCpumem, "pause")
			cgroupCpuCollect(c.podCpumem, "pod")
		}

		if cpuCoresLimit != 0 {
			calculateCPUUsage("cgroup_cpu_user", out, userCpu)
			calculateCPUUsage("cgroup_cpu_sys", out, sysCpu)
			calculateCPUUsage("cgroup_cpu_total", out, totalCpu)
			if _, ok := out["cgroup_cpu_total"]; ok {
				out["cpu_user_usage"] = out["cgroup_cpu_user"].(float64) / float64(cpuCoresLimit)
				out["cpu_sys_usage"] = out["cgroup_cpu_sys"].(float64) / float64(cpuCoresLimit)
				out["cpu_total_usage"] = out["cgroup_cpu_total"].(float64) / float64(cpuCoresLimit)
			}
		}
		c.logger.Debug("db_cpu_info",
			log.Float64("cpuCoresLimit", cpuCoresLimit),
			log.Uint64("userCpu", userCpu), log.Uint64("sysCpu", sysCpu),
			log.String("c.Envs[\"cgroup_cpuset_path\"]", c.Envs["cgroup_cpuset_path"]),
			log.String("c.Envs[\"cgroup_cpu_path\"]", c.Envs["cgroup_cpu_path"]))
	}

	cpuDetail, err := c.cpuacct.GetCpuDetail()
	if err != nil {
		c.logger.Warn("get cpu detail failed.", err)
	} else {
		calculateCPUUsage("cpu_iowait", out, cpuDetail.IoWait)
		calculateCPUUsage("cpu_idle", out, cpuDetail.Idle)
		calculateCPUUsage("cpu_irq", out, cpuDetail.Irq)
		calculateCPUUsage("cpu_softirq", out, cpuDetail.SoftIrq)
		out["cpu_nr_running"] = cpuDetail.NrRunning
		out["cpu_nr_uninterrupible"] = cpuDetail.NrUnInterrupible
	}

	cpuNrPeriods, cpuNrThrottled, throttledTime, err := c.cpumem.GetCpuStat()
	if err != nil {
		c.logger.Warn("get cpu limit failed.", err)
	} else {
		c.calcDeltaWithoutSuffix("cpu_nr_periods", out, float64(cpuNrPeriods))
		c.calcDeltaWithoutSuffix("cpu_nr_throttled", out, float64(cpuNrThrottled))
		c.calcDeltaWithoutSuffix("cpu_throttled_time", out, float64(throttledTime))
	}

	// CPU formalize
	c.buildOutDictSendToMultiBackend(out, "cpu_cores",
		"cpu_user_usage", "cpu_sys_usage", "cpu_total_usage",
		"cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_idle",
		"cpu_nr_running", "cpu_nr_uninterrupible",
		"cpu_nr_periods", "cpu_nr_throttled", "cpu_throttled_time")

	return nil
}

func (c *PolarDBPgCollector) collectCgroupMemory(out map[string]interface{}) error {
	var memlimitcgroup *cgroup.CPUMem

	memlimitcgroup = c.cpumem
	if (c.environment == "public_cloud" ||
		c.environment == "polarbox" ||
		c.environment == "private_cloud") && c.isPolarDB() {
		podlimit, err := c.podmem.GetMemoryLimit()
		if err != nil {
			c.logger.Warn("get pod mem limit failed.", err)
		} else {
			memlimit, err := c.cpumem.GetMemoryLimit()
			if err != nil {
				c.logger.Warn("get container mem limit failed", err)
				return err
			}

			if podlimit < memlimit {
				memlimitcgroup = c.podmem
			}
		}
	}

	mem, err := c.cpumem.GetMemoryUsage()
	if err != nil {
		c.logger.Warn("get mem metrics failed.", err)
		return err
	}
	out["mem_used"] = mem / 1024 / 1024 // MB

	memlimit, err := memlimitcgroup.GetMemoryLimit()
	if err != nil {
		c.logger.Warn("get mem limit failed.", err)
		return err
	}

	if memlimit > uint64(1024)*1024*1024*1024*1024 {
		log.Info("mem unlimited in cgroup")
		//无限内存
		memInfoBytes, err := ioutil.ReadFile("/proc/meminfo")
		if err == nil {
			var MemTotal, hugePageTotal, hugePageSize uint64

			memInfoStr := string(memInfoBytes)
			memInfoLines := strings.Split(memInfoStr, "\n")
			for _, memInfoLine := range memInfoLines {
				kv := strings.Split(memInfoLine, ":")
				if len(kv) == 2 {
					kv[0] = strings.TrimSpace(kv[0])
					kv[1] = strings.TrimSpace(kv[1])
					if kv[0] == "MemTotal" {
						kv[1] = strings.Split(kv[1], " ")[0]
						MemTotal, _ = strconv.ParseUint(kv[1], 10, 64)
					} else if kv[0] == "HugePages_Total" {
						hugePageTotal, _ = strconv.ParseUint(kv[1], 10, 64)
					} else if kv[0] == "Hugepagesize" {
						kv[1] = strings.Split(kv[1], " ")[0]
						hugePageSize, _ = strconv.ParseUint(kv[1], 10, 64)
					}
				}
			}

			// if c.isPolarDB() {
			// 	pfsdMemUsage, err := c.pfsdcpumem.GetMemoryUsage()
			// 	if err != nil {
			// 		c.logger.warn("get pfsd memory usage failed", err)
			// 	} else {
			// 		out["mem_used"] = (mem + pfsdMemUsage) / 1024 / 1024
			// 	}

			// 	pfsdMemStat, err := c.pfsdcpumem.GetMemoryStat()
			// 	if err != nil {
			// 		c.logger.warn("get pfsd memory stat failed", err)
			memlimit = (MemTotal - hugePageTotal*hugePageSize) * 1024
		} else {
			log.Error("read /proc/meminfo failed", log.String("err", err.Error()))
		}
	}

	out["cgroup_mem_limit"] = float64(memlimit) / 1024 / 1024 // MB
	out["mem_total"] = float64(memlimit) / 1024 / 1024

	memstat, err := c.cpumem.GetMemoryStat()
	if err != nil {
		c.logger.Warn("get cgroup memory failed.", err)
		return err
	}
	out["mem_rss"] = float64(memstat.Rss) / 1024 / 1024
	out["mem_cache"] = float64(memstat.Cache) / 1024 / 1024
	out["mem_mapped_file"] = float64(memstat.MappedFile) / 1024 / 1024
	out["mem_inactiverss"] = float64(memstat.InactiveAnon) / 1024 / 1024
	out["mem_inactivecache"] = float64(memstat.InActiveFile) / 1024 / 1024
	out["cgroup_mem_usage"] =
		float64(memstat.Rss+memstat.MappedFile) * 100 / float64(memlimit)
	out["mem_total_usage"] = out["cgroup_mem_usage"]
	out["mem_total_used"] = float64(memstat.Rss+memstat.MappedFile) / 1024 / 1024

	if c.isPolarDB() {
		cgroupMemCollect := func(cg *cgroup.CPUMem, name string) {
			memusage, err := cg.GetMemoryUsage()
			if err != nil {
				c.logger.Warn("get memory usage failed", err, log.String("container", name))
			} else {
				out[name+"_mem_used"] = float64(memusage) / 1024 / 1024
			}

			memstat, err := cg.GetMemoryStat()
			if err != nil {
				c.logger.Warn("get memory stat failed", err, log.String("container", name))
			} else {
				out[name+"_mem_rss"] = float64(memstat.Rss) / 1024 / 1024
				out[name+"_mem_cache"] = float64(memstat.Cache) / 1024 / 1024
				out[name+"_mem_mapped_file"] = float64(memstat.MappedFile) / 1024 / 1024
				out[name+"_mem_swap"] = float64(memstat.Swap) / 1024 / 1024
				out[name+"_mem_activeanon"] = float64(memstat.ActiveAnon) / 1024 / 1024
				out[name+"_mem_activefile"] = float64(memstat.ActiveFile) / 1024 / 1024
				out[name+"_mem_inactiveanon"] = float64(memstat.InactiveAnon) / 1024 / 1024
				out[name+"_mem_inactivefile"] = float64(memstat.InActiveFile) / 1024 / 1024
			}

			kmemUsage, err := cg.GetKernalMemoryUsage()
			if err != nil {
				c.logger.Warn("get kernel memory usage failed", err, log.String("container", name))
			} else {
				out[name+"_kmem_used"] = float64(kmemUsage) / 1024 / 1024
			}

		}

		cgroupMemCollect(c.pfsdcpumem, "pfsd")
		cgroupMemCollect(c.pfsdToolCpumem, "pfsd_tool")
		cgroupMemCollect(c.managerCpumem, "manager")
		cgroupMemCollect(c.pauseCpumem, "pause")
		cgroupMemCollect(c.cpumem, "engine")
		cgroupMemCollect(c.podCpumem, "pod")

		pfsdMemUsage, err := c.pfsdcpumem.GetMemoryUsage()
		if err != nil {
			c.logger.Warn("get pfsd memory usage failed", err)
		} else {
			out["mem_used"] = (mem + pfsdMemUsage) / 1024 / 1024
		}

		pfsdMemStat, err := c.pfsdcpumem.GetMemoryStat()
		if err != nil {
			c.logger.Warn("get pfsd memory stat failed", err)
		} else {
			out["mem_rss"] = float64(memstat.Rss+pfsdMemStat.Rss) / 1024 / 1024
			out["mem_cache"] = float64(memstat.Cache+pfsdMemStat.Cache) / 1024 / 1024
			out["mem_mapped_file"] = float64(memstat.MappedFile+pfsdMemStat.MappedFile) / 1024 / 1024
			out["mem_inactiverss"] = float64(memstat.InactiveAnon+pfsdMemStat.InactiveAnon) / 1024 / 1024
			out["mem_inactivecache"] = float64(memstat.InActiveFile+pfsdMemStat.InActiveFile) / 1024 / 1024
			if memlimitcgroup != c.cpumem {
				out["cgroup_mem_usage"] = float64(memstat.Rss+memstat.MappedFile+pfsdMemStat.Rss+pfsdMemStat.MappedFile) * 100 / float64(memlimit)
				out["mem_total_used"] = float64(memstat.Rss+memstat.MappedFile+pfsdMemStat.Rss+pfsdMemStat.MappedFile) / 1024 / 1024
				out["mem_total_usage"] = out["cgroup_mem_usage"]
			}
		}
	}

	// Memory formalize
	c.buildOutDictSendToMultiBackend(out,
		"mem_total", "mem_rss", "mem_cache", "mem_mapped_file", "mem_inactiverss", "mem_inactivecache",
		"cgroup_mem_usage", "mem_total_usage", "mem_total_used")

	out["hugetlb_usage_2m"] = uint64(0)
	if c.cInfo.enableHugePage {
		hmem, err := c.cpumem.GetHugePageMemory()
		if err != nil {
			c.logger.Warn("get huge mem metrics failed.", err)
			return err
		}

		out["hugetlb_usage_2m"] = hmem / 1024 / 1024
	}

	return nil
}

func (c *PolarDBPgCollector) collectCgroupIO(out map[string]interface{}) error {
	stat, err := c.cgroupio.GetIo()
	if err != nil {
		c.logger.Warn("get local io count failed.", err)
	} else {
		c.calcDeltaWithoutSuffix("local_iops", out, float64(stat.DataIo+stat.LogIo))
		c.calcDeltaWithoutSuffix("local_data_iops", out, float64(stat.DataIo))
		c.calcDeltaWithoutSuffix("local_wal_iops", out, float64(stat.LogIo))
	}

	stat, err = c.cgroupio.GetIoBytes()
	if err != nil {
		c.logger.Warn("get local throughput failed.", err)
	} else {
		c.calcDeltaWithoutSuffix("local_throughput", out, float64((stat.DataIo+stat.LogIo)/1024/1024))
		c.calcDeltaWithoutSuffix("local_data_throughput", out, float64(stat.DataIo/1024/1024))
		c.calcDeltaWithoutSuffix("local_wal_throughput", out, float64(stat.LogIo/1024/1024))
	}

	return nil
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

func (c *PolarDBPgCollector) collectSQLStat(
	queries map[string]db_config.QueryCtx,
	out map[string]interface{}) error {

	var columnsPtr []interface{}
	var cols []*sql.ColumnType

	for _, queryCtx := range queries {
		line := -1

		query := queryCtx.Query
		c.logger.Debug("executing query", log.String("query", query),
			log.Int("use snapshot", queryCtx.UseSnapshot),
			log.Bool("need snapshot", c.cInfo.dbNeedSnapshot))

		if queryCtx.Enable == 0 {
			continue
		}

		if c.cInfo.count%queryCtx.Cycle != 0 {
			if queryCtx.UseSnapshot == 0 {
				continue
			}

			if !c.cInfo.dbNeedSnapshot {
				continue
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
		rows, err := c.dbInfo.db.QueryContext(ctx, "/* rds internal mark */ "+query)
		if err != nil {
			c.logger.Error("executing query failed", err, log.String("query", query))
			goto RELEASE
		}

		cols, err = rows.ColumnTypes()
		if err != nil {
			c.logger.Error("get columns name failed", err, log.String("query", query))
			goto RELEASE
		}

		columnsPtr = make([]interface{}, len(cols))
		for i := range columnsPtr {
			col := cols[i]
			if queryCtx.WriteIntoLog != 0 {
				columnsPtr[i] = &sql.NullString{}
				continue

			}

			if strings.HasPrefix(col.Name(), "dim_") {
				columnsPtr[i] = &sql.NullString{}
				continue
			}
			columnsPtr[i] = &sql.NullFloat64{}
		}

		for rows.Next() {
			line += 1

			if err := rows.Scan(columnsPtr...); err != nil {
				c.logger.Error("row scan failed", err, log.String("query", query))
				continue
			}

			if queryCtx.WriteIntoLog != 0 {
				resultlist := make([]string, len(cols))
				for i, col := range cols {
					ptrString, ok := columnsPtr[i].(*sql.NullString)
					if ok {
						resultlist[i] = fmt.Sprintf("%s: %s", col.Name(), ptrString.String)
					} else {
						resultlist[i] = ""
						c.logger.Info("result may not be string", log.String("name", queryCtx.Name))
					}
				}
				c.logger.Info("db collect info", log.Int("line", line), log.String("name", queryCtx.Name),
					log.String("result", strings.Join(resultlist, ", ")))
				continue
			}

			// column name must the same as keys in schema.json
			// it will calculate delta for those column name which has "_delta" suffix
			for i, col := range cols {
				colName := col.Name()

				ptrInt, ok := columnsPtr[i].(*sql.NullFloat64)
				if !ok {
					c.logger.Error("parse to float64 error",
						errors.New("parse int error"),
						log.String("col", col.Name()))
					continue
				}

				if strings.HasSuffix(colName, "_delta") {
					c.calDeltaData(colName, c.cInfo.timeNow.Unix(), out, (ptrInt.Float64))
				} else {
					out[colName] = ptrInt.Float64
				}

				if queryCtx.SendToMultiBackend != 0 {
					c.buildOutDictSendToMultiBackend(out, colName)
				}
			}
		}

	RELEASE:
		if rows != nil {
			rows.Close()
		}

		if cancel != nil {
			cancel()
		}

	}

	if c.cInfo.dbNeedSnapshot {
		out["snapshot_no"] = uint64(c.cInfo.dbSnapshotNo)
		c.cInfo.dbNeedSnapshot = false
	}

	return nil
}

func (c *PolarDBPgCollector) collectDBStat(out map[string]interface{}) error {
	var err error
	if err = c.collectSQLStat(c.cInfo.querymap, out); err != nil {
		c.logger.Error("collect sql stat failed", err)
	}

	// other
	if _, ok := out["shared_memory_size_mb"]; ok {
		meta.GetMetaService().SetFloat("shared_memory_size_mb", strconv.Itoa(c.Port),
			out["shared_memory_size_mb"].(float64))
	}

	return nil
}

func (c *PolarDBPgCollector) calDeltaData(deltaname string, timestamp int64,
	out map[string]interface{}, value float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// orgname := strings.TrimSuffix(deltaname, "_delta")
	if orgvalue, ok := c.preValueMap[deltaname]; ok {
		if value >= orgvalue.Value && timestamp > orgvalue.LastTime {
			out[deltaname] = (value - orgvalue.Value) / float64(timestamp-orgvalue.LastTime)
		}
	}
	c.preValueMap[deltaname] = PreValue{LastTime: timestamp, Value: value}
	// out[orgname] = value
}

func (c *PolarDBPgCollector) calcDeltaWithoutSuffix(name string,
	out map[string]interface{}, value float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if orgvalue, ok := c.rawPre[name]; ok {
		if value >= orgvalue {
			out[name] = value - orgvalue
		}
	}
	// update previous data
	c.rawPre[name] = value
}

func (c *PolarDBPgCollector) execDB(sql string) error {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	_, err := c.dbInfo.db.ExecContext(ctx, "/* rds internal mark */ "+sql)
	if err != nil {
		c.logger.Error("query db failed", err)
		return err
	}

	return nil
}

func (c *PolarDBPgCollector) queryDB(sql string) (*sql.Rows, context.CancelFunc, error) {
	defer c.timeTrack(sql, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)

	rows, err := c.dbInfo.db.QueryContext(ctx, "/* rds internal mark */ "+sql)

	if err != nil {
		c.logger.Error("query db failed", err, log.String("sql", sql))
		return nil, cancel, err
	}
	return rows, cancel, nil
}

// stop this collector, release resources
func (c *PolarDBPgCollector) Stop() error {
	c.logger.Info("collector stop")
	if c.cInfo.enablePFS {
		c.importedCollector.removeInstance(c.plutoCtx, "",
			c.dirInfo.dataDir, c.dirInfo.newlogDir, c.dirInfo.baseDir)
	} else {
		c.importedCollector.removeInstance(c.plutoCtx, "",
			c.dirInfo.dataDir, c.dirInfo.newlogDir,
			c.dirInfo.baseDir, c.dirInfo.newWalDir)
	}

	for _, c := range c.asyncCollectionRunChannel {
		close(c)
	}

	for _, c := range c.asyncCollectionResultChannel {
		close(c)
	}

	c.prCollector.Stop()

	if c.cInfo.enablePFS {
		c.pfsCollector.Stop()
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

	return nil
}

func (c *PolarDBPgCollector) isPolarDB() bool {
	return c.dbType == "polardb_pg"
}

func (c *PolarDBPgCollector) GetMapValue(
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

func (c *PolarDBPgCollector) GetConfigMapValue(
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
			c.logger.Debug("[polardb_pg] cannot recognize this value type", log.String("type", valueType))
			return defValue
		}
	}

	c.logger.Debug("[polardb_pg] cannot get config map key", log.String("key", key))
	return defValue
}

func (c *PolarDBPgCollector) collectLocalVolumeCapacityWithDf(dfmap map[string]interface{}) error {
	var stdout, stderr bytes.Buffer
	dfCmdStr := fmt.Sprintf(
		"df --output=itotal,iused,iavail,ipcent,size,used,avail,pcent,source,target -a %s | "+
			"awk '{if(NR>1)print}'", c.cInfo.localDiskPath)

	cmd := exec.Command("/bin/sh", "-c", "timeout 10 "+dfCmdStr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		c.logger.Warn("exec df command failed", err,
			log.String("stderr", stderr.String()), log.String("dfcmd", dfCmdStr))
		return nil
	}

	c.logger.Debug("df output", log.String("out", stdout.String()))

	for _, line := range strings.Split(stdout.String(), "\n") {
		rlist := strings.Fields(line)
		if len(rlist) != 10 {
			c.logger.Info("df command split result not equal than 10",
				log.String("line", line), log.Int("length", len(rlist)))
			continue
		}

		dfmap["inodes_total"], _ = strconv.ParseUint(rlist[0], 10, 64)
		dfmap["inodes_used"], _ = strconv.ParseUint(rlist[1], 10, 64)
		dfmap["inodes_avail"], _ = strconv.ParseUint(rlist[2], 10, 64)
		dfmap["inodes_usage"], _ = strconv.ParseUint(strings.TrimRight(rlist[3], "%"), 10, 64)
		dfmap["fs_inodes_total"] = dfmap["inodes_total"]
		dfmap["fs_inodes_used"] = dfmap["inodes_used"]
		dfmap["fs_inodes_usage"] = dfmap["inodes_usage"]
		dfmap["size_total"], _ = strconv.ParseUint(rlist[4], 10, 64)
		dfmap["size_used"], _ = strconv.ParseUint(rlist[5], 10, 64)
		dfmap["size_avail"], _ = strconv.ParseUint(rlist[6], 10, 64)
		dfmap["size_usage"], _ = strconv.ParseUint(strings.TrimRight(rlist[7], "%"), 10, 64)
		dfmap["fs_blocks_total"] = dfmap["size_total"]
		dfmap["fs_blocks_used"] = dfmap["size_used"]
		dfmap["fs_blocks_usage"] = dfmap["size_usage"]
		dfmap["fs_size_total"] = float64(dfmap["size_total"].(uint64)) / 1024
		dfmap["fs_size_used"] = float64(dfmap["size_used"].(uint64)) / 1024
		dfmap["fs_size_usage"] = float64(dfmap["size_usage"].(uint64))

		break
	}

	return nil
}

func (c *PolarDBPgCollector) initCgroupCollectObj(cg *cgroup.CPUMem, envs map[string]string, name string) {
	cg.InitMemory(c.getContainerCgroupPath(envs[consts.CGroupMemPath], name, envs))
	if _, ok := envs[consts.CGroupCpuAcctPath]; ok {
		cg.InitCpu(c.getContainerCgroupPath(envs[consts.CGroupCpuAcctPath], name, envs))
	} else {
		cg.InitCpu(c.getContainerCgroupPath(envs[consts.CGroupCpuPath], name, envs))
	}
	c.logger.Info("cgroup path",
		log.String("container name", name),
		log.String("cpu", c.getContainerCgroupPath(envs[consts.CGroupCpuPath], name, envs)),
		log.String("memory", c.getContainerCgroupPath(envs[consts.CGroupMemPath], name, envs)))
	cg.InitMemory(c.getContainerCgroupPath(envs[consts.CGroupMemPath], name, envs))
	if _, ok := envs[consts.CGroupCpuAcctPath]; ok {
		cg.InitCpu(c.getContainerCgroupPath(envs[consts.CGroupCpuAcctPath], name, envs))
	} else {
		cg.InitCpu(c.getContainerCgroupPath(envs[consts.CGroupCpuPath], name, envs))
	}
	c.logger.Debug("pfsd cgroup path",
		log.String("cpu", c.getContainerCgroupPath(envs[consts.CGroupCpuPath], name, envs)),
		log.String("memory", c.getContainerCgroupPath(envs[consts.CGroupMemPath], name, envs)))
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
