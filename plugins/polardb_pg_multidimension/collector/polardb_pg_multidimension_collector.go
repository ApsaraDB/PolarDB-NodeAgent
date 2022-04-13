/*-------------------------------------------------------------------------
 *
 * polardb_pg_multidimension_collector.go
 *    Polardb multidimension collector
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
 *           plugins/polardb_pg_multidimension/collector/polardb_pg_multidimension_collector.go
 *-------------------------------------------------------------------------
 */
package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/db_config"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/logger"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
)

const Splitter = "^^^"
const (
	PolarMinReleaseDate                 = int64(20190101)
	MinDBVersion                        = -1 // means no limit
	MaxDBVersion                        = -1 // means no limit
	CollectMinDBVersion                 = 90200
	CollectMaxDBVersion                 = MaxDBVersion
	DBConfigCheckVersionInterval        = 5
	DBConnTimeout                       = 10
	DBQueryTimeout                      = 60
	basePath                            = "base_path"
	KEY_SEND_TO_MULTIBACKEND            = "send_to_multibackend"
	DefaultCollectLongIntervalThreshold = 600
	MIN_TRACKLOG_TIME                   = 500
)

const (
	All     = 0
	RW      = 1
	RO      = 2
	StandBy = 3
	DataMax = 4
	User    = 5
)

type PolarDBPgMultidimensionCollector struct {
	DataDir                        string
	Endpoint                       string
	Port                           int
	InsName                        string
	HostInsId                      int
	Role                           string
	PolarVersion                   string
	PolarReleaseDate               string
	SystemIdentifier               string
	UserName                       string
	Database                       string
	Envs                           map[string]string
	dbType                         string
	dbInfo                         *DBInfo
	cInfo                          *CollectorInfo
	preValueMap                    map[string]map[string]PreValue
	configCollectQueryContext      []interface{}
	configCollectInitContext       []interface{}
	configCollectConfigInitContext []interface{}
	SQLQueryTextMap                map[string]string
	dbConfig                       *db_config.DBConfig
	logger                         *logger.PluginLogger
	enableOutDictLog               bool
	ConfigMap                      map[string]interface{}
}

type CollectorInfo struct {
	querymap map[string]db_config.QueryCtx
	// interval               int64
	// timeNow                time.Time
	count                        int64
	dbConfigVersion              int64
	dbConfigCenterVersion        int64
	dbConfigNeedUpdate           bool
	dbSnapshotNo                 int64
	dbNeedSnapshot               bool
	enableDBConfig               bool
	enableDBConfigReleaseDate    string
	enableDBConfigCenter         bool
	dbConfigCenterHost           string
	dbConfigCenterPort           int
	dbConfigCenterUser           string
	dbConfigCenterPass           string
	dbConfigCenterDatabase       string
	dbConfigSchema               string
	commandInfos                 []interface{}
	longCollectIntervalThreshold int
	slowCollectionLogThreshold   int64
}

type DBInfo struct {
	username        string
	password        string
	database        string
	version         int64
	applicationName string
	releaseDate     int64
	db              *sql.DB
	configdb        *sql.DB
	role            int
	extensions      []string
	connections     map[string]*sql.DB
	configCenterdb  *sql.DB
}

type PreValue struct {
	LastTimestamp int64
	Value         float64
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

func NewPolarDBPgMultidimensionCollector() *PolarDBPgMultidimensionCollector {
	return &PolarDBPgMultidimensionCollector{
		dbInfo:   &DBInfo{},
		cInfo:    &CollectorInfo{},
		logger:   logger.NewPluginLogger("polardb_multidimension", nil),
		dbConfig: db_config.NewDBConfig(),
	}
}

func (c *PolarDBPgMultidimensionCollector) Init(m map[string]interface{}) error {
	var err error

	// wait a while
	time.Sleep(2 * time.Second)

	err = c.initParams(m)
	if err != nil {
		c.logger.Error("init params failed", err)
		return err
	}

	c.configCollectInitContext = c.GetMapValue(m, "collect_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectConfigInitContext = c.GetMapValue(m, "config_db_init_sqls",
		make([]interface{}, 0)).([]interface{})
	c.configCollectQueryContext = c.GetMapValue(m, "queries",
		make([]interface{}, 0)).([]interface{})

	err = c.initCollctor(m)
	if err != nil {
		c.logger.Error("init collector failed", err)
		goto ERROR
	}

	err = c.initQueries(c.configCollectQueryContext)
	if err != nil {
		c.logger.Error("init queries failed", err)
		goto ERROR
	}

	err = c.initMetaService()
	if err != nil {
		c.logger.Error("init for meta service failed", err)
		goto ERROR
	}

	c.logger.Info("init successfully")
	c.logger.Debug("init result",
		log.String("c", fmt.Sprintf("%+v", c)),
		log.String("cInfo", fmt.Sprintf("%+v", c.cInfo)),
		log.String("dbInfo", fmt.Sprintf("%+v", c.dbInfo)))

ERROR:
	if err != nil {
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

		return err
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) initParams(m map[string]interface{}) error {
	c.Port = m[consts.PluginContextKeyPort].(int)
	c.InsName = m[consts.PluginContextKeyInsName].(string)

	// init logger first
	logIdentifier := map[string]string{
		"ins_name": c.InsName,
		"port":     strconv.Itoa(c.Port),
	}
	c.logger.SetIdentifier(logIdentifier)

	c.logger.Debug("init with info", log.String("info", fmt.Sprintf("%+v", m)))
	c.logger.Info("init")
	c.enableOutDictLog = c.GetMapValue(m, "enable_outdict_log", false).(bool)

	// init params
	c.ConfigMap = c.GetMapValue(m, "configmap",
		make(map[string]interface{})).(map[string]interface{})

	c.dbType = m["dbtype"].(string)
	envs := m["env"].(map[string]string)
	if _, ok := envs["apsara.metric.ins_name"]; ok {
		// in docker
		c.HostInsId, _ = strconv.Atoi(envs["apsara.metric.ins_id"])
		c.DataDir = c.getDataDir(m[basePath].(string), envs)
		c.Endpoint = c.DataDir
		c.logger.Info("endpoint", log.String("endpoint", c.Endpoint))
	} else {
		c.DataDir = c.getDataDir(m[basePath].(string), envs)
		if socketpath, ok := envs["socket_path"]; ok {
			c.Endpoint = socketpath
		} else {
			c.Endpoint = fmt.Sprintf("%s/", m[basePath].(string))
		}
	}
	c.Envs = envs

	log.Debug("envs", log.String("envs", fmt.Sprintf("%+v", c.Envs)))

	c.cInfo.enableDBConfig = m["enable_dbconfig"].(bool)
	c.cInfo.enableDBConfigReleaseDate = c.GetMapValue(m, "enable_dbconfig_releasedate", "20211130").(string)
	c.cInfo.enableDBConfigCenter = c.GetMapValue(m, "enable_dbconfig_center", false).(bool)
	c.cInfo.dbConfigCenterHost = c.GetMapValue(m, "dbconfig_center_host", "").(string)
	c.cInfo.dbConfigCenterPort = int(c.GetMapValue(m, "dbconfig_center_port", float64(5432)).(float64))
	c.cInfo.dbConfigCenterUser = c.GetMapValue(m, "dbconfig_center_user", "postgres").(string)
	c.cInfo.dbConfigCenterPass = c.GetMapValue(m, "dbconfig_center_pass", "").(string)
	c.cInfo.dbConfigCenterDatabase = c.GetMapValue(m, "dbconfig_center_database", "postgres").(string)
	c.cInfo.dbConfigSchema = m["dbconfig_schema"].(string)
	c.cInfo.slowCollectionLogThreshold = int64(c.GetMapValue(m, "slow_collection_log_thres", float64(MIN_TRACKLOG_TIME)).(float64))
	c.cInfo.commandInfos = c.GetMapValue(m, "commands", make([]interface{}, 0)).([]interface{})
	c.cInfo.longCollectIntervalThreshold = int(c.GetMapValue(m, "long_collect_interval_threshold",
		float64(DefaultCollectLongIntervalThreshold)).(float64))
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
	log.Debug("user and database",
		log.String("username", c.UserName), log.String("database", c.Database))
	c.dbInfo.password = m[consts.PluginContextKeyPassword].(string)
	c.dbInfo.applicationName = m["application_name"].(string)
	c.dbInfo.extensions = make([]string, 0)
	for _, extension := range m["extensions"].([]interface{}) {
		c.dbInfo.extensions = append(c.dbInfo.extensions, extension.(string))
	}
	c.preValueMap = make(map[string]map[string]PreValue)

	c.cInfo.count = 0

	return nil
}

func (c *PolarDBPgMultidimensionCollector) initCollctor(m map[string]interface{}) error {
	// connect to unix socket
	var err error

	dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d fallback_application_name=%s "+
		"sslmode=disable connect_timeout=%d",
		c.Endpoint, c.dbInfo.username, c.dbInfo.database, c.Port,
		c.dbInfo.applicationName, DBConnTimeout)

	c.dbInfo.connections = make(map[string]*sql.DB)
	c.dbInfo.db, err = sql.Open("postgres", dbUrl)
	if err != nil {
		c.logger.Error("connect to db failed", err)
		c.dbInfo.db = nil
		return err
	}

	c.logger.Info("create db connection ok")

	err = c.dbInfo.db.Ping()
	if err != nil {
		c.logger.Error("ping db failed", err)
		return err
	}

	c.dbInfo.db.Exec("SET log_min_messages=FATAL")

	c.dbInfo.version, err = c.getDBVersion("Postgres Version", "SHOW server_version_num")
	if err != nil {
		c.logger.Error("get db version failed", err)
		return err
	}

	c.dbInfo.releaseDate = PolarMinReleaseDate
	if c.isPolarDB() {
		c.dbInfo.releaseDate, err = c.getDBVersion("PolarDB Release Date", "SHOW polar_release_date")
		if err != nil {
			c.dbInfo.releaseDate = PolarMinReleaseDate
			c.logger.Warn("get polardb release date failed", err)
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

		c.PolarVersion, err = c.getDBVersionString("PolarDB Version", "SHOW polar_version")
		if err != nil {
			c.logger.Warn("get polar_version failed", err)
		}
	}

	if c.SystemIdentifier, err =
		c.getValueFromDB("SELECT system_identifier FROM pg_control_system()"); err != nil {
		c.logger.Warn("get system identifier from db failed", err)
	}

	if c.dbInfo.role, err = c.getDBRole(); err != nil {
		c.logger.Error("get db role failed.", err)
		return err
	}

	c.logger.Info("db role", log.Int("role", c.dbInfo.role))

	if c.dbInfo.role == RW {
		if err = c.prepareDBCollect(); err != nil {
			c.logger.Error("prepare db collect failed", err)
		}
	}

	configdb := "postgres"
	if _, ok := m["configdb"]; ok {
		configdb = m["configdb"].(string)
	}

	if c.dbInfo.database != configdb {
		configDBUrl := fmt.Sprintf("host=%s user=%s dbname=postgres port=%d "+
			"fallback_application_name=%s sslmode=disable connect_timeout=%d",
			c.Endpoint, c.dbInfo.username, c.Port, "ue_multidimension_configdb", DBConnTimeout)

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

	c.dbInfo.configCenterdb = nil
	if c.cInfo.enableDBConfigCenter {
		configCenterDBURL := fmt.Sprintf("host=%s user=%s dbname=%s port=%d password=%s "+
			"fallback_application_name=%s sslmode=disable connect_timeout=%d",
			c.cInfo.dbConfigCenterHost,
			c.cInfo.dbConfigCenterUser,
			c.cInfo.dbConfigCenterDatabase,
			c.cInfo.dbConfigCenterPort,
			c.cInfo.dbConfigCenterPass,
			"ue_multidimension_configcenter",
			DBConnTimeout)

		if c.dbInfo.configCenterdb, err = sql.Open("postgres", configCenterDBURL); err != nil {
			c.logger.Warn("connect to config center db failed, use origin database for config", err,
				log.String("database", c.cInfo.dbConfigCenterDatabase))
		} else {
			c.dbInfo.configCenterdb.SetMaxIdleConns(1)
			c.dbInfo.configCenterdb.Exec("SET log_min_messages=FATAL")
			c.logger.Info("connect config center db success", log.String("url", configCenterDBURL))
		}
	}

	if c.cInfo.enableDBConfig {
		err = c.dbConfig.Init(c.InsName, "polardb_pg_multidimension",
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

	return nil
}

func (c *PolarDBPgMultidimensionCollector) initQueries(queryContexts []interface{}) error {
	// init query context
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
		UseSnapshot:      1,
		OriginQuery:      "",
	}

	c.cInfo.querymap = make(map[string]db_config.QueryCtx)
	for i, query := range queryContexts {
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
			c.logger.Info("sql is disabled", log.String("name", queryCtx.Name))
		}

		if queryCtx.MinVersion != MinDBVersion && c.dbInfo.version < queryCtx.MinVersion {
			c.logger.Info("instance version is too low",
				log.Int64("conf_version", queryCtx.MinVersion),
				log.Int64("ins_version", c.dbInfo.version),
				log.String("name", queryCtx.Name))
			continue
		}

		if queryCtx.MaxVersion != MaxDBVersion && c.dbInfo.version > queryCtx.MaxVersion {
			log.Info("instance version is high",
				log.Int64("conf_version", queryCtx.MaxVersion),
				log.Int64("ins_version", c.dbInfo.version),
				log.String("name", queryCtx.Name))
			continue
		}

		if c.isPolarDB() {
			if c.dbInfo.releaseDate < queryCtx.PolarReleaseDate {
				log.Info("instance release date is older",
					log.Int64("conf_release_date", queryCtx.PolarReleaseDate),
					log.Int64("ins_release_date", c.dbInfo.releaseDate),
					log.String("name", queryCtx.Name))
				continue
			}
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

		if c.dbInfo.role == DataMax {
			c.logger.Info("this db is DataMax, would not execute any sql")
			continue
		}

		if queryCtx.Name == "Default" {
			c.logger.Warn("query ctx does not have a name, "+
				"this may cause problems when using db config",
				errors.New("query ctx does not have a name"), log.String("query", queryCtx.Query))
			queryCtx.Name = strconv.Itoa(i + 100)
		}

		c.cInfo.querymap[queryCtx.Name] = queryCtx
	}

	if c.cInfo.enableDBConfig {
		var err error
		if c.dbInfo.role != DataMax {
			if err = c.dbConfig.InitFromDBConfig(c.ConfigMap, c.cInfo.querymap,
				c.dbInfo.role == RW); err != nil {
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

	i := 1
	for _, qctx := range c.cInfo.querymap {
		// calculate offset for not doing collection altogether
		if qctx.Cycle > 1 && qctx.Offset == 0 {
			qctx.Offset = int64(i) % qctx.Cycle
		}

		i += 1
		c.logger.Debug("query context", log.String("name", qctx.Name),
			log.String("context", fmt.Sprintf("%+v", qctx)))
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) initMetaService() error {
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

func (c *PolarDBPgMultidimensionCollector) collectSQL(query db_config.QueryCtx) (*[]map[string]interface{}, error) {
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

		singleMsgContext, err := c.collectSingleSQL(query, dbName)
		if err != nil {
			c.logger.Warn("collect single SQL error", err,
				log.String("dbName", dbName), log.String("query.Query", query.Query))
		}
		msgContext = append(msgContext, singleMsgContext...)
	}

	return &msgContext, nil

}

func (c *PolarDBPgMultidimensionCollector) getAllDBName() ([]string, error) {
	var DBList []string

	querySQL := "SELECT datname FROM pg_database WHERE datname NOT LIKE 'template%'"
	rows, cancel, err := c.queryDB(querySQL)
	if cancel != nil {
		defer cancel()
	}
	if err != nil {
		c.logger.Warn("get all db name, exec sql error", err, log.String("querySQL", querySQL))
		return DBList, err
	}
	defer rows.Close()
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			c.logger.Warn("get all db name, rows scan error", err, log.String("querySQL", querySQL))
			return DBList, err
		}
		DBList = append(DBList, dbName)
	}

	return DBList, nil
}

func (c *PolarDBPgMultidimensionCollector) getConnection(dbName string) (*sql.DB, error) {
	var ok bool
	var conn *sql.DB

	if conn, ok = c.dbInfo.connections[dbName]; !ok {
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
		if err != nil {
			cancel()
			c.logger.Error("ping db failed before exec query", err)
			return nil, err
		}
		cancel()

		conn = tempDb
		c.dbInfo.connections[dbName] = conn

		conn.Exec("SET log_min_messages=FATAL")
		conn.SetMaxIdleConns(0)
	}

	return conn, nil
}

func (c *PolarDBPgMultidimensionCollector) execQuerySQL(query string, dbName string) (*sql.Rows, context.CancelFunc, error) {
	var conn *sql.DB

	conn, err := c.getConnection(dbName)
	if err != nil {
		c.logger.Error("get db connection failed", err)
		return nil, nil, err
	}

	defer c.timeTrack(dbName+": "+query, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)

	rows, err := conn.QueryContext(ctx, "/* rds internal mark */ "+query)
	if err != nil {
		c.logger.Error("query db failed", err)
		return rows, cancel, err
	}

	return rows, cancel, nil
}

func (c *PolarDBPgMultidimensionCollector) collectSingleSQL(qctx db_config.QueryCtx,
	dbName string) ([]map[string]interface{}, error) {

	var valRows *sql.Rows
	var cancel context.CancelFunc
	var msgContext []map[string]interface{}
	var err error
	c.logger.Debug("exec query", log.String("db", dbName), log.String("query", qctx.Query))

	if !qctx.QueryAllDB {
		valRows, cancel, err = c.queryDB(qctx.Query)
		if cancel != nil {
			defer cancel()
		}
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
		valRows, cancel, err = c.execQuerySQL(qctx.Query, dbName)
		if cancel != nil {
			defer cancel()
		}

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

	infos, err := ParseCols(cols)
	if err != nil {
		c.logger.Warn("parse columns failed.", err,
			log.String("sql", qctx.Query))
		return msgContext, err
	}

	columnsPtr := make([]interface{}, len(cols))
	for i := range columnsPtr {
		col := cols[i]
		if strings.HasPrefix(col.Name(), "dim_") {
			columnsPtr[i] = &sql.NullString{}
			continue
		}

		if strings.HasPrefix(col.Name(), "rt_") ||
			strings.HasPrefix(col.Name(), "delta_") ||
			strings.HasPrefix(col.Name(), "rate_") {
			columnsPtr[i] = &sql.NullFloat64{}
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

		ns := qctx.DataModel + "_" + dbName
		dimensionKey := ""
		kv := map[string]interface{}{}
		for i, col := range cols {
			// 维度数据
			if strings.HasPrefix(col.Name(), "dim_") {
				ptrString, ok := columnsPtr[i].(*sql.NullString)
				if ok {
					kv[NormalizeColumn(col.Name())] = ptrString.String
					dimensionKey += (ptrString.String + "_")
				}
				continue
			}

			ptrInt, ok := columnsPtr[i].(*sql.NullFloat64)
			if !ok {
				c.logger.Warn("parse to float error", errors.New("parse int error"))
				continue
			}

			// 增量数据
			if strings.HasPrefix(col.Name(), "delta_") {
				k := dimensionKey + col.Name()
				c.CalDeltaData(ns, k, kv, NormalizeColumn(col.Name()), time.Now().Unix(),
					ptrInt.Float64, false, qctx.Cycle >= int64(c.cInfo.longCollectIntervalThreshold))
				continue
			}

			if strings.HasPrefix(col.Name(), "rate_") {
				k := dimensionKey + col.Name()
				c.CalDeltaData(ns, k, kv,
					NormalizeColumn(col.Name()), time.Now().Unix(),
					ptrInt.Float64, true, qctx.Cycle >= int64(c.cInfo.longCollectIntervalThreshold))
				continue
			}

			// 实时数据
			if strings.HasPrefix(col.Name(), "rt_") {
				kv[NormalizeColumn(col.Name())] = ptrInt.Float64
				continue
			}

			kv[NormalizeColumn(col.Name())] = ptrInt.Float64
		}
		msgContext = append(msgContext, kv)
	}

	if rowcount > 1000 {
		c.logger.Info("row count may be too large",
			log.String("db", dbName),
			log.String("query", qctx.Query),
			log.Int("rowcount", rowcount))
	}

	for _, process := range infos.process {
		var err error
		msgContext, err = process(msgContext, infos)
		if err != nil {
			c.logger.Warn("process failed", err)
			return msgContext, err
		}
		infos.step++
	}

	return msgContext, nil
}

func (c *PolarDBPgMultidimensionCollector) getDataDir(basepath string, env map[string]string) string {
	if path, ok := env["host_data_dir"]; ok {
		if path != "" {
			return env["host_data_dir"]
		}
	}

	if path, ok := env["host-data-dir"]; ok {
		if path != "" {
			return env["host-data-dir"]
		}
	}

	return fmt.Sprintf("%s/%d/data", basepath, c.HostInsId)
}

func (p *PolarDBPgMultidimensionCollector) CalDeltaData(ns string,
	deltaName string, kv map[string]interface{},
	key string, timestamp int64, value float64, do_rate, do_record_start bool) float64 {

	var result float64

	if nsmap, ok := p.preValueMap[ns]; ok {
		if oldValue, ok := nsmap[deltaName]; ok {
			if do_rate {
				if value >= oldValue.Value && timestamp-oldValue.LastTimestamp > 0 {
					result = (value - oldValue.Value) / float64(timestamp-oldValue.LastTimestamp)
					kv[key] = result
					if do_record_start {
						kv["begin_snapshot_time"] = strconv.FormatInt(oldValue.LastTimestamp, 10)
					}
				} else {
					result = float64(0)
				}
			} else {
				if value >= oldValue.Value {
					result = (value - oldValue.Value)
					kv[key] = result
					if do_record_start {
						kv["begin_snapshot_time"] = strconv.FormatInt(oldValue.LastTimestamp, 10)
					}
				} else {
					result = float64(0)
				}
			}
		}
		nsmap[deltaName] = PreValue{LastTimestamp: timestamp, Value: value}
	} else {
		nsmap := make(map[string]PreValue)
		nsmap[deltaName] = PreValue{LastTimestamp: timestamp, Value: value}
		p.preValueMap[ns] = nsmap
	}

	return result
}

func (c *PolarDBPgMultidimensionCollector) CollectSysInfo(out map[string]interface{}) error {
	out["sys_info"] = make([]map[string]interface{}, 0)
	c.logger.Debug("commands", log.String("command info", fmt.Sprintf("%+v", c.cInfo.commandInfos)))
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
				sysinfo["count"] = 1 // no use at all
				out["sys_info"] = append(out["sys_info"].([]map[string]interface{}), sysinfo)
			}
		}
	}

	c.logger.Debug("sys info", log.String("sys info", fmt.Sprintf("%+v", out)))

	return nil
}

func (c *PolarDBPgMultidimensionCollector) Collect(out map[string]interface{}) error {

	sendToMultiBackendMap := make(map[string]interface{})

	if c.cInfo.count == 5 || c.cInfo.count%3600 == 5 {
		if err := c.CollectSysInfo(sendToMultiBackendMap); err != nil {
			c.logger.Warn("collect sys info failed", err)
		}
	}

	if err := c.checkIfNeedRestart(); err != nil {
		c.logger.Warn("we need to restart", err)
		return err
	}

	if c.GetConfigMapValue(c.ConfigMap,
		"enable_db_multidimension_collect", "integer", 1).(int) == 0 {
		// TODO(wormhole.gl): not exit?
		c.logger.Debug("enable_db_multidimension_collect is off")
	}

	if c.GetConfigMapValue(c.ConfigMap,
		"enable_ro_multidimension_collect", "integer", 0).(int) == 0 &&
		c.dbInfo.role != RW {
		// TODO(wormhole.gl): not exit?
		c.logger.Debug("enable_ro_multidimension_collect is off")
	}

	for name, query := range c.cInfo.querymap {
		c.logger.Debug("query snapshot",
			log.String("query", query.Query), log.Int("use snapshot", query.UseSnapshot),
			log.Bool("need snapshot", c.cInfo.dbNeedSnapshot))

		if query.Enable == 0 {
			continue
		}

		if c.cInfo.count%query.Cycle-query.Offset != 0 {
			if query.UseSnapshot == 0 {
				continue
			}

			if !c.cInfo.dbNeedSnapshot {
				continue
			}
		}

		if int(query.Cycle) >= c.cInfo.longCollectIntervalThreshold {
			c.logger.Info("collect long interval metrics",
				log.String("name", query.Name),
				log.Int64("cycle", query.Cycle))
		}

		collectTime := time.Now().UnixNano() / 1e6

		msgContext, err := c.collectSQL(query)
		if err != nil {
			c.logger.Warn("collect failed", err, log.String("sql", query.Query))
			continue
		}

		duration := time.Now().UnixNano()/1e6 - collectTime
		if duration >= 1500 {
			c.logger.Info("slow collection query",
				log.String("name", name),
				log.Int64("duration", duration))
		}

		if len(*msgContext) == 0 {
			continue
		}

		sendToMultiBackendMap[query.DataModel] = *msgContext
	}

	if c.cInfo.dbNeedSnapshot {
		c.cInfo.dbNeedSnapshot = false
	}

	c.cInfo.count += 1

	out[KEY_SEND_TO_MULTIBACKEND] = sendToMultiBackendMap

	if c.enableOutDictLog {
		c.logger.Info("out data dict", log.String("out", fmt.Sprintf("%+v", sendToMultiBackendMap)))
	}

	return nil
}

func (p *PolarDBPgMultidimensionCollector) Close() error {
	if p.dbInfo.db != nil {
		p.dbInfo.db.Close()
		p.dbInfo.db = nil
	}

	if p.dbInfo.configdb != nil {
		p.dbInfo.configdb.Close()
		p.dbInfo.configdb = nil
	}

	if p.dbInfo.configCenterdb != nil {
		p.dbInfo.configCenterdb.Close()
		p.dbInfo.configCenterdb = nil
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) getDBVersion(prefix string, query string) (int64, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get db version", err)
		return 0, err
	}
	defer rows.Close()

	// Value
	var value sql.NullInt64

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return 0, err
		}

		if !value.Valid {
			err := errors.New("version value is invalid")
			c.logger.Error("version value is invalid", err)
			return 0, err
		}

		version := int64(value.Int64)
		c.logger.Info(prefix, log.Int64("version", version))

		return version, nil
	}

	return 0, nil
}

func (c *PolarDBPgMultidimensionCollector) getDBVersionString(prefix, query string) (string, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get db version", err)
		return "", err
	}
	defer rows.Close()

	// Value
	var value sql.NullString

	if rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			c.logger.Error("scan for values failed", err)
			return "", err
		}

		if !value.Valid {
			err := errors.New("version value is invalid")
			c.logger.Error("version value is invalid", err)
			return "", err
		}

		version := value.String
		c.logger.Info(prefix, log.String("version", version))

		return version, nil
	}

	return "", nil
}

func (c *PolarDBPgMultidimensionCollector) prepareDBCollect() error {
	err := c.preparePolarExtensions()
	if err != nil {
		c.logger.Warn("prepare monitor extension failed", err)
	}

	err = c.prepareObjects()
	if err != nil {
		c.logger.Warn("prepare view failed", err)
	}

	return err
}

func (c *PolarDBPgMultidimensionCollector) prepareObjects() error {
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
			c.logger.Debug("sql is disabled", log.String("query", queryCtx.Query))
			continue
		}

		if err := c.execDB(queryCtx.Query); err != nil {
			c.logger.Warn("exec collect init sql failed", err, log.String("query", queryCtx.Query))
		} else {
			c.logger.Debug("exec collect init sql succeed", log.String("query", queryCtx.Query))
		}
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) preparePolarExtensions() error {
	for _, extension := range c.dbInfo.extensions {
		err := c.execDB(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", extension))
		if err != nil {
			c.logger.Warn("create extension failed.", err, log.String("extension", extension))
		}

		err = c.execDB(fmt.Sprintf("ALTER EXTENSION %s UPDATE", extension))
		if err != nil {
			c.logger.Warn("update extension failed", err, log.String("extension", extension))
		}
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) execDB(sql string) error {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	_, err := c.dbInfo.db.ExecContext(ctx, "/* rds internal mark */ "+sql)
	if err != nil {
		c.logger.Error("query db failed", err)
		return err
	}

	return nil
}

func (c *PolarDBPgMultidimensionCollector) queryDB(sql string) (*sql.Rows, context.CancelFunc, error) {
	defer c.timeTrack(sql, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)

	rows, err := c.dbInfo.db.QueryContext(ctx, "/* rds internal mark */ "+sql)
	if err != nil {
		// c.logger.Error("query db failed", err)
		return nil, cancel, err
	}
	return rows, cancel, nil
}

func (c *PolarDBPgMultidimensionCollector) isPolarDB() bool {
	return c.dbType == "polardb_pg"
}

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

func (c *PolarDBPgMultidimensionCollector) getDBRole() (int, error) {
	var inRecoveryMode bool
	var err error

	if inRecoveryMode, err = c.isInRecoveryMode(); err != nil {
		c.logger.Error("check with recovery mode failed.", err)
		c.Role = "Standby"
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

func (c *PolarDBPgMultidimensionCollector) getNodeType() (string, error) {
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

func (c *PolarDBPgMultidimensionCollector) isInRecoveryMode() (bool, error) {
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

func (c *PolarDBPgMultidimensionCollector) checkIfNeedRestart() error {
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

	// when config version changed, we need to reload config
	if c.cInfo.enableDBConfig {
		if c.cInfo.dbConfigNeedUpdate {
			c.logger.Info("db config version update")
			return errors.New("db config version update, we must reinit to reload")
		}

		if c.dbInfo.role != DataMax {
			if c.cInfo.count%DBConfigCheckVersionInterval == 0 {
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
					c.logger.Info("db snapshot needed",
						log.Int64("snapshot no", c.cInfo.dbSnapshotNo))
				}
			}
		}
	}

	return nil
}

// TODO(wormhole.gl): merge these two functions
func (c *PolarDBPgMultidimensionCollector) GetMapValue(m map[string]interface{},
	key string, defValue interface{}) interface{} {
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

	c.logger.Debug("map does not have key", log.String("key", key))

	return defValue
}

func (c *PolarDBPgMultidimensionCollector) GetConfigMapValue(m map[string]interface{},
	key string, valueType string, defValue interface{}) interface{} {
	if v, ok := m[key]; ok {
		value, ok := v.(string)
		if !ok {
			c.logger.Debug("config value is not string",
				log.String("key", key),
				log.String("value", fmt.Sprintf("%+v", v)))
			return defValue
		}

		switch valueType {
		case "string":
			return value
		case "integer":
			if intv, err := strconv.Atoi(value); err == nil {
				return intv
			} else {
				c.logger.Warn("config value type is not integer", err,
					log.String("key", key), log.String("value", fmt.Sprintf("%+v", intv)))
				return defValue
			}
		default:
			c.logger.Debug("cannot recognize this value type", log.String("type", valueType))
			return defValue
		}
	}

	c.logger.Debug("cannot get config map key", log.String("key", key))
	return defValue
}

func (c *PolarDBPgMultidimensionCollector) getValueFromDB(query string) (string, error) {
	rows, cancel, err := c.queryDB(query)
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		c.logger.Error("cannot get db value", err)
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

func (c *PolarDBPgMultidimensionCollector) timeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() >= c.cInfo.slowCollectionLogThreshold {
		c.logger.Info("slow collection",
			log.String("function", key),
			log.Int64("thres", c.cInfo.slowCollectionLogThreshold),
			log.Int64("elapsed", elapsed.Milliseconds()))
	}
}
