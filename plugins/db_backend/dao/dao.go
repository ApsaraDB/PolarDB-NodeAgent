/*-------------------------------------------------------------------------
 *
 * dao.go
 *    Dao for database backend
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
 *           plugins/db_backend/dao/dao.go
 *-------------------------------------------------------------------------
 */
package dao

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/db_config"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/logger"
)

const DATAMODEL_CONFIG_TABLE = "meta_data_model_config"
const TABLE_CONFIG_VERSION = "meta_data_model_version"
const DBConnTimeout = 10
const DBQueryTimeout = 60
const ASYNC_CHANNEL_SIZE = 50

const (
	RETENTION_HOUR    = 03
	RETENTION_MIN     = 23
	RETENTION_SECOND  = 43
	MIN_TRACKLOG_TIME = 500
)

var RETENTION_INTERVAL int = 3600
var RETENTION_TIME int = 3 * 24 * 60 * 60

type Database struct {
	Name        string    `json:"name"`
	DBTemplate  *Instance `json:"dbtemplate"`
	InstanceMap map[string]*Instance
}

type Instance struct {
	DataType string
	Name     string
	InsInfo  *InsInfo
	DBInfo   *DBInfo
	db       *sql.DB
	mutex    sync.Mutex
	hasinit  bool
	version  uint64
	dimcache map[string]int
	logger   *logger.PluginLogger

	Tables   []*TimeSeriesTable `json:"tables"`
	TableMap map[string]*TimeSeriesTable

	routinemap       map[string]int
	retentionStopper chan int
	aggStoppers      map[string]chan int

	HighPriorityAsyncChannel chan *AsyncInsertData
	LowPriorityAsyncChannel  chan *AsyncInsertData
	AsyncInsertStopper       chan int
}

type AsyncInsertData struct {
	name   string
	schema []string
	data   [][]interface{}
}

type DBInfo struct {
	InsName  string
	Host     string
	Port     int
	UserName string
	Password string
	Role     string

	DBPath string // base path for sqlite
	DBName string
	Schema string
	DBType string
}

type InsInfo struct {
	LogicalInsName  string
	PhysicalInsName string
}

type DBManager struct {
	Databases []*Database `json:"db_schemas"`
	dbconf    map[string]*Database
	path      string
	logger    *logger.PluginLogger

	mutex    sync.Mutex
	insmutex map[string]*sync.Mutex
	hasinit  bool
}

var manager *DBManager
var once sync.Once

func NewDBManager() *DBManager {
	once.Do(func() {
		manager = &DBManager{}
	})

	return manager
}

func (s *DBManager) Init(conf string, path string) error {
	var err error

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.hasinit {
		s.logger = logger.NewPluginLogger("db_manager", make(map[string]string))

		if err = s.initFromConf(conf); err != nil {
			s.logger.Error("init db template failed", err, log.String("conf", conf))
			return err
		}

		s.insmutex = make(map[string]*sync.Mutex)
		s.path = path
		s.hasinit = true
	} else {
		s.logger.Info("db manager has been initialized already")
	}

	return nil
}

func (s *DBManager) GetOrInitInstance(
	dbname string,
	insname string,
	insinfo *InsInfo,
	dbinfo *DBInfo,
	initsqls []db_config.QueryCtx) (*Instance, error) {

	var inslock *sync.Mutex
	var ok bool

	s.mutex.Lock()
	if inslock, ok = s.insmutex[dbname+"_"+insname]; !ok {
		inslock = &sync.Mutex{}
		s.insmutex[dbname+"_"+insname] = inslock
	}
	s.mutex.Unlock()

	inslock.Lock()
	defer inslock.Unlock()

	s.mutex.Lock()

	db, ok := s.dbconf[dbname]
	if !ok {
		err := fmt.Errorf("cannot find database dbname[%s], insname[%s]", dbname, insname)
		s.logger.Error("cannot find database", err,
			log.String("db", dbname),
			log.String("insname", insname),
			log.Int("size", len(s.dbconf)))
		s.mutex.Unlock()
		return nil, err
	}

	if dbins, dbok := db.InstanceMap[insname]; dbok {
		oldinfo := dbins.DBInfo

		if oldinfo.Host == dbinfo.Host &&
			oldinfo.Port == dbinfo.Port &&
			oldinfo.UserName == dbinfo.UserName &&
			oldinfo.DBName == dbinfo.DBName &&
			oldinfo.Schema == dbinfo.Schema {
			s.mutex.Unlock()
			return dbins, nil
		} else {
			s.logger.Info("db info changed, we try to reinitialize it",
				log.String("db", dbname), log.String("insname", insname),
				log.String("origin dbinfo", fmt.Sprintf("%+v", oldinfo)),
				log.String("new dbinfo", fmt.Sprintf("%+v", dbinfo)))

			dbins.Stop()
			delete(db.InstanceMap, insname)
		}
	}

	s.mutex.Unlock()

	ins := db.DBTemplate.DeepCopy()
	dbinfo.DBPath = s.path
	if err := ins.Init(insname, insinfo, dbinfo, initsqls); err != nil {
		s.logger.Error("init ins failed", err,
			log.String("dbname", dbname),
			log.String("insname", insname),
			log.String("error", err.Error()))
		return nil, err
	}

	s.logger.Info("init ins successfully",
		log.String("dbname", dbname), log.String("insname", insname))

	s.mutex.Lock()
	db.InstanceMap[insname] = ins
	s.mutex.Unlock()

	return ins, nil
}

func (s *DBManager) StopInstance(dbname string, insname string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	db, ok := s.dbconf[dbname]
	if !ok {
		s.logger.Warn("cannot find database to stop", nil,
			log.String("db", dbname),
			log.String("ins", insname))
		return nil
	}

	if dbins, dbok := db.InstanceMap[insname]; dbok {
		dbins.Stop()
		delete(db.InstanceMap, insname)
	}

	return nil
}

func (s *DBManager) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Info("stop db manager begin")

	for dbname, db := range s.dbconf {
		for insname, ins := range db.InstanceMap {
			if err := ins.Stop(); err != nil {
				s.logger.Warn("stop db manager failed", err,
					log.String("db", dbname),
					log.String("ins", insname))
			}
		}
	}

	return nil
}

func (s *DBManager) initFromConf(jsonconf string) error {
	s.dbconf = make(map[string]*Database)

	err := json.Unmarshal([]byte(jsonconf), s)
	if err != nil {
		s.logger.Error("failed to unmarshal json conf", err)
		return err
	}

	for _, db := range s.Databases {
		db.InstanceMap = make(map[string]*Instance)

		database := db.DBTemplate
		database.hasinit = false
		database.db = nil
		database.TableMap = make(map[string]*TimeSeriesTable)

		for _, t := range database.Tables {
			initTable(t)
			database.TableMap[t.Name] = t
			s.logger.Debug("init table", log.String("table", fmt.Sprintf("%+v", t)))
		}

		s.dbconf[db.Name] = db

		s.logger.Info("init db conf success", log.String("db", db.Name))
	}

	return nil
}

func initTable(t *TimeSeriesTable) {
	t.TagMap = make(map[string]*Column)
	t.ValueMap = make(map[string]*Column)

	t.FactTable = &FactTable{TagMap: make(map[string]*Column), ValueMap: make(map[string]*Column)}
	t.DimensionTableMap = make(map[string]*DimensionTable)
	t.AggregationMap = make(map[string]Aggregation)

	if t.Tags == nil {
		t.Tags = make([]*Column, 0)
	}

	t.addDefaultColumn()

	for _, tag := range t.Tags {
		if tag.DimTable != nil {
			tag.DimTable.Name = tag.Name

			for _, name := range tag.DimTable.DimValue {
				c := *tag
				c.Name = name
				c.Type = "TEXT"
				c.DimTable = tag.DimTable
				t.TagMap[name] = &c
			}
		} else {
			t.TagMap[tag.Name] = tag
		}

		t.FactTable.TagMap[tag.Name] = tag
		if tag.DimTable != nil {
			t.DimensionTableMap[tag.Name] = tag.DimTable
		}

		log.Debug("[dao] create table with tags", log.String("tag", fmt.Sprintf("%+v", tag)))
	}

	if t.Values == nil {
		t.Values = make([]*Column, 0)
	}

	for _, value := range t.Values {
		t.ValueMap[value.Name] = value
		t.FactTable.ValueMap[value.Name] = value
	}

	if t.Aggregations == nil {
		// default
		t.Aggregations = make([]Aggregation, 0)
	}

	for _, aggregation := range t.Aggregations {
		t.AggregationMap[strconv.Itoa(aggregation.Granularity)] = aggregation
	}

	// default retention time
	if t.Retention == 0 {
		t.Retention = RETENTION_TIME
	}

}

func (ins *Instance) DeepCopy() *Instance {
	newins := &Instance{Name: "NewCopy", db: nil, hasinit: false}
	newins.TableMap = make(map[string]*TimeSeriesTable)

	for name, table := range ins.TableMap {
		newins.TableMap[name] = table.DeepCopy()
	}

	return newins
}

func (t *TimeSeriesTable) DeepCopy() *TimeSeriesTable {
	newt := *t
	newt.TagMap = make(map[string]*Column)
	newt.ValueMap = make(map[string]*Column)

	for name, column := range t.TagMap {
		newt.TagMap[name] = column.DeepCopy()
	}

	for name, column := range t.ValueMap {
		newt.ValueMap[name] = column.DeepCopy()
	}

	newt.FactTable = t.FactTable.DeepCopy()

	newt.DimensionTableMap = make(map[string]*DimensionTable)
	for name, dimtable := range t.DimensionTableMap {
		newt.DimensionTableMap[name] = dimtable.DeepCopy()
	}

	newt.AggregationMap = make(map[string]Aggregation)
	for agg, retention := range t.AggregationMap {
		newt.AggregationMap[agg] = retention
	}

	return &newt
}

func (c *Column) DeepCopy() *Column {
	newc := *c
	if c.DimTable != nil {
		newc.DimTable = c.DimTable.DeepCopy()
	}
	return &newc
}

func (dim *DimensionTable) DeepCopy() *DimensionTable {
	newdim := *dim
	newdim.DimValue = make([]string, len(dim.DimValue))
	copy(newdim.DimValue, dim.DimValue)

	return &newdim
}

func (fact *FactTable) DeepCopy() *FactTable {
	newfact := *fact
	newfact.TagMap = make(map[string]*Column)
	newfact.ValueMap = make(map[string]*Column)

	for name, column := range fact.TagMap {
		newfact.TagMap[name] = column.DeepCopy()
	}

	for name, column := range fact.ValueMap {
		newfact.ValueMap[name] = column.DeepCopy()
	}

	return &newfact
}

func (t *TimeSeriesTable) addDefaultColumn() {
	dimvalue := []string{"logical_ins_name", "physical_ins_name", "host", "port", "role", "version"}
	dim := &DimensionTable{
		Name:     "id",
		DimKey:   DimKey{Name: "id", Source: "autogen"},
		DimValue: dimvalue,
	}

	c := &Column{Name: "ins_info", Type: "INTEGER", DimTable: dim}

	t.Tags = append(t.Tags, c)
}

func (t *TimeSeriesTable) isDefaultDolumn(column string) bool {
	defaultColumnMap := map[string]string{
		"logical_ins_name":  "",
		"physical_ins_name": "",
		"host":              "",
		"port":              "",
		"role":              "",
		"version":           "",
	}

	if _, ok := defaultColumnMap[column]; ok {
		return true
	}

	return false
}

type TimeSeriesTable struct {
	Name   string    `json:"name"`
	Tags   []*Column `json:"tags"`
	Values []*Column `json:"values"`

	TagMap   map[string]*Column
	ValueMap map[string]*Column

	FactTable         *FactTable
	DimensionTableMap map[string]*DimensionTable

	Aggregations   []Aggregation `json:"aggregations"`
	AggregationMap map[string]Aggregation
	Retention      int `json:"retention"`
}

type Aggregation struct {
	Granularity int `json:"granularity"`
	Retention   int `json:"retention"`
}

type FactTable struct {
	TagMap   map[string]*Column
	ValueMap map[string]*Column
}

type Column struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	DimTable *DimensionTable `json:"dimtable"`
	AggType  string          `json:"aggtype"`
}

type DimensionTable struct {
	Name     string
	DimKey   DimKey   `json:"dimkey"`
	DimValue []string `json:"dimvalue"`
}

type DimKey struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}

func (ins *Instance) Init(name string,
	insinfo *InsInfo, dbinfo *DBInfo,
	initsqls []db_config.QueryCtx) error {
	var err error

	ins.mutex.Lock()
	defer ins.mutex.Unlock()

	if !ins.hasinit {
		ins.Name = name
		ins.InsInfo = insinfo
		ins.DBInfo = dbinfo
		ins.LowPriorityAsyncChannel = make(chan *AsyncInsertData, 20)
		ins.HighPriorityAsyncChannel = make(chan *AsyncInsertData, 40)
		ins.dimcache = make(map[string]int)

		ins.logger = logger.NewPluginLogger("dao",
			map[string]string{"ins": name, "schema": dbinfo.Schema})

		// 	we do not close it and this will leak,
		// 	but it doesn't matter because not too many instances will be created.
		if dbinfo.DBType == "SQLITE" {
			path := dbinfo.DBPath
			err = os.MkdirAll(path, 0775)
			if err != nil {
				ins.logger.Error("mkdir failed", err, log.String("path", path))
				return err
			}

			ins.db, err = sql.Open("sqlite3", path+"/"+dbinfo.Schema)
			if err != nil {
				ins.logger.Error("init db instance failed", err,
					log.String("path", path+"/"+dbinfo.Schema))
				return err
			}
		} else {
			if dbinfo.Host == "-" {
				ins.logger.Info("no need to init this instance now")
				return nil
			}

			dbUrl := ""
			if dbinfo.Password == "" {
				dbUrl = fmt.Sprintf("host=%s port=%d user=%s dbname=%s "+
					"fallback_application_name=ue_sqldao_%s sslmode=disable search_path=%s "+
					"connect_timeout=%d",
					dbinfo.Host, dbinfo.Port, dbinfo.UserName, dbinfo.DBName,
					name, dbinfo.Schema, DBConnTimeout)
			} else {
				dbUrl = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s "+
					"fallback_application_name=ue_sqldao_%s sslmode=disable search_path=%s "+
					"connect_timeout=%d",
					dbinfo.Host, dbinfo.Port, dbinfo.UserName, dbinfo.Password, dbinfo.DBName,
					name, dbinfo.Schema, DBConnTimeout)
			}

			ins.db, err = sql.Open("postgres", dbUrl)
			if err != nil {
				ins.logger.Error("init polardb-o connection failed",
					err, log.String("dburl", dbUrl))
				return err
			}

			ins.db.SetMaxIdleConns(1)
			ins.db.Exec("SET log_min_messages=FATAL")
		}

		if dbinfo.DBType != "SQLITE" {
			query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbinfo.Schema)
			_, err = ins.execDB(query)
			if err != nil {
				ins.logger.Error("execute create schema failed", err, log.String("query", query))
				ins.db.Close()
				return err
			}
		}

		err := ins.initDBMeta()
		if err != nil {
			ins.logger.Error("create meta table failed", err)
			ins.db.Close()
			return err
		}

		err = ins.initSQLs(initsqls)
		if err != nil {
			ins.logger.Warn("init sqls failed", err)
		}

		ins.routinemap = make(map[string]int)
		ins.aggStoppers = make(map[string]chan int)
		ins.retentionStopper = make(chan int)
		ins.AsyncInsertStopper = make(chan int)

		err = ins.initDB(true)
		if err != nil {
			ins.logger.Error("init db failed", err)
			ins.db.Close()
			return err
		}

		ins.hasinit = true

		ins.logger.Info("init dao success")
	}

	return nil
}

func (ins *Instance) Stop() error {
	ins.mutex.Lock()
	defer ins.mutex.Unlock()

	for key, stopper := range ins.aggStoppers {
		select {
		case stopper <- 1:
		default:
			ins.logger.Warn("stop failed", nil, log.String("key", key))
		}
	}

	select {
	case ins.retentionStopper <- 1:
	default:
		ins.logger.Warn("stop retention failed", nil)
	}

	select {
	case ins.AsyncInsertStopper <- 1:
	default:
		ins.logger.Warn("stop async insert failed", errors.New("stop async insert failed"))
	}

	ins.db.Close()
	ins.logger.Info("stop instance")

	return nil
}

func (ins *Instance) Insert(name string, schema []string, data [][]interface{}) error {
	defer ins.timeTrack("insert for "+name, time.Now())

	if len(data) == 0 {
		ins.logger.Debug("no data for inserting")
		return nil
	}

	// param sanity check
	if len(schema) != len(data[0]) {
		err := fmt.Errorf("schema length is not equal to data length"+
			"schema length[%d] data length[%d]", len(schema), len(data[0]))
		ins.logger.Error("schema length is not equal to data length", err,
			log.String("table", name), log.Int("schema length", len(schema)),
			log.Int("data length", len(data[0])))
		return err
	}

	ins.mutex.Lock()
	defer ins.mutex.Unlock()

REINIT:

	var err error

	// check datamodel version
	if err = ins.checkConfigVersionAndReinit(); err != nil {
		ins.logger.Error("check version for reinit failed", err)
		return err
	}

	// 1. create fact table if not exists
	table, ok := ins.TableMap[name]
	if !ok {
		table = &TimeSeriesTable{Name: name}
		initTable(table)
		ins.TableMap[name] = table
		err := ins.createTables(table)
		if err != nil {
			ins.logger.Error("create tables failed", err, log.String("table", name))
			return err
		}

		ins.logger.Info("create table", log.String("table", fmt.Sprintf("%+v", table)))
	}

	// 2. prepare insert schemas
	dimtablemap, schemanames, fullschemas, placement, err := ins.prepareInsert(table, schema)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			ins.logger.Info("prepare insert failed, we will reinit this db meta",
				log.String("table", name), log.String("err", err.Error()))

			if err := ins.initDB(false); err != nil {
				ins.logger.Error("reinit db failed", err)
				return err
			}

			ins.logger.Info("reinit success, we will reinsert the data", log.String("table", name))
			goto REINIT
		}

		ins.logger.Error("prepare insert failed", err, log.String("table", name))
		return err
	}

	defer ins.timeTrack("exec insert sql "+name, time.Now())

	// 3. insert
	valueholders := make([]string, 0, len(data))
	valueargs := make([]interface{}, 0, len(data)*len(schemanames))

	// insert data line by line
	lineno := 0
	for _, line := range data {
		holders := make([]string, len(schemanames))
		for index := 0; index < len(schemanames); index += 1 {
			holders[index] = fmt.Sprintf("$%d", lineno*len(schemanames)+index+1)
		}

		lineno += 1
		valueholders = append(valueholders, "("+strings.Join(holders, ", ")+")")

		for i, item := range fullschemas {
			if dim, ok := dimtablemap[item]; ok {
				id, err := ins.getDimensionValueID(table, item, dim, line)
				if err != nil {
					ins.logger.Error("get dim failed", err,
						log.String("table", table.Name), log.String("dim", item))
					return err
				}

				valueargs = append(valueargs, id)
			} else {
				valueargs = append(valueargs, line[placement[i]])
			}
		}
	}

	insertsql := fmt.Sprintf("INSERT INTO fact_%s (%s) VALUES %s",
		table.Name, strings.Join(schemanames, ", "), strings.Join(valueholders, ", "))

	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	_, err = ins.db.ExecContext(ctx, insertsql, valueargs...)
	if err != nil {
		ins.logger.Error("insert failed", err,
			log.String("table", table.Name), log.String("sql", insertsql))
		return err
	}

	return nil
}

func (ins *Instance) IsLowPriority(name string) bool {
	// TODO(wormhole.gl): just hard code now
	if name == "polar_aas_history" || name == "dbmetrics" {
		return true
	}

	return false
}

func (ins *Instance) AsyncInsert(name string, schema []string, data [][]interface{}) error {
	insertdata := &AsyncInsertData{
		name:   name,
		schema: schema,
		data:   data,
	}

	if ins.IsLowPriority(name) {
		select {
		case ins.LowPriorityAsyncChannel <- insertdata:
			ins.logger.Debug("low priority async channel",
				log.Int("length", len(ins.LowPriorityAsyncChannel)))
		default:
			err := fmt.Errorf("low priority async channel is full, we will drop for %s", name)
			ins.logger.Debug("low priority async channel is full, we will drop it.",
				log.String("error", err.Error()))
		}
	} else {
		select {
		case ins.HighPriorityAsyncChannel <- insertdata:
			ins.logger.Debug("high priority async channel",
				log.Int("length", len(ins.HighPriorityAsyncChannel)))
		default:
			err := fmt.Errorf("high priority async channel is full, we will drop for %s", name)
			ins.logger.Warn("high priority async channel is full, we will drop it", err)
		}
	}

	return nil
}

func (ins *Instance) asyncInsertRoutine() {
	for {
		select {
		case <-ins.AsyncInsertStopper:
			return
		case data := <-ins.HighPriorityAsyncChannel:
			err := ins.Insert(data.name, data.schema, data.data)
			if err != nil {
				ins.logger.Warn("high priority async insert failed", err)
			} else {
				ins.logger.Debug("after insert high priority, queue size",
					log.Int("high priority queue", len(ins.HighPriorityAsyncChannel)),
					log.Int("low priority queue", len(ins.LowPriorityAsyncChannel)))

			}
		case data := <-ins.LowPriorityAsyncChannel:
			err := ins.Insert(data.name, data.schema, data.data)
			if err != nil {
				ins.logger.Debug("low priority async insert failed",
					log.String("error", err.Error()))
			} else {
				ins.logger.Debug("after insert low priority, queue size",
					log.Int("high priority queue", len(ins.HighPriorityAsyncChannel)),
					log.Int("low priority queue", len(ins.LowPriorityAsyncChannel)))
			}
		}
	}
}

func (ins *Instance) initConfigVersion() error {
	query := fmt.Sprintf("SELECT version FROM %s LIMIT 1", TABLE_CONFIG_VERSION)
	versionrows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Error("query db version failed", err)
		return err
	}
	defer versionrows.Close()

	if !versionrows.Next() {
		query := fmt.Sprintf("INSERT INTO %s VALUES (1)", TABLE_CONFIG_VERSION)
		_, err := ins.execDB(query)
		if err != nil {
			ins.logger.Error("insert into config version failed", err)
			return err
		}
		ins.version = uint64(1)
	} else {
		var version uint64
		if err := versionrows.Scan(&version); err != nil {
			ins.logger.Error("scan db version failed", err)
			return err
		}
		ins.version = version
	}

	ins.logger.Info("ins data model version", log.Int64("version", int64(ins.version)))

	return nil
}

func (ins *Instance) initDBMeta() error {
	createSQL := []string{
		`
	CREATE TABLE IF NOT EXISTS meta_data_model_config (
		name TEXT PRIMARY KEY NOT NULL,
		retention INT,
		properties TEXT
	)
	`,
		`
	CREATE TABLE IF NOT EXISTS meta_data_model_version (
		version INT8 PRIMARY KEY
	)
	`,
	}

	for _, sql := range createSQL {
		ins.logger.Debug("create meta table", log.String("sql", sql))
		if _, err := ins.execDB(sql); err != nil {
			ins.logger.Error("create meta table failed", err, log.String("sql", sql))
			return err
		}
	}

	if err := ins.initConfigVersion(); err != nil {
		ins.logger.Error("into config version failed", err)
		return err
	}

	return nil
}

func (ins *Instance) initSQLs(initsqls []db_config.QueryCtx) error {

	for _, sqlctx := range initsqls {
		if _, err := ins.execDB(sqlctx.Query); err != nil {
			ins.logger.Warn("init sql failed", err, log.String("sql", sqlctx.Query))
		} else {
			ins.logger.Debug("init sql succeed", log.String("sql", sqlctx.Query))
		}
	}

	return nil
}

func (ins *Instance) initDB(isFirst bool) error {
	localdb, err := ins.initFromDB()
	if err != nil {
		ins.logger.Error("init from db failed", err)
		return err
	}

	err = ins.checkAndUpgradeDB(localdb, isFirst)
	if err != nil {
		ins.logger.Error("recheck and upgrade db failed", err)
		return err
	}

	err = ins.initHouseKeeperRoutines()
	if err != nil {
		ins.logger.Warn("start house keeper routine failed", err)
		return err
	}

	ins.logger.Info("init db success")

	return nil
}

func (ins *Instance) initFromDB() (*Instance, error) {
	var query string

	localins := &Instance{}
	localins.TableMap = make(map[string]*TimeSeriesTable)

	if ins.DBInfo.DBType == "SQLITE" {
		query = fmt.Sprintf("SELECT name FROM sqlite_master " +
			"WHERE type='table' AND name like 'fact_%%'")
	} else {
		query = fmt.Sprintf("SELECT table_name FROM INFORMATION_SCHEMA.TABLES "+
			"WHERE table_schema='%s' AND table_type='BASE TABLE' AND table_name like 'fact_%%'",
			ins.DBInfo.Schema)
	}

	// 1. get all fact tables
	rows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Error("show tables failed", err, log.String("query", query))
		return nil, err
	}
	defer rows.Close()

	var fulltablename string

	for rows.Next() {
		err := rows.Scan(&fulltablename)
		if err != nil {
			ins.logger.Error("scan for values failed", err, log.String("query", query))
			return nil, err
		}

		// a. desc table
		table, err := ins.initFromFactTable(fulltablename)
		if err != nil {
			ins.logger.Error("desc table failed", err,
				log.String("table", fulltablename), log.String("query", query))
			return nil, err
		}

		// b. list dim tables
		err = ins.initFromDimTable(table)
		if err != nil {
			ins.logger.Error("init from dim table failed", err,
				log.String("table", table.Name), log.String("query", query))
			return nil, err
		}

		// c. init from aggregation table
		err = ins.initFromAggTable(table)
		if err != nil {
			ins.logger.Error("init from agg table failed", err,
				log.String("table", table.Name), log.String("query", query))
		}

		localins.TableMap[table.Name] = table
		ins.logger.Debug("init from db",
			log.String("table name", table.Name),
			log.String("table", fmt.Sprintf("%+v", table)))

	}

	return localins, nil
}

func (ins *Instance) initFromFactTable(fulltablename string) (*TimeSeriesTable, error) {
	var query string

	if ins.DBInfo.DBType == "SQLITE" {
		query = fmt.Sprintf("PRAGMA table_info('%s')", fulltablename)
	} else {
		query = fmt.Sprintf("SELECT table_name, column_name, data_type, "+
			"column_default, is_nullable, interval_type FROM "+
			"INFORMATION_SCHEMA.COLUMNS WHERE table_schema='%s' AND table_name='%s'",
			ins.DBInfo.Schema, fulltablename)
	}

	rows, cancel1, err := ins.queryDB(query)
	defer cancel1()

	if err != nil {
		ins.logger.Error("describe table failed", err, log.String("table", fulltablename))
		return nil, err
	}
	defer rows.Close()

	var table TimeSeriesTable

	tablename := strings.TrimPrefix(fulltablename, "fact_")
	table.Name = tablename
	table.FactTable = &FactTable{TagMap: make(map[string]*Column), ValueMap: make(map[string]*Column)}
	table.DimensionTableMap = make(map[string]*DimensionTable)
	table.AggregationMap = make(map[string]Aggregation)

	table.TagMap = make(map[string]*Column)
	table.ValueMap = make(map[string]*Column)

	for rows.Next() {
		var name string
		var xtype string
		var x interface{}
		if err := rows.Scan(&x, &name, &xtype, &x, &x, &x); err != nil {
			ins.logger.Error("row scan failed in initFromFactTable", err,
				log.String("table", fulltablename))
			continue
		}

		var col Column

		if strings.HasPrefix(name, "tag_") {
			col.Name = strings.TrimPrefix(name, "tag_")
			col.Type = xtype
			table.FactTable.TagMap[col.Name] = &col
			table.TagMap[col.Name] = &col
		}

		if strings.HasPrefix(name, "value_") {
			col.Name = strings.TrimPrefix(name, "value_")
			col.Type = xtype
			col.AggType = "AVG"
			table.FactTable.ValueMap[col.Name] = &col
			table.ValueMap[col.Name] = &col
		}
	}

	// get table meta
	propertyQuery := fmt.Sprintf("SELECT retention FROM %s WHERE name='%s' LIMIT 1",
		DATAMODEL_CONFIG_TABLE, fulltablename)
	propertyRows, cancel2, err := ins.queryDB(propertyQuery)
	defer cancel2()

	if err != nil {
		ins.logger.Error("get data model info failed", err, log.String("table", fulltablename))
		return nil, err
	}
	defer propertyRows.Close()

	if propertyRows.Next() {
		var retention int

		if err := propertyRows.Scan(&retention); err != nil {
			ins.logger.Error("row scan failed for data model info", err,
				log.String("table", fulltablename))
			table.Retention = 0
		} else {
			ins.logger.Debug("get table retention time",
				log.String("table", fulltablename), log.Int("retention", retention))
			table.Retention = retention
		}
	} else {
		ins.logger.Debug("no retention time set in db", log.String("table", fulltablename))
		table.Retention = 0
	}

	ins.logger.Debug("init fact table",
		log.String("table", fmt.Sprintf("%+v", table)),
		log.String("fact table", fmt.Sprintf("%+v", table.FactTable)))

	return &table, nil
}

func (ins *Instance) initFromDimTable(table *TimeSeriesTable) error {
	var fulltablename string
	var query string

	if ins.DBInfo.DBType == "SQLITE" {
		query = fmt.Sprintf("SELECT name FROM sqlite_master "+
			"WHERE type='table' AND (name like 'dim_%s_%%' OR name = 'dim_ins_info')", table.Name)
	} else {
		query = fmt.Sprintf("SELECT table_name FROM INFORMATION_SCHEMA.TABLES "+
			"WHERE table_type='BASE TABLE' AND table_schema='%s' AND "+
			"(table_name like 'dim_%s_%%' OR table_name = 'dim_ins_info')",
			ins.DBInfo.Schema, table.Name)
	}

	rows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Error("show tables failed", err,
			log.String("table", table.Name), log.String("query", query))
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&fulltablename); err != nil {
			ins.logger.Error("row scan failed", err,
				log.String("table", table.Name), log.String("query", query))
			continue
		}

		var descquery string
		if ins.DBInfo.DBType == "SQLITE" {
			descquery = fmt.Sprintf("PRAGMA table_info('%s')", fulltablename)
		} else {
			descquery = fmt.Sprintf("SELECT c.table_name, c.column_name, c.data_type, "+
				" c.column_default, c.is_nullable, "+
				" CASE WHEN c.is_nullable='NO' THEN 1 ELSE 0 END AS PRIKEY "+
				" FROM information_schema.columns AS c "+
				" WHERE c.table_schema='%s' AND c.table_name = '%s'",
				ins.DBInfo.Schema, fulltablename)
		}

		subrows, cancel2, err := ins.queryDB(descquery)

		if err != nil {
			ins.logger.Error("describe table failed", err, log.String("table", fulltablename))
			cancel2()
			return err
		}

		var dimkey string
		dimvalues := make([]string, 0)

		for subrows.Next() {
			var x interface{}
			var name string
			var datatype string
			var primary int
			if err := subrows.Scan(&x, &name, &datatype, &x, &x, &primary); err != nil {
				ins.logger.Error("scan result failed", err, log.String("table", fulltablename))
				subrows.Close()
				cancel2()
				return err
			}

			if primary != 0 {
				dimkey = name
			} else {
				dimvalues = append(dimvalues, name)
			}
		}
		subrows.Close()
		cancel2()

		var t string

		if fulltablename == "dim_ins_info" {
			t = "ins_info"
		} else {
			t = strings.TrimPrefix(fulltablename, fmt.Sprintf("dim_%s_", table.Name))
		}

		table.DimensionTableMap[t] = &DimensionTable{
			Name:     t,
			DimKey:   DimKey{Name: dimkey, Source: "autogen"},
			DimValue: dimvalues,
		}

		for name, dimtable := range table.DimensionTableMap {
			delete(table.TagMap, name)
			if tag, ok := table.FactTable.TagMap[name]; !ok {
				ins.logger.Warn("fact table does not have such column", nil,
					log.String("table", table.Name), log.String("column", name))
			} else {
				tag.DimTable = dimtable
			}

			for _, dimname := range dimtable.DimValue {
				table.TagMap[dimname] = &Column{Name: dimname, Type: "TEXT", DimTable: dimtable}
			}
		}
	}

	return nil
}

func (ins *Instance) initFromAggTable(table *TimeSeriesTable) error {
	var fulltablename string
	var query string

	if ins.DBInfo.DBType == "SQLITE" {
		query = fmt.Sprintf("SELECT name FROM %s WHERE name like 'agg_%s_%%'",
			DATAMODEL_CONFIG_TABLE, table.Name)
	} else {
		query = fmt.Sprintf("SELECT name FROM %s WHERE name SIMILAR TO 'agg_%s_' || '[0-9]+'",
			DATAMODEL_CONFIG_TABLE, table.Name)
	}

	rows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Error("show tables failed", err,
			log.String("table", table.Name), log.String("query", query))
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&fulltablename); err != nil {
			ins.logger.Error("row scan failed", err,
				log.String("table", fulltablename), log.String("query", query))
			return err
		}

		aggregationstr := strings.TrimPrefix(fulltablename, fmt.Sprintf("agg_%s_", table.Name))
		agg, err := strconv.ParseInt(aggregationstr, 10, 64)
		if err != nil {
			ins.logger.Error("parse int from agg failed", err, log.String("name", table.Name))
			return err
		}

		propertyQuery := fmt.Sprintf("SELECT retention FROM %s WHERE name='%s' LIMIT 1",
			DATAMODEL_CONFIG_TABLE, fulltablename)
		propertyRows, cancel2, err := ins.queryDB(propertyQuery)
		if err != nil {
			ins.logger.Error("get data model info failed", err,
				log.String("table", fulltablename))
			cancel2()
			return err
		}

		retention := 0
		if propertyRows.Next() {
			if err := propertyRows.Scan(&retention); err != nil {
				ins.logger.Warn("row scan failed for data model info", err,
					log.String("table", fulltablename))
				continue
			}

			ins.logger.Debug("get table retention time",
				log.String("table", fulltablename), log.Int("retention", retention))
		} else {
			ins.logger.Debug("no retention time set in db",
				log.String("ins", ins.Name), log.String("table", fulltablename))
		}
		cancel2()
		propertyRows.Close()

		// retention is just a mock value, will be overwrite in merge meta
		table.AggregationMap[aggregationstr] = Aggregation{int(agg), retention}
	}

	return nil
}

func (ins *Instance) checkAndUpgradeDB(local *Instance, isFirst bool) error {
	for k, v := range ins.TableMap {
		if lt, ok := local.TableMap[k]; !ok {
			ins.logger.Info("need to create table", log.String("name", k))
			err := ins.createTables(v)
			if err != nil {
				ins.logger.Error("create tables failed", err,
					log.String("table", v.Name))
				return err
			}
		} else {
			// add dimension column
			for name, column := range v.FactTable.TagMap {
				if _, ok := lt.FactTable.TagMap[name]; !ok {
					if err := ins.addDimensionColumn(v, column, 0); err != nil {
						ins.logger.Error("add tag column failed", err,
							log.String("table", v.Name),
							log.String("column", name))
						return err
					}

					for aggkey := range lt.AggregationMap {
						agg, _ := strconv.Atoi(aggkey)
						if err := ins.addDimensionColumn(v, column, int(agg)); err != nil {
							ins.logger.Error("add tag column failed", err,
								log.String("table", v.Name),
								log.String("agg", aggkey))
							return err
						}
					}
				}
			}

			// add dimension table
			for _, dimtable := range v.DimensionTableMap {
				if err := ins.createDimensionTable(v, dimtable); err != nil {
					ins.logger.Error("create dimension table failed", err,
						log.String("table", v.Name),
						log.String("column", dimtable.Name))
					return err
				}
			}

			// add value column
			for name, column := range v.FactTable.ValueMap {
				if _, ok := lt.FactTable.ValueMap[name]; !ok {
					if err := ins.addValueColumn(v, column, 0); err != nil {
						ins.logger.Error("add value column failed", err,
							log.String("table", v.Name),
							log.String("column", name))
						return err
					}

					for aggkey := range lt.AggregationMap {
						agg, _ := strconv.ParseInt(aggkey, 10, 64)
						if err := ins.addValueColumn(v, column, int(agg)); err != nil {
							ins.logger.Error("add value column failed", err,
								log.String("table", v.Name),
								log.String("agg", aggkey))
							return err
						}
					}
				}
			}
		}
	}

	ins.mergeLocalMeta(local)

	for k, v := range ins.TableMap {
		if err := ins.recreateView(v, 0); err != nil {
			ins.logger.Warn("recreate fact table view failed", err,
				log.String("table", v.Name))
		}

		// add agg table
		for aggkey := range v.AggregationMap {
			agg, _ := strconv.ParseInt(aggkey, 10, 64)
			if lt, ok := local.TableMap[k]; ok {
				if _, ok := lt.AggregationMap[aggkey]; !ok {
					if err := ins.createFactTable(v, int(agg)); err != nil {
						ins.logger.Error("add agg table failed", err,
							log.String("table", v.Name),
							log.String("aggkey", aggkey))
						return err
					}
				} else {
					if isFirst {
						for _, column := range v.FactTable.TagMap {
							agg, _ := strconv.Atoi(aggkey)
							if err := ins.addDimensionColumn(v, column, int(agg)); err != nil {
								ins.logger.Debug("add tag column failed, but we will ignore it",
									log.String("error", err.Error()),
									log.String("table", v.Name),
									log.String("agg", aggkey))
							}
						}

						for _, column := range v.FactTable.ValueMap {
							agg, _ := strconv.Atoi(aggkey)
							if err := ins.addValueColumn(v, column, int(agg)); err != nil {
								ins.logger.Debug("try add value column failed, but we will ignore it",
									log.String("error", err.Error()),
									log.String("table", v.Name),
									log.String("agg", aggkey))
							}
						}
					}
				}
			}

			if err := ins.recreateView(v, int(agg)); err != nil {
				ins.logger.Warn("recreate agg table view failed", err,
					log.String("table", v.Name),
					log.String("agg", aggkey))
			}
		}

		if err := ins.updateTableMeta(v); err != nil {
			ins.logger.Error("update table meta failed", err, log.String("table", v.Name))
			return err
		}
	}

	return nil
}

func (ins *Instance) mergeLocalMeta(localdb *Instance) {
	for tname, dbtable := range localdb.TableMap {
		if instable, ok := ins.TableMap[tname]; !ok {
			ins.TableMap[tname] = dbtable
		} else {
			for name, column := range dbtable.TagMap {
				if _, ok := instable.TagMap[name]; !ok {
					instable.TagMap[name] = column
				}
			}

			for name, column := range dbtable.ValueMap {
				if _, ok := instable.ValueMap[name]; !ok {
					instable.ValueMap[name] = column
				}
			}

			for name, column := range dbtable.FactTable.TagMap {
				if _, ok := instable.FactTable.TagMap[name]; !ok {
					instable.FactTable.TagMap[name] = column
				}
			}

			for name, column := range dbtable.FactTable.ValueMap {
				if _, ok := instable.FactTable.ValueMap[name]; !ok {
					instable.FactTable.ValueMap[name] = column
				}
			}

			for name, column := range dbtable.DimensionTableMap {
				if _, ok := instable.DimensionTableMap[name]; !ok {
					instable.DimensionTableMap[name] = column
				}
			}

			for name, column := range dbtable.AggregationMap {
				if _, ok := instable.AggregationMap[name]; !ok {
					instable.AggregationMap[name] = column
				} else {
					if column.Retention != 0 {
						instable.AggregationMap[name] = column
					}
				}
			}

			if dbtable.Retention != 0 {
				instable.Retention = dbtable.Retention
			}
		}
	}
}

func (ins *Instance) initHouseKeeperRoutines() error {
	for key, stopper := range ins.aggStoppers {
		select {
		case stopper <- 1:
		default:
			ins.logger.Warn("stop failed", nil, log.String("key", key))
		}
	}
	ins.aggStoppers = make(map[string]chan int)

	for _, table := range ins.TableMap {
		for _, agg := range table.AggregationMap {
			if _, ok := ins.routinemap[table.Name+"_"+strconv.Itoa(agg.Granularity)]; ok {
				ins.logger.Info("aggregation routine already exist",
					log.String("table", table.Name),
					log.Int("granularity", agg.Granularity))
				continue
			}

			stop := make(chan int)
			go ins.aggregateRoutine(table, agg.Granularity, stop)
			ins.routinemap[table.Name+"_"+strconv.Itoa(agg.Granularity)] = 1
			ins.aggStoppers[table.Name+"_"+strconv.Itoa(agg.Granularity)] = stop
		}
	}

	if !ins.hasinit {
		stop := make(chan int)
		go ins.retainRoutine(stop)
		ins.retentionStopper = stop

		go ins.asyncInsertRoutine()
	}

	return nil
}

func (ins *Instance) aggregateRoutine(table *TimeSeriesTable, agg int, stop chan int) {
	var timer *time.Timer
	extraTime := 10 + rand.Int()%30
	timenow := time.Now()

	ins.logger.Info("start aggregation",
		log.String("table", table.Name), log.Int("granularity", agg))

	updater := func(agg int) time.Duration {
		ts := time.Now().Unix()
		nextts := int(ts) - int(ts)%agg + agg + extraTime // give it more seconds

		now := time.Now()
		nexttick := time.Unix(int64(nextts), 0)

		if nexttick.Before(now) {
			nexttick = nexttick.Add(time.Duration(agg))
		}

		return time.Until(nexttick)
	}

	timer = time.NewTimer(updater(agg))

	for {
		select {
		case ticknow := <-timer.C:
			timenow = time.Now()
			if err := ins.aggregateTable(table, int(ticknow.Unix())-extraTime, agg); err != nil {
				ins.logger.Error("aggregate failed.", err,
					log.String("table", table.Name),
					log.Int("aggregation", agg))
			}
		case <-stop:
			ins.logger.Info("stop the timer",
				log.String("table", table.Name),
				log.Int("aggregation", agg))
			return
		}

		nexttick := updater(agg)

		ins.logger.Info("aggregation",
			log.String("table", table.Name),
			log.String("base time",
				time.Now().Add(-time.Duration(extraTime)*time.Second).Format("2006-01-02 15:04:05")),
			log.Int("granularity", agg),
			log.String("duration", time.Now().Sub(timenow).String()),
			log.String("next time", time.Now().Add(nexttick).Format("2006-01-02 15:04:05")),
		)

		timer.Reset(nexttick)
	}
}

func (ins *Instance) retainRoutine(stop chan int) {
	var timer *time.Timer

	ins.logger.Info("start retention",
		log.String("ins", ins.Name),
		log.Int("default retention interval", RETENTION_INTERVAL),
		log.Int("default retention time", RETENTION_TIME))

	updater := func() time.Duration {
		var nexttick time.Time
		now := time.Now()

		if time.Duration(RETENTION_INTERVAL)*time.Second >= 12*time.Hour {
			nexttick = time.Date(now.Year(), now.Month(), now.Day(),
				RETENTION_HOUR, RETENTION_MIN, RETENTION_SECOND, 0, time.Local)
		} else {
			nexttick = now.Add(time.Duration(RETENTION_INTERVAL) * time.Second)
		}

		if nexttick.Before(now) {
			nexttick = nexttick.Add(time.Duration(RETENTION_INTERVAL) * time.Second)
		}

		ins.logger.Info("next retention time for instance",
			log.String("time", nexttick.String()),
			log.Int("default retention interval", RETENTION_INTERVAL),
			log.Int("default retention time", RETENTION_TIME))
		return time.Until(nexttick)
	}

	retention := func(timenow int) {
		for _, table := range ins.TableMap {
			agg := Aggregation{0, table.Retention}
			ins.logger.Info("retention fact table",
				log.String("table", table.Name),
				log.Int("aggregation", agg.Granularity),
				log.Int("retention", agg.Retention))
			if err := ins.retainTables(table, timenow, agg); err != nil {
				ins.logger.Warn("retention fact table failed", err,
					log.String("table", table.Name),
					log.Int("aggregation", agg.Granularity),
					log.Int("retention", agg.Retention))
			}

			for _, agg := range table.AggregationMap {
				ins.logger.Info("retention agg table",
					log.String("table", table.Name),
					log.Int("aggregation", agg.Granularity),
					log.Int("retention", agg.Retention))
				if err := ins.retainTables(table, timenow, agg); err != nil {
					ins.logger.Warn("retention agg table failed", err,
						log.String("table", table.Name),
						log.Int("aggregation", agg.Granularity),
						log.Int("retention", agg.Retention))
				}
			}
		}
	}

	retention(int(time.Now().Unix()))

	timer = time.NewTimer(updater())

	for {
		select {
		case ticknow := <-timer.C:
			timenow := int(ticknow.Unix())
			for _, table := range ins.TableMap {
				agg := Aggregation{0, table.Retention}

				ins.logger.Info("retain fact table",
					log.String("table", table.Name),
					log.Int("retention", agg.Retention))

				if err := ins.retainTables(table, timenow, agg); err != nil {
					ins.logger.Warn("retain table failed", err,
						log.String("table", table.Name),
						log.Int("aggregation", agg.Granularity),
						log.Int("retention", agg.Retention))
				}

				for _, agg := range table.AggregationMap {
					ins.logger.Info("retain agg table",
						log.String("table", table.Name),
						log.Int("retention", agg.Retention))

					if err := ins.retainTables(table, timenow, agg); err != nil {
						ins.logger.Warn("retain agg failed", err,
							log.String("table", table.Name),
							log.Int("aggregation", agg.Granularity),
							log.Int("retention", agg.Retention))
					}
				}
			}

		case <-stop:
			ins.logger.Info("stop the timer for retention")
			return
		}

		timer.Reset(updater())
	}
}

func (ins *Instance) aggregateTable(table *TimeSeriesTable, now int, agg int) error {
	var err error
	var query string

	ins.mutex.Lock()
	defer ins.mutex.Unlock()

	name := table.getTableName(agg)
	org := table.getTableName(0)

	schemalist := make([]string, 0)
	groupbylist := make([]string, 0)
	selectlist := make([]string, 0)

	for _, tag := range table.FactTable.TagMap {
		name := "tag_" + tag.Name
		groupbylist = append(groupbylist, name)
		selectlist = append(selectlist, name)
		schemalist = append(schemalist, name)
	}

	for _, value := range table.FactTable.ValueMap {
		name := "value_" + value.Name

		selectstr := ""

		if ins.DBInfo.DBType == "SQLITE" {
			if value.AggType == "AVG" {
				selectstr = "ROUND(AVG(" + name + "), 2)"
			} else {
				selectstr = fmt.Sprintf("ROUND(CAST(SUM("+name+") AS REAL) / %d, 2)", agg)
			}
		} else {
			if value.AggType == "AVG" {
				selectstr = "ROUND(CAST(AVG(" + name + ") AS NUMERIC), 2)::FLOAT"
			} else {
				selectstr = fmt.Sprintf("ROUND(CAST(SUM("+name+") AS NUMERIC) / %d, 2)::FLOAT", agg)
			}
		}

		selectlist = append(selectlist, selectstr)
		schemalist = append(schemalist, name)
	}

	selectlist = append(selectlist, fmt.Sprintf("time - time %% %d AS times", agg))
	schemalist = append(schemalist, "time")

	selectstr := strings.Join(selectlist, ", ")
	groupby := strings.Join(groupbylist, ", ")
	schemastr := strings.Join(schemalist, ", ")
	if len(groupbylist) == 0 {
		query = fmt.Sprintf("INSERT INTO %s (%s) "+
			"SELECT %s FROM %s, dim_ins_info WHERE time >= %d AND time < %d AND "+
			" %s.tag_ins_info=dim_ins_info.id AND physical_ins_name='%s' "+
			"GROUP BY times ORDER BY times ASC",
			name, schemastr, selectstr, org, now-agg, now, org, ins.InsInfo.PhysicalInsName)
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) "+
			"SELECT %s FROM %s, dim_ins_info WHERE time >= %d AND time < %d AND "+
			" %s.tag_ins_info=dim_ins_info.id AND physical_ins_name='%s' "+
			"GROUP BY %s, times ORDER BY times ASC",
			name, schemastr, selectstr, org, now-agg, now, org, ins.InsInfo.PhysicalInsName, groupby)
	}

	_, err = ins.db.Exec(query)
	if err != nil {
		ins.logger.Error("execute aggregate query failed", err,
			log.String("table", table.Name),
			log.Int("agg", agg),
			log.String("query", query))
		return err
	}

	return nil
}

func (ins *Instance) retainTables(table *TimeSeriesTable, now int, aggregation Aggregation) error {
	ins.mutex.Lock()
	defer ins.mutex.Unlock()

	name := table.getTableName(aggregation.Granularity)
	query := fmt.Sprintf("DELETE FROM %s WHERE time < %d", name, now-aggregation.Retention)
	_, err := ins.db.Exec(query)
	if err != nil {
		ins.logger.Error("execute retention query failed", err,
			log.String("table", table.Name),
			log.Int("agg", aggregation.Granularity),
			log.Int("retention", aggregation.Retention),
			log.String("query", query))
		return err
	}

	return nil
}

func (ins *Instance) prepareInsert(table *TimeSeriesTable,
	schema []string) (map[string]map[int]string, []string, []string, []int, error) {
	defer ins.timeTrack("prepare insert for "+table.Name, time.Now())

	dimtablemap := make(map[string]map[int]string)
	schemanames := make([]string, 0)
	fullschemas := make([]string, 0)
	placement := make([]int, 0)

    needRecreateView := false
	hastime := false
	for i, column := range schema {
		column = strings.ToLower(column)

		if column == "time" {
			schemanames = append(schemanames, "time")
			fullschemas = append(fullschemas, "time")
			placement = append(placement, i)
			hastime = true
			continue
		}

		if strings.HasPrefix(column, "dim_") {
			// prepare for dimension column
			column = strings.TrimPrefix(column, "dim_")
			if tag, ok := table.TagMap[column]; ok {
				if tag.DimTable != nil {
					if x, xok := dimtablemap[tag.DimTable.Name]; xok {
						x[i] = column
					} else {
						x := make(map[int]string)
						x[i] = column
						dimtablemap[tag.DimTable.Name] = x
						schemanames = append(schemanames, "tag_"+tag.DimTable.Name)
						fullschemas = append(fullschemas, tag.DimTable.Name)
						placement = append(placement, i)
					}
				} else {
					schemanames = append(schemanames, "tag_"+tag.Name)
					fullschemas = append(fullschemas, tag.Name)
					placement = append(placement, i)
				}
			} else {
				if isdefault := table.isDefaultDolumn(column); isdefault {
					err := fmt.Errorf("column '%s' is default column", column)
					ins.logger.Error("column is default column", err,
						log.String("column", column),
						log.String("schema", fmt.Sprintf("%+v", schema)))
					return nil, nil, nil, nil, err
				}

				c := &Column{Name: column, Type: "TEXT"}
				err := ins.addDimensionColumn(table, c, 0)
				if err != nil {
					ins.logger.Error("add dimension column failed", err,
						log.String("table", table.Name), log.String("column", column))
					return nil, nil, nil, nil, err
				}

				table.TagMap[column] = c
				table.FactTable.TagMap[column] = c

                needRecreateView = true

				ins.logger.Info("add column success for",
					log.String("table", table.Name), log.String("column", column))

				for aggkey := range table.AggregationMap {
					agg, _ := strconv.ParseInt(aggkey, 10, 64)
					if err := ins.addDimensionColumn(table, c, int(agg)); err != nil {
						ins.logger.Error("add column failed for agg table", err,
							log.String("table", table.Name), log.Int("agg", int(agg)))
						return nil, nil, nil, nil, err
					}

				//	err = ins.recreateView(table, int(agg))
				//	if err != nil {
				//		ins.logger.Warn("recreate view failed", err,
				//			log.String("table", table.Name))
				//	}
				}

				schemanames = append(schemanames, "tag_"+column)
				fullschemas = append(fullschemas, column)
				placement = append(placement, i)
			}
		} else if _, ok := table.ValueMap[column]; !ok {
			// add non exist value column automaticly
			c := &Column{Name: column, Type: "REAL", AggType: "AVG"}
			err := ins.addValueColumn(table, c, 0)
			if err != nil {
				ins.logger.Error("add value column failed", err,
					log.String("table", table.Name), log.String("column", column))
				return nil, nil, nil, nil, err
			}

			// add to meta
			table.ValueMap[column] = c
			table.FactTable.ValueMap[column] = c

            needRecreateView = true
			// err = ins.recreateView(table, 0)
			// if err != nil {
			// 	ins.logger.Warn("recreate view failed", err,
			// 		log.String("table", table.Name), log.String("column", column))
			// }

			for aggkey := range table.AggregationMap {
				agg, _ := strconv.ParseInt(aggkey, 10, 64)
				if err := ins.addValueColumn(table, c, int(agg)); err != nil {
					ins.logger.Error("add column failed for agg table", err,
						log.String("table", table.Name), log.Int("agg", int(agg)))
					return nil, nil, nil, nil, err
				}

				// err = ins.recreateView(table, int(agg))
				// if err != nil {
				// 	ins.logger.Warn("recreate view failed", err, log.String("table", table.Name))
				// }
			}

			schemanames = append(schemanames, "value_"+column)
			fullschemas = append(fullschemas, column)
			placement = append(placement, i)
		} else {
			schemanames = append(schemanames, "value_"+column)
			fullschemas = append(fullschemas, column)
			placement = append(placement, i)
		}
	}

	if !hastime {
		err := fmt.Errorf("insert column does not have time")
		ins.logger.Error("insert column does not have time", err, log.String("table", table.Name))
		return nil, nil, nil, nil, err
	}

    if needRecreateView {
        err := ins.recreateView(table, 0)
        if err != nil {
            ins.logger.Warn("recreate view failed", err, log.String("table", table.Name))
        }

        for aggkey := range table.AggregationMap {
            agg, _ := strconv.ParseInt(aggkey, 10, 64)
            err = ins.recreateView(table, int(agg))
            if err != nil {
                ins.logger.Warn("recreate agg view failed", err, log.String("table", table.Name))
            }
        }
    }

	return dimtablemap, schemanames, fullschemas, placement, nil
}

func (ins *Instance) getDimensionValueID(
	table *TimeSeriesTable,
	dimname string, dim map[int]string, line []interface{}) (int, error) {

	var dimid int
	name := table.Name
	condition := make([]string, 0)
	insertschema := make([]string, 0)
	dimvalue := make([]string, 0)
	dimkeys := make([]int, len(dim))

	i := 0
	for k := range dim {
		dimkeys[i] = k
		i += 1
	}
	sort.Ints(dimkeys)

	// 1. build dimension value SELECT
	for _, i := range dimkeys {
		c := dim[i]
		if _, ok := line[i].(int); ok {
			dimid = line[i].(int)
			dimvalue = append(dimvalue, "'"+strconv.FormatInt(int64(line[i].(int)), 10)+"'")
		} else {
			condition = append(condition, fmt.Sprintf(" %s = '%s' ", c, line[i].(string)))
			dimvalue = append(dimvalue, "'"+line[i].(string)+"'")
		}
		insertschema = append(insertschema, c)
	}
	conditionstr := strings.Join(condition, " AND ")

	if ins.cacheDimName(table.getDimTableName(dimname)) {
		if res, ok := ins.dimcache[table.getDimTableName(dimname)+"_"+conditionstr]; ok {
			ins.logger.Debug("get dim cache",
				log.Int("id", res),
				log.String("conditionstr", conditionstr))
			return res, nil
		}
	}

	dimtable, ok := table.DimensionTableMap[dimname]
	if !ok {
		err := fmt.Errorf("cannot find dimension table: tablename[%s], dimtable[%s]", name, dimname)
		ins.logger.Error("cannot find dimension table", err,
			log.String("table", name), log.String("dimtable", dimname))
		return 0, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		dimtable.DimKey.Name, table.getDimTableName(dimname), conditionstr)
	rows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Error("get id from dimention table failed", err,
			log.String("table", name), log.String("query", query))
		return 0, err
	}
	defer rows.Close()

	var id int

	// 2. get dimension value id
	if rows.Next() {
		// dimension value already exist, get and return its id
		err := rows.Scan(&id)
		if err != nil {
			ins.logger.Error("scan for values failed", err, log.String("table", name))
			return 0, err
		}
	} else {
		// new dimension value, insert and get last id
		// a. insert new dimension value
		insertschemastr := strings.Join(insertschema, ", ")
		valuestr := strings.Join(dimvalue, ", ")
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table.getDimTableName(dimname), insertschemastr, valuestr)
		ins.logger.Debug("insert dim", log.String("table", name), log.String("query", query))
		ctx2, cancel2 := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
		res, err := ins.db.ExecContext(ctx2, query)
		defer cancel2()

		if err != nil {
			ins.logger.Error("insert dimension failed", err,
				log.String("table", name), log.String("query", query))
			return 0, err
		}

		// b. get auto increment id
		if dimtable.DimKey.Source == "autogen" {
			if ins.DBInfo.DBType == "SQLITE" {
				idint64, err := res.LastInsertId()
				if err != nil {
					ins.logger.Error("get last insert id failed", err, log.String("table", name))
					return 0, err
				}
				id = int(idint64)
			} else {
				serialquery := fmt.Sprintf("SELECT currval(pg_get_serial_sequence('%s', '%s'))",
					table.getDimTableName(dimname), dimtable.DimKey.Name)
				serialrows, cancel3, err := ins.queryDB(serialquery)
				defer cancel3()

				if err != nil {
					ins.logger.Error("get last insert id failed", err,
						log.String("table", name), log.String("query", serialquery))
					return 0, err
				}

				if serialrows.Next() {
					err := serialrows.Scan(&id)
					if err != nil {
						ins.logger.Error("scan serial id failed", err, log.String("table", name))
						serialrows.Close()
						return 0, err
					}
				} else {
					id = 0
				}

				serialrows.Close()
			}
		} else {
			id = dimid
		}

		ins.logger.Info("add dim value success",
			log.String("table", name),
			log.String("dim", dimname),
			log.String("value", conditionstr))
	}

	if ins.cacheDimName(table.getDimTableName(dimname)) {
		ins.dimcache[table.getDimTableName(dimname)+"_"+conditionstr] = id
	}

	return id, nil
}

func (ins *Instance) checkConfigVersionAndReinit() error {
	if rand.Int()%10 != 0 {
		return nil
	}

	if version, err := ins.getConfigVersion(); err != nil {
		ins.logger.Warn("check version failed", err)
	} else {
		if version != ins.version {
			ins.logger.Info("db version update",
				log.String("ins", ins.Name),
				log.Uint64("old version", ins.version),
				log.Uint64("new version", version))

			if err := ins.initDB(false); err != nil {
				ins.logger.Error("reinit failed", err)
				return err
			}

			ins.version = version
		}
	}

	return nil
}

func (ins *Instance) getConfigVersion() (uint64, error) {
	query := "SELECT version FROM meta_data_model_version LIMIT 1"
	rows, cancel, err := ins.queryDB(query)
	defer cancel()

	if err != nil {
		ins.logger.Warn("get db config version failed", err)
		return uint64(0), nil
	}
	defer rows.Close()

	if rows.Next() {
		var version uint64
		if err := rows.Scan(&version); err != nil {
			ins.logger.Warn("scan db version failed", err)
			return uint64(0), nil
		}

		return version, nil
	}

	return uint64(0), errors.New("cannot get version")
}

func (ins *Instance) createTables(v *TimeSeriesTable) error {
	if err := ins.createFactTable(v, 0); err != nil {
		ins.logger.Error("create table failed", err, log.String("table", v.Name))
		return err
	}

	for _, dimtable := range v.DimensionTableMap {
		if err := ins.createDimensionTable(v, dimtable); err != nil {
			ins.logger.Error("create dimension table failed", err,
				log.String("ins", ins.Name),
				log.String("table", v.Name),
				log.String("column", dimtable.Name))
			return err
		}
	}

	for aggkey := range v.AggregationMap {
		agg, _ := strconv.ParseInt(aggkey, 10, 64)
		if err := ins.createFactTable(v, int(agg)); err != nil {
			ins.logger.Error("add agg table failed", err,
				log.String("ins", ins.Name),
				log.String("table", v.Name),
				log.String("aggkey", aggkey))
			return err
		}
	}

	if err := ins.updateTableMeta(v); err != nil {
		ins.logger.Error("update table meta failed", err, log.String("table", v.Name))
		return err
	}

	return nil
}

func (ins *Instance) updateTableMeta(t *TimeSeriesTable) error {
	sql := fmt.Sprintf("INSERT INTO %s(name, retention) "+
		" VALUES ($1, $2) ON CONFLICT(name) "+
		" DO UPDATE SET retention=$3", DATAMODEL_CONFIG_TABLE)
	name := t.getTableName(0)
	if _, err := ins.execDB(sql, name, t.Retention, t.Retention); err != nil {
		ins.logger.Error("insert into meta_data_model_info failed", err,
			log.String("sql", sql), log.String("table", t.Name))
		return err
	}

	for aggkey, aggmap := range t.AggregationMap {
		aggkeyint, _ := strconv.Atoi(aggkey)
		name = t.getTableName(aggkeyint)
		if _, err := ins.execDB(sql, name, aggmap.Retention, aggmap.Retention); err != nil {
			ins.logger.Error("insert into meta_data_model_info failed", err,
				log.String("sql", sql), log.String("table", name))
			return err
		}

		ins.logger.Debug("update meta table",
			log.String("table", name),
			log.Int("retention", aggmap.Retention))
	}

	return nil
}

func (ins *Instance) createFactTable(t *TimeSeriesTable, agg int) error {
	sql := t.genCreateFactTableSql(agg, ins.DBInfo.DBType)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute create table failed", err, log.String("sql", sql))
		return err
	}

	sqls := t.genCreateTimeIndexSql(agg)
	for _, sql = range sqls {
		if _, err := ins.execDB(sql); err != nil {
			ins.logger.Error("execute create index failed", err, log.String("sql", sql))
			return err
		}
	}

	ins.logger.Info("create fact table", log.String("table", t.Name), log.Int("agg", agg))

	return nil
}

func (ins *Instance) checkDimensionTableExist(
	t *TimeSeriesTable, dim *DimensionTable) (bool, error) {

	sql := t.genCheckDimTableExistSql(dim, ins.DBInfo.DBType, ins.DBInfo.Schema)

	rows, cancel, err := ins.queryDB(sql)
	defer cancel()

	if err != nil {
		ins.logger.Error("query db failed", err,
			log.String("tablename", t.Name),
			log.String("sql", sql))
		return false, err
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			ins.logger.Error("scan result failed", err,
				log.String("tablename", t.Name),
				log.String("sql", sql))
			return false, err
		}
	}

	return count > 0, nil
}

func (ins *Instance) createDimensionTable(t *TimeSeriesTable, dim *DimensionTable) error {
	var exist bool
	var err error

	if exist, err = ins.checkDimensionTableExist(t, dim); err != nil {
		ins.logger.Error("check dimension table exist failed", err)
		return err
	}

	if exist {
		ins.logger.Debug("table already exist",
			log.String("table", t.Name),
			log.String("dim", dim.Name))
		return nil
	}

	sql := t.genCreateDimTableSql(dim, ins.DBInfo.DBType)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute create dim table failed", err, log.String("sql", sql))
		return err
	}

	sql = t.genCreateDimTableIndexSql(dim)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Warn("execute create dim table index failed", err, log.String("sql", sql))
	}

	sql = t.genInsertDimTableSql(dim)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute insert dim table failed, but we ignore it", err,
			log.String("sql", sql))
	}

	ins.logger.Info("create dimension table",
		log.String("table", t.Name), log.String("dim", dim.Name))

	return nil
}

func (ins *Instance) recreateView(t *TimeSeriesTable, agg int) error {
	sql := t.genDropViewSql(agg)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute drop view failed", err, log.String("sql", sql))
		return err
	}

	sql = t.genCreateViewSql(agg, ins.DBInfo.DBType)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute create or replace view failed", err, log.String("sql", sql))
		return err
	}

	ins.logger.Debug("create or replace view",
		log.String("table", t.Name),
		log.Int("agg", agg),
		log.String("sql", sql))

	return nil
}

func (ins *Instance) addValueColumn(t *TimeSeriesTable, c *Column, agg int) error {
	sql := t.genAddColumnSql(ins.DBInfo.DBType, "value_", c, agg)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute add value column failed", err, log.String("sql", sql))
		return err
	}

	if agg == 0 {
		ins.logger.Info("add value column",
			log.String("table", t.Name),
			log.Int("agg", agg),
			log.String("column", c.Name))
	} else {
		ins.logger.Debug("try add value column",
			log.String("table", t.Name),
			log.Int("agg", agg),
			log.String("column", c.Name))
	}

	return nil
}

func (ins *Instance) addDimensionColumn(t *TimeSeriesTable, c *Column, agg int) error {
	sql := t.genAddColumnSql(ins.DBInfo.DBType, "tag_", c, agg)
	if _, err := ins.execDB(sql); err != nil {
		ins.logger.Error("execute add dimension column failed", err, log.String("sql", sql))
		return err
	}

	if agg == 0 {
		ins.logger.Info("add tag column",
			log.String("table", t.Name),
			log.Int("agg", agg),
			log.String("column", c.Name))
	} else {
		ins.logger.Debug("try add tag column",
			log.String("table", t.Name),
			log.Int("agg", agg),
			log.String("column", c.Name))
	}

	return nil
}

func (ins *Instance) timeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > MIN_TRACKLOG_TIME {
		ins.logger.Info("db operation in ms",
			log.String("function", key), log.Int64("elapsed", elapsed.Milliseconds()))
	}
}

func (ins *Instance) queryDB(query string, args ...interface{}) (*sql.Rows, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)

	rows, err := ins.db.QueryContext(ctx, query, args...)
	if err != nil {
		ins.logger.Error("exec query failed", err, log.String("query", query))
		return nil, cancel, err
	}

	return rows, cancel, nil
}

func (ins *Instance) execDB(query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	res, err := ins.db.ExecContext(ctx, query, args...)
	if err != nil {
		ins.logger.Error("exec sql failed", err, log.String("query", query))
		return nil, err
	}

	return res, nil
}

func (t *TimeSeriesTable) getAggKey(granularity int) int {
	aggkeylist := make([]int, 0)
	if granularity == 0 {
		return 0
	}

	for k := range t.AggregationMap {
		agg, _ := strconv.ParseInt(k, 10, 64)
		aggkeylist = append(aggkeylist, int(agg))
	}

	sort.Sort(sort.Reverse(sort.IntSlice(aggkeylist)))

	for _, agg := range aggkeylist {
		if granularity%agg == 0 {
			return agg
		}
	}

	return 0
}

func (ins *Instance) cacheDimName(dimname string) bool {
	if dimname == "dim_ins_info" {
		return true
	}

	if strings.HasPrefix(dimname, "dim_polar_stat_io") {
		return true
	}

	if strings.HasPrefix(dimname, "dim_polar_stat_process") {
		return true
	}

	return false
}

func (t *TimeSeriesTable) getDimTableName(dimname string) string {
	// NOTE: special ins_info logic
	if dimname == "ins_info" {
		return fmt.Sprintf("dim_%s", dimname)
	}

	return fmt.Sprintf("dim_%s_%s", t.Name, dimname)
}

func (t *TimeSeriesTable) getViewName(aggkey int) string {
	var name string
	agg := t.getAggKey(aggkey)
	if agg != 0 {
		name = fmt.Sprintf("view_agg_%s_%d", t.Name, agg)
	} else {
		name = fmt.Sprintf("view_fact_%s", t.Name)
	}

	return name
}

func (t *TimeSeriesTable) getTableName(aggkey int) string {
	var name string
	agg := t.getAggKey(aggkey)
	if agg != 0 {
		name = fmt.Sprintf("agg_%s_%d", t.Name, agg)
	} else {
		name = fmt.Sprintf("fact_%s", t.Name)
	}

	return name
}

func (t *TimeSeriesTable) genCreateFactTableSql(agg int, dbtype string) string {
	var columns string
	for k := range t.FactTable.TagMap {
		columns += fmt.Sprintf("tag_%s INTEGER DEFAULT NULL, ", k)
	}

	for k := range t.FactTable.ValueMap {
		columns += fmt.Sprintf("value_%s REAL DEFAULT NULL, ", k)
	}

	var name string
	if agg > 0 {
		name = fmt.Sprintf("agg_%s_%d", t.Name, agg)
	} else {
		name = fmt.Sprintf("fact_%s", t.Name)
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
						(%s time INTEGER DEFAULT NULL);`, name, columns)
}

func (t *TimeSeriesTable) genCreateTimeIndexSql(agg int) []string {
	sqls := make([]string, 0)

	var name string
	if agg > 0 {
		name = fmt.Sprintf("agg_%s_%d", t.Name, agg)
	} else {
		name = fmt.Sprintf("fact_%s", t.Name)
	}

	timeindexsql := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS index_%s_time ON %s (time)`,
		name, name)
	sqls = append(sqls, timeindexsql)

	return sqls
}

func (t *TimeSeriesTable) genCreateViewSql(agg int, dbtype string) string {
	var query string
	dimtables := make(map[string]*DimensionTable)
	groupby := make([]string, 0)
	values := make([]string, 0)
	joincond := make([]string, 0)

	for _, tag := range t.FactTable.TagMap {
		if tag.DimTable == nil || tag.DimTable.Name != "ins_info" {
			continue
		}

		dimtables[tag.DimTable.Name] = tag.DimTable
		for _, dimname := range tag.DimTable.DimValue {
			groupby = append(groupby,
				fmt.Sprintf("%s.%s", t.getDimTableName(tag.DimTable.Name), dimname))
		}
	}

	for k, tag := range t.FactTable.TagMap {
		if tag.DimTable != nil && tag.DimTable.Name == "ins_info" {
			continue
		}

		if tag.DimTable != nil {
			dimtables[tag.DimTable.Name] = tag.DimTable
			for _, dimname := range tag.DimTable.DimValue {
				groupby = append(groupby,
					fmt.Sprintf("%s.%s", t.getDimTableName(tag.DimTable.Name), dimname))
			}
		} else {
			groupby = append(groupby, fmt.Sprintf("%s AS %s", "tag_"+k, k))
		}
	}

	for k := range t.FactTable.ValueMap {
		values = append(values, fmt.Sprintf("%s AS %s", "value_"+k, k))
	}

	tablename := t.getTableName(agg)

	for dimtablename, dim := range dimtables {
		joincond = append(joincond,
			fmt.Sprintf(" tag_%s = %s.%s ",
				dimtablename,
				t.getDimTableName(dimtablename),
				dim.DimKey.Name))
	}

	groupbystr := strings.Join(groupby, ", ")
	var valuestr string
	if len(values) == 0 {
		valuestr = ""
	} else {
		valuestr = ", " + strings.Join(values, ", ")
	}
	timestr := "time"

	if len(dimtables) > 0 {
		i := 0
		dimtablestr := ""
		for _, dim := range dimtables {
			if i != 0 {
				dimtablestr += ", " + t.getDimTableName(dim.Name)
			} else {
				dimtablestr += t.getDimTableName(dim.Name)
			}
			i += 1
		}

		joincondstr := strings.Join(joincond, " AND ")
		query = fmt.Sprintf("SELECT %s, %s %s FROM %s, %s WHERE %s",
			groupbystr, timestr, valuestr, tablename, dimtablestr, joincondstr)
	} else {
		if len(groupby) == 0 {
			query = fmt.Sprintf("SELECT %s %s FROM %s",
				timestr, valuestr, tablename)
		} else {
			query = fmt.Sprintf("SELECT %s %s, %s FROM %s",
				groupbystr, valuestr, timestr, tablename)
		}
	}

	if dbtype == "SQLITE" {
		query = fmt.Sprintf("CREATE VIEW IF NOT EXISTS view_%s AS %s", tablename, query)
	} else {
		query = fmt.Sprintf("CREATE OR REPLACE VIEW view_%s AS %s", tablename, query)
	}

	return query
}

func (t *TimeSeriesTable) genDropViewSql(agg int) string {
	tablename := t.getTableName(agg)
	viewname := fmt.Sprintf("view_%s", tablename)

	return fmt.Sprintf("DROP VIEW IF EXISTS %s", viewname)
}

func (t *TimeSeriesTable) genAddColumnSql(dbType string, prefix string, c *Column, agg int) string {
	var name string
	if agg > 0 {
		name = fmt.Sprintf("agg_%s_%d", t.Name, agg)
	} else {
		name = fmt.Sprintf("fact_%s", t.Name)
	}

	if dbType == "SQLITE" {
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s%s %s DEFAULT NULL",
			name, prefix, c.Name, c.Type)
	} else {
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s%s %s DEFAULT NULL",
			name, prefix, c.Name, c.Type)
	}
}

func (t *TimeSeriesTable) genCheckDimTableExistSql(
	dim *DimensionTable,
	dbtype string,
	schema string) string {

	var query string

	tablename := t.getDimTableName(dim.Name)
	if dbtype == "SQLITE" {
		query = fmt.Sprintf("SELECT COUNT(1) "+
			" FROM sqlite_master "+
			" WHERE type='table' AND name = '%s'", tablename)
	} else {
		query = fmt.Sprintf("SELECT COUNT(1) "+
			" FROM INFORMATION_SCHEMA.TABLES "+
			" WHERE table_schema='%s' AND "+
			" table_type='BASE TABLE' AND "+
			" table_name='%s'", schema, tablename)
	}

	return query
}

func (t *TimeSeriesTable) genCreateDimTableSql(dim *DimensionTable, dbtype string) string {
	var primarykey string

	if dbtype == "SQLITE" {
		primarykey = fmt.Sprintf("%s INTEGER PRIMARY KEY ", dim.DimKey.Name)
		if dim.DimKey.Source == "autogen" {
			primarykey += "AUTOINCREMENT"
		}
	} else {
		if dim.DimKey.Source == "autogen" {
			primarykey = fmt.Sprintf("%s SERIAL PRIMARY KEY ", dim.DimKey.Name)
		} else {
			primarykey = fmt.Sprintf("%s PRIMARY KEY ", dim.DimKey.Name)
		}
	}

	values := make([]string, 0)
	for _, v := range dim.DimValue {
		values = append(values, fmt.Sprintf("%s TEXT", v))
	}
	valuestr := strings.Join(values, ", ")

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, %s)",
		t.getDimTableName(dim.Name), primarykey, valuestr)

	return sql
}

func (t *TimeSeriesTable) genCreateDimTableIndexSql(dim *DimensionTable) string {
	return fmt.Sprintf("CREATE UNIQUE INDEX %s_index ON %s (%s)",
		t.getDimTableName(dim.Name),
		t.getDimTableName(dim.Name),
		strings.Join(dim.DimValue, ", "))
}

func (t *TimeSeriesTable) genInsertDimTableSql(dim *DimensionTable) string {
	values := make([]string, 0)
	i := 0
	for i < len(dim.DimValue) {
		values = append(values, "'others'")
		i++
	}
	valuestr := strings.Join(values, ", ")

	return fmt.Sprintf("INSERT INTO %s VALUES (0, %s)", t.getDimTableName(dim.Name), valuestr)
}
