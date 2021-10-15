/*-------------------------------------------------------------------------
 *
 * db_config.go
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
 *           common/polardb_pg/db_config/db_config.go
 *-------------------------------------------------------------------------
 */
package db_config

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/logger"
)

const DATAMODEL = "dbmetrics"
const SQL_MARK = "/* rds internal mark */ "

const TABLE_BASE_CONFIG = "meta_base_config"
const TABLE_COLLECT_SQL_CONFIG = "meta_collect_sql_config"
const TABLE_CONFIG_VERSION = "meta_config_version"
const TABLE_SNAPSHOT_NO = "meta_snapshots"
const TABLE_DATA_STORE_CONFIG = "meta_data_store_config"

const (
	PolarMinReleaseDate = int64(20190101)
	MinDBVersion        = -1 // means no limit
	MaxDBVersion        = -1 // means no limit
	CollectMinDBVersion = 90200
	CollectMaxDBVersion = MaxDBVersion
)

const DBQueryTimeout = 60

type DBConfig struct {
	insname                  string
	instype                  string
	schema                   string
	db                       *sql.DB
	configcenterdb           *sql.DB
	collectConfigInitContext []interface{}
	logger                   *logger.PluginLogger
}

type QueryCtx struct {
	Name               string `json:"name"`
	Query              string `json:"query"`
	DataModel          string `json:"datamodel"`
	MinVersion         int64  `json:"min_version"`
	MaxVersion         int64  `json:"max_version"`
	PolarReleaseDate   int64  `json:"polar_release_date"`
	DBRole             int    `json:"role"`
	Offset             int64  `json:"offset"`
	Cycle              int64  `json:"cycle"`
	Enable             int    `json:"enable"`
	UseSnapshot        int    `json:"use_snapshot"`
	QueryAllDB         bool   `json:"queryalldb"`
	SendToMultiBackend int    `json:"send_to_multibackend"`
	Comment            string `json:"comment"`
	UseTmpTable        bool   `json:"usetmptable"`
	WriteIntoLog       int    `json:"write_into_log"`
	OriginQuery        string
}

func NewDBConfig() *DBConfig {
	return &DBConfig{}
}

func (d *DBConfig) Init(
	insname string,
	instype string,
	schema string,
	db *sql.DB,
	configcenterdb *sql.DB,
	collectConfigInitContext []interface{},
	logger *logger.PluginLogger) error {

	d.insname = insname
	d.schema = schema
	d.db = db
	d.configcenterdb = configcenterdb
	d.collectConfigInitContext = collectConfigInitContext
	d.logger = logger
	d.instype = instype

	return nil
}

func (d *DBConfig) InitFromDBConfig(
	configmap map[string]interface{},
	querymap map[string]QueryCtx, rw bool) error {

	d.logger.Debug("init from db config",
		log.String("configmap", fmt.Sprintf("%+v", configmap)),
		log.String("querymap", fmt.Sprintf("%+v", querymap)))

	// create schema if not exists
	if rw {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", d.schema)
		err := d.execSQL(d.db, SQL_MARK+query)
		if err != nil {
			d.logger.Warn("create schema failed", err, log.String("schema", d.schema))
		}
	}

	if d.configcenterdb != nil {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", d.schema)
		err := d.execSQL(d.configcenterdb, SQL_MARK+query)
		if err != nil {
			d.logger.Warn("create schema failed", err, log.String("schema", d.schema))
		}
	}

	err := d.initDB(rw)
	if err != nil {
		d.logger.Error("init db failed", err)
		return err
	}

	if d.configcenterdb != nil {
		err = d.updateLocalConfig(d.configcenterdb, configmap, querymap, "config center")
		if err != nil {
			d.logger.Warn("update local config failed", err, log.String("db", "config center"))
		} else {
			d.logger.Info("update config center db config")
			err = d.updateDBConfig(d.configcenterdb, configmap, querymap, "config center")
			if err != nil {
				d.logger.Warn("update db config failed", err, log.String("db", "config center"))
			}
		}
	}

	err = d.updateLocalConfig(d.db, configmap, querymap, "local db")
	if err != nil {
		d.logger.Warn("update local config failed", err, log.String("db", "local db"))
		return err
	}

	if rw {
		err = d.updateDBConfig(d.db, configmap, querymap, "local db")
		if err != nil {
			d.logger.Warn("update db config failed", err, log.String("db", "local db"))
			return err
		}
	}

	d.logger.Info("init from db config success")

	return nil
}

func (d *DBConfig) GetConfigCenterDBConfigVersion() (int64, error) {
	query := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", d.schema, TABLE_CONFIG_VERSION)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := d.configcenterdb.QueryContext(ctx, SQL_MARK+query)
	if err != nil {
		d.logger.Warn("get db config version failed", err)
		return int64(0), nil
	}
	defer rows.Close()

	if rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			d.logger.Warn("scan db version failed", err)
			return int64(0), nil
		}

		return version, nil
	}

	return int64(0), errors.New("cannot get version")

}

func (d *DBConfig) GetDBConfigVersion() (int64, error) {
	query := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", d.schema, TABLE_CONFIG_VERSION)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := d.db.QueryContext(ctx, SQL_MARK+query)
	if err != nil {
		d.logger.Warn("get db config version failed", err)
		return int64(0), nil
	}
	defer rows.Close()

	if rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			d.logger.Warn("scan db version failed", err)
			return int64(0), nil
		}

		return version, nil
	}

	return int64(0), errors.New("cannot get version")
}

func (d *DBConfig) GetDBSnapshotNo() (int64, error) {
	query := fmt.Sprintf("SELECT MAX(snapshotno) FROM %s.%s LIMIT 1", d.schema, TABLE_SNAPSHOT_NO)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := d.db.QueryContext(ctx, SQL_MARK+query)
	if err != nil {
		d.logger.Warn("get db snapshot no failed", err)
		return int64(0), nil
	}
	defer rows.Close()

	if rows.Next() {
		var snapshotno int64
		if err := rows.Scan(&snapshotno); err != nil {
			d.logger.Warn("scan snapshot no failed", err)
			return int64(0), nil
		}

		return snapshotno, nil
	}

	return int64(0), nil
}

func (d *DBConfig) initDB(rw bool) error {
	var err error

	if err = d.initTables(rw); err != nil {
		d.logger.Error("init tables failed", err)
		return err
	}

	if err = d.initRecords(rw); err != nil {
		d.logger.Error("init records failed", err)
		return err
	}

	if err = d.initObjects(rw); err != nil {
		d.logger.Warn("init objects faield", err)
	}

	return nil
}

func (d *DBConfig) initRecords(rw bool) error {
	var err error

	if rw {
		if err = d.initConfigVersion(d.db); err != nil {
			d.logger.Error("init config version failed", err)
			return err
		}
	}

	if d.configcenterdb != nil {
		if err = d.initConfigVersion(d.configcenterdb); err != nil {
			d.logger.Warn("init config version in config center failed", err)
		}
	}

	if rw {
		if err = d.initSnapshotNo(); err != nil {
			d.logger.Error("init snapshot no failed", err)
			return err
		}
	}

	return nil
}

func (d *DBConfig) initConfigVersion(db *sql.DB) error {
	query := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", d.schema, TABLE_CONFIG_VERSION)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	versionrows, err := db.QueryContext(ctx, SQL_MARK+query)
	if err != nil {
		d.logger.Error("query db version failed", err)
		return err
	}
	defer versionrows.Close()

	if !versionrows.Next() {
		query := fmt.Sprintf("INSERT INTO %s.%s VALUES (1)", d.schema, TABLE_CONFIG_VERSION)
		err := d.execSQL(db, SQL_MARK+query)
		if err != nil {
			d.logger.Error("insert into config version failed", err)
			return err
		}
	}

	return nil
}

func (d *DBConfig) initTables(rw bool) error {
	// 1. check table exists
	createConfigTableSQL := []string{
		`
	    CREATE TABLE IF NOT EXISTS %s.meta_base_config (
			name TEXT PRIMARY KEY,
			value TEXT,
			modifiable_byconf BOOL DEFAULT true,
			comment TEXT DEFAULT '{}'
		)
	`,
		`
	    CREATE TABLE IF NOT EXISTS %s.meta_collect_sql_config (
			name TEXT PRIMARY KEY,
			query TEXT,
			datamodel TEXT,
			cycle INT8,
			enable INT,
			modifiable_byconf BOOL DEFAULT true,
			comment TEXT,
			properties TEXT DEFAULT '{}'
		)
	`,
		`
		CREATE TABLE IF NOT EXISTS %s.meta_config_version (
			version INT8 PRIMARY KEY
		)
	`,
		`
		CREATE TABLE IF NOT EXISTS %s.meta_snapshots (
			snapshotno SERIAL PRIMARY KEY,
			time TIMESTAMP DEFAULT NOW()
		)
	`,
	}

	for _, query := range createConfigTableSQL {
		if rw {
			err := d.execSQL(d.db, SQL_MARK+fmt.Sprintf(query, d.schema))
			if err != nil {
				d.logger.Error("create config table failed", err, log.String("sql", query))
				return err
			}
		}

		if d.configcenterdb != nil {
			err := d.execSQL(d.configcenterdb, SQL_MARK+fmt.Sprintf(query, d.schema))
			if err != nil {
				d.logger.Warn("create config table in config center failed", err,
					log.String("sql", query))
			}
		}
	}

	return nil
}

func (d *DBConfig) initObjects(rw bool) error {
	defaultQueryCtx := QueryCtx{
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

	d.logger.Debug("config db init sqls", log.Bool("is rw", rw), log.String("sql list", fmt.Sprintf("%+v", d.collectConfigInitContext)))

	for _, query := range d.collectConfigInitContext {
		queryCtx := defaultQueryCtx
		queryMap := query.(map[string]interface{})
		confStr, err := json.Marshal(queryMap)
		if err != nil {
			d.logger.Warn("marshal query conf to string failed", err,
				log.String("query", fmt.Sprintf("%+v", queryMap)))
			continue
		}

		err = json.Unmarshal(confStr, &queryCtx)
		if err != nil {
			d.logger.Warn("unmarshal conf string failed", err,
				log.String("query", fmt.Sprintf("%+v", queryMap)))
			continue
		}

		if queryCtx.Enable == 0 {
			d.logger.Debug("sql is disabled", log.String("query", queryCtx.Query))
			continue
		}

		if rw {
			err := d.execSQL(d.db, SQL_MARK+queryCtx.Query)
			if err != nil {
				d.logger.Warn("exec config db init sql failed", err,
					log.String("sql", queryCtx.Query))
			} else {
				d.logger.Debug("exec config db init sql succeed",
					log.String("sql", queryCtx.Query))
			}
		}

		// if d.configcenterdb != nil {
		// 	err := d.execSQL(d.configcenterdb, SQL_MARK+queryCtx.Query)
		// 	if err != nil {
		// 		d.logger.Warn("exec config center db init sql failed", err,
		// 			log.String("sql", queryCtx.Query))
		// 	} else {
		// 		d.logger.Info("exec config center db init sql succeed",
		// 			log.String("sql", queryCtx.Query))
		// 	}
		// }
	}

	return nil
}

func (d *DBConfig) initSnapshotNo() error {
	query := fmt.Sprintf("SELECT snapshotno FROM %s.%s LIMIT 1", d.schema, TABLE_SNAPSHOT_NO)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	snapshotrows, err := d.db.QueryContext(ctx, SQL_MARK+query)
	if err != nil {
		d.logger.Error("query snapshot no failed", err)
		return err
	}
	defer snapshotrows.Close()

	if !snapshotrows.Next() {
		query := fmt.Sprintf("INSERT INTO %s.%s (time) VALUES (NOW())", d.schema, TABLE_SNAPSHOT_NO)
		err := d.execSQL(d.db, SQL_MARK+query)
		if err != nil {
			d.logger.Error("insert into snapshot no failed", err)
			return err
		}
	}

	return nil
}

func (d *DBConfig) updateLocalConfig(
	db *sql.DB,
	configmap map[string]interface{},
	querymap map[string]QueryCtx, info string) error {

	var err error

	if err = d.updateLocalBaseConfig(db, configmap, info); err != nil {
		d.logger.Error("update local base config failed", err, log.String("db", info))
		return err
	}

	if err = d.updateLocalQueryConfig(db, querymap, info); err != nil {
		d.logger.Error("update local query config failed", err, log.String("db", info))
		return err
	}

	return nil
}

func (d *DBConfig) updateLocalBaseConfig(
	db *sql.DB,
	configmap map[string]interface{},
	info string) error {

	query := fmt.Sprintf("SELECT name, value, modifiable_byconf FROM %s.%s",
		d.schema, TABLE_BASE_CONFIG)
	sql := fmt.Sprintf(SQL_MARK + query)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		d.logger.Error("query db config table failed", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name, value string
		var modifiable bool
		if err := rows.Scan(&name, &value, &modifiable); err != nil {
			d.logger.Error("row scan failed for base config table", err)
			continue
		}

		if _, ok := configmap[name]; !ok {
			d.logger.Debug("use database config because there is no local config",
				log.String("name", name), log.String("db", info))
			configmap[name] = value
		} else if !modifiable {
			d.logger.Debug("use database config", log.String("name", name), log.String("db", info))
			configmap[name] = value
		} else {
			d.logger.Debug("use local config", log.String("name", name), log.String("db", info))
		}
	}

	return nil
}

func (d *DBConfig) updateLocalQueryConfig(db *sql.DB,
	querymap map[string]QueryCtx, info string) error {

	// NOTE(wormhole.gl): nearly the only diff with the db_config.go in polardb_pg_multidimension plugin
	var query string

	if d.instype == "polardb_pg" {
		query = fmt.Sprintf("SELECT name, query, datamodel, "+
			"cycle, enable, modifiable_byconf, properties FROM %s.%s WHERE datamodel='%s'",
			d.schema, TABLE_COLLECT_SQL_CONFIG, DATAMODEL)
	} else {
		query = fmt.Sprintf("SELECT name, query, datamodel, "+
			"cycle, enable, modifiable_byconf, properties FROM %s.%s WHERE datamodel!='%s'",
			d.schema, TABLE_COLLECT_SQL_CONFIG, DATAMODEL)
	}

	sql := fmt.Sprintf(SQL_MARK + query)
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		d.logger.Error("query db config table failed", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		qctx := QueryCtx{
			Name:               "Default",
			Query:              "",
			MinVersion:         CollectMinDBVersion,
			MaxVersion:         CollectMaxDBVersion,
			PolarReleaseDate:   PolarMinReleaseDate,
			Cycle:              1,
			Offset:             0,
			Enable:             1,
			SendToMultiBackend: 0,
			QueryAllDB:         false,
			DataModel:          DATAMODEL,
		}

		var properties string
		var modifiable bool
		if err := rows.Scan(&qctx.Name, &qctx.Query, &qctx.DataModel,
			&qctx.Cycle, &qctx.Enable, &modifiable, &properties); err != nil {
			d.logger.Error("row scan failed for query config table", err)
			continue
		}

		if err := json.Unmarshal([]byte(properties), &qctx); err != nil {
			d.logger.Error("properties cannot parse", err)
			continue
		}

		if _, ok := querymap[qctx.Name]; !ok {
			if modifiable {
				qctx.Enable = 0
				d.logger.Debug("there is no local query context at all, "+
					"and query context in database is modifiable, disable it",
					log.String("qctx", fmt.Sprintf("%+v", qctx)), log.String("info", info))
			} else {
				d.logger.Debug("use database query context "+
					"because there is no local query context at all",
					log.String("qctx", fmt.Sprintf("%+v", qctx)), log.String("info", info))
			}
			querymap[qctx.Name] = qctx
		} else if !modifiable {
			d.logger.Debug("use database query context",
				log.String("qctx", fmt.Sprintf("%+v", qctx)), log.String("info", info))
			querymap[qctx.Name] = qctx
		} else {
			d.logger.Debug("use local query context",
				log.String("qctx", fmt.Sprintf("%+v", qctx)), log.String("info", info))
		}
	}

	return nil
}

func (d *DBConfig) updateDBConfig(db *sql.DB,
	configmap map[string]interface{}, querymap map[string]QueryCtx, info string) error {

	var err error

	if err = d.updateDBBaseConfig(db, configmap, info); err != nil {
		d.logger.Error("update db base config failed", err, log.String("info", info))
		return err
	}

	if err = d.updateDBQueryConfig(db, querymap, info); err != nil {
		d.logger.Error("update db query config failed", err, log.String("db", info))
		return err
	}

	return nil
}

func (d *DBConfig) updateDBBaseConfig(db *sql.DB,
	configmap map[string]interface{}, info string) error {

	for name, ivalue := range configmap {
		var value string

		switch ivalue.(type) {
		case string:
			value = ivalue.(string)
		case int:
			value = strconv.Itoa(ivalue.(int))
		case int64:
			value = strconv.Itoa(int(ivalue.(int64)))
		case float64:
			value = strconv.Itoa(int(ivalue.(float64)))
		default:
			err := errors.New(fmt.Sprintf("cannot recognize ivalue type: %s",
				reflect.TypeOf(ivalue).String()))
			return err
		}

		sql := fmt.Sprintf("INSERT INTO %s.%s(name, value) VALUES ($1, $2) ON CONFLICT(name)"+
			" DO UPDATE SET value=$3 WHERE %s.%s.modifiable_byconf",
			d.schema, TABLE_BASE_CONFIG, d.schema, TABLE_BASE_CONFIG)
		err := d.execSQL(db, SQL_MARK+sql, name, value, value)
		if err != nil {
			d.logger.Error("upsert config failed", err,
				log.String("name", name), log.String("value", value))
			return err
		}

		sql = fmt.Sprintf("SELECT value FROM %s.%s WHERE name=$1", d.schema, TABLE_BASE_CONFIG)
		ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
		rows, err := db.QueryContext(ctx, SQL_MARK+sql, name)
		if err != nil {
			d.logger.Error("get config from db failed", err, log.String("name", name))
			cancel()
			return err
		}

		if rows.Next() {
			var newvalue string
			err = rows.Scan(&newvalue)
			if err != nil {
				d.logger.Error("scan config failed", err, log.String("name", name))
				rows.Close()
				cancel()
				return err
			}

			configmap[name] = newvalue
			d.logger.Debug("get config from db success",
				log.String("name", name), log.String("value", newvalue), log.String("db", info))
		} else {
			d.logger.Debug("still use original config",
				log.String("name", name), log.String("value", value), log.String("db", info))
		}
		rows.Close()
		cancel()
	}

	return nil
}

func (d *DBConfig) updateDBQueryConfig(db *sql.DB,
	querymap map[string]QueryCtx, info string) error {

	for _, query := range querymap {
		propertymap := make(map[string]interface{})
		propertymap["min_version"] = query.MinVersion
		propertymap["max_version"] = query.MaxVersion
		propertymap["polar_release_date"] = query.PolarReleaseDate
		propertymap["role"] = query.DBRole
		propertymap["use_snapshot"] = query.UseSnapshot
		propertymap["comment"] = query.Comment
		propertymap["send_to_multibackend"] = query.SendToMultiBackend
		propertymap["offset"] = query.Offset
		propertymap["use_tmptable"] = query.UseTmpTable
		propertymap["queryalldb"] = query.QueryAllDB
		properties, err := json.Marshal(propertymap)
		if err != nil {
			d.logger.Error("property map json marshal failed", err)
			return err
		}

		sql := fmt.Sprintf("INSERT INTO %s.%s(name, query, datamodel, cycle, enable, properties) "+
			"VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT(name)"+
			" DO UPDATE SET query=$7, datamodel=$8, cycle=$9, enable=$10, properties=$11 "+
			"WHERE %s.%s.modifiable_byconf",
			d.schema, TABLE_COLLECT_SQL_CONFIG, d.schema, TABLE_COLLECT_SQL_CONFIG)
		err = d.execSQL(db, SQL_MARK+sql,
			query.Name, query.Query, query.DataModel, query.Cycle, query.Enable, properties,
			query.Query, query.DataModel, query.Cycle, query.Enable, properties)
		if err != nil {
			d.logger.Error("upsert query ctx failed", err, log.String("query", query.Query))
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
		sql = fmt.Sprintf("SELECT query, datamodel, cycle, enable FROM %s.%s WHERE name=$1",
			d.schema, TABLE_COLLECT_SQL_CONFIG)
		rows, err := db.QueryContext(ctx, SQL_MARK+sql, query.Name)
		if err != nil {
			d.logger.Error("get properties failed", err, log.String("query", query.Query))
			cancel()
			return err
		}

		if rows.Next() {
			err = rows.Scan(&query.Query, &query.DataModel, &query.Cycle, &query.Enable)
			if err != nil {
				d.logger.Error("scan properties failed", err, log.String("query", query.Query))
				rows.Close()
				cancel()
				return err
			}

			querymap[query.Name] = query
		}
		rows.Close()
		cancel()

		d.logger.Debug("get query info success",
			log.String("query", fmt.Sprintf("%+v", query)),
			log.String("db", info))
	}

	return nil
}

func (d *DBConfig) execSQL(db *sql.DB, sql string, params ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "/* rds internal mark */ "+sql, params...)
	if err != nil {
		d.logger.Error("query db failed", err)
		return err
	}

	return nil
}
