/*
 * Copyright (c) 2021. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/db_backend/dao"
)

const SlowRequestTrackInMS = 500

type InfluxDBCtx struct {
	Endpoint          string `json:"endpoint"`
	Timeout           int64  `json:"timeout_ms"`
	LogOutDict        bool   `json:"log_outdict"`
	PreserveDimPrefix bool   `json:"preserve_dim_prefix"`
	MetricPrefix      string `json:"metric_prefix"`
	Database          string `json:"database"`
	Username          string `json:"username"`
	Password          string `json:"password"`
	PushChannelLength int    `json:"push_channel_length"`
}

type InfluxDBPushChannelData struct {
	conn *client.Client
	data *client.BatchPoints
}

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("[influxdb_backend] invalid ctx")
	}
	extern := m[consts.PluginExternKey].(string)

	log.Info("[influxdb_backend] plugin init", log.String("m", fmt.Sprintf("%+v", m)))
	content, err := ioutil.ReadFile(extern)
	if err != nil {
		log.Error("[influxdb_backend] read influxdb conf fail", log.String("err", err.Error()))
		return nil, err
	}

	influxdbCtx := &InfluxDBCtx{
		Endpoint:          "localhost:8086",
		Timeout:           5000,
		LogOutDict:        false,
		PreserveDimPrefix: false,
		MetricPrefix:      "",
		Database:          "polardb_o",
		Username:          "",
		Password:          "",
		PushChannelLength: 20,
	}

	err = json.Unmarshal(content, influxdbCtx)
	if err != nil {
		log.Warn("[influxdb_backend] unmarshal content failed", log.String("err", err.Error()))
		return nil, err
	}

	m["endpoint"] = influxdbCtx.Endpoint
	m["timeout"] = influxdbCtx.Timeout
	m["log_outdict"] = influxdbCtx.LogOutDict
	m["preserve_dim_prefix"] = influxdbCtx.PreserveDimPrefix
	m["metric_prefix"] = influxdbCtx.MetricPrefix
	m["database"] = influxdbCtx.Database

	host, err := url.Parse(fmt.Sprintf("http://%s", influxdbCtx.Endpoint))
	if err != nil {
		log.Error("[influxdb_backend] parse url failed", log.String("error", err.Error()))
		return nil, err
	}

	con, err := client.NewClient(client.Config{URL: *host,
		Timeout:  time.Duration(influxdbCtx.Timeout) * time.Millisecond,
		Username: influxdbCtx.Username,
		Password: influxdbCtx.Password})
	if err != nil {
		log.Error("[influxdb_backend] create client failed", log.String("error", err.Error()))
		return nil, err
	}

	q := client.Query{
		Command:  fmt.Sprintf("create database %s", influxdbCtx.Database),
		Database: influxdbCtx.Database,
	}

	_, err = con.Query(q)
	if err != nil {
		log.Info("[influxdb_backend] create database failed, but we will ignore it",
			log.String("error", err.Error()))
	} else {
		log.Info("[influxdb_backend] create database success",
			log.String("database", influxdbCtx.Database))
	}

	m["influxdb_conn"] = con

	m["push_channel"] = make(chan InfluxDBPushChannelData, influxdbCtx.PushChannelLength)
    m["push_channel_length"] = influxdbCtx.PushChannelLength
	m["stop_channel"] = make(chan int)

	go runPushChannel(
		m["push_channel"].(chan InfluxDBPushChannelData),
		m["stop_channel"].(chan int),
	)

	log.Info("[influxdb_backend] plugin init done",
		log.String("content", string(content)),
		log.String("m", fmt.Sprintf("%+v", m)))

	return ctx, nil
}

func writeInfluxDB(data InfluxDBPushChannelData) {
	defer TimeTrack("InfluxDB", time.Now())

	_, err := data.conn.Write(*data.data)
	if err != nil {
		log.Warn("[influxdb_backend] write to influxdb failed",
			log.String("error", err.Error()))
	}
}

func runPushChannel(channel chan InfluxDBPushChannelData, stop chan int) {
	for {
		select {
		case data := <-channel:
			writeInfluxDB(data)
		case <-stop:
			return
		}
	}
}

func buildLableMap(m map[string]interface{}) map[string]string {
	var envs map[string]string

	envs, _ = meta.GetMetaService().GetStringMap("envs", m["backend_port"].(string))

	// build labels
	labelmap := map[string]string{
		"job":              "polardb_o",
		"plugin":           m["plugin"].(string),
		"datatype":         m["datatype"].(string),
		"instance":         m["logical_ins_name"].(string),
		"logical_insname":  m["logical_ins_name"].(string),
		"physical_insname": m["physical_ins_name"].(string),
		"kind":             "polardb-o",
		"container_name":   "universe",
		"node_name":        m["backend_hostname"].(string),
		"node_ip":          m["backend_host"].(string),
	}

	if content, ok := envs["io.kubernetes.pod.namespace"]; ok {
		labelmap["namespace"] = content
	}

	if content, ok := envs["io.kubernetes.pod.name"]; ok {
		labelmap["pod_name"] = content
		labelmap["pod_ip"] = m["backend_host"].(string)
		labelmap["service_name"] = fmt.Sprintf("%s-%s-noclusterip",
			m["logical_ins_name"].(string), m["physical_ins_name"].(string))
	}

	if content, ok := envs["io.kubernetes.pod.uid"]; ok {
		labelmap["pod_uid"] = content
	}

	return labelmap
}

func PluginRun(ctx interface{}, param interface{}) error {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return errors.New("[influxdb_backend] invalid ctx")
	}

	backendCtx, ok := m["backend"].(map[string]interface{})
	if !ok {
		return errors.New("[influxdb_backend] no backends ctx")
	}

	preserveDim := backendCtx["preserve_dim_prefix"].(bool)
	logOutDict := backendCtx["log_outdict"].(bool)
	paramap := param.(map[string]interface{})
	datatype := m["datatype"].(string)

	if datatype == "maxscale" || datatype == "cluster_manager" {
		if x, ok := meta.GetMetaService().GetInterface("topology", m["backend_port"].(string)); ok {
			dbinfo := x.(*dao.DBInfo)
			if dbinfo.InsName != "" {
				log.Debug("[influxdb_backend] use meta dbinfo",
					log.String("ins", dbinfo.InsName))
				m["logical_ins_name"] = dbinfo.InsName
			} else {
				insname, err := GetInsNameFromDBInfo(dbinfo)
				if err != nil {
					log.Warn("[influxdb_backend] get logical ins name failed",
						log.String("err", err.Error()))
				} else {
					log.Debug("[influxdb_backend] get logical ins name",
						log.String("ins", insname))
					dbinfo.InsName = insname
					m["logical_ins_name"] = insname
				}
			}
		} else {
			log.Debug("[influxdb_backend] cannot get port",
				log.String("port", m["backend_port"].(string)))
			return nil
		}
	}

	pts := make([]client.Point, 0)

	for model, valuelist := range paramap {
		labelmap := buildLableMap(m)
		labelmap["datamodel"] = model

		for _, valuemap := range valuelist.([]map[string]interface{}) {
			tags := make(map[string]string)
			values := make(map[string]interface{})

			// tags
			for k, v := range valuemap {
				if strings.HasPrefix(k, "dim_") {
					if k == "dim_uuid" {
						continue
					}

					if preserveDim {
						tags[k] = strconv.Quote(v.(string))
					} else {
						tags[strings.TrimPrefix(k, "dim_")] = strconv.Quote(v.(string))
					}
				} else if x, ok := v.(string); ok {
					tags[k] = strconv.Quote(x)
				}
			}

			for k, v := range labelmap {
				tags[k] = strconv.Quote(v)
			}

			// fields
			for k, v := range valuemap {
				if k == "time" {
					continue
				}

				if _, ok := v.(string); ok {
					continue
				}

                if _, ok := v.(int64); ok {
				    values[k] = float64(v.(int64))
                    continue
                }

                if _, ok := v.(uint64); ok {
				    values[k] = float64(v.(uint64))
                    continue
                }

                values[k] = v
			}

			// time
			t := time.Now()
			if t, ok := valuemap["time"]; ok {
				t = time.Unix(t.(int64), 0)
			}

			point := client.Point{
				Measurement: model,
				Tags:        tags,
				Fields:      values,
				Time:        t,
				Precision:   "ns",
			}

			pts = append(pts, point)
		}

		if logOutDict {
			log.Info("[influxdb_backend] influxdb out dict",
				log.String("out dict", fmt.Sprintf("%+v", pts)))
		}
	}

	bps := client.BatchPoints{
		Points:   pts,
		Database: backendCtx["database"].(string),
	}

	data := InfluxDBPushChannelData{
		conn: backendCtx["influxdb_conn"].(*client.Client),
		data: &bps,
	}

	select {
	case backendCtx["push_channel"].(chan InfluxDBPushChannelData) <- data:
	default:
		log.Warn("[influxdb_backend] push channel is full",
			log.Int("length", backendCtx["push_channel_length"].(int)))
	}

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[influxdb_backend] PluginExit")

	m, ok := ctx.(map[string]interface{})
	if !ok {
		return errors.New("[influxdb_backend] invalid ctx")
	}

	backendCtx, ok := m["backend"].(map[string]interface{})
	if !ok {
		return errors.New("[influxdb_backend] no backends ctx")
	}

	backendCtx["stop_channel"].(chan int) <- 1

	log.Info("[influxdb_backend] exit successfully")

	return nil
}

func GetInsNameFromDBInfo(dbinfo *dao.DBInfo) (string, error) {
	dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d password=%s "+
		"fallback_application_name=%s sslmode=disable connect_timeout=%d",
		dbinfo.Host, dbinfo.UserName, dbinfo.DBName, dbinfo.Port, dbinfo.Password,
		"ue_backend_get_insname", 1)
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Error("[influxdb_backend] connect err", log.String("dbUrl", dbUrl))
		return "", err
	}
	defer db.Close()

	query := "SELECT system_identifier FROM pg_control_system()"
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error("[influxdb_backend] get meta data store config fail",
			log.String("query", query),
			log.String("dbUrl", dbUrl),
			log.String("err", err.Error()))
		return "", err
	}
	defer rows.Close()

	var system_identifier string

	if rows.Next() {
		if err := rows.Scan(&system_identifier); err != nil {
			log.Error("[influxdb_backend] row scan failed for system identifier",
				log.String("query", query),
				log.String("err", err.Error()))
			return "", err
		}
	} else {
		log.Warn("[influxdb_backend] cannot get system identifier")
		return "", errors.New("cannot get system identifier")
	}

	log.Info("[influxdb_backend] get system identifier",
		log.String("system identifier", system_identifier))

	return system_identifier, nil
}

func TimeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > SlowRequestTrackInMS {
		log.Info("[influxdb_backend] influxdb write in ms.",
			log.String("function", key),
			log.Int64("elapsed", elapsed.Milliseconds()))
	}
}
