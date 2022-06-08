/*-------------------------------------------------------------------------
 *
 * prometheus_backend.go
 *    prometheus backend
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
 *           plugins/prometheus_backend/prometheus_backend.go
 *-------------------------------------------------------------------------
 */

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/db_backend/dao"
)

const DefaultTimeout = 3
const SlowRequestTrackInMS = 500

const PrometheusLabelCleanTTL = 300
const PrometheusLabelCleanInterval = 60

const DefaultExporterPort = 9974

var PushgatewayEndpoint = ""

type PrometheusCtx struct {
	PushgatewayEndpoint string `json:"endpoint"`
	PreserveDimPrefix   bool   `json:"preserve_dim_prefix"`
	MetricPrefix        string `json:"metric_prefix"`
	EnableExporter      bool   `json:"enable_exporter"`
	EnablePushgateway   bool   `json:"enable_pushgateway"`
	ExporterPort        int    `json:"exporter_port"`
	LogOutDict          bool   `json:"log_outdict"`
	Timeout             int    `json:"timeout"`
	LabelCleanTTL       int64  `json:"label_clean_ttl"`
	LabelCleanInterval  int64  `json:"label_clean_interval"`
}

type ExporterService struct {
	mutex    *sync.Mutex
	labelmap map[string]string
}

func NewExporterService() *ExporterService {
	return &ExporterService{
		labelmap: make(map[string]string),
		mutex:    &sync.Mutex{},
	}
}

var ExporterServiceOnce sync.Once
var ExporterServiceIns *ExporterService = nil

func GetExporterService(port int) *ExporterService {
	ExporterServiceOnce.Do(func() {
		ExporterServiceIns = NewExporterService()
		err := ExporterServiceIns.Start(port)
		if err != nil {
			log.Error("[prometheus_exporter] Failed to initialize Exporter Service ", log.String("err", err.Error()))
			ExporterServiceIns = nil
		}
	})
	return ExporterServiceIns
}

func (s *ExporterService) Set(label string, metric string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.labelmap[label] = metric
}

func (s *ExporterService) Delete(label string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.labelmap[label]; ok {
		delete(s.labelmap, label)
		return nil
	}

	return fmt.Errorf("label key not exist: %s", label)
}

func (s *ExporterService) Start(port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", s)

	portstr := strconv.Itoa(port)

	go func() {
		for {
			err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
			if err == http.ErrServerClosed {
				log.Info("exporter service closed!")
				return
			}
			log.Error("[prometheus_exporter] exporter service stopped unexpected err %s, retrying",
				log.String("err", err.Error()),
				log.String("port", portstr))
			time.Sleep(time.Second * 60)
		}
	}()

	log.Info("[prometheus_exporter] success to initialize exporter service ", log.String("port", portstr))
	return nil
}

func (s *ExporterService) ServeHTTP(rsp http.ResponseWriter, req *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	defer func() {
		if err := recover(); err != nil {
			s.ResponseError(rsp, fmt.Errorf("%v", err))
		}
	}()

	fullresponse := ""
	for _, v := range s.labelmap {
		fullresponse += v
	}
	rsp.Write([]byte(fullresponse))
}

func (s *ExporterService) ResponseError(rsp http.ResponseWriter, err error) {
	log.Warn("exporter service response err", log.String("err", err.Error()))
	rsp.Write([]byte(err.Error()))
}

type LabelTTL struct {
	modify int64
	ttl    int64
}

type cleaner func(label string) error

type LabelCleaner struct {
	mutex         *sync.Mutex
	cleanInterval int64
	cleanTTL      int64
	labelmap      *sync.Map
	cleanermap    map[string]cleaner
}

func (p *LabelCleaner) Init(interval, ttl int64) {
	p.cleanInterval = interval
	p.cleanTTL = ttl
	log.Info("[prometheus_backend] label cleaner init",
		log.Int64("interval", interval),
		log.Int64("ttl", ttl))
	p.cleanermap = make(map[string]cleaner)
	p.labelmap = &sync.Map{}
	p.mutex = &sync.Mutex{}
	go p.backgroundCleaner()
}

func (p *LabelCleaner) AddLable(label string) {
	p.labelmap.Store(label, LabelTTL{time.Now().Unix(), p.cleanTTL})
}

func (p *LabelCleaner) RegistCleaner(name string, cleaner cleaner) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cleanermap[name] = cleaner
}

func ExporterServiceCleaner(key string) error {
	log.Info("[prometheus_backend] exporter service label clean", log.String("label", key))
	if err := GetExporterService(0).Delete(key); err != nil {
		log.Warn("[prometheus_backend] delete exporter service key failed", log.String("label", key))
	}

	return nil
}

func PushgatewayCleaner(key string) error {
	log.Info("[prometheus_backend] pushgateway label clean", log.String("label", key))

	requeststr := fmt.Sprintf("http://%s/metrics/%s", PushgatewayEndpoint, key)

	client := &http.Client{
		Timeout: time.Duration(5) * time.Second,
	}

	req, err := http.NewRequest("DELETE", requeststr, nil)
	if err != nil {
		log.Error("[prometheus_backend] delete failed",
			log.String("request", requeststr),
			log.String("error", err.Error()))
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Warn("[prometheus_backend] delete response failed",
			log.String("error", err.Error()))
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Debug("[prometheus_backend] clean body",
		log.String("response", string(body)),
		log.Int("code", resp.StatusCode))
	if err != nil {
		log.Warn("[prometheus_backend] read body failed",
			log.String("error", err.Error()))
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		log.Warn("[prometheus_backend] clean response status", log.Int("status", resp.StatusCode))
		return fmt.Errorf("clean label response failed with code %d", resp.StatusCode)
	}

	return nil
}

func (p *LabelCleaner) backgroundCleaner() {
	for {
		p.labelmap.Range(func(k, v interface{}) bool {
			if time.Now().Unix()-v.(LabelTTL).modify > v.(LabelTTL).ttl {
				p.mutex.Lock()
				for name, cleaner := range p.cleanermap {
					err := cleaner(k.(string))
					if err != nil {
						log.Error("[prometheus_backend] label clean failed",
							log.String("cleaner func", name),
							log.String("label", k.(string)),
							log.String("error", err.Error()))
					}
				}
				p.mutex.Unlock()
				p.labelmap.Delete(k)
			}

			return true
		})

		time.Sleep(time.Duration(p.cleanInterval) * time.Second)
	}
}

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("[prometheus_backend] invalid ctx")
	}
	extern := m[consts.PluginExternKey].(string)

	log.Info("[prometheus_backend] plugin init", log.String("m", fmt.Sprintf("%+v", m)))
	content, err := ioutil.ReadFile(extern)
	if err != nil {
		log.Error("[prometheus_backend] read prometheus conf fail", log.String("err", err.Error()))
		return nil, err
	}

	prometheusCtx := &PrometheusCtx{
		PushgatewayEndpoint: "",
		PreserveDimPrefix:   true,
		MetricPrefix:        "",
		LogOutDict:          false,
		LabelCleanInterval:  int64(PrometheusLabelCleanInterval),
		LabelCleanTTL:       int64(PrometheusLabelCleanTTL),
		EnableExporter:      true,
		EnablePushgateway:   true,
		ExporterPort:        DefaultExporterPort,
	}

	err = json.Unmarshal(content, prometheusCtx)
	if err != nil {
		log.Warn("[prometheus_backend] unmarshal content failed", log.String("err", err.Error()))
		return nil, err
	}

	PushgatewayEndpoint = prometheusCtx.PushgatewayEndpoint
	m["preserve_dim_prefix"] = prometheusCtx.PreserveDimPrefix
	m["log_outdict"] = prometheusCtx.LogOutDict
	m["metric_prefix"] = prometheusCtx.MetricPrefix
	m["enable_exporter"] = prometheusCtx.EnableExporter
	m["enable_pushgateway"] = prometheusCtx.EnablePushgateway
	m["label_clean_interval"] = prometheusCtx.LabelCleanInterval
	m["label_clean_ttl"] = prometheusCtx.LabelCleanTTL
	m["exporter_port"] = prometheusCtx.ExporterPort

	if prometheusCtx.Timeout == 0 {
		m["timeout"] = DefaultTimeout
	} else {
		m["timeout"] = prometheusCtx.Timeout
	}

	cleaner := &LabelCleaner{}
	cleaner.Init(
		m["label_clean_interval"].(int64),
		m["label_clean_ttl"].(int64))

	m["cleaner"] = cleaner

	if prometheusCtx.EnableExporter {
		GetExporterService(prometheusCtx.ExporterPort)
		cleaner.RegistCleaner("exporter", ExporterServiceCleaner)
		log.Info("[prometheus_backend] start exporter service", log.Int("port", prometheusCtx.ExporterPort))
	}

	if prometheusCtx.EnablePushgateway {
		cleaner.RegistCleaner("pushgateway", PushgatewayCleaner)
	}

	log.Info("[prometheus_backend] plugin init done", log.String("content", string(content)), log.String("m", fmt.Sprintf("%+v", m)))

	return ctx, nil
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

func buildLabelString(m map[string]string) string {
	labels := make([]string, 0)

	if job, ok := m["job"]; ok {
		labels = append(labels, "job/"+job)
	}

	if instance, ok := m["instance"]; ok {
		labels = append(labels, "instance/"+instance)
	}

	if model, ok := m["datamodel"]; ok {
		labels = append(labels, "datamodel/"+model)
	}

	// sort keys' order
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if k == "job" || k == "datamodel" || k == "instance" {
			continue
		}

		labels = append(labels, fmt.Sprintf("%s/%s", k, m[k]))
	}

	return strings.Join(labels, "/")
}

func PluginRun(ctx interface{}, param interface{}) error {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return errors.New("[prometheus_backend] invalid ctx")
	}

	backendCtx, ok := m["backend"].(map[string]interface{})
	if !ok {
		return errors.New("[prometheus_backend] no backends ctx")
	}

	preserveDim := backendCtx["preserve_dim_prefix"].(bool)
	logOutDict := backendCtx["log_outdict"].(bool)
	enableExporter := backendCtx["enable_exporter"].(bool)
	enablePushgateway := backendCtx["enable_pushgateway"].(bool)
	paramap := param.(map[string]interface{})
	datatype := m["datatype"].(string)

	if datatype == "maxscale" || datatype == "cluster_manager" {
		if x, ok := meta.GetMetaService().GetInterface("topology", m["backend_port"].(string)); ok {
			dbinfo := x.(*dao.DBInfo)
			if dbinfo.InsName != "" {
				log.Debug("[prometheus_backend] use meta dbinfo",
					log.String("ins", dbinfo.InsName))
				m["logical_ins_name"] = dbinfo.InsName
			} else {
				insname, err := GetInsNameFromDBInfo(dbinfo)
				if err != nil {
					log.Warn("[prometheus_backend] get logical ins name failed",
						log.String("err", err.Error()))
				} else {
					log.Debug("[prometheus_backend] get logical ins name",
						log.String("ins", insname))
					dbinfo.InsName = insname
					m["logical_ins_name"] = insname
				}
			}
		} else {
			log.Debug("[prometheus_backend] cannot get port",
				log.String("port", m["backend_port"].(string)))
			return nil
		}
	}

	for model, valuelist := range paramap {
		pushgatewaymetrics := make([]string, 0)
		exportermetrics := make([]string, 0)
		labelmap := buildLableMap(m)
		metriclabel := make(map[string][]string)
		metricvalue := make(map[string][]interface{})
		metrictime := make(map[string][]int64)
		labelmap["datamodel"] = model

		for _, valuemap := range valuelist.([]map[string]interface{}) {
			dimensions := make([]string, 0)
			for k, v := range valuemap {
				// string key means dimension here
				if strings.HasPrefix(k, "dim_") {
					if k == "dim_uuid" {
						continue
					}

					if preserveDim {
						dimensions = append(dimensions,
							fmt.Sprintf("%s=%s", k, strconv.Quote(v.(string))))
					} else {
						dimensions = append(dimensions,
							fmt.Sprintf("%s=%s",
								strings.TrimPrefix(k, "dim_"),
								strconv.Quote(v.(string))))
					}
				} else if x, ok := v.(string); ok {
					dimensions = append(dimensions, fmt.Sprintf("%s=%s", k, strconv.Quote(x)))
				}
			}

			if enableExporter {
				for k, v := range labelmap {
					dimensions = append(dimensions, fmt.Sprintf("%s=%s", k, strconv.Quote(v)))
				}
			}

			dimensionstr := ""
			if len(dimensions) > 0 {
				dimensionstr = "{" + strings.Join(dimensions, ",") + "}"
			}

			for k, v := range valuemap {
				if k == "time" {
					continue
				}

				if _, ok := v.(string); ok {
					continue
				}

				if _, ok := metriclabel[k]; !ok {
					metriclabel[k] = make([]string, 0)
					metricvalue[k] = make([]interface{}, 0)
					metrictime[k] = make([]int64, 0)
				}

				metriclabel[k] = append(metriclabel[k], dimensionstr)
				metricvalue[k] = append(metricvalue[k], v)
				if t, ok := valuemap["time"]; ok {
					metrictime[k] = append(metrictime[k], t.(int64)*1000)
				} else {
					metrictime[k] = append(metrictime[k], int64(0))
				}
			}
		}

		fulllabel := buildLabelString(labelmap)

		// aggregate to value name
		for k, lables := range metriclabel {
			key := backendCtx["metric_prefix"].(string) + k
			if enableExporter {
				exportermetrics = append(exportermetrics, fmt.Sprintf("# TYPE %s gauge", key))
				for i, lable := range lables {
					if metrictime[k][i] == int64(0) {
						exportermetrics = append(exportermetrics,
							fmt.Sprintf("%s%s %v", key, lable, metricvalue[k][i]))
					} else {
						exportermetrics = append(exportermetrics,
							fmt.Sprintf("%s%s %v %d", key, lable, metricvalue[k][i], metrictime[k][i]))
					}
				}
			}

			if enablePushgateway {
				pushgatewaymetrics = append(pushgatewaymetrics,
					fmt.Sprintf("# TYPE %s gauge", key))
				for i, lable := range lables {
					pushgatewaymetrics = append(pushgatewaymetrics,
						fmt.Sprintf("%s%s %v", key, lable, metricvalue[k][i]))
				}

			}
		}

		if logOutDict {
			if enableExporter {
				log.Info("[prometheus_backend] exporter out dict",
					log.String("labels", fulllabel),
					log.String("out dict", fmt.Sprintf("%+v", exportermetrics)))
			}

			if enablePushgateway {
				log.Info("[prometheus_backend] pushgateway out dict",
					log.String("labels", fulllabel),
					log.String("out dict", fmt.Sprintf("%+v", pushgatewaymetrics)))
			}
		}

		if enableExporter {
			GetExporterService(0).Set(fulllabel, strings.Join(exportermetrics, "\n")+"\n")
		}

		if enablePushgateway {
			PostToPushgateway(backendCtx["timeout"].(int), fulllabel, pushgatewaymetrics)
		}

		backendCtx["cleaner"].(*LabelCleaner).AddLable(fulllabel)
	}

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[prometheus_backend] PluginExit")
	return nil
}

func PostToPushgateway(timeout int, lables string, metrics []string) error {
	if len(metrics) == 0 {
		log.Debug("[prometheus_backend] metric list is empty, we won't post anything to promethues")
		return nil
	}

	defer TimeTrack("PushToPrometheus", time.Now())

	metricstr := strings.Join(metrics, "\n") + "\n"
	requeststr := fmt.Sprintf("http://%s/metrics/%s", PushgatewayEndpoint, lables)
	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	resp, err := client.Post(requeststr, "", strings.NewReader(metricstr))
	if err != nil {
		log.Warn("[prometheus_backend] response failed", log.String("error", err.Error()))
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Debug("[prometheus_backend] response body",
		log.String("response", string(body)),
		log.Int("code", resp.StatusCode))
	if err != nil {
		log.Warn("[prometheus_backend] read body failed", log.String("error", err.Error()))
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		log.Warn("[prometheus_backend] post to pushgateway failed", log.Int("code", resp.StatusCode))
		return fmt.Errorf("post to pushgateway failed with code %d", resp.StatusCode)
	}

	return nil
}

func GetInsNameFromDBInfo(dbinfo *dao.DBInfo) (string, error) {
	dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d password=%s "+
		"fallback_application_name=%s sslmode=disable connect_timeout=%d",
		dbinfo.Host, dbinfo.UserName, dbinfo.DBName, dbinfo.Port, dbinfo.Password,
		"ue_backend_get_insname", 1)
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Error("[prometheus_backend] connect err", log.String("dbUrl", dbUrl))
		return "", err
	}
	defer db.Close()

	query := "SELECT system_identifier FROM pg_control_system()"
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error("[prometheus_backend] get meta data store config fail",
			log.String("query", query),
			log.String("dbUrl", dbUrl),
			log.String("err", err.Error()))
		return "", err
	}
	defer rows.Close()

	var system_identifier string

	if rows.Next() {
		if err := rows.Scan(&system_identifier); err != nil {
			log.Error("[prometheus_backend] row scan failed for system identifier",
				log.String("query", query),
				log.String("err", err.Error()))
			return "", err
		}
	} else {
		log.Warn("[prometheus_backend] cannot get system identifier")
		return "", errors.New("cannot get system identifier")
	}

	log.Info("[prometheus_backend] get system identifier",
		log.String("system identifier", system_identifier))

	return system_identifier, nil
}

func TimeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > SlowRequestTrackInMS {
		log.Info("[prometheus_backend] prometheus operation in ms.",
			log.String("function", key),
			log.Int64("elapsed", elapsed.Milliseconds()))
	}
}
