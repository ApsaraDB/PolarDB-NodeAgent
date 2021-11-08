/*-------------------------------------------------------------------------
 *
 * prometheus_backend.go
 *    Prometheus Plugin
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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
)

const DefaultTimeout = 3
const SlowRequestTrackInMS = 500

type PrometheusCtx struct {
	Endpoint          string `json:"endpoint"`
	PreserveDimPrefix bool   `json:"preserve_dim_prefix"`
	MetricPrefix      string `json:"metric_prefix"`
	LogOutDict        bool   `json:"log_outdict"`
	Timeout           int    `json:"timeout"`
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

	prometheusCtx := &PrometheusCtx{PreserveDimPrefix: true, MetricPrefix: "", LogOutDict: false}

	err = json.Unmarshal(content, prometheusCtx)
	if err != nil {
		log.Warn("[prometheus__backend] unmarshal content failed", log.String("err", err.Error()))
		return nil, err
	}

	m["endpoint"] = prometheusCtx.Endpoint
	m["preserve_dim_prefix"] = prometheusCtx.PreserveDimPrefix
	m["log_outdict"] = prometheusCtx.LogOutDict
	m["metric_prefix"] = prometheusCtx.MetricPrefix
	if prometheusCtx.Timeout == 0 {
		m["timeout"] = DefaultTimeout
	} else {
		m["timeout"] = prometheusCtx.Timeout
	}

	log.Info("[prometheus_backend] plugin init done", log.String("m", fmt.Sprintf("%+v", m)))

	return ctx, nil
}

func buildLableMap(m map[string]interface{}) map[string]string {
	var ok bool
	var envs map[string]string

	if envs, ok = meta.GetMetaService().GetStringMap("envs", m["backend_port"].(string)); !ok {
		log.Warn("[prometheus_backend] get env map failed",
			log.String("port", m["backend_port"].(string)))
	}

	// build labels
	labelmap := map[string]string{
		"job":              "polardb_o",
		"plugin":           m["plugin"].(string),
		"datatype":         m["datatype"].(string),
		"instance":         m["logical_ins_name"].(string),
		"kind":             "polardb-o",
		"logical_insname":  m["logical_ins_name"].(string),
		"physical_insname": m["physical_ins_name"].(string),
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

	if model, ok := m["datamodel"]; ok {
		labels = append(labels, "datamodel/"+model)
	}

	for k, v := range m {
		if k == "job" {
			continue
		}

		labels = append(labels, fmt.Sprintf("%s/%s", k, v))
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
	endpoint := backendCtx["endpoint"].(string)
	labelmap := buildLableMap(m)
	paramap := param.(map[string]interface{})

	metrics := make([]string, 0)

	for model, valuelist := range paramap {
		metriclabel := make(map[string][]string)
		metricvalue := make(map[string][]interface{})
		metrictime := make(map[string][]int64)

		for _, valuemap := range valuelist.([]map[string]interface{}) {
			dimensions := make([]string, 0)
			for k, v := range valuemap {
				// string key means dimension here
				if strings.HasPrefix(k, "dim_") {
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

			dimensionstr := ""
			if len(dimensions) > 0 {
				dimensionstr = "{" + strings.Join(dimensions, ",") + "}"
			}

			for k, v := range valuemap {
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

		labelmap["datamodel"] = model
		fulllabel := buildLabelString(labelmap)

		// aggregate to value name
		for k, lables := range metriclabel {
			key := backendCtx["metric_prefix"].(string) + k
			metrics = append(metrics, fmt.Sprintf("# TYPE %s gauge", key))
			for i, lable := range lables {
				metrics = append(metrics, fmt.Sprintf("%s%s %v", key, lable, metricvalue[k][i]))
			}
		}

		if logOutDict {
			log.Info("[prometheus_backend] log out dict",
				log.String("labels", fulllabel),
				log.String("out dict", fmt.Sprintf("%+v", metrics)))
		}

		postToPrometheus(endpoint, backendCtx["timeout"].(int), fulllabel, metrics)
	}

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[prometheus_backend] PluginExit")
	return nil
}

func postToPrometheus(endpoint string, timeout int, lables string, metrics []string) error {
	if len(metrics) == 0 {
		log.Debug("[prometheus_backend] metric list is empty, we won't post anything to promethues")
		return nil
	}

	defer TimeTrack("PushToPrometheus", time.Now())

	metricstr := strings.Join(metrics, "\n") + "\n"
	requeststr := fmt.Sprintf("http://%s/metrics/%s", endpoint, lables)
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

	return nil
}

func TimeTrack(key string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > SlowRequestTrackInMS {
		log.Info("[prometheus_backend] prometheus operation in ms.",
			log.String("function", key),
			log.Int64("elapsed", elapsed.Milliseconds()))
	}
}
