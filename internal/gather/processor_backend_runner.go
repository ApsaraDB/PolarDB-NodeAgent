/*-------------------------------------------------------------------------
 *
 * processor_backend_runner.go
 *    Polardb backend runner
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
 *           internal/gather/processor_backend_runner.go
 *-------------------------------------------------------------------------
 */

package gather

import (
	"fmt"
	"net"
	"strconv"

	"github.com/ApsaraDB/db-monitor/common/consts"
	"github.com/ApsaraDB/db-monitor/common/log"
)

var g_HostIP string
var PluginNameFormalizeMap map[string]string
var DatatypeFormalizeMap map[string]string

func (runner *Runner) buildBackendCtxMap(
	backCtxMap map[string]interface{},
	collectHeaderMap map[string]interface{},
	collectContentMap interface{},
	collectStartTime int64,
	ins *Instance) {

	backCtxMap["logical_ins_name"] = ins.env["logical_ins_name"]
	backCtxMap["physical_ins_name"] = ins.env["physical_ins_name"]
	backCtxMap["backend_hostname"] = collectHeaderMap[consts.SchemaHeaderHostname].(string)
	backCtxMap["backend_host"] = getHostIP()
	backCtxMap["backend_port"] = collectHeaderMap[consts.SchemaHeaderPort].(string)
	backCtxMap["time"] = collectStartTime
	// normalization for plugin name
	if name, ok := PluginNameFormalizeMap[runner.pInfo.Name]; ok {
		backCtxMap["plugin"] = name
	} else {
		backCtxMap["plugin"] = runner.pInfo.Name
	}
	if datatype, ok := DatatypeFormalizeMap[backCtxMap["plugin"].(string)]; ok {
		backCtxMap["datatype"] = datatype
	} else {
		backCtxMap["datatype"] = backCtxMap["plugin"].(string)
	}

	backCtxMap["collect_type"] = runner.externConf.Type
	backCtxMap["schema"] = runner.schema
}

func (runner *Runner) getBackCtxAndRunFunc(backend string,
	backCtxesMap map[string]interface{},
	backRunMap map[string]func(interface{}, interface{}) error) (map[string]interface{},
	func(interface{}, interface{}) error, error) {
	var backRun func(interface{}, interface{}) error
	var backCtxMapI interface{}
	var backCtxMap map[string]interface{}
	var ok bool

	if backCtxMapI, ok = backCtxesMap[backend]; !ok {
		log.Warn("[multibackend_runner] processor context not found",
			log.String("context", backend))
		return nil, nil, fmt.Errorf("backend ctx not found %s", backend)
	}

	if backRun, ok = backRunMap[backend]; !ok {
		log.Warn("[multibackend_runner] processor not found",
			log.String("processor", backend))
		return nil, nil, fmt.Errorf("backend func not found %s", backend)
	}

	if backCtxMap, ok = backCtxMapI.(map[string]interface{}); !ok {
		log.Warn("[multibackend_runner] backend ctx type not map[string]interface{}",
			log.String("context", backend))
		return nil, nil, fmt.Errorf("backend %s ctx type not map[string]interface{}", backend)
	}

	return backCtxMap, backRun, nil
}

func (runner *Runner) runProcessorBackends(
	backCtxesMap map[string]interface{},
	backRunMap map[string]func(interface{}, interface{}) error,
	collectHeaderMap map[string]interface{},
	collectContentMap interface{},
	collectStartTime int64,
	ins *Instance) {

	processContent := make(map[string]interface{})
	processContent["in"] = collectContentMap

	// 1. processor
	if runner.pInfo.Processor != "" {
		backCtxMap, backRun, err := runner.getBackCtxAndRunFunc(runner.pInfo.Processor,
			backCtxesMap, backRunMap)
		if err != nil {
			log.Warn("[multibackend_runner] panic processor fail.")
			return
		}

		backCtxMap["processor"] = backCtxMap["backend"]
		runner.buildBackendCtxMap(backCtxMap, collectHeaderMap, collectContentMap,
			collectStartTime, ins)
		err = backRun(backCtxMap, processContent)
		if err != nil {
			log.Error("[multibackend_runner] panic processor fail.")
			return
		}
	}

	// 2. multibackend
	backendIdxMap := make(map[string]int)
	for _, backend := range runner.pInfo.ProcessorBackends {
		backCtxMap, backRun, err := runner.getBackCtxAndRunFunc(backend, backCtxesMap, backRunMap)
		if err != nil {
			log.Warn("[multibackend_runner] panic backend fail.")
			return
		}

		if idx, iok := backendIdxMap[backend]; iok {
			backCtxMap["backend_idx"] = idx + 1
			backendIdxMap[backend] = idx + 1
		} else {
			backCtxMap["backend_idx"] = 0
			backendIdxMap[backend] = 0
		}

		if separator, ok := runner.externConf.Context["line_separator"].(string); ok {
			backCtxMap["line_separator"] = separator
		}

		runner.buildBackendCtxMap(backCtxMap, collectHeaderMap, collectContentMap,
			collectStartTime, ins)
		if runner.pInfo.Processor != "" {
			err = backRun(backCtxMap, processContent["out"])
			if err != nil {
				log.Error("[multibackend_runner] panic backend fail",
					log.String("module", ins.pInfo.Name),
					log.String("backend", backend),
					log.Int("port", ins.port),
					log.String("ctx", fmt.Sprintf("%+v", backCtxMap)),
					log.String("err", err.Error()))
			}
		} else {
			err = backRun(backCtxMap, collectContentMap)
			if err != nil {
				log.Error("[multibackend_runner] panic backend fail without processor",
					log.String("module", ins.pInfo.Name),
					log.String("backend", backend),
					log.Int("port", ins.port),
					log.String("ctx", fmt.Sprintf("%+v", backCtxMap)),
					log.String("err", err.Error()))
			}
		}
	}
}

func (runner *Runner) exitProcessorBackends(initCtx interface{}, ins *Instance) error {
	backCtxMap := make(map[string]interface{})
	backendIdxMap := make(map[string]int)

	// normalization for plugin name
	if name, ok := PluginNameFormalizeMap[runner.pInfo.Name]; ok {
		backCtxMap["plugin"] = name
	} else {
		backCtxMap["plugin"] = runner.pInfo.Name
	}
	if datatype, ok := DatatypeFormalizeMap[backCtxMap["plugin"].(string)]; ok {
		backCtxMap["datatype"] = datatype
	} else {
		backCtxMap["datatype"] = backCtxMap["plugin"].(string)
	}

	backCtxMap["logical_ins_name"] = ins.env["logical_ins_name"]
	backCtxMap["physical_ins_name"] = ins.env["physical_ins_name"]
	backCtxMap["backend_port"] = strconv.Itoa(ins.port)

	for _, backend := range runner.pInfo.ProcessorBackends {
		module, ok := runner.mInfoMap.Load(backend)
		if !ok {
			err := fmt.Errorf("backend:%s module not found,plugin:%s", backend, ins.pInfo.Name)
			log.Error("[multibackend_runner] load backend failed", log.String("error", err.Error()))
			return err
		}
		backendModule, ok := module.(*ModuleInfo)
		if !ok {
			err := fmt.Errorf("backend:%s module not match,plugin:%s", backend, ins.pInfo.Name)
			log.Error("[multibackend_runner] module not match", log.String("error", err.Error()))
			return err
		}
		ctx, ok := backendModule.Contexts.Load(backend)
		if !ok {
			err := fmt.Errorf("backend:%s ctx not found,plugin:%s", backend, ins.pInfo.Name)
			log.Error("[multibackend_runner] ctx not found", log.String("error", err.Error()))
			return err
		}

		backCtxMap["backend"] = ctx
		if idx, iok := backendIdxMap[backend]; iok {
			backCtxMap["backend_idx"] = idx + 1
			backendIdxMap[backend] = idx + 1
		} else {
			backCtxMap["backend_idx"] = 0
			backendIdxMap[backend] = 0
		}

		backExit := backendModule.PluginABI.Exit
		err := backExit(backCtxMap)
		if err != nil {
			log.Error("[runner] backend exit failed.",
				log.String("module", ins.pInfo.Name),
				log.String("backend", backend),
				log.Int("port", ins.port),
				log.String("ctx", fmt.Sprintf("%+v", ctx)),
				log.String("err", err.Error()))
		}
	}

	return nil
}

func getHostIP() string {
	if g_HostIP != "" {
		return g_HostIP
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				g_HostIP = ipnet.IP.String()
				return g_HostIP
			}
		}
	}
	return ""
}

func init() {
	PluginNameFormalizeMap = map[string]string{
		"golang-collector-polardb_pg":                     "polardb_pg_collector",
		"golang-collector-polardb_pg_k8s":                     "polardb_pg_collector",
		"golang-collector-polardb_pg_opensource_refactor": "polardb_pg_collector",
		"golang-collector-polarbox_oracle_perf":           "polardb_pg_collector",
		"golang-collector-polardb_pg_multidimension":      "polardb_pg_multidimension_collector",
		"golang-collector-polardb_pg_multidimension_k8s":      "polardb_pg_multidimension_collector",
		"golang-collector-polarbox-maxscale-perf":         "maxscale_perf",
		"golang-collector-sar":                            "sar",
		"golang-collector-cluster-manager-eventlog":       "cluster_manager_eventlog",
		"golang-collector-polardb_pg_errorlog":            "polardb_pg_errlog",
		"golang-collector-polarbox_oracle_errorlog":       "polardb_pg_errlog",
	}

	DatatypeFormalizeMap = map[string]string{
		"polardb_pg_collector":                            "polardb-o",
		"polardb_pg_multidimension_collector":             "polardb-o",
		"maxscale_perf":                                   "maxscale",
		"sar":                                             "host",
		"cluster_manager_eventlog":                        "cluster_manager",
		"polardb_pg_errlog":                               "polardb-o",
	}
}
