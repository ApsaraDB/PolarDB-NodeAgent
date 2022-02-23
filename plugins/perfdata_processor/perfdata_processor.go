/*-------------------------------------------------------------------------
 *
 * perfdata_processor.go
 *    Perf data processor Plugin
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
 *           plugins/perfdata_processor/perfdata_processor.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/ApsaraDB/PolarDB-NodeAgent/internal/gather"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
)

type PolarDBPGConfig struct {
	ModelKeyMap     map[string][]string `json:"model_key_map"`
	EnableWhiteList bool                `json:"enable_white_list"`
	WhiteList       []string            `json:"white_list"`
	EnableBlackList bool                `json:"enable_black_list"`
	BlackList       []string            `json:"black_list"`
	WhiteListMap    map[string]string
	BlackListMap    map[string]string
}

type SarConfig struct {
	EnableCPUPerCore bool `json:"enable_cpu_percore"`
}

type ETLHandler func(
	map[string]interface{},
	interface{},
	*gather.BusinessSchema,
	map[string]interface{}) map[string]interface{}

var ETLHandlerMap map[string]ETLHandler

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("[perfdata_processor] invalid ctx")
	}
	extern := m[consts.PluginExternKey].(string)

	content, err := ioutil.ReadFile(extern)
	if err != nil {
		log.Error("[perfdata_processor] read perfdata processor conf fail",
			log.String("err", err.Error()))
		return nil, err
	}

	err = initContext(content, m)
	if err != nil {
		log.Error("[perfdata_processor] init context failed",
			log.String("err", err.Error()))
		return nil, err
	}

	log.Info("[perfdata_processor] init perfdata processor success")

	return ctx, nil
}

func PluginRun(ctx interface{}, param interface{}) error {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		log.Error("[perfdata_processor] invalid ctx")
		return errors.New("[perfdata_processor] invalid ctx")
	}

	processor, ok := m["processor"].(map[string]interface{})
	if !ok {
		log.Error("[perfdata_processor] no processor config at all")
		return errors.New("[perfdata_processor] no processor config at all")
	}

	pluginconfig, ok := processor["config"].(map[string]json.RawMessage)
	if !ok {
		log.Error("[perfdata_processor] no processor plugin config at all")
		return errors.New("[perfdata_processor] no processor plugin config at all")
	}

	var outmap map[string]interface{}
	parammap := param.(map[string]interface{})
	inmap := parammap["in"].(map[string]interface{})
	plugin := m["plugin"].(string)

	if handler, ok := ETLHandlerMap[plugin]; ok {
		config, xok := pluginconfig[plugin]
		if xok {
			outmap = handler(inmap, config, m["schema"].(*gather.BusinessSchema), m)
		} else {
			outmap = handler(inmap, nil, m["schema"].(*gather.BusinessSchema), m)
		}
	} else {
		log.Warn("[perfdata_processor] unrecognize this plugin name, use inmap as outmap directly",
			log.String("plugin", plugin))
		outmap = inmap
	}

	parammap["out"] = outmap

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[perfdata_processor] PluginExit")
	return nil
}

func rebuildPolarDBPG(in map[string]interface{},
	config interface{},
	schema *gather.BusinessSchema,
	m map[string]interface{}) map[string]interface{} {

	out := make(map[string]interface{})
	pgconfig := &PolarDBPGConfig{}
	if config != nil {
		pgconfig = config.(*PolarDBPGConfig)
	}
	modelkeymap := pgconfig.ModelKeyMap
	keymodelmap := make(map[string]string)
	for k, mlist := range modelkeymap {
		for _, m := range mlist {
			keymodelmap[m] = k
		}
	}

	// get from meta service
	metaService := meta.GetMetaService()
	configmap, ok := metaService.GetInterfaceMap("configmap", strconv.Itoa(m["port"].(int)))
	if !ok {
		log.Warn("[perfdata_processor] cannot get configmap ", log.Int("port", m["port"].(int)))
	} else {
		log.Debug("[perfdata_processor] get config map",
			log.Int("port", m["port"].(int)),
			log.String("config map", fmt.Sprintf("%+v", configmap)))
	}

	dbEnableWhiteList := GetConfigMapValue(configmap,
		"enable_polardb_pg_collect_whitelist", "integer", 1).(int)
	dbEnableBlackList := GetConfigMapValue(configmap,
		"enable_polardb_pg_collect_blacklist", "integer", 0).(int)
	dbWhiteListMap := make(map[string]string)
	dbBlackListMap := make(map[string]string)

	out["dbmetrics"] = make([]map[string]interface{}, 1)
	if _, ok := in["send_to_multibackend"]; ok {
		out["dbmetrics"].([]map[string]interface{})[0] = in["send_to_multibackend"].(map[string]interface{})
	} else {
		out["dbmetrics"].([]map[string]interface{})[0] = make(map[string]interface{})
	}
	dbmetrics := out["dbmetrics"].([]map[string]interface{})[0]

	if dbEnableWhiteList > 0 {
		whiteListStr := GetConfigMapValue(configmap,
			"polardb_pg_collect_whitelist", "string", "").(string)
		for _, x := range strings.Split(whiteListStr, ",") {
			dbWhiteListMap[x] = ""
		}
	}

	if dbEnableBlackList > 0 {
		blackListStr := GetConfigMapValue(configmap,
			"polardb_pg_collect_blacklist", "string", "").(string)
		for _, x := range strings.Split(blackListStr, ",") {
			dbBlackListMap[x] = ""
		}
	}

	for k, v := range in {
		if k == "send_to_multibackend" {
			continue
		}

		var value uint64
		if _, ok := v.(string); ok {
			value, _ = strconv.ParseUint(v.(string), 10, 64)
		} else {
			value, _ = v.(uint64)
		}

		if dbEnableWhiteList > 0 {
			if _, ok := dbWhiteListMap[k]; !ok {
				continue
			}
		}

		// black list first
		if dbEnableBlackList > 0 {
			if _, ok := dbBlackListMap[k]; ok {
				delete(dbmetrics, k)
				continue
			}
		}

		if model, ok := keymodelmap[k]; ok {
			if _, ok = out[model]; !ok {
				out[model] = make([]map[string]interface{}, 1)
				out[model].([]map[string]interface{})[0] = make(map[string]interface{})
			}
			x := out[model].([]map[string]interface{})[0]
			x[k] = value
		} else {
			dbmetrics[k] = value
		}
	}

	return out
}

func rebuildPolarDBPGMultidimension(in map[string]interface{},
	config interface{},
	schema *gather.BusinessSchema,
	m map[string]interface{}) map[string]interface{} {
	if _, ok := in["send_to_multibackend"]; ok {
		return in["send_to_multibackend"].(map[string]interface{})
	}

	return make(map[string]interface{})
}

func rebuildMaxscale(in map[string]interface{},
	config interface{},
	schema *gather.BusinessSchema, m map[string]interface{}) map[string]interface{} {
	var err error

	outmap := make(map[string]interface{})
	model := "maxscale_resource"
	results := make([]map[string]interface{}, 0)
	metricmap := make(map[string]interface{})
	results = append(results, metricmap)
	itemmap := make(map[string]int)

	// to keep either delta value or instant value in the result
	for _, item := range schema.Metrics.SchemaItems {
		if item.ValueType == gather.ListType {
			itemmap[item.Name] = 1
		}
	}

	for _, item := range schema.Metrics.SchemaItems {
		if item.ValueType == gather.IntType {
			if _, ok := in[item.Name]; !ok {
				continue
			}

			if strings.HasSuffix(item.Name, "_delta") ||
				strings.HasPrefix(item.Name, "cgroup_mem") ||
				strings.HasPrefix(item.Name, "mem_") {
				metricmap[item.Name], err = strconv.ParseUint(in[item.Name].(string), 10, 64)
				if err != nil {
					log.Warn("[perfdata_processor] convert maxscale int type value failed",
						log.String("value", in[item.Name].(string)))
				}
			}
		} else if item.ValueType == gather.MapKeyType {
			outmap[model] = results

			if _, ok := in[item.Name]; !ok {
				break
			}

			results = make([]map[string]interface{}, 0)
			model = "maxscale_" + item.Name

			for _, key := range strings.Split(in[item.Name].(string), ",") {
				metricmap = make(map[string]interface{})
				metricmap["dim_"+item.Name] = key
				results = append(results, metricmap)
			}
		} else if item.ValueType == gather.ListType {
			if _, ok := in[item.Name]; !ok {
				continue
			}

			if strings.HasSuffix(item.Name, "_delta") {
				for i, valuestr := range strings.Split(in[item.Name].(string), ",") {
					if i >= len(results) {
						log.Debug("[perfdata_processor] convert maxscale list not match",
							log.String("name", item.Name))
						break
					}
					results[i][item.Name], err = strconv.ParseUint(valuestr, 10, 64)
					if err != nil && valuestr != "" {
						log.Warn("[perfdata_processor] convert maxscale list type value failed",
							log.String("name", item.Name),
							log.String("value", in[item.Name].(string)))
					}
				}
			} else {
				if _, ok := itemmap[item.Name+"_delta"]; !ok {
					for i, valuestr := range strings.Split(in[item.Name].(string), ",") {
						if i >= len(results) {
							log.Debug("[perfdata_processor] convert maxscale list not match",
								log.String("name", item.Name))
							break
						}
						results[i][item.Name], err = strconv.ParseUint(valuestr, 10, 64)
						if err != nil && valuestr != "" {
							log.Warn("[perfdata_processor] convert maxscale list type value failed",
								log.String("name", item.Name),
								log.String("value", in[item.Name].(string)))
						}
					}
				}
			}
		}
	}

	outmap[model] = results

	return outmap
}

func rebuildSarMap(param map[string]interface{},
	config interface{},
	schema *gather.BusinessSchema,
	m map[string]interface{}) map[string]interface{} {

	sarconfig := &SarConfig{EnableCPUPerCore: true}
	if config != nil {
		err := json.Unmarshal(config.(json.RawMessage), sarconfig)
		if err != nil {
			log.Warn("[perfdata_processor] unmarshal failed for sar config")
		}
	}

	listkey := map[string][]string{
		"cpu_list": []string{
			"user_delta",
			"nice_delta",
			"system_delta",
			"idle_delta",
			"iowait_delta",
			"irq_delta",
			"softirq_delta",
			"steal_delta",
			"guest_delta",
			"guest_nice_delta"},
		"disk_list": []string{
			"read_complete_delta",
			"read_merge_delta",
			"read_sectors_delta",
			"read_time_delta",
			"write_complete_delta",
			"write_merge_delta",
			"write_sectors_delta",
			"write_time_delta",
			"io_in_flight",
			"io_time_delta",
			"io_weight_time_delta"},
		"nic_list": []string{
			"recv_bytes_delta",
			"recv_packets_delta",
			"recv_errs_delta",
			"recv_drop_delta",
			"recv_fifo_delta",
			"recv_frame_delta",
			"recv_compressed_delta",
			"recv_multicast_delta",
			"transmit_bytes_delta",
			"transmit_packets_delta",
			"transmit_errs_delta",
			"transmit_drop_delta",
			"transmit_fifo_delta",
			"transmit_colls_delta",
			"transmit_carrier_delta",
			"transmit_compressed_delta"},
		"fs_partition_list": []string{"bsize", "blocks", "bfree", "bavail", "files", "ffree"},
	}

	listkeymap := map[string]string{
		"cpu_list":          "cpu",
		"disk_list":         "disk",
		"fs_partition_list": "fs",
		"nic_list":          "netdev",
	}

	listdimmap := map[string][]string{
		"cpu_list":          []string{"dim_cpu"},
		"disk_list":         []string{"dim_disk"},
		"fs_partition_list": []string{"dim_partition", "dim_fs"},
		"nic_list":          []string{"dim_dev"},
	}

	mapkey := map[string][]string{
		"procs":   []string{"context_switch_delta", "procs_running", "procs_blocked"},
		"loadavg": []string{"load1", "load5", "load15"},
		"memory": []string{"MemTotal",
			"MemFree",
			"MemAvailable",
			"Buffers",
			"Cached",
			"SwapCached",
			"Active",
			"Inactive",
			"Active(anon)",
			"Inactive(anon)",
			"Active(file)",
			"Inactive(file)",
			"Unevictable",
			"Mlocked",
			"SwapTotal",
			"SwapFree",
			"Dirty",
			"Writeback",
			"AnonPages",
			"Mapped",
			"Shmem",
			"Slab",
			"SReclaimable",
			"SUnreclaim",
			"KernelStack",
			"PageTables",
			"NFS_Unstable",
			"Bounce",
			"WritebackTmp",
			"CommitLimit",
			"Committed_AS",
			"VmallocTotal",
			"VmallocUsed",
			"VmallocChunk",
			"HardwareCorrupted",
			"AnonHugePages",
			"HugePages_Total",
			"HugePages_Free",
			"HugePages_Rsvd",
			"HugePages_Surp",
			"Hugepagesize",
			"DirectMap4k",
			"DirectMap2M",
			"DirectMap1G"},
		"socketstat": []string{
			"sockets_used",
			"tcp_inuse",
			"tcp_orphan",
			"tcp_tw",
			"tcp_alloc",
			"tcp_mem"},
		"snmpstat": []string{
			"AttemptFails_delta",
			"CurrEstab",
			"EstabResets_delta",
			"InErrs_delta",
			"InSegs_delta",
			"MaxConn",
			"OutRsts_delta",
			"OutSegs_delta",
			"PassiveOpens_delta",
			"RetransSegs_delta",
		},
		"netstat": []string{
			"ListenOverflows_delta",
			"ListenDrops_delta",
			"TWRecycled_delta",
			"TCPFastRetrans_delta",
			"TCPForwardRetrans_delta",
		},
	}

	out := map[string]interface{}{
		"cpu":        make([]map[string]interface{}, 0),
		"loadavg":    make([]map[string]interface{}, 0),
		"memory":     make([]map[string]interface{}, 0),
		"netdev":     make([]map[string]interface{}, 0),
		"socketstat": make([]map[string]interface{}, 0),
		"disk":       make([]map[string]interface{}, 0),
		"procs":      make([]map[string]interface{}, 0),
		"fs":         make([]map[string]interface{}, 0),
		"snmpstat":   make([]map[string]interface{}, 0),
		"netstat":    make([]map[string]interface{}, 0),
		// "sysinfo":    make([]map[string]interface{}, 0, 1),
	}

	var err error

	for datamodel, keylist := range mapkey {
		modelmap := make(map[string]interface{})
		for _, key := range keylist {
			if res, ok := param[key]; ok {
				k := strings.ToLower(key)
				k = strings.Replace(k, "(", "_", -1)
				k = strings.Replace(k, ")", "", -1)
				if x, err := strconv.ParseFloat(res.(string), 64); err == nil {
					modelmap[k] = x
				} else {
					log.Warn("[perfdata_processor] parse single value failed",
						log.String("key", key),
						log.String("res", res.(string)))
				}
			}
		}

		out[datamodel] = append(out[datamodel].([]map[string]interface{}), modelmap)
	}

	for datamodelkey, keylist := range listkey {
		dimlist := strings.Split(param[datamodelkey].(string), ",")
		indexlist := make([]int, 0)
		result := make([]map[string]interface{}, 0)

		for i, item := range dimlist {
			if datamodelkey == "cpu_list" {
				// if item == "cpu" {
				// 	out["sysinfo"].([]map[string]interface{})[0]["cpucores"] = len(dimlist) - 1
				// }

				if !sarconfig.EnableCPUPerCore {
					if item != "cpu" {
						continue
					}
				}
			}

			m := make(map[string]interface{})
			for j, itemj := range strings.Split(item, "|||") {
				if j >= len(listdimmap[datamodelkey]) {
					log.Warn("[perfdata_processor] parse multi key failed",
						log.String("key", item),
						log.String("map list", fmt.Sprintf("%+v", listdimmap[datamodelkey])))
					continue
				}
				// log.Info("split it", log.String("item", fmt.Sprintf("%+v", item)), log.String("key", listdimmap[datamodelkey][j]))
				m[listdimmap[datamodelkey][j]] = itemj
			}

			indexlist = append(indexlist, i)
			result = append(result, m)
		}

		for _, key := range keylist {
			if _, ok := param[key]; !ok {
				continue
			}

			itemlist := strings.Split(param[key].(string), ",")
			index := 0
			for _, i := range indexlist {
				result[index][key], err = strconv.ParseFloat(itemlist[i], 64)
				if err != nil {
					log.Warn("[perfdata_processor] parse list error",
						log.String("key", key), log.String("res", itemlist[i]))
				}
				index += 1
			}
		}

		out[listkeymap[datamodelkey]] = result
	}

	return out
}

func initContext(content []byte, m map[string]interface{}) error {
	configMap := make(map[string]json.RawMessage)
	err := json.Unmarshal(content, &configMap)
	if err != nil {
		log.Warn("[perfdata_processor] unmarshal content failed",
			log.String("content", string(content)),
			log.String("err", err.Error()))
		return err
	}

	m["config"] = configMap

	return nil
}

func GetConfigMapValue(m map[string]interface{},
	key string, valueType string, defValue interface{}) interface{} {
	if v, ok := m[key]; ok {
		value, ok := v.(string)
		if !ok {
			log.Debug("[perfdata_processor] config value is not string",
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
				log.Warn("[perfdata_processor] config value type is not integer",
					log.String("err", err.Error()),
					log.String("key", key),
					log.String("value", fmt.Sprintf("%+v", intv)))
				return defValue
			}
		default:
			log.Debug("[perfdata_processor] cannot recognize this value type",
				log.String("type", valueType))
			return defValue
		}
	}

	log.Debug("[perfdata_processor] cannot get config map key", log.String("key", key))
	return defValue
}

func init() {
	ETLHandlerMap = map[string]ETLHandler{
		"polardb_pg_collector":                rebuildPolarDBPG,
		"polardb_pg_multidimension_collector": rebuildPolarDBPGMultidimension,
		"sar":                                 rebuildSarMap,
		"maxscale_perf":                       rebuildMaxscale,
	}
}
