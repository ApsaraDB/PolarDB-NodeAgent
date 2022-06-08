/*-------------------------------------------------------------------------
 *
 * environment_collector.go
 *    environment collector interface and base class
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
 *           plugins/polardb_pg_opensource/environment/environment_collector.go
 *-------------------------------------------------------------------------
 */
package environment

import (
	"reflect"
	"runtime"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
)

const (
	KEY_SEND_TO_MULTIBACKEND = "send_to_multibackend"
)

type EnvironmentCollectorInterface interface {
	Init(m map[string]interface{}, logger *log.PluginLogger) error
	Collect(out map[string]interface{}) error
	Stop() error
}

type EnvironmentCollectorBase struct {
	logger       *log.PluginLogger
	Collectors   map[string]EnvironmentCollectorInterface
	Collectfuncs []func(map[string]interface{}) error
	DefaultOut   map[string]interface{}
}

func NewEnvironmentCollectorBase() EnvironmentCollectorBase {
	return EnvironmentCollectorBase{
		Collectors: make(map[string]EnvironmentCollectorInterface),
	}
}

func (c *EnvironmentCollectorBase) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	c.logger = logger

	for key, collectors := range c.Collectors {
		if err := collectors.Init(m, logger); err != nil {
			c.logger.Error("init failed", err, log.String("collector", key))
			return err
		}
	}

	c.DefaultOut = make(map[string]interface{})
	if _, ok := m["default_out"]; ok {
		for k, v := range m["default_out"].(map[string]interface{}) {
			c.DefaultOut[k] = v
		}
	}

	return nil
}

func (c *EnvironmentCollectorBase) RegistCollecFunc(function func(map[string]interface{}) error) {
	c.Collectfuncs = append(c.Collectfuncs, function)
}

func (c *EnvironmentCollectorBase) Collect(out map[string]interface{}) error {
	for key, collector := range c.Collectors {
		if err := collector.Collect(out); err != nil {
			c.logger.Warn("collect failed", err, log.String("key", key))
		}
	}

	for _, f := range c.Collectfuncs {
		if err := f(out); err != nil {
			c.logger.Warn("collect failed", err, log.String("collector",
				runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()))
		}
	}

	for k, v := range c.DefaultOut {
		out[k] = v
	}

	c.BuildMultiBackendOutDict(out)

	return nil
}

func (c *EnvironmentCollectorBase) BuildMultiBackendOutDict(out map[string]interface{}, keys ...string) {
	// CPU
	c.BuildOutDictSendToMultiBackend(out, "cpu_cores",
		"cpu_user_usage", "cpu_sys_usage", "cpu_total_usage",
		"cpu_iowait", "cpu_irq", "cpu_softirq", "cpu_idle",
		"cpu_nr_running", "cpu_nr_uninterrupible",
		"cpu_nr_periods", "cpu_nr_throttled", "cpu_throttled_time")

	// Memory
	c.BuildOutDictSendToMultiBackend(out,
		"mem_total", "mem_rss", "mem_cache", "mem_mapped_file", "mem_inactiverss", "mem_inactivecache",
		"cgroup_mem_usage", "mem_total_usage", "mem_total_used")

	// FS
	c.BuildOutDictSendToMultiBackend(out, "enable_pfs",
		"fs_inodes_total", "fs_inodes_used", "fs_inodes_usage",
		"fs_blocks_total", "fs_blocks_used", "fs_blocks_usage",
		"fs_size_total", "fs_size_used", "fs_size_usage")
	c.BuildOutDictSendToMultiBackend(out, "polar_base_dir_size", "polar_wal_dir_size")

	// IO
	c.BuildOutDictSendToMultiBackend(out, "pls_iops",
		"pls_iops_read", "pls_iops_write", "pls_throughput",
		"pls_throughput_read", "pls_throughput_write", "pls_latency_read",
		"pls_latency_write")
}

func (c *EnvironmentCollectorBase) BuildOutDictSendToMultiBackend(
	out map[string]interface{}, keys ...string) error {

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

func (c *EnvironmentCollectorBase) Stop() error {
	for key, collectors := range c.Collectors {
		if err := collectors.Stop(); err != nil {
			c.logger.Warn("stop failed", err, log.String("collector", key))
		}
	}
	return nil
}
