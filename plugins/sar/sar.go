/*-------------------------------------------------------------------------
 *
 * sar.go
 *    Sar plugin
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
 *           plugins/sar/sar.go
 *-------------------------------------------------------------------------
 */

package main

import (
	"errors"
	"fmt"

	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/plugins/sar/collector"
)

// PluginInit init the plugin
func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("plugin sar invalid ctx")
	}
	log.Info("Initializing sar plugin")
	c := collector.New()
	err := c.Init(m)
	if err != nil {
		return nil, fmt.Errorf("sar init failed,err:%+v", err)
	}
	log.Info("Plugin module sar init succeeded")
	return c, err
}

// PluginRun runs the plugin
func PluginRun(ctx interface{}, param interface{}) error {
	out, ok1 := param.(map[string]interface{})
	if !ok1 {
		return errors.New("plugin sar invalid ctx")
	}
	c, ok2 := ctx.(*collector.SarCollector)
	if !ok2 {
		return errors.New("plugin sar invalid ctx")
	}

	return c.Collect(out)
}

// PluginExit  exit the plugin and release related resources
func PluginExit(ctx interface{}) error {
	c, ok := ctx.(*collector.SarCollector)
	if !ok {
		return errors.New("plugin sar invalid ctx")
	}
	err := c.Stop()
	if err != nil {
		return fmt.Errorf("sar stop failed,err:%+v", err)
	}
	log.Info("stop plugin sar done")
	return nil
}
