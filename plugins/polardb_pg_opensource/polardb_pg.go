/*-------------------------------------------------------------------------
 *
 * polardb_pg.go
 *    Polardb pg collector plugin
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
 *           plugins/polardb_pg_opensource/polardb_pg.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"errors"

	"github.com/ApsaraDB/db-monitor/plugins/polardb_pg_opensource/service"

	_ "github.com/lib/pq"
	"github.com/ApsaraDB/db-monitor/common/log"
	pg "github.com/ApsaraDB/db-monitor/plugins/polardb_pg_opensource/collector"
)

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("plugin polardb_pg invalid ctx")
	}
	log.Info("Initializing polardb_pg plugin")
	collector := pg.New()
	err := collector.Init(m)
	if err != nil {
		log.Error("plugin module polardb_pg init failed",
			log.String("err", err.Error()),
			log.Int("port", collector.Port),
			log.String("name", collector.InsName))
		return nil, err
	}
	svc := service.GetPolarDBPgService()
	if svc == nil {
		return nil, errors.New("failed to initialize PolarDBPgService")
	}

	return collector, err
}

func PluginRun(ctx interface{}, param interface{}) error {

	collector, ok := ctx.(*pg.PolarDBPgCollector)
	if !ok {
		log.Error("plugin polardb_pg invalid collector_ctx")
		return errors.New("plugin polardb_pg invalid collector_ctx")
	}
	out, ok := param.(map[string]interface{})
	if !ok {
		return errors.New("plugin polardb_pg invalid param")
	}
	err := collector.Collect(out)

	if len(out) != 0 {
		res := map[string]interface{}{}
		for k, v := range out {
			res[k] = v
		}
		service.GetPolarDBPgService().Set(collector.HostInsIdStr, res)
	}

	return err
}

func PluginExit(ctx interface{}) error {
	collector, ok := ctx.(*pg.PolarDBPgCollector)
	if !ok {
		return errors.New("plugin polardb_pg invalid ctx")
	}
	err := collector.Stop()
	if err != nil {
		log.Error("stop polardb_pg plugin err",
			log.String("err", err.Error()),
			log.Int("port", collector.Port))
		return err
	}
	log.Info("stop polardb_pg plugin done",
		log.String("name", collector.InsName),
		log.Int("port", collector.Port))
	return nil
}
