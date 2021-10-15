/*-------------------------------------------------------------------------
 *
 * polardb_pg_multidimension_stat.go
 *    Polardb multidimension plugin
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
 *           plugins/polardb_pg_multidimension/polardb_pg_multidimension_stat.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"errors"
	"fmt"
	"reflect"

	_ "github.com/lib/pq"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/plugins/polardb_pg_multidimension/collector"
)

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("polardb_pg_stat invalid ctx")
	}
	pgCollector := collector.NewPolarDBPgMultidimensionCollector()

	err := pgCollector.Init(m)
	if err != nil {
		log.Error("[polardb_multidimension] init fail.", log.String("err", err.Error()))
		return nil, err
	}

	return pgCollector, err
}

func PluginRun(ctx interface{}, param interface{}) error {
	collector, ok := ctx.(*collector.PolarDBPgMultidimensionCollector)
	if !ok {
		return errors.New("polardb_pg_stat invalid ctx")
	}

	paraMap, ok := param.(map[string]interface{})
	if !ok {
		return fmt.Errorf("bad param type: %s" + reflect.TypeOf(param).String())
	}

	return collector.Collect(paraMap)
}

func PluginExit(ctx interface{}) error {
	collector, ok := ctx.(*collector.PolarDBPgMultidimensionCollector)
	if !ok {
		return errors.New("polardb_pg_stats invalid ctx")
	}
	err := collector.Close()
	if err != nil {
		log.Error("[polardb_multidimension] stop plugin polardb_pg_stats failed",
			log.String("err", err.Error()))
		return err
	}

	log.Info("[polardb_multidimension] stop plugin done", log.String("name", collector.InsName))
	return nil
}
