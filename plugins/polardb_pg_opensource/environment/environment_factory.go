/*-------------------------------------------------------------------------
 *
 * environment_factory.go
 *    environment collector factory
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
 *           plugins/polardb_pg_opensource/environment/environment_factory.go
 *-------------------------------------------------------------------------
 */
package environment

import (
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
)

func CreateEnvironmentCollector(m map[string]interface{},
	logger *log.PluginLogger) EnvironmentCollectorInterface {
	environment := m["environment"].(string)
	switch environment {
	case "polarflex":
		return &PolarFlexEnvironmentCollector{}
	default:
		return &PolarFlexEnvironmentCollector{}
	}
}
