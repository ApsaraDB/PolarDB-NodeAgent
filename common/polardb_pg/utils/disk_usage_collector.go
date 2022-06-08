/*-------------------------------------------------------------------------
 *
 * disk_usage_collector.go
 *    async disk usage collect wrapper
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
 *           common/polardb_pg/utils/disk_usage_collector.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"fmt"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
)

type ImportedCollector struct {
	newInstance    func(ctx interface{}, home, regex string, dirs ...string) error
	removeInstance func(ctx interface{}, home string, dirs ...string) error
	dirSize        func(interface{}, string) (int64, error)
}

type DiskUsageCollector struct {
	importedCollector *ImportedCollector
	plutoCtx          interface{}
	dirList           []string
}

func NewDiskUsageCollector() *DiskUsageCollector {
	return &DiskUsageCollector{
		importedCollector: &ImportedCollector{},
		dirList:           make([]string, 0),
	}
}

func (c *DiskUsageCollector) Init(m map[string]interface{}) error {
	importsMap, ok := m[consts.PluginContextKeyImportsMap].(map[string]interface{})
	if !ok {
		return fmt.Errorf("init: imports not found: %v", m)
	}

	newInstanceFn, ok := importsMap[consts.PlutoNewInstanceIdentifier]
	if !ok {
		return fmt.Errorf("newInstance func not found: %s", consts.PlutoNewInstanceIdentifier)
	}
	c.importedCollector.newInstance, _ =
		newInstanceFn.(func(ctx interface{}, home, regex string, dirs ...string) error)

	removeInstanceFn, ok := importsMap[consts.PlutoRemoveInstanceIdentifier]
	if !ok {
		return fmt.Errorf("removeInstance func not found: %s", consts.PlutoRemoveInstanceIdentifier)
	}
	c.importedCollector.removeInstance =
		removeInstanceFn.(func(ctx interface{}, home string, dirs ...string) error)

	dirSizeFn, ok := importsMap[consts.PlutoDirSizeCollectorIdentifier]
	if !ok {
		return fmt.Errorf("dirSize func not found: %s", consts.PlutoDirSizeCollectorIdentifier)
	}
	c.importedCollector.dirSize = dirSizeFn.(func(ctx interface{}, path string) (int64, error))

	plutoCtx, ok := m[consts.PlutoPluginIdentifier]
	if !ok {
		return fmt.Errorf("context not found: %s", consts.PlutoPluginIdentifier)
	}
	c.plutoCtx = plutoCtx

	c.dirList = append(c.dirList, m["dir_list"].([]string)...)
	c.importedCollector.newInstance(plutoCtx, "", "", c.dirList...)

	return nil
}

func (c *DiskUsageCollector) Collect(dirname string) (int64, error) {
	if size, err := c.importedCollector.dirSize(c.plutoCtx, dirname); err != nil {
		return 0, err
	} else {
		return size, nil
	}
}

func (c *DiskUsageCollector) Close() error {
	c.importedCollector.removeInstance(c.plutoCtx, "", c.dirList...)
	return nil
}
