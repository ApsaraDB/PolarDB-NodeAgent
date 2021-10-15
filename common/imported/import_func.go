/*-------------------------------------------------------------------------
 *
 * import_func.go
 *    Get plugin import function
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
 *           common/imported/import_func.go
 *-------------------------------------------------------------------------
 */
package imported

import (
	"errors"

	"github.com/ApsaraDB/db-monitor/common/consts"
)

type PlutoCollector struct {
	PlutoDirSize                  func(ctx interface{}, path string) (int64, error)
	PlutoFilteredDirSize          func(ctx interface{}, path string) (int64, error)
	PlutoNewInstance              func(ctx interface{}, home, regex string, dirs ...string) error
	PlutoRemoveInstance           func(ctx interface{}, home string, dirs ...string) error
	PlutoRegisterDiskDetailSize   func(ctx interface{}, port int, hostDataDir, hostLogDir, homeDir, dataDir, tmpDir, backupDir string) error
	PlutoUnRegisterDiskDetailSize func(ctx interface{}, port int) error
	PlutoDiskDetailSize           func(ctx interface{}, port int) (int64, int64, int64, int64, int64, int64, int64, int64, int64, int64, error)
}

func (pc *PlutoCollector) InitCollector(importsMap map[string]interface{}) error {
	// new instance function
	plutoNewInstanceFn, ok := importsMap[consts.PlutoNewInstanceIdentifier]
	if !ok {
		return errors.New("plutoNewInstance func not found")
	}
	pc.PlutoNewInstance, ok = plutoNewInstanceFn.(func(ctx interface{}, home, regex string, dirs ...string) error)

	plutoRemoveInstanceFn, ok := importsMap[consts.PlutoRemoveInstanceIdentifier]
	if !ok {
		return errors.New("plutoRemoveInstance func not found")
	}
	pc.PlutoRemoveInstance = plutoRemoveInstanceFn.(func(ctx interface{}, home string, dirs ...string) error)

	plutoDirSizeFn, ok := importsMap[consts.PlutoDirSizeCollectorIdentifier]
	if !ok {
		return errors.New("plutoDirSize func not found")
	}
	pc.PlutoDirSize = plutoDirSizeFn.(func(ctx interface{}, path string) (int64, error))

	// binlog size function
	plutoFilteredDirSizeFn, ok := importsMap[consts.PlutoFilteredDirSizeCollectorIdentifier]
	if ok {
		pc.PlutoFilteredDirSize = plutoFilteredDirSizeFn.(func(ctx interface{}, path string) (int64, error))
	}

	plutoRegisterDiskDetailSizeFn, ok := importsMap[consts.PlutoRegisterMySQLDiskDetailCollectorIdentifier]
	if ok {
		pc.PlutoRegisterDiskDetailSize = plutoRegisterDiskDetailSizeFn.(func(ctx interface{}, port int, hostDataDir, hostLogDir, homeDir, dataDir, tmpDir, backupDir string) error)
	}

	plutoUnRegisterDiskDetailSizeFn, ok := importsMap[consts.PlutoUnRegisterMySQLDiskDetailCollectorIdentifier]
	if ok {
		pc.PlutoUnRegisterDiskDetailSize = plutoUnRegisterDiskDetailSizeFn.(func(ctx interface{}, port int) error)
	}

	plutoDiskDetailSizeFn, ok := importsMap[consts.PlutoMysqlDiskDetailSizeCollectorIdentifier]
	if ok {
		pc.PlutoDiskDetailSize = plutoDiskDetailSizeFn.(func(ctx interface{}, port int) (int64, int64, int64, int64, int64, int64, int64, int64, int64, int64, error))
	}
	return nil
}

type SaturnCollector struct {
	SaturnNewInstance      func(ctx interface{}, port int) error
	SaturnRemoveInstance   func(ctx interface{}, port int) error
	SaturnDeletedFilesSize func(ctx interface{}, port int) (int64, error)
}

func (sc *SaturnCollector) InitCollector(importsMap map[string]interface{}) error {
	// new instance function
	saturnNewInstanceFn, ok := importsMap[consts.SaturnNewInstanceIdentifier]
	if !ok {
		return errors.New("saturnNewInstance func not found")
	}
	sc.SaturnNewInstance, ok = saturnNewInstanceFn.(func(ctx interface{}, port int) error)

	saturnRemoveInstanceFn, ok := importsMap[consts.SaturnRemoveInstanceIdentifier]
	if !ok {
		return errors.New("saturnRemoveInstance func not found")
	}
	sc.SaturnRemoveInstance = saturnRemoveInstanceFn.(func(ctx interface{}, port int) error)
	// deleted files size function
	saturnDeletedFilesSizeFn, ok := importsMap[consts.SaturnDeletedFilesCollectorIdentifier]
	if !ok {
		return errors.New("saturnDirSize func not found")
	}
	sc.SaturnDeletedFilesSize = saturnDeletedFilesSizeFn.(func(ctx interface{}, port int) (int64, error))
	return nil
}
