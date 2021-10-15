/*-------------------------------------------------------------------------
 *
 * pluto.go
 *    Pluto plugin
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
 *           plugins/pluto/pluto.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ApsaraDB/db-monitor/common/system"

	"github.com/ApsaraDB/db-monitor/common/log"
)

type Pluto struct {
	instances                []string
	fdPath                   string
	dirSizeCollector         *dirSizeCollector
	filteredDirSizeCollector *filteredDirSizeCollector
	mysqlDiskDetailCollector *MySQLDiskDetailCollecotr
	mysqlDiskDetails         []*MySQLDiskDetail
}

type dirSizeCollector struct {
	metricMap sync.Map // path as key
}

type filteredDirSizeCollector struct {
	paramMap  sync.Map // path as key, regex pattern as value
	metricMap sync.Map // path as key
}

type MySQLDiskDetail struct {
	port           int
	binlogSize     int64
	relayLogSize   int64
	redoLogSize    int64
	undoLogSize    int64
	slowLogSize    int64
	sysDataSize    int64
	userDataSize   int64
	generalLogSize int64
	otherFileSize  int64
	tempFileSize   int64
	hostDataDir    string
	hostLogDir     string
	homeDir        string
	dataDir        string
	tmpDir         string
	backupDir      string
}

type MySQLDiskDetailCollecotr struct {
	instanceMap sync.Map //port as key, MySQLDiskDetail as value
}

func PluginInit(ctx interface{}) (interface{}, error) {
	plutoCtx := &Pluto{
		dirSizeCollector:         &dirSizeCollector{},
		filteredDirSizeCollector: &filteredDirSizeCollector{},
		mysqlDiskDetailCollector: &MySQLDiskDetailCollecotr{},
		instances:                make([]string, 200),
		mysqlDiskDetails:         make([]*MySQLDiskDetail, 200),
	}
	return plutoCtx, nil
}

// PluginRun run pluto plugin, param not used yet
func PluginRun(ctx interface{}, param interface{}) error {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return fmt.Errorf("PluginRun: invalid ctx %v", ctx)
	}

	pluto.instances = pluto.instances[:0]

	// data size
	pluto.dirSizeCollector.metricMap.Range(func(k interface{}, v interface{}) bool {
		// collect dir size
		ins, ok := k.(string)
		if !ok {
			return true
		}
		pluto.instances = append(pluto.instances, ins)
		return true
	})

	for _, path := range pluto.instances {
		err := collectDirSize(pluto, path)
		if err != nil {
			log.Error("[pluto] collectDirSize failed",
				log.String("path", path),
				log.String("err", err.Error()))
			continue
		}
		//time.Sleep(100 * time.Millisecond)
	}

	// binlog size
	pluto.instances = pluto.instances[:0]
	pluto.filteredDirSizeCollector.metricMap.Range(func(k interface{}, v interface{}) bool {
		// collect dir size
		ins, ok := k.(string)
		if !ok {
			return true
		}

		pluto.instances = append(pluto.instances, ins)
		return true
	})

	for _, path := range pluto.instances {
		//time.Sleep(100 * time.Millisecond)
		regex, ok := pluto.filteredDirSizeCollector.paramMap.Load(path)
		if !ok {
			log.Error("[pluto] regex not found",
				log.String("path", path))
			continue
		}

		err := collectFilteredDirSize(pluto, path, regex.(string))
		if err != nil {
			log.Error("[pluto] collectFilteredDirSize failed",
				log.String("path", path),
				log.String("err", err.Error()))
			continue
		}
	}

	pluto.mysqlDiskDetails = pluto.mysqlDiskDetails[:0]
	pluto.mysqlDiskDetailCollector.instanceMap.Range(func(key, value interface{}) bool {
		detail, ok := value.(*MySQLDiskDetail)
		if !ok {
			return true
		}
		pluto.mysqlDiskDetails = append(pluto.mysqlDiskDetails, detail)
		return true
	})

	for _, detail := range pluto.mysqlDiskDetails {
		err := collectMySQLDiskDetail(pluto, detail)
		if err != nil {
			log.Error("[pluto] collectMySQLDiskDetail failed", log.Int("port", detail.port), log.String("err", err.Error()))
		}
		continue
	}

	return nil
}

// PluginExit : exit plugin and release resources here
func PluginExit(ctx interface{}) error {

	log.Info("[pluto] PluginExit: stop plugin pluto done")
	return nil
}

// GetDirSize : get the specified path dir size, for data
func GetDirSize(ctx interface{}, path string) (int64, error) {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return -1, errors.New("GetDirSize: invalid ctx")
	}

	v, ok := pluto.dirSizeCollector.metricMap.Load(path)
	if !ok || v == nil {
		return -1, nil
	}

	size, ok := v.(int64)
	if !ok || size == -1 {
		return -1, fmt.Errorf("GetDirSize: path:%s, value not ready: %v", path, v)
	}

	return size, nil
}

// GetFilteredDirSize : get the specified path dir size, for binlog
func GetFilteredDirSize(ctx interface{}, path string) (int64, error) {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return -1, errors.New("GetFilteredDirSize: invalid ctx")
	}

	v, ok := pluto.filteredDirSizeCollector.metricMap.Load(path)
	if !ok {
		return -1, nil
	}

	size, ok := v.(int64)
	if !ok || size == -1 {
		return -1, fmt.Errorf("GetFilteredDirSize: value not ready: %v", v)
	}

	return size, nil
}

func NewInstance(ctx interface{}, home, regex string, dirs ...string) error {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return errors.New("GetDeletedFilesSize: invalid ctx")
	}
	if home != "" {
		pluto.dirSizeCollector.metricMap.Store(home, int64(-1))
		pluto.filteredDirSizeCollector.metricMap.Store(home, int64(-1))
		pluto.filteredDirSizeCollector.paramMap.Store(home, regex)
	}

	for _, dir := range dirs {
		pluto.dirSizeCollector.metricMap.Store(dir, int64(-1))
	}
	return nil
}

func RemoveInstance(ctx interface{}, home string, dirs ...string) error {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return errors.New("GetDeletedFilesSize: invalid ctx")
	}

	for _, dir := range dirs {
		pluto.dirSizeCollector.metricMap.Delete(dir)
	}

	if home != "" {
		pluto.dirSizeCollector.metricMap.Delete(home)
		pluto.filteredDirSizeCollector.metricMap.Delete(home)
		pluto.filteredDirSizeCollector.paramMap.Delete(home)
	}
	return nil
}

func RegisterMySQLDiskDetailInstance(ctx interface{}, port int, hostDataDir, hostLogDir, homeDir, dataDir, tmpDir, backupDir string) error {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return errors.New("RegisterDirSize: invalid ctx")
	}
	detail := &MySQLDiskDetail{
		port:        port,
		hostDataDir: hostDataDir,
		hostLogDir:  hostLogDir,
		homeDir:     homeDir,
		dataDir:     dataDir,
		tmpDir:      tmpDir,
		backupDir:   backupDir,

		binlogSize:     -1,
		relayLogSize:   -1,
		redoLogSize:    -1,
		undoLogSize:    -1,
		slowLogSize:    -1,
		sysDataSize:    -1,
		userDataSize:   -1,
		generalLogSize: -1,
		otherFileSize:  -1,
		tempFileSize:   -1,
	}

	pluto.mysqlDiskDetailCollector.instanceMap.Store(port, detail)
	return nil
}

func UnRegisterMySQLDiskDetailInstance(ctx interface{}, port int) error {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return errors.New("UnRegisterDirSize: invalid ctx")
	}
	pluto.mysqlDiskDetailCollector.instanceMap.Delete(port)
	return nil
}

// GetMySQLDiskDetail : get mysql disk detail size
func GetMySQLDiskDetailSize(ctx interface{}, port int) (int64, int64, int64, int64, int64, int64, int64, int64, int64, int64, error) {
	pluto, ok := ctx.(*Pluto)
	if !ok {
		return -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, errors.New("GetMySQLDiskDetail invalid ctx")
	}

	insDetail, ok := pluto.mysqlDiskDetailCollector.instanceMap.Load(port)
	if !ok {
		return -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, fmt.Errorf("GetMySQLDiskDetail port:%d not found ", port)
	}

	detail, ok := insDetail.(*MySQLDiskDetail)
	if !ok {
		return -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, fmt.Errorf("GetMySQLDiskDetail detail not MySQLDiskDetail,port:%d ", port)
	}

	return detail.binlogSize, detail.relayLogSize, detail.redoLogSize, detail.undoLogSize, detail.slowLogSize, detail.sysDataSize, detail.userDataSize, detail.generalLogSize, detail.otherFileSize, detail.tempFileSize, nil
}

func collectDirSize(pluto *Pluto, path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	// 对于符号连接的文件，采集所指向的文件大小
	target := path
	if fi.Mode()&os.ModeSymlink != 0 {
		target, err = os.Readlink(path)
		if err != nil {
			return err
		}
	}
	size, _, err := system.DirSize(target)
	if err != nil {
		return err
	}
	pluto.dirSizeCollector.metricMap.Store(path, size)
	return nil
}

func collectFilteredDirSize(pluto *Pluto, path, regex string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	// 对于符号连接的文件，采集所指向的文件大小
	target := path
	if fi.Mode()&os.ModeSymlink != 0 {
		target, err = os.Readlink(path)
		if err != nil {
			return err
		}
	}
	size, _, err := system.FilteredDirSize(target, regex)
	if err != nil {
		return err
	}
	pluto.filteredDirSizeCollector.metricMap.Store(path, size)
	return nil
}

func collectMySQLDiskDetail(pluto *Pluto, detail *MySQLDiskDetail) error {
	var binlogSize, relayLogSize, redoLogSize, undoLogSize, slowLogSize, sysDataSize, userDataSize, generalLogSize, otherFileSize, tempFileSize int64
	files, err := ioutil.ReadDir(detail.homeDir)
	if err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", detail.homeDir))
	} else {
		for _, file := range files {
			if strings.Contains(file.Name(), "-bin.") { // All binlog
				binlogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "slave-relay") { // All relayLog
				relayLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ib_logfile") { // All RedoLog
				redoLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "slow_query") { // All slow query
				slowLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "master-error") {
				generalLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ibtmp") {
				tempFileSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "undo") || strings.HasPrefix(file.Name(), "ibdata") {
				undoLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ib_buffer_pool") {
				sysDataSize += file.Size()
			}
		}
	}

	files, err = ioutil.ReadDir(detail.dataDir)
	if err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", detail.dataDir))
	} else {
		for _, file := range files {
			if file.IsDir() {
				size, _, err := system.DirSize(filepath.Join(detail.dataDir, file.Name()))
				if err != nil {
					continue
				}
				switch file.Name() {
				case "mysql", "performance_schema", "sys", "__recycle_bin__":
					sysDataSize += size
				case "#innodb_temp":
					tempFileSize += size
				default:
					userDataSize += size
				}
			} else if strings.HasPrefix(file.Name(), "ALISQL_PERFORMANCE_AGENT") {
				generalLogSize += file.Size()
			} else {
				otherFileSize += file.Size()
			}
		}
	}

	// https://aone.alibaba-inc.com/v2/project/1003777/bug/29600770
	files, err = ioutil.ReadDir(filepath.Join(detail.dataDir, "mysql"))
	if err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", filepath.Join(detail.dataDir, "mysql")))
	} else {
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "slow_log") {
				slowLogSize += file.Size()
			}
		}
	}

	files, err = ioutil.ReadDir(detail.hostDataDir)
	if err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", detail.hostDataDir))
	} else {
		for _, file := range files {
			if file.Mode().IsRegular() {
				if strings.HasPrefix(file.Name(), "recover-") {
					generalLogSize += file.Size()
				} else {
					otherFileSize += file.Size()
				}
			}
		}
	}

	files, err = ioutil.ReadDir(filepath.Join(detail.hostLogDir, "mysql"))
	if err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", filepath.Join(detail.hostLogDir, "mysql")))
	} else {
		// k8s 架构下，MySQL mysql 目录下内部文件放置路径被拆开为两个目录
		for _, file := range files {
			if strings.Contains(file.Name(), "-bin.") { // All binlog
				binlogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "slave-relay") { // All relayLog
				relayLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ib_logfile") { // All RedoLog
				redoLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "slow_query") { // All slow query
				slowLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "master-error") {
				generalLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ibtmp") {
				tempFileSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "undo") || strings.HasPrefix(file.Name(), "ibdata") {
				undoLogSize += file.Size()
			} else if strings.HasPrefix(file.Name(), "ib_buffer_pool") {
				sysDataSize += file.Size()
			}
		}
	}
	tmpDir := filepath.Join(filepath.Join(detail.hostLogDir, "tmp"))
	if _, err := os.Stat(tmpDir); err != nil {
		log.Error("[pluto] collect mysql disk detail failed",
			log.String("path", filepath.Join(detail.hostLogDir, "tmp")))
	} else {
		// k8s 架构下，MySQL tmp 目录下内部文件放置路径被拆开为两个目录
		size, _, _ := system.DirSize(tmpDir)
		tempFileSize += size
	}

	if size, _, err := system.DirSize(detail.backupDir); err != nil {
		otherFileSize += size
	}

	if size, _, err := system.DirSize(detail.tmpDir); err != nil {
		tempFileSize += size
	}

	detail.binlogSize = binlogSize
	detail.relayLogSize = relayLogSize
	detail.redoLogSize = redoLogSize
	detail.undoLogSize = undoLogSize
	detail.slowLogSize = slowLogSize
	detail.sysDataSize = sysDataSize
	detail.userDataSize = userDataSize
	detail.generalLogSize = generalLogSize
	detail.otherFileSize = otherFileSize
	detail.tempFileSize = tempFileSize

	pluto.mysqlDiskDetailCollector.instanceMap.Store(detail.port, detail)
	return err
}
