/*-------------------------------------------------------------------------
 *
 * local_dir_collector.go
 *    collect dir usage in local fs(ext4)
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
 *           plugins/polardb_pg_opensource/resource/fs/local_dir_collector.go
 *-------------------------------------------------------------------------
 */
package fs

import (
	"fmt"
	"strconv"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/utils"
)

type DirInfo struct {
	Path    string
	Outkeys []string
}

type LocalDirCollector struct {
	logger      *log.PluginLogger
	duCollector *utils.DiskUsageCollector
	dirInfoMap  map[string]DirInfo
	dirInfoList []string
}

func NewLocalDirCollector() *LocalDirCollector {
	return &LocalDirCollector{
		duCollector: utils.NewDiskUsageCollector(),
	}
}

func (c *LocalDirCollector) Init(
	m map[string]interface{},
	logger *log.PluginLogger) error {
	c.logger = logger

	dirinfo := m["dirinfo"].(map[string]DirInfo)
	c.logger.Info("dir info", log.String("dirinfo", fmt.Sprintf("%+v", dirinfo)))

	c.dirInfoList = make([]string, 0)
	c.dirInfoMap = make(map[string]DirInfo)
	for k, v := range dirinfo {
		c.dirInfoList = append(c.dirInfoList, v.Path)
		c.dirInfoMap[k] = v
	}

	m["dir_list"] = c.dirInfoList

	if err := c.duCollector.Init(m); err != nil {
		c.logger.Error("init disk usage collector failed", err)
		return err
	}

	return nil
}

func (c *LocalDirCollector) Collect(out map[string]interface{}) error {
	if err := c.collectLocalDirSize(out); err != nil {
		c.logger.Warn("collect local dir size failed", err)
	}

	return nil
}

func (c *LocalDirCollector) collectLocalDirSize(out map[string]interface{}) error {
	for _, dirinfo := range c.dirInfoMap {
		if size, err := c.duCollector.Collect(dirinfo.Path); err != nil {
			c.logger.Debug("get dir size failed",
				log.String("error", err.Error()), log.String("dir", dirinfo.Path))
		} else {
			for _, outkey := range dirinfo.Outkeys {
				c.addNonNegativeValue(out, outkey, size/1024/1024)
			}
		}
	}

	return nil
}

func (c *LocalDirCollector) addNonNegativeValue(out map[string]interface{}, metric string, value int64) {
	if value >= 0 {
		out[metric] = strconv.FormatInt(value, 10)
	} else {
		//only add non negative value
		out[metric] = ""
	}
}

func (c *LocalDirCollector) Stop() error {
	c.duCollector.Close()
	return nil
}
