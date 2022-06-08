/*-------------------------------------------------------------------------
 *
 * fs_collector.go
 *    collect local fs block and inode usage
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
 *           plugins/polardb_pg_opensource/resource/fs/fs_collector.go
 *-------------------------------------------------------------------------
 */
package fs

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
)

const (
	DefaultLocalDiskCollectInterval = 15
)

type FSCollector struct {
	logger                   *log.PluginLogger
	counter                  int64
	localDiskCollectInterval int64
	localDiskPath            string
}

func (c *FSCollector) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	envs := m["envs"].(map[string]string)

	c.logger = logger
	c.counter = 0

	if _, ok := m["local_disk_collect_interval"]; ok {
		c.localDiskCollectInterval = int64(m["local_disk_collect_interval"].(float64))
	} else {
		c.localDiskCollectInterval = DefaultLocalDiskCollectInterval
	}

	if pvdatadir, ok := envs["/disk1"]; ok {
		c.localDiskPath = pvdatadir
	}

	if _, ok := envs["host_data_local_dir"]; ok {
		c.localDiskPath = envs["host_data_dir"]
	}
	return nil
}

func (c *FSCollector) Collect(out map[string]interface{}) error {
	if c.counter%c.localDiskCollectInterval == 0 {
		if err := c.collectLocalVolumeCapacityWithDf(out); err != nil {
			c.logger.Warn("collect volume capacity with df failed", err)
		}
	}

	c.counter++
	return nil
}

func (c *FSCollector) collectLocalVolumeCapacityWithDf(dfmap map[string]interface{}) error {
	var stdout, stderr bytes.Buffer
	dfCmdStr := fmt.Sprintf(
		"df --output=itotal,iused,iavail,ipcent,size,used,avail,pcent,source,target -a %s | "+
			"awk '{if(NR>1)print}'", c.localDiskPath)

	cmd := exec.Command("/bin/sh", "-c", "timeout 10 "+dfCmdStr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		c.logger.Warn("exec df command failed", err,
			log.String("stderr", stderr.String()), log.String("dfcmd", dfCmdStr))
		return nil
	}

	c.logger.Debug("df output", log.String("out", stdout.String()))

	for _, line := range strings.Split(stdout.String(), "\n") {
		rlist := strings.Fields(line)
		if len(rlist) != 10 {
			c.logger.Info("df command split result not equal than 10",
				log.String("line", line), log.Int("length", len(rlist)))
			continue
		}

		dfmap["inodes_total"], _ = strconv.ParseUint(rlist[0], 10, 64)
		dfmap["inodes_used"], _ = strconv.ParseUint(rlist[1], 10, 64)
		dfmap["inodes_avail"], _ = strconv.ParseUint(rlist[2], 10, 64)
		dfmap["inodes_usage"], _ = strconv.ParseUint(strings.TrimRight(rlist[3], "%"), 10, 64)
		dfmap["fs_inodes_total"] = dfmap["inodes_total"]
		dfmap["fs_inodes_used"] = dfmap["inodes_used"]
		dfmap["fs_inodes_usage"] = dfmap["inodes_usage"]
		dfmap["size_total"], _ = strconv.ParseUint(rlist[4], 10, 64)
		dfmap["size_used"], _ = strconv.ParseUint(rlist[5], 10, 64)
		dfmap["size_avail"], _ = strconv.ParseUint(rlist[6], 10, 64)
		dfmap["size_usage"], _ = strconv.ParseUint(strings.TrimRight(rlist[7], "%"), 10, 64)
		dfmap["fs_blocks_total"] = dfmap["size_total"]
		dfmap["fs_blocks_used"] = dfmap["size_used"]
		dfmap["fs_blocks_usage"] = dfmap["size_usage"]
		dfmap["fs_size_total"] = float64(dfmap["size_total"].(uint64)) / 1024
		dfmap["fs_size_used"] = float64(dfmap["size_used"].(uint64)) / 1024
		dfmap["fs_size_usage"] = float64(dfmap["size_usage"].(uint64))

		break
	}

	return nil
}

func (c *FSCollector) Stop() error {
	return nil
}
