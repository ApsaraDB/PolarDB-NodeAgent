/*-------------------------------------------------------------------------
 *
 * polarflex_collector.go
 *    polarflex environment collector
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
 *           plugins/polardb_pg_opensource/environment/polarflex_collector.go
 *-------------------------------------------------------------------------
 */
package environment

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"unicode"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/meta"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/system"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/polardb_pg_opensource/resource/fs"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/polardb_pg_opensource/resource/process"
)

const (
	DefaultLocalDiskCollectInterval = 15
)

type PolarFlexEnvironmentCollector struct {
	EnvironmentCollectorBase
	logger *log.PluginLogger

	localDiskPath            string
	localDiskCollectInterval int64
	count                    int64
	Port                     int

	Envs map[string]string
}

func NewPolarFlexEnvironmentCollector() *PolarFlexEnvironmentCollector {
	return &PolarFlexEnvironmentCollector{
		EnvironmentCollectorBase: NewEnvironmentCollectorBase()}
}

func (c *PolarFlexEnvironmentCollector) Init(m map[string]interface{},
	logger *log.PluginLogger) error {
	c.logger = logger
	c.Port = m[consts.PluginContextKeyPort].(int)
	c.Envs = m["env"].(map[string]string)
	c.EnvironmentCollectorBase.Collectors = map[string]EnvironmentCollectorInterface{
		"disk_collector":    fs.NewLocalDirCollector(),
		"process_collector": process.NewPgProcessResourceCollector(),
	}

	if _, ok := m["local_disk_collect_interval"]; ok {
		c.localDiskCollectInterval = int64(m["local_disk_collect_interval"].(float64))
	} else {
		c.localDiskCollectInterval = DefaultLocalDiskCollectInterval
	}
	c.count = 0

	if err := c.prepareDiskCollector(m); err != nil {
		c.logger.Warn("prepare disk collector failed", err)
		return err
	}

	if err := c.EnvironmentCollectorBase.Init(m, logger); err != nil {
		c.logger.Warn("init resource collector failed", err)
		return err
	}

	c.RegistCollecFunc(c.collectCpuMem)
	c.RegistCollecFunc(c.collectLocalVolumeCapacityWithDf)

	m["default_out"] = make(map[string]interface{})
	m["default_out"].(map[string]interface{})["enable_pfs"] = 0

	return nil
}

func (c *PolarFlexEnvironmentCollector) prepareDiskCollector(m map[string]interface{}) error {
	var newlogDir, baseDir, newWalDir string
	datadir := m["datadir"].(string)
	localdatadir := ""

	envs := m["env"].(map[string]string)

	if pvdatadir, ok := envs["/disk1"]; ok {
		c.localDiskPath = pvdatadir
		datadir = pvdatadir
	}

	if _, ok := envs["host_data_local_dir"]; ok {
		c.localDiskPath = envs["host_data_dir"]
	}

	if waldir, ok := envs["host_wal_dir"]; ok {
		newWalDir = fmt.Sprintf("%s/pg_wal", waldir)
	} else {
		newWalDir = fmt.Sprintf("%s/pg_wal", datadir)
	}

	if waldir, ok := envs["host_wal_full_path"]; ok {
		newWalDir = waldir
	}

	if logdir, ok := envs["host_log_full_path"]; ok {
		newlogDir = logdir
	} else {
		newlogDir = fmt.Sprintf("%s/%s", datadir, "log")
	}

	baseDir = fmt.Sprintf("%s/base", datadir)

	m["dirinfo"] = map[string]fs.DirInfo{
		"data":       {Path: localdatadir, Outkeys: []string{"local_data_dir_size"}},
		"new_logdir": {Path: newlogDir, Outkeys: []string{"local_pg_log_dir_size"}},
		"base_dir":   {Path: baseDir, Outkeys: []string{"local_base_dir_size", "polar_base_dir_size"}},
		"new_waldir": {Path: newWalDir, Outkeys: []string{"local_pg_wal_dir_size", "polar_wal_dir_size"}},
	}

	return nil
}

func (c *PolarFlexEnvironmentCollector) collectLocalVolumeCapacityWithDf(dfmap map[string]interface{}) error {
	var stdout, stderr bytes.Buffer

	if c.count%c.localDiskCollectInterval == 0 {
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
	}

	c.count++

	return nil
}

func (c *PolarFlexEnvironmentCollector) collectCpuMem(out map[string]interface{}) error {
	cpuCores, _ := system.GetCPUCount()
	memTotal, _ := c.getMemTotal()

	if out["procs_cpu_user_sum"] != nil && out["procs_cpu_sys_sum"] != nil {
		out["cpu_user"] = out["procs_cpu_user_sum"]
		out["cpu_sys"] = out["procs_cpu_sys_sum"]
		out["cpu_total"] =
			out["procs_cpu_user_sum"].(float64) + out["procs_cpu_sys_sum"].(float64)
	} else {
		out["cpu_user"] = float64(0)
		out["cpu_sys"] = float64(0)
		out["cpu_total"] = float64(0)
	}

	out["cpu_user_usage"] = out["cpu_user"].(float64) / float64(cpuCores)
	out["cpu_sys_usage"] = out["cpu_sys"].(float64) / float64(cpuCores)
	out["cpu_total_usage"] = out["cpu_total"].(float64) / float64(cpuCores)

	out["cpu_cores"] = uint64(cpuCores)
	// include shared memory
	if share, ok := meta.GetMetaService().GetFloat("shared_memory_size_mb",
		strconv.Itoa(c.Port)); ok {
		if _, ok := out["procs_mem_rss_sum"]; ok {
			out["mem_total_usage"] =
				(out["procs_mem_rss_sum"].(float64) +
					share) * 100 / float64(memTotal)
			out["mem_total_used"] = (out["procs_mem_rss_sum"].(float64) + share)
		}
	} else {
		if _, ok := out["procs_mem_rss_sum"]; ok {
			out["mem_total_usage"] =
				out["procs_mem_rss_sum"].(float64) * 100 / float64(memTotal)
			out["mem_total_used"] = out["procs_mem_rss_sum"]
		}
	}

	out["mem_total"] = memTotal

	// CPU and Memory formalize
	c.BuildOutDictSendToMultiBackend(out, "cpu_cores",
		"cpu_user_usage", "cpu_sys_usage", "cpu_total_usage",
		"mem_total_usage", "mem_total_used", "mem_total")

	return nil
}

func (c *PolarFlexEnvironmentCollector) getMemTotal() (uint64, error) {
	memfile, err := os.Open("/proc/meminfo")
	if err != nil {
		return uint64(0), err
	}
	defer memfile.Close()

	buf := make([]byte, 10*1024)

	num, err := memfile.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return uint64(0), err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.FieldsFunc(buf[:index], func(r rune) bool {
			return r == ':' || unicode.IsSpace(r)
		})
		buf = buf[index+1:]

		if len(fields) < 2 {
			continue
		}

		size, err := strconv.ParseUint(string(fields[1]), 10, 64)
		return size / 1024, err
	}

	return uint64(0), nil
}
