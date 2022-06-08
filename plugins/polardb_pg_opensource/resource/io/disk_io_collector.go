/*-------------------------------------------------------------------------
 *
 * disk_io_collector.go
 *    collect block device IO stat
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
 *           plugins/polardb_pg_opensource/resource/io/disk_io_collector.go
 *-------------------------------------------------------------------------
 */
package io

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/utils"
)

type DiskIOCollector struct {
	logger         *log.PluginLogger
	delta          *utils.DeltaCalculator
	cli            *utils.CommandExecutor
	pfsdDmDevName  string
	pfsdLunDevList []string
	pfsdBuf        bytes.Buffer
	InsName        string
}

func NewDiskIOCollector() *DiskIOCollector {
	return &DiskIOCollector{
		cli:   utils.NewCommandExecutor(),
		delta: utils.NewDeltaCalculator()}
}

func (c *DiskIOCollector) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	// /dev/mapper/pv-36e00084100ee7ec9de6e77c5000008e9 -> ../dm-71
	// mapper是逻辑卷，dm-71是物理卷，通过软连接找到对应关系
	var err error
	var res string
	var dmCmd string

	c.logger = logger
	c.InsName = m[consts.PluginContextKeyInsName].(string)

	envs := m["env"].(map[string]string)
	pvname := envs["apsara.metric.pv_name"]

	if err := c.delta.Init(); err != nil {
		c.logger.Error("init delta collector for disk io collector failed", err)
		return err
	}

	if err := c.cli.Init(); err != nil {
		c.logger.Error("command line executor init failed", err)
		return err
	}

	// 存在多重软连接，需要多次readlink
	for i := 0; i <= 5; i++ {
		dmCmd = fmt.Sprintf("timeout 10 basename `readlink /dev/mapper/%s`", pvname)
		if res, err = c.cli.ExecCommand(dmCmd); err != nil {
			c.logger.Warn("exec basename cmd failed.", err, log.String("command", dmCmd))
		}
		for _, line := range strings.Split(string(res), "\n") {
			if line != "" {
				pvname = line
			}
		}
		if strings.HasPrefix(pvname, "/dev/mapper/dm") {
			c.logger.Info("get the really dm device name", log.String("device_name", pvname), log.String("original pvname", envs["apsara.metric.pv_name"]))
			break
		}
	}

	c.pfsdDmDevName = pvname

	c.logger.Debug("get pfsdDmDevName", log.String("pfsdDmDevName", c.pfsdDmDevName))

	c.pfsdLunDevList = make([]string, 0)

	if c.pfsdDmDevName != "" {
		getLunCmd := fmt.Sprintf("timeout 10 ls /sys/block/%s/slaves/ ", c.pfsdDmDevName)
		var cmdResult string
		if cmdResult, err = c.cli.ExecCommand(getLunCmd); err != nil {
			c.logger.Warn("exec get lun list cmd failed.", err, log.String("command", dmCmd))
		}

		for _, devName := range strings.Split(string(cmdResult), "\n") {
			if devName != "" {
				c.pfsdLunDevList = append(c.pfsdLunDevList, devName)
			}
		}

		c.logger.Debug("get pfsdpfsdLunDevListDmDevName", log.String("getLunCmd", getLunCmd),
			log.String("cmdResult", cmdResult),
			log.String("pfsdLunDevList", fmt.Sprintf("%+v", c.pfsdLunDevList)))
	}

	return nil
}

func (c *DiskIOCollector) Collect(out map[string]interface{}) error {
	if len(c.pfsdLunDevList) == 0 {
		// c.logger.info("can't get multi path dev name")
		return nil
	}

	for _, LunDevName := range c.pfsdLunDevList {
		readIo, readBytes, readTime, writeIo, writeBytes, writeTime, timeInQueue, err := c.PfsdGetDiskIo(LunDevName)
		if err != nil {
			c.logger.Warn("get pfsd container io info failed.", err)
			continue
		}

		c.logger.Debug("getdiskio", log.String("LunDevName", LunDevName), log.Uint64("readIo", readIo), log.Uint64("readBytes", readBytes),
			log.Uint64("readTime", readTime), log.Uint64("writeIo", writeIo),
			log.Uint64("writeBytes", writeBytes), log.Uint64("writeTime", writeTime), log.Uint64("timeInQueue", timeInQueue))

		dimPrefix := c.InsName + "_" + LunDevName
		c.delta.CalRateData(dimPrefix+"_read_iops", out, float64(readIo))
		c.delta.CalRateData(dimPrefix+"_write_iops", out, float64(writeIo))
		c.delta.CalRateData(dimPrefix+"_read_throughput", out, float64(readBytes)/1024/1024)
		c.delta.CalRateData(dimPrefix+"_write_throughput", out, float64(writeBytes)/1024/1024)
		c.delta.CalRateData(dimPrefix+"_read_time", out, float64(readTime))   //Unit: ms
		c.delta.CalRateData(dimPrefix+"_write_time", out, float64(writeTime)) //Unit: ms

		out[dimPrefix+"_read_wait"] = float64(0.0)
		if _, ok := out[dimPrefix+"_read_iops"]; ok {
			if out[dimPrefix+"_read_iops"].(float64) != 0 {
				out[dimPrefix+"_read_wait"] = out[dimPrefix+"_read_time"].(float64) / out[dimPrefix+"_read_iops"].(float64)
			}
		}

		out[dimPrefix+"_write_wait"] = float64(0.0)
		if _, ok := out[dimPrefix+"_write_iops"]; ok {
			if out[dimPrefix+"_write_iops"].(float64) != 0 {
				out[dimPrefix+"_write_wait"] = out[dimPrefix+"_write_time"].(float64) / out[dimPrefix+"_write_iops"].(float64)
			}
		}

		// add mapkey
		if _, ok := out["multipath_iops_dev_name"]; ok {
			out["multipath_iops_dev_name"] = out["multipath_iops_dev_name"].(string) + "," + LunDevName
		} else {
			out["multipath_iops_dev_name"] = LunDevName
		}

		if _, ok := out[dimPrefix+"_read_iops"]; ok {
			read_iops_str := strconv.FormatInt(int64(out[dimPrefix+"_read_iops"].(float64)), 10)
			if _, ok := out["multipath_read_iops"]; ok {
				out["multipath_read_iops"] = out["multipath_read_iops"].(string) + "," + read_iops_str
			} else {
				out["multipath_read_iops"] = read_iops_str
			}
		}

		if _, ok := out[dimPrefix+"_write_iops"]; ok {
			write_iops_str := strconv.FormatInt(int64(out[dimPrefix+"_write_iops"].(float64)), 10)
			if _, ok := out["multipath_write_iops"]; ok {
				out["multipath_write_iops"] = out["multipath_write_iops"].(string) + "," + write_iops_str
			} else {
				out["multipath_write_iops"] = write_iops_str
			}
		}

		// add mapkey
		if _, ok := out["multipath_io_throughput_dev_name"]; ok {
			out["multipath_io_throughput_dev_name"] = out["multipath_io_throughput_dev_name"].(string) + "," + LunDevName
		} else {
			out["multipath_io_throughput_dev_name"] = LunDevName
		}

		if _, ok := out[dimPrefix+"_read_throughput"]; ok {
			read_throughput_str := strconv.FormatInt(int64(out[dimPrefix+"_read_throughput"].(float64)), 10)
			if _, ok := out["multipath_read_throughput"]; ok {
				out["multipath_read_throughput"] = out["multipath_read_throughput"].(string) + "," + read_throughput_str
			} else {
				out["multipath_read_throughput"] = read_throughput_str
			}
		}

		if _, ok := out[dimPrefix+"_write_throughput"]; ok {
			write_throughput_str := strconv.FormatInt(int64(out[dimPrefix+"_write_throughput"].(float64)), 10)
			if _, ok := out["multipath_write_throughput"]; ok {
				out["multipath_write_throughput"] = out["multipath_write_throughput"].(string) + "," + write_throughput_str
			} else {
				out["multipath_write_throughput"] = write_throughput_str
			}
		}

		// add mapkey
		if _, ok := out["multipath_latency_dev_name"]; ok {
			out["multipath_latency_dev_name"] = out["multipath_latency_dev_name"].(string) + "," + LunDevName
		} else {
			out["multipath_latency_dev_name"] = LunDevName
		}

		if _, ok := out[dimPrefix+"_read_wait"]; ok {
			read_wait_str := strconv.FormatInt(int64(out[dimPrefix+"_read_wait"].(float64)), 10)
			if _, ok := out["multipath_read_wait"]; ok {
				out["multipath_read_wait"] = out["multipath_read_wait"].(string) + "," + read_wait_str
			} else {
				out["multipath_read_wait"] = read_wait_str
			}
		}

		if _, ok := out[dimPrefix+"_write_wait"]; ok {
			write_wait_str := strconv.FormatInt(int64(out[dimPrefix+"_write_wait"].(float64)), 10)
			if _, ok := out["multipath_write_wait"]; ok {
				out["multipath_write_wait"] = out["multipath_write_wait"].(string) + "," + write_wait_str
			} else {
				out["multipath_write_wait"] = write_wait_str
			}
		}
	}

	return nil
}

func (c *DiskIOCollector) Stop() error {
	c.cli.Close()
	return nil
}

func (c *DiskIOCollector) PfsdGetDiskIo(devName string) (uint64, uint64, uint64, uint64, uint64, uint64, uint64, error) {
	var readIo, readBytes, readTime, writeIo, writeBytes, writeTime, timeInQueue uint64
	var err error
	sysFileName := "/sys/block/" + devName + "/stat"
	err = c.scanFile(sysFileName, func(fields []string) error {
		readIo, err = strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return err
		}
		readBytes, err = strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return err
		}

		readTime, err = strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			return err
		}

		writeIo, err = strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return err
		}
		writeBytes, err = strconv.ParseUint(fields[6], 10, 64)
		if err != nil {
			return err
		}

		writeTime, err = strconv.ParseUint(fields[7], 10, 64)
		if err != nil {
			return err
		}

		timeInQueue, err = strconv.ParseUint(fields[10], 10, 64)
		if err != nil {
			return err
		}

		readBytes = readBytes * 512
		writeBytes = writeBytes * 512

		return nil
	})
	return readIo, readBytes, readTime, writeIo, writeBytes, writeTime, timeInQueue, err
}

func (c *DiskIOCollector) scanFile(path string, fn func([]string) error) error {
	err := c.readFile(path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(c.pfsdBuf.Bytes()))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		err = fn(fields)
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (c *DiskIOCollector) readFile(filename string) error {
	c.pfsdBuf.Reset()
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var n int64 = bytes.MinRead
	if fi, err := f.Stat(); err == nil {
		if size := fi.Size() + bytes.MinRead; size > n {
			n = size
		}
	}
	return c.readAll(f, n)
}

func (c *DiskIOCollector) readAll(r io.Reader, capacity int64) error {
	var err error
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok {
			err = panicErr
		} else {
			err = fmt.Errorf("read file panic")
		}
	}()

	if int64(int(capacity)) == capacity {
		c.pfsdBuf.Grow(int(capacity))
	}
	_, err = c.pfsdBuf.ReadFrom(r)
	return err
}
