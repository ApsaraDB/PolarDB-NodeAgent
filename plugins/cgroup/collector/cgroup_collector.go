// Copyright (c) 2018. Alibaba Cloud, All right reserved.
// This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
// You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
// the license agreement you entered into with Alibaba Cloud.

package collector

import (
	"bytes"
	"fmt"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/cgroup"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"os"
	"strconv"
)


type CGroupCollector struct {
	isOnEcs      bool
	Port         int
	gID          int32
	cgroupCpuMem *cgroup.CPUMem
	buf          bytes.Buffer
	env          map[string]string
	dataDev      string
	logDev       string
	rawPre       map[string]interface{} // raw value last time
	stop         chan bool
}

func New() *CGroupCollector {
	c := &CGroupCollector{
		rawPre: make(map[string]interface{}, 64),
	}
	c.cgroupCpuMem = cgroup.New(&c.buf)
	return c
}

func (c *CGroupCollector) Init(m map[string]interface{}) error {
	c.Port = m[consts.PluginContextKeyPort].(int)
	c.gID = m["g_id"].(int32)
	env, ok := m[consts.PluginContextKeyEnv].(map[string]string)
	if !ok {
		return fmt.Errorf("cgroup init env not found:%d", c.Port)
	}
	c.env = env
	if _, err := os.Stat(env[consts.CGroupCpuPath]); os.IsNotExist(err) {
		return err
	}
	c.cgroupCpuMem.InitCpu(env[consts.CGroupCpuPath])
	if _, err := os.Stat(env[consts.CGroupMemPath]); os.IsNotExist(err) {
		return err
	}
	c.cgroupCpuMem.InitMemory(env[consts.CGroupMemPath])
	return nil
}

func (c *CGroupCollector) Collect(out map[string]interface{}) error {
	if err := c.collectCGroupStat(out); err != nil {
		log.Error("[cgroup] collect cgroup stat failed", log.String("err", err.Error()), log.Int("port", c.Port))
	}
	return nil
}

func (c *CGroupCollector) collectCGroupStat(out map[string]interface{}) error {
	userCpu, sysCpu, totalCpu, err := c.cgroupCpuMem.GetCpuUsage()
	if err != nil {
		log.Warn("[cgroup] get cpu metrics failed.", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		c.calcDelta("cgroup_cpu_user", out, userCpu)
		c.calcDelta("cgroup_cpu_sys", out, sysCpu)
		c.calcDelta("cgroup_cpu_total", out, totalCpu)
	}

	cpudetail, err := c.cgroupCpuMem.GetCpuDetail()
	if err == nil {
		c.calcDelta("cgroup_cpu_iowait", out, cpudetail.IoWait)
		c.calcDelta("cgroup_cpu_idle", out, cpudetail.Idle)
		c.calcDelta("cgroup_cpu_irq", out, cpudetail.Irq)
		c.calcDelta("cgroup_cpu_softirq", out, cpudetail.SoftIrq)
		c.calcDelta("cgroup_cpu_nr_running", out, cpudetail.NrRunning)
		c.calcDelta("cgroup_cpu_nr_uninterrupible", out, cpudetail.NrUnInterrupible)
		out["cgroup_load1"] = strconv.FormatUint(cpudetail.Load1, 10)
	}

	cpuNrPeriods, cpuNrThrottled, throttledTime, err := c.cgroupCpuMem.GetCpuStat()
	if err != nil {
		log.Warn("[cgroup] get cpu limit failed", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		c.calcDelta("cgroup_cpu_nr_periods", out, cpuNrPeriods)
		c.calcDelta("cgroup_cpu_nr_throttled", out, cpuNrThrottled)
		c.calcDelta("cgroup_cpu_throttled_time", out, throttledTime)
	}

	cpuCores, err := c.cgroupCpuMem.GetCpuLimit()
	if err != nil {
		//log.Error("[cgroup] get cpu limit failed", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		out["cgroup_cpu_cores_limit"] = strconv.FormatUint(cpuCores, 10)
	}

	mem, err := c.cgroupCpuMem.GetMemoryUsage()
	if err != nil {
		log.Warn("[cgroup] get mem metrics failed.",
			log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	}

	out["cgroup_mem_used"] = strconv.FormatUint(mem, 10)

	memoryLimit, err := c.cgroupCpuMem.GetMemoryLimit()
	if err != nil {
		//log.Error("[cgroup] get memory limit failed", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		out["cgroup_mem_limit"] = strconv.FormatUint(memoryLimit, 10)
	}

	memStat, err := c.cgroupCpuMem.GetMemoryStat()
	if err != nil {
		log.Warn("[cgroup] get cgroup memory failed", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		out["cgroup_mem_rss"] = strconv.FormatUint(memStat.Rss, 10)
		out["cgroup_mem_cache"] = strconv.FormatUint(memStat.Cache, 10)
		out["cgroup_mem_inactiverss"] = strconv.FormatUint(memStat.InactiveAnon, 10)
		out["cgroup_mem_active_rss"] = strconv.FormatUint(memStat.ActiveAnon, 10)
		out["cgroup_mem_inactivecache"] = strconv.FormatUint(memStat.InActiveFile, 10)
		out["cgroup_mem_activecache"] = strconv.FormatUint(memStat.ActiveFile, 10)
		out["cgroup_mem_mapped_file"] = strconv.FormatUint(memStat.MappedFile, 10)
		out["cgroup_mem_dirty"] = strconv.FormatUint(memStat.Dirty, 10)
		out["cgroup_mem_writeback"] = strconv.FormatUint(memStat.WriteBack, 10)
		out["cgroup_mem_rss_huge"] = strconv.FormatUint(memStat.RssHuge, 10)
	}
	max, err := c.cgroupCpuMem.GetMaxUsageInBytes()
	if err != nil {
		log.Error("[cgroup] GetMaxUsageInBytes failed", log.String("err", err.Error()), log.Int("port", c.Port), log.Int32("g_id", c.gID))
	} else {
		out["cgroup_mem_max_usage_in_bytes"] = strconv.FormatUint(max, 10)
	}

	return nil
}


// stop this collector, release resources
func (c *CGroupCollector) Stop() error {
	c.stop <- true
	return nil
}

func (c *CGroupCollector) calcDelta(name string, out map[string]interface{}, value uint64) {
	out[name] = strconv.FormatUint(value, 10)
	c.buf.Reset()
	c.buf.WriteString(name)
	c.buf.WriteString(consts.MetricNameSuffixDelta)
	pre := c.rawPre[name]

	if pre != nil { // 容错, 避免delta突增
		if value < pre.(uint64) {
			out[c.buf.String()] = strconv.FormatUint(uint64(0), 10)
		} else {
			out[c.buf.String()] = strconv.FormatUint(value-pre.(uint64), 10)
		}
	}

	// update previous data
	c.rawPre[name] = value
}
