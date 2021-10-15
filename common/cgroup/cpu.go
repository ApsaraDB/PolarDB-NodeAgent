/*-------------------------------------------------------------------------
 *
 * cpu.go
 *    Read cgroup cpu/memory metrics
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
 *           common/cgroup/cpu.go
 *-------------------------------------------------------------------------
 */
package cgroup

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/tklauser/go-sysconf"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

/*
cpuacct.stat file lists a few statistics which further divide the
cPU time obtained by the cgroup into user and system times. Currently
the following statistics are supported:

user: Time spent by tasks of the cgroup in user mode.
system: Time spent by tasks of the cgroup in kernel mode.

user and system are in USER_HZ unit.


/sys/fs/cgroup/cpuacct.usage gives the CPU time (in nanoseconds) obtained
by this group which is essentially the CPU time obtained by all the tasks
in the system.
*/

const (
	cpuAcctUsage        = "cpuacct.usage"
	cpuAcctStat         = "cpuacct.stat"
	cpuStat             = "cpu.stat"
	cpuAcctUsagePerfCpu = "cpuacct.usage_percpu"
	cpuAcctProcStat     = "cpuacct.proc_stat"

	cpuCfsQuotaUs  = "cpu.cfs_quota_us"
	cpuCfsPeriodUs = "cpu.cfs_period_us"
	cpuSetCpus     = "cpuset.cpus"

	memoryUsageInBytes = "memory.usage_in_bytes"
	memoryStat         = "memory.stat"
	memoryLimitInBytes = "memory.limit_in_bytes"

	hugeTlb2MUsageInBytes = "hugetlb.2MB.usage_in_bytes"
	hugeTlb1GUsageInbytes = "hugetlb.1GB.usage_in_bytes"
)

type CpuDetail struct {
	Usr              uint64
	Nice             uint64
	Sys              uint64
	IoWait           uint64
	Idle             uint64
	SoftIrq          uint64
	Irq              uint64
	NrRunning        uint64
	NrUnInterrupible uint64
	Load1            uint64
}

type MemoryStat struct {
	Cache              uint64
	Rss                uint64
	RssHuge            uint64
	MappedFile         uint64
	Pgpgin             uint64
	Pgpgout            uint64
	Swap               uint64
	Dirty              uint64
	WriteBack          uint64
	WorkingSetReFault  uint64
	WorkingSetActivate uint64
	WorkingSetRestore  uint64
	InactiveAnon       uint64
	ActiveAnon         uint64
	InActiveFile       uint64
	ActiveFile         uint64
	UnEvictable        uint64
}

type CPUMem struct {
	cpuCount                int64
	port                    uint64
	buf                     *bytes.Buffer
	perCpu                  []uint64
	detail                  CpuDetail
	sys                     uint64
	usr                     uint64
	total                   uint64
	nrPeriods               uint64
	nrThrottled             uint64
	throttledTime           uint64
	memoryStat              MemoryStat
	cpuAcctUsagePath        string
	cpuAcctStatPath         string
	cpuStatPath             string
	cpuAcctUsagePerCpuPath  string
	cpuAcctProcStatPath     string
	cpuCfsQuotaUsPath       string
	cpuCfsPeriodUsPath      string
	cpuSetCpusPath          string
	memoryUsageInBytesPath  string
	memoryStatPath          string
	memoryLimitInBytesPath  string
	hugeTlbUsageInBytesPath string
	tick                    uint64
}

func New(buf *bytes.Buffer) *CPUMem {
	return &CPUMem{buf: buf}
}

func (cm *CPUMem) InitHugePageMemory(mmpath, mode string) error {
	// hugetlb/kubepods/burstable/pod%s/%s/hugetlb.2MB.usage_in_bytes
	// hugetlb/kubepods/burstable/pod%s/%s/hugetlb.1GB.usage_in_bytes
	if mode == "2M" {
		cm.hugeTlbUsageInBytesPath = filepath.Join(mmpath, hugeTlb2MUsageInBytes)
	} else if mode == "1G" {
		cm.hugeTlbUsageInBytesPath = filepath.Join(mmpath, hugeTlb1GUsageInbytes)
	} else {
		return fmt.Errorf("invalid hugepage size")
	}
	return nil
}

func (cm *CPUMem) InitMemory(path string) error {
	cm.memoryUsageInBytesPath = filepath.Join(path, memoryUsageInBytes)
	cm.memoryStatPath = filepath.Join(path, memoryStat)
	cm.memoryLimitInBytesPath = filepath.Join(path, memoryLimitInBytes)
	return nil
}

func (cm *CPUMem) InitCpu(path string) error {
	clkTck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		return err
	}
	cm.tick = uint64(clkTck)

	cm.cpuAcctUsagePath = filepath.Join(path, cpuAcctUsage)
	cm.cpuAcctStatPath = filepath.Join(path, cpuAcctStat)
	cm.cpuStatPath = filepath.Join(path, cpuStat)
	cm.cpuAcctUsagePerCpuPath = filepath.Join(path, cpuAcctUsagePerfCpu)
	cm.cpuAcctProcStatPath = filepath.Join(path, cpuAcctProcStat)
	cm.cpuCfsQuotaUsPath = filepath.Join(path, cpuCfsQuotaUs)
	cm.cpuCfsPeriodUsPath = filepath.Join(path, cpuCfsPeriodUs)
	cm.cpuSetCpusPath = filepath.Join(path, cpuSetCpus)
	return nil
}

func (cm *CPUMem) GetHugePageMemory() (uint64, error) {
	usage, err := cm.getSingleValue(cm.hugeTlbUsageInBytesPath)
	return uint64(usage), err
}

func (cm *CPUMem) GetMemoryStat() (*MemoryStat, error) {
	err := cm.scanFile(cm.memoryStatPath, func(fields []string) error {
		var err error
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return err
		}

		switch fields[0] {
		case "cache":
			cm.memoryStat.Cache = value
		case "rss":
			cm.memoryStat.Rss = value
		case "rss_huge":
			cm.memoryStat.RssHuge = value
		case "mapped_file":
			cm.memoryStat.MappedFile = value
		case "swap":
			cm.memoryStat.Swap = value
		case "pgpgin":
			cm.memoryStat.Pgpgin = value
		case "pgpgout":
			cm.memoryStat.Pgpgout = value
		case "dirty":
			cm.memoryStat.Dirty = value
		case "writeback":
			cm.memoryStat.WriteBack = value
		case "workingset_refault":
			cm.memoryStat.WorkingSetReFault = value
		case "workingset_activate":
			cm.memoryStat.WorkingSetActivate = value
		case "workingset_restore":
			cm.memoryStat.WorkingSetRestore = value
		case "inactive_anon":
			cm.memoryStat.InactiveAnon = value
		case "active_anon":
			cm.memoryStat.ActiveAnon = value
		case "inactive_file":
			cm.memoryStat.InActiveFile = value
		case "active_file":
			cm.memoryStat.ActiveFile = value
		case "unevictable":
			cm.memoryStat.UnEvictable = value
		}
		return err
	})

	if err != nil {
		return nil, err
	}
	return &cm.memoryStat, err
}

func (cm *CPUMem) GetMemoryUsage() (uint64, error) {
	usage, err := cm.getSingleValue(cm.memoryUsageInBytesPath)
	return uint64(usage), err
}

func (cm *CPUMem) GetMemoryLimit() (uint64, error) {
	limit, err := cm.getSingleValue(cm.memoryLimitInBytesPath)
	return uint64(limit), err
}
func (cm *CPUMem) GetCpuStat() (uint64, uint64, uint64, error) {

	err := cm.scanFile(cm.cpuStatPath, func(fields []string) error {
		var err error
		if fields[0] == "nr_periods" {
			cm.nrPeriods, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
		if fields[0] == "nr_throttled" {
			cm.nrThrottled, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
		if fields[0] == "throttled_time" {
			cm.throttledTime, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}

	return cm.nrPeriods, cm.nrThrottled, cm.throttledTime, nil
}

/** 获取CGroup CPU 规格
对于CPUshare 场景，取cfs_quota_us/cfs_peroid_us
对于CPUSet 场景，取cpuset
由于线上配置问题，可能存在同时开启cpuset 和cpushare ，此时取两者的最小值。
*/
func (cm *CPUMem) GetCpuLimit() (uint64, error) {
	isCpuShare, isCpuSet := false, false
	quotaUs, err := cm.getSingleValue(cm.cpuCfsQuotaUsPath)
	if err != nil {
		return 0.0, err
	}

	cpuSetCores, err := cm.getCpuSetCores()
	if err != nil {
		isCpuShare = true
		isCpuSet = false
	}

	if quotaUs == -1 {
		isCpuSet = true
		isCpuShare = false
		return uint64(cpuSetCores), nil
	}

	periodUs, err := cm.getSingleValue(cm.cpuCfsPeriodUsPath)
	if err != nil {
		return 0, err
	}

	var cpuShareCores int64 = 0
	if periodUs > 0 {
		cpuShareCores = quotaUs / periodUs
	}

	if !isCpuSet {
		return uint64(cpuShareCores), nil
	}

	if isCpuSet && isCpuShare {
		if cpuShareCores < cpuSetCores {
			return uint64(cpuShareCores), nil
		} else {
			return uint64(cpuSetCores), nil
		}
	}
	return uint64(cpuSetCores), nil
}

func (cm *CPUMem) getSingleValue(path string) (int64, error) {
	err := cm.readFile(path)
	if err != nil {
		return 0, err
	}
	cm.buf.Truncate(cm.buf.Len() - 1)
	value, err := strconv.ParseInt(cm.buf.String(), 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (cm *CPUMem) getCpuSetCores() (int64, error) {
	err := cm.readFile(cm.cpuSetCpusPath)
	if err != nil {
		return 0, err
	}
	cm.buf.Truncate(cm.buf.Len() - 1)
	fields := bytes.FieldsFunc(cm.buf.Bytes(), func(r rune) bool {
		return r == ','
	})
	var cores int64 = 0
	for _, field := range fields {
		if bytes.ContainsRune(field, '-') {
			coreSet := bytes.FieldsFunc(field, func(r rune) bool {
				return r == '-'
			})
			if len(coreSet) == 2 {
				start, err := strconv.ParseInt(string(coreSet[0]), 10, 64)
				if err != nil {
					return 0, err
				}
				end, err := strconv.ParseInt(string(coreSet[1]), 10, 64)
				if err != nil {
					return 0, err
				}
				cores += end - start + 1
			} else {
				return 0, fmt.Errorf("wrong format for cpuset,path:%s", cm.cpuSetCpusPath)
			}
		} else {
			cores++
		}
	}
	return cores, nil
}

func (cm *CPUMem) GetPerCpuUsage() ([]uint64, error) {
	err := cm.readFile(cm.cpuAcctUsagePerCpuPath)
	if err != nil {
		return nil, err
	}
	cm.buf.Truncate(cm.buf.Len() - 1)
	toks := strings.Split(cm.buf.String(), " ")
	if int64(len(toks)) != cm.cpuCount {
		return nil, fmt.Errorf("invalid cpu count")
	}
	for i, tok := range toks {
		cm.perCpu[i], err = strconv.ParseUint(tok, 10, 64)
		if err != nil {
			return nil, nil
		}
	}
	return cm.perCpu, nil
}

func (cm *CPUMem) GetCpuDetail() (*CpuDetail, error) {
	err := cm.scanFile(cm.cpuAcctProcStatPath, func(fields []string) error {
		var err error
		switch fields[0] {
		case "user":
			cm.detail.Usr, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "nice":
			cm.detail.Nice, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "system":
			cm.detail.Sys, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "iowait":
			cm.detail.IoWait, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "idle":
			cm.detail.Idle, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "irq":
			cm.detail.Irq, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "softirq":
			cm.detail.SoftIrq, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "nr_running":
			cm.detail.NrRunning, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "nr_uninterrupible":
			cm.detail.NrUnInterrupible, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		case "load":
			if fields[2] == "average(1min)" {
				cm.detail.Load1, err = strconv.ParseUint(fields[3], 10, 64)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &cm.detail, nil
}

func (cm *CPUMem) GetCpuUsage() (uint64, uint64, uint64, error) {
	err := cm.readFile(cm.cpuAcctUsagePath)
	if err != nil {
		return 0, 0, 0, err
	}

	// remove \n from the file
	cm.buf.Truncate(cm.buf.Len() - 1)
	cm.total, err = strconv.ParseUint(cm.buf.String(), 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	err = cm.scanFile(cm.cpuAcctStatPath, func(fields []string) error {
		if fields[0] == "user" {
			cm.usr, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}

		if fields[0] == "system" {
			cm.sys, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}

	return (cm.usr * uint64(time.Second.Nanoseconds())) / cm.tick, (cm.sys * uint64(time.Second.Nanoseconds())) / cm.tick, cm.total, nil
}

func (cm *CPUMem) readFile(filename string) error {
	cm.buf.Reset()
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return cm.readAll(f)
}

func (cm *CPUMem) readAll(r io.Reader) error {
	var err error
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok {
			err = panicErr
		} else {
			err = fmt.Errorf("read file panic: %d", cm.port)
		}
	}()

	_, err = cm.buf.ReadFrom(r)
	return err
}

func (cm *CPUMem) scanFile(path string, fn func([]string) error) error {
	err := cm.readFile(path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(cm.buf.Bytes()))
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
