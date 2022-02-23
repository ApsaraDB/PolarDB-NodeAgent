/*-------------------------------------------------------------------------
 *
 * sar_collector.go
 *    Sar collector
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
 *           plugins/sar/collector/sar_collector.go
 *-------------------------------------------------------------------------
 */

package collector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/system"
)

const (
	nvmeDeviceNum                 = 32
	nvmeLatencyDistributionNum    = 16
	nvmeBlockSizeDistributionNum  = 12
	nvmeAddrOffsetDistributionNum = 11
	nvmeKeySeparator              = '_'
)

var cpuNum int64
var bootTime uint64
var Hz uint64
var gMonitors map[string]bool
var gTopList int

// SarCollector struct
type SarCollector struct {
	isOnEcs           bool
	enableProcessTop  bool
	mem               *memStat
	cpu               *cpuStat
	load              *loadAvg
	disk              *diskStat
	fs                *fSStat
	net               *netStat
	vm                *vmStat
	proc              *procStat
	buf               []byte
	buf2              bytes.Buffer
	monitors          map[string]bool
}

func fastJoin(buf *bytes.Buffer, toks []string, sep byte) {
	buf.Reset()
	if len(toks) == 0 {
		return
	}
	if len(toks) == 1 {
		buf.WriteString(toks[0])
		return
	}

	buf.WriteString(toks[0])
	for _, tok := range toks[1:] {
		buf.WriteByte(sep)
		buf.WriteString(tok)
	}
}

func fastJoinFunc(buf *bytes.Buffer, procs []*procInfo, sep byte, cb func(*procInfo) string) {
	buf.Reset()
	var count int
	for _, proc := range procs {
		if count == 0 {
			buf.WriteString(cb(proc))
		} else {
			buf.WriteByte(sep)
			buf.WriteString(cb(proc))
		}
		count++
	}
}

// New create new SarCollector
func New() *SarCollector {
	sc := &SarCollector{
		mem:  memStatNew(),
		cpu:  cpuStatNew(),
		load: loadAvgNew(),
		disk: diskStatNew(),
		fs:   fSStatNew(),
		net:  netStatNew(),
		vm:   vmStatNew(),
		proc: procStatNew(),
		buf:  make([]byte, 131072), // 131072=128*1024
	}
	return sc
}

// Init sar
func (sc *SarCollector) Init(m map[string]interface{}) error {
	sc.isOnEcs = m["is_on_ecs"].(bool)
	if _, ok := m["enable_process_top"]; ok {
		sc.enableProcessTop = m["enable_process_top"].(bool)
	} else {
		sc.enableProcessTop = false
	}

	blockDeviceTypeMap := make(map[string]bool)
	if _, ok := m["block_device_type"]; ok {
		for _, dtype := range m["block_device_type"].([]interface{}) {
			blockDeviceTypeMap[dtype.(string)] = true
		}
	} else {
		blockDeviceTypeMap["all"] = true
	}
	sc.disk.blockDeviceTypeMap = blockDeviceTypeMap
	log.Info("[sar_collector] block device type map", log.String("map", fmt.Sprintf("%+v", blockDeviceTypeMap)))

	gMonitors = make(map[string]bool, 64)
	monitors, ok := m["monitors"].([]interface{})
	if ok {
		for _, monitor := range monitors {
			if mon, ok := monitor.(string); ok {
				gMonitors[mon] = true
			}
		}
	}
	gTopList = 15
	mc, ok := m["monitor_count"]
	if ok {
		if count, ok := mc.(float64); ok {
			gTopList = int(count)
		}
	}

	bootTime = getBootTime()
	clk, err := system.GetClkTck()
	if err != nil {
		Hz = 100
	} else {
		Hz = uint64(clk)
	}

	return nil
}

// Collect collect metric
func (sc *SarCollector) Collect(out map[string]interface{}) error {
	err := sc.mem.collect(out, sc.buf)
	if err != nil {
		log.Error("[sar_collector] mem collect failed", log.String("err", err.Error()))
	}

	err = sc.cpu.collect(out, sc.buf, &sc.buf2)
	if err != nil {
		log.Error("[sar_collector] cpu collect failed", log.String("err", err.Error()))
	}

	err = sc.load.collect(out, sc.buf)
	if err != nil {
		log.Error("[sar_collector] load collect failed", log.String("err", err.Error()))
	}

	err = sc.disk.collect(out, sc.buf, &sc.buf2)
	if err != nil {
		log.Error("[sar_collector] disk collect failed", log.String("err", err.Error()))

	}

	err = sc.fs.collect(out, sc.buf, &sc.buf2)
	if err != nil {
		log.Error("[sar_collector] fs collect failed", log.String("err", err.Error()))
	}

	if sc.enableProcessTop {
		err = sc.proc.collect(out, sc.buf, &sc.buf2)
		if err != nil {
			log.Error("[sar_collector] proc/pid collect failed", log.String("err", err.Error()))
		}
	}

	err = sc.net.collect(out, sc.buf, &sc.buf2)
	if err != nil {
		log.Error("[sar_collector] net collect failed", log.String("err", err.Error()))
	}

	err = sc.vm.collect(out, sc.buf)
	if err != nil {
		log.Error("[sar_collector] vm collect failed", log.String("err", err.Error()))
	}

	return nil
}

// Stop sar
func (sc *SarCollector) Stop() error {
	sc.mem.stop()
	sc.cpu.stop()
	sc.load.stop()
	sc.disk.stop()
	sc.fs.stop()
	sc.net.stop()
	sc.vm.stop()
	sc.proc.stop()

	return nil
}

type procInfo struct {
	isUsed    bool
	isNew     bool
	isDeleted bool

	cpu system.ProcessStat
	mem system.MemoryStat
	io  system.IOStat

	cpuDelta system.ProcessStat
	memDelta system.MemoryStat
	ioDelta  system.IOStat

	fpCPU *os.File
	fpIO  *os.File
	fpMem *os.File
	fpCmd *os.File
}

//var (
//	mysqlCmdlineRegexp       = regexp.MustCompile(`.*--port=(\d+)`)
//	redisCmdlineRegexp       = regexp.MustCompile(`.*:(\d+)`)
//	postgresCmdlineRegexp    = regexp.MustCompile(`postgres: maoyugou.*\((\d+)\).*`)
//	mongoCmdlineRegexp       = regexp.MustCompile(`.*/mongo/mongo(\d+).*`)
//	erlangProxyCmdlineRegexp = regexp.MustCompile(`.*-progname([a-z]+)-.*`)
//	maxscaleCmdlineRegexp    = regexp.MustCompile(`.*_(\d+)_(\d+).*`)
//)

func (p *procInfo) shortenCmdline() string {
	if strings.IndexAny(p.cpu.CmdLine, ",") > 0 {
		p.cpu.CmdLine = strings.ReplaceAll(p.cpu.CmdLine, ",", " ")
	}
	if p.cpu.BinName == "mysqld" || strings.HasPrefix(p.cpu.BinName, "mysqld_jemalloc") {
		index := strings.Index(p.cpu.CmdLine, "--port=")
		end := index + len("--port=")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return "mysqld" + p.cpu.CmdLine[end:end+4]
		}
	} else if p.cpu.BinName == "redis-server" {
		//TODO fix redis-server port
		index := strings.Index(p.cpu.CmdLine, "*:")
		end := index + len("*:")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return "redis-server" + p.cpu.CmdLine[end:end+4]
		}
	} else if p.cpu.BinName == "redis_proxy" {
		index := strings.Index(p.cpu.CmdLine, "redis-proxy/redis-proxy")
		end := index + len("redis-proxy/redis-proxy")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return "redis-server" + p.cpu.CmdLine[end:end+4]
		}
	} else if p.cpu.BinName == "postgres" {
	} else if strings.HasPrefix(p.cpu.BinName, "mongo") {
		index := strings.Index(p.cpu.CmdLine, "mongo/mongo")
		end := index + len("mongo/mongo")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return p.cpu.BinName + p.cpu.CmdLine[end:end+4]
		}
	} else if p.cpu.BinName == "beam.smp" {
		index := strings.Index(p.cpu.CmdLine, "haproxy-paladin/haproxy-paladin")
		end := index + len("haproxy-paladin/haproxy-paladin")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return p.cpu.BinName + p.cpu.CmdLine[end:end+4]
		}
	} else if p.cpu.BinName == "maxscale" {
		index := strings.Index(p.cpu.CmdLine, "maxscale_")
		end := index + len("maxscale_")
		if index > 0 && end < len(p.cpu.CmdLine) {
			return p.cpu.BinName + p.cpu.CmdLine[end:end+17]
		}
	} else if strings.HasPrefix(p.cpu.BinName, "python") {
		fields := strings.Fields(p.cpu.CmdLine)
		if len(fields) > 1 && strings.Contains(fields[0], "python") {
			return filepath.Base(fields[1])
		}
		return strings.Replace(p.cpu.CmdLine, "/usr/local/rds/", "", 1)
	} else if len(p.cpu.CmdLine) > 31 {
		return p.cpu.CmdLine[:32]
	}
	return p.cpu.CmdLine
}

type procStat struct {
	procFile       *os.File
	procs          map[uint64]*procInfo
	topCPU         []*procInfo
	topMem         []*procInfo
	topIO          []*procInfo
	topWchar       []*procInfo
	topMinFault    []*procInfo
	topWriteIO     []*procInfo
	topRealWriteIO []*procInfo
	topReadIO      []*procInfo
	whitelist      []*procInfo
	dProc          []*procInfo
	targetProc     map[uint64]*procInfo
	mons           []*procInfo
	buf            bytes.Buffer
	ZombieCount    int
}

func procStatNew() *procStat {
	proc := new(procStat)
	proc.procs = make(map[uint64]*procInfo, 2048)
	proc.topCPU = make([]*procInfo, gTopList)
	proc.topIO = make([]*procInfo, gTopList)
	proc.topMem = make([]*procInfo, gTopList)
	proc.topWriteIO = make([]*procInfo, gTopList)
	proc.topRealWriteIO = make([]*procInfo, gTopList)
	proc.topReadIO = make([]*procInfo, gTopList)
	proc.dProc = make([]*procInfo, gTopList)
	proc.topWchar = make([]*procInfo, gTopList)
	proc.topMinFault = make([]*procInfo, gTopList)
	proc.whitelist = make([]*procInfo, gTopList)
	proc.targetProc = make(map[uint64]*procInfo, 10*gTopList)
	proc.mons = make([]*procInfo, 10*gTopList)
	return proc
}

func procsToString(list []*procInfo, buf *bytes.Buffer) string {
	buf.Reset()
	var tmp []string
	for _, iter := range list {
		buf.Reset()
		buf.WriteString(iter.cpu.BinName)
		buf.WriteString("|||")
		buf.WriteString(iter.shortenCmdline())
		buf.WriteString("|||")
		buf.WriteString(strconv.FormatUint(iter.cpu.Pid, 10))
		tmp = append(tmp, buf.String())
	}
	if len(list) > 0 {
		fastJoin(buf, tmp, ',')
	}
	return buf.String()
}

func (p *procStat) collect(out map[string]interface{}, ba []byte, buf *bytes.Buffer) error {
	var err error
	if p.procFile == nil {
		if p.procFile, err = os.Open("/proc"); err != nil {
			return err
		}
	}
	err = p.getMonitorProcInfo(ba, buf)
	if err != nil {
		return err
	}

	for pid := range p.targetProc {
		delete(p.targetProc, pid)
	}
	for _, proc := range p.topCPU {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topIO {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topMem {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topWriteIO {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topRealWriteIO {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topReadIO {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.dProc {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topWchar {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.topMinFault {
		p.targetProc[proc.cpu.Pid] = proc
	}
	for _, proc := range p.whitelist {
		p.targetProc[proc.cpu.Pid] = proc
	}

	p.mons = p.mons[:0]
	for _, proc := range p.targetProc {
		if proc.cpu.Tgid == 0 && !strings.Contains(proc.cpu.Comm, "jbd") && !strings.Contains(proc.cpu.Comm, "ksoftirqd") && !strings.Contains(proc.cpu.Comm, "kswap") {
			continue
		}
		p.mons = append(p.mons, proc)
	}

	out["sar_process_list"] = procsToString(p.mons, buf)

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.Wchar, 10)
	})
	out["sar_process_wchar"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.Rchar, 10)
	})
	out["sar_process_rchar"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.ReadBytes, 10)

	})
	out["sar_process_read_bytes"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.WriteBytes, 10)
	})
	out["sar_process_write_bytes"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.Syscw, 10)
	})
	out["sar_process_syscw"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.Syscr, 10)
	})
	out["sar_process_syscr"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.ioDelta.CancelledWriteBytes, 10)
	})
	out["sar_process_cancelled_write_bytes"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpuDelta.Utime, 10)
	})
	out["sar_process_utime"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpuDelta.Stime, 10)
	})
	out["sar_process_stime"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpu.Pid, 10)
	})
	out["sar_process_pid"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpu.Starttime/Hz+bootTime, 10)
	})
	out["sar_process_start_time"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		if proc.cpu.Status == 'D' {
			return string('1')
		}
		return string('0')
	})
	out["sar_process_D_status"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpuDelta.MinFault, 10)

	})
	out["sar_process_minflt"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.cpuDelta.MajFault, 10)

	})
	out["sar_process_majflt"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.mem.Resident, 10)
	})
	out["sar_process_rss"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.mem.Shared, 10)
	})
	out["sar_process_shared"] = buf.String()

	fastJoinFunc(buf, p.mons, ',', func(proc *procInfo) string {
		return strconv.FormatUint(proc.mem.Size, 10)
	})
	out["sar_process_virt"] = buf.String()

	return nil
}

func (p *procStat) getMonitorProcInfo(ba []byte, buf *bytes.Buffer) error {
	err := p.getAllProcInfo(ba, buf)
	if err != nil {
		return err
	}

	count := len(p.procs)
	if count > gTopList {
		count = gTopList
	}

	p.topCPU = p.topCPU[:0]
	p.topMem = p.topMem[:0]
	p.topIO = p.topIO[:0]
	p.topWriteIO = p.topWriteIO[:0]
	p.topReadIO = p.topReadIO[:0]
	p.topRealWriteIO = p.topRealWriteIO[:0]
	p.dProc = p.dProc[:0]
	p.topWchar = p.topWchar[:0]
	p.topMinFault = p.topMinFault[:0]
	p.whitelist = p.whitelist[:0]
	for _, proc := range p.procs {
		if proc.cpu.Status == 'Z' {
			continue
		}
		if proc.cpu.Status == 'D' {
			p.dProc = append(p.dProc, proc)
		}
		p.topCPU = append(p.topCPU, proc)
		p.topMem = append(p.topMem, proc)
		p.topIO = append(p.topIO, proc)
		p.topWriteIO = append(p.topWriteIO, proc)
		p.topReadIO = append(p.topReadIO, proc)
		p.topRealWriteIO = append(p.topRealWriteIO, proc)
		p.topMinFault = append(p.topMinFault, proc)
		p.topWchar = append(p.topWchar, proc)

		if _, ok := gMonitors[proc.cpu.BinName]; ok {
			p.whitelist = append(p.whitelist, proc)
		}

		if strings.Contains(proc.cpu.Comm, "jbd") {
			p.whitelist = append(p.whitelist, proc)
		}
	}
	sort.Slice(p.topCPU, func(i, j int) bool {
		return p.topCPU[i].cpuDelta.Utime+p.topCPU[i].cpuDelta.Stime > p.topCPU[j].cpuDelta.Utime+p.topCPU[j].cpuDelta.Stime
	})

	sort.Slice(p.topMem, func(i, j int) bool {
		return p.topMem[i].mem.Resident > p.topMem[j].mem.Resident
	})

	sort.Slice(p.topIO, func(i, j int) bool {
		return p.topIO[i].ioDelta.WriteBytes+p.topIO[i].ioDelta.ReadBytes > p.topIO[j].ioDelta.WriteBytes+p.topIO[j].ioDelta.ReadBytes
	})

	sort.Slice(p.topWriteIO, func(i, j int) bool {
		return p.topIO[i].ioDelta.WriteBytes > p.topIO[j].ioDelta.WriteBytes
	})

	sort.Slice(p.topRealWriteIO, func(i, j int) bool {
		return p.topIO[i].ioDelta.WriteBytes-p.topIO[i].ioDelta.CancelledWriteBytes > p.topIO[j].ioDelta.WriteBytes-p.topIO[i].ioDelta.CancelledWriteBytes
	})

	sort.Slice(p.topReadIO, func(i, j int) bool {
		return p.topIO[i].ioDelta.ReadBytes > p.topIO[j].ioDelta.ReadBytes
	})

	sort.Slice(p.topWchar, func(i, j int) bool {
		return p.topIO[i].ioDelta.Wchar > p.topIO[j].ioDelta.Wchar
	})

	sort.Slice(p.topMinFault, func(i, j int) bool {
		return p.topCPU[i].cpuDelta.MinFault > p.topCPU[j].cpuDelta.MinFault
	})

	p.topCPU = p.topCPU[0:count]
	p.topIO = p.topIO[0:count]
	p.topMem = p.topMem[0:count]
	p.topWriteIO = p.topWriteIO[0:count]
	p.topRealWriteIO = p.topRealWriteIO[0:count]
	p.topReadIO = p.topReadIO[0:count]
	p.topMinFault = p.topMinFault[0:count]
	p.topWchar = p.topWchar[0:count]
	return nil
}

func (p *procStat) getAllProcInfo(ba []byte, buf *bytes.Buffer) error {
	var err error
	p.ZombieCount = 0
	if p.procFile == nil {
		p.procFile, err = os.Open("/proc")
		return err
	}
	p.procFile.Seek(0, io.SeekStart)
	procDirFi, err := p.procFile.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, proc := range p.procs {
		proc.isDeleted = true
	}

	curProc := &procInfo{}
	for _, pidStr := range procDirFi {
		var path string
		pid, err := strconv.ParseUint(pidStr, 10, 64)
		if err != nil {
			continue
		}

		buf.Reset()
		proc, ok := p.procs[pid]
		if !ok {
			proc = &procInfo{}
			p.procs[pid] = proc
			proc.isNew = true
			proc.isUsed = false
			proc.isDeleted = false
		} else {
			proc.isUsed = true
			proc.isNew = false
			proc.isDeleted = false
		}
		buf.WriteString("/proc/")
		buf.WriteString(pidStr)
		buf.WriteString("/stat")
		path = buf.String()
		curProc.fpCPU, err = system.ReadProcessStat(path, buf, &curProc.cpu, proc.fpCPU)
		if err != nil {
			proc.isDeleted = true
			continue
		}
		if curProc.cpu.Status == 'Z' {
			p.ZombieCount++
		}
		buf.Reset()
		buf.WriteString("/proc/")
		buf.WriteString(pidStr)
		buf.WriteString("/statm")
		path = buf.String()
		curProc.fpMem, err = system.ReadMemStat(path, buf, &curProc.mem, proc.fpMem)
		if err != nil {
			proc.isDeleted = true
			continue
		}

		buf.Reset()
		buf.WriteString("/proc/")
		buf.WriteString(pidStr)
		buf.WriteString("/io")
		path = buf.String()
		curProc.fpIO, err = system.ReadIOStat(path, buf, &curProc.io, proc.fpIO)
		if err != nil {
			proc.isDeleted = true
			continue
		}
		buf.Reset()
		buf.WriteString("/proc/")
		buf.WriteString(pidStr)
		buf.WriteString("/cmdline")
		path = buf.String()
		curProc.fpCmd, err = system.GetCmdLine(path, buf, &curProc.cpu, proc.fpCmd)
		if err != nil {
			proc.isDeleted = true
			continue
		}
		buf.Reset()
		buf.WriteString("/proc/")
		buf.WriteString(pidStr)
		buf.WriteString("/exe")
		path = buf.String()
		_ = system.GetBinName(path, ba, &curProc.cpu)
		if curProc.cpu.BinName == "" {
			curProc.cpu.BinName = curProc.cpu.Comm
		}
		proc.cpuDelta.Stime = curProc.cpu.Stime - proc.cpu.Stime
		proc.cpuDelta.Utime = curProc.cpu.Utime - proc.cpu.Utime
		proc.cpuDelta.MinFault = curProc.cpu.MinFault - proc.cpu.MinFault
		proc.cpuDelta.MajFault = curProc.cpu.MajFault - proc.cpu.MajFault
		proc.ioDelta.CancelledWriteBytes = curProc.io.CancelledWriteBytes - proc.io.CancelledWriteBytes
		proc.ioDelta.Rchar = curProc.io.Rchar - proc.io.Rchar
		proc.ioDelta.Wchar = curProc.io.Wchar - proc.io.Wchar
		proc.ioDelta.Syscr = curProc.io.Syscr - proc.io.Syscr
		proc.ioDelta.Syscw = curProc.io.Syscw - proc.io.Syscw
		proc.ioDelta.WriteBytes = curProc.io.WriteBytes - proc.io.WriteBytes
		proc.ioDelta.ReadBytes = curProc.io.ReadBytes - proc.io.ReadBytes

		proc.cpu = curProc.cpu
		proc.io = curProc.io
		proc.mem = curProc.mem

		proc.fpCmd = curProc.fpCmd
		proc.fpIO = curProc.fpIO
		proc.fpMem = curProc.fpMem
		proc.fpCPU = curProc.fpCPU
	}

	for pid, proc := range p.procs {
		if proc.isDeleted {
			proc.fpCmd.Close()
			proc.fpIO.Close()
			proc.fpMem.Close()
			proc.fpCPU.Close()
			delete(p.procs, pid)
		}
	}
	return nil
}

func (p *procStat) stop() {
	if p.procFile != nil {
		p.procFile.Close()
	}
}

// memStat collect mem stat
type memStat struct {
	file *os.File
}

//
func memStatNew() *memStat {
	return &memStat{}
}

func (m *memStat) collect(out map[string]interface{}, buf []byte) error {
	var err error
	if m.file == nil {
		m.file, err = os.Open(consts.ProcMemInfo)
		if err != nil {
			return err
		}
	}

	num, err := m.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
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
		out[string(fields[0])] = string(fields[1])
	}

	return nil
}

func (m *memStat) stop() {
	if m.file != nil {
		m.file.Close()
		m.file = nil
	}
}

type cpuStat struct {
	contextSwitch     uint64
	metricSet         []string
	cpuList           []string
	curStat           []uint64
	deltaStat         map[string][]uint64 //保存delta 数据
	preStat           map[string][]uint64 // 保存前一次数据
	originValueOutput map[string][]uint64 //为了便于输出，保存原始数据
	deltaValueOutput  map[string][]uint64 // 为了便于输出，保留的delta 数据
	file              *os.File
}

func cpuStatNew() *cpuStat {
	c := new(cpuStat)
	c.metricSet = []string{
		"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice",
	}
	num, err := system.GetCPUCount()
	if err != nil {
		num = 192
	}
	c.curStat = make([]uint64, consts.CPUStatColNum)
	cpuNum = num + 1
	c.cpuList = make([]string, cpuNum)
	c.preStat = make(map[string][]uint64, consts.CPUStatColNum)
	c.deltaStat = make(map[string][]uint64, consts.CPUStatColNum)
	c.originValueOutput = make(map[string][]uint64, cpuNum)
	c.deltaValueOutput = make(map[string][]uint64, cpuNum)
	c.curStat = make([]uint64, consts.CPUStatColNum)
	return c
}

func getBootTime() uint64 {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer file.Close()

	var buf []byte
	buf = make([]byte, 128*1024)
	_, err = file.Read(buf)
	if err != nil {
		return 0
	}

	var bootTime uint64
	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.Fields(buf[:index])
		if bytes.HasPrefix(fields[0], []byte("btime")) {
			bootTime, err = strconv.ParseUint(string(fields[1]), 10, 64)
			break
		}
		buf = buf[index+1:]
	}
	return bootTime
}

func (c *cpuStat) collect(out map[string]interface{}, buf []byte, buf2 *bytes.Buffer) error {
	var err error
	if c.file == nil {
		c.file, err = os.Open(consts.ProcCpuStat)
		if err != nil {
			return err
		}
	}

	num, err := c.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	var count uint64

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]
		if bytes.HasPrefix(fields[0], []byte("cpu")) {
			curCPU := string(fields[0])
			for i, value := range fields[1:] {
				c.curStat[i], err = strconv.ParseUint(string(value), 10, 64)
			}
			pre, ok := c.preStat[curCPU]
			if !ok {
				pre = make([]uint64, 10)
			}
			delta, ok := c.deltaStat[curCPU]
			if !ok {
				delta = make([]uint64, 10)
			}
			for i := range fields[1:] {
				delta[i] = c.curStat[i] - pre[i]
				pre[i] = c.curStat[i]
			}
			c.deltaStat[curCPU] = delta
			c.preStat[curCPU] = pre
			c.transform2Output(curCPU, pre, delta, count)
			count++
		} else if bytes.HasPrefix(fields[0], []byte("ctxt")) {
			if ctxt, err := strconv.ParseUint(string(fields[1]), 10, 64); err == nil {
				delta := ctxt - c.contextSwitch
				if delta > 0 {
					out["context_switch_delta"] = strconv.Itoa(int(delta))
				}
				c.contextSwitch = ctxt
			}
			out["context_switch"] = string(fields[1])

		} else if bytes.HasPrefix(fields[0], []byte("procs_running")) {
			out["procs_running"] = string(fields[1])
		} else if bytes.HasPrefix(fields[0], []byte("procs_blocked")) {
			out["procs_blocked"] = string(fields[1])
		}
	}

	var tmp []string
	for k, v := range c.originValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}
	for k, v := range c.deltaValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}

	fastJoin(buf2, c.cpuList, ',')
	out["cpu_list"] = buf2.String()

	return nil
}

func (c *cpuStat) transform2Output(cpu string, pre, delta []uint64, index uint64) {
	c.cpuList[index] = cpu
	for i, key := range c.metricSet {
		originOutput, ok := c.originValueOutput[key]
		if !ok {
			originOutput = make([]uint64, cpuNum)
			c.originValueOutput[key] = originOutput
		}
		originOutput[index] = pre[i]
		deltaOutput, ok := c.deltaValueOutput[key+"_delta"]
		if !ok {
			deltaOutput = make([]uint64, cpuNum)
			c.deltaValueOutput[key+"_delta"] = deltaOutput
		}
		deltaOutput[index] = delta[i]
	}
}

func (c *cpuStat) stop() {
	if c.file != nil {
		c.file.Close()
		c.file = nil
	}
}

type loadAvg struct {
	load1  uint64
	load5  uint64
	load15 uint64
	file   *os.File
}

func loadAvgNew() *loadAvg {
	return &loadAvg{}
}

func (l *loadAvg) collect(out map[string]interface{}, buf []byte) error {
	var err error
	if l.file == nil {
		l.file, err = os.Open(consts.ProcLoadAvg)
		if err != nil {
			return err
		}
	}

	num, err := l.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}
		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]

		load1, err := strconv.ParseFloat(string(fields[0]), 64)
		if err != nil {
			log.Error("[sar_collector] loadAvg collect parseUint failed", log.String("field", string(fields[0])), log.String("error", err.Error()))
			continue
		}
		l.load1 = uint64(load1)

		load5, err := strconv.ParseFloat(string(fields[1]), 64)
		if err != nil {
			log.Error("[sar_collector] loadAvg collect parseUint failed", log.String("field", string(fields[1])), log.String("error", err.Error()))
			continue
		}
		l.load5 = uint64(load5)

		load15, err := strconv.ParseFloat(string(fields[2]), 64)
		if err != nil {
			log.Error("[sar_collector] loadAvg collect parseUint failed", log.String("field", string(fields[1])), log.String("error", err.Error()))
			continue
		}
		l.load15 = uint64(load15)
	}
	out["load1"] = strconv.FormatUint(l.load1, 10)
	out["load5"] = strconv.FormatUint(l.load5, 10)
	out["load15"] = strconv.FormatUint(l.load15, 10)
	return nil
}

func (l *loadAvg) stop() {
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
}

type diskStat struct {
	first              bool
	currentStat        []uint64
	metricSet          []string
	diskList           []string
	preStat            map[string][]uint64
	deltaStat          map[string][]uint64
	originValueOutput  map[string][]uint64
	deltaValueOutput   map[string][]uint64
	blockDeviceTypeMap map[string]bool
	blockDeviceNumMap  map[int]bool
	deviceFile         *os.File
	file               *os.File
}

var filterMajorDisk = map[string]bool{
	"1": true,
	"7": true,
}

func diskStatNew() *diskStat {
	d := new(diskStat)
	d.metricSet = []string{
		"read_complete", "read_merge", "read_sectors", "read_time", "write_complete", "write_merge",
		"write_sectors", "write_time", "io_in_flight", "io_time", "io_weight_time",
	}
	d.diskList = make([]string, consts.DiskNum)
	d.preStat = make(map[string][]uint64)
	d.deltaStat = make(map[string][]uint64)
	d.originValueOutput = make(map[string][]uint64)
	d.deltaValueOutput = make(map[string][]uint64)
	d.blockDeviceTypeMap = make(map[string]bool)
	d.blockDeviceNumMap = make(map[int]bool)
	d.currentStat = make([]uint64, consts.DiskIostatColNum)
	return d
}

func (d *diskStat) collect(out map[string]interface{}, buf []byte, buf2 *bytes.Buffer) error {
	var err error
	if d.file == nil {
		d.file, err = os.Open(consts.ProcDiskStat)
		if err != nil {
			return err
		}
	}

	if d.deviceFile == nil {
		tmpbuf := make([]byte, 256*1024)
		d.deviceFile, err = os.Open(consts.ProcDevice)
		if err != nil {
			return err
		}

		num, err := d.deviceFile.ReadAt(tmpbuf, 0)
		if err != nil && err != io.EOF {
			return err
		}

		tmpbuf = tmpbuf[:num]
		isBlockDeviceBegin := false

		for {
			index := bytes.IndexRune(tmpbuf, '\n')
			if index < 0 {
				break
			}

			if !isBlockDeviceBegin {
				if bytes.HasPrefix(tmpbuf[:index], []byte("Block devices:")) {
					isBlockDeviceBegin = true
				}

				tmpbuf = tmpbuf[index+1:]
				continue
			} else {
				fields := bytes.Fields(tmpbuf[:index])
				tmpbuf = tmpbuf[index+1:]
				if len(fields) != 2 {
					log.Warn("[sar_collector] block device fields failed", log.Int("len", len(fields)))
					continue
				}

				x, err := strconv.Atoi(string(fields[0]))
				if err != nil {
					log.Warn("[sar_collector] convert block num field error",
						log.String("block num", string(fields[0])), log.String("error", err.Error()))
					continue
				}

				if _, ok := d.blockDeviceTypeMap["all"]; ok {
					d.blockDeviceNumMap[x] = true
					continue
				}

				if _, ok := d.blockDeviceTypeMap[string(fields[1])]; ok {
					d.blockDeviceNumMap[x] = true
				}
			}
		}
		log.Info("[sar_collector] block device num map", log.String("map", fmt.Sprintf("%+v", d.blockDeviceNumMap)))
	}

	num, err := d.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}

	buf = buf[:num]
	var count uint64

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}
		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]

		if len(fields) < 14 {
			continue
		}

		x, err := strconv.Atoi(string(fields[0]))
		if err != nil {
			log.Warn("[sar_collector] convert block num field error",
				log.String("block num", string(fields[0])), log.String("error", err.Error()))
			continue
		}

		if _, ok := d.blockDeviceNumMap[x]; !ok {
			continue
		}

		if filterMajorDisk[string(fields[0])] {
			continue
		}
		//var stat [11]uint64
		for i, value := range fields[3:] {
			if i >= 11 {
				break
			}
			d.currentStat[i], err = strconv.ParseUint(string(value), 10, 64)
		}
		pre, ok := d.preStat[string(fields[2])]
		if !ok {
			pre = make([]uint64, consts.DiskIostatColNum)
		}
		delta, ok := d.deltaStat[string(fields[2])]
		if !ok {
			delta = make([]uint64, consts.DiskIostatColNum)
		}
		for i := range fields[3:] {
			delta[i] = d.currentStat[i] - pre[i]
			pre[i] = d.currentStat[i]
		}
		d.deltaStat[string(fields[2])] = delta
		d.preStat[string(fields[2])] = pre
		d.transform2Output(string(fields[2]), pre, delta, count)
		count++
	}

	var tmp []string
	for k, v := range d.originValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}
	for k, v := range d.deltaValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}
	fastJoin(buf2, d.diskList[:count], ',')
	out["disk_list"] = buf2.String()
	return nil
}

func (d *diskStat) transform2Output(disk string, pre, delta []uint64, index uint64) {
	d.diskList[index] = disk
	for i, key := range d.metricSet {
		originOutput, ok := d.originValueOutput[key]
		if !ok {
			originOutput = make([]uint64, consts.DiskNum)
			d.originValueOutput[key] = originOutput
		}

		originOutput[index] = pre[i]
		deltaOutput, ok := d.deltaValueOutput[key+"_delta"]
		if !ok {
			deltaOutput = make([]uint64, consts.DiskNum)
			d.deltaValueOutput[key+"_delta"] = deltaOutput
		}
		deltaOutput[index] = delta[i]
	}
}

func (d *diskStat) stop() {
	if d.file != nil {
		d.file.Close()
		d.file = nil
	}
}

type fSStat struct {
	partitionNum    uint64
	bsizeOutput     []uint64
	blocksOutput    []uint64
	bfreeOutput     []uint64
	bavailOutput    []uint64
	filesOutput     []uint64
	ffreeOutput     []uint64
	metricSet       []string
	diskList        []string
	fsType          map[string]bool
	diskSet         map[string]bool
	lastCollectTime time.Time
	file            *os.File
}

func fSStatNew() *fSStat {
	fs := new(fSStat)
	fs.metricSet = []string{
		"bsize", "blocks", "bfree", "bavail", "files", "ffree",
	}
	fs.diskList = make([]string, consts.PartitionNum)
	fs.bsizeOutput = make([]uint64, consts.PartitionNum)
	fs.blocksOutput = make([]uint64, consts.PartitionNum)
	fs.bfreeOutput = make([]uint64, consts.PartitionNum)
	fs.bavailOutput = make([]uint64, consts.PartitionNum)
	fs.filesOutput = make([]uint64, consts.PartitionNum)
	fs.ffreeOutput = make([]uint64, consts.PartitionNum)
	fs.fsType = map[string]bool{
		"ext2": true,
		"ext3": true,
		"ext4": true,
		"xfs":  true,
	}
	fs.diskSet = make(map[string]bool, 32)
	return fs
}

type perFsStat struct {
	BSize  uint64
	Blocks uint64
	BFree  uint64
	BAvail uint64
	Files  uint64
	FFree  uint64
}

func (f *fSStat) collect(out map[string]interface{}, buf []byte, buf2 *bytes.Buffer) error {
	var err error
	if time.Since(f.lastCollectTime) > 300*time.Second {
		for k := range f.diskSet {
			delete(f.diskSet, k)
		}

		if f.file == nil {
			f.file, err = os.Open(consts.EtcMTab)
			if err != nil {
				return err
			}
		}

		num, err := f.file.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			return err
		}
		buf = buf[:num]
		f.partitionNum = 0
		for {
			index := bytes.IndexRune(buf, '\n')
			if index <= 0 {
				break
			}
			fields := bytes.Fields(buf[:index])
			buf = buf[index+1:]

			if len(fields) < 3 {
				continue
			}

			if _, ok := f.fsType[string(fields[2])]; !ok {
				continue
			}
			_, ok := f.diskSet[string(fields[0])]
			if ok {
				continue
			} else {
				f.diskSet[string(fields[0])] = true
			}

			fs := new(perFsStat)
			err := collectPerFsStat(string(fields[1]), fs)
			if err != nil {
				log.Error("[sar_collector] collectPerFsStat failed", log.String("mountPoint", string(fields[1])), log.String("err", err.Error()))
				continue
			}
			target := transformSoftLink(string(fields[0]))
			f.transform2Output(string(fields[1])+"|||"+target, fs, f.partitionNum)
			//f.transform2Output(string(fields[1]), fs, f.partitionNum)
			f.partitionNum++
		}

		f.lastCollectTime = time.Now()
	}

	var tmp []string

	fastJoin(buf2, f.diskList[:f.partitionNum], ',')
	out["fs_partition_list"] = buf2.String()

	for _, iter := range f.bsizeOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["bsize"] = buf2.String()

	tmp = tmp[:0]
	for _, iter := range f.blocksOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["blocks"] = buf2.String()

	tmp = tmp[:0]
	for _, iter := range f.bfreeOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["bfree"] = buf2.String()

	tmp = tmp[:0]
	for _, iter := range f.bavailOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["bavail"] = buf2.String()

	tmp = tmp[:0]
	for _, iter := range f.filesOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["files"] = buf2.String()

	tmp = tmp[:0]
	for _, iter := range f.ffreeOutput[:f.partitionNum] {
		tmp = append(tmp, strconv.FormatUint(iter, 10))
	}
	fastJoin(buf2, tmp, ',')
	out["ffree"] = buf2.String()

	return nil
}

func transformSoftLink(dev string) string {
	target, err := os.Readlink(dev)
	if err == nil {
		if strings.HasPrefix(target, "../") {
			return filepath.Base(filepath.Join(filepath.Dir(dev), target))
		}
	}
	return filepath.Base(dev)
}

func (f *fSStat) transform2Output(partition string, stat *perFsStat, index uint64) {
	f.diskList[index] = partition
	f.bsizeOutput[index] = stat.BSize
	f.blocksOutput[index] = stat.Blocks
	f.bfreeOutput[index] = stat.BFree
	f.bavailOutput[index] = stat.BAvail
	f.filesOutput[index] = stat.Files
	f.ffreeOutput[index] = stat.FFree
}

func (f *fSStat) stop() {
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
}

func collectPerFsStat(mp string, fs *perFsStat) error {
	stat := new(syscall.Statfs_t)
	err := syscall.Statfs(mp, stat)
	if err != nil {
		return err
	}
	fs.BSize = uint64(stat.Bsize)
	fs.Blocks = stat.Blocks
	fs.BFree = stat.Bfree
	fs.BAvail = stat.Bavail
	fs.Files = stat.Files
	fs.FFree = stat.Ffree
	return nil
}

type netStat struct {
	sockStat *sockStat
	extStat  *netExtStat
	snmpStat *netSnmp
	devStat  *netDevStat
}

func netStatNew() *netStat {
	return &netStat{
		sockStat: &sockStat{},
		extStat:  netExtStatNew(),
		snmpStat: netSnmpNew(),
		devStat:  netDevStatNew(),
	}
}

func (n *netStat) collect(out map[string]interface{}, buf []byte, buf2 *bytes.Buffer) error {
	err := n.sockStat.collectNetSockStat(out, buf)
	if err != nil {
		return err
	}
	err = n.extStat.collectNetStat(out, buf)
	if err != nil {
		return err
	}
	err = n.snmpStat.collect(out, buf)
	if err != nil {
		return err
	}
	err = n.devStat.collectNetDev(out, buf, buf2)
	if err != nil {
		return err
	}
	return nil
}

func (n *netStat) stop() {
	n.sockStat.stop()
	n.extStat.stop()
	n.snmpStat.stop()
	n.devStat.stop()
}

type sockStat struct {
	socketsUsed uint64
	tcpInUse    uint64
	tcpOrphan   uint64
	tcpTW       uint64
	tcpAlloc    uint64
	tcpMem      uint64
	file        *os.File
}

func (s *sockStat) collectNetSockStat(out map[string]interface{}, buf []byte) error {
	var err error
	if s.file == nil {
		s.file, err = os.Open(consts.ProcNetSockStat)
		if err != nil {
			return err
		}
	}

	num, err := s.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	for {

		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]

		if bytes.HasPrefix(fields[0], []byte("sockets")) {
			if len(fields) == 3 && bytes.Equal(fields[1], []byte("used")) {
				s.socketsUsed, err = strconv.ParseUint(string(fields[2]), 10, 64)
				if err != nil {
					log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[2])), log.String("error", err.Error()))
					continue
				}
			}
		} else if bytes.HasPrefix(fields[0], []byte("TCP")) {
			if len(fields) >= 11 {
				if bytes.Equal(fields[1], []byte("inuse")) {
					s.tcpInUse, err = strconv.ParseUint(string(fields[2]), 10, 64)
					if err != nil {
						log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[2])), log.String("error", err.Error()))
						continue
					}
				}
				if bytes.Equal(fields[3], []byte("orphan")) {
					s.tcpOrphan, err = strconv.ParseUint(string(fields[4]), 10, 64)
					if err != nil {
						log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[4])), log.String("error", err.Error()))
						continue
					}
				}
				if bytes.Equal(fields[5], []byte("tw")) {
					s.tcpTW, err = strconv.ParseUint(string(fields[6]), 10, 64)
					if err != nil {
						log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[6])), log.String("error", err.Error()))
						continue
					}
				}
				if bytes.Equal(fields[7], []byte("alloc")) {
					s.tcpAlloc, err = strconv.ParseUint(string(fields[8]), 10, 64)
					if err != nil {
						log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[8])), log.String("error", err.Error()))
						continue
					}
				}
				if bytes.Equal(fields[9], []byte("mem")) {
					s.tcpMem, err = strconv.ParseUint(string(fields[10]), 10, 64)
					if err != nil {
						log.Error("[sar_collector] collectNetSockStat parseUint failed", log.String("field", string(fields[10])), log.String("error", err.Error()))
						continue
					}
				}
			}
		}
	}

	out["sockets_used"] = strconv.FormatUint(s.socketsUsed, 10)
	out["tcp_inuse"] = strconv.FormatUint(s.tcpInUse, 10)
	out["tcp_orphan"] = strconv.FormatUint(s.tcpOrphan, 10)
	out["tcp_tw"] = strconv.FormatUint(s.tcpTW, 10)
	out["tcp_alloc"] = strconv.FormatUint(s.tcpAlloc, 10)
	out["tcp_mem"] = strconv.FormatUint(s.tcpMem, 10)

	return nil
}

func (s *sockStat) stop() {
	if s.file != nil {
		s.file.Close()
	}
}

type netExtStat struct {
	tcpExtkeys       []string
	tcpExtPreValue   []uint64
	tcpExtDeltaValue []uint64
	ipExtKeys        []string
	ipExtPreValue    []uint64
	ipExtDeltaValue  []uint64
	current          []uint64
	currentStat      []uint64
	file             *os.File
}

func netExtStatNew() *netExtStat {
	return &netExtStat{
		currentStat: make([]uint64, consts.NetExtStatColNum),
	}
}

func (n *netExtStat) collectNetStat(out map[string]interface{}, buf []byte) error {
	var err error
	if n.file == nil {
		n.file, err = os.Open(consts.ProcNetStat)
		if err != nil {
			return err
		}
	}

	num, err := n.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]

		if bytes.Equal(fields[0], []byte("TcpExt:")) {
			if n.tcpExtkeys == nil {
				n.tcpExtkeys = make([]string, len(fields)-1)
			}
			for i, field := range fields[1:] {
				n.tcpExtkeys[i] = string(field)
			}

			index := bytes.IndexRune(buf, '\n')
			if index <= 0 {
				break
			}

			fields = bytes.Fields(buf[:index])
			buf = buf[index+1:]

			if bytes.Equal(fields[0], []byte("TcpExt:")) {
				for i, field := range fields[1:] {
					n.currentStat[i], err = strconv.ParseUint(string(field), 10, 64)
				}

				if n.tcpExtPreValue == nil {
					n.tcpExtPreValue = make([]uint64, len(fields)-1)
				}
				if n.tcpExtDeltaValue == nil {
					n.tcpExtDeltaValue = make([]uint64, len(fields)-1)
				}

				for i := range fields[1:] {
					n.tcpExtDeltaValue[i] = n.currentStat[i] - n.tcpExtPreValue[i]
					n.tcpExtPreValue[i] = n.currentStat[i]
				}
			}

		} else if bytes.Equal(fields[0], []byte("IpExt:")) {
			if n.ipExtKeys == nil {
				n.ipExtKeys = make([]string, len(fields)-1)
			}
			for i, field := range fields[1:] {
				n.ipExtKeys[i] = string(field)
			}

			index := bytes.IndexRune(buf, '\n')
			if index <= 0 {
				break
			}

			fields = bytes.Fields(buf[:index])
			buf = buf[index+1:]
			if bytes.Equal(fields[0], []byte("IpExt:")) {
				for i, field := range fields[1:] {
					n.currentStat[i], err = strconv.ParseUint(string(field), 10, 64)
				}
				if n.ipExtPreValue == nil {
					n.ipExtPreValue = make([]uint64, len(fields)-1)
				}
				if n.ipExtDeltaValue == nil {
					n.ipExtDeltaValue = make([]uint64, len(fields)-1)
				}
				for i := range fields[1:] {
					n.ipExtDeltaValue[i] = n.currentStat[i] - n.ipExtPreValue[i]
					n.ipExtPreValue[i] = n.currentStat[i]
				}
			}
		}
	}

	for i, key := range n.tcpExtkeys {
		out[key] = strconv.FormatUint(n.tcpExtPreValue[i], 10)
		out[key+"_delta"] = strconv.FormatUint(n.tcpExtDeltaValue[i], 10)
	}
	for i, key := range n.ipExtKeys {
		out[key] = strconv.FormatUint(n.ipExtPreValue[i], 10)
		out[key+"_delta"] = strconv.FormatUint(n.ipExtDeltaValue[i], 10)
	}

	return nil
}

func (n *netExtStat) stop() {
	if n.file != nil {
		n.file.Close()
		n.file = nil
	}
}

type netSnmp struct {
	tcpKeys       []string
	tcpPreValue   []uint64
	tcpDeltaValue []uint64
	current       []uint64
	currentStat   []uint64
	file          *os.File
}

func netSnmpNew() *netSnmp {
	return &netSnmp{
		currentStat: make([]uint64, consts.NetSnmpColNum),
	}
}

func (ns *netSnmp) collect(out map[string]interface{}, buf []byte) error {
	var err error
	if ns.file == nil {
		ns.file, err = os.Open(consts.ProcNetSnmp)
		if err != nil {
			return err
		}
	}

	num, err := ns.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}
		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]
		if bytes.Equal(fields[0], []byte("Tcp:")) {
			if ns.tcpKeys == nil {
				ns.tcpKeys = make([]string, len(fields)-1)
			}
			for i, field := range fields[1:] {
				ns.tcpKeys[i] = string(field)
			}

			index := bytes.IndexRune(buf, '\n')
			if index <= 0 {
				break
			}

			fields := bytes.Fields(buf[:index])
			buf = buf[index+1:]
			if bytes.Equal(fields[0], []byte("Tcp:")) {
				for i, field := range fields[1:] {
					ns.currentStat[i], err = strconv.ParseUint(string(field), 10, 64)
				}
				if ns.tcpDeltaValue == nil {
					ns.tcpDeltaValue = make([]uint64, len(fields)-1)
				}
				if ns.tcpPreValue == nil {
					ns.tcpPreValue = make([]uint64, len(fields)-1)
				}
				for i := range fields[1:] {
					ns.tcpDeltaValue[i] = ns.currentStat[i] - ns.tcpPreValue[i]
					ns.tcpPreValue[i] = ns.currentStat[i]
				}
			}
		}
	}

	for i, key := range ns.tcpKeys {
		out[key] = strconv.FormatUint(ns.tcpPreValue[i], 10)
		out[key+"_delta"] = strconv.FormatUint(ns.tcpDeltaValue[i], 10)
	}
	return err
}

func (ns *netSnmp) stop() {
	if ns.file != nil {
		ns.file.Close()
		ns.file = nil
	}
}

type netDevStat struct {
	currentStat       []uint64
	metricSet         []string
	devList           []string
	preStat           map[string][]uint64
	deltaStat         map[string][]uint64
	originValueOutput map[string][]uint64
	deltaValueOutput  map[string][]uint64
	file              *os.File
	typeMap           map[string]string
}

func netDevStatNew() *netDevStat {
	return &netDevStat{
		metricSet: []string{
			"recv_bytes", "recv_packets", "recv_errs", "recv_drop", "recv_fifo", "recv_frame", "recv_compressed", "recv_multicast",
			"transmit_bytes", "transmit_packets", "transmit_errs", "transmit_drop", "transmit_fifo", "transmit_colls", "transmit_carrier", "transmit_compressed",
		},
		devList:           make([]string, consts.NetDevNum),
		preStat:           make(map[string][]uint64),
		deltaStat:         make(map[string][]uint64),
		originValueOutput: make(map[string][]uint64),
		deltaValueOutput:  make(map[string][]uint64),
		currentStat:       make([]uint64, consts.NetDevColNum),
		typeMap:           make(map[string]string),
	}
}

func (n *netDevStat) collectNetDev(out map[string]interface{}, buf []byte, buf2 *bytes.Buffer) error {
	var err error
	if n.file == nil {
		n.file, err = os.Open(consts.ProcNetDev)
		if err != nil {
			return err
		}
	}

	num, err := n.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}

	buf = buf[:num]
	index := bytes.IndexRune(buf, '\n')

	if index <= 0 {
		return fmt.Errorf("collectNetDev file content too short")
	}
	buf = buf[index+1:]

	index = bytes.IndexRune(buf, '\n')
	if index <= 0 {
		return fmt.Errorf("collectNetDev file content too short")
	}
	buf = buf[index+1:]

	var count uint64
	for {
		index = bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.FieldsFunc(buf[:index], func(r rune) bool {
			return r == ':' || unicode.IsSpace(r)
		})
		buf = buf[index+1:]

		if typestr, ok := n.typeMap[string(fields[0])]; ok {
			if typestr != "physical" {
				continue
			}
		} else {
			if _, err := os.Stat(fmt.Sprintf("/sys/class/net/%s", string(fields[0]))); err == nil {
				if _, err = os.Stat(fmt.Sprintf("/sys/class/net/%s/device", string(fields[0]))); err == nil {
					n.typeMap[string(fields[0])] = "physical"
				} else if errors.Is(err, os.ErrNotExist) {
					n.typeMap[string(fields[0])] = "virtual"
				}
			}
			log.Info("[sar_collector] find net dev",
				log.String("type map", fmt.Sprintf("%+v", n.typeMap)),
				log.String("dev", string(fields[0])))
		}

		for i, value := range fields[1:] {
			n.currentStat[i], err = strconv.ParseUint(string(value), 10, 64)
		}
		pre, ok := n.preStat[string(fields[0])]
		if !ok {
			pre = make([]uint64, consts.NetDevColNum)
		}
		delta, ok := n.deltaStat[string(fields[0])]
		if !ok {
			delta = make([]uint64, consts.NetDevColNum)
			n.deltaStat[string(fields[0])] = delta
		}

		for i := range fields[1:] {
			delta[i] = n.currentStat[i] - pre[i]
			pre[i] = n.currentStat[i]
		}

		n.transform2Output(string(fields[0]), pre, delta, count)
		n.preStat[string(fields[0])] = pre
		count++
	}
	var tmp []string
	for k, v := range n.originValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}
	for k, v := range n.deltaValueOutput {
		tmp = tmp[:0]
		for _, iter := range v[:count] {
			tmp = append(tmp, strconv.FormatUint(iter, 10))
		}
		fastJoin(buf2, tmp, ',')
		out[k] = buf2.String()
	}
	fastJoin(buf2, n.devList[:count], ',')
	out["nic_list"] = buf2.String()

	return nil
}

func (n *netDevStat) transform2Output(dev string, pre, delta []uint64, index uint64) {
	n.devList[index] = dev
	for i, key := range n.metricSet {
		originOutput, ok := n.originValueOutput[key]
		if !ok {
			originOutput = make([]uint64, consts.NetDevNum)
			n.originValueOutput[key] = originOutput
		}
		originOutput[index] = pre[i]
		deltaOutput, ok := n.deltaValueOutput[key+"_delta"]
		if !ok {
			deltaOutput = make([]uint64, consts.NetDevNum)
			n.deltaValueOutput[key+"_delta"] = deltaOutput
		}
		deltaOutput[index] = delta[i]
	}
}

func (n *netDevStat) stop() {
	if n.file != nil {
		n.file.Close()
		n.file = nil
	}
}

func (v *vmStat) collect(out map[string]interface{}, buf []byte) error {
	var err error
	if v.file == nil {
		v.file, err = os.Open(consts.ProcVmStat)
		if err != nil {
			return err
		}
	}

	num, err := v.file.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:num]

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.Fields(buf[:index])
		buf = buf[index+1:]

		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			log.Error("[sar_collector] vmStat ParseUint failed", log.String("value", string(fields[1])), log.String("err", err.Error()))
			continue
		}
		v.stat[string(fields[0])] = value
	}

	for k, v := range v.stat {
		out[k] = strconv.FormatUint(v, 10)
	}
	return nil
}

func (v *vmStat) stop() {
	if v.file != nil {
		v.file.Close()
		v.file = nil
	}
	v.stat = nil
}

type vmStat struct {
	stat map[string]uint64
	file *os.File
}

func vmStatNew() *vmStat {
	return &vmStat{
		stat: make(map[string]uint64),
	}
}
