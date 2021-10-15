/*-------------------------------------------------------------------------
 *
 * pg_process_collector.go
 *    Polardb pg processes info collector
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
 *           plugins/polardb_pg/collector/pg_process_collector.go
 *-------------------------------------------------------------------------
 */
package collector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tklauser/go-sysconf"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/common/polardb_pg/logger"

	_ "github.com/lib/pq"
)

const MaxTopSize = 3

var BackendTypeMap = map[string]string{
	"logger":            "logger",
	"checkpointer":      "checkpoint",
	"background writer": "bgwriter",
	"walwriter":         "walwriter",
	"autovacuum":        "autovacuum",
	"archiver":          "archiver",
	"stats collector":   "pgstat",
	"walsender":         "walsender",
	"walreceiver":       "walreceiver",
	"startup":           "startup",
	"writer":            "bgwriter",
	"bgworker":          "bgworker",
}

type PgProcessResource struct {
	BackendType       string
	State             string
	CpuUser           uint64
	CpuSys            uint64
	Rss               uint64
	ReadIOCount       uint64
	WriteIOCount      uint64
	ReadIOThroughput  uint64
	WriteIOThroughput uint64

	CpuUserDelta           uint64
	CpuSysDelta            uint64
	ReadIOCountDelta       uint64
	WriteIOCountDelta      uint64
	ReadIOThroughputDelta  uint64
	WriteIOThroughputDelta uint64

	Count uint64
}

type PgProcessResourceCollector struct {
	buf               []byte
	lbuf              []byte
	count             uint64
	logger            *logger.PluginLogger
	processes         map[string]*PgProcessResource
	cgroup_tasks_path string
	cpuCoreNumber     float64
	intervalNano      uint64
	postmaster_pid    int64
	datadir           string
	prefix            *regexp.Regexp
	tick              uint64
}

func NewPgProcessResourceCollector() *PgProcessResourceCollector {
	p := &PgProcessResourceCollector{}
	p.processes = make(map[string]*PgProcessResource)
	p.buf = make([]byte, 4096)
	p.lbuf = make([]byte, 4096)
	return p
}

func (p *PgProcessResourceCollector) Init(m map[string]interface{},
	logger *logger.PluginLogger) error {

	p.logger = logger

	envs := m["env"].(map[string]string)
	if _, ok := envs["cgroup_cpu_path"]; ok {
		p.cgroup_tasks_path = envs["cgroup_cpu_path"] + "/tasks"
		p.postmaster_pid = int64(0)
	} else {
		var err error

		if path, ok := envs["host_data_local_dir"]; ok {
			if path != "" {
				p.datadir = path
			}
		}

		if p.datadir == "" {
			err := errors.New("cannot find data dir")
			p.logger.Error("cannot find data dir", err)
			return err
		}

		p.cgroup_tasks_path = ""
		p.postmaster_pid, err = p.getPostmasterPid()
		if err != nil {
			p.logger.Error("get postmaster pid failed", err)
			return err
		}
	}

	clkTck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		p.logger.Error("get clock tick fail", err)
		return err
	}
	p.tick = uint64(clkTck)

	p.prefix = regexp.MustCompile(`postgres(\(\d+\))?: `)

	return nil
}

func (p *PgProcessResourceCollector) Stop() error {
	return nil
}

func (p *PgProcessResourceCollector) Collect(out map[string]interface{}) error {
	var buf []byte
	p.count += 1

	if p.postmaster_pid == int64(0) {
		tasksfd, err := os.Open(p.cgroup_tasks_path)
		if err != nil {
			p.logger.Error("open cgroup tasks error", err,
				log.String("cgroup tasks path", p.cgroup_tasks_path))
			return err
		}
		defer tasksfd.Close()

		buf = p.buf
		// 1. get all processes
		// read cgroup tasks
		tasksfd.Seek(0, 0)
		num, err := tasksfd.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			p.logger.Error("read tasks file error", err)
			return err
		}
		buf = buf[:num]
	} else {
		postmaster_pid, err := p.getPostmasterPid()
		if err != nil {
			p.logger.Warn("get postmaster pid failed", err)
		} else {
			p.postmaster_pid = postmaster_pid
			buf = []byte(fmt.Sprintf("%d\n", postmaster_pid))
			cmdstr := fmt.Sprintf("ps h --ppid %d -o pid", int(p.postmaster_pid))
			cmd := exec.Command("bash", "-c", cmdstr)
			if out, err := cmd.Output(); err != nil {
				p.logger.Error("get pids from postmaster pid failed", err,
					log.String("command", cmdstr))
				return err
			} else {
				p.logger.Debug("get children pid result is",
					log.String("output", string(out)),
					log.String("cmdstr", cmdstr))
				buf = append(buf, out...)
			}
		}
	}

	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}
		pidstr := strings.TrimSpace(string(buf[:index]))
		buf = buf[index+1:]

		r, err := p.getPgProcessResource(pidstr)
		if err != nil {
			p.logger.Debug("get pg process resource failed", log.String("err", err.Error()))
			continue
		}

		if x, ok := p.processes[pidstr]; ok {
			p.mergePgProcessResource(x, r)
		} else {
			p.processes[pidstr] = r
		}
	}

	// remove the exit processes
	for k, v := range p.processes {
		if v.Count < p.count {
			delete(p.processes, k)
		}
	}

	// 3. build the result according to backend type
	if err := p.buildResult(out); err != nil {
		p.logger.Error("build result failed", err)
		return err
	}

	return nil
}

func (p *PgProcessResourceCollector) mergePgProcessResource(
	x *PgProcessResource, y *PgProcessResource) bool {

	if x.BackendType != y.BackendType {
		return false
	}

	nanoSecondsPerTick := uint64(time.Second.Nanoseconds()) / p.tick

	// CPU计算百分比乘以100
	nanoSecondsPerTickWithPercent := nanoSecondsPerTick * 100

	x.CpuUserDelta =
		(y.CpuUser - x.CpuUser) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.CpuSysDelta =
		(y.CpuSys - x.CpuSys) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.ReadIOCountDelta =
		(y.ReadIOCount - x.ReadIOCount) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.WriteIOCountDelta =
		(y.WriteIOCount - x.WriteIOCount) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.ReadIOThroughputDelta =
		(y.ReadIOThroughput - x.ReadIOThroughput) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.WriteIOThroughputDelta =
		(y.WriteIOThroughput - x.WriteIOThroughput) * nanoSecondsPerTickWithPercent / p.intervalNano
	x.Rss = y.Rss

	x.CpuUser = y.CpuUser
	x.CpuSys = y.CpuSys
	x.ReadIOCount = y.ReadIOCount
	x.WriteIOCount = y.WriteIOCount
	x.ReadIOThroughput = y.ReadIOThroughput
	x.WriteIOThroughput = y.WriteIOThroughput
	x.Count = y.Count

	return true
}

func (p *PgProcessResourceCollector) getPgProcessResource(pid string) (*PgProcessResource, error) {
	backendType, err := p.getBackendType(pid)
	if err != nil {
		p.logger.Debug("get backend type failed",
			log.String("err", err.Error()), log.String("pid", pid))
		return nil, err
	}

	r := &PgProcessResource{BackendType: backendType}
	if err = p.getCpuInfo(pid, r); err != nil {
		p.logger.Error("get cpu info failed", err, log.String("pid", pid))
		return nil, err
	}

	if err = p.getMemInfo(pid, r); err != nil {
		p.logger.Error("get mem info failed", err, log.String("pid", pid))
		return nil, err
	}

	if err = p.getIoInfo(pid, r); err != nil {
		p.logger.Error("get io info failed", err, log.String("pid", pid))
		return nil, err
	}

	r.Count = p.count

	return r, nil
}

func (p *PgProcessResourceCollector) getBackendType(pid string) (string, error) {
	var f *os.File
	var err error
	var size int

	buf := p.lbuf

	path := "/proc/" + pid + "/cmdline"
	if f, err = os.Open(path); err != nil {
		p.logger.Debug("open proc cmdline failed",
			log.String("error", err.Error()), log.String("path", path))
		return "", err
	}
	defer f.Close()

	if size, err = f.ReadAt(buf, 0); err != nil && err != io.EOF {
		p.logger.Error("read proc cmdline failed", err, log.String("path", path))
		return "", err
	}
	buf = buf[:size]

	backendType, err := p.resolveBackendType(string(buf))
	if err != nil {
		p.logger.Debug("get backend type error",
			log.String("err", err.Error()), log.String("backend type", backendType))
		return "", err
	}

	return backendType, nil
}

func (p *PgProcessResourceCollector) resolveBackendType(cmdline string) (string, error) {
	prefix := p.prefix.FindString(cmdline)
	if prefix != "" {
		proctitle := strings.TrimPrefix(cmdline, prefix)

		for k, v := range BackendTypeMap {
			if strings.HasPrefix(proctitle, k) {
				return v, nil
			}
		}
	}

	if strings.HasPrefix(cmdline, "/u01") || strings.Contains(cmdline, "-D") {
		return "postmaster", nil
	}

	if strings.HasPrefix(cmdline, "postgres") {
		return "backend", nil
	}

	return "", fmt.Errorf("cannot recognize backend type '%s'", cmdline)
}

func (p *PgProcessResourceCollector) getCpuInfo(pid string, r *PgProcessResource) error {
	var f *os.File
	var err error
	var size int

	buf := p.lbuf

	path := "/proc/" + pid + "/stat"
	if f, err = os.Open(path); err != nil {
		p.logger.Error("open proc status failed", err, log.String("path", path))
		return err
	}
	defer f.Close()

	if size, err = f.ReadAt(buf, 0); err != nil && err != io.EOF {
		p.logger.Error("read proc status failed", err, log.String("path", path))
		return err
	}

	fields := bytes.Fields(buf[:size])

	r.State = string(fields[2])
	r.CpuUser, err = strconv.ParseUint(string(fields[13]), 10, 64)
	if err != nil {
		p.logger.Error("parse cpu user failed", err, log.String("cpu user", string(fields[13])))
		return err
	}

	r.CpuSys, err = strconv.ParseUint(string(fields[14]), 10, 64)
	if err != nil {
		p.logger.Error("parse cpu sys failed", err, log.String("cpu sys", string(fields[14])))
		return err
	}

	return nil
}

func (p *PgProcessResourceCollector) getMemInfo(pid string, r *PgProcessResource) error {
	var f *os.File
	var err error
	var size int

	buf := p.lbuf

	path := "/proc/" + pid + "/statm"
	if f, err = os.Open(path); err != nil {
		p.logger.Error("open proc status failed", err, log.String("path", path))
		return err
	}
	defer f.Close()

	if size, err = f.ReadAt(buf, 0); err != nil && err != io.EOF {
		p.logger.Error("read proc status failed", err, log.String("path", path))
		return err
	}

	fields := bytes.Fields(buf[:size])

	rss, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		p.logger.Error("parse mem rss failed", err, log.String("mem rss", string(fields[2])))
		return err
	}

	shared, err := strconv.ParseUint(string(fields[2]), 10, 64)
	if err != nil {
		p.logger.Error("parse mem shared failed", err, log.String("mem shared", string(fields[3])))
		return err
	}

	r.Rss = (rss - shared) * 4096 / 1024 / 1024
	return nil
}

func (p *PgProcessResourceCollector) getIoInfo(pid string, r *PgProcessResource) error {
	var f *os.File
	var err error
	var size int

	buf := p.lbuf

	path := "/proc/" + pid + "/io"
	if f, err = os.Open(path); err != nil {
		p.logger.Error("open proc status failed", err, log.String("path", path))
		return err
	}
	defer f.Close()

	if size, err = f.ReadAt(buf, 0); err != nil && err != io.EOF {
		p.logger.Error("read proc status failed", err, log.String("path", path))
		return err
	}
	buf = buf[:size]

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

		x, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			p.logger.Warn("parse io failed", err,
				log.String("io", string(fields[1])), log.String("line", string(fields[0])))
			continue
		}

		switch string(fields[0]) {
		case "rchar:":
			r.ReadIOThroughput = x / 1024 / 1024
		case "wchar:":
			r.WriteIOThroughput = x / 1024 / 1024
		case "syscr:":
			r.ReadIOCount = x
		case "syscw:":
			r.WriteIOCount = x
		}
	}

	return nil
}

func (p *PgProcessResourceCollector) buildResult(out map[string]interface{}) error {

	cpuCores := p.cpuCoreNumber
	if cpuCores == 0 {
		cpuCores = 1
	}

	p.logger.Debug("process_cpu_info", log.Float64("cpuCores", cpuCores))
	cpuBackends := make(map[string]int)

	for _, v := range p.processes {
		if v.Count != p.count {
			continue
		}

		// 1. summary
		if x, ok := out["procs_cpu_user_sum"]; ok {
			out["procs_cpu_user_sum"] = x.(uint64) + v.CpuUserDelta
		} else {
			out["procs_cpu_user_sum"] = v.CpuUserDelta
		}

		if x, ok := out["procs_cpu_sys_sum"]; ok {
			out["procs_cpu_sys_sum"] = x.(uint64) + v.CpuSysDelta
		} else {
			out["procs_cpu_sys_sum"] = v.CpuSysDelta
		}

		if x, ok := out["procs_mem_rss_sum"]; ok {
			out["procs_mem_rss_sum"] = x.(uint64) + v.Rss
		} else {
			out["procs_mem_rss_sum"] = v.Rss
		}

		if x, ok := out["procs_read_iops_sum"]; ok {
			out["procs_read_iops_sum"] = x.(uint64) + v.ReadIOCountDelta
			out["procs_write_iops_sum"] = x.(uint64) + v.WriteIOCountDelta
			out["procs_read_iothroughput_sum"] = x.(uint64) + v.ReadIOThroughputDelta
			out["procs_write_iothroughput_sum"] = x.(uint64) + v.WriteIOThroughputDelta
		} else {
			out["procs_read_iops_sum"] = v.ReadIOCountDelta
			out["procs_write_iops_sum"] = v.WriteIOCountDelta
			out["procs_read_iothroughput_sum"] = v.ReadIOThroughputDelta
			out["procs_write_iothroughput_sum"] = v.WriteIOThroughputDelta
		}

		// 2. every backend type
		if x, ok := out[v.BackendType+"_cpu"]; ok {
			out[v.BackendType+"_cpu"] = x.(uint64) + v.CpuUserDelta+v.CpuSysDelta
		} else {
			cpuBackends[v.BackendType + "_cpu"] = 1
			out[v.BackendType+"_cpu"] = v.CpuUserDelta+v.CpuSysDelta
		}

		if x, ok := out[v.BackendType+"_mem"]; ok {
			out[v.BackendType+"_mem"] = x.(uint64) + v.Rss
		} else {
			out[v.BackendType+"_mem"] = v.Rss
		}

		if x, ok := out[v.BackendType+"_iops"]; ok {
			out[v.BackendType+"_iops"] = x.(uint64) + v.ReadIOCountDelta + v.WriteIOCountDelta
		} else {
			out[v.BackendType+"_iops"] = v.ReadIOCountDelta + v.WriteIOCountDelta
		}

		if x, ok := out[v.BackendType+"_iothroughput"]; ok {
			out[v.BackendType+"_iothroughput"] =
				x.(uint64) + v.ReadIOThroughputDelta + v.WriteIOThroughputDelta
		} else {
			out[v.BackendType+"_iothroughput"] = v.ReadIOThroughputDelta + v.WriteIOThroughputDelta
		}

	}

	// cpu normalize
	if _, ok := out["procs_cpu_user_sum"]; ok {
		out["procs_cpu_user_sum"] = uint64(float64(out["procs_cpu_user_sum"].(uint64)) / cpuCores)
		out["procs_cpu_sys_sum"] = uint64(float64(out["procs_cpu_sys_sum"].(uint64)) / cpuCores)
	}

	for k := range cpuBackends {
		out[k] = uint64(float64(out[k].(uint64)) / cpuCores)
	}

	return nil
}

func (p *PgProcessResourceCollector) getPostmasterPid() (int64, error) {
	var err error

	postmasterControlData, err := ioutil.ReadFile(p.datadir + "/postmaster.pid")
	if err != nil {
		p.logger.Error("cannot read postmaster pid file", err, log.String("datadir", p.datadir))
		return int64(0), err
	}

	pid := int64(0)

	index := bytes.IndexRune(postmasterControlData, '\n')
	if index <= 0 {
		err := errors.New("postmaster pid file format not right")
		p.logger.Error("postmaster pid file format not right", err)
		return int64(0), err
	}
	pidstr := string(postmasterControlData[:index])
	pid, err = strconv.ParseInt(pidstr, 10, 64)
	if err != nil {
		p.logger.Error("cannot parse postmaster pid", err, log.String("pidstr", pidstr))
		return int64(0), err
	}

	return pid, nil
}

