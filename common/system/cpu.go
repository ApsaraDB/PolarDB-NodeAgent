/*-------------------------------------------------------------------------
 *
 * cpu.go
 *    Read process cpu info in procfs
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
 *           common/system/cpu.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
)

type Cpu struct {
	ClkTck   int64
	statPath string
	buf      *bytes.Buffer
	fp       *os.File
	Stat     *ProcessStat
}

func NewCpu(buf *bytes.Buffer) *Cpu {
	return &Cpu{Stat: &ProcessStat{}, buf: buf}
}

func (c *Cpu) Reset() {
	c.statPath = ""
	c.buf.Reset()
	if c.fp != nil {
		c.fp.Close()
		c.fp = nil
	}
}

// CpuStatByPid get process stat by pid
func (c *Cpu) CpuStatByPid(pid uint32) error {
	var err error
	c.buf.Reset()
	// get CLK_TCK at 1st time
	if c.ClkTck <= 0 {
		clkTck, err := GetClkTck()
		if err != nil {
			return err
		}

		c.ClkTck = clkTck
	}
	if c.statPath == "" {
		c.statPath = fmt.Sprintf("/proc/%d/stat", pid)
	}

	c.fp, err = ReadProcessStat(c.statPath, c.buf, c.Stat, c.fp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cpu) ThreadCpuStat(pid uint32) error {
	var err error
	c.buf.Reset()
	// get CLK_TCK at 1st time
	if c.ClkTck <= 0 {
		clkTck, err := GetClkTck()
		if err != nil {
			return err
		}

		c.ClkTck = clkTck
	}
	if c.statPath == "" {
		c.statPath = fmt.Sprintf("/proc/%d/task/%d/stat", pid, pid)
	}

	c.fp, err = ReadProcessStat(c.statPath, c.buf, c.Stat, c.fp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cpu) Close() {
	c.fp.Close()
}

func GetPid(port int, pidFile string) (pid uint32, err error) {
	if pidFile != "" {
		pid, err = getPIDFromPidFile(pidFile)
		if err == nil {
			return
		}
	}

	pid, err = getPID(port)
	if err != nil {
		// failed to get pid, try to find pid by netstat, timout 2s
		pid, err = getPIDCmd(port)
		if err != nil {
			return pid, err
		}
	}
	return pid, nil
}

func ValidPid(pid uint32, pattern string) bool {
	if pattern != "" {
		cmdLineFile := fmt.Sprintf("/proc/%d/cmdline", pid)
		content, err := ioutil.ReadFile(cmdLineFile)
		if err != nil {
			return false
		}
		if !bytes.Contains(content, []byte(pattern)) {
			return false
		}
	}
	return true
}
