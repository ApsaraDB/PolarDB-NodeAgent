/*-------------------------------------------------------------------------
 *
 * mem.go
 *    Read process mem info in procfs
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
 *           common/system/mem.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"fmt"
	"os"
)

// Mem the memory size in bytes
type Mem struct {
	statPath string
	PageSize uint64
	buf      *bytes.Buffer
	fp       *os.File
	Stat     *MemoryStat
}

func NewMem(buf *bytes.Buffer) *Mem {
	return &Mem{Stat: &MemoryStat{}, buf: buf}
}

func (m *Mem) Reset() {
	m.statPath = ""
	m.buf.Reset()
	if m.fp != nil {
		m.fp.Close()
		m.fp = nil
	}
}

func (m *Mem) MemStatByPid(pid uint32) error {
	var err error
	// get pagesize at the 1st time
	if m.PageSize <= 0 {
		pgSize, err := GetPageSize()

		if err != nil {
			return err
		}
		m.PageSize = uint64(pgSize)
	}

	if m.statPath == "" {
		m.statPath = fmt.Sprintf("/proc/%d/statm", pid)
	}

	m.fp, err = ReadMemStat(m.statPath, m.buf, m.Stat, m.fp)
	if err != nil {
		return err
	}
	return nil
}

func (m *Mem) Close() {
	m.fp.Close()
}
