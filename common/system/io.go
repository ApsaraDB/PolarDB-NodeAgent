/*-------------------------------------------------------------------------
 *
 * io.go
 *    Read process io info in procfs
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
 *           common/system/io.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"fmt"
	"os"
)

type IO struct {
	statPath string
	buf      *bytes.Buffer
	fp       *os.File
	Stat     *IOStat
}

func NewIO(buf *bytes.Buffer) *IO {
	return &IO{Stat: &IOStat{}, buf: buf}
}

func (i *IO) Reset() {
	i.statPath = ""
	i.buf.Reset()
	if i.fp != nil {
		i.fp.Close()
		i.fp = nil
	}
}

func (i *IO) IOStatByPid(pid uint32) error {
	var err error
	if i.statPath == "" {
		i.statPath = fmt.Sprintf("/proc/%d/io", pid)
	}

	i.fp, err = ReadIOStat(i.statPath, i.buf, i.Stat, i.fp)
	if err != nil {
		return err
	}
	return nil
}

func (i *IO) Close() {
	i.fp.Close()
}
