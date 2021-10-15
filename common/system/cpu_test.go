/*-------------------------------------------------------------------------
 *
 * cpu_test.go
 *    Test case for cpu.go
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
 *           common/system/cpu_test.go
 *-------------------------------------------------------------------------
 */

package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var procNetTcp = ` 465: 964CDA0A:0BBB 81AA650A:5FCE 01 00000000:00000000 02:0000031F 00000000   502        0 1059735793 2 ffff881673e88f80 21 3 8 10 9
 466: 964CDA0A:0BFC 944CDA0A:A237 01 00000000:00000000 02:000012D7 00000000   502        0 1438801452 2 ffff882fa00eeb40 20 3 0 5 3
 634: 0100007F:0BFC 0100007F:A274 01 00000000:00000000 02:000012D5 00000000   502        0 4252142652 2 ffff8821f4174880 21 3 12 10 -1`

//l-wx------ 1 root root 64 Dec 23 06:40 320 -> /u01/dbs3068/clouddata_0157/org_info_1512_0156.ibd
//lrwx------ 1 root root 64 Dec 23 06:40 321 -> /u01/dbs3068/clouddata_0114/org_info_1678.ibd
//lrwx------ 1 root root 64 Dec 23 06:28 322 -> /u01/dbs3068/clouddata_0136/data_temp_0135.ibd
//lrwx------ 1 root root 64 Dec 23 06:40 323 -> socket:[1438801452]
//lrwx------ 1 root root 64 Dec 23 06:40 324 -> socket:[4252142652]
//lrwx------ 1 root root 64 Dec 23 06:28 326 -> /u02/data3068/mysql/slave-relay.003501
//lrwx------ 1 root root 64 Dec 23 06:40 327 -> /u01/dbs3068/mysql/event.MYI
//lrwx------ 1 root root 64 Dec 23 06:28 328 -> /u01/dbs3068/mysql/event.MYD
//lr-x------ 1 root root 64 Dec 23 06:12 329 -> /u02/data3068/mysql/mysql-bin.003483

func setup(t *testing.T) {
	if err := ioutil.WriteFile("tcp", []byte(procNetTcp), 0700); err != nil {
		t.Fail()
	}
}

func teardown() {
	os.Remove("tcp")
}

func TestGetPid(t *testing.T) {
	t.Skip("")
	fmt.Println(GetSysconf(0x1D))
}

// BenchmarkReadConf-8   	100000000	        10.8 ns/op
func BenchmarkReadConf(b *testing.B) {
	b.Skip()
	for i := 0; i < b.N; i++ {
		readConf()
	}
}

func readConf() {
	GetClkTck()
}

func TestGetPIDCmd(t *testing.T) {
	t.Skip()
	pid, err := getPIDCmd(6942)
	assert.Nil(t, err)
	assert.Zero(t, pid)
}

func TestNewCpu(t *testing.T) {
	t.Skip()
	pid, err := GetPid(3000, "asdfa")
	fmt.Println(pid, err)
	fmt.Println(ValidPid(pid, "abcd"))
	var buf bytes.Buffer
	cpu := NewCpu(&buf)
	err = cpu.CpuStatByPid(pid)
	fmt.Println(err)
	fmt.Println(cpu.Stat.Utime, cpu.Stat.Stime)
	mem := NewMem(&buf)
	err = mem.MemStatByPid(pid)
	fmt.Println(err)
	//fmt.Println(mem.Stat.Size,mem.pageSize)
	io := NewIO(&buf)
	fmt.Println(io.IOStatByPid(pid))
	fmt.Println(io.Stat.ReadBytes)

}
