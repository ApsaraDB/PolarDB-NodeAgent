/*-------------------------------------------------------------------------
 *
 * sar_test.go
 *    Test case for sar.go
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
 *           plugins/sar/collector/sar_test.go
 *-------------------------------------------------------------------------
 */
package collector

import (
	"bytes"
	"fmt"
	"github.com/ApsaraDB/db-monitor/common/consts"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemStatNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	m := memStatNew()
	buf := make([]byte, 16*1024)
	err := m.collect(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestCPUStatNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	m := cpuStatNew()
	buf := make([]byte, 16*1024)
	bytesBuf := &bytes.Buffer{}
	err := m.collect(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("################")
	err = m.collect(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestLoadAvgNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	l := loadAvgNew()
	buf := make([]byte, 16*1024)
	err := l.collect(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestDiskStatNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	d := diskStatNew()
	buf := make([]byte, 16*1024)
	bytesBuf := &bytes.Buffer{}
	err := d.collect(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("#############")
	err = d.collect(out, buf, bytesBuf)
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestFSStatNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	d := fSStatNew()
	buf := make([]byte, 16*1024)
	bytesBuf := &bytes.Buffer{}
	err := d.collect(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
	time.Sleep(5 * time.Second)
	fmt.Println("--------")
	err = d.collect(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestNetDevStatNew(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	//
	buf := make([]byte, 16*1024)
	n := new(sockStat)
	err := n.collectNetSockStat(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
	err = n.collectNetSockStat(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestNetExt_collectNetStat(t *testing.T) {
	//t.Skip()
	out := make(map[string]interface{})
	n := netExtStatNew()
	buf := make([]byte, 16*1024)
	err := n.collectNetStat(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("---------")
	err = n.collectNetStat(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestNetDev_collectNetDev(t *testing.T) {
	t.Skip()
	out := make(map[string]interface{})
	buf := make([]byte, 16*1024)
	dev := netDevStatNew()
	bytesBuf := &bytes.Buffer{}
	err := dev.collectNetDev(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("-------")
	time.Sleep(1 * time.Second)
	err = dev.collectNetDev(out, buf, bytesBuf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestSnmp_collect(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	buf := make([]byte, 16*1024)
	ns := &netSnmp{}
	err := ns.collect(out, buf)
	fmt.Println(err)
	time.Sleep(1 * time.Second)
	err = ns.collect(out, buf)
	fmt.Println(err)
	for k, v := range out {
		fmt.Println(k, v)
	}

}

func TestVmStat_collect(t *testing.T) {
	t.Skip("")
	out := make(map[string]interface{})
	v := vmStatNew()
	buf := make([]byte, 16*1024)
	err := v.collect(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("######")
	time.Sleep(1 * time.Second)
	err = v.collect(out, buf)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestProcsShortedCmdLine(t *testing.T) {
	t.Skip("")
	p := &procInfo{}
	p.cpu.BinName = "python"
	p.cpu.CmdLine = "/usr/local/rds/agent-dbDaemon/0.0.12/bin/python2.7 /usr/local/rds/agent-dbDaemon/0.0.12/bin/adb_dbDaemon"
	fmt.Println(p.shortenCmdline())
}

func TestNvmeStatCollect(t *testing.T) {
	t.Skip()
	n := nvmeStatNew()
	out := make(map[string]interface{})
	buf := make([]byte, 131072)
	buf2 := &bytes.Buffer{}
	err := n.collect(out, buf, buf2)
	fmt.Println("########3err", err)
	time.Sleep(1)
	err = n.collect(out, buf, buf2)
	fmt.Println(err)
	for k, v := range out {
		fmt.Println("####", k, v)
	}
}

func TestNvmeQueueCollect(t *testing.T) {
	t.Skip("")
	n := nvmeQueueStatNew()
	out := make(map[string]interface{})
	buf := make([]byte, 4096)
	buf2 := &bytes.Buffer{}
	err := n.collect(out, buf, buf2)
	fmt.Println("########3", err)
	time.Sleep(1)
	err = n.collect(out, buf, buf2)
	fmt.Println(err)
	for k, v := range out {
		fmt.Println("####", k, v)
	}
}

func TestGetDebugFSMountPoint(t *testing.T) {
	t.Skip("")
	buf := make([]byte, 10*1024)
	mp, err := getDebugfsMountPoint(buf)
	fmt.Println(mp, err)
}

func TestSNMP(t *testing.T) {
	t.Skip()
	//s:=netSnmpNew()
	out := make(map[string]interface{}, 100)
	buf := make([]byte, 5096)
	//err:=s.collect(out,buf,&buf2)
	//fmt.Println(err)
	//for k,v:=range out{
	//	fmt.Println(k,v)
	//}
	l := loadAvgNew()
	err := l.collect(out, buf)
	fmt.Println(err)
	for k, v := range out {
		fmt.Println(k, v)
	}
}

func TestRead(t *testing.T) {
	t.Skip("")
	buf := make([]byte, 131072)
	file, err := os.Open(consts.ProcNvmeStatFile)
	if err != nil {
		fmt.Println(err)
	}
	num, err := file.Read(buf)
	fmt.Println(len(buf), num, err)
	fmt.Println("----------first-------", string(buf))
	num, err = file.Read(buf)
	fmt.Println(len(buf), num, err)

	fmt.Println("----------se-------", string(buf))
	data, err := ioutil.ReadFile(consts.ProcNvmeStatFile)
	fmt.Println(len(data), err)
}

func TestReadLink(t *testing.T) {
	dev := "/dev/vda"
	target, err := os.Readlink(dev)
	fmt.Println(target, err)
	fmt.Println(filepath.Dir(dev), filepath.Base(target))
	if strings.HasPrefix(target, "../") {
		//fmt.Println(file)
		target = filepath.Join(filepath.Dir(dev), target)
	}
	fmt.Println(target)
	//fmt.Println(filepath.Join(dev, target))
	p := &procInfo{}
	fmt.Println(p.cpu.BinName, p.cpu.CmdLine, p.cpu.Comm)
}
