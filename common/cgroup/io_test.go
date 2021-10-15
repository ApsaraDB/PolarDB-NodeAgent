/*-------------------------------------------------------------------------
 *
 * io_test.go
 *    test cgroup io metrics
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
 *           common/cgroup/io_test.go
 *-------------------------------------------------------------------------
 */
package cgroup

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func setup(t *testing.T) {
	ioServiced := `252:64 Read 10698689
252:64 Write 3868807128
252:64 Sync 0
252:64 Async 3879505817
252:64 Total 3879505817
253:1 Read 21366158
253:1 Write 7304062679
253:1 Sync 0
253:1 Async 7325428837
253:1 Total 7325428837
252:0 Read 10705125
252:0 Write 4211279610
252:0 Sync 0
252:0 Async 4221984735
252:0 Total 4221984735
253:0 Read 37656
253:0 Write 776025055
253:0 Sync 0
253:0 Async 776062711
253:0 Total 776062711
Total 16202982100`
	mountinfo := `26 21 253:0 / /u01 rw,noatime,nodiratime - ext4 /dev/mapper/vgdata-volume1 rw,barrier=0,nodelalloc,stripe=64,data=ordered
27 21 253:1 / /u02 rw,noatime,nodiratime - ext4 /dev/mapper/vgdata-volume2 rw,barrier=0,nodelalloc,stripe=64,data=ordered`
	f, err := os.Create("blkio.test")
	if err != nil {
		t.Fatal("file create fail")
		return
	}
	_, err = io.WriteString(f, ioServiced)
	if err != nil {
		t.Fatal("write error", err)
		return
	}

	ioutil.WriteFile("mountinfo.test", []byte(mountinfo), 777)
}

func teardown() {

	os.Remove("blkio.test")
	os.Remove("mountinfo.test")
}

func TestGetIoFound(t *testing.T) {
	//t.Skip("")
	//setup(t)
	//defer teardown()
	var buf bytes.Buffer
	cio := NewIo(&buf)
	if err := cio.initIo("/cgroup/rds/rule3003/blkio.throttle.io_serviced"); err != nil {
		t.Fatal(err.Error())
	}

	path := "blkio.test"
	path = "/cgroup/rds/rule3003/blkio.throttle.io_serviced"
	dataIo, err := cio.getBlkio(path)
	fmt.Println(err)
	fmt.Printf("%+v\n", dataIo)
	//assert.Nil(t, err)
	//assert.Equal(t, uint64(776062711), dataIo)
	//assert.Equal(t, uint64(7325428837), logIo)

}

func TestGetIO(t *testing.T) {
	//t.Skip()
	//t.Skip("")
	var buf bytes.Buffer
	cio := NewIo(&buf)
	err := cio.InitPath("/sys/fs/cgroup/blkio/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podc6c5c7c9_4e76_11ea_9283_00163e0a97ec.slice/docker-e282b4ececbfd0492d2e1d45317a2f88251168046066b6f8ddba08cdb2f5e618.scope", true)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println("dev",cio.dataDevId,cio.dataDev,cio.logDevId,cio.logDev)
	//data,log,err:=cio.GetIo()
	//fmt.Println(data,log,err)
}

func TestGetIO1(t *testing.T) {
	t.Skip()
	var buf bytes.Buffer
	cio := NewIo(&buf)
	err := cio.InitPathWithMp("/sys/fs/cgroup/blkio/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod40b29278_8b30_4990_9409_978ddcc52362.slice/docker-da82ea78f35b6a92379f96ae9a6d1adb38b488821fac19aa6c61c28879d90982.scope", true, "/var/lib/kubelet/pods/40b29278-8b30-4990-9409-978ddcc52362/volumes/kubernetes.io~csi/xdbcloudautotestvxbg52a167-e1f1e8ab-log-7e7ce883/mount", "/var/lib/kubelet/pods/40b29278-8b30-4990-9409-978ddcc52362/volumes/kubernetes.io~csi/xdbcloudautotestvxbg52a167-e1f1e8ab-log-7e7ce883/mount")
	fmt.Println(err)
	fmt.Println(cio.logDev, cio.dataDev, cio.logDevId, cio.dataDevId)
}
