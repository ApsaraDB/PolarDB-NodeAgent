/*-------------------------------------------------------------------------
 *
 * io.go
 *    Read cgroup io metrics
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
 *           common/cgroup/io.go
 *-------------------------------------------------------------------------
 */
package cgroup

/**
https://help.aliyun.com/document_detail/155474.html?utm_content=g_1000230851&spm=5176.20966629.toubu.3.f2991ddcpxxvD1#title-o7x-mni-aez
*/
import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	sysDevBlockDir                   = "/sys/dev/block"
	blkioGroupDir                    = "/cgroup/rds/rule%d/"
	blkioGroupDirOnECS               = "/sys/fs/cgroup/blkio/"
	blkioThrottleIoServicedName      = "blkio.throttle.io_serviced"
	blkioThrottleIoServicedBytesName = "blkio.throttle.io_service_bytes"
	blkioThrottleReadIOPSDevice      = "blkio.throttle.read_iops_device"
	blkioThrottleWriteIOPSDevice     = "blkio.throttle.write_iops_device"
	blkioThrottleReadBpsDevice       = "blkio.throttle.read_bps_device"
	blkioThrottleWriteBpsDevice      = "blkio.throttle.write_bps_device"
	blkioThrottleIoWaitTime          = "blkio.throttle.io_wait_time"
	blkioThrottleIoServiceTime       = "blkio.throttle.io_service_time"

	mountinfoPath = "/proc/self/mountinfo"

	// on BM
	blkioIoServicedPath      = blkioGroupDir + blkioThrottleIoServicedName
	blkioIoServicedBytesPath = blkioGroupDir + blkioThrottleIoServicedBytesName

	// on ECS
	blkioIoServicedOnECSPath      = blkioGroupDirOnECS + blkioThrottleIoServicedName
	blkioIoServicedBytesOnECSPath = blkioGroupDirOnECS + blkioThrottleIoServicedBytesName
)

type DeviceUEvent struct {
	Major   string
	Minor   string
	DevName string
	DevType string
}

var Devices sync.Map

type IoStat struct {
	DataIo         uint64
	DataWIo        uint64
	DataRIo        uint64
	LogIo          uint64
	LogRIo         uint64
	LogWIo         uint64
	ReadIOPSLimit  uint64
	WriteIOPSLimit uint64
	ReadBpsLimit   uint64
	WriteBpsLimit  uint64
	RWaitTime      uint64
	WWaitTime      uint64
	WaitTime       uint64
	RServiceTime   uint64
	WServiceTime   uint64
	ServiceTime    uint64
}

type Io struct {
	port    int
	isOnEcs bool

	//isK8s          bool
	dataDevId         string
	dataDev           string
	logDevId          string
	logDev            string
	blkioPath         string
	blkioBytesPath    string
	rIOPSLimitPath    string
	wIOPSLimitPath    string
	rBpsLimitPath     string
	wBpsLimitPath     string
	ioWaitTimePath    string
	ioServiceTimePath string

	iostat IoStat
	buf    *bytes.Buffer
}

// NewIo build a new Io object
func NewIo(buf *bytes.Buffer) *Io {
	return &Io{buf: buf}
}

// Init get dev number from mountinfo
func (cio *Io) Init(port int, onEcs bool) error {
	cio.isOnEcs = onEcs
	cio.port = port
	cio.blkioPath = fmt.Sprintf(blkioIoServicedPath, cio.port)
	cio.blkioBytesPath = fmt.Sprintf(blkioIoServicedBytesPath, cio.port)
	return cio.initIo(mountinfoPath)
}

func (cio *Io) InitPath(blkPath string, onEcs bool) error {
	cio.isOnEcs = onEcs
	cio.blkioPath = path.Join(blkPath, blkioThrottleIoServicedName)
	cio.blkioBytesPath = path.Join(blkPath, blkioThrottleIoServicedBytesName)
	cio.rIOPSLimitPath = path.Join(blkPath, blkioThrottleReadIOPSDevice)
	cio.wIOPSLimitPath = path.Join(blkPath, blkioThrottleWriteIOPSDevice)
	cio.rBpsLimitPath = path.Join(blkPath, blkioThrottleReadBpsDevice)
	cio.wBpsLimitPath = path.Join(blkPath, blkioThrottleWriteBpsDevice)
	cio.ioWaitTimePath = path.Join(blkPath, blkioThrottleIoWaitTime)
	cio.ioServiceTimePath = path.Join(blkPath, blkioThrottleIoServiceTime)
	cio.initIo(mountinfoPath)
	return nil
}

func (cio *Io) InitPathWithMp(blkPath string, onEcs bool, dataDev string, logDev string) error {
	cio.isOnEcs = onEcs
	cio.blkioPath = path.Join(blkPath, blkioThrottleIoServicedName)
	cio.blkioBytesPath = path.Join(blkPath, blkioThrottleIoServicedBytesName)
	cio.rIOPSLimitPath = path.Join(blkPath, blkioThrottleReadIOPSDevice)
	cio.wIOPSLimitPath = path.Join(blkPath, blkioThrottleWriteIOPSDevice)
	cio.rBpsLimitPath = path.Join(blkPath, blkioThrottleReadBpsDevice)
	cio.wBpsLimitPath = path.Join(blkPath, blkioThrottleWriteBpsDevice)
	cio.ioWaitTimePath = path.Join(blkPath, blkioThrottleIoWaitTime)
	cio.ioServiceTimePath = path.Join(blkPath, blkioThrottleIoServiceTime)

	cio.dataDev = dataDev
	cio.logDev = logDev
	cio.initIo(mountinfoPath)

	return nil
}

// 26 21 253:0 / /u01 rw,noatime,nodiratime - ext4 /dev/mapper/vgdata-volume1 rw,barrier=0,nodelalloc,stripe=64,data=ordered
// 27 21 253:1 / /u02 rw,noatime,nodiratime - ext4 /dev/mapper/vgdata-volume2 rw,barrier=0,nodelalloc,stripe=64,data=ordered
func (cio *Io) initIo(iopath string) error {

	return cio.scanFile(iopath, func(fields []string) error {
		if len(fields) < 10 {
			return fmt.Errorf("invalid fields from mountinfo: %s", fields)
		}
		if cio.isOnEcs {
			if fields[4] == "/disk1" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
				cio.dataDev = filepath.Base(devField)
			} else if fields[4] == "/datadisk" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
				cio.dataDev = filepath.Base(devField)
			} else if fields[4] == "/u01" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
			} else if fields[4] == "/flash" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
				cio.dataDev = filepath.Base(devField)
			} else {
				devField := fields[len(fields)-2]
				if cio.dataDev != "" && cio.dataDev == devField {
					cio.dataDevId = cio.getDevId(fields[2], devField)
					cio.dataDev = filepath.Base(devField)
				}
			}

			if fields[4] == "/u02" {
				devField := fields[len(fields)-2]
				cio.logDevId = cio.getDevId(fields[2], devField)
			} else {
				devField := fields[len(fields)-2]
				if cio.logDev == devField {
					cio.logDevId = cio.getDevId(fields[2], devField)
					cio.logDev = devField
				}
			}

		} else {
			if fields[4] == "/u01" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
			} else if fields[4] == "/flash" {
				devField := fields[len(fields)-2]
				cio.dataDevId = cio.getDevId(fields[2], devField)
			}
			if fields[4] == "/u02" {
				devField := fields[len(fields)-2]
				cio.logDevId = cio.getDevId(fields[2], devField)
			} else if fields[4] == "/flash" {
				devField := fields[len(fields)-2]
				cio.logDevId = cio.getDevId(fields[2], devField)
			}
		}
		return nil
	})
}

// GetIO return dataIo and logIo count
func (cio *Io) GetIo() (stat *IoStat, err error) {
	return cio.getBlkio(cio.blkioPath)
}

// GetIO return dataIo and logIo bytes count
func (cio *Io) GetIoBytes() (*IoStat, error) {
	return cio.getBlkio(cio.blkioBytesPath)
}

// GetDiskIo get stat from /proc/diskstats
func (cio *Io) GetDiskIo() (uint64, uint64, uint64, uint64, string, string, error) {
	var dataIo, logIo, dataIoBytes, logIoBytes uint64
	var err error
	err = cio.scanFile("/proc/diskstats", func(fields []string) error {
		if fields[2] == cio.dataDev {
			rio, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil {
				return err
			}
			rSec, err := strconv.ParseUint(fields[5], 10, 64)
			if err != nil {
				return err
			}
			wio, err := strconv.ParseUint(fields[7], 10, 64)
			if err != nil {
				return err
			}
			wSec, err := strconv.ParseUint(fields[9], 10, 64)
			if err != nil {
				return err
			}
			dataIo = rio + wio
			dataIoBytes = (rSec + wSec) * 512
		} else if fields[2] == cio.logDev {
			rio, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil {
				return err
			}
			rSec, err := strconv.ParseUint(fields[5], 10, 64)
			if err != nil {
				return err
			}
			wio, err := strconv.ParseUint(fields[7], 10, 64)
			if err != nil {
				return err
			}
			wSec, err := strconv.ParseUint(fields[9], 10, 64)
			if err != nil {
				return err
			}
			logIo = rio + wio
			logIoBytes = (rSec + wSec) * 512
		}
		return err
	})
	return dataIo, dataIoBytes, logIo, logIoBytes, cio.dataDev, cio.dataDevId, err
}

// get throttle iops limit
func (cio *Io) GetIOLimit() (stat *IoStat, err error) {
	if cio.iostat.ReadIOPSLimit, err = cio.readIOLimit(cio.rIOPSLimitPath, cio.dataDevId); err != nil {
		return &cio.iostat, err
	}
	if cio.iostat.WriteIOPSLimit, err = cio.readIOLimit(cio.wIOPSLimitPath, cio.dataDevId); err != nil {
		return &cio.iostat, err
	}
	if cio.iostat.ReadBpsLimit, _ = cio.readIOLimit(cio.rBpsLimitPath, cio.dataDevId); err != nil {
		return &cio.iostat, err
	}
	if cio.iostat.WriteBpsLimit, err = cio.readIOLimit(cio.wBpsLimitPath, cio.dataDevId); err != nil {
		return &cio.iostat, err
	}
	return &cio.iostat, nil
}

func (cio *Io) GetIoWaitTime() (stat *IoStat, err error) {
	var value uint64
	err = cio.scanFile(cio.ioWaitTimePath, func(fields []string) error {
		if fields[0] == cio.dataDevId {
			value, err = strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return err
			}
			if fields[1] == "Read" {
				cio.iostat.RWaitTime = value
			} else if fields[1] == "Write" {
				cio.iostat.WWaitTime = value
			} else if fields[1] == "Total" {
				cio.iostat.WWaitTime = value
			}
		}
		return nil
	})
	return &cio.iostat, err
}

func (cio *Io) GetIoServiceTime() (stat *IoStat, err error) {
	var value uint64
	err = cio.scanFile(cio.ioServiceTimePath, func(fields []string) error {
		if fields[0] == cio.dataDevId {
			value, err = strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return err
			}
			if fields[1] == "Read" {
				cio.iostat.RServiceTime = value
			} else if fields[1] == "Write" {
				cio.iostat.WServiceTime = value
			} else if fields[1] == "Total" {
				cio.iostat.ServiceTime = value
			}
		}
		return nil
	})
	return &cio.iostat, err
}

func (cio *Io) readIOLimit(path, devID string) (value uint64, err error) {
	err = cio.scanFile(path, func(fields []string) error {
		if fields[0] == devID {
			value, err = strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return value, nil
}

// line looks like:
//253:0 Read 37656
//253:0 Write 775896981
//253:0 Sync 0
//253:0 Async 775934637
//253:0 Total 775934637
//Total 16191280374
func (cio *Io) getBlkio(path string) (stat *IoStat, err error) {
	var value uint64
	err = cio.scanFile(path, func(fields []string) error {
		if fields[0] == cio.dataDevId {
			value, err = strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return err
			}
			if fields[1] == "Read" {
				cio.iostat.DataRIo = value
			} else if fields[1] == "Write" {
				cio.iostat.DataWIo = value
			} else if fields[1] == "Total" {
				cio.iostat.DataIo = value
			}
		}
		// logIo always be 0 if on ecs
		if fields[0] == cio.logDevId {
			value, err = strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return err
			}
			if fields[1] == "Read" {
				cio.iostat.LogRIo = value
			} else if fields[1] == "Write" {
				cio.iostat.LogWIo = value
			} else if fields[1] == "Total" {
				cio.iostat.LogIo = value
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &cio.iostat, nil
}

func (cio *Io) scanFile(path string, fn func([]string) error) error {
	err := cio.readFile(path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(cio.buf.Bytes()))
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

func (cio *Io) readFile(filename string) error {
	cio.buf.Reset()
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var n int64 = bytes.MinRead
	if fi, err := f.Stat(); err == nil {
		if size := fi.Size() + bytes.MinRead; size > n {
			n = size
		}
	}
	return cio.readAll(f, n)
}

func (cio *Io) readAll(r io.Reader, capacity int64) error {
	var err error
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok {
			err = panicErr
		} else {
			err = fmt.Errorf("read file panic: %d", cio.port)
		}
	}()

	if int64(int(capacity)) == capacity {
		cio.buf.Grow(int(capacity))
	}
	_, err = cio.buf.ReadFrom(r)
	return err
}

func (cio *Io) getDevId(devNo string, devName string) string {

	if due, ok := Devices.Load(devNo); ok {
		due1 := due.(*DeviceUEvent)
		if due1.DevType == "disk" {
			return devNo
		}
	}
	var res string
	Devices.Range(func(key, value interface{}) bool {
		if due, ok := value.(*DeviceUEvent); ok {
			if strings.Contains(devName, due.DevName) && due.DevType == "disk" {
				res = due.Major + ":" + due.Minor
				return false
			}
		}
		return true
	})

	// 如果获取不到设备名，重新更新下设备信息,重新查找
	if res == "" {
		updateDeviceInfo()
		Devices.Range(func(key, value interface{}) bool {
			if due, ok := value.(*DeviceUEvent); ok {
				if strings.Contains(devName, due.DevName) && due.DevType == "disk" {
					res = due.Major + ":" + due.Minor
					return false
				}
			}
			return true
		})
	}
	return res
}

func init() {
	updateDeviceInfo()
}

func updateDeviceInfo() {
	devices, err := ioutil.ReadDir(sysDevBlockDir)
	if err != nil {
		return
	}
	for _, device := range devices {
		due, err := getUEvent(filepath.Join(sysDevBlockDir, device.Name(), "uevent"))
		if err != nil {
			continue
		}
		Devices.Store(device.Name(), due)
	}
	return
}

func getUEvent(path string) (*DeviceUEvent, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	due := &DeviceUEvent{}
	for {
		index := bytes.IndexRune(content, '\n')
		if index < 0 {
			break
		}
		line := string(content[:index])
		content = content[index+1:]
		fields := strings.Split(line, "=")
		if len(fields) == 2 {
			switch fields[0] {
			case "MAJOR":
				due.Major = fields[1]
			case "MINOR":
				due.Minor = fields[1]
			case "DEVNAME":
				due.DevName = fields[1]
			case "DEVTYPE":
				due.DevType = fields[1]
			}
		}
	}
	return due, nil
}
