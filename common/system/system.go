/*-------------------------------------------------------------------------
 *
 * system.go
 *    Read process other info in procfs
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
 *           common/system/system.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ApsaraDB/db-monitor/common/utils"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tklauser/go-sysconf"
)

const (
	invalidPID         = 0xFFFFFFFF
	localAddrIndex     = 1 // pos of localAddr in /proc/net/tcp or /proc/net/tcp6
	inodeIndex         = 9
	tcp4PortOffset     = 9  // pos of inode in /proc/net/tcp
	tcp6PortOffset     = 33 // pos of inode in /proc/net/tcp6
	tcpConnectionFile  = "/proc/net/tcp"
	tcp6ConnectionFile = "/proc/net/tcp6"
)

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func mapFields(s []byte, cb func(interface{}, []byte, int), arg interface{}) {
	i := 0
	fieldStart := 0
	for i < len(s) && asciiSpace[s[i]] != 0 {
		i++
	}
	fieldStart = i
	count := 0
	var buf []byte
	for i < len(s) {
		if asciiSpace[s[i]] == 0 {
			i++
			continue
		}
		buf = s[fieldStart:i:i]
		cb(arg, buf, count)
		count++
		i++
		// Skip spaces in between fields.
		for i < len(s) && asciiSpace[s[i]] != 0 {
			i++
		}
		fieldStart = i
	}
	if fieldStart < len(s) { // Last field might end at EOF.
		cb(arg, s[fieldStart:len(s):len(s)], count)
	}
}

type ProcessStat struct {
	Pid       uint64 // 0
	PPid      uint64
	Comm      string // 1
	Status    byte
	Tgid      uint64 // 4
	MinFault  uint64 // 10
	MajFault  uint64 // 12
	Utime     uint64 // 13
	Stime     uint64 // 14
	Starttime uint64 // 21

	// /proc/pid/cmd
	CmdLine string
	// /proc/pid/exe
	BinName string
}

type MemoryStat struct {
	Size     uint64
	Resident uint64
	Shared   uint64
	Text     uint64
	Data     uint64
	fields   []string
}

type IOStat struct {
	Rchar               uint64
	Wchar               uint64
	Syscr               uint64
	Syscw               uint64
	ReadBytes           uint64
	WriteBytes          uint64
	CancelledWriteBytes uint64
}

// GetHostname return the os hostname of this machine
func GetHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown.host"
	}
	return hostname
}

func GetIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

// GetCPUCount return cpu count
func GetCPUCount() (int64, error) {
	return GetSysconf(sysconf.SC_NPROCESSORS_ONLN)
}

// GetClkTck return the system clock ticks
// this will return the same as C.sysconf(C._SC_CLK_TCK)
func GetClkTck() (int64, error) {
	return GetSysconf(sysconf.SC_CLK_TCK)
}

// GetPageSize return the pagesize on this machine
func GetPageSize() (int64, error) {
	return GetSysconf(sysconf.SC_PAGE_SIZE)
}

// GetSysconf return the sysconf value associated with th scID
func GetSysconf(scID int) (int64, error) {
	return sysconf.Sysconf(scID)
}

// ReadIOStat read statm from /proc/pid/io
func ReadIOStat(path string, buf *bytes.Buffer, stat *IOStat, fp *os.File) (*os.File, error) {
	var err error
	fp, err = ReadFile(path, buf, fp)
	if err != nil {
		return nil, err
	}

	content := buf.Bytes()
	var line []byte
	pos := 0
	var key []byte
	var value []byte
	for {
		pos = bytes.IndexByte(content, '\n')
		if pos < 0 {
			break
		}
		line = content[:pos]
		content = content[pos+1:]
		idx := bytes.IndexByte(line, ':')
		if idx < 0 {
			break
		}
		key = bytes.TrimSpace(line[:idx])
		value = bytes.TrimSpace(line[idx+1:])
		if string(key) == "rchar" {
			stat.Rchar = utils.Atoi(value)
		} else if string(key) == "wchar" {
			stat.Wchar = utils.Atoi(value)
		} else if string(key) == "syscr" {
			stat.Syscr = utils.Atoi(value)
		} else if string(key) == "syscw" {
			stat.Syscw = utils.Atoi(value)
		} else if string(key) == "read_bytes" {
			stat.ReadBytes = utils.Atoi(value)
		} else if string(key) == "write_bytes" {
			stat.WriteBytes = utils.Atoi(value)
		} else if string(key) == "cancelled_write_bytes" {
			stat.CancelledWriteBytes = utils.Atoi(value)
		}
	}
	return fp, nil
}

func readlink(path string, buf []byte) (int, error) {
	n, e := syscall.Readlink(path, buf)
	if e != nil {
		return 0, e
	}
	if n < 0 {
		return 0, fmt.Errorf("real link faild: %s", path)
	}
	return n, nil
}

// GetBinName get binary name, is kernel ""
func GetBinName(path string, buf []byte, stat *ProcessStat) error {
	if stat.Tgid == 0 {
		stat.BinName = ""
		return nil
	}
	n, err := readlink(path, buf)
	if err != nil {
		return err
	}
	tmp := buf[0:n]
	idx := bytes.LastIndexByte(tmp, '/')
	var bin []byte
	if idx >= 0 {
		bin = tmp[idx+1:]
	}
	idx = bytes.Index(bin, []byte(" (deleted)"))
	if idx > 0 {
		bin = bin[:idx]
	}
	stat.BinName = string(bin)
	return nil
}

// GetCmdLine  get cmdline, if kernel cmd
func GetCmdLine(path string, buf *bytes.Buffer, stat *ProcessStat, fp *os.File) (*os.File, error) {
	if stat.Tgid == 0 {
		stat.CmdLine = stat.Comm
		return nil, nil
	}

	if fp != nil {
		_, err := fp.Seek(0, io.SeekStart)
		if err != nil {
			fp.Close()
			fp = nil
		}
	}

	if fp == nil {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		fp = f
	}

	buf.Reset()
	_, err := buf.ReadFrom(fp)
	if err != nil {
		fp.Close()
		return nil, err
	}
	// remove \n
	if buf.Len() > 0 && buf.Bytes()[buf.Len()-1] == 0 {
		buf.Truncate(buf.Len() - 1)
	}

	b := buf.Bytes()
	for i := 0; i < buf.Len(); i++ {
		if b[i] == 0 {
			b[i] = ' '
		}
	}
	b = bytes.TrimSpace(b)
	stat.CmdLine = string(b)
	return fp, nil
}

func getMemInfo(s interface{}, f []byte, idx int) {
	stat, ok := s.(*MemoryStat)
	if !ok {
		return
	}
	// (1) total program size (same as VmSize in /proc/[pid]/status)
	if idx == 0 {
		stat.Size = utils.Atoi(f)
		// (2) resident set size (same as VmRSS in /proc/[pid]/status)
	} else if idx == 1 {
		stat.Resident = utils.Atoi(f)
		// (3) number of resident shared pages (i.e., backed by a file) (same as RssFile+RssShmem in /proc/[pid]/status)
	} else if idx == 2 {
		stat.Shared = utils.Atoi(f)
	}
}

// ReadMemStat read statm from /proc/pid/statm
func ReadMemStat(path string, buf *bytes.Buffer, stat *MemoryStat, fp *os.File) (*os.File, error) {
	var err error
	fp, err = ReadFile(path, buf, fp)
	if err != nil {
		return nil, err
	}
	mapFields(buf.Bytes(), getMemInfo, stat)

	return fp, nil
}

func getProcInfo(s interface{}, f []byte, idx int) {
	stat, ok := s.(*ProcessStat)
	if !ok {
		return
	}
	if idx == 0 {
		stat.Pid = utils.Atoi(f)
	} else if idx == 1 {
		stat.Comm = string(f)
	} else if idx == 2 {
		stat.Status = f[0]
	} else if idx == 3 {
		stat.PPid = utils.Atoi(f)
	} else if idx == 4 {
		stat.Tgid = utils.Atoi(f)
	} else if idx == 10 {
		stat.MinFault = utils.Atoi(f)
	} else if idx == 12 {
		stat.MajFault = utils.Atoi(f)
	} else if idx == 13 {
		stat.Utime = utils.Atoi(f)
	} else if idx == 14 {
		stat.Stime = utils.Atoi(f)
	} else if idx == 21 {
		stat.Starttime = utils.Atoi(f)
	}
}

// ReadProcessStat reads stat from /proc/pid/stat
func ReadProcessStat(path string, buf *bytes.Buffer, stat *ProcessStat, fp *os.File) (*os.File, error) {
	fp, err := ReadFile(path, buf, fp)
	if err != nil {
		return nil, err
	}

	mapFields(buf.Bytes(), getProcInfo, stat)
	return fp, nil
}

func getPID(port int) (uint32, error) {
	pid, err := getPIDFromTCP(port, "tcp")
	if err != nil {
		pid, err = getPIDFromTCP(port, "tcp6")
	}
	return pid, err
}

// version 取值为tcp 或tcp6
func getPIDFromTCP(port int, version string) (uint32, error) {
	var tcpFileName string
	var portOffset, addrLens int
	if version == "tcp" {
		tcpFileName = tcpConnectionFile
		portOffset = 9
		addrLens = 13
	} else if version == "tcp6" {
		tcpFileName = tcp6ConnectionFile
		portOffset = 33
		addrLens = 37
	} else {
		return 0, errors.New("invalid version for tcp")
	}
	tcpBuff, err := readFile(tcpFileName)
	if err != nil {
		return invalidPID, err
	}

	for {
		line, err := tcpBuff.ReadString(0x0A)
		if err != nil {
			if err == io.EOF {
				break
			}
			return invalidPID, err
		}

		//  332: 964CDA0A:C629 944CDA0A:0BFC 01 00000000:00000000 02:00000DFB 00000000   502        0 1438812368 2 ffff8802ba28cb40 20 4 30 1 5
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}

		// 964CDA0A:C629 that is int32:int16
		localAddr := fields[localAddrIndex]
		if len(localAddr) != addrLens {
			continue
		}

		localPort := localAddr[portOffset:]
		if p, err := strconv.ParseInt(localPort, 16, 32); err != nil || int(p) != port {
			continue
		}
		inode := fields[inodeIndex]
		if fdPort, err := findPIDFromProc(inode); err == nil {
			return fdPort, nil
		}
		continue
	}
	return invalidPID, fmt.Errorf("not found pid by port [%d]", port)
}

func getPIDFromPidFile(pidFile string) (uint32, error) {
	if _, err := os.Stat(pidFile); os.IsNotExist(err) {
		return 0, err
	}

	content, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}
	// if got a \n, remove it
	pidStr := strings.TrimSuffix(string(content), "\n")
	pid, err := strconv.Atoi(pidStr)
	return uint32(pid), err
}

func getPIDCmd(port int) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmdStr := fmt.Sprintf("ps -ef | grep %d |grep -v grep", port) //兜底方案，可能会找到错误的pid
	cmd := exec.CommandContext(ctx, "bash", "-c", cmdStr)
	if cmd == nil {
		return 0, errors.New("getPIDCmd: create exec failed, cmd is nil")
	}

	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	cmdBuff := bytes.NewBuffer(out)
	for {
		line, err := cmdBuff.ReadString(0x0A)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		// 1000      62967  53324 35 Sep24 ?        1-07:34:17 /u01/xcluster/bin/mysqld --defaults-file=/home/mysql/data/my.cnf --basedir=/u01/xcluster --datadir=/home/mysql/data/dbs --plugin-dir=/u01/xcluster/lib/plugin --user=mysql --log-error=/home/mysql/log/mysql/master-error.log --open-files-limit=615350 --pid-file=/home/mysql/data/my3000.pid --socket=/home/mysql/data/tmp/mysql.sock --port=3000
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		pid, err := strconv.ParseUint(fields[1], 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(pid), nil
	}
	return 0, errors.New("pid not found by cmd ps")
}

func readFile(filename string) (*bytes.Buffer, error) {
	if fi, err := os.Stat(filename); os.IsNotExist(err) || fi.IsDir() {
		return nil, err
	}

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(content), nil
}

func findPIDFromProc(inode string) (uint32, error) {
	pidFdPathPatten := "/proc/%d/fd/"
	procDirFi, err := ioutil.ReadDir("/proc")
	if err != nil {
		return invalidPID, err
	}

	for _, procFi := range procDirFi {
		pid, err := strconv.ParseUint(procFi.Name(), 10, 32)
		if err != nil {
			continue
		}
		fdPath := fmt.Sprintf(pidFdPathPatten, pid)
		if _, err := os.Stat(fdPath); os.IsNotExist(err) {
			continue
		}

		fdDirFi, err := ioutil.ReadDir(fdPath)
		if err != nil {
			continue
		}

		for _, fdFi := range fdDirFi {
			linkFile := fdPath + fdFi.Name()
			if linkFi, err := os.Lstat(linkFile); err != nil || linkFi.Mode()&os.ModeSymlink == 0 {
				continue
			}

			str, err := os.Readlink(linkFile)
			if err != nil {
				continue
			}
			if !strings.HasPrefix(str, "socket:[") {
				continue
			}

			realNamePatten := "socket:[%s]"
			if fmt.Sprintf(realNamePatten, inode) == str {
				// found
				return uint32(pid), err
			}
		}
	}
	return invalidPID, fmt.Errorf("not found pid by inode %s", inode)
}

// ReadFile reads the filename to buf
func ReadFile(filename string, buf *bytes.Buffer, fp *os.File) (*os.File, error) {
	buf.Reset()
	if fp != nil {
		_, err := fp.Seek(0, io.SeekStart)
		if err != nil {
			fp.Close()
			fp = nil
		}
	}

	if fp == nil {
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		fp = f
	}

	_, err := buf.ReadFrom(fp)
	if err != nil {
		fp.Close()
		return nil, err
	}

	return fp, nil
}
