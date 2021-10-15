/*-------------------------------------------------------------------------
 *
 * disk.go
 *    Read process disk info in procfs
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
 *           common/system/disk.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DirSizeExcludeHardLink calculate the file size one by one(hard link only calculate once) and return file count also
func DirSize(path string) (int64, int64, error) {
	var size int64
	var count int64
	inodeMap := make(map[uint64]bool)
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			size += info.Size()
		} else {
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return nil
			}
			if st.Nlink > 1 {
				// 对于硬链接的文件，只算一次
				if !inodeMap[st.Ino] {
					inodeMap[st.Ino] = true
					size += info.Size()
				}
			} else {
				size += info.Size()
			}
		}
		count++
		if count%100000 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		return err
	})
	return size, count, err
}

// DirSize calculate the file size one by one and return file count also
func FilteredDirSize(path, regex string) (int64, int64, error) {
	var size int64
	var count int64
	re, err := regexp.Compile(regex)
	if err != nil {
		return size, count, err
	}

	err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			matched := re.MatchString(info.Name())
			if matched {
				size += info.Size()
				count++
			}
		}
		return err
	})
	return size, count, err
}

func DeletedFilesSize(port int, pidsMap *sync.Map) (int64, int64, uint32, error) {
	var size int64
	var count int64
	var pid1 uint32

	pid, ok := pidsMap.Load(port)
	if !ok {
		newPid, err := getPID(port)

		if err != nil {
			// failed to get pid, try to find pid by netstat, timout 2s
			if newPid, err = getPIDCmd(port); err != nil {
				return 0, 0, 0, err
			}
		}

		pidsMap.Store(port, newPid)
		pid = newPid
	} else {
		cmdlinePath := fmt.Sprintf("/proc/%d/cmdline", pid)
		cmdline, err := ioutil.ReadFile(cmdlinePath)

		if err != nil || (cmdline != nil && !bytes.Contains(cmdline, []byte(strconv.FormatInt(int64(port), 10)))) {
			// double check
			newPid, err := getPID(port)

			if err != nil {
				// failed to get pid, try to find pid by netstat, timout 2s
				if newPid, err = getPIDCmd(port); err != nil {
					return 0, 0, 0, err
				}
			}

			pidsMap.Store(port, newPid)
			pid = newPid

		}
	}

	fdPath := fmt.Sprintf("/proc/%d/fd", pid)

	fdDirFi, err := ioutil.ReadDir(fdPath)
	if err != nil {
		return 0, 0, 0, err
	}

	for _, fdFi := range fdDirFi {
		linkFile := path.Join(fdPath, fdFi.Name())
		if linkFi, err := os.Lstat(linkFile); err != nil || linkFi.Mode()&os.ModeSymlink == 0 {
			continue
		}

		str, err := os.Readlink(linkFile)
		if err != nil {
			continue
		}

		if strings.HasSuffix(str, "(deleted)") {
			fi, err := os.Stat(linkFile)
			if err != nil {
				continue
			}
			size += fi.Size()
			count++
		}
	}

	pid1 = pid.(uint32)
	return size, count, pid1, nil
}
