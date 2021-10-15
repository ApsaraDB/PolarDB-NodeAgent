/*-------------------------------------------------------------------------
 *
 * utils.go
 *    Utils functions
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
 *           common/utils/utils.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ApsaraDB/db-monitor/common/consts"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gofrs/flock"
)

// lockedFiles stores all *Flock in this single process. for now, only one flock which provided by main
var lockedFiles map[string]*flock.Flock

func init() {
	lockedFiles = make(map[string]*flock.Flock)
}

func ReadJsonConf(filename string, conf interface{}) error {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	if len(content) != 0 {
		err = json.Unmarshal([]byte(content), conf)
		if err != nil {
			return err
		}
	}
	return nil
}

// LockFile lock the specified file, return true if locked ok, otherwise false
func LockFile(filePath string) (bool, error) {
	fileLock := flock.New(filePath)

	ok, err := fileLock.TryLock()
	if ok {
		lockedFiles[filePath] = fileLock
	}
	return ok, err
}

// UnlockFile unlock the specified file, return err if unlock failed
func UnlockFile(filePath string) error {
	fileLock := lockedFiles[filePath]
	if fileLock.Locked() {
		return fileLock.Unlock()
	}
	return nil
}

// ToUint64 convert str to uint64 on base 10
func ToUint64(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer f.Close()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// get instance name from the file /home/mysql/dataXXXX/InsName
func GetInsName(insPath string, buf *bytes.Buffer) (string, error) {
	fp, err := os.Open(filepath.Join(insPath, "insname"))
	if err != nil {
		return "", err
	}
	defer fp.Close()
	buf.Reset()
	_, err = buf.ReadFrom(fp)
	if err != nil {
		return "", err
	}
	// if got a \n, remove it
	name := strings.TrimSuffix(string(buf.String()), "\n")
	return name, nil
}

func GetUserName(insPath string, buf *bytes.Buffer) (string, error) {
	fp, err := os.Open(filepath.Join(insPath, "insinfo"))
	if err != nil {
		return "", err
	}
	defer fp.Close()
	buf.Reset()
	_, err = buf.ReadFrom(fp)
	if err != nil {
		return "", err
	}
	content := buf.Bytes()
	for {
		index := bytes.IndexRune(content, '\n')
		if index <= 0 {
			break
		}
		kv := bytes.FieldsFunc(content[:index], func(r rune) bool {
			return r == '='
		})
		content = content[index+1:]
		if len(kv) != 2 {
			continue
		}
		name := strings.TrimSpace(string(kv[0]))
		if name == "user" || name == "account" {
			return strings.TrimSpace(string(kv[1])), nil
		}
	}

	return "root", nil
}

func Minus(current, origin uint64) uint64 {
	if current > origin {
		return current - origin
	}
	return 0
}

func AddNonNegativeValue(out map[string]interface{}, metric string, value int64) {
	if value >= 0 {
		out[metric] = strconv.FormatInt(value, 10)
	} else {
		//TODO only add non negative value
		out[metric] = ""
	}
}

func Atoi(buf []byte) uint64 {
	var x uint64
	for _, c := range buf {
		x = x*10 + uint64(c-'0')
	}
	return x
}

func PortInUse(port int) (bool, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil || conn == nil {
		return false, fmt.Errorf("dial to db port failed,err:%+v", err)
	}
	conn.Close()
	return true, nil
}

func GetMountDev(path string) string {
	file, err := os.Open(consts.ProcMountInfo)
	if err != nil {
		return ""
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if fields[4] != "/" && fields[4] != "/var/lib/kubelet" && strings.Contains(path, fields[4]) {
			return fields[len(fields)-2]
		}
	}
	return ""
}

func GetBasePath() string {
    ex, err := os.Executable()
    if err != nil {
        panic(err)
    }

    return filepath.Dir(filepath.Clean(ex + "/../"))
}

