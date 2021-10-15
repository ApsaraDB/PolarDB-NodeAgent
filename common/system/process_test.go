/*-------------------------------------------------------------------------
 *
 * system.go
 *    Test for all procfs info reader
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
 *           common/system/process_test.go
 *-------------------------------------------------------------------------
 */
package system

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	t.Skip("")
	var buf bytes.Buffer
	f, err := os.Open("/proc/106397/cmdline")
	if err != nil {
		fmt.Println(err)
	}
	n, err := buf.ReadFrom(f)
	if err != nil {
		fmt.Println(err, n)
	}
	fmt.Println(buf.String())
	fmt.Println("###3last", buf.Bytes()[n-1])
	if buf.Bytes()[buf.Len()-1] == 0 {
		fmt.Println("#####TestRead")
	}

	fmt.Println("second")
	buf.Reset()
	f1, err1 := os.Open("/proc/92389/cmdline")
	if err1 != nil {
		fmt.Println(err1)
	}
	n1, err1 := buf.ReadFrom(f1)
	if err != nil {
		fmt.Println(err1, n1)
	}
	fmt.Println(buf.String())
	fmt.Println("###3last", buf.Bytes()[n1-1])
	if buf.Bytes()[buf.Len()-1] == 0 {
		fmt.Println("#####TestRead")
	}

}

func TestGetPid2(t *testing.T) {
	t.Skip()
	pid, err := getPID(3006)
	fmt.Println(pid, err)
	pid, err = getPIDCmd(3006)
	fmt.Println(pid, err)
}
