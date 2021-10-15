/*-------------------------------------------------------------------------
 *
 * disk_test.go
 *    Test case for disk.go
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
 *           common/system/disk_test.go
 *-------------------------------------------------------------------------
 */

package system

import (
	"fmt"
	"sync"
	"testing"
)

func TestDirSize(t *testing.T) {
	fmt.Println(DirSize("/tmp/test"))
}

// size: 9136230,  file count: 1186
// BenchmarkDirSize-8   	      50	  21075372 ns/op
func BenchmarkDirSize(b *testing.B) {
	b.Skip()
	for i := 0; i < b.N; i++ {
		DirSize("../")
	}
}

// BenchmarkFilteredDirSize-8   	      50	  25337499 ns/op
func BenchmarkFilteredDirSize(b *testing.B) {
	b.Skip()
	for i := 0; i < b.N; i++ {
		_, _, _ = FilteredDirSize("../", "[a-zA-Z].*\\.[0-9]{6}")
	}
}

func TestDeletedFilesSize(t *testing.T) {
	t.Skip()
	var m sync.Map
	size, count, pid, err := DeletedFilesSize(3003, &m)
	fmt.Println(size, count, pid, err)
}
