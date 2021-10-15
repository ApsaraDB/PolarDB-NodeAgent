/*-------------------------------------------------------------------------
 *
 * cycle_container_test.go
 *    Test case for RingBuffer
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
 *           common/utils/cycle_container_test.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkPadding-8   	100000000	        11.1 ns/op
func BenchmarkPadding(b *testing.B) {
	cc := NewCycleContainer(2)
	for i := 0; i < b.N; i++ {
		cc.GetRead()
		cc.NextReadSlot()
	}
}

func TestCycleContainer_Get(t *testing.T) {
	cc := NewCycleContainer(2)
	cc.GetWrite().WriteByte(3)
	cc.NextWriteSlot()

	v := cc.GetRead().Bytes()
	cc.GetRead().Reset()
	cc.NextReadSlot()

	cc.GetWrite().WriteByte(5)

	assert.Equal(t, uint8(3), v[0])
	assert.Equal(t, uint8(5), cc.GetRead().Bytes()[0])
}

func TestCycleContainer_Write(t *testing.T) {
	cc := NewCycleContainer(2)
	w := cc.GetWrite()

	w.WriteByte(3)
	t.Log(cc.GetRead().Bytes())

	cc.NextWriteSlot()
	w.WriteByte(6)
	t.Log(cc.GetRead().Bytes())
}

func TestGetInsName(t *testing.T) {
	var buf bytes.Buffer
	insname, err := GetInsName("/tmp", &buf)
	fmt.Println(insname, err)
}

func TestGetUserName(t *testing.T) {
	var buf bytes.Buffer
	insname, err := GetUserName("/tmp", &buf)
	fmt.Println(insname, err)
}
