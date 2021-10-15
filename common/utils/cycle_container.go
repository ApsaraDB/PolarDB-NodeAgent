/*-------------------------------------------------------------------------
 *
 * cycle_container.go
 *    RingBuffer
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
 *           common/utils/cycle_container.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"bytes"
	"strconv"
	"sync/atomic"
)

// CycleContainer def
type CycleContainer struct {
	//padding1  [8]uint64 // paddings to ensure each item is on a separate cache line
	cap uint32
	//padding2  [8]uint64
	indexMask uint32
	//padding3  [8]uint64
	readIndex uint32
	//padding4  [8]uint64
	writeIndex uint32
	//padding5  [8]uint64
	buffs []bytes.Buffer
	//padding6  [8]uint64
}

// build a new container with specified cap
func NewCycleContainer(cap uint32) *CycleContainer {
	return &CycleContainer{
		cap:        cap,
		buffs:      make([]bytes.Buffer, cap),
		readIndex:  0,
		writeIndex: 0,
		indexMask:  cap - 1,
	}
}

// get current read buff
func (cc *CycleContainer) GetRead() *bytes.Buffer {
	return &cc.buffs[cc.readIndex&cc.indexMask]
}

// get current read index
func (cc *CycleContainer) GetReadIndex() uint32 {
	return cc.readIndex
}

// set read buff to next
func (cc *CycleContainer) NextReadSlot() {
	atomic.AddUint32(&cc.readIndex, 1)
}

// get current write buff
func (cc *CycleContainer) GetWrite() *bytes.Buffer {
	return &cc.buffs[cc.writeIndex&cc.indexMask]
}

// get current write index
func (cc *CycleContainer) GetWriteIndex() uint32 {
	return cc.writeIndex
}

// set write buff to next
func (cc *CycleContainer) NextWriteSlot() {
	atomic.AddUint32(&cc.writeIndex, 1)
}

// print container info
func (cc *CycleContainer) Info() string {
	var info bytes.Buffer
	info.WriteString("cap:")
	info.WriteString(strconv.FormatUint(uint64(cc.cap), 10))
	info.WriteByte(',')
	info.WriteString("readIndex:")
	info.WriteString(strconv.FormatUint(uint64(cc.readIndex), 10))
	info.WriteByte(',')
	info.WriteString("writeIndex:")
	info.WriteString(strconv.FormatUint(uint64(cc.writeIndex), 10))
	return info.String()
}
