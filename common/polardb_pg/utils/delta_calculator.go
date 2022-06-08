/*-------------------------------------------------------------------------
 *
 * delta_calculator.go
 *    delta metric calculator
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
 *           common/polardb_pg/utils/delta_calculator.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"sync"
	"time"
)

type PreValue struct {
	LastTime int64
	Value    float64
}

type DeltaCalculator struct {
	preValueMap map[string]PreValue
	mutex       *sync.Mutex
}

func NewDeltaCalculator() *DeltaCalculator {
	return &DeltaCalculator{
		mutex:       &sync.Mutex{},
		preValueMap: make(map[string]PreValue),
	}
}

func (c *DeltaCalculator) Init() error {
	return nil
}

func (c *DeltaCalculator) Stop() error {
	return nil
}

func (c *DeltaCalculator) CalRateData(deltaname string, out map[string]interface{}, value float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	timestamp := time.Now().Unix()

	if orgvalue, ok := c.preValueMap[deltaname]; ok {
		if value >= orgvalue.Value && timestamp > orgvalue.LastTime {
			out[deltaname] = (value - orgvalue.Value) / float64(timestamp-orgvalue.LastTime)
		}
	}
	c.preValueMap[deltaname] = PreValue{LastTime: timestamp, Value: value}
}

func (c *DeltaCalculator) CalRateDataWithNano(deltaname string, out map[string]interface{}, value float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	timestamp := time.Now().UnixNano()

	if orgvalue, ok := c.preValueMap[deltaname]; ok {
		if value >= orgvalue.Value && timestamp > orgvalue.LastTime {
			out[deltaname] = (value - orgvalue.Value) / float64(timestamp-orgvalue.LastTime)
		}
	}
	c.preValueMap[deltaname] = PreValue{LastTime: timestamp, Value: value}
}

func (c *DeltaCalculator) CalDeltaData(deltaname string, out map[string]interface{}, value float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if orgvalue, ok := c.preValueMap[deltaname]; ok {
		if value >= orgvalue.Value {
			out[deltaname] = (value - orgvalue.Value)
		}
	}
	c.preValueMap[deltaname] = PreValue{Value: value}
}
