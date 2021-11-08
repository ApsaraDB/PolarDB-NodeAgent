/*-------------------------------------------------------------------------
 *
 * schema.go
 *    Metrics schema
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
 *           internal/gather/schema.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/fnv"
	"net"
	"os"
	"strconv"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
)

const (
	// IntType for schema
	IntType = 0
	// FloatType for schema
	FloatType = 1
	// StringType for schema
	StringType = 2
	// MapKeyType from schema
	MapKeyType = 3
	// ListType from schema
	ListType = 4
	// CPUTopType from schema
	CPUTopType = 5
	// IOTopType from schema
	IOTopType = 6
	// MemTopType from schema
	MemTopType = 7
	// DStatusType from schema
	DStatusType = 8
	// mapType for schema
	MapType = 9
	// map with Int as key and double as value
	IntDoubleMap = 10
)

// SchemaItem for the schema
type SchemaItem struct {
	Index     int
	ValueType int
	Name      string
}

// SchemaItems set of schema
type SchemaItems struct {
	SchemaItems []SchemaItem
}

// BusinessSchema for business
type BusinessSchema struct {
	Version uint32
	Headers SchemaItems
	Metrics SchemaItems
}

// Schema entry
type Schema struct {
	BizSchemaMap map[string]BusinessSchema
}

// SchemaItemDesc schema json definition begin
type SchemaItemDesc struct {
	Index int    `json:"index"`
	Type  string `json:"type"`
	Name  string `json:"name"`
}

// BusinessSchemaDesc schema json definition begin
type BusinessSchemaDesc struct {
	Headers []SchemaItemDesc `json:"headers"`
	Metrics []SchemaItemDesc `json:"metrics"`
}

// schema json definition end
//type SchemaDesc struct {
//	Schema BusinessSchemaDesc `json:"schema"`
//}

//func AddSchema(s *Schema, Name string, ms BusinessSchema) {
//	s.Metrics[Name] = ms
//}

// JSON2Schema parser json schema
func JSON2Schema(j []byte, buf *bytes.Buffer) (*BusinessSchema, error) {
	var bs BusinessSchema
	var bsDesc BusinessSchemaDesc
	if err := json.Unmarshal(j, &bsDesc); err != nil {
		return nil, err
	}

	var m SchemaItem
	bs.Metrics.SchemaItems = make([]SchemaItem, len(bsDesc.Metrics))
	bs.Headers.SchemaItems = make([]SchemaItem, len(bsDesc.Headers))

	// body
	for _, v := range bsDesc.Metrics {
		if err := toMetricsType(&m, v); err != nil {
			return nil, err
		}

		bs.Metrics.SchemaItems[v.Index] = m
	}

	// header
	for _, v := range bsDesc.Headers {
		if err := toMetricsType(&m, v); err != nil {
			return nil, err
		}
		bs.Headers.SchemaItems[v.Index] = m
	}
	buf.Reset()
	if err := json.Compact(buf, j); err != nil {
		return nil, err
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	bs.Version = h.Sum32()
	return &bs, nil
}

func toMetricsType(m *SchemaItem, v SchemaItemDesc) error {
	m.Index = v.Index
	if v.Type == consts.SchemaTypeString {
		m.ValueType = StringType
	} else if v.Type == consts.SchemaTypeInt {
		m.ValueType = IntType
	} else if v.Type == consts.SchemaTypeFloat {
		m.ValueType = FloatType
	} else if v.Type == consts.SchemaTypeMapKey {
		m.ValueType = MapKeyType
	} else if v.Type == consts.SchemaTypeList {
		m.ValueType = ListType
	} else if v.Type == consts.SchemaTypeTopCPU {
		m.ValueType = CPUTopType
	} else if v.Type == consts.SchemaTypeTopIO {
		m.ValueType = IOTopType
	} else if v.Type == consts.SchemaTypeTopMem {
		m.ValueType = MemTopType
	} else if v.Type == consts.SchemaTypeDStatus {
		m.ValueType = DStatusType
	} else if v.Type == consts.SchemeTypeMap {
		m.ValueType = MapType
	} else if v.Type == consts.SchemaTypeIntDoubleMap {
		m.ValueType = IntDoubleMap
	} else {
		return errors.New("schema item is invalid: " + v.Name)
	}
	m.Name = v.Name
	return nil
}

func generateFixedKeySegment() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "null;127.0.0.1"
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return hostname + ";127.0.0.1"
	}
	return hostname + ";" + addrs[0].String()
}

func generateFixedMessageHeaderEntity(bs *BusinessSchema, dbtype, business string, headerMap map[string]interface{}, ins *Instance) {
	headerMap[consts.SchemaHeaderVersion] = strconv.FormatUint(uint64(bs.Version), 10)
	headerMap[consts.SchemaHeaderDbType] = dbtype
	headerMap[consts.SchemaHeaderBusiness] = business

	hostname, err := os.Hostname()
	if err != nil {
		headerMap[consts.SchemaHeaderHostname] = "null"
		headerMap[consts.SchemaHeaderIp] = "127.0.0.1"
		return
	}
	headerMap[consts.SchemaHeaderHostname] = hostname
	addrs, err := net.LookupIP(hostname)
	if err != nil {
		headerMap[consts.SchemaHeaderIp] = "127.0.0.1"
		return
	}
	headerMap[consts.SchemaHeaderIp] = addrs[0].String()
	headerMap[consts.SchemaHeaderPort] = strconv.FormatUint(uint64(ins.port), 10)
}

func generateFixedMessageHeaderEntityForMulti(bs *BusinessSchema, dbtype, business string, headerMap map[string]interface{}, ins *Instance) {
	headerMap[consts.SchemaHeaderVersion] = strconv.FormatUint(uint64(bs.Version), 10)
	headerMap[consts.SchemaHeaderDbType] = dbtype
	headerMap[consts.SchemaHeaderBusiness] = business

	hostname, err := os.Hostname()
	if err != nil {
		headerMap[consts.SchemaHeaderHostname] = "null"
		headerMap[consts.SchemaHeaderIp] = "127.0.0.1"
		return
	}
	headerMap[consts.SchemaHeaderHostname] = hostname
	addrs, err := net.LookupIP(hostname)
	if err != nil {
		headerMap[consts.SchemaHeaderIp] = "127.0.0.1"
		return
	}
	headerMap[consts.SchemaHeaderIp] = addrs[0].String()
	//headerMap[consts.SchemaHeaderPort] = strconv.FormatUint(uint64(ins.port), 10)
}

func generateMutableMessageHeaderEntity(collectStartTime int64, endTime int64, elapsedTimeWithin int64, headerMap map[string]interface{}) {
	headerMap[consts.SchemaHeaderTime] = strconv.FormatUint(uint64(collectStartTime), 10)
	headerMap[consts.SchemaHeaderCollectTime] = strconv.FormatUint(uint64(endTime), 10)
	headerMap[consts.SchemaHeaderInterval] = strconv.FormatUint(uint64(elapsedTimeWithin), 10)
}

func generateMutableMessageHeaderEntityForMulti(collectStartTime int64, endTime int64, elapsedTimeWithin int64, headerMap map[string]interface{}) {
	//headerMap[consts.SchemaHeaderTime] = strconv.FormatUint(uint64(collectStartTime), 10)
	headerMap[consts.SchemaHeaderCollectTime] = strconv.FormatUint(uint64(endTime), 10)
	headerMap[consts.SchemaHeaderInterval] = strconv.FormatUint(uint64(elapsedTimeWithin), 10)
}

// MessageHeaderSerializeBySchema method
func MessageHeaderSerializeBySchema(schemaHeader *SchemaItems, ins *Instance, headerMap map[string]interface{}) error {
	buf := &ins.msgBuff
	for _, m := range schemaHeader.SchemaItems {
		v, ok := headerMap[m.Name]
		if ok {
			buf.WriteString(v.(string))
		}
		if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
			return err
		}
	}
	return nil
}

// DefaultMessageSerializeBySchema method
func DefaultMessageSerializeBySchema(ms *SchemaItems, content map[string]interface{}, buf *bytes.Buffer) error {
	metricsCount := len(ms.SchemaItems)
	for i, m := range ms.SchemaItems {
		v, ok := content[m.Name]
		if ok {
			switch v.(type) {
			case string:
				buf.WriteString(v.(string))
			case []string:
				OnlyArrayMessageSerialize(v.([]string), buf)
			default:
			}
		}

		if i < metricsCount-1 {
			if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnlyArrayMessageSerialize method
func OnlyArrayMessageSerialize(v []string, buf *bytes.Buffer) error {
	l := len(v)
	for idx := 0; idx < l; idx++ {

		if _, err := buf.WriteString(v[idx]); err != nil {
			return err
		}

		if idx < l-1 {
			if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnlyIntMessageSerializeBySchema method
func OnlyIntMessageSerializeBySchema(ms *SchemaItems, content map[string]uint64, buf *bytes.Buffer) error {
	metricsCount := len(ms.SchemaItems)
	for i, m := range ms.SchemaItems {
		v, ok := content[m.Name]
		if ok {
			buf.WriteString(strconv.FormatUint(v, 10))
		}

		if i < metricsCount-1 {
			if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnlyIntArrayMessageSerialize method
func OnlyIntArrayMessageSerialize(ms *SchemaItems, values map[string][]int, buf *bytes.Buffer) error {
	metricsCount := len(ms.SchemaItems)
	for i, m := range ms.SchemaItems {
		v, ok := values[m.Name]
		if !ok {
			err := errors.New("schema Metrics [" + m.Name + "] not exists")
			return err
		}
		l := len(v)
		for idx := 0; idx < l; idx++ {
			if _, err := buf.WriteString(strconv.Itoa(v[idx])); err != nil {
				return err
			}

			if idx < l-1 {
				if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
					return err
				}
			}
		}
		if i < metricsCount-1 {
			if err := buf.WriteByte(consts.SchemaItemSeparator); err != nil {
				return err
			}
		}
	}
	return nil
}

// MessageSerialize method
func MessageSerialize(ms *SchemaItems, values map[string]interface{}, buf *bytes.Buffer) error {

	metricsCount := len(ms.SchemaItems)
	for i, m := range ms.SchemaItems {
		var intArr []int
		var floatArr []float64
		var strArr []string
		var l int
		v, ok := values[m.Name]
		if !ok {
			err := errors.New("schema Metrics [" + m.Name + "] not exists")
			return err
		}

		if m.ValueType == IntType {
			intArr = v.([]int)
			l = len(intArr)
		} else if m.ValueType == FloatType {
			floatArr = v.([]float64)
			l = len(floatArr)
		} else if m.ValueType == StringType {
			strArr = v.([]string)
			l = len(strArr)
		} else {
			err := errors.New("schema Metrics [" + m.Name + "] not support value type")
			buf.Reset()
			return err
		}

		for idx := 0; idx < l; idx++ {
			if intArr != nil {
				buf.WriteString(strconv.Itoa(intArr[idx]))
			}
			if floatArr != nil {
				buf.WriteString(strconv.FormatFloat(floatArr[idx], 'f', 6, 64))
			}
			if strArr != nil {
				buf.WriteString(strArr[idx])
			}

			if idx < l-1 {
				buf.WriteByte(consts.SchemaItemSeparator)
			}
		}
		if i < metricsCount-1 {
			buf.WriteByte(consts.SchemaItemSeparator)
		}
	}
	return nil
}
