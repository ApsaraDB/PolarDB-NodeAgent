/*-------------------------------------------------------------------------
 *
 * schema_test.go
 *    Test case for schema.go
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
 *           internal/gather/schema_test.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOnlyIntMessageSerialize(t *testing.T) {
	t.Skip()
	var buf bytes.Buffer
	bs, err := JSON2Schema([]byte(schemaJSON), &buf)
	assert.Nil(t, err, "JSON2Schema failed")

	var metricsBuff bytes.Buffer
	collectContent := make(map[string]uint64, 1024)
	collectContent["aaa"] = 1
	collectContent["bbb"] = 90302
	collectContent["ccc"] = 6023402

	OnlyIntMessageSerializeBySchema(&bs.Metrics, collectContent, &metricsBuff)
	assert.True(t, strings.HasSuffix(metricsBuff.String(), "1;90302;6023402"))
}

func TestHash(t *testing.T) {
	t.Skip()
	path := "../../conf/plugin/polardb_mysql/schema.json"
	content, _ := ioutil.ReadFile(path)
	var buf bytes.Buffer
	bs, _ := JSON2Schema(content, &buf)
	fmt.Println(bs.Version)
}

func BenchmarkOnlyIntMessageSerialize(b *testing.B) {
	b.Skip()
	var buf bytes.Buffer
	bs, err := JSON2Schema([]byte(schemaJSON), &buf)
	assert.Nil(b, err, "JSON2Schema failed")

	var metricsBuff bytes.Buffer
	collectContent := make(map[string]uint64, 1024)
	collectContent["aaa"] = 1
	collectContent["bbb"] = 90302
	collectContent["ccc"] = 6023402

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		OnlyIntMessageSerializeBySchema(&bs.Metrics, collectContent, &metricsBuff)
	}
	b.StopTimer()
}

func TestNewType(t *testing.T) {
	bt, err := ioutil.ReadFile("/opt/pdb_ue/conf/plugin/polartrace_latency_bsr_csv/schema.json")
	if err != nil {
		fmt.Printf("read file %s error: %s", "file", err.Error())
		//return
	}
	var buf bytes.Buffer
	_, err = JSON2Schema(bt, &buf)
	fmt.Println(err)

}

var schemaJSON = `{
  "headers":[
    {
      "Index": 0,
      "Name":"version",
      "type":"string"
    },
    {
      "Index": 1,
      "Name":"db_type",
      "type":"string"
    }
  ],
  "metrics": [
    {
      "Index": 0,
      "type": "int",
      "Name": "aaa"
    },
    {
      "Index": 1,
      "type": "int",
      "Name": "bbb"
    },
    {
      "Index": 2,
      "type": "int",
      "Name": "ccc"
    }]
}`
