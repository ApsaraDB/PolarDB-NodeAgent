/*-------------------------------------------------------------------------
 *
 * plugin_test.go
 *    Test case for plugin.go
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
 *           internal/gather/plugin_test.go
 *-------------------------------------------------------------------------
 */

package gather

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ApsaraDB/db-monitor/common/log"

	"github.com/stretchr/testify/assert"
)

func setup() {
	log.Init()
}

func TestResolveDependencies_SingleSerialDependencies(t *testing.T) {
	t.Skip()
	setup()
	pCtx := &PluginCtx{
		ReverseDependMap: make(map[string][]*PluginInfo),
	}
	pCtx.Plugins = sync.Map{}
	pInfoA := &PluginInfo{
		Name:         "a",
		Dependencies: []string{"b"},
	}
	pInfoB := &PluginInfo{
		Name:         "b",
		Dependencies: []string{"c"},
	}
	pInfoC := &PluginInfo{
		Name:         "c",
		Dependencies: []string{},
	}
	pCtx.resolveDependencies(pInfoA)
	pCtx.resolveDependencies(pInfoB)
	pCtx.resolveDependencies(pInfoC)
	fmt.Println(pCtx.ReverseDependMap)
	//fmt.Println(pCtx.Plugins)
	//assert.NotNil(t, pCtx.Plugins.Load("a"))
	//assert.NotNil(t, pCtx.Plugins["b"])
	//assert.NotNil(t, pCtx.Plugins["c"])
}

func TestResolveDependencies_MultiDependencies(t *testing.T) {
	t.Skip()
	setup()
	pCtx := &PluginCtx{
		ReverseDependMap: make(map[string][]*PluginInfo),
	}
	pCtx.Plugins = sync.Map{}
	pInfoA := &PluginInfo{
		Name:         "a",
		Dependencies: []string{"b", "c"},
	}
	pInfoB := &PluginInfo{
		Name:         "b",
		Dependencies: []string{"c"},
	}
	pInfoC := &PluginInfo{
		Name:         "c",
		Dependencies: []string{},
	}
	pCtx.resolveDependencies(pInfoA)
	pCtx.resolveDependencies(pInfoB)
	pCtx.resolveDependencies(pInfoC)
	fmt.Println(pCtx.ReverseDependMap)
	//fmt.Println(pCtx.Plugins)
	//assert.NotNil(t, pCtx.Plugins["a"])
	//assert.NotNil(t, pCtx.Plugins["b"])
	//assert.NotNil(t, pCtx.Plugins["c"])
}

func TestResolveDependencies_CycleDependencies(t *testing.T) {
	t.Skip()
	setup()
	pCtx := &PluginCtx{
		//Plugins:          make(map[string]*PluginInfo),
		ReverseDependMap: make(map[string][]*PluginInfo),
	}
	pCtx.Plugins = sync.Map{}
	pInfoA := &PluginInfo{
		Name:         "a",
		Dependencies: []string{"b"},
	}
	pInfoB := &PluginInfo{
		Name:         "b",
		Dependencies: []string{"c"},
	}
	pInfoC := &PluginInfo{
		Name:         "c",
		Dependencies: []string{"a"},
	}
	assert.Nil(t, pCtx.resolveDependencies(pInfoA))
	assert.Nil(t, pCtx.resolveDependencies(pInfoB))

	err := pCtx.resolveDependencies(pInfoC)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "cycle dependency found"))
}

func TestResolveDependencies_MultiCycleDependencies(t *testing.T) {
	t.Skip()
	setup()
	pCtx := &PluginCtx{
		//Plugins:          make(map[string]*PluginInfo),
		ReverseDependMap: make(map[string][]*PluginInfo),
	}
	pCtx.Plugins = sync.Map{}

	pInfoA := &PluginInfo{
		Name:         "a",
		Dependencies: []string{"c"},
	}
	pInfoB := &PluginInfo{
		Name:         "b",
		Dependencies: []string{"c"},
	}
	pInfoC := &PluginInfo{
		Name:         "c",
		Dependencies: []string{"b"},
	}
	assert.Nil(t, pCtx.resolveDependencies(pInfoA))
	assert.Nil(t, pCtx.resolveDependencies(pInfoB))

	err := pCtx.resolveDependencies(pInfoC)
	t.Log(err)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "cycle dependency found"))
}

// BenchmarkResolveDependencies-8   	 5000000	       287 ns/op
func BenchmarkResolveDependencies(b *testing.B) {
	b.Skip()
	setup()
	pCtx := &PluginCtx{
		//Plugins:          make(map[string]*PluginInfo),
		ReverseDependMap: make(map[string][]*PluginInfo),
	}
	pCtx.Plugins = sync.Map{}
	pInfoA := &PluginInfo{
		Name:         "a",
		Dependencies: []string{"b"},
	}
	pInfoB := &PluginInfo{
		Name:         "b",
		Dependencies: []string{"c"},
	}
	pInfoC := &PluginInfo{
		Name:         "c",
		Dependencies: []string{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pCtx.resolveDependencies(pInfoA)
		pCtx.resolveDependencies(pInfoB)
		pCtx.resolveDependencies(pInfoC)
	}
	b.StopTimer()
}
