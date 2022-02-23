/*-------------------------------------------------------------------------
 *
 * meta_service.go
 *    Meta service for collector
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
 *           common/polardb_pg/meta/meta_service.go
 *-------------------------------------------------------------------------
 */
package meta

import (
	"sync"
	"time"
)

var meta *MetaService
var once sync.Once

type TTL struct {
	modify int64
	ttl    int64
}

type MetaService struct {
	MetaMap *sync.Map
	TTLMap  *sync.Map
}

func GetMetaService() *MetaService {
	once.Do(func() {
		meta = &MetaService{MetaMap: &sync.Map{}, TTLMap: &sync.Map{}}
	})

	return meta
}

func genkey(namespace string, name string) string {
	return namespace + "_" + name
}

func (m *MetaService) SetInterfaceTTL(namespace string, name string, value interface{}, ttl int64) {
	m.MetaMap.Store(genkey(namespace, name), value)
	m.TTLMap.Store(genkey(namespace, name), TTL{modify: time.Now().Unix(), ttl: ttl})
}

func (m *MetaService) SetInterface(namespace string, name string, value interface{}) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetInterface(namespace string, name string) (interface{}, bool) {
	key := genkey(namespace, name)
	v, ok := m.MetaMap.Load(key)
	if ok {
		ttl, tok := m.TTLMap.Load(key)
		if tok {
			if time.Now().Unix()-ttl.(TTL).modify > ttl.(TTL).ttl {
				m.MetaMap.Delete(key)
				m.TTLMap.Delete(key)
				return nil, false
			}
		}

		return v, ok
	}

	return nil, ok
}

func (m *MetaService) SetInterfaceMap(namespace string, name string, value map[string]interface{}) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) SetStringMap(namespace string, name string, value map[string]string) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetInterfaceMap(namespace string, name string) (map[string]interface{}, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(map[string]interface{}), ok
	}

	return make(map[string]interface{}), ok
}

func (m *MetaService) GetStringMap(namespace string, name string) (map[string]string, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(map[string]string), ok
	}

	return make(map[string]string), ok
}

func (m *MetaService) GetSingleMap(namespace string, name string) map[string]interface{} {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(map[string]interface{})
	}

	return make(map[string]interface{})
}

func (m *MetaService) SetString(namespace string, name string, value string) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetString(namespace string, name string) (string, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(string), ok
	}

	return "", ok
}

func (m *MetaService) SetInteger(namespace string, name string, value int64) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetInteger(namespace string, name string) (int64, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(int64), ok
	}

	return int64(0), ok
}

func (m *MetaService) SetFloat(namespace string, name string, value float64) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetFloat(namespace string, name string) (float64, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(float64), ok
	}

	return float64(0), ok
}

func (m *MetaService) GetSingleString(namespace string, name string) string {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(string)
	}

	return ""
}
