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
)

var meta *MetaService
var once sync.Once

type MetaService struct {
	MetaMap *sync.Map
}

func GetMetaService() *MetaService {
	once.Do(func() {
		meta = &MetaService{MetaMap: &sync.Map{}}
	})

	return meta
}

func genkey(namespace string, name string) string {
	return namespace + "_" + name
}

func (m *MetaService) SetInterface(namespace string, name string, value interface{}) {
	m.MetaMap.Store(genkey(namespace, name), value)
}

func (m *MetaService) GetInterface(namespace string, name string) (interface{}, bool) {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
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

func (m *MetaService) GetSingleString(namespace string, name string) string {
	v, ok := m.MetaMap.Load(genkey(namespace, name))
	if ok {
		return v.(string)
	}

	return ""
}
