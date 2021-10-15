/*-------------------------------------------------------------------------
 *
 * plugin.go
 *    Plugin Interface
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
 *           internal/gather/plugin.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/db-monitor/common/consts"

	"github.com/ApsaraDB/db-monitor/common/log"
)

// PluginInfo for plugin
type PluginInfo struct {
	Type              string   `json:"type"`
	Mode              string   `json:"mode"`
	Runner            string   `json:"runner"`
	Exports           []string `json:"exports"`
	Imports           []string `json:"imports"`
	Path              string   `json:"path"`
	Name              string   `json:"name"`
	Extern            string   `json:"extern"`
	WorkDir           string   `json:"workdir"`
	WorkDirPrefix     string   `json:"prefix"`
	DoubleCheckDir    string   `json:"doulecheckdir"`
	Target            string   `json:"target"`       // 用于判断实例类型,对齐本地元数据的dbtype
	DbType            string   `json:"dbtype"`       // 用于上报数据的dbtype,不一定等于target
	BusinessType      string   `json:"businessType"` // 用于上报数据的类型
	BizType           string   `json:"biz_type"`     //区分实例是属于公共云还是阿里集团
	Enable            bool     `json:"enable"`
	Interval          int      `json:"interval"`
	Backend           []string `json:"backend"`
	Processor         string   `json:"processor"`      // NOTE(wormhole.gl): processor
	ProcessorBackends []string `json:"multi_backends"` // NOTE(wormhole.gl): processor backends
	Dependencies      []string `json:"dependencies"`
}

// PluginCtx : plugin context
// ReverseDependMap : depOn 被依赖 : dep 依赖, 如 A 依赖 B & C，则记录两条：B:A, C:A
type PluginCtx struct {
	running bool
	bizType string
	//Plugins          map[string]*PluginInfo
	Plugins          sync.Map // pluginName as key,pluginInfo as value
	ReverseDependMap map[string][]*PluginInfo
	PluginQueue      chan *PluginInfo
	stopEvent        chan bool
}

func pluginConfParse(file string, buf *bytes.Buffer) (*PluginInfo, error) {
	var plugin PluginInfo
	plugin.Enable = true
	buf.Reset()
	fp, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	_, err = buf.ReadFrom(fp)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(buf.Bytes(), &plugin)
	if err != nil {
		return nil, err
	}
	return &plugin, nil
}

// PluginStop stop
func (pCtx *PluginCtx) PluginStop() {
	pCtx.running = false
	pCtx.stopEvent <- true
	pCtx.PluginQueue <- nil
}

// PluginInit init
func (pCtx *PluginCtx) PluginInit() {
	pCtx.PluginQueue = make(chan *PluginInfo)
	pCtx.stopEvent = make(chan bool, 1)
	pCtx.ReverseDependMap = make(map[string][]*PluginInfo)
	pCtx.running = false
	/*	NOTE
		背景: 集团上云的机器和公共云的机器是混部的，在天龙软件栈无法区分，对于集团上云的实例会存在AligroupLabel 文件
			 有些插件是集团上云专属插件，公共云不开启，因此需要判断是否运行改插件
	*/
	if _, err := os.Stat(consts.AligroupLabel); err != nil {
		pCtx.bizType = consts.Aliyun
	} else {
		pCtx.bizType = consts.Aligroup
	}
}

// PluginExit exit
func (pCtx *PluginCtx) PluginExit() {
	close(pCtx.PluginQueue)
}

// Run run plugin
func (pCtx *PluginCtx) Run(dir string, wait *sync.WaitGroup) {

	wait.Add(1)
	defer wait.Done()

	pCtx.running = true
	var buf bytes.Buffer
	for {
		if !pCtx.running {
			log.Info("[plugin] exiting...")
			break
		}
		file, err := os.Open(dir)
		if err != nil {
			log.Info("[plugin] open dir failed", log.String("dir", dir), log.String("err", err.Error()))
			return
		}

		filenames, err := file.Readdirnames(-1)
		file.Close()
		if err != nil {
			log.Error("[plugin] Readdirnames failed", log.String("dir", dir), log.String("err", err.Error()))
			return
		}

		for _, filename := range filenames {
			if !strings.HasPrefix(filename, "plugin_") {
				continue
			}
			filePath := path.Join(dir, filename)
			// parse plugin conf
			plugin, err := pluginConfParse(filePath, &buf)
			if err != nil {
				log.Error("[plugin] plugin conf err",
					log.String("file", filePath),
					log.String("err", err.Error()))
				continue
			}

			// already exists
			if _, ok := pCtx.Plugins.Load(plugin.Name); ok {
				continue
			}
			if err := pCtx.resolveDependencies(plugin); err != nil {
				// resolve dependencies error, skip this plugin
				log.Error("[plugin] resolve dependencies error, skip this plugin",
					log.String("name", plugin.Name),
					log.String("err", err.Error()))
				continue
			}
		}
		select {
		case <-pCtx.stopEvent:
			break
		case <-time.After(60 * time.Second):
			break
		}
	}
}

func (pCtx *PluginCtx) resolveDependencies(plugin *PluginInfo) error {
	depsReady := true
	// resolve dependencies
	if len(plugin.Dependencies) > 0 {
		// depOnName 被依赖项的Name
		for _, depOnName := range plugin.Dependencies {
			// depOnName 被依赖项已经存在了, continue
			if _, ok := pCtx.Plugins.Load(depOnName); ok {
				continue
			}

			// 递归检查循环依赖
			if pCtx.isDependent(plugin.Name, depOnName) {
				depsReady = false
				return fmt.Errorf("cyclic dependency found: %s -> %s", plugin.Name, depOnName)
			}

			// not loaded
			pCtx.ReverseDependMap[depOnName] = append(pCtx.ReverseDependMap[depOnName], plugin)

			// will wait for dependencies
			depsReady = false
		}
	}
	if depsReady {
		pCtx.handleDependOn(plugin)
	}

	return nil
}

// WARNING: Recursive call
func (pCtx *PluginCtx) handleDependOn(plugin *PluginInfo) {

	pCtx.Plugins.Store(plugin.Name, plugin)
	pCtx.notifyPluginChange(plugin)

	// notify 依赖此plugin的项, im ready
	for _, dep := range pCtx.ReverseDependMap[plugin.Name] {
		pCtx.notifyDependReady(dep)
	}
}

// the ready plugin
func (pCtx *PluginCtx) notifyDependReady(plugin *PluginInfo) {
	// check if all dependencies were loaded
	depsReady := true
	for _, depOnName := range plugin.Dependencies {
		if _, ok := pCtx.Plugins.Load(depOnName); ok {
			continue
		}
		depsReady = false
	}
	// notify only if all dependencies were loaded
	if depsReady {
		pCtx.handleDependOn(plugin)
	} else {
		log.Info("[plugin]  notifyDependReady not ready", log.String("name", plugin.Name))
	}
}

// cyclic dependencies check:
// since src depends on dest, so we should first check whether dest depends on src
// and recursively check
// WARNING : recursive call
func (pCtx *PluginCtx) isDependent(src, dest string) bool {
	// find who are depending on src
	for _, dep := range pCtx.ReverseDependMap[src] {
		if dep.Name == dest {
			return true
		}

		if pCtx.isDependent(dep.Name, dest) {
			return true
		}
	}
	return false
}

func (pCtx *PluginCtx) notifyPluginChange(plugin *PluginInfo) {
	if plugin.Enable {
		pCtx.PluginQueue <- plugin
		log.Info("[plugin] new plugin found", log.String("name", plugin.Name))
	} else {
		log.Info("[plugin] disabled plugin found", log.String("name", plugin.Name))
	}
}
