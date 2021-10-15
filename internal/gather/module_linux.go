/*-------------------------------------------------------------------------
 *
 * module_linux.go
 *    Plugin module interface
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
 *           internal/gather/module_linux.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"errors"
	"fmt"
	"plugin"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PluginInterface plugin common
type PluginInterface struct {
	Init func(interface{}) (interface{}, error)
	Run  func(interface{}, interface{}) error
	Exit func(interface{}) error
}

// ModuleInfo a module struct
type ModuleInfo struct {
	ref       int32
	inited    int32
	plugin    *plugin.Plugin
	ID        string
	Mode      string
	Extern    string
	PluginABI PluginInterface
	Eat       map[string]interface{} // module-name.FuncName as key
	Iat       map[string]interface{} // module-name.FuncName as key
	Contexts  sync.Map               // module-name as key
}

func (module *ModuleInfo) loadFunction(info *PluginInfo) error {
	for _, identifier := range info.Exports {

		// identifier looks like : module-name.FuncName
		list := strings.Split(identifier, ".")
		if len(list) != 2 {
			return fmt.Errorf("[module] invalid export identifier: %s", identifier)
		}
		fnName := list[1]
		fn, err := module.plugin.Lookup(fnName)
		if err != nil {
			return err
		}
		module.Eat[identifier] = fn
	}
	return nil
}

func (module *ModuleInfo) modulePrepareGolang(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("modulePrepareGolang open error, path:%s, err:%s", path, err.Error())
	}

	module.PluginABI.Init = nil
	module.PluginABI.Run = nil
	module.PluginABI.Exit = nil

	var ok bool

	initPlugin, err := p.Lookup("PluginInit")
	if err != nil {
		return err
	}

	module.PluginABI.Init, ok = initPlugin.(func(interface{}) (interface{}, error))
	if !ok {
		return errors.New("PluginInit not match abi")
	}

	runPlugin, err := p.Lookup("PluginRun")
	if err != nil {
		return errors.New("PluginRun lookup error")
	}
	module.PluginABI.Run, ok = runPlugin.(func(interface{}, interface{}) error)
	if !ok {
		return errors.New("PluginRun not match abi")
	}

	exitPlugin, err := p.Lookup("PluginExit")
	if err != nil {
		return err
	}

	module.PluginABI.Exit, ok = exitPlugin.(func(interface{}) error)
	if !ok {
		return errors.New("PluginExit not match abi")
	}

	module.plugin = p
	return nil
}

// ModuleInit init a module
func (module *ModuleInfo) ModuleInit(pInfo *PluginInfo) error {
	if atomic.AddInt32(&module.ref, 1) > 1 {
		for atomic.LoadInt32(&module.inited) == 0 {
			time.Sleep(1)
		}
		return nil
	}
	module.Mode = pInfo.Mode
	module.Extern = pInfo.Extern
	module.PluginABI = PluginInterface{}
	module.Eat = make(map[string]interface{})
	module.Iat = make(map[string]interface{})

	if pInfo.Type == "golang" {
		if err := module.modulePrepareGolang(pInfo.Path); err != nil {
			return err
		}
		if err := module.loadFunction(pInfo); err != nil {
			return err
		}
	} else {
		return errors.New("not support plugin type")
	}

	atomic.StoreInt32(&module.inited, 1)

	return nil
}

// ModuleExit exit module
func (module *ModuleInfo) ModuleExit() {
	// TODO Close lua vm and other resource like socket, chan
	// XXX Do not need to call dlclose to release a loaded dynamic library
	v := atomic.AddInt32(&module.ref, -1)
	if v == 0 {
		// do cleanup
	}
}
