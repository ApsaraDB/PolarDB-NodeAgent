/*-------------------------------------------------------------------------
 *
 * singleton_runner.go
 *    SingletonRunner
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
 *           internal/gather/singleton_runner.go
 *-------------------------------------------------------------------------
 */

package gather

import (
	"fmt"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
)

// SingletonRunner struct
type SingletonRunner struct {
	Runner
}

// NewSingletonRunner method
func NewSingletonRunner() RunnerService {
	return &SingletonRunner{}
}

// RunnerInit init
func (sr *SingletonRunner) RunnerInit(mInfoMap *sync.Map, pInfo *PluginInfo, pCtx *PluginCtx) error {
	return sr.runnerInit(mInfoMap, pInfo, pCtx)
}

// RunnerRun run
func (sr *SingletonRunner) RunnerRun(wg *sync.WaitGroup) error {
	sr.running = true
	wg.Add(1)
	defer wg.Done()
	log.Info("[singleton_runner] Run", log.String("module", sr.pInfo.Name))
	ins := &Instance{
		stop:    make(chan bool, 1),
		running: true,
		insName: "",
		pInfo:   sr.pInfo,
		index:   getNewInstanceID(),
	}
	if sr.externConf.Type != "log" {
		// save data on host for non-log business
		ins.dataLogger = log.NewDataLogger(sr.pInfo.Name, consts.SingletonDataJsonConf)
	}
	if err := sr.lazyLoadModule(); err != nil {
		log.Error("[singleton_runner] lazyLoadModule failed", log.String("module", sr.pInfo.Name), log.String("err", err.Error()))
		return fmt.Errorf("singleton_runner lazyLoadModule failed,module:%s,err:%v", sr.pInfo.Name, err.Error())
	}
	info, ok := sr.mInfoMap.Load(sr.pInfo.Name)
	if !ok {
		log.Error("[singleton_runner] module not found", log.String("module", sr.pInfo.Name))
		return fmt.Errorf("singleton_runner module not found:%s", sr.pInfo.Name)
	}
	ins.mInfo, _ = info.(*ModuleInfo)

	log.Info("[singleton_runner] find a new instance", log.String("runner", sr.pInfo.Name), log.String("target", sr.pInfo.Target))

	sr.instances[sr.pInfo.Name] = ins

	go sr.runSingletonRunner(ins, wg)

	return nil
}

// RunnerStatus status
func (sr *SingletonRunner) RunnerStatus() interface{} {
	return sr.status
}

// RunnerNotify notify
func (sr *SingletonRunner) RunnerNotify() {
	// set all runner instance `notify` from false to true
	for _, v := range sr.instances {
		v.notify = true
	}
}

// RunnerStop stop
func (sr *SingletonRunner) RunnerStop() {
	log.Info("[singleton_runner] stop", log.String("module", sr.pInfo.Name))
	for _, v := range sr.instances {
		v.running = false
		v.stop <- true
	}
	sr.running = false

	err := sr.exitBackendModule()
	if err != nil {
		log.Error("[singleton_runner] exit backend module failed",
			log.String("name", sr.pInfo.Name), log.String("err", err.Error()))
	}
	return
}

func (sr *SingletonRunner) runSingletonRunner(ins *Instance, wg *sync.WaitGroup) {
	wg.Add(1)
	defer sr.errorHandler(ins, wg)

	var initCtx interface{}
	var err error

	// invoke plugin for gathering
	for {
		if !ins.running {
			log.Info("[singleton_runner] not running", log.String("runner", sr.pInfo.Name), log.String("target", sr.pInfo.Target))
			break
		}

		// init plugin
		initCtx, err = sr.initModule(ins)
		if err != nil {
			goto SLEEP_INTERVAL
		}

		// store init context
		ins.mInfo.Contexts.Store(ins.pInfo.Name, initCtx)

		// run module
		// backendCtx not used yet, always nil
		if sr.externConf.Type == "perf" {
			sr.runModule(initCtx, ins)
		} else if sr.externConf.Type == "multi_perf" {
			sr.runModuleM(initCtx, ins)
		} else {
			sr.runLogModule(initCtx, ins)
		}

		err = sr.exitModule(initCtx, ins)
		if err != nil {
			log.Error("[singleton_runner] exit module failed", log.String("name", sr.pInfo.Name), log.String("insName", ins.insName))
		}

		// if module crashed, wait 1min and reinit
	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			break
		case <-time.After(3 * time.Second):
			break
		}
	}

}
