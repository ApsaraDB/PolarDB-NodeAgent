/*-------------------------------------------------------------------------
 *
 * export_runner.go
 *    Export Runner
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
 *           internal/gather/export_runner.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"fmt"
	"github.com/ApsaraDB/db-monitor/common/consts"
	"github.com/ApsaraDB/db-monitor/common/log"
	"sync"
	"time"
)

// ExportRunner a runner only for export functions
type ExportRunner struct {
	Runner
	firstRun bool
}

// NewExportRunner new a docker runner object, one plugin conf will run only one runner and one instance in this runner
func NewExportRunner() RunnerService {
	er := &ExportRunner{firstRun: true}

	return er
}

// RunnerInit init this runner
func (er *ExportRunner) RunnerInit(mInfoMap *sync.Map, pInfo *PluginInfo, pCtx *PluginCtx) error {
	return er.runnerInit(mInfoMap, pInfo, pCtx)
}

// RunnerRun export runner run
func (er *ExportRunner) RunnerRun(wg *sync.WaitGroup) error {
	er.running = true
	wg.Add(1)
	defer wg.Done()
	log.Info("[export_runner] Run", log.String("module", er.pInfo.Name))

	ins := &Instance{
		running:    true,
		insName:    "",
		pInfo:      er.pInfo,
		stop:       make(chan bool, 1),
		dataLogger: log.NewDataLogger(er.pInfo.Name, consts.DataJsonConf),
	}
	if err := er.lazyLoadModule(); err != nil {
		log.Error("[export_runner] lazyLoadModule failed", log.String("module", er.pInfo.Name), log.String("err", err.Error()))
		return fmt.Errorf("export_runner lazyLoadModule failed,module:%s,err:%v", er.pInfo.Name, err.Error())
	}
	info, ok := er.mInfoMap.Load(er.pInfo.Name)
	if !ok {
		log.Error("[export_runner] module info not found", log.String("module", er.pInfo.Name))
		return fmt.Errorf("export_runner module not found:%s", er.pInfo.Name)
	}
	ins.mInfo, _ = info.(*ModuleInfo)

	log.Info("[export_runner] find a new instance",
		log.String("runner", er.pInfo.Name),
		log.String("target", er.pInfo.Target))

	er.instances[er.pInfo.Name] = ins
	go er.runSingleExportRunner(ins, wg)

	return nil
}

// RunnerStatus get status
func (er *ExportRunner) RunnerStatus() interface{} {
	return er.status
}

// RunnerNotify notify a runner
func (er *ExportRunner) RunnerNotify() {
	// set all runner instance `notify` from false to true
	for _, v := range er.instances {
		v.notify = true
	}
}

// RunnerStop stop runner
func (er *ExportRunner) RunnerStop() {
	log.Info("[export_runner] Stop", log.String("module", er.pInfo.Name))
	for _, v := range er.instances {
		v.running = false
		v.stop <- true
	}
	return
}

func (er *ExportRunner) runSingleExportRunner(ins *Instance, wg *sync.WaitGroup) {
	wg.Add(1)
	defer er.errorHandler(ins, wg)

	var initCtx interface{}
	var err error
	var stop bool
	// invoke plugin for gathering
	for {
		if !ins.running {
			log.Info("[export_runner] not running",
				log.String("runner", er.pInfo.Name),
				log.String("target", er.pInfo.Target))
			break
		}
		// init plugin
		initCtx, err = er.initModule(ins)
		if err != nil {
			goto SLEEP_INTERVAL
		}

		// store init context
		ins.mInfo.Contexts.Store(ins.pInfo.Name, initCtx)
		// run module
		er.runModule(initCtx, ins)

		err = er.exitModule(initCtx, ins)
		if err != nil {
			log.Error("[export_runner] exit module failed",
				log.String("name", er.pInfo.Name),
				log.String("insName", ins.insName))
		}
		// if module crashed, wait 1min and reinit
	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			stop = true
			break
		case <-time.After(3 * time.Second):
		}
		if stop {
			break
		}
	}

}

// backendCtx not used yet, always nil
func (er *ExportRunner) runModule(initCtx interface{}, ins *Instance) {
	errCount := 0
	var costMillis int64
	mainRun := ins.mInfo.PluginABI.Run
	collectContentMap := make(map[string]interface{}, 8)

	// interval configured in SECOND
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		if ins.notify {
			ins.notify = false
			// TODO notify action
		}
		if !ins.running {
			break
		}

		collectStartTimeMillis := time.Now().UnixNano() / 1e6

		// 2. invoke PluginRun in plugin.so
		err := mainRun(initCtx, collectContentMap)
		if err != nil {
			errCount++
			if errCount > 3 {
				log.Error("[export_runner] collector plugin run error, reinit it",
					log.String("name", er.pInfo.Name),
					log.Int("errCount", errCount))
				break
			}
			log.Error("[export_runner] collector plugin run error",
				log.String("name", er.pInfo.Name),
				log.String("err", err.Error()))
			goto SLEEP_INTERVAL
		}

		// post process
		costMillis = time.Now().UnixNano()/1e6 - collectStartTimeMillis
		log.Info("[export_runner] all costs ms",
			log.Int64("cost", costMillis), log.String("name", er.pInfo.Name))

	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			ticker.Stop()
			ins.stop <- true
			break
		case <-ticker.C:
			if er.firstRun {
				// 为了应对agent 重启后磁盘相关数据需要较长时间才能获取到，对第一次运行做特殊化处理
				er.firstRun = false
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(er.pInfo.Interval) * time.Second)
				log.Info("[export_runner] new ticker",
					log.Int("interval", er.pInfo.Interval), log.String("name", er.pInfo.Name))
			}
			break
		}
	}
}
