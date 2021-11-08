/*-------------------------------------------------------------------------
 *
 * software_polardb_runner.go
 *    SoftwarePolardbRunner
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
 *           internal/gather/software_polardb_runner.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/internal/discover"

	"gopkg.in/yaml.v2"
)

const (
	MonitorConfPath = "conf/monitor.yaml"
)

type MonitorConf struct {
	Collector struct {
		Database struct {
			Socketpath string `yaml:"socketpath"`
			Username   string `yaml:"username"`
			Database   string `yaml:"database"`
		} `yaml:"database"`
		ClusterManager struct {
			LogPath string `yaml:"logpath"`
		} `yaml:"clustermanager"`
	} `yaml:"collector"`
	Service struct {
		Netdev string `yaml:"netdev"`
		Port   int    `yaml:"port"`
	} `yaml:"service"`
}

// SoftwarePolardbRunner struct
type SoftwarePolardbRunner struct {
	Runner
	discoverer  *discover.SoftwarePolardbDiscoverer
	monitorConf *MonitorConf
}

// NewSoftwarePolardbRunner new a SoftwarePolardbRunner object
func NewSoftwarePolardbRunner() RunnerService {
	privateCloudRunner := &SoftwarePolardbRunner{}

	return privateCloudRunner
}

// RunnerInit init this runner
func (rr *SoftwarePolardbRunner) RunnerInit(mInfos *sync.Map, pInfo *PluginInfo, ctx *PluginCtx) error {
	log.Info("[software_polardb_runner] runner init")

	err := rr.runnerInit(mInfos, pInfo, ctx)
	if err != nil {
		log.Error("[software_polardb_runner] runner init failed", log.String("error", err.Error()))
		return err
	}

	confstr, err := ioutil.ReadFile(MonitorConfPath)
	if err != nil {
		log.Error("[software_polardb_runner] read software version conf file failed",
			log.String("error", err.Error()))
		return err
	}

	log.Info("[software_polardb_runner] read conf result", log.String("conf", string(confstr)))

	rr.monitorConf = &MonitorConf{}
	err = yaml.Unmarshal(confstr, rr.monitorConf)
	if err != nil {
		log.Error("[software_polardb_runner] yaml unmarshal conf file failed",
			log.String("error", err.Error()), log.String("conf", string(confstr)))
		return err
	}

	rr.discoverer = &discover.SoftwarePolardbDiscoverer{
		WorkDir:         rr.monitorConf.Collector.Database.Socketpath,
		UserName:        rr.monitorConf.Collector.Database.Username,
		Database:        rr.monitorConf.Collector.Database.Database,
		ApplicationName: "ue_polardb_discover",
	}

	return nil
}

// RunnerRun run
func (rr *SoftwarePolardbRunner) RunnerRun(wg *sync.WaitGroup) error {
	log.Info("[software_polardb_runner] runner init")

	rr.running = true
	wg.Add(1)
	defer wg.Done()

	log.Info("[software_polardb_runner]: Run", log.String("module", rr.pInfo.Name))

	rr.discoverer.DiscoverInit()

	// start discover
	go rr.discoverer.DiscoverRun(wg)

	go rr.handleDiscover(wg)

	return nil
}

// RunnerNotify notify
func (rr *SoftwarePolardbRunner) RunnerNotify() {
	for _, v := range rr.instances {
		v.notify = true
	}
}

// RunnerStatus status
func (rr *SoftwarePolardbRunner) RunnerStatus() interface{} {
	return rr.status
}

func (rr *SoftwarePolardbRunner) handleDiscover(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		if !rr.running {
			log.Info("[software_polardb_runner] handleDiscover stop",
				log.String("module", rr.pInfo.Name))
			break
		}

		insInfo, ok := <-rr.discoverer.InstanceQueue
		if !ok || insInfo == nil {
			log.Info("[software_polardb_runner] handleDiscover: got invalid info, I will exit",
				log.String("module", rr.pInfo.Name))
			break
		}

		log.Debug("[software_polardb_runner] handle new discover",
			log.String("ins info", fmt.Sprintf("%+v", insInfo)))

		// XXX stop runner if plugin->Enable is false
		if !rr.pInfo.Enable {
			rr.running = false
			// TODO error log, log status to ctrl status
			break
		}

		key := fmt.Sprintf("%s_%d", insInfo.InsName, insInfo.Port)

		// stop running ins if it is running
		if !insInfo.Running {
			if oldIns, ok := rr.instances[key]; ok {
				oldIns.running = false
				delete(rr.instances, key)
				continue
			}
		}
		instance, ok := rr.instances[key]
		if ok {
			instance.insName = insInfo.InsName
		} else {
			envs := make(map[string]string)

			envs["host_data_dir"] = insInfo.ShareDataDir
			envs["host_log_dir"] = insInfo.LocalDataDir
			envs["host_log_full_path"] = insInfo.LogDir
			envs["host_data_local_dir"] = insInfo.LocalDataDir
			envs["host-data-dir"] = insInfo.ShareDataDir
			envs["host-log-dir"] = insInfo.LogDir
			envs["host-log-full-path"] = insInfo.LogDir
			envs["logical_ins_name"] = insInfo.LogicInsName
			envs["physical_ins_name"] = fmt.Sprintf("%s:%d", rr.getHostIP(), insInfo.Port)
			envs["host_ins_id"] = fmt.Sprintf("%s:%d", rr.getHostIP(), insInfo.Port)
			envs["socket_path"] = rr.monitorConf.Collector.Database.Socketpath
			envs["username"] = insInfo.Username
			envs["database"] = insInfo.Database
			ins := &Instance{
				running:    true,
				port:       insInfo.Port,
				insPath:    insInfo.LocalDataDir,
				insName:    insInfo.InsName,
				insID:      insInfo.InsName,
				env:        envs,
				pInfo:      rr.pInfo,
				dataLogger: log.NewDataLogger(rr.pInfo.Name+"_"+insInfo.InsName, consts.DataJsonConf),
				stop:       make(chan bool, 1),
				index:      getNewInstanceID(),
			}

			if err := rr.lazyLoadModule(); err != nil {
				log.Error("[software_polardb_runner] lazyLoadModule failed",
					log.String("module", rr.pInfo.Name),
					log.String("err", err.Error()))
				continue
			}

			info, ok := rr.mInfoMap.Load(rr.pInfo.Name)
			if !ok {
				log.Error("[software_polardb_runner] module not found", log.String("module", rr.pInfo.Name))
				return
			}
			ins.mInfo, _ = info.(*ModuleInfo)

			log.Info("[software_polardb_runner] find a new instance",
				log.String("runner", rr.pInfo.Name),
				log.String("target", rr.pInfo.Target),
				log.Int("port", ins.port))

			rr.instances[key] = ins
			go rr.runSingleSoftwarePolardbRunner(ins, wg)
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
}

// RunnerStop stop
func (rr *SoftwarePolardbRunner) RunnerStop() {
	log.Info("[software_polardb_runner] about to stop", log.String("module", rr.pInfo.Name))
	rr.running = false
	err := rr.discoverer.DiscoverStop()
	if err != nil {
		log.Error("[software_polardb_runner] stop discover failed",
			log.String("module", rr.pInfo.Name),
			log.String("err", err.Error()))
	}

	for _, v := range rr.instances {
		v.running = false
		v.stop <- true
	}

	// err = rr.exitBackendModule(rr.backendCtx)
	// if err != nil {
	// 	log.Error("[software_polardb_runner] exit backend module failed",
	// 		log.String("name", rr.pInfo.Name),
	// 		log.String("backend", rr.pInfo.Backend))
	// }
}

func (rr *SoftwarePolardbRunner) runSingleSoftwarePolardbRunner(ins *Instance, wg *sync.WaitGroup) {
	wg.Add(1)
	defer rr.errorHandler(ins, wg)

	for {
		if !ins.running {
			log.Info("[software_polardb_runner] ins not running",
				log.String("runner", rr.pInfo.Name),
				log.String("target", rr.pInfo.Target),
				log.Int("port", ins.port))
			break
		}

		// init plugin
		initCtx, err := rr.initModule(ins)
		if err != nil {
			log.Error("[software_polardb_runner] init failed",
				log.String("ins", fmt.Sprintf("%+v", ins)),
				log.String("error", err.Error()))
			goto SLEEP_INTERVAL
		}

		// run module
		if rr.externConf.Type == "perf" {
			// rr.runModule(initCtx, rr.backendCtx, ins)
			rr.runModule(initCtx, ins)
		} else if rr.externConf.Type == "multi_perf" {
			// rr.runModuleM(initCtx, rr.backendCtx, ins)
			rr.runModuleM(initCtx, ins)
		} else {
			// rr.runLogModule(initCtx, rr.backendCtx, ins)
			rr.runLogModule(initCtx, ins)
		}

		// exit module
		err = rr.exitModule(initCtx, ins)
		if err != nil {
			log.Error("[software_polardb_runner] exit module failed",
				log.String("name", rr.pInfo.Name),
				log.String("insName", ins.insName),
				log.Int("port", ins.port))
		}

	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			break
		case <-time.After(60 * time.Second):
			break
		}
	}
}

func (rr *SoftwarePolardbRunner) getHostIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
