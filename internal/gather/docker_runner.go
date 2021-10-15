/*-------------------------------------------------------------------------
 *
 * docker_runner.go
 *    Runner for DB Instance In Docker or Pod
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
 *           internal/gather/docker_runner.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/db-monitor/common/consts"

	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/internal/discover"
)

// DockerRunner for a instance
type DockerRunner struct {
	Runner
	discoverer discover.Service
	// add instance map in docker runner
}

// NewDockerRunner new a docker runner object
func NewDockerRunner() RunnerService {
	dockerRunner := &DockerRunner{}

	return dockerRunner
}

// RunnerInit init this runner
func (dr *DockerRunner) RunnerInit(mInfoMap *sync.Map, pInfo *PluginInfo, pCtx *PluginCtx) error {
	if pInfo.Runner == "docker" {
		// normal docker
		dr.discoverer = &discover.DockerDiscoverer{
			RawDockerDiscoverer: discover.RawDockerDiscoverer{
				WorkDir: pInfo.WorkDir,
				Target:  pInfo.Target,
			},
		}
	} else if pInfo.Runner == "k8s" {

		dr.discoverer = &discover.K8sDiscoverer{
			RawDockerDiscoverer: discover.RawDockerDiscoverer{
				WorkDir: pInfo.WorkDir,
				Target:  pInfo.Target,
			},
		}
	} else {
		return fmt.Errorf("invalid runner type: %s", pInfo.Runner)
	}

	return dr.runnerInit(mInfoMap, pInfo, pCtx)
}

// RunnerRun run plugin for a docker instance
func (dr *DockerRunner) RunnerRun(wg *sync.WaitGroup) error {
	dr.running = true
	wg.Add(1)
	defer wg.Done()
	log.Info("[docker_runner] Run", log.String("module", dr.pInfo.Name))

	dr.discoverer.DiscoverInit()

	// start docker discover
	go dr.discoverer.DiscoverRun(wg)

	go dr.handleDiscover(wg)

	return nil
}

// RunnerStatus get status from a runner
func (dr *DockerRunner) RunnerStatus() interface{} {
	return dr.status
}

// RunnerNotify notify a runner
func (dr *DockerRunner) RunnerNotify() {
	// set all runner instance `notify` from false to true
	for _, v := range dr.instances {
		v.notify = true
	}
}

func (dr *DockerRunner) handleDiscover(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		if !dr.running {
			log.Info("[docker_runner] handleDiscover exiting", log.String("module", dr.pInfo.Name))
			break
		}

		one, ok := dr.discoverer.DiscoverFetch()
		if !ok || one == nil {
			log.Info("[docker_runner] handleDiscover: got invalid info, I will exit",
				log.String("module", dr.pInfo.Name))
			break
		}

		container, ok := one.(*discover.ContainerInfo)
		if !ok {
			log.Info("[docker_runner] container invalid", log.String("module", dr.pInfo.Name))
			continue
		}

		if container == nil {
			dr.running = false
			log.Info("[docker_runner] handleDiscover exiting", log.String("module", dr.pInfo.Name))
			break
		}

		// XXX stop runner if plugin->Enable is false
		if !dr.pInfo.Enable {
			dr.running = false
			log.Info("[docker_runner] module is disabled", log.String("module", dr.pInfo.Name))
			// TODO error log, log status to ctrl status
			break
		}

		dbType, hasType := container.Labels[consts.ApsaraMetricDbType]
		if hasType {
			// 检测dbtype 是否等于target 或者target 中包含*号，dbtype 已target 开头
			if strings.HasSuffix(dr.pInfo.Target, "*") {
				target := strings.TrimSuffix(dr.pInfo.Target, "*")
				if !strings.HasPrefix(dbType, target) {
					log.Info("[docker_runner] db type not match",
						log.String("container", container.ContainerID),
						log.String("target", dr.pInfo.Target),
						log.String("dbtype", dbType))
					continue
				}
			} else {
				if dbType != dr.pInfo.Target {
					log.Info("[docker_runner] db type not match",
						log.String("container", container.ContainerID),
						log.String("target", dr.pInfo.Target),
						log.String("dbtype", dbType))
					continue
				}
			}
		}

		sPort, ok := container.Labels[consts.ApsaraInsPort]
		if !ok {
			log.Info("[docker_runner] apsara.ins.port not found,target not match",
				log.String("target", dr.pInfo.Name), log.String("module", dr.pInfo.Name))
			continue
		}
		port, err := strconv.Atoi(sPort)
		if err != nil {
			log.Error("[docker_runner] invalid port",
				log.String("port", sPort), log.String("module", dr.pInfo.Name))
			continue
		}

		if !container.Running {
			instances := dr.instances[sPort]
			if instances != nil {
				instances.running = false
				delete(dr.instances, sPort)
			}
			log.Error("[docker_runner] container not running",
				log.String("port", sPort),
				log.String("module", dr.pInfo.Name))
			continue
		}

		custInsName := container.Labels["apsara.metric.ins_name"]
		instance, ok := dr.instances[sPort]
		if !ok {
			envs := make(map[string]string)

			for k, v := range container.Env {
				envs[k] = v
			}

			for k, v := range container.Labels {
				envs[k] = v
			}

			autoFind := false

			autoFindDeep := 1
			dr.autoFindCgroupPath(&envs, "cgroup_cpu_path", container.ContainerID,
				"/sys/fs/cgroup/cpu/kubepods.slice/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_mem_path", container.ContainerID,
				"/sys/fs/cgroup/memory/kubepods.slice/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_blkio_path", container.ContainerID,
				"/sys/fs/cgroup/blkio/kubepods.slice/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_huge_mem_path", container.ContainerID,
				"/sys/fs/cgroup/hugetlb/kubepods.slice/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_cpuset_path", container.ContainerID,
				"/sys/fs/cgroup/cpuset/kubepods.slice/", &autoFind, &autoFindDeep)

			if _, ok := container.PodContainerInfo["pfsd"]; ok {
				for key, c := range container.PodContainerInfo {
					containerid := c
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_cpu_path", key), containerid,
						"/sys/fs/cgroup/cpu/kubepods.slice/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_mem_path", key), containerid,
						"/sys/fs/cgroup/memory/kubepods.slice/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_blkio_path", key), containerid,
						"/sys/fs/cgroup/blkio/kubepods.slice/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_cpuset_path", key), containerid,
						"/sys/fs/cgroup/cpuset/kubepods.slice/", &autoFind, &autoFindDeep)
				}
			}

			autoFindDeep = 0
			dr.autoFindCgroupPath(&envs, "cgroup_cpu_path", container.ContainerID,
				"/sys/fs/cgroup/cpu/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_mem_path", container.ContainerID,
				"/sys/fs/cgroup/memory/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_blkio_path", container.ContainerID,
				"/sys/fs/cgroup/blkio/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_huge_mem_path", container.ContainerID,
				"/sys/fs/cgroup/hugetlb/", &autoFind, &autoFindDeep)
			dr.autoFindCgroupPath(&envs, "cgroup_cpuset_path", container.ContainerID,
				"/sys/fs/cgroup/cpuset/", &autoFind, &autoFindDeep)

			if _, ok := container.PodContainerInfo["pfsd"]; ok {
				for key, c := range container.PodContainerInfo {
					containerid := c
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_cpu_path", key), containerid,
						"/sys/fs/cgroup/cpu/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_mem_path", key), containerid,
						"/sys/fs/cgroup/memory/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_blkio_path", key), containerid,
						"/sys/fs/cgroup/blkio/", &autoFind, &autoFindDeep)
					dr.autoFindCgroupPath(&envs, fmt.Sprintf("%s_cgroup_cpuset_path", key), containerid,
						"/sys/fs/cgroup/cpuset/", &autoFind, &autoFindDeep)
				}
			}

			// cust_ins_id === ins_name
			//custInsId := container.Labels["apsara.metric.custins_id"]
			custInsName := container.Labels[consts.ApsaraMetricInsName]
			custInsID := container.Labels[consts.ApsaraMetricLogicCustinsID]

			// // physical ins_id (ex: on master host)
			// envs["host_log_dir"] = container.Env["host_log_dir"]
			// // envs["host-data-dir"] = container.Env["host_data_dir"]
			// envs["engine_type"] = container.Env["engine_type"]
			// envs["pbd_number"] = container.Env["pbd_number"]
			// envs["data_version"] = container.Env["data_version"]
			// envs["container_id"] = container.ContainerID
			// log.Debug("[docker_runner] get instance envs", log.String("data_version", envs["data_version"]), log.String("pbd_number", envs["pbd_number"]))

			if _, ok := container.Labels["apsara.metric.logic_custins_name"]; ok {
				// db
				envs["logical_ins_name"] = container.Labels["apsara.metric.logic_custins_name"]
				envs["physical_ins_name"] = container.Labels["apsara.metric.ins_name"]
			} else if _, xok := container.Labels["apsara.metric.cluster_name"]; xok {
				// maxscale
				envs["logical_ins_name"] = container.Labels["apsara.metric.cluster_name"]
				envs["physical_ins_name"] = container.Labels["apsara.metric.physical_custins_id"]
			} else {
				envs["logical_ins_name"] = custInsName
				envs["physical_ins_name"] = container.Labels["apsara.metric.physical_custins_id"]
			}

			// logic_ins_name
			//logicInsId := container.Env["logic_ins_id"]
			//logicInsName := container.Labels["apsara.metric.logic_ins_name"]
			insId, ok := container.Labels["apsara.metric.insid"]
			if !ok {
				log.Warn("[docker_runner] apsara.metric.insid is nil",
					log.String("containerID", container.ContainerID))
				insId = "nil"
			}
			envs["host_ins_id"] = insId
			ins := &Instance{
				running:  true,
				port:     port,
				userName: consts.AccountAliyunRoot,
				insName:  custInsName,
				insID:    insId,
				env:      envs,
				pInfo:    dr.pInfo,
				stop:     make(chan bool, 1),
				index:    getNewInstanceID(),
			}
			if err := dr.lazyLoadModule(); err != nil {
				log.Error("[docker_runner] lazyLoadModule failed",
					log.String("module", dr.pInfo.Name), log.String("err", err.Error()))
				continue
			}

			info, ok := dr.mInfoMap.Load(dr.pInfo.Name)
			if !ok {
				log.Error("[docker_runner] module not found", log.String("module", dr.pInfo.Name))
				return
			}
			ins.mInfo, _ = info.(*ModuleInfo)
			if dr.externConf.Type != "log" {
				// 对于日志类型的采集，不需要输出到本地文件中
				if len(custInsName) != 0 {
					ins.dataLogger = log.NewDataLogger(dr.pInfo.Name+"_"+custInsName, consts.DataJsonConf)
				} else {
					ins.dataLogger = log.NewDataLogger(dr.pInfo.Name+"_"+custInsID, consts.DataJsonConf)
				}
			}
			dr.instances[strconv.Itoa(port)] = ins
			//insDir := fmt.Sprintf("%s/%s", dr.pInfo.WorkDir, container.ContainerID)
			log.Info("[docker_runner] find a new instance",
				log.String("runner", dr.pInfo.Name),
				log.String("target", dr.pInfo.Target),
				log.Int("port", port))

			go dr.runSingleDockerRunner(ins, wg)
			time.Sleep(1 * time.Second)
		} else {
			instance.insName = custInsName
		}
	}
}

// RunnerStop stop all runner instance
func (dr *DockerRunner) RunnerStop() {
	log.Info("[docker_runner] about to stop", log.String("module", dr.pInfo.Name))
	dr.running = false
	err := dr.discoverer.DiscoverStop()
	if err != nil {
		log.Error("[docker_runner] stop discover failed",
			log.String("err", err.Error()), log.String("module", dr.pInfo.Name))
	}

	for _, v := range dr.instances {
		v.running = false
		v.stop <- true
	}

	err = dr.exitBackendModule()
	if err != nil {
		log.Error("[docker_runner] exit backend module failed",
			log.String("name", dr.pInfo.Name), log.String("err", err.Error()))
	}
}

func (dr *DockerRunner) runSingleDockerRunner(ins *Instance, wg *sync.WaitGroup) {
	wg.Add(1)
	// TODO panic recover
	defer dr.errorHandler(ins, wg)

	// invoke plugin for gathering
	for {
		if !ins.running {
			log.Info("[docker_runner] docker runner not running",
				log.String("runner", dr.pInfo.Name),
				log.String("target", dr.pInfo.Target),
				log.Int("port", ins.port))
			break
		}

		// init plugin
		initCtx, err := dr.initModule(ins)
		if err != nil {
			log.Error("[docker_runner] initModule failed",
				log.String("err", err.Error()),
				log.String("module", dr.pInfo.Name))
			goto SLEEP_INTERVAL
		}

		// run module
		if dr.externConf.Type == "perf" {
			dr.runModule(initCtx, ins)
		} else if dr.externConf.Type == "multi_perf" {
			dr.runModuleM(initCtx, ins)
		} else {
			dr.runLogModule(initCtx, ins)
		}

		// exit module
		err = dr.exitModule(initCtx, ins)
		if err != nil {
			log.Error("[docker_runner] exit module failed",
				log.String("name", dr.pInfo.Name),
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

func (dr *DockerRunner) autoFindCgroupPath(env *map[string]string,
	key, dockerId, path string, autoFindResult *bool, autoFindDeep *int) {
	_, ok := (*env)[key]
	if ok {
		return
	}

	if *autoFindDeep >= 5 {
		return
	}

	*autoFindDeep = *autoFindDeep + 1
	defer func() { *autoFindDeep = *autoFindDeep - 1 }()

	rootFiles, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	//finding in root dir
	for _, dir := range rootFiles {
		if !dir.IsDir() {
			continue
		}

		if strings.Contains(dir.Name(), dockerId) {
			*autoFindResult = true
			(*env)[key] = path + dir.Name()
			log.Info("[docker_runner] auto find cgroup path success",
				log.String("container", dockerId),
				log.String("target", dr.pInfo.Target),
				log.String("envKey", key),
				log.String("envVal", (*env)[key]))
			return
		}
	}

	//finding in sub dir
	for _, dir := range rootFiles {
		if !dir.IsDir() {
			continue
		}
		dr.autoFindCgroupPath(env, key, dockerId, path+dir.Name()+"/", autoFindResult, autoFindDeep)
		_, ok := (*env)[key]
		if ok {
			return
		}
	}
}
