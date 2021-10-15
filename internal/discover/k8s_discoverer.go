/*-------------------------------------------------------------------------
 *
 * k8s_discoverer.go
 *    k8s pod discover
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
 *           internal/discover/k8s_discoverer.go
 *-------------------------------------------------------------------------
 */
package discover

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/db-monitor/common/consts"

	"github.com/ApsaraDB/db-monitor/common/log"
)

// CloudifyK8sDiscoverer docker discover define
type K8sDiscoverer struct {
	RawDockerDiscoverer
}

// DiscoverInit init discover, will allocate queue chan here
func (d *K8sDiscoverer) DiscoverInit() {
	d.containers = make(map[string]*ContainerInfo)
	d.ContainerQueue = make(chan *ContainerInfo)
	d.stopEvent = make(chan bool, 1)
}

// DiscoverFetch method
func (d *K8sDiscoverer) DiscoverFetch() (interface{}, bool) {
	c, ok := <-d.ContainerQueue
	return c, ok
}

// DiscoverRun start discover
func (d *K8sDiscoverer) DiscoverRun(wait *sync.WaitGroup) {
	wait.Add(1)
	defer wait.Done()

	d.running = true
	dockerConfPath := "/etc/systemd/system/docker.service.d/docker.conf"
	d.running = true
	dockerGraph, err := getDockerGraph(dockerConfPath, &d.buf)
	if err == nil {
		d.WorkDir = dockerGraph
	} else {
		log.Error("[k8s_discoverer] getDockerGraph failed", log.String("err", err.Error()))
	}
	d.enumerateContainer()
}

// NOTE 对于k8s 部署的实例，实例重启是重新拉起一个容器，而不是对原有容器做docker restart
func (d *K8sDiscoverer) enumerateContainer() {
	activeContainers := make(map[string]*ContainerInfo)
	podContainers := make(map[string]map[string]string)
	var containersFi []string
	dockerConfPath := "/etc/systemd/system/docker.service.d/docker.conf"
	cur := time.Now().Unix()
	old := cur
	for {
		cur = time.Now().Unix()
		if cur-old > 5*60 {
			workDir, err := getDockerGraph(dockerConfPath, &d.buf)
			if err == nil {
				d.WorkDir = workDir
			} else {
				log.Error("[k8s_discoverer] getDockerGraph failed", log.String("err", err.Error()))
			}
		}
		if !d.running {
			d.ContainerQueue <- nil
			log.Error("[k8s_discoverer] stop...", log.String("target", d.WorkDir))
			break
		}
		for k := range activeContainers {
			delete(activeContainers, k)
		}
		for k := range podContainers {
			delete(podContainers, k)
		}

		file, err := os.Open(d.WorkDir)
		if err != nil {
			log.Info("[k8s_discoverer] Open dir failed",
				log.String("path", d.WorkDir),
				log.String("err", err.Error()))
			goto SLEEP_INTERVAL
		}
		containersFi, err = file.Readdirnames(-1)
		file.Close()
		if err != nil {
			log.Error("[k8s_discoverer] Readdirnames failed",
				log.String("path", d.WorkDir),
				log.String("err", err.Error()))
			goto SLEEP_INTERVAL
		}

		for _, containerID := range containersFi {
			// scan containerInfo config file
			configfile := fmt.Sprintf("%s/%s/config.v2.json", d.WorkDir, containerID)

			// file not exists or it is a directory, skip
			if fi, err := os.Stat(configfile); err != nil || fi.IsDir() {
				log.Info("[k8s_discoverer] file not found",
					log.String("file", configfile))
				continue
			}

			err := readFile(configfile, &d.buf)
			if err != nil {
				log.Error("[k8s_discoverer] read file %s failed",
					log.String("file", configfile),
					log.String("err", err.Error()))
				continue
			}
			content := d.buf.Bytes()

			tmpContainerInfo := getContainerInfo(content)
			tmpContainerInfo.ContainerID = containerID
			tmpContainerInfo.PodContainerInfo = make(map[string]string)
			containerInfo, ok := d.containers[containerID]
			if !ok {
				containerInfo = tmpContainerInfo
				if !containerInfo.Running {
					continue
				}
				containerInfo.FirstRun = true
				containerInfo.Stop = make(chan bool, 1)

				sandboxID := containerInfo.Labels["io.kubernetes.sandbox.id"]
				if sandboxID != "" {
					if containerName, ok := containerInfo.Labels["io.kubernetes.container.name"]; ok {

						if _, ok := podContainers[sandboxID]; ok {
							podContainers[sandboxID][containerName] = containerInfo.ContainerID
						} else {
							tempContainer := make(map[string]string)
							tempContainer[containerName] = containerInfo.ContainerID
							podContainers[sandboxID] = tempContainer
						}

					}
				}

				name := containerInfo.Name
				if name != "" {
					if strings.HasPrefix(name, "/k8s_engine") || strings.HasPrefix(name, "/k8s_service") {
						// read hostconfig.json for more infomation.
						hostconfigfile := fmt.Sprintf("%s/%s/hostconfig.json", d.WorkDir, containerID)
						if fi, err := os.Stat(hostconfigfile); err != nil || fi.IsDir() {
							log.Warn("[k8s_discoverer] hostconfig.json file not found, "+
								"but we will ignore it now.",
								log.String("file", hostconfigfile))
						} else {
							err := readFile(hostconfigfile, &d.buf)
							if err != nil {
								log.Warn("[k8s_discoverer] read hostconfig.json failed, "+
									"but we will ignore it now.",
									log.String("file", hostconfigfile),
									log.String("err", err.Error()))
							} else {
								content := d.buf.Bytes()
								containerInfo = patchContainerInfoWithHostConfig(containerInfo, content)
							}
						}

						d.containers[containerID] = containerInfo
						log.Info("[k8s_discoverer]  container first running",
							log.String("containerId", containerInfo.ContainerID),
							log.Bool("running", containerInfo.Running))
					}
				}
			} else {
				if containerInfo.Running && !tmpContainerInfo.Running {
					containerInfo.Running = false
					log.Info("[cloudify_k8s_discoverer] stopped container found",
						log.String("containerId", containerID),
						log.Bool("Running", containerInfo.Running))
				}
				d.notifyInstanceChange(containerInfo)
			}
			activeContainers[containerInfo.ContainerID] = containerInfo

		}

		for _, containerInfo := range d.containers {
			sandboxID := containerInfo.Labels["io.kubernetes.sandbox.id"]
			if oneContainerMap, ok := podContainers[sandboxID]; ok {
				for containerName, containerId := range oneContainerMap {
					containerInfo.PodContainerInfo[containerName] = containerId
				}
				containerInfo.PodContainerInfo["POD"] = sandboxID
			}
		}

		for _, containerInfo := range d.containers {
			if containerInfo.FirstRun {
				pauseID := containerInfo.Labels["io.kubernetes.sandbox.id"]
				pauseContainer, ok := activeContainers[pauseID]
				if !ok {
					log.Warn("[k8s_discoverer] pause container is not alive",
						log.String("pause container ID", pauseID),
						log.String("container ID", containerInfo.ContainerID))
					delete(d.containers, containerInfo.ContainerID)
					continue
				}
				containerInfo.Labels["apsara.metric.store.pbd_number"] =
				pauseContainer.Labels["apsara.metric.store.pbd_number"]
				containerInfo.Labels[consts.ApsaraMetricInsID] = pauseContainer.Labels[consts.ApsaraMetricInsID]
				containerInfo.Labels[consts.ApsaraMetricLogicCustinsID] = pauseContainer.Labels[consts.ApsaraMetricLogicCustinsID]
				containerInfo.Labels[consts.ApsaraMetricPhysicalCustinsID] = pauseContainer.Labels[consts.ApsaraMetricPhysicalCustinsID]
				containerInfo.Labels[consts.ApsaraMetricInsName] = pauseContainer.Labels[consts.ApsaraMetricInsName]
				containerInfo.Labels[consts.ApsaraInsPort] = pauseContainer.Labels[consts.ApsaraInsPort]
				if _, ok := pauseContainer.Labels["apsara.metric.logic_custins_name"]; ok {
					containerInfo.Labels["apsara.metric.logic_custins_name"] = pauseContainer.Labels["apsara.metric.logic_custins_name"]
				}
				if _, ok := pauseContainer.Labels["apsara.metric.cluster_name"]; ok {
					containerInfo.Labels["apsara.metric.cluster_name"] = pauseContainer.Labels["apsara.metric.cluster_name"]
				}
				if _, ok := pauseContainer.Labels[consts.ApsaraMetricDbType]; ok {
					containerInfo.Labels[consts.ApsaraMetricDbType] = pauseContainer.Labels[consts.ApsaraMetricDbType]
				}
				if _, ok := pauseContainer.Labels["apsara.metric.pv_name"]; ok {
					containerInfo.Labels["apsara.metric.pv_name"] = pauseContainer.Labels["apsara.metric.pv_name"]
				}
				containerInfo.FirstRun = false
				d.notifyInstanceChange(containerInfo)

				log.Info("[k8s_discoverer] new running container found",
					log.String("containerId", containerInfo.ContainerID),
					log.Bool("Running", containerInfo.Running))
			}
		}

		// notify runner to stop this instance
		for containerID, containerInfo := range d.containers {
			keep := false
			_, ok := activeContainers[containerID]
			if ok {
				keep = true
			}

			if !keep {
				log.Info("[k8s_discoverer] remove container",
					log.String("containerId", containerID))
				containerInfo.Running = false
				d.notifyInstanceChange(containerInfo)
				delete(d.containers, containerID)
			}
		}

	SLEEP_INTERVAL:
		select {
		case <-d.stopEvent:
			break
		case <-time.After(60 * time.Second):
			break
		}
	}
}

func (d *K8sDiscoverer) notifyInstanceChange(containerInfo *ContainerInfo) {
	if !containerInfo.Running {
		delete(d.containers, containerInfo.ContainerID)
	}
	d.ContainerQueue <- containerInfo
}

// DiscoverStop should be called at runner Stop()
func (d *K8sDiscoverer) DiscoverStop() error {
	log.Info("[k8s_discoverer] DiscoverStop", log.String("workdir", d.WorkDir))
	d.running = false
	d.stopEvent <- true
	return nil
}

// DiscoverExit should be called at runner Exit()
func (d *K8sDiscoverer) DiscoverExit() error {
	close(d.ContainerQueue)
	return nil
}
