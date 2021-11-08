/*-------------------------------------------------------------------------
 *
 * docker_discoverer.go
 *    docker discover
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
 *           internal/discover/docker_discoverer.go
 *-------------------------------------------------------------------------
 */
package discover

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"

	"github.com/buger/jsonparser"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
)

const (
	unixDockerSock = "unix:///var/run/docker.sock"
)

// DockerDiscoverer docker discover define
type DockerDiscoverer struct {
	RawDockerDiscoverer
}

// DiscoverInit init discover, will allocate queue chan here
func (d *DockerDiscoverer) DiscoverInit() {
	d.containers = make(map[string]*ContainerInfo)
	d.ContainerQueue = make(chan *ContainerInfo)
	d.stopEvent = make(chan bool, 1)
}

// DiscoverFetch get a new container
func (d *DockerDiscoverer) DiscoverFetch() (interface{}, bool) {
	c, ok := <-d.ContainerQueue
	return c, ok
}

func getDockerGraph(confPath string, buf *bytes.Buffer) (string, error) {
	var err error
	if dir, err := getDockerGraphByFile(confPath, buf); err == nil {
		return dir, err
	}
	if dir, err := getDockerGraphBySocket(); err == nil {
		return dir, err
	}
	return "", err
}
func getDockerGraphByFile(confPath string, buf *bytes.Buffer) (string, error) {
	f, err := os.Open(confPath)
	if err != nil {
		return "", fmt.Errorf("cannot find docker config")
	}
	defer f.Close()
	buf.Reset()
	_, err = buf.ReadFrom(f)
	if err != nil {
		return "", err
	}
	content := buf.Bytes()
	for {
		index := bytes.IndexRune(content, '\n')
		if index <= 0 {
			break
		}
		line := content[:index]
		content = content[index+1:]
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if bytes.HasPrefix(line, []byte("ExecStart")) {
			toks := bytes.Fields(line)
			for _, tok := range toks {
				kv := bytes.FieldsFunc(tok, func(r rune) bool {
					return r == '='
				})
				if len(kv) == 2 {
					if strings.TrimSpace(string(kv[0])) == "--graph" {
						return path.Join(strings.TrimSpace(string(kv[1])), "containers"), nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("cannot find docker graph")
}

func getDockerGraphBySocket() (string, error) {
	c, err := client.NewClient(unixDockerSock, "", nil, nil)
	if err != nil {
		return "", err
	}
	defer c.Close()
	info, err := c.Info(context.Background())
	if err != nil {
		return "", err
	}
	return filepath.Join(info.DockerRootDir, "containers"), nil
}

// DiscoverRun start discover
func (d *DockerDiscoverer) DiscoverRun(wait *sync.WaitGroup) {
	wait.Add(1)
	defer wait.Done()

	// so hack for hardcode, this is docker default config path in systemd
	dockerConfPath := "/etc/systemd/system/docker.service.d/docker.conf"
	d.running = true
	dockerGraph, err := getDockerGraph(dockerConfPath, &d.buf)
	if err == nil {
		d.WorkDir = dockerGraph
	} else {
		log.Error("[docker_discoverer] getDockerGraph failed", log.String("err", err.Error()))
	}
	d.enumerateContainer()
}

func getDockerEnv(env string, key string, envs map[string]string) bool {
	strlen := len(env)
	if strings.HasPrefix(env, key) {
		pos := strings.IndexByte(env, '=')
		if pos >= 0 && pos+1 < strlen {
			envs[key] = env[pos+1:]
			return true
		}
	}
	return false
}

func getContainerInfo(content []byte) *ContainerInfo {
	var container ContainerInfo
	container.Labels = make(map[string]string)
	container.Env = make(map[string]string)
	running := false
	paused := false
	jsonparser.ObjectEach(content, func(key, value []byte, _ jsonparser.ValueType, _ int) error {
		tmpkey := string(key)
		if tmpkey == "State" {
			running, _ = jsonparser.GetBoolean(value, "Running")
			paused, _ = jsonparser.GetBoolean(value, "Paused")
		} else if tmpkey == "Name" {
			container.Name = string(value)
		} else if tmpkey == "MountPoints" {
			mounts := value
			jsonparser.ObjectEach(mounts,
				func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
					mount := value
					var dest, source string
					jsonparser.ObjectEach(mount,
						func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
							if string(key) == "Destination" {
								dest = string(value)
							} else if string(key) == "Source" {
								source = string(value)
							}
							return nil
						})
					container.Env[dest] = source
					return nil
				})
		} else if tmpkey == "Config" {
			configs := value
			jsonparser.ObjectEach(configs, func(key, value []byte, _ jsonparser.ValueType, _ int) error {
				if string(key) == "Labels" {
					labels := value
					jsonparser.ObjectEach(labels, func(key, value []byte, _ jsonparser.ValueType, _ int) error {
						container.Labels[string(key)] = string(value)
						return nil
					})
				} else if string(key) == "Env" {
					envs := value
					hasHostLogDir := false
					hasHostDataDir := false
					hasPbdNumber := false
					hasEngineType := false
					hasDataVersion := false
					hasPort := false
					hasPerfBusinessType := false
					jsonparser.ArrayEach(envs, func(value []byte, _ jsonparser.ValueType, _ int, _ error) {
						env := string(value)
						if !hasHostLogDir && getDockerEnv(env, "host_log_dir", container.Env) {
							hasHostLogDir = true
							return
						}
						if !hasHostDataDir && getDockerEnv(env, "host_data_dir", container.Env) {
							hasHostDataDir = true
							return
						}
						if !hasPbdNumber && getDockerEnv(env, "pbd_number", container.Env) {
							hasPbdNumber = true
							return
						}
						if !hasEngineType && getDockerEnv(env, "engine_type", container.Env) {
							hasEngineType = true
							return
						}
						if !hasDataVersion && getDockerEnv(env, "data_version", container.Env) {
							hasDataVersion = true
							return
						}
						if !hasPort && getDockerEnv(env, "port", container.Env) {
							hasPort = true
							return
						}
						if !hasPerfBusinessType && getDockerEnv(env, "PERF_BUSINESS_TYPE", container.Env) {
							hasPerfBusinessType = true
							return
						}

						return
					})
				}
				return nil
			})
		} else {
		}
		return nil
	})
	container.Running = running && !paused
	return &container
}

func patchContainerInfoWithHostConfig(container *ContainerInfo, content []byte) *ContainerInfo {
	jsonparser.ObjectEach(content, func(key, value []byte, _ jsonparser.ValueType, _ int) error {
		tmpkey := string(key)
		if tmpkey == "CgroupParent" {
			container.CgroupParentPath = string(value)
		}
		return nil
	})
	return container
}

func readFile(filename string, buf *bytes.Buffer) error {
	buf.Reset()
	fp, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = buf.ReadFrom(fp)
	if err != nil {
		return err
	}

	return nil
}

func (d *DockerDiscoverer) enumerateContainer() {
	activeContainers := make(map[string]*ContainerInfo)
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
				log.Error("[docker_discoverer] getDockerGraph failed", log.String("err", err.Error()))
			}
		}

		log.Info("[docker_discoverer] start find new container for", log.String("target", d.WorkDir))
		if !d.running {
			d.ContainerQueue <- nil
			log.Error("[docker_discoverer] stop...", log.String("target", d.WorkDir))
			break
		}
		for k := range activeContainers {
			delete(activeContainers, k)
		}

		file, err := os.Open(d.WorkDir)
		if err != nil {
			log.Info("[docker_discoverer] open workdir failed",
				log.String("path", d.WorkDir),
				log.String("err", err.Error()))
			goto SLEEP_INTERVAL
		}
		containersFi, err = file.Readdirnames(-1)
		file.Close()
		if err != nil {
			log.Error("[docker_discoverer] Readdirnames failed",
				log.String("path", d.WorkDir),
				log.String("err", err.Error()))
			goto SLEEP_INTERVAL
		}

		for _, containerID := range containersFi {
			// scan containerInfo config file
			configfile := fmt.Sprintf("%s/%s/config.v2.json", d.WorkDir, containerID)

			// file not exists or it is a directory, skip
			if fi, err := os.Stat(configfile); err != nil || fi.IsDir() {
				log.Info("[docker_discoverer] file not found",
					log.String("file", configfile))
				continue
			}

			err := readFile(configfile, &d.buf)
			if err != nil {
				log.Error("[docker_discoverer] read file %s failed",
					log.String("file", configfile),
					log.String("err", err.Error()))
				continue
			}
			content := d.buf.Bytes()

			tmpContainerInfo := getContainerInfo(content)
			tmpContainerInfo.ContainerID = containerID
			containerInfo, ok := d.containers[containerID]
			if !ok {
				containerInfo = tmpContainerInfo
				if !containerInfo.Running {
					log.Info("[docker_discoverer] container not running",
						log.String("id", containerInfo.ContainerID), log.String("config", configfile))
					continue
				}
				containerInfo.FirstRun = true
				containerInfo.Stop = make(chan bool, 1)

				role, ok := containerInfo.Labels[consts.ApsaraMetricRole]
				if ok && role == "parent" {
					d.containers[containerID] = containerInfo
					d.notifyInstanceChange(containerInfo)
					log.Info("[docker_discoverer]  container first running",
						log.String("containerId", containerInfo.ContainerID),
						log.Bool("Running", containerInfo.Running))
				}
			} else {
				if containerInfo.Running && !tmpContainerInfo.Running {
					containerInfo.Running = false
					log.Info("[docker_discoverer] stopped container found",
						log.String("containerId", containerID),
						log.Bool("Running", containerInfo.Running))
				}
				d.notifyInstanceChange(containerInfo)
			}
			activeContainers[containerInfo.ContainerID] = containerInfo
		}

		// notify runner to stop this instance
		for containerID, containerInfo := range d.containers {
			keep := false
			if _, ok := activeContainers[containerID]; ok {
				keep = true
			}

			if !keep {
				log.Info("[docker_discoverer] remove container",
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

func (d *DockerDiscoverer) notifyInstanceChange(containerInfo *ContainerInfo) {
	if !containerInfo.Running {
		delete(d.containers, containerInfo.ContainerID)
	}
	d.ContainerQueue <- containerInfo
}

// DiscoverStop should be called at runner Stop()
func (d *DockerDiscoverer) DiscoverStop() error {
	log.Info("[docker_discoverer] DiscoverStop", log.String("workdir", d.WorkDir))
	d.running = false
	d.stopEvent <- true
	return nil
}

// DiscoverExit should be called at runner Exit()
func (d *DockerDiscoverer) DiscoverExit() error {
	close(d.ContainerQueue)
	return nil
}
