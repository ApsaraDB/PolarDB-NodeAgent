/*-------------------------------------------------------------------------
 *
 * oom_controller.go
 *    OOM controller in k8s environment
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
 *           plugins/polardb_pg_opensource/resource/process/oom_controller.go
 *-------------------------------------------------------------------------
 */
package process

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
)

type OOMController struct {
	logger *log.PluginLogger
}

func NewOOMController() *OOMController {
	c := &OOMController{}

	return c
}

func (c *OOMController) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	c.logger = logger

	if err := c.initOOMScore(m); err != nil {
		c.logger.Warn("change oom score failed", err)
	}

	return nil
}

func (c *OOMController) changeOOMScore(name string, pid int64) error {
	if err := ioutil.WriteFile(fmt.Sprintf("/proc/%d/oom_score_adj", int(pid)), []byte("-1000"), 0644); err != nil {
		c.logger.Warn("write oom adj failed", err,
			log.String("name", name),
			log.String("file", fmt.Sprintf("/proc/%d/oom_adj", int(pid))))
		return err
	}

	c.logger.Info("change oom score success", log.String("name", name), log.Int64("pid", pid))

	return nil
}

func (c *OOMController) initOOMScore(m map[string]interface{}) error {
	envs := m["env"].(map[string]string)

	getContainerCgroupPath := func(path string, name string, envs map[string]string) string {
		return path + "/../" + envs[fmt.Sprintf("pod_container_%s_containerid", name)]
	}

	// 1. pause
	pausePath := getContainerCgroupPath(envs[consts.CGroupCpuPath], "POD", envs)

	content, err := ioutil.ReadFile(pausePath + "/tasks")
	if err != nil {
		c.logger.Warn("read pause tasks file failed", err)
	} else {
		if pid, err := strconv.ParseInt(strings.TrimSuffix(string(content), "\n"), 10, 64); err != nil {
			c.logger.Warn("tasks file content cannot be parsed to int64", err,
				log.String("path", pausePath+"/tasks"),
				log.String("content", string(content)))
		} else {
			if err := c.changeOOMScore("pause", pid); err != nil {
				c.logger.Warn("change puase oom score failed", err,
					log.Int64("pid", pid))
			}
		}
	}

	// 2. postmaster
	// content, err = ioutil.ReadFile(c.DataDir + "/postmaster.pid")
	// if err != nil {
	// 	c.logger.warn("read postmaster pid failed", err)
	// } else {
	// 	for _, line := range(strings.Split(string(content), "\n")) {
	// 		if pid, err := strconv.ParseInt(line, 10, 64); err != nil {
	// 			c.logger.warn("postmaster pid content cannot be parsed to int64", err, log.String("content", string(content)))
	// 		} else {
	// 			if err := c.changeOOMScore("postmaster", pid); err != nil {
	// 				c.logger.warn("change postmaster oom score failed", err,
	// 						log.Int64("pid", pid))
	// 			}
	// 		}
	// 		break
	// 	}
	// }

	return nil
}

func (c *OOMController) Collect(out map[string]interface{}) error {
	return nil
}

func (c *OOMController) Stop() error {
	return nil
}
