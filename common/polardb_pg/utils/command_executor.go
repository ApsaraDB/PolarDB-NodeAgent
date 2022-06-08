/*-------------------------------------------------------------------------
 *
 * command_executor.go
 *    command line executor
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
 *           common/polardb_pg/utils/command_executor.go
 *-------------------------------------------------------------------------
 */
package utils

import (
	"os/exec"
	"strings"
)

type CommandExecutor struct {
}

func NewCommandExecutor() *CommandExecutor {
	return &CommandExecutor{}
}

func (c *CommandExecutor) Init() error {
	return nil
}

func (c *CommandExecutor) ExecCommand(cmd string) (string, error) {
	var res []byte
	var err error

	arglist := strings.Fields(cmd)

	commander := exec.Command(arglist[0], arglist[1:]...)
	if res, err = commander.Output(); err != nil {
		return "", err
	}

	return string(res), nil
}

func (c *CommandExecutor) Close() error {
	return nil
}
