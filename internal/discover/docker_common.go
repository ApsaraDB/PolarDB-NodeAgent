/*-------------------------------------------------------------------------
 *
 * docker_common.go
 *    docker discover common struct
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
 *           internal/discover/docker_common.go
 *-------------------------------------------------------------------------
 */
package discover

import "bytes"

// ContainerInfo struct
type ContainerInfo struct {
	FirstRun         bool
	Running          bool
	ContainerID      string
	Env              map[string]string
	Labels           map[string]string
	Stop             chan bool
	Name             string
	CgroupParentPath string
	PodContainerInfo map[string]string
}

// RawDockerDiscoverer docker discover define
type RawDockerDiscoverer struct {
	running        bool
	stopEvent      chan bool
	containers     map[string]*ContainerInfo
	WorkDir        string
	Target         string
	ContainerQueue chan *ContainerInfo
	buf            bytes.Buffer
}
