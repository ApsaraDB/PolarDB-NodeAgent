/*-------------------------------------------------------------------------
 *
 * ssh_client.go
 *    SSH client
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
 *           common/polardb_pg/utils/ssh_client.go
 *-------------------------------------------------------------------------
 */
package utils

import (
    "bytes"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
    "strconv"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
)

const (
    SSHPort = 22
    PublicKeyFileName = "/root/.ssh/id_rsa"
    Password = "12345678"
)

type SSHClient struct {
    hostname string
    port int
    username string
    password string
    publickeyfile string
    client ssh.Client
}

func NewSSHClient(hostname string, username string) *SSHClient {
    return &SSHClient{
        hostname: hostname,
        port: SSHPort,
        username: username,
        password: Password,
        publickeyfile: PublicKeyFileName,
    }
}

func (c *SSHClient) Connect() error {
	// SSH client config
	config := ssh.ClientConfig{
		User: c.username,
		Auth: []ssh.AuthMethod{
			PublicKeyFile(c.publickeyfile),
			ssh.Password(c.password),
		},
		// Non-production only
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// Connect to host
	client, err := ssh.Dial("tcp", c.hostname+":"+strconv.Itoa(c.port), &config)
	if err != nil {
		log.Error("ssh connect failed", log.String("error", err.Error()))
		return err
	}

    c.client = *client
    return nil
}

func (c *SSHClient) Close() error {
    if err := c.client.Close(); err != nil {
        log.Error("ssh close connection failed", log.String("error", err.Error()))
        return err
    }

    return nil
}

func (c *SSHClient) Host() string {
    return c.hostname
}

func (c *SSHClient) ExecCommand(cmd string) (string, error) {
	// Create sesssion
	session, err := c.client.NewSession()
	if err != nil {
		log.Error("ssh create new session failed", log.String("error", err.Error()))
		return "", err
	}
	defer session.Close()

	var out bytes.Buffer
	session.Stdout = &out

	if err = session.Run(cmd); err != nil {
		log.Error("ssh run command failed", log.String("command", cmd), log.String("error", err.Error()), log.String("errmsg", out.String()))
		return "", err
	}

	return out.String(), nil
}

func PublicKeyFile(file string) ssh.AuthMethod {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("failed to read ssh key file", log.String("filename", file), log.String("error", err.Error()))
		return nil
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		log.Error("failed to parse private key", log.String("error", err.Error()))
		return nil
	}

	return ssh.PublicKeys(key)
}

