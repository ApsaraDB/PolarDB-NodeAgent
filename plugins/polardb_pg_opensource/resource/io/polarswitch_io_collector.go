/*-------------------------------------------------------------------------
 *
 * polarswitch_io_collector.go
 *    collect polar switch IO stat
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
 *           plugins/polardb_pg_opensource/resource/io/polarswitch_io_collector.go
 *-------------------------------------------------------------------------
 */
package io

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"

	_ "github.com/lib/pq"
)

const (
	PFSCmdTimeout        = 5
	PLSIOCollectInterval = 15
)

type PbdInfo struct {
	pbdNum       string
	plsPrefix    string
	plsIOInfoMap *sync.Map
}

type PlsIOCollector struct {
	hasInit     bool
	InsName     string
	DBType      string
	Environment string
	pbdInfo     PbdInfo
	logger      *log.PluginLogger
	stopped     chan int
}

func NewPlsIOCollector() *PlsIOCollector {
	collector := &PlsIOCollector{hasInit: false}
	collector.pbdInfo = PbdInfo{
		plsIOInfoMap: &sync.Map{},
	}
	return collector
}

func (p *PlsIOCollector) getPrefix(m map[string]interface{}) string {
	return fmt.Sprintf("%s-1", p.pbdInfo.pbdNum)
}

func (p *PlsIOCollector) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	if !p.hasInit {
		p.hasInit = true

		envs := m["env"].(map[string]string)
		p.InsName = m[consts.PluginContextKeyInsName].(string)
		p.DBType = m["dbtype"].(string)
		p.Environment = m["environment"].(string)

		p.pbdInfo.pbdNum = envs["apsara.metric.store.pbd_number"]
		p.pbdInfo.plsPrefix = p.getPrefix(m)
		p.stopped = make(chan int)
		p.logger = logger

		go p.collectloop()

		p.logger.Info("init polarswitch iocollector successfully",
			log.String("polarswitch info", fmt.Sprintf("%+v", p)))
	} else {
		p.logger.Info("polarswitch collector has already inited")
	}

	return nil
}

func (p *PlsIOCollector) collectloop() {
	ioTimer := time.NewTimer(PLSIOCollectInterval * time.Second)

	p.logger.Info("polarswitch io collect loop start")

	p.collectPlsDiskIO()

	for {
		select {
		case <-p.stopped:
			p.logger.Info("polarswitch io collect loop stop")
			return
		case <-ioTimer.C:
			p.logger.Debug("polarswitch io collect loop")
			p.collectPlsDiskIO()
			ioTimer.Reset(PLSIOCollectInterval * time.Second)
		}
	}
}

func (p *PlsIOCollector) collectPlsDiskIO() error {
	var res string
	var err error

	// pfs iostat
	pfsMagicNum := "4294967292"
	plsadminCmd := fmt.Sprintf("plsadmin iostat %s-3-%s-1", pfsMagicNum, p.pbdInfo.pbdNum)

	if res, err = p.ExecCommand(plsadminCmd); err != nil {
		p.logger.Error("exec plsadmin failed", err, log.String("command", plsadminCmd))
		return err
	} else {
		var iopsR, iopsW, iops float64
		var ioThroughputR, ioThroughputW, ioThroughput float64
		var ioUtil float64
		var avgQueueSize float64
		var ioLatencyR, ioLatencyW float64
		var found bool

		found = false
		for _, line := range strings.Split(res, "\n") {
			if strings.HasPrefix(line, pfsMagicNum) {

				items := strings.Fields(line)

				iopsR = ParseFloat(items[1])
				iopsW = ParseFloat(items[2])
				iops = iopsR + iopsW
				// unit: MB
				ioThroughputR = ParseFloat(items[3])
				ioThroughputW = ParseFloat(items[4])
				ioThroughput = ioThroughputR + ioThroughputW
				ioLatencyR = ParseFloat(items[8])
				ioLatencyW = ParseFloat(items[9])
				ioUtil = ParseFloat(strings.TrimSuffix(items[10], "%"))

				found = true

				break
			}
		}

		if found {
			p.pbdInfo.plsIOInfoMap.Store("pls_iops_read", iopsR)
			p.pbdInfo.plsIOInfoMap.Store("pls_iops_write", iopsW)
			p.pbdInfo.plsIOInfoMap.Store("pls_iops", iops)
			p.pbdInfo.plsIOInfoMap.Store("pls_throughput_read", ioThroughputR)
			p.pbdInfo.plsIOInfoMap.Store("pls_throughput_write", ioThroughputW)
			p.pbdInfo.plsIOInfoMap.Store("pls_throughput", ioThroughput)
			p.pbdInfo.plsIOInfoMap.Store("pls_latency_read", ioLatencyR)
			p.pbdInfo.plsIOInfoMap.Store("pls_latency_write", ioLatencyW)
			p.pbdInfo.plsIOInfoMap.Store("pls_queue_size", avgQueueSize)
			p.pbdInfo.plsIOInfoMap.Store("pls_ioutil", ioUtil)
		} else {
			p.logger.Info("invalid result of plsadmin iostat", log.String("detail", string(res)))
		}
	}

	return nil
}

func (p *PlsIOCollector) collectPlsIOResult(out map[string]interface{}) error {
	rangeFunc := func(key, value interface{}) bool {
		if intv, ok := value.(uint64); ok {
			out[key.(string)] = intv
			return true
		}

		if floatv, ok := value.(float64); ok {
			out[key.(string)] = floatv
			return true
		}

		return true
	}

	p.pbdInfo.plsIOInfoMap.Range(rangeFunc)
	return nil
}

func (p *PlsIOCollector) Collect(out map[string]interface{}) error {
	if err := p.collectPlsIOResult(out); err != nil {
		p.logger.Warn("collect pfs io info failed.", err)
	}

	return nil
}

func (p *PlsIOCollector) Stop() error {
	p.stopped <- 1

	return nil
}

func (p *PlsIOCollector) ExecCommand(cmd string) (string, error) {
	fullcmd := exec.Command("timeout", strconv.Itoa(PFSCmdTimeout), "bash", "-c", cmd)
	res, err := fullcmd.Output()
	if err != nil {
		p.logger.Error("exec command failed", err, log.String("command", cmd))
		return "", err
	}

	return string(res), nil
}

func ParseFloat(s string) float64 {
	var ret float64
	var err error

	if ret, err = strconv.ParseFloat(s, 64); err != nil {
		ret = 0.0
	}

	return ret
}
