/*-------------------------------------------------------------------------
 *
 * ctrl.go
 *    Ctrl interface for db-monitor
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
 *           internal/ctrl/ctrl.go
 *-------------------------------------------------------------------------
 */
package ctrl

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
	"unicode"

	"github.com/ApsaraDB/db-monitor/common/consts"
	"github.com/ApsaraDB/db-monitor/common/system"
	"github.com/ApsaraDB/db-monitor/internal/gather"

	"github.com/ApsaraDB/db-monitor/common/log"
)

type Ctrl struct {
	route     map[string]func(http.ResponseWriter, *http.Request)
	http      *http.Server
	addr      string
	status    map[string]interface{}
	stopEvent chan bool

	cpu             *system.Cpu
	mem             *system.Mem
	rawPre          map[string]uint64 // raw value last time
	buf             bytes.Buffer
	lastCollectTime time.Time
	cpuUsageList    *list.List
	memUsedList     *list.List
	extern          *Extern
}

type ConfigCenter struct {
	Endpoint     string `json:"endpoint"`
	PullInterval int    `json:"pull_interval"`
}

type Extern struct {
	ConfigCenter *ConfigCenter          `json:"config_center"`
	RouteMap     map[string]string      `json:"route_map"`
	Context      map[string]interface{} `json:"context"`
}

func (c *Ctrl) Init() {
	c.route = make(map[string]func(http.ResponseWriter, *http.Request))
	c.http = nil
	c.addr = "127.0.0.1:9070"
	c.status = make(map[string]interface{})
	c.stopEvent = make(chan bool, 1)

	c.cpu = system.NewCpu(&c.buf)
	c.mem = system.NewMem(&c.buf)
	c.cpuUsageList = list.New()
	c.memUsedList = list.New()
}

func (c *Ctrl) GetModuleStatus() []byte {
	// TODO c.status[name] -> []byte
	return nil
}

func (c *Ctrl) GetStatus(w http.ResponseWriter, req *http.Request) {
	output := c.GetModuleStatus()
	w.Write(output)
}

func (c *Ctrl) AddRoute(url string, cb func(http.ResponseWriter, *http.Request)) {
	if _, ok := c.route[url]; !ok {
		c.route[url] = cb
		log.Info("[ctrl] add route", log.String("url", url))
	} else {
		log.Warn("[ctrl] has same route, ignore current", log.String("url", url))
	}
}

func (c *Ctrl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Info("[c] received request", log.String("url", req.URL.String()))
	if cb, ok := c.route[req.URL.Path]; ok {
		cb(w, req)
	} else {
		w.Write([]byte("404 Not Found"))
	}
}

func (c *Ctrl) Start() {
	go c.checkResourcesLimit()

	c.http = &http.Server{
		Addr:    c.addr,
		Handler: c,
	}

	if err := c.http.ListenAndServe(); err != nil {
		log.Error("[ctrl] stopped", log.String("addr", c.addr), log.String("err", err.Error()))
	}
}

func (c *Ctrl) Stop() {
	log.Info("[ctrl] Stop")
	c.stopEvent <- true
	c.http.Shutdown(context.Background())

	c.cpu.Close()
	c.mem.Close()

}

func (c *Ctrl) LoadCtrlPlugin(mInfo *gather.ModuleInfo, pInfo *gather.PluginInfo) error {
	ext, err := externConfParse(pInfo.Extern)
	if err != nil {
		log.Error("[ctrl] module extern conf parse failed",
			log.String("name", pInfo.Name),
			log.String("path", pInfo.Path),
			log.String("err", err.Error()))
		return err
	}

	c.extern = ext
	if err = c.registerRoute(ext, mInfo, pInfo); err != nil {
		return err
	}

	// TODO pull config center should move to ctrl
	//if err = c.runCtrlPlugin(ext, mInfo, pInfo); err != nil {
	//	return err
	//}
	return nil
}

func (c *Ctrl) runCtrlPlugin(ext *Extern, mInfo *gather.ModuleInfo, pInfo *gather.PluginInfo) error {
	// initialize control plugin and run it
	ctx := make(map[string]interface{})
	ctx["endpoint"] = ext.ConfigCenter.Endpoint
	ctx["pullInterval"] = &ext.ConfigCenter

	init := mInfo.PluginABI.Init
	initCtx, err := init(ctx)
	if err != nil {
		log.Error("[ctrl] plugin not running",
			log.String("name", pInfo.Name),
			log.String("err", err.Error()))
		return err
	}

	mInfo.Contexts.Store(pInfo.Name, initCtx)

	run := mInfo.PluginABI.Run
	go func() {
		err := run(initCtx, nil)
		if err != nil {
			log.Error("[ctrl] run plugin result", log.String("err", err.Error()))
		}
	}()
	return nil
}

func (c *Ctrl) registerRoute(ext *Extern, mInfo *gather.ModuleInfo, pInfo *gather.PluginInfo) error {
	for _, fn := range ext.RouteMap {
		if url, ok := mInfo.Eat[fn]; !ok {
			return fmt.Errorf("plugin route not found,url:%s, function: %s, path: %s", url, fn, pInfo.Path)
		}
	}

	for url, fn := range ext.RouteMap {
		log.Info("[ctrl] add handler",
			log.String("name", pInfo.Name),
			log.String("detail", fmt.Sprintf("%s@%s:%s ==> %s/%s", fn, pInfo.Path, pInfo.Name, c.addr, url)))
		c.AddRoute("/"+url, mInfo.Eat[fn].(func(http.ResponseWriter, *http.Request)))
	}

	return nil
}

func (c *Ctrl) checkResourcesLimit() {
	if c.rawPre == nil {
		c.rawPre = make(map[string]uint64, 8)
	}

	ticker := time.NewTicker(10 * time.Second)
	lastCollectTime := time.Now()
	out := make(map[string]uint64)
	stop := false
	continuePoints, cpuThreshold, memThreshold := 10.0, 100.0, 500.0
	mem, err := c.getHostMemory()
	if err != nil {
		log.Error("[ctrl] getHostMemory failed", log.String("err", err.Error()))
	} else {
		if mem/1024 < 1024 {
			memThreshold = float64(mem / 1024)
		} else {
			memThreshold = 4096.0
		}
	}
	for {
		select {
		case <-c.stopEvent:
			stop = true
			break
		case <-ticker.C:
			if c.extern != nil {
				continuePoints = c.extern.Context["continue_points"].(float64)
				cpuThreshold = c.extern.Context["cpu_threshold"].(float64)
				memThreshold = c.extern.Context["mem_threshold_in_MB"].(float64)
			}
			break
		}

		if stop {
			ticker.Stop()
			log.Info("[ctrl] checkResourcesUsage stopped")
			break
		}
		if c.extern != nil {
			continuePoints = c.extern.Context["continue_points"].(float64)
			cpuThreshold = c.extern.Context["cpu_threshold"].(float64)
			memThreshold = c.extern.Context["mem_threshold_in_MB"].(float64)
		} else {
			log.Info("[ctrl] checkResourcesUsage extern is nil")
		}
		for k := range out {
			delete(out, k)
		}
		err := c.collectSys(out)
		now := time.Now()
		if err != nil {
			log.Error("[ctrl] checkResourcesUsage collectSys failed", log.String("error", err.Error()))
			continue
		}

		uTimeDelta, uTimeDeltaOk := out["cpu_user_delta"]
		sTimeDelta, sTimeDeltaOk := out["cpu_sys_delta"]
		clkTck, clkTclOk := out[consts.MetricNameClkTck]
		memResidentSize, memResidentSizeOk := out[consts.MetricNameMemResident]
		if memResidentSizeOk && uTimeDeltaOk && sTimeDeltaOk && clkTclOk {
			delta := time.Now().Sub(lastCollectTime).Seconds()
			cpuUsage := float64(uTimeDelta+sTimeDelta) / float64(clkTck) / delta * 100
			c.cpuUsageList.PushBack(cpuUsage)
			c.memUsedList.PushBack(memResidentSize)
			log.Info("[ctrl] check self resource usage", 
				log.String("cpu usage (%)", fmt.Sprintf("%.2f", cpuUsage)), log.Float64("cpu threshold (%)", cpuThreshold),
				log.Uint64("mem used (MB)", uint64(memResidentSize / 1024 / 1024)), log.Float64("mem threshold (MB)", memThreshold))
		}
		lastCollectTime = now

		if c.cpuUsageList.Len() < int(continuePoints) || c.memUsedList.Len() < int(continuePoints) {
			continue
		}

		cpuOverLoad := true
		for e := c.cpuUsageList.Front(); e != nil; e = e.Next() {
			if e.Value.(float64) < cpuThreshold {
				cpuOverLoad = false
				break
			}
		}
		memOverLoad := true
		for e := c.memUsedList.Front(); e != nil; e = e.Next() {
			if e.Value.(uint64) < uint64(memThreshold)*1048576 { //1048576=1024*1024
				memOverLoad = false
				break
			}
		}

		if c.cpuUsageList.Len() > 9 {
			if head := c.cpuUsageList.Front(); head != nil {
				c.cpuUsageList.Remove(head)
			}
			if head := c.memUsedList.Front(); head != nil {
				c.memUsedList.Remove(head)
			}
		}

		if cpuOverLoad || memOverLoad {
			log.Info("[ctrl] Agent is using too much resources, and going to die", log.Float64("cpu_threshold", cpuThreshold), log.Float64("mem_threshold", memThreshold))
			os.Exit(1)
		}
	}
}

func (c *Ctrl) getHostMemory() (int64, error) {
	var err error
	var mem int64
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	c.buf.Reset()
	_, err = c.buf.ReadFrom(file)
	if err != nil {
		return 0, err
	}
	buf := c.buf.Bytes()
	for {
		index := bytes.IndexRune(buf, '\n')
		if index <= 0 {
			break
		}

		fields := bytes.FieldsFunc(buf[:index], func(r rune) bool {
			return r == ':' || unicode.IsSpace(r)
		})
		buf = buf[index+1:]

		if len(fields) < 2 {
			continue
		}
		if bytes.Equal(fields[0], []byte("MemTotal")) {
			m, err := strconv.Atoi(string(fields[1]))
			if err != nil {
				return mem, err
			}
			mem = int64(m)
			break
		}
	}
	return mem, nil
}

func externConfParse(file string) (*Extern, error) {
	var ext Extern
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(content, &ext)
	if err != nil {
		return nil, err
	}
	return &ext, nil
}

// 通用逻辑，后续考虑抽象出来
func (c *Ctrl) collectSys(out map[string]uint64) error {
	err := c.cpu.CpuStatByPid(uint32(os.Getpid()))
	if err != nil {
		return err
	}

	out[consts.MetricNameCPUUser] = c.cpu.Stat.Utime
	out[consts.MetricNameCPUSys] = c.cpu.Stat.Stime
	out[consts.MetricNameClkTck] = uint64(c.cpu.ClkTck)

	c.calcDelta(consts.MetricNameCPUUser, out, c.cpu.Stat.Utime)
	c.calcDelta(consts.MetricNameCPUSys, out, c.cpu.Stat.Stime)
	// mem
	err = c.mem.MemStatByPid(uint32(os.Getpid()))
	if err != nil {
		return err
	}

	out[consts.MetricNameMemSize] = c.mem.Stat.Size * c.mem.PageSize
	out[consts.MetricNameMemResident] = c.mem.Stat.Resident * c.mem.PageSize
	out[consts.MetricNameMemShared] = c.mem.Stat.Shared * c.mem.PageSize

	return nil
}

func (c *Ctrl) calcDelta(name string, out map[string]uint64, value uint64) {
	c.buf.Reset()
	c.buf.WriteString(name)
	c.buf.WriteString(consts.MetricNameSuffixDelta)
	pre := c.rawPre[name]

	// 容错
	delta := value - pre
	if value < pre {
		delta = 0
	}

	out[c.buf.String()] = delta
	// update previous data
	c.rawPre[name] = value
}
