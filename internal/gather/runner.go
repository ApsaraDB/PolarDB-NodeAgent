/*-------------------------------------------------------------------------
 *
 * runner.go
 *    Runner interface
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
 *           internal/gather/runner.go
 *-------------------------------------------------------------------------
 */
package gather

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ApsaraDB/db-monitor/common/consts"
	"github.com/ApsaraDB/db-monitor/common/log"
	"github.com/ApsaraDB/db-monitor/common/utils"
)

var gRunnerInstanceIdx int32
var gRunnerIdx int32

// ExternConf struct
type ExternConf struct {
	Schema   string                 `json:"schema"`
	Type     string                 `json:"type"`
	Business string                 `json:"business"`
	Context  map[string]interface{} `json:"context"`
}

// Runner base struct
type Runner struct {
	notify    bool
	running   bool
	fixedKey  string
	schema    *BusinessSchema
	mInfoMap  *sync.Map
	pInfo     *PluginInfo
	pluginCtx *PluginCtx
	instances map[string]*Instance
	//backendCtx interface{}
	backendCtxMap sync.Map // name as key,ctx as value
	status        map[string]string
	externConf    *ExternConf
	index         int32
	buf           bytes.Buffer
}

// Instance base struct
type Instance struct {
	notify     bool
	notifyType int
	running    bool
	port       int
	userName   string
	insPath    string
	insName    string
	insID      string
	env        map[string]string
	mInfo      *ModuleInfo
	msgBuff    bytes.Buffer
	pInfo      *PluginInfo
	dataLogger *log.DataLogger
	crashCount int32
	stop       chan bool
	index      int32
}

// RunnerService base interface
type RunnerService interface {
	RunnerInit(mInfoMap *sync.Map, pInfo *PluginInfo, ctx *PluginCtx) error
	RunnerRun(wait *sync.WaitGroup) error
	RunnerStop()
	RunnerStatus() interface{}
	RunnerNotify()
}

func getNewInstanceID() int32 {
	return atomic.AddInt32(&gRunnerInstanceIdx, 1)
}

func getNewRunnerID() int32 {
	return atomic.AddInt32(&gRunnerIdx, 1)
}

func (runner *Runner) runnerInit(mInfoMap *sync.Map, pInfo *PluginInfo, ctx *PluginCtx) error {
	log.Info("[runner] runner init module", log.String("name", pInfo.Name))
	runner.running = false
	runner.pInfo = pInfo
	runner.mInfoMap = mInfoMap
	runner.pluginCtx = ctx
	runner.schema = nil
	runner.fixedKey = generateFixedKeySegment()
	runner.index = getNewRunnerID()

	runner.instances = make(map[string]*Instance)
	runner.status = make(map[string]string)

	if runner.pInfo.Extern != "" {
		err := runner.initExtern()
		if err != nil {
			return err
		}
	}

	return nil
}

//lazy load module
func (runner *Runner) lazyLoadModule() error {
	var err error
	if err = runner.loadBackendModules(); err != nil {
		return fmt.Errorf("load backend module failed:%+v", err)
	}

	if err = runner.initBackendModules(); err != nil {
		return fmt.Errorf("init backend module failed:%+v", err)
	}

	err = runner.loadAndInitModule()
	if err != nil {
		return fmt.Errorf("load module failed:%+v", err)
	}
	return nil
}

func (runner *Runner) loadBackendModules() error {
	for _, backend := range runner.pInfo.Backend {
		if err := runner.loadSingleBackendModule(backend); err != nil {
			return fmt.Errorf("load single backend module failed:%s", backend)
		}
	}

	if runner.pInfo.ProcessorBackends != nil {
		if err := runner.loadSingleBackendModule(runner.pInfo.Processor); err != nil {
			return fmt.Errorf("load processor %s failed:%v", runner.pInfo.Processor, err)
		}

		for _, backend := range runner.pInfo.ProcessorBackends {
			if err := runner.loadSingleBackendModule(backend); err != nil {
				return fmt.Errorf("load single backend module failed:%s", backend)
			}
		}
	}

	return nil
}

func (runner *Runner) loadSingleBackendModule(backend string) error {
	if _, ok := runner.mInfoMap.Load(backend); ok {
		log.Info("[runner] backend module has already loaded",
			log.String("name", backend))
		return nil
	}
	var pInfo *PluginInfo
	pluginInfo, ok := runner.pluginCtx.Plugins.Load(backend)
	if ok {
		pInfo = pluginInfo.(*PluginInfo)
	} else {
		return fmt.Errorf("runner backend module not found:%s", backend)
	}
	mInfo := new(ModuleInfo)

	// load .so
	err := mInfo.ModuleInit(pInfo)
	if err != nil {
		log.Error("[universe] module init failed",
			log.String("name", pInfo.Name),
			log.String("err", err.Error()))
		return err
	}
	log.Info("[universe] module init success",
		log.String("name", pInfo.Name))
	runner.mInfoMap.Store(pInfo.Name, mInfo)

	return nil
}

// init extern conf: include golang_extern_xxx.conf and xxx_schema.json
func (runner *Runner) initExtern() error {
	var externConf *ExternConf
	externConf, err := readExternConf(runner.pInfo.Extern, &runner.buf)
	if err != nil {
		return fmt.Errorf("readExternConf:%s failed %s", runner.pInfo.Extern, err.Error())
	}
	runner.externConf = externConf
	// no schema specified
	if externConf.Schema == "" {
		log.Info("[runner] no schema specified", log.String("module", runner.pInfo.Name),
			log.String("extern", runner.pInfo.Extern))
		return nil
	}

	// load schema
	fp, err := os.Open(externConf.Schema)
	if err != nil {
		return err
	}
	defer fp.Close()
	runner.buf.Reset()
	_, err = runner.buf.ReadFrom(fp)
	if err != nil {
		return err
	}
	schema, err := JSON2Schema(runner.buf.Bytes(), &runner.buf)
	if err != nil {
		log.Error("[runner] parse schema failed : ", log.String("module", runner.pInfo.Name), log.String("error", err.Error()))
		return err
	}
	log.Info("[runner] parse schema has finished : ", log.String("module", runner.pInfo.Name), log.String("externConf", externConf.Schema), log.Uint32("version", schema.Version))
	runner.schema = schema
	return nil
}

// init collector plugin
func (runner *Runner) loadAndInitModule() error {
	if _, ok := runner.mInfoMap.Load(runner.pInfo.Name); ok {
		log.Info("[runner] module has already loaded",
			log.String("name", runner.pInfo.Name),
			log.String("path", runner.pInfo.Path))
	} else {
		// load a new module instance
		mInfo := new(ModuleInfo)
		err := mInfo.ModuleInit(runner.pInfo)
		if err != nil {
			return err
		}

		err = runner.injectModule(mInfo)
		if err != nil {
			return err
		}

		log.Info("[runner] module init success",
			log.String("name", runner.pInfo.Name),
			log.String("path", runner.pInfo.Path))

		mInfo.ID = runner.pInfo.Name
		runner.mInfoMap.Store(runner.pInfo.Name, mInfo)
	}

	return nil
}

// inject Iat from other-module.Eat
func (runner *Runner) injectModule(mInfo *ModuleInfo) error {
	for _, identifier := range runner.pInfo.Imports {
		// identifier looks like : module-name.FuncName
		list := strings.Split(identifier, ".")
		if len(list) != 2 {
			return fmt.Errorf("invalid import identifier: %s", identifier)
		}
		depModuleName := list[0]
		var depModuleInfo *ModuleInfo
		info, ok := runner.mInfoMap.Load(depModuleName)
		if !ok {
			return fmt.Errorf("dependency module not found: %s", depModuleName)
		}
		if depModuleInfo, ok = info.(*ModuleInfo); !ok {
			return fmt.Errorf("dependency module is not found:%s", depModuleName)
		}
		fn, ok := depModuleInfo.Eat[identifier]
		if !ok {
			return fmt.Errorf("imported identifier not found: %s", identifier)
		}
		mInfo.Iat[identifier] = fn
	}
	return nil
}

// invoke plugin method: PluginInit()
func (runner *Runner) initModule(ins *Instance) (interface{}, error) {
	//defer func() {
	//	if e := recover(); e != nil {
	//		log.Error("[runner] init module panic",
	//			log.String("err", fmt.Sprintf("%v", e)))
	//	}
	//}()
	ctx, err := runner.initPluginContext(ins)

	if err != nil {
		return nil, err
	}
	init := ins.mInfo.PluginABI.Init
	initCtx, err := init(ctx)
	if err != nil {
		log.Error("[runner] plugin not running",
			log.String("runner", runner.pInfo.Name),
			log.String("target", runner.pInfo.Target),
			log.Int("port", ins.port),
			log.String("err", err.Error()))
	}

	//ins.insCommon.mInfo.InitCtx = initCtx
	return initCtx, err
}

// make a new map for PluginInit ctx, only allocate mem once per instance
func (runner *Runner) initPluginContext(ins *Instance) (map[string]interface{}, error) {
	ctx := make(map[string]interface{})
	iat := make(map[string]interface{})
	for k, v := range ins.mInfo.Iat {
		iat[k] = v
	}
	ctx[consts.PluginContextKeyImportsMap] = iat
	ctx[consts.PluginContextKeyPort] = ins.port
	ctx[consts.PluginContextKeyInsName] = ins.insName
	ctx[consts.PluginContextKeyEnv] = ins.env
	ctx[consts.PluginContextKeyInsId] = ins.insID
	ctx[consts.PluginTargetKey] = runner.pInfo.Target
	ctx[consts.PluginContextKeyInsPath] = ins.insPath
	ctx["runner_id"] = runner.index
	ctx["g_id"] = ins.index
	ctx[consts.PluginIntervalKey] = runner.pInfo.Interval
	if ins.pInfo.Type == "lua" {
		ctx[consts.PluginLuaScriptPath] = ins.pInfo.Path
	}
	if runner.externConf != nil && runner.externConf.Context != nil {
		for k, v := range runner.externConf.Context {
			ctx[k] = v
		}
	}

	// inject initContext of dependencies to this ins.mInfo
	if ins.pInfo.Dependencies != nil {
		var mInfo *ModuleInfo
		for _, v := range ins.pInfo.Dependencies {
			info, ok := runner.mInfoMap.Load(v)
			if !ok {
				return nil, fmt.Errorf("dependency not found: %s", v)
			}
			if mInfo, ok = info.(*ModuleInfo); !ok {
				return nil, fmt.Errorf("dependency not found: %s", v)
			}
			initCtx, ok := mInfo.Contexts.Load(v)
			if !ok {
				return nil, fmt.Errorf("dependency context not found: %s", v)
			}

			ctx[v] = initCtx
		}
	}

	return ctx, nil
}

// init backend module before request to collect
func (runner *Runner) initBackendModules() error {
	for _, backend := range runner.pInfo.Backend {
		if err := runner.initSingleBackendModule(backend); err != nil {
			return fmt.Errorf("backend init failed: %s", backend)
		}
	}

	if runner.pInfo.ProcessorBackends != nil {
		if runner.pInfo.Processor != "" {
			if err := runner.initSingleBackendModule(runner.pInfo.Processor); err != nil {
				return fmt.Errorf("processor %s init failed: %s", runner.pInfo.Processor, err)
			}
		}

		for _, backend := range runner.pInfo.ProcessorBackends {
			if err := runner.initSingleBackendModule(backend); err != nil {
				return fmt.Errorf("multi backends %s init failed: %s", backend, err)
			}
		}
	}

	return nil
}

func (runner *Runner) initSingleBackendModule(backendname string) error {
	ctx := make(map[string]interface{})
	backend, ok := runner.mInfoMap.Load(backendname)
	if !ok {
		return fmt.Errorf("backend not found:%s", backendname)
	}
	var moduleInfoBackend *ModuleInfo
	if moduleInfoBackend, ok = backend.(*ModuleInfo); !ok {
		return fmt.Errorf("module info backend not found:%s", backendname)
	}
	ctx[consts.PluginExternKey] = moduleInfoBackend.Extern
	// db_type
	ctx[consts.PluginDBTypeKey] = runner.pInfo.DbType
	//TODO add to common consts
	ctx["business_type"] = runner.pInfo.BusinessType
	ctx[consts.SchemaVersionKey] = runner.schema.Version
	ctx[consts.ExternTypeKey] = runner.externConf.Type
	ctx[consts.ExternBusinessKey] = runner.externConf.Business
	ctx[consts.ErrorFile] = utils.GetBasePath() + "/log/backend_status_"

	backendCtx, err := moduleInfoBackend.PluginABI.Init(ctx)
	if err != nil {
		log.Error("[runner]init backend failed",
			log.String("name", backendname),
			log.String("err", err.Error()))
	}
	moduleInfoBackend.Contexts.Store(backendname, backendCtx)
	return err
}

// invoke plugin method: PluginExit()
func (runner *Runner) exitModule(initCtx interface{}, ins *Instance) error {
	if ins.dataLogger != nil {
		ins.dataLogger.FlushData()
	}

	if err := runner.exitProcessorBackends(initCtx, ins); err != nil {
		log.Warn("[runner] exit backend failed")
	}

	return ins.mInfo.PluginABI.Exit(initCtx)
}

// exit backend
func (runner *Runner) exitBackendModule() error {
	var err error
	for _, backend := range runner.pInfo.Backend {
		backendModule, ok := runner.mInfoMap.Load(backend)
		if !ok {
			return fmt.Errorf("module for backend:%s not found", backend)
		}
		var moduleInfoBackend *ModuleInfo
		if moduleInfoBackend, ok = backendModule.(*ModuleInfo); !ok {
			return fmt.Errorf("module info backend not match:%s", backend)
		}
		backendCtx, ok := moduleInfoBackend.Contexts.Load(backend)
		if !ok {
			return fmt.Errorf("ctx for backend:%s not found,module:%s", backend, runner.pInfo.Name)
		}

		if err = moduleInfoBackend.PluginABI.Exit(backendCtx); err != nil {
			return err
		}
	}
	return nil
}

// invoke plugin method PluginRun
func (runner *Runner) runModule(
	initCtx interface{},
	ins *Instance) error {

	errCount := 0
	backCtxes := make(map[string]interface{})
	backRunMap := make(map[string]func(interface{}, interface{}) error)
	itemNum := 32
	if runner.schema != nil {
		itemNum += len(runner.schema.Metrics.SchemaItems) + 32
	}
	collectHeaderMap := make(map[string]interface{}, 15)
	collectContentMap := make(map[string]interface{}, int64(itemNum))

	// NOTE(wormhole.gl): init backends include all backends
	initBackends := make([]string, 0, len(runner.pInfo.Backend))
	initBackends = append(initBackends, runner.pInfo.Backend...)

	if runner.pInfo.ProcessorBackends != nil && len(runner.pInfo.ProcessorBackends) > 0 {
		if runner.pInfo.Processor != "" {
			initBackends = append(initBackends, runner.pInfo.Processor)
		}

		initBackends = append(initBackends, runner.pInfo.ProcessorBackends...)
	}

	for _, backend := range initBackends {
		module, ok := runner.mInfoMap.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s module not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backendModule, ok := module.(*ModuleInfo)
		if !ok {
			return fmt.Errorf("backend:%s module not match,plugin:%s", backend, ins.pInfo.Name)
		}
		backendRun := backendModule.PluginABI.Run
		backendCtx, ok := backendModule.Contexts.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s ctx not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backCtxMap := make(map[string]interface{}, 8)
		backCtxMap["backend"] = backendCtx
		backCtxMap["port"] = ins.port
		backCtxMap[consts.PluginContextKeyPort] = ins.port
		backCtxMap[consts.PluginContextKeyInsId] = ins.insID
		backCtxMap["business_type"] = runner.externConf.Business
		backCtxMap["db_type"] = runner.pInfo.DbType
		backCtxMap["sep"] = ""
		backCtxMap["initCtx"] = initCtx
		backCtxes[backend] = backCtxMap
		backRunMap[backend] = backendRun
	}

	generateFixedMessageHeaderEntity(runner.schema, runner.pInfo.DbType, runner.externConf.Business, collectHeaderMap, ins)
	mainRun := ins.mInfo.PluginABI.Run

	var collectStartTime int64
	var lastCollectStartTime int64
	var elapsedTimeWithin int64
	var costMillis int64
	var err error

	// interval configured in SECOND
	ticker := time.NewTicker(time.Duration(runner.pInfo.Interval) * time.Second)
	for {
		if !ins.running {
			log.Info("[runner] ins not running, exit now",
				log.Int("port", ins.port), log.String("module", ins.pInfo.Name), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			break
		}
		if ins.notify {
			ins.notify = false
			// TODO notify action
		}
		// 采集开始时间
		collectStartTime = time.Now().UnixNano() / 1e6
		if !ins.running {
			break
		}

		// 1. clean collectContentMap map before invoke PluginRun()
		for key := range collectContentMap {
			delete(collectContentMap, key)
		}
		generateFixedMessageHeaderEntity(runner.schema, runner.pInfo.DbType, runner.externConf.Business, collectContentMap, ins)

		// 2. invoke PluginRun in plugin.so
		runError := mainRun(initCtx, collectContentMap)
		if runError != nil {
			log.Error("[runner] runModule mainRun failed", log.String("err", runError.Error()), log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			errCount++
			if errCount > consts.RunnerCollectorMaxFailCounts {
				break
			}
		} else {
			// 重置errCount
			errCount = 0
		}
		// 采集完成时间
		collectEndTime := time.Now().UnixNano() / 1e6
		// 两次采集实际间隔时间: 本次开始时间 - 上次开始时间
		elapsedTimeWithin = collectStartTime - lastCollectStartTime
		log.Debug("[runner] collect done",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("cost", collectEndTime-collectStartTime), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

		if lastCollectStartTime == 0 {
			lastCollectStartTime = collectStartTime
			goto SLEEP_INTERVAL
		}
		lastCollectStartTime = collectStartTime
		collectHeaderMap[consts.SchemaHeaderInsName] = ins.insName
		collectContentMap[consts.SchemaHeaderInsName] = ins.insName
		//backCtxMap[consts.PluginContextKeyInsName] = ins.insName
		generateMutableMessageHeaderEntity(collectStartTime, collectEndTime, elapsedTimeWithin, collectContentMap)

		// 3. serialize header and body
		if err = MessageHeaderSerializeBySchema(&runner.schema.Headers, ins, collectContentMap); err != nil {
			goto SLEEP_INTERVAL
		}
		if len(collectContentMap) == 0 {
			ins.msgBuff.Reset()
			goto SLEEP_INTERVAL
		}
		if err = DefaultMessageSerializeBySchema(&runner.schema.Metrics, collectContentMap, &ins.msgBuff); err != nil {
			ins.msgBuff.Reset()
			goto SLEEP_INTERVAL
		}

		// NOTE(wormhole.gl): multi backend run
		if runner.pInfo.ProcessorBackends != nil && len(runner.pInfo.ProcessorBackends) > 0 {
			generateMutableMessageHeaderEntity(collectStartTime, collectEndTime, elapsedTimeWithin, collectHeaderMap)
			runner.runProcessorBackends(backCtxes, backRunMap, collectHeaderMap,
				collectContentMap, collectStartTime, ins)
		}

		for _, backend := range ins.pInfo.Backend {
			backRun := backRunMap[backend]
			backCtx := backCtxes[backend]
			backCtx.(map[string]interface{})[consts.PluginContextKeyInsName] = ins.insName
			// 4. send to backend
			if strings.Contains(backend, "json") {
				for k, v := range collectHeaderMap {
					collectContentMap[k] = v
				}
				data, err := json.Marshal(collectContentMap)
				if err != nil {
					log.Error("[runner] aligoup json marshal failed.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
				}
				//ins.msgBuff.Reset()
				//ins.msgBuff.Write(data)
				err = backRun(backCtx, data)
				if err != nil {
					log.Error("[runner] panic backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
				}
			} else {
				err = backRun(backCtx, ins.msgBuff.Bytes())
				if err != nil {
					log.Error("[runner] panic backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
				}
			}
		}
		ins.dataLogger.PrintData(ins.msgBuff.String())
		//// 4. send to backend
		//if backRun != nil {
		//	err = backRun(backCtxMap, ins.msgBuff.Bytes())
		//	if err != nil {
		//		log.Error("[runner] panic backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
		//	}
		//	ins.dataLogger.PrintData(ins.msgBuff.String())
		//}

		// 5. reset buffer
		ins.msgBuff.Reset()
		// 6. post process
		costMillis = time.Now().UnixNano()/1e6 - collectStartTime
		log.Debug("[runner] all costs ms",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("cost", costMillis), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			ticker.Stop()
			ins.stop <- true
			break
		case <-ticker.C:
			break
		}
	}
	return nil
}

// invoke plugin method PluginRun
func (runner *Runner) runModuleM(
	initCtx interface{},
	ins *Instance) error {

	errCount := 0
	backCtxes := make(map[string]interface{})
	backRunMap := make(map[string]func(interface{}, interface{}) error)
	//collectHeaderMap := make(map[string]string, 15)
	packetNum := runner.externConf.Context["packet_num"].(float64)
	itemNum := 32
	if runner.schema != nil {
		itemNum += len(runner.schema.Metrics.SchemaItems) + 32
	}
	//collectHeaderMap := make(map[string]string, 15)
	collectContentMapList := make([]map[string]interface{}, int64(packetNum))
	for k := range collectContentMapList {
		collectContentMapList[k] = make(map[string]interface{}, int64(itemNum))
	}

	for _, backend := range runner.pInfo.Backend {
		module, ok := runner.mInfoMap.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s module not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backendModule, ok := module.(*ModuleInfo)
		if !ok {
			return fmt.Errorf("backend:%s module not match,plugin:%s", backend, ins.pInfo.Name)
		}
		backendRun := backendModule.PluginABI.Run
		backendCtx, ok := backendModule.Contexts.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s ctx not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backCtxMap := make(map[string]interface{}, 8)
		backCtxMap["backend"] = backendCtx
		backCtxMap["port"] = ins.port
		backCtxMap[consts.PluginContextKeyPort] = ins.port
		backCtxMap[consts.PluginContextKeyInsId] = ins.insID
		backCtxMap["business_type"] = runner.externConf.Business
		backCtxMap["db_type"] = runner.pInfo.DbType
		backCtxMap["sep"] = ""
		backCtxMap["id"] = ""
		backCtxes[backend] = backCtxMap
		backRunMap[backend] = backendRun
	}

	mainRun := ins.mInfo.PluginABI.Run
	var collectStartTime int64
	var lastCollectStartTime int64
	var elapsedTimeWithin int64
	var costMillis int64
	var err error

	// interval configured in SECOND
	ticker := time.NewTicker(time.Duration(runner.pInfo.Interval) * time.Second)
	for {
		if !ins.running {
			log.Info("[runner] ins not running, exit now",
				log.Int("port", ins.port), log.String("module", ins.pInfo.Name), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			break
		}
		if ins.notify {
			ins.notify = false
			// TODO notify action
		}
		// 采集开始时间
		collectStartTime = time.Now().UnixNano() / 1e6
		if !ins.running {
			break
		}

		// 1. clean collectContentMap map before invoke PluginRun()
		for _, collectContentMap := range collectContentMapList {
			for key := range collectContentMap {
				delete(collectContentMap, key)
			}
		}

		// 2. invoke PluginRun in plugin.so
		runError := mainRun(initCtx, collectContentMapList)
		if runError != nil {
			log.Error("[runner] runModule mainRun failed", log.String("err", runError.Error()), log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			errCount++
			if errCount > consts.RunnerCollectorMaxFailCounts {
				break
			}
		} else {
			// 重置errCount
			errCount = 0
		}

		// 采集完成时间
		collectEndTime := time.Now().UnixNano() / 1e6
		// 两次采集实际间隔时间: 本次开始时间 - 上次开始时间
		elapsedTimeWithin = collectStartTime - lastCollectStartTime
		log.Debug("[runner] collect done",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("cost", collectEndTime-collectStartTime), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

		if lastCollectStartTime == 0 {
			lastCollectStartTime = collectStartTime
			log.Info("[runner] runModuleM lastCollectStartTime=0,goto sleep", log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			goto SLEEP_INTERVAL
		}
		for _, contentMap := range collectContentMapList {
			if len(contentMap) > 0 {
				generateFixedMessageHeaderEntityForMulti(runner.schema, runner.pInfo.DbType, runner.externConf.Business, contentMap, ins)
				lastCollectStartTime = collectStartTime
				contentMap[consts.SchemaHeaderInsName] = ins.insName
				//backCtxMap[consts.PluginContextKeyInsName] = ins.insName
				generateMutableMessageHeaderEntityForMulti(collectStartTime, collectEndTime, elapsedTimeWithin, contentMap)
			}
		}

		for _, contentMap := range collectContentMapList {
			if len(contentMap) == 0 {
				ins.msgBuff.Reset()
				continue
			}

			if err = MessageHeaderSerializeBySchema(&runner.schema.Headers, ins, contentMap); err != nil {
				log.Info("[runner] runModuleM MessageHeaderSerializeBySchema failed,goto sleep", log.String("err", err.Error()), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
				goto SLEEP_INTERVAL
			}

			if err = DefaultMessageSerializeBySchema(&runner.schema.Metrics, contentMap, &ins.msgBuff); err != nil {
				ins.msgBuff.Reset()
				log.Info("[runner] runModuleM DefaultMessageSerializeBySchema failed,goto sleep", log.String("err", err.Error()), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
				goto SLEEP_INTERVAL
			}

			for _, backend := range ins.pInfo.Backend {
				backRun := backRunMap[backend]
				backCtx := backCtxes[backend]
				backCtx.(map[string]interface{})[consts.PluginContextKeyInsName] = ins.insName
				// 4. send to backend
				if strings.Contains(backend, "json") {
					data, err := json.Marshal(contentMap)
					if err != nil {
						log.Error("[runner] aligoup json marshal failed.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
					}
					//ins.msgBuff.Write(data)
					err = backRun(backCtx, data)
					if err != nil {
						log.Error("[runner] panic backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
					}

				} else {
					err = backRun(backCtx, ins.msgBuff.Bytes())
					if err != nil {
						log.Error("[runner] panic backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
					}
				}
			}
			ins.dataLogger.PrintData(ins.msgBuff.String())

			// 5. reset buffer
			ins.msgBuff.Reset()
		}
		// 6. post process
		costMillis = time.Now().UnixNano()/1e6 - collectStartTime
		log.Debug("[runner] all costs ms",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("cost", costMillis), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			ticker.Stop()
			ins.stop <- true
			break
		case <-ticker.C:
			break
		}
	}
	return nil
}

// invoke plugin method PluginRun
func (runner *Runner) runLogModule(

	initCtx interface{},
	ins *Instance) error {
	var err error

	var collectStartTime int64
	var lastCollectStartTime int64
	var elapsedTimeWithin int64
	var costMillis int64
	var content interface{}

	errCount := 0
	packetID := uint64(0)
	backCtxes := make(map[string]interface{})
	backRunMap := make(map[string]func(interface{}, interface{}) error)
	collectContentMap := make(map[string]interface{}, 32)
	for _, backend := range runner.pInfo.Backend {
		module, ok := runner.mInfoMap.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s module not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backendModule, ok := module.(*ModuleInfo)
		if !ok {
			return fmt.Errorf("backend:%s module not match,plugin:%s", backend, ins.pInfo.Name)
		}
		backendRun := backendModule.PluginABI.Run
		backendCtx, ok := backendModule.Contexts.Load(backend)
		if !ok {
			return fmt.Errorf("backend:%s ctx not found,plugin:%s", backend, ins.pInfo.Name)
		}
		backCtxMap := make(map[string]interface{}, 8)
		backCtxMap["backend"] = backendCtx
		backCtxMap["port"] = ins.port
		backCtxMap[consts.PluginContextKeyPort] = ins.port
		backCtxMap[consts.PluginContextKeyInsId] = ins.insID
		backCtxMap["business_type"] = runner.externConf.Business
		backCtxMap["db_type"] = runner.pInfo.DbType
		backCtxMap["sep"] = ""
		backCtxMap["packet_id"] = ""
		backCtxMap["separator"] = "" // since a line of log has its own separator, extra separator isn't necessary
		backCtxes[backend] = backCtxMap
		backRunMap[backend] = backendRun
	}
	// var encoder *zstd.Encoder
	// var zstdBuffer []byte
	// if ins.pInfo.BizType == "aligroup" && strings.Contains(ins.pInfo.Name, "auditlog") {
	// 	encoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	// 	if err != nil {
	// 		log.Error("[runnner] zstd create writer failed", log.String("err", err.Error()), log.String("module", ins.pInfo.Name), log.Int("port", ins.port))
	// 		return fmt.Errorf("zstd create writer failed,err:%+v", err.Error())
	// 	}
	// 	defer encoder.Close()
	// }

	mainRun := ins.mInfo.PluginABI.Run
	interval := ins.pInfo.Interval
	for {
		if !ins.running {
			log.Info("[runner] ins not running, exit now",
				log.Int("port", ins.port), log.String("module", ins.pInfo.Name), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			break
		}

		if ins.notify {
			ins.notify = false
			// TODO notify action
		}
		// 采集开始时间
		collectStartTime = time.Now().UnixNano() / 1e6

		// 1. clean collectContentMap map before invoke PluginRun()
		for key := range collectContentMap {
			delete(collectContentMap, key)
		}

		// 2. invoke PluginRun in plugin.so
		runError := mainRun(initCtx, collectContentMap)
		if runError != nil {
			log.Error("[runner] runLogModule mainRun failed", log.String("err", runError.Error()), log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))
			errCount++
			if errCount > consts.RunnerCollectorMaxFailCounts {
				break
			}
		} else {
			// 重置errCount
			errCount = 0
		}
		interInterval, ok := collectContentMap["interval"]
		if ok {
			interval = interInterval.(int)
		} else {
			interval = ins.pInfo.Interval
		}
		// 采集完成时间
		collectEndTime := time.Now().UnixNano() / 1e6
		// 两次采集实际间隔时间: 本次开始时间 - 上次开始时间
		elapsedTimeWithin = collectStartTime - lastCollectStartTime
		log.Debug("[runner] runLogModule collect done",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("elapsed", elapsedTimeWithin),
			log.Int64("cost", collectEndTime-collectStartTime), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

		if lastCollectStartTime == 0 {
			lastCollectStartTime = collectStartTime
		}
		lastCollectStartTime = collectStartTime
		// no data needs to be send to backend
		if len(collectContentMap) == 0 {
			ins.msgBuff.Reset()
			goto SLEEP_INTERVAL
		}

		// 3. send log to backend
		if len(collectContentMap) == 0 {
			ins.msgBuff.Reset()
			goto SLEEP_INTERVAL
		}

		content, ok = collectContentMap["msg"]
		if !ok || len(content.([]byte)) == 0 {
			goto SLEEP_INTERVAL
		}

		if ins.pInfo.BizType == "aligroup" && strings.Contains(ins.pInfo.Name, "auditlog") {
			// if zstdBuffer == nil {
			// 	zstdBuffer = make([]byte, 5242880)
			// }
			// tmp := encoder.EncodeAll(content.([]byte), zstdBuffer)
			// ins.msgBuff.Write(tmp)
			// zstdBuffer = zstdBuffer[:0]
			// encoder.Close()
		} else {
			ins.msgBuff.Write(content.([]byte))
		}
		for _, backend := range ins.pInfo.Backend {
			backRun := backRunMap[backend]
			backCtx := backCtxes[backend].(map[string]interface{})
			backCtx[consts.PluginContextKeyInsName] = ins.insName
			if sep, ok := collectContentMap["sep"]; ok {
				backCtx["sep"] = sep
			}
			if tz, ok := collectContentMap["db_timezone"]; ok {
				backCtx["db_timezone"] = tz
			}
			if id, ok := collectContentMap["id"]; ok {
				backCtx["id"] = id
			}
			if version, ok := collectContentMap["version"]; ok {
				backCtx["version"] = version
			}
			backCtx["packet_id"] = strconv.FormatUint(packetID, 10)
			// 4. send to backend
			err = backRun(backCtx, ins.msgBuff.Bytes())
			if err != nil {
				log.Error("[runner] panic runLogModule backend fail.", log.String("module", ins.pInfo.Name), log.Int("port", ins.port), log.String("err", err.Error()))
			}
		}
		// 5. reset buffer
		ins.msgBuff.Reset()
		packetID++

		// 6. post process
		costMillis = time.Now().UnixNano()/1e6 - collectStartTime
		log.Info("[runner] all costs ms",
			log.Int("port", ins.port), log.String("module", ins.pInfo.Name),
			log.Int64("cost", costMillis), log.Int32("runner_id", runner.index), log.Int32("g_id", ins.index))

	SLEEP_INTERVAL:
		select {
		case <-ins.stop:
			//ticker.Stop()
			ins.stop <- true
			break
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			break
		}
	}
	return nil
}

func (runner *Runner) errorHandler(ins *Instance, wait *sync.WaitGroup) {
	wait.Done()
	if err := recover(); err != nil {
		log.Error("[runner] panic", log.String("module", ins.pInfo.Name), log.Int("port", ins.port),
			log.String("err", fmt.Sprintf("%+v: %s", err, debug.Stack())))
		delete(runner.instances, strconv.Itoa(ins.port))
		atomic.AddInt32(&ins.crashCount, 1)
		// TODO 100 use config
		if atomic.LoadInt32(&ins.crashCount) > 100 {
			ins.pInfo.Enable = false
		}

	}
}

func readExternConf(filename string, buf *bytes.Buffer) (*ExternConf, error) {
	var conf ExternConf
	buf.Reset()
	fp, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	_, err = buf.ReadFrom(fp)
	if err != nil {
		return nil, err
	}

	if len(buf.Bytes()) != 0 {
		err = json.Unmarshal(buf.Bytes(), &conf)
		if err != nil {
			return nil, err
		}
	}
	return &conf, nil
}
