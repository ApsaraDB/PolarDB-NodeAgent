/*-------------------------------------------------------------------------
 *
 * main.go
 *    Entrypoint
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
 *           cmd/universe/main.go
 *-------------------------------------------------------------------------
 */

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/utils"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/internal/ctrl"
	"github.com/ApsaraDB/PolarDB-NodeAgent/internal/gather"
)

var (
	RpmName     string
	RpmRelease  string
	RpmVersion  string
	GitBranch   string
	GitCommitID string
	Buildtime   string
)

func LogInit() {
	log.Init()
	log.Info("Logger initialized finish.")
}

type UniverseExplorer struct {
	stopEvent       chan bool
	stop            bool
	enableDocker    bool
	pluginCtx       gather.PluginCtx
	runnerMap       map[string]gather.RunnerService
	mInfoMap        *sync.Map
	ctrl            ctrl.Ctrl
	wait            sync.WaitGroup
	runnerLoaderMap sync.Map
}

func (serve *UniverseExplorer) IsStop() bool {
	return serve.stop
}

func (serve *UniverseExplorer) Init(enableDocker bool) {
	serve.runnerMap = make(map[string]gather.RunnerService)
	serve.stop = false
	serve.mInfoMap = &sync.Map{}
	serve.stopEvent = make(chan bool, 1)
	serve.enableDocker = enableDocker
	serve.ctrl.Init()
	serve.pluginCtx.PluginInit()

	serve.AddDefaultRunnerLoader()
}

func (serve *UniverseExplorer) AddDefaultRunnerLoader() {
	serve.RunnerLoaderRegister("export", gather.NewExportRunner)
	serve.RunnerLoaderRegister("singleton", gather.NewSingletonRunner)
	serve.RunnerLoaderRegister("k8s", gather.NewDockerRunner)
	serve.RunnerLoaderRegister("software_polardb", gather.NewSoftwarePolardbRunner)
}

// collector plugin loader
func (serve *UniverseExplorer) RunnerPluginLoader(pInfo *gather.PluginInfo) {
	var runnerSvc gather.RunnerService

	// if runnerSvc exists already, use it
	if oldRunnerSvc, ok := serve.runnerMap[pInfo.Name]; ok {
		log.Info("[universe]runner has already loaded", log.String("runnerSvc", pInfo.Name))
		runnerSvc = oldRunnerSvc
	} else {
		// docker or rds
		runnerType := pInfo.Runner

		// invoke runnerSvc Init
		funcNew, ok := serve.runnerLoaderMap.Load(runnerType)
		if !ok {
			log.Error("[universe] cannot find match loader",
				log.String("collector", pInfo.Name),
				log.String("runnerType", pInfo.Runner))
			return
		}
		newRunnerSvc := funcNew.(func() gather.RunnerService)()

		//bizType, businessSchema := newRunnerSvc.RunnerGetSchema()
		//serve.schema.BizSchemaMap[bizType] = *businessSchema
		log.Info("[universe] plugin loader load succeed",
			log.String("collector", pInfo.Name))

		runnerSvc = newRunnerSvc
		err := runnerSvc.RunnerInit(serve.mInfoMap, pInfo, &serve.pluginCtx)
		if err != nil {
			log.Error("[universe] RunnerInit failed",
				log.String("name", pInfo.Name),
				log.String("err", err.Error()))
			return
		}
		serve.runnerMap[pInfo.Name] = runnerSvc
	}

	go runnerSvc.RunnerRun(&serve.wait)
}

func (serve *UniverseExplorer) BackendPluginLoader(pInfo *gather.PluginInfo) {
	// load backend plugin
	serve.loadPlugin(pInfo)

	// init backend plugin
	_, ok := serve.mInfoMap.Load(pInfo.Name)
	if !ok {
		log.Error("[universe] plugin not found",
			log.String("name", pInfo.Name))
		return
	}
}

// load ctrl and backend plugin
func (serve *UniverseExplorer) loadPlugin(pInfo *gather.PluginInfo) error {
	if _, ok := serve.mInfoMap.Load(pInfo.Name); ok {
		log.Info("[universe] module has already loaded",
			log.String("name", pInfo.Name))
		return nil
	}

	mInfo := new(gather.ModuleInfo)

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
	serve.mInfoMap.Store(pInfo.Name, mInfo)

	return nil
}

// functional plugin
func (serve *UniverseExplorer) CtrlPluginLoader(pInfo *gather.PluginInfo) {
	var mInfo *gather.ModuleInfo
	err := serve.loadPlugin(pInfo)
	if err != nil {
		log.Error("[universe] CtrlPluginLoader load plugin failed", log.String("err", err.Error()), log.String("plugin", pInfo.Name))
		return
	}
	if info, ok := serve.mInfoMap.Load(pInfo.Name); ok {
		mInfo = info.(*gather.ModuleInfo)
		err = serve.ctrl.LoadCtrlPlugin(mInfo, pInfo)
		if err != nil {
			log.Error("[universe] CtrlPluginLoader failed", log.String("err", err.Error()))
			return
		}
	} else {
		log.Error("[universe] CtrlPluginLoader load plugin failed", log.String("plugin", pInfo.Name))
	}
}

func (serve *UniverseExplorer) PluginLoader() {
	for {
		pInfo, _ := <-serve.pluginCtx.PluginQueue
		// a nil value from plugin chan means the plugin has been stopped
		// we need to stop the plugin loader loop
		if pInfo == nil {
			if serve.IsStop() {
				log.Info("[universe] Plugin Loader exiting...")
				break
			}
		}

		if pInfo.Mode == consts.PluginModeCtrl {
			serve.CtrlPluginLoader(pInfo)
		} else if pInfo.Mode == consts.PluginModeCollector {
			serve.RunnerPluginLoader(pInfo)
		}
	}
}

/*
// XXX: need support plugin add runnerMap loader?
func (serve *UniverseExplorer) AddNewRunnerLoader(info *PluginInfo) {
    var module *ModuleInfo
    if len(info.Exports) != 1 {
        fmt.Printf("[UniverseExplorer RunnerLoader] module %s not invalid\n", info.Name)
    }

    if _, ok := serve.mInfoMap[info.Path]; !ok {
        mInfo := new(ModuleInfo)
        err := mInfo.ModuleInit(info, false)
        if err != nil {
            fmt.Printf("[UniverseExplorer RunnerLoader] module %s init error: %s\n", info.Name, err.Error())
            return
        } else {
            serve.mInfoMap[info.Path] = mInfo
        }
        module = mInfo
    } else {
        fmt.Printf("[UniverseExplorer RunnerLoader] module %s already loaded\n", info.Name)
        return
    }

    serve.runnerLoaderMap.Store(info.Exports[0], module.Eat[info.Exports[0]])
}
*/

func (serve *UniverseExplorer) RunnerLoaderRegister(name string, init func() gather.RunnerService) {
	serve.runnerLoaderMap.Store(name, init)
}

// all plugin is loaded by Start
// module will be loaded if a plugin runnerMap need a new module
func (serve *UniverseExplorer) Start() {

	go serve.ctrl.Start()

	go serve.pluginCtx.Run("conf/plugin", &serve.wait)
	go serve.PluginLoader()

	stop := false
	for {
		select {
		case <-serve.stopEvent:
			stop = true
			break
		case <-time.After(120 * time.Second):
			break
		}
		if stop {
			break
		}
	}

	log.Info("[universe] wait for other goroutine exit...\n")
	serve.wait.Wait()
	log.Info("[universe] wait a moment for backend...\n")
	time.Sleep(3 * time.Second)
}

// all runnerMap has been stopped, it's safe to clean runnerMap resource here
func (serve *UniverseExplorer) Exit() {
	serve.pluginCtx.PluginExit()
}

func (serve *UniverseExplorer) Stop() {
	serve.stop = true
	serve.stopEvent <- true

	serve.pluginCtx.PluginStop()
	serve.ctrl.Stop()

	for k, m := range serve.runnerMap {
		log.Info("[universe] begin to stop", log.String("plugin", k))
		m.RunnerStop()
	}

	// backend will be stopped after the collector stopped
}

func (serve *UniverseExplorer) StopPlugin(name string) {
	if r, ok := serve.runnerMap[name]; ok {
		r.RunnerStop()
	}
}

func main() {
	var rlimit syscall.Rlimit
	rlimit.Max = 200000
	rlimit.Cur = 200000

	var printversion bool
	var perfport int

	flag.BoolVar(&printversion, "v", false, "print version info")
	flag.IntVar(&perfport, "p", 9060, "perf port")
	flag.Parse()

	if printversion {
		fmt.Printf("go version: %s\ncompiler: %s\nbuildtime: %s\nplarform: %s\n"+
			"git branch: %s\ncommit id: %s\n",
			runtime.Version(), runtime.Compiler,
			Buildtime, fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			GitBranch, GitCommitID,
		)
		return
	}

	// err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	// if err != nil {
	// 	fmt.Println("set rlimit failed,err:", err)
	// 	return
	// }
	basepath := utils.GetBasePath()
	os.MkdirAll(basepath + "/offset", 0755)
	ok, err := utils.LockFile(basepath + "/universe.lock")
	if !ok || err != nil {
		e := fmt.Sprint("LockFile %s return false")
		if err != nil {
			e = err.Error()
		}
		fmt.Println("start universe failed, lock file failed, err:" + e)
		return
	}
	defer utils.UnlockFile(basepath + "/universe.lock")

	LogInit()
	defer log.Sync()

	var service UniverseExplorer

	log.Info("[universe] Starting...")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		_ = <-sigs
		log.Info("[universe] about to stop...")
		service.Stop()
		log.Info("[universe] All stopped.")
	}()

	go func() {
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", perfport), nil)
	}()

	service.Init(true)
	service.Start()
	service.Exit()

	log.Info("[universe] exited")
}
