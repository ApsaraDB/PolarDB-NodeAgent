/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package gather

import (
	"errors"
	"fmt"
	"plugin"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
    "github.com/ApsaraDB/PolarDB-NodeAgent/common/log"

     "github.com/aarzilli/golua/lua"
)

const lua_vm_key = "__lua_vm__"
const lua_ctx = "__lua_ctx__"
const lua_run_table = "__lua_run_table__"

func luaPushMap(L *lua.State, ctx interface{}) {
    L.NewTable()

	ctxMap := ctx.(map[string]interface{})
	for k, v := range ctxMap {
        L.PushString(k)
		switch v.(type) {
		case string:
            L.PushString(v.(string))
		case int:
            L.PushInteger(int64(v.(int)))
		case int64:
            L.PushInteger(int64(v.(int64)))
		case int32:
            L.PushInteger(int64(v.(int32)))
        case float64:
            L.PushNumber(v.(float64))
        case bool:
            L.PushBoolean(v.(bool))
        default:
            L.PushGoStruct(v)
		}
        L.SetTable(-3)
	}
}

func luaCallWithCheck(L *lua.State, paramn, retn int) error {
    err := L.Call(paramn, retn)                
    if err != nil {
        return err
    }

    if L.GetTop() != retn {
        return fmt.Errorf("lua call function failed with return value number %d is not as expected %d",
                L.GetTop(), retn)
    }

    return nil
}

func luaPopBoolean(L *lua.State) (bool, error) {
    if !L.IsBoolean(-1) {
        log.Error("[module_linux] the second return value is not bool from lua")
        return false, errors.New("lua PluginInit failed")
    }

	ret := L.ToBoolean(-1)
	if !ret {
        log.Error("[module_linux] return value is false, lua PluginInit failed")
		return false, errors.New("lua PluginInit failed")
	}

    L.Pop(1)
    return ret, nil
}

func luaPopLable(L *lua.State, ctx map[string]interface{}) error {
    if !L.IsTable(-1) {
        log.Error("[module_linux] the first return value is not table from lua")
        return errors.New("lua PluginInit failed")
    }

    L.PushNil()
    for L.Next(-2) != 0 { 
        var v interface{}
        if L.IsGoStruct(-1) {
            v = L.ToGoStruct(-1)
        } else if L.IsBoolean(-1) {
            v = L.ToBoolean(-1)
        } else if L.IsNumber(-1) {
            v = L.ToNumber(-1)
        } else if L.IsString(-1) {
            v = L.ToString(-1)
        } else {
            v = L.ToString(-1)
        }
        L.Pop(1);
        k := L.ToString(-1);
        ctx[k] = v
    }

    L.Pop(1)
    return nil
}

func luaGetVM(_ctx interface{}) (*lua.State, error) {
	ctx, ok := _ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("ctx must be map[string]interface{}")
	}
	L, err := ctx[lua_vm_key].(*lua.State)
	if err != true {
		return nil, errors.New("cannot find lua vm instance")
	}
	return L, nil
}

func luaGetScript(_ctx interface{}) (string, error) {
	ctx, ok := _ctx.(map[string]interface{})
	if !ok {
		return "", errors.New("invalid lua ctx")
	}
	luapath, ok := ctx[consts.PluginLuaScriptPath]
	if !ok {
		return "", errors.New("ctx.PluginLuaScriptPath must not be nil")
	}
	path, bl := luapath.(string)
	if !bl {
		return "", errors.New("ctx.PluginLuaScriptPath must be string")
	}
	return path, nil
}

func luaCreateVM(path string) (*lua.State, error) {
	L := lua.NewState()
    L.OpenLibs()

	if err := L.DoFile(path); err != nil {
		L.Close()
		return nil, err
	}
	return L, nil
}

func luaInit(ctx interface{}) (interface{}, error) {
    var retCtx, retMap map[string]interface{}
    var err error
    var ret bool

	luapath, err := luaGetScript(ctx)
	if err != nil {
		return nil, err
	}

	L, err := luaCreateVM(luapath)
	if err != nil {
		return nil, err
	}

    L.GetGlobal("PluginInit")
    luaPushMap(L, ctx)

    // one param (ctx map[string]interface{}), two return values (ctx map[string]interface{}, bool)
    err = luaCallWithCheck(L, 1, 2)
    if err != nil {
        log.Error("[module_linux] call PluginInit failed", log.String("error", err.Error()))
        goto LUA_INIT_ERROR
    }

    ret, err = luaPopBoolean(L)
    if err != nil {
        log.Error("[module_linux] get ret value failed", log.String("error", err.Error()))
        goto LUA_INIT_ERROR
    }

    if !ret {
        err = fmt.Errorf("init return is false")
        goto LUA_INIT_ERROR
    }

    retCtx = make(map[string]interface{})
    err = luaPopLable(L, retCtx)
    if err != nil {
        log.Error("[module_linux] get context failed", log.String("error", err.Error()))
        goto LUA_INIT_ERROR
    }

    L.SetTop(0)

    log.Info("[module_linux] Lua Init", log.String("context", fmt.Sprintf("%+v", retCtx)))

	retMap = make(map[string]interface{})
	retMap[lua_vm_key] = L
	retMap[lua_ctx] = retCtx
	return retMap, nil

LUA_INIT_ERROR:

    L.SetTop(0)
    L.Close()
    return nil, err
}

func luaRun(ctx interface{}, param interface{}) error {
    var err error
    var retMap map[string]interface{}
    var ret bool

	L, err := luaGetVM(ctx)
	if err != nil {
		return err
	}

    L.GetGlobal("PluginRun")
    luaPushMap(L, ctx)

    // one param (ctx map[string]interface{}), two return values (ctx map[string]interface{}, bool)
    err = luaCallWithCheck(L, 1, 2)
    if err != nil {
        log.Error("[module_linux] call PluginRun failed", log.String("error", err.Error()))
        goto LUA_RUN_ERROR
    }

    ret, err = luaPopBoolean(L)
    if err != nil {
        log.Error("[module_linux] get ret value failed", log.String("error", err.Error()))
        goto LUA_RUN_ERROR
    }

    if !ret {
        err = fmt.Errorf("init return is false")
        goto LUA_RUN_ERROR
    }

	retMap = param.(map[string]interface{})
    err = luaPopLable(L, retMap)
    log.Info("[module_linux] RunRUN", log.String("module", fmt.Sprintf("%+v", retMap)))

    L.SetTop(0)
	return nil

LUA_RUN_ERROR:
    L.SetTop(0)
    return err
}

func luaExit(ctx interface{}) error {
    var ret bool
    var err error

	L, err := luaGetVM(ctx)
	if err != nil {
		return err
	}

    L.GetGlobal("PluginExit")
    luaPushMap(L, ctx)

    // one param (ctx map[string]interface{}), one return value (bool)
    err = luaCallWithCheck(L, 1, 1)
    if err != nil {
        log.Error("[module_linux] call PluginRun failed", log.String("error", err.Error()))
        goto LUA_EXIT_ERROR
    }

    ret, err = luaPopBoolean(L)
    if err != nil {
        log.Error("[module_linux] get ret value failed", log.String("error", err.Error()))
        goto LUA_EXIT_ERROR
    }

    if !ret {
        err = fmt.Errorf("init return is false")
        goto LUA_EXIT_ERROR
    }

    L.Close()
    return nil

LUA_EXIT_ERROR:
	L.Close()
	return err
}

// PluginInterface plugin common
type PluginInterface struct {
	Init func(interface{}) (interface{}, error)
	Run  func(interface{}, interface{}) error
	Exit func(interface{}) error
}

// ModuleInfo a module struct
type ModuleInfo struct {
	ref       int32
	inited    int32
	plugin    *plugin.Plugin
	ID        string
	Mode      string
	Extern    string
	PluginABI PluginInterface
	Eat       map[string]interface{} // module-name.FuncName as key
	Iat       map[string]interface{} // module-name.FuncName as key
	Contexts  sync.Map               // module-name as key
}

func (module *ModuleInfo) loadFunction(info *PluginInfo) error {
	for _, identifier := range info.Exports {

		// identifier looks like : module-name.FuncName
		list := strings.Split(identifier, ".")
		if len(list) != 2 {
			return fmt.Errorf("[module] invalid export identifier: %s", identifier)
		}
		fnName := list[1]
		fn, err := module.plugin.Lookup(fnName)
		if err != nil {
			return err
		}
		module.Eat[identifier] = fn
	}
	return nil
}

func (module *ModuleInfo) modulePrepareLua(path string) error {
	module.PluginABI.Init = luaInit
	module.PluginABI.Run = luaRun
	module.PluginABI.Exit = luaExit
	return nil
}

func (module *ModuleInfo) modulePrepareGolang(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("modulePrepareGolang open error, path:%s, err:%s", path, err.Error())
	}

	module.PluginABI.Init = nil
	module.PluginABI.Run = nil
	module.PluginABI.Exit = nil

	var ok bool

	initPlugin, err := p.Lookup("PluginInit")
	if err != nil {
		return err
	}

	module.PluginABI.Init, ok = initPlugin.(func(interface{}) (interface{}, error))
	if !ok {
		return errors.New("PluginInit not match abi")
	}

	runPlugin, err := p.Lookup("PluginRun")
	if err != nil {
		return errors.New("PluginRun lookup error")
	}
	module.PluginABI.Run, ok = runPlugin.(func(interface{}, interface{}) error)
	if !ok {
		return errors.New("PluginRun not match abi")
	}

	exitPlugin, err := p.Lookup("PluginExit")
	if err != nil {
		return err
	}

	module.PluginABI.Exit, ok = exitPlugin.(func(interface{}) error)
	if !ok {
		return errors.New("PluginExit not match abi")
	}

	module.plugin = p
	return nil
}

// ModuleInit init a module
func (module *ModuleInfo) ModuleInit(pInfo *PluginInfo) error {
	if atomic.AddInt32(&module.ref, 1) > 1 {
		for atomic.LoadInt32(&module.inited) == 0 {
			time.Sleep(1)
		}
		return nil
	}
	module.Mode = pInfo.Mode
	module.Extern = pInfo.Extern
	module.PluginABI = PluginInterface{}
	module.Eat = make(map[string]interface{})
	module.Iat = make(map[string]interface{})

	if pInfo.Type == "lua" {
		if err := module.modulePrepareLua(pInfo.Path); err != nil {
			return err
		}
	} else if pInfo.Type == "golang" {
		if err := module.modulePrepareGolang(pInfo.Path); err != nil {
			return err
		}
		if err := module.loadFunction(pInfo); err != nil {
			return err
		}
	} else {
		return errors.New("not support plugin type")
	}

	atomic.StoreInt32(&module.inited, 1)

	return nil
}

// ModuleExit exit module
func (module *ModuleInfo) ModuleExit() {
	// TODO Close lua vm and other resource like socket, chan
	// XXX Do not need to call dlclose to release a loaded dynamic library
	v := atomic.AddInt32(&module.ref, -1)
	if v == 0 {
		// do cleanup
	}
}
