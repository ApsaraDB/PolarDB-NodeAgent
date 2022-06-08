package main

import (
	"errors"
	"fmt"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/perf/collector"
)

// PluginInit init the plugin
func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("plugin perf invalid ctx")
	}
	log.Info("Initializing perf plugin")
	c := collector.New()
	err := c.Init(m)
	if err != nil {
		return nil, fmt.Errorf("perf init failed,err:%+v", err)
	}
	log.Info("Plugin module perf init succeeded")
	return c, err
}

// PluginRun runs the plugin
func PluginRun(ctx interface{}, param interface{}) error {
	out, ok1 := param.(map[string]interface{})
	if !ok1 {
		return errors.New("plugin perf invalid ctx")
	}
	c, ok2 := ctx.(*collector.PerfCollector)
	if !ok2 {
		return errors.New("plugin perf invalid ctx")
	}

	return c.Collect(out)
}

// PluginExit  exit the plugin and release related resources
func PluginExit(ctx interface{}) error {
	c, ok := ctx.(*collector.PerfCollector)
	if !ok {
		return errors.New("plugin perf invalid ctx")
	}
	err := c.Stop()
	if err != nil {
		return fmt.Errorf("perf stop failed,err:%+v", err)
	}
	log.Info("stop plugin perf done")
	return nil
}

