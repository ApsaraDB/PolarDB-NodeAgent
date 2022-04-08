/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package main

import (
	"errors"
	"fmt"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/plugins/cgroup/collector"
)

func PluginInit(ctx interface{}) (interface{}, error) {
	m, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("plugin cgroup invalid ctx")
	}
	log.Info("Initializing cgroup plugin")
	c := collector.New()
	err := c.Init(m)
	if err != nil {
		return nil, fmt.Errorf("cgroup init failed,err:%+v", err)
	}
	log.Info("plugin module cgroup init succeeded", log.Int("port", c.Port))
	return c, err
}

func PluginRun(ctx interface{}, param interface{}) error {

	out, ok1 := param.(map[string]interface{})
	if !ok1 {
		return errors.New("plugin cgroup invalid param")
	}

	c, ok2 := ctx.(*collector.CGroupCollector)
	if !ok2 {
		return errors.New("cgroup invalid ctx")
	}
	return c.Collect(out)
}

func PluginExit(ctx interface{}) error {
	c, ok := ctx.(*collector.CGroupCollector)
	if !ok {
		return errors.New("plugin cgroup invalid ctx")
	}
	err := c.Stop()
	if err != nil {
		return fmt.Errorf("cgroup collector stop failed,err:%+v", err)
	}
	log.Info("stop cgroup plugin done", log.Int("port", c.Port))
	return nil
}

