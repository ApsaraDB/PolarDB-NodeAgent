/*-------------------------------------------------------------------------
 *
 * ctrl.go
 *    Ctrl plugin
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
 *           plugins/ctrl/ctrl.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"errors"
	"net/http"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
)

func PluginInit(ctx interface{}) (interface{}, error) {
	_, ok := ctx.(map[string]interface{})
	if !ok {
		return nil, errors.New("[ctrl] PluginInit invalid ctx")
	}
	log.Info("[ctrl] plugin init")

	return nil, nil
}

func PluginRun(ctx, param interface{}) error {
	log.Info("[ctrl] plugin run")

	return nil
}

func PluginExit(ctx interface{}) error {
	log.Info("[ctrl] plugin exit")
	return nil
}

func TestRoute(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("This is test Router in plugin\n"))
}
