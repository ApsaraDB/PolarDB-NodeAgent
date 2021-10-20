/*-------------------------------------------------------------------------
 *
 * polardb_pg_service.go
 *    Polardb pg metrics Provider
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
 *           plugins/polardb_pg_opensource/service/polardb_pg_service.go
 *-------------------------------------------------------------------------
 */

package service

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/drone/routes"
	"github.com/pkg/errors"
	"github.com/ApsaraDB/db-monitor/common/log"
)

const (
	RESPONSE_OK = "{\"code\":200}"
)

var polardbPgServiceOnce sync.Once
var polardbPgServiceIns *PolarDBPgService = nil

func GetPolarDBPgService() *PolarDBPgService {
	polardbPgServiceOnce.Do(func() {
		polardbPgServiceIns = &PolarDBPgService{
			Port:    "818",
			Metrics: make(map[string]map[string]interface{}),
		}
		err := polardbPgServiceIns.Start()
		if err != nil {
			log.Error("Failed to initialize PolarDBPgService ", log.String("err", err.Error()))
			polardbPgServiceIns = nil
		}
	})
	return polardbPgServiceIns
}

type CollectRequest struct {
	InsID string `json:"ins_id"`
}

type CollectResponse struct {
	ErrCode int                    `json:"err_code"`
	ErrMsg  string                 `json:"err_msg"`
	Metrics map[string]interface{} `json:"metrics"`
}

type PolarDBPgService struct {
	Port    string
	Metrics map[string]map[string]interface{}
	Lock    sync.Mutex
	Server  *http.Server
}

func (s *PolarDBPgService) Start() error {
	mux := routes.New()
	mux.Post("/v1/metrics", s.CollectMetrics)
	http.Handle("/", mux)

	s.Server = &http.Server{
		Addr: ":" + s.Port,
	}
	go func() {
		for {
			err := s.Server.ListenAndServe()
			if err == http.ErrServerClosed {
				log.Info("PolarDBPgService server closed!")
				return
			}
			log.Error("PolarDBPgService stopped unexpected err %s, retrying", log.String("err", err.Error()), log.String("port", s.Port))
			time.Sleep(time.Second * 60)
		}
	}()
	log.Info("Success to initialize PolarDBPgService ", log.String("port", s.Port))
	return nil
}

func (s *PolarDBPgService) Stop() error {
	s.Server.Shutdown(context.Background())
	return nil
}

func (s *PolarDBPgService) ResponseError(rsp http.ResponseWriter, err error) {
	log.Warn("Failed to service response err", log.String("err", err.Error()), log.String("port", s.Port))
	collectResp := CollectResponse{
		ErrCode: 500,
		ErrMsg:  err.Error(),
	}
	resp, _ := json.Marshal(collectResp)
	rsp.Write(resp)
}

func (s *PolarDBPgService) CollectMetrics(rsp http.ResponseWriter, req *http.Request) {
	var err error
	var buffer []byte

	defer func() {
		if err := recover(); err != nil {
			s.ResponseError(rsp, errors.Errorf("%v", err))
		}
	}()

	buffer, err = ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to Read Body", log.String("err", err.Error()), log.String("port", s.Port))
		s.ResponseError(rsp, err)
		return
	}
	defer req.Body.Close()

	var collectRequest CollectRequest
	err = json.Unmarshal(buffer, &collectRequest)
	if err != nil {
		log.Error("Failed to Unmarshal", log.String("err", err.Error()), log.String("port", s.Port))
		s.ResponseError(rsp, err)
		return
	}

	metrics, err := s.Get(collectRequest.InsID)
	if err != nil {
		log.Warn("Failed to get metrics", log.String("ins_id", collectRequest.InsID), log.String("err", err.Error()), log.String("port", s.Port))
		s.ResponseError(rsp, err)
		return
	}

	collectResp := CollectResponse{
		ErrCode: 200,
		Metrics: metrics,
	}
	resp, err := json.Marshal(collectResp)
	if err != nil {
		log.Warn("Failed to marshal metrics", log.String("ins_id", collectRequest.InsID), log.String("err", err.Error()), log.String("port", s.Port))
		s.ResponseError(rsp, err)
		return
	}
	rsp.Write(resp)
}

func (s *PolarDBPgService) Set(k string, v map[string]interface{}) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	s.Metrics[k] = v

	return nil
}

func (s *PolarDBPgService) Get(k string) (map[string]interface{}, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	if v, exist := s.Metrics[k]; exist {
		return v, nil
	} else {
		return nil, errors.Errorf("Metrcis %s not exist", k)
	}
}
