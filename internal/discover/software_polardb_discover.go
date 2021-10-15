/*-------------------------------------------------------------------------
 *
 * software_polardb_discoverer.go
 *    Discoverer for software polardb
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
 *           internal/discover/software_polardb_discoverer.go
 *-------------------------------------------------------------------------
 */

package discover

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/ApsaraDB/db-monitor/common/log"
)

const DBConnTimeout = 1
const DBQueryTimeout = 5

type InstanceInfo struct {
	// FirstRun    bool
	Running      bool
	Stop         chan bool
	LogicInsName string
	InsName      string
	Host         string
	ShareDataDir string
	LocalDataDir string
	LogDir       string
	PFSName      string
	Port         int
	Username     string
	Database     string
}

// DockerDiscoverer docker discover define
type SoftwarePolardbDiscoverer struct {
	WorkDir         string
	UserName        string
	Database        string
	ApplicationName string

	running       bool
	stopEvent     chan bool
	instances     map[string]*InstanceInfo
	InstanceQueue chan *InstanceInfo
}

func (d *SoftwarePolardbDiscoverer) DiscoverInit() {
	d.instances = make(map[string]*InstanceInfo)
	d.InstanceQueue = make(chan *InstanceInfo)
	d.running = true
	d.stopEvent = make(chan bool, 1)
}

func (d *SoftwarePolardbDiscoverer) DiscoverFetch() (interface{}, bool) {
	c, ok := <-d.InstanceQueue
	return c, ok
}

// DiscoverRun start discover
func (d *SoftwarePolardbDiscoverer) DiscoverRun(wait *sync.WaitGroup) {
	wait.Add(1)
	defer wait.Done()

	d.enumerateContainer()
}

func (d *SoftwarePolardbDiscoverer) getDataDirectory(db *sql.DB) (string, string, error) {
	query := "SELECT current_setting('data_directory')"
	localdir, err := d.getOneConfFromDB(db, query)
	if err != nil {
		log.Error("[software_polardb_discoverer] get data_directory from database failed",
			log.String("query", query),
			log.String("error", err.Error()))
		return "", "", err
	}

	query = "SELECT (string_to_array(current_setting('polar_datadir'), '://', NULL))[2]"
	sharedir, err := d.getOneConfFromDB(db, query)
	if err != nil {
		log.Warn("[software_polardb_discoverer] get polar_datadir from database failed",
			log.String("query", query),
			log.String("error", err.Error()))
		sharedir = localdir
	}

	return sharedir, localdir, nil
}

func (d *SoftwarePolardbDiscoverer) getLogDirectory(db *sql.DB) (string, error) {
	query := "SELECT FORMAT('%s/%s', current_setting('data_directory'), current_setting('log_directory'))"
	dir, err := d.getOneConfFromDB(db, query)
	if err != nil {
		log.Error("[software_polardb_discoverer] get log_directory from database failed",
			log.String("query", query),
			log.String("error", err.Error()))
		return "", err
	}

	return dir, nil
}

func (d *SoftwarePolardbDiscoverer) getPFSName(db *sql.DB) (string, error) {
	query := "SHOW polar_disk_name"
	pfs, err := d.getOneConfFromDB(db, query)
	if err != nil {
		log.Error("[software_polardb_discoverer] get polar_disk_name from database failed",
			log.String("query", query),
			log.String("error", err.Error()))
		return "", err
	}

	return pfs, nil
}

func (d *SoftwarePolardbDiscoverer) getSystemIdentifier(db *sql.DB) (int64, error) {
	query := "SELECT system_identifier FROM pg_control_system()"
	id, err := d.getOneConfFromDB(db, query)
	if err != nil {
		log.Error("[software_polardb_discoverer] get system_identifier from database failed",
			log.String("query", query),
			log.String("error", err.Error()))
		return 0, err
	}

	idint, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		log.Error("[software_polardb_discoverer] convert id to int64 failed",
			log.String("id", id),
			log.String("error", err.Error()))
		return 0, err
	}

	return idint, nil
}

func (d *SoftwarePolardbDiscoverer) getOneConfFromDB(db *sql.DB, query string) (string, error) {
	var res string

	ctx, _ := context.WithTimeout(context.Background(), DBQueryTimeout*time.Second)
	rows, err := db.QueryContext(ctx, "/* rds internal mark */"+query)
	if err != nil {
		log.Error("[software_polardb_discoverer] execute query failed",
			log.String("query", query),
			log.String("error", err.Error()))
		return "", err
	}

	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&res); err != nil {
			log.Error("[software_polardb_discoverer] row scan failed for query result",
				log.String("query", query),
				log.String("error", err.Error()))
			return "", err
		}
	} else {
		log.Error("[software_polardb_discoverer] query nothing from database",
			log.String("query", query),
			log.String("error", err.Error()))
		return "", fmt.Errorf("query nothing from database")
	}

	return res, nil
}

func (d *SoftwarePolardbDiscoverer) enumerateContainer() {

	for {
		aliveinstances := make(map[string]bool)

		if !d.running {
			d.InstanceQueue <- nil
			log.Info("[software_polardb_discoverer] stop...")
			break
		}

		log.Info("[software_polardb_discoverer] start to look for new instance",
				log.String("read dir", d.WorkDir))

		files, err := ioutil.ReadDir(d.WorkDir)
		if err != nil {
			log.Error("[software_polardb_discoverer] cannot read unix domain socket dir",
				log.String("dir", d.WorkDir),
				log.String("error", err.Error()))
			goto SLEEP_INTERVAL
		}

		for _, f := range files {
			log.Debug("[software_polardb_discoverer] discover file",
				log.String("file", f.Name()),
				log.Int("mode", int(f.Mode())))

			if f.Mode()&os.ModeSocket == 0 {
				continue
			}

			var key string
			var systemid int64
			var err error

			log.Info("[software_polardb_discoverer] find socket file",
				log.String("file", f.Name()))

			socketlist := strings.Split(f.Name(), ".")
			port, err := strconv.ParseInt(socketlist[len(socketlist)-1], 10, 64)
			if err != nil {
				log.Warn("[software_polardb_discoverer] cannot get port from socket",
					log.String("filename", f.Name()),
					log.String("error", err.Error()))
				continue
			}

			dbUrl := fmt.Sprintf("host=%s user=%s dbname=%s port=%d "+
				"fallback_application_name=%s sslmode=disable connect_timeout=%d",
				d.WorkDir, d.UserName, d.Database, port, d.ApplicationName, DBConnTimeout)

			// connect
			db, err := sql.Open("postgres", dbUrl)
			if err != nil {
				log.Error("[software_polardb_discoverer] fail to connect db",
					log.String("dburl", dbUrl), log.String("error", err.Error()))
				continue
			}

			db.Exec("SET log_min_messages=FATAL")

			// get system identifier
			systemid, err = d.getSystemIdentifier(db)
			if err != nil {
				log.Error("[software_polardb_discoverer] get system identifier failed",
					log.String("dburl", dbUrl), log.String("error", err.Error()))
				goto RELEASE_RESOURCE
			}

			key = fmt.Sprintf("%d_%d", systemid, port)
			if _, ok := d.instances[key]; !ok {
				ins := &InstanceInfo{}
				sharedatadir, localdatadir, err := d.getDataDirectory(db)
				if err != nil {
					log.Error("[software_polardb_discoverer] get data directory failed",
						log.String("ins", key),
						log.String("error", err.Error()))
					goto RELEASE_RESOURCE
				}

				logdir, err := d.getLogDirectory(db)
				if err != nil {
					log.Error("[software_polardb_discoverer] get log directory failed",
						log.String("ins", key),
						log.String("error", err.Error()))
					goto RELEASE_RESOURCE
				}

				pfs, err := d.getPFSName(db)
				if err != nil {
					log.Error("[software_polardb_discoverer] get pfs name failed",
						log.String("ins", key),
						log.String("error", err.Error()))
					goto RELEASE_RESOURCE
				}

				d.instances[key] = ins
				ins.LogicInsName = strconv.FormatInt(systemid, 10)
				ins.InsName = key
				ins.Host = d.WorkDir
				ins.Port = int(port)
				ins.ShareDataDir = sharedatadir
				ins.LocalDataDir = localdatadir
				ins.Username = d.UserName
				ins.Database = d.Database
				ins.LogDir = logdir
				ins.PFSName = pfs
				ins.Stop = make(chan bool, 1)
				ins.Running = true
				log.Info("[software_polardb_discoverer] discover new ins",
					log.String("ins", ins.InsName))
				d.notifyInstanceChange(ins)
			}
			aliveinstances[key] = true

		RELEASE_RESOURCE:
			db.Close()
		}

		for key, ins := range d.instances {
			if _, ok := aliveinstances[key]; !ok {
				log.Info("[software_polardb_discoverer] instance not alive, stop collecting it",
					log.String("ins", key))
				ins.Running = false
				d.notifyInstanceChange(ins)
			}
		}

	SLEEP_INTERVAL:
		select {
		case <-d.stopEvent:
			break
		case <-time.After(60 * time.Second):
			break
		}
	}
}

func (d *SoftwarePolardbDiscoverer) notifyInstanceChange(instance *InstanceInfo) {
	if !instance.Running {
		delete(d.instances, instance.InsName)
	}
	d.InstanceQueue <- instance
}

// DiscoverStop should be called at runner Stop()
func (d *SoftwarePolardbDiscoverer) DiscoverStop() error {
	log.Info("[software_polardb_discoverer] DiscoverStop")
	d.running = false
	d.stopEvent <- true
	return nil
}

// DiscoverExit should be called at runner Exit()
func (d *SoftwarePolardbDiscoverer) DiscoverExit() error {
	close(d.InstanceQueue)
	return nil
}
