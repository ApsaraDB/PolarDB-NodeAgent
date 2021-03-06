/*-------------------------------------------------------------------------
 *
 * pfs_collector.go
 *    collect pfs disk usage
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
 *           plugins/polardb_pg_opensource/resource/fs/pfs_collector.go
 *-------------------------------------------------------------------------
 */
package fs

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/consts"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/log"
	"github.com/ApsaraDB/PolarDB-NodeAgent/common/polardb_pg/utils"

	_ "github.com/lib/pq"
)

const (
	PFSCmdTimeout              = 5
	PFSUsageCollectInterval    = 240
	PFSDiskInfoCollectInterval = 15
)

type PbdInfo struct {
	plsPrefix       string
	pfsUsageInfoMap *sync.Map
	pfsDiskInfoMap  *sync.Map
}

type PfsCollector struct {
	hasInit     bool
	InsName     string
	DBType      string
	Environment string
	pbdInfo     PbdInfo
	logger      *log.PluginLogger
	cli         *utils.CommandExecutor
	stopped     chan int
}

func NewPfsCollector() *PfsCollector {
	collector := &PfsCollector{
		hasInit: false,
		cli:     utils.NewCommandExecutor(),
	}
	collector.pbdInfo = PbdInfo{
		pfsUsageInfoMap: &sync.Map{},
		pfsDiskInfoMap:  &sync.Map{},
	}
	return collector
}

func (p *PfsCollector) Init(m map[string]interface{}, logger *log.PluginLogger) error {
	if !p.hasInit {
		p.logger = logger
		p.hasInit = true

		envs := m["env"].(map[string]string)
		p.InsName = m[consts.PluginContextKeyInsName].(string)
		p.DBType = m["dbtype"].(string)
		p.Environment = m["environment"].(string)

		p.pbdInfo.plsPrefix = "None"
		// public cloud
		if pbdNum, ok := envs["apsara.metric.store.pbd_number"]; ok {
			p.pbdInfo.plsPrefix = fmt.Sprintf("%s-1", pbdNum)
		} else {
			// dbstack
			if pvname, ok := envs["apsara.metric.pv_name"]; ok {
				p.pbdInfo.plsPrefix = fmt.Sprintf("mapper_%s", pvname)
			} else {
				p.logger.Warn("pbd init failed, neither pbd_num nor pv_name exists", nil)
			}
		}
		p.stopped = make(chan int)

		if err := p.cli.Init(); err != nil {
			p.logger.Warn("command line executor init failed", err)
			return err
		}

		go p.collectloop()

		p.logger.Info("init pfs collector successfully",
			log.String("pfs info", fmt.Sprintf("%+v", p)))
	} else {
		p.logger.Info("pfs collector has already inited")
	}

	return nil
}

func (p *PfsCollector) collectloop() {
	usageTimer := time.NewTimer(PFSUsageCollectInterval * time.Second)
	diskInfoTimer := time.NewTimer(PFSDiskInfoCollectInterval * time.Second)

	p.logger.Info("pfs collect loop start")

	p.collectPfsDiskInfo()
	p.collectPfsDiskUsage()

	for {
		select {
		case <-p.stopped:
			p.logger.Info("pfs collect loop stop")
			return
		case <-usageTimer.C:
			p.logger.Debug("pfs usage collect loop")
			p.collectPfsDiskUsage()
			usageTimer.Reset(PFSUsageCollectInterval * time.Second)
		case <-diskInfoTimer.C:
			p.logger.Debug("pfs disk info collect loop")
			p.collectPfsDiskInfo()
			diskInfoTimer.Reset(PFSDiskInfoCollectInterval * time.Second)
		}
	}
}

func (p *PfsCollector) collectPfsDiskUsage() error {
	var res string
	var err error

	pfsadmCmd := fmt.Sprintf("pfsadm du -d 1 /%s/data/", p.pbdInfo.plsPrefix)
	pfsCmd := fmt.Sprintf("pfsadm du -d 1 /%s/data/", p.pbdInfo.plsPrefix)

	if res, err = p.ExecCommand(pfsadmCmd); err != nil {
		p.logger.Error("exec pfsadm du failed. We will retry this use pfsadm du", err,
			log.String("cmd", pfsadmCmd))
		if res, err = p.ExecCommand(pfsCmd); err != nil {
			p.logger.Error("exec pfsadm du failed again", err,
				log.String("cmd", pfsCmd))
			return err
		}
	}

	for _, line := range strings.Split(res, "\n") {
		if len(line) == 0 {
			continue
		}

		dirsize := strings.Split(line, "\t")
		if len(dirsize) != 2 {
			p.logger.Info("pfs du result format is not correct.", log.String("line", line))
			return nil
		}

		size, _ := strconv.ParseUint(dirsize[0], 10, 64)
		dirname := path.Base(dirsize[1])
		p.pbdInfo.pfsUsageInfoMap.Store("pls_"+dirname+"_dir_size", size/1024)
		switch dirname {
		case "base":
			p.pbdInfo.pfsUsageInfoMap.Store("polar_base_dir_size", size/1024)
		case "pg_wal":
			p.pbdInfo.pfsUsageInfoMap.Store("polar_wal_dir_size", size/1024)
		}
	}

	return nil
}

//Blktag Info:
//(0)allocnode: id 0, shift 0, nchild=5, nall 12800, nfree 11836, next 0
//Direntry Info:
//(0)allocnode: id 0, shift 0, nchild=5, nall 10240, nfree 10100, next 0
//Inode Info:
//(0)allocnode: id 0, shift 0, nchild=5, nall 10240, nfree 10100, next 0
func (p *PfsCollector) parsePbdInfo(bt []byte) error {
	buf := bytes.NewBuffer(bt)
	for i := 0; i < 6; i++ {
		line, err := buf.ReadString(0x0A)
		if err != nil {
			return err
		}

		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}

		// blktag
		if i == 1 {
			if err := p.parseBlktag(fields); err != nil {
				p.logger.Error("parse blk tag line failed", err, log.String("line", line))
				return err
			}
			continue
		}

		// direntry
		if i == 3 {
			if err := p.parseDirentry(fields); err != nil {
				p.logger.Error("parse direntry line failed", err, log.String("line", line))
				return err
			}
			continue
		}

		// inode
		if i == 5 {
			if err := p.parseInode(fields); err != nil {
				p.logger.Error("parse inode line failed", err, log.String("line", line))
				return err
			}
			continue
		}
	}
	return nil
}

// get inode total and used
func (p *PfsCollector) parseInode(fields []string) error {
	inodeTotal, inodeUsed, err := p.getPbdInfoField(fields)
	if err != nil {
		return err
	}

	p.pbdInfo.pfsDiskInfoMap.Store("pls_inode_total", inodeTotal)
	p.pbdInfo.pfsDiskInfoMap.Store("pls_inode_used", inodeUsed)
	if inodeTotal > 0 {
		p.pbdInfo.pfsDiskInfoMap.Store("pls_inode_usage", float64(inodeUsed*100)/float64(inodeTotal))
	}

	return err
}

// get direntry total and used
func (p *PfsCollector) parseDirentry(fields []string) error {
	direntryTotal, direntryUsed, err := p.getPbdInfoField(fields)
	if err != nil {
		return err
	}

	p.pbdInfo.pfsDiskInfoMap.Store("pls_direntry_total", direntryTotal)
	p.pbdInfo.pfsDiskInfoMap.Store("pls_direntry_used", direntryUsed)
	if direntryTotal > 0 {
		p.pbdInfo.pfsDiskInfoMap.Store("pls_direntry_usage", float64(direntryUsed*100)/float64(direntryTotal))
	}

	return nil
}

func (p *PfsCollector) parseBlktag(fields []string) error {
	blkTotal, blkUsed, err := p.getPbdInfoField(fields)
	if err != nil {
		return err
	}

	nchildArray := strings.Split(fields[5], "=")
	if len(nchildArray) != 2 {
		return fmt.Errorf("parse nchild failed: %v", fields)
	}

	nchild, err := strconv.ParseUint(strings.TrimSuffix(nchildArray[1], ","), 10, 64)
	if err != nil {
		return err
	}
	// 4MB on each block, and meta data 4MB on 1st block of each chunk, .pfs-paxos: 4MB, .pfs-journal: 1024MB
	// totalUsedSize := blkUsed*4 - nchild*4 - 4 - 1024
	// 10G in each chunk
	p.pbdInfo.pfsDiskInfoMap.Store("pls_blk_total", blkTotal)
	p.pbdInfo.pfsDiskInfoMap.Store("pls_blk_used", blkUsed)
	if blkTotal > 0 {
		totalUsedSize := uint64(float64(nchild*10*1024*1024) * (float64(blkUsed) / float64(blkTotal)))
		p.pbdInfo.pfsDiskInfoMap.Store("pls_user_data_size", totalUsedSize)
		p.pbdInfo.pfsDiskInfoMap.Store("pls_blk_usage", float64(blkUsed*100)/float64(blkTotal))
	}
	return nil
}

func (p *PfsCollector) getPbdInfoField(fields []string) (uint64, uint64, error) {
	total, err := strconv.ParseUint(strings.TrimSuffix(fields[7], ","), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	free, err := strconv.ParseUint(strings.TrimSuffix(fields[9], ","), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	used := total - free
	return total, used, nil
}

func (p *PfsCollector) collectPfsDiskInfo() error {
	var res string
	var err error

	pfsadmCmd := fmt.Sprintf("pfsadm info %s", p.pbdInfo.plsPrefix)
	pfsCmd := fmt.Sprintf("pfsadm info %s", p.pbdInfo.plsPrefix)

	if res, err = p.ExecCommand(pfsadmCmd); err != nil {
		p.logger.Error("exec pfsadm info failed. We will retry this use pfs info", err,
			log.String("cmd", pfsadmCmd))
		if res, err = p.ExecCommand(pfsCmd); err != nil {
			p.logger.Error("exec pfsadm info failed again", err,
				log.String("cmd", pfsCmd))
			return err
		}
	}

	if err = p.parsePbdInfo([]byte(res)); err != nil {
		p.logger.Error("parse pbd info failed", err, log.String("res", string(res)))
		return err
	}

	return nil
}

func (p *PfsCollector) collectPfsUsageResult(out map[string]interface{}) error {
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

	p.pbdInfo.pfsUsageInfoMap.Range(rangeFunc)
	return nil
}

func (p *PfsCollector) collectPfsDiskResult(out map[string]interface{}) error {
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

	p.pbdInfo.pfsDiskInfoMap.Range(rangeFunc)
	return nil
}

func (p *PfsCollector) collectPfsFormalizeResult(out map[string]interface{}) error {
	pls_inodes_list := []string{"pls_inode_total", "pls_inode_used", "pls_inode_usage"}
	formalize_inodes_list := []string{"fs_inodes_total", "fs_inodes_used", "fs_inodes_usage"}
	pls_blk_list := []string{"pls_blk_total", "pls_blk_used", "pls_blk_usage"}
	formalize_blk_list := []string{"fs_blocks_total", "fs_blocks_used", "fs_blocks_usage"}
	formalize_size_list := []string{"fs_size_total", "fs_size_used", "fs_size_usage"}

	// FS formalize
	for i, inode_key := range pls_inodes_list {
		if _, ok := out[inode_key]; ok {
			out[formalize_inodes_list[i]] = out[inode_key]
		}
	}

	for i, blk_key := range pls_blk_list {
		if _, ok := out[blk_key]; ok {
			if formalize_size_list[i] == "fs_size_usage" {
				out[formalize_blk_list[i]] = out[blk_key]
				out[formalize_size_list[i]] = out[blk_key]
			} else {
				out[formalize_blk_list[i]] = out[blk_key]
				// 4MB per block on pfs
				out[formalize_size_list[i]] = uint64(out[blk_key].(uint64) * 4)
			}
		}
	}

	out["enable_pfs"] = uint64(1)
	if _, ok := out["pls_base_dir_size"]; ok {
		out["polar_base_dir_size"] = out["pls_base_dir_size"]
		out["polar_wal_dir_size"] = out["pls_pg_wal_dir_size"]
	}

	return nil
}

func (p *PfsCollector) Collect(out map[string]interface{}) error {
	if err := p.collectPfsUsageResult(out); err != nil {
		p.logger.Error("collect pfs usage info failed.", err)
	}

	if err := p.collectPfsDiskResult(out); err != nil {
		p.logger.Error("collect pfs disk info failed.", err)
	}

	if err := p.collectPfsFormalizeResult(out); err != nil {
		p.logger.Error("formalize pfs collect result.", err)
	}

	return nil
}

func (p *PfsCollector) Stop() error {
	p.stopped <- 1

	p.cli.Close()

	return nil
}

func (p *PfsCollector) ExecCommand(cmd string) (string, error) {
	// fullcmd := exec.Command("timeout", strconv.Itoa(PFSCmdTimeout), "bash", "-c", cmd)
	fullcmd := fmt.Sprintf("timeout %d %s", PFSCmdTimeout, cmd)
	res, err := p.cli.ExecCommand(fullcmd)
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
