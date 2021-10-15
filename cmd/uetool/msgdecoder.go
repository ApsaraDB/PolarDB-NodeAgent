/*-------------------------------------------------------------------------
 *
 * msgdecoder.go
 *    Decoder for local metrics
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
 *           cmd/uetool/msgdecoder.go
 *-------------------------------------------------------------------------
 */
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ApsaraDB/db-monitor/internal/gather"
)

type schemaDecode struct {
	headers map[int]string
	metrics map[int]string
}

func mapSector2Size(strArr []string) []int {
	l := len(strArr)
	ret := make([]int, l)
	for i := 0; i < l; i++ {
		v, _ := strconv.Atoi(strArr[i])
		v /= 2048
		ret[i] = v
	}
	return ret
}

func mapBlock2Size(strArr []string, size int) []string {
	l := len(strArr)
	ret := make([]string, l)
	for i := 0; i < l; i++ {
		blocks, _ := strconv.Atoi(strArr[i])
		ret[i] = strconv.Itoa(blocks * size / 1024 / 1024)
	}
	return ret
}

func mapAtoi(strArr []string) []int {
	l := len(strArr)
	ret := make([]int, l)
	for i := 0; i < l; i++ {
		v, _ := strconv.Atoi(strArr[i])
		ret[i] = v
	}
	return ret
}

func mapMem(strArr []string) []string {
	l := len(strArr)
	ret := make([]string, l)
	for i := 0; i < l; i++ {
		size, _ := strconv.Atoi(strArr[i])
		ret[i] = strconv.Itoa(size * 4 / 1024)
	}
	return ret
}

func mapAwait(iops, iotime []int) []string {
	l := len(iops)
	ret := make([]string, l)
	for i := 0; i < l; i++ {
		var await float64
		if iotime[i] == 0 || iops[i] == 0 {
			await = 0
		} else {
			await = float64(iotime[i]) / float64(iops[i])
		}
		ret[i] = strconv.FormatFloat(await, 'f', 5, 32)
	}
	return ret
}

var itemPos int
var itemBegin int

func eachItem(line []byte) (string, bool) {
	itemPos = bytes.IndexByte(line[itemBegin:], ';')
	if itemPos < 0 {
		return "", false
	}
	ret := string(line[itemBegin : itemBegin+itemPos])
	itemBegin += itemPos + 1
	return ret, true
}

func main() {
	dataFile := flag.String("d", "", "-d, datapath /path/to/datafile")
	schemaFile := flag.String("s", "", "-s, schema path /path/to/schema.json")
	filter := flag.String("f", "", "-f, filter mode support cpu/mem/disk/net/tcp/fs # only host metrics")
	pid := flag.String("p", "", "-p pid # only host metrics")
	tmRange := flag.String("t", "", "-t start,end  #  example: -t 19:55:00,19:56:00")
	list := flag.String("l", "", "-l, list special metrics # host and db metrics")
	hintsCount := flag.Int("i", 30, "-i # print metrics name per line")
	cpu := flag.String("c", "", "-c cpu or startCPU-endCPU # example: -c 1-3 only host metrics")
	//_:=flag.Bool("interval",false,"--interval show collector interval")
	flag.Parse()

	if *schemaFile == "" || *dataFile == "" {
		flag.Usage()
		return
	}

	var err error
	tmStart := "00:00:00"
	tmEnd := "25:00:00"
	if *tmRange != "" {
		toks := strings.Split(*tmRange, ",")
		if len(toks) != 2 {
			flag.Usage()
			return
		}
		tmStart = strings.TrimSpace(toks[0])
		tmEnd = strings.TrimSpace(toks[1])
	}

	startCPU := 0
	endCPU := 0
	if *cpu != "" {
		toks := strings.Split(*cpu, "-")
		if len(toks) == 1 {
			startCPU, err = strconv.Atoi(toks[0])
			endCPU = startCPU
			if err != nil {
				flag.Usage()
				return
			}
		} else {
			startCPU, err = strconv.Atoi(toks[0])
			if err != nil {
				flag.Usage()
				return
			}
			endCPU, err = strconv.Atoi(toks[1])
			if err != nil {
				flag.Usage()
				return
			}
		}
	}

	bt, err := ioutil.ReadFile(*schemaFile)
	if err != nil {
		fmt.Printf("read file %s error: %s", *schemaFile, err.Error())
		return
	}
	var buf bytes.Buffer
	bs, _ := gather.JSON2Schema(bt, &buf)
	var sd schemaDecode
	sd.metrics = make(map[int]string)
	sd.headers = make(map[int]string)
	for _, v := range bs.Headers.SchemaItems {
		sd.headers[v.Index] = v.Name
	}
	for _, v := range bs.Metrics.SchemaItems {
		sd.metrics[v.Index] = v.Name
	}

	headerLen := len(bs.Headers.SchemaItems)
	totalLen := len(bs.Headers.SchemaItems) + len(bs.Metrics.SchemaItems)
	datafile, err := os.Open(*dataFile)
	if err != nil {
		fmt.Printf("open file %s error: %s", *dataFile, err.Error())
		return
	}

	reader := bufio.NewReader(datafile)

	var hints string
	if *filter == "cpu" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|index\t|usr\t|sys\t|idle\t|si\t|iowait\t|nice\t|\033[0m")
	} else if *filter == "disk" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|disk\t|queue\t|read\t|r_iops\t|r_await\t|write\t|w_iops\t|w_await\t\033[0m|")
	} else if *filter == "mem" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|memtotal\t|mem free\t|mem buffer\t|cached\t\t|active\t\t|inactive\t|dirty\t|pagetables\t|hugetotal\t|hugefree\t|\033[0m")
	} else if *filter == "net" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|nic\t|rcvbyte|rcvpack|rcvdrop|rcverr |sntbyte|sntpack|sntdrop|snterr |\033[0m")
	} else if *filter == "fs" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|fs\t|free\t|\033[0m\n")
	} else if *filter == "tcp" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|alloc\t|inuse\t|listendrop\t|listenoverflow\t|fastRetrans\t|forwardRetrans\t|retransSeg\t|\033[0m")
	}
	if *pid != "" {
		hints = fmt.Sprintf("\033[0;31mtimestamp\t\t|usr\t|sys\t|read\t|write\t|write_cancel\t|mem\t|D\t|\033[0m")
	}

	var dbMetrics []string
	var ms []string
	if *list != "" {
		dbMetrics = strings.Split(*list, ",")
		ms = make([]string, len(dbMetrics))
		hints = "\033[0;31mtimestamp\t\t|"
		for _, cur := range dbMetrics {
			hints += cur + "|"
		}
		hints += "\033[0m"
	}

	fmt.Println(hints)

	if dbMetrics != nil {
		if *filter != "" {
			flag.Usage()
			return
		}
		if *pid != "" {
			flag.Usage()
			return
		}
	}

	outputline := 0
	buf.Reset()
	for {
		itemBegin = 0
		itemPos = 0
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			fmt.Printf("read file %s error: %s\n", *dataFile, err.Error())
			return
		}

		if len(line) == 0 {
			break
		}
		if len(line) == 1 && line[0] == '\n' {
			continue
		}
		nextLine := false
		if outputline%*hintsCount == 0 {
			if outputline != 0 {
				fmt.Println(hints)
			}
		}

		i := 0
		//fmt.Println(len(line), "####", string(line))
		verStr, _ := eachItem(line)
		ver, err := strconv.ParseUint(verStr, 10, 32)
		if err != nil {
			fmt.Printf("ERROR: cannot parse data\n")
			goto out
		}
		if bs.Version != uint32(ver) {
			fmt.Printf("WARNING: version does not match, data version: %d, schema version: %d\n", uint32(ver), bs.Version)
			goto out
		}
		itemBegin = 0
		itemPos = 0

		if dbMetrics != nil {
			count := 0
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr := tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
					buf.WriteString(tmstr + "\t|")
				}

				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					for idx, curMetrics := range dbMetrics {
						if name == curMetrics {
							ms[idx] = item
							count++
						}
					}

					if count == len(dbMetrics) {
						for _, cur := range ms {
							buf.WriteString(cur + "\t|")
						}
						break
					}
				}
			}
			if nextLine {
				continue
			}
			fmt.Println(buf.String())
			buf.Reset()
			outputline++
		} else if *filter == "fs" {
			mask := 0
			var tmstr string
			var fs, size, free []string
			for item, ret := eachItem(line); ret; i++ {
				//for i := 0; i < len(items); i++ {
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr = tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						break
					}
				}
				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "fs_partition_list" {
						mask |= 1
						fs = strings.Split(item, ",")
					} else if name == "bsize" {
						size = strings.Split(item, ",")
						mask |= 2
					} else if name == "bfree" {
						free = strings.Split(item, ",")
						mask |= 4
					}
					if mask == 7 {
						break
					}
				}
			}
			blkSize := 0
			if len(fs) > 0 {
				blkSize, _ = strconv.Atoi(size[0])
			}
			freeSize := mapBlock2Size(free, blkSize)
			for i := range fs {
				buf.WriteString(tmstr + "\t|")
				buf.WriteString(fs[i] + "\t|")
				buf.WriteString(freeSize[i] + "\t|")
				fmt.Println(buf.String())
				buf.Reset()
				outputline++
			}
		} else if *filter == "cpu" {
			mask := 0
			var tmstr string
			var sys, user, si, nice, iowait, idle []string
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr = tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
				}
				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "system_delta" {
						sys = strings.Split(item, ",")
						mask |= 1
					} else if name == "user_delta" {
						user = strings.Split(item, ",")
						mask |= 2
					} else if name == "softirq_delta" {
						si = strings.Split(item, ",")
						mask |= 4
					} else if name == "idle_delta" {
						idle = strings.Split(item, ",")
						mask |= 8
					} else if name == "iowait_delta" {
						iowait = strings.Split(item, ",")
						mask |= 16
					} else if name == "nice_delta" {
						nice = strings.Split(item, ",")
						mask |= 32
					}
					if mask == 63 {
						break
					}
				}
			}
			if nextLine {
				continue
			}
			if *cpu == "" {
				buf.WriteString(tmstr + "\t|")
				buf.WriteString("cpu\t|")
				buf.WriteString(user[0] + "\t|")
				buf.WriteString(sys[0] + "\t|")
				buf.WriteString(idle[0] + "\t|")
				buf.WriteString(si[0] + "\t|")
				buf.WriteString(iowait[0] + "\t|")
				buf.WriteString(nice[0] + "\t|")
				fmt.Println(buf.String())
				buf.Reset()
				outputline++
			} else {
				if endCPU >= len(user)-1 {
					endCPU = len(user) - 2
				}
				if startCPU >= len(user)-1 {
					flag.Usage()
					os.Exit(0)
				}
				if startCPU == endCPU {
					i := startCPU + 1
					buf.WriteString(tmstr + "\t|")
					buf.WriteString("cpu" + strconv.Itoa(i-1) + "\t|")
					buf.WriteString(user[i] + "\t|")
					buf.WriteString(sys[i] + "\t|")
					buf.WriteString(idle[i] + "\t|")
					buf.WriteString(si[i] + "\t|")
					buf.WriteString(iowait[i] + "\t|")
					buf.WriteString(nice[i] + "\t|")
					fmt.Println(buf.String())
					buf.Reset()
					outputline++
				}
				for i := startCPU; i <= endCPU; i++ {
					buf.Reset()
					outputline++
					buf.WriteString(tmstr + "\t|")
					buf.WriteString("cpu" + strconv.Itoa(i) + "\t|")
					buf.WriteString(user[i+1] + "\t|")
					buf.WriteString(sys[i+1] + "\t|")
					buf.WriteString(idle[i+1] + "\t|")
					buf.WriteString(si[i+1] + "\t|")
					buf.WriteString(iowait[i+1] + "\t|")
					buf.WriteString(nice[i+1] + "\t|")
					fmt.Println(buf.String())
					buf.Reset()
					outputline++
				}
				if startCPU == 0 && endCPU == len(user)-2 {
					buf.WriteString(tmstr + "\t|")
					buf.WriteString("total\t|")
					buf.WriteString(user[0] + "\t|")
					buf.WriteString(sys[0] + "\t|")
					buf.WriteString(idle[0] + "\t|")
					buf.WriteString(si[0] + "\t|")
					buf.WriteString(iowait[0] + "\t|")
					buf.WriteString(nice[0] + "\t|")
					fmt.Println(buf.String())
					buf.Reset()
					outputline++
				}
			}
		} else if *filter == "disk" {
			var disks []string
			var rs []string       // read sector
			var ws []string       // write sector
			var inflight []string // inflight
			var wc []string       // write complete
			var rc []string       // read complete
			var wt []string       // write time
			var rt []string       // read time
			mask := 0
			var tmstr string
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr = tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
				}
				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "disk_list" {
						disks = strings.Split(item, ",")
						mask |= 1
					} else if name == "read_complete_delta" {
						rc = strings.Split(item, ",")
						mask |= 2
					} else if name == "read_sectors_delta" {
						rs = strings.Split(item, ",")
						mask |= 4
					} else if name == "read_time_delta" {
						rt = strings.Split(item, ",")
						mask |= 8
					} else if name == "write_complete_delta" {
						wc = strings.Split(item, ",")
						mask |= 16
					} else if name == "write_sectors_delta" {
						ws = strings.Split(item, ",")
						mask |= 32
					} else if name == "write_time_delta" {
						wt = strings.Split(item, ",")
						mask |= 64
					} else if name == "io_in_flight" {
						inflight = strings.Split(item, ",")
						mask |= 127
					}
					if mask == 255 {
						break
					}
				}
			}
			if nextLine {
				continue
			}

			rsI := mapSector2Size(rs)
			wsI := mapSector2Size(ws)
			rcI := mapAtoi(rc)
			wcI := mapAtoi(wc)
			rtI := mapAtoi(rt)
			wtI := mapAtoi(wt)
			rwait := mapAwait(rcI, rtI)
			wwait := mapAwait(wcI, wtI)
			l := len(rsI)
			for i := 0; i < l; i++ {
				buf.WriteString(tmstr + "\t|")
				buf.WriteString(disks[i] + "\t|")
				buf.WriteString(inflight[i] + "\t|")
				buf.WriteString(strconv.Itoa(rsI[i]) + "\t|")
				buf.WriteString(rc[i] + "\t|")
				buf.WriteString(rwait[i] + "\t|")
				buf.WriteString(strconv.Itoa(wsI[i]) + "\t|")
				buf.WriteString(wc[i] + "\t|")
				buf.WriteString(wwait[i] + "\t|\n")
			}
			fmt.Println(buf.String())
			buf.Reset()
			outputline++
		} else if *filter == "mem" {
			mask := 0
			var total, free, buffer, cached, active, inactive, dirty, pt, hugeTotal, hugeFree string
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr := tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
					buf.WriteString(tmstr + "\t|")
				}

				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "MemTotal" {
						total = item
						mask |= 1
					} else if name == "MemFree" {
						free = item
						mask |= 2
					} else if name == "Buffers" {
						buffer = item
						mask |= 4
					} else if name == "Cached" {
						cached = item
						mask |= 8
					} else if name == "Active" {
						active = item
						mask |= 16
					} else if name == "Inactive" {
						inactive = item
						mask |= 32
					} else if name == "Dirty" {
						dirty = item
						mask |= 64
					} else if name == "PageTables" {
						pt = item
						mask |= 128
					} else if name == "HugePages_Total" {
						hugeTotal = item
						mask |= 256
					} else if name == "HugePages_Free" {
						hugeFree = item
						mask |= 512
					}
					if mask == 1023 {
						break
					}
				}
			}
			if nextLine {
				continue
			}
			buf.WriteString(total + "\t|")
			buf.WriteString(free + "\t|")
			buf.WriteString(buffer + "\t|")
			buf.WriteString(cached + "\t|")
			buf.WriteString(active + "\t|")
			buf.WriteString(inactive + "\t|")
			buf.WriteString(dirty + "\t|")
			buf.WriteString(pt + "\t\t|")
			buf.WriteString(hugeTotal + "\t\t|")
			buf.WriteString(hugeFree + "\t\t|")
			fmt.Println(buf.String())
			buf.Reset()
			outputline++
		} else if *filter == "tcp" {
			var tmstr string
			mask := 0
			var alloc, inuse, listenDrop, listenOverflow, fastRetrans, forwardRetrans, retransSeg string
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr = tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
				}
				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "tcp_alloc" {
						alloc = item
						mask |= 1
					} else if name == "tcp_inuse" {
						inuse = item
						mask |= 2
					} else if name == "ListenOverflows_delta" {
						listenOverflow = item
						mask |= 4
					} else if name == "ListenDrops_delta" {
						listenDrop = item
						mask |= 8
					} else if name == "TCPFastRetrans_delta" {
						fastRetrans = item
						mask |= 16
					} else if name == "TCPForwardRetrans_delta" {
						forwardRetrans = item
						mask |= 32
					} else if name == "RetransSegs_delta" {
						retransSeg = item
						mask |= 64
					}
					if mask == 127 {
						break
					}
				}
			}
			if nextLine {
				continue
			}
			buf.WriteString(tmstr + "\t|")
			buf.WriteString(alloc + "\t|")
			buf.WriteString(inuse + "\t|")
			buf.WriteString(listenOverflow + "\t\t|")
			buf.WriteString(listenDrop + "\t\t|")
			buf.WriteString(fastRetrans + "\t\t|")
			buf.WriteString(forwardRetrans + "\t\t|")
			buf.WriteString(retransSeg + "\t\t|")
			fmt.Println(buf.String())
			buf.Reset()
			outputline++
		} else if *filter == "net" {
			var tmstr string
			mask := 0
			var nic, recvPack, recvByte, recvDrop, recvErr []string
			var sentPack, sentByte, sentDrop, sentErr []string
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr = tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						continue
					}
				}
				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "nic_list" {
						nic = strings.Split(item, ",")
						mask |= 1
					} else if name == "recv_bytes_delta" {
						recvByte = strings.Split(item, ",")
						mask |= 2
					} else if name == "recv_packets_delta" {
						recvPack = strings.Split(item, ",")
						mask |= 4
					} else if name == "recv_drop_delta" {
						recvDrop = strings.Split(item, ",")
						mask |= 8
					} else if name == "recv_errs_delta" {
						recvErr = strings.Split(item, ",")
						mask |= 16
					} else if name == "transmit_bytes_delta" {
						sentByte = strings.Split(item, ",")
						mask |= 32
					} else if name == "transmit_packets_delta" {
						sentPack = strings.Split(item, ",")
						mask |= 64
					} else if name == "transmit_errs_delta" {
						sentErr = strings.Split(item, ",")
						mask |= 128
					} else if name == "transmit_drop_delta" {
						sentDrop = strings.Split(item, ",")
						mask |= 256
					}
					if mask == 511 {
						break
					}
				}
			}
			if nextLine {
				continue
			}
			l := len(nic)
			for i := 0; i < l; i++ {
				buf.WriteString(tmstr + "\t|")
				buf.WriteString(nic[i] + "\t|")
				buf.WriteString(recvByte[i] + "\t|")
				buf.WriteString(recvPack[i] + "\t|")
				buf.WriteString(recvErr[i] + "\t|")
				buf.WriteString(recvDrop[i] + "\t|")
				buf.WriteString(sentByte[i] + "\t|")
				buf.WriteString(sentPack[i] + "\t|")
				buf.WriteString(sentErr[i] + "\t|")
				buf.WriteString(sentDrop[i] + "\t|")
				fmt.Println(buf.String())
				buf.Reset()
				outputline++
			}
		} else if *pid != "" {
			target := -1
			var rbs []string
			var wbs []string
			var cwbs []string
			var utime []string
			var stime []string
			var mem []string
			var dstatus []string
			mask := 0
			for i = 0; i < totalLen; i++ {
				item, ret := eachItem(line)
				if !ret {
					break
				}
				if i < headerLen && sd.headers[i] == "time" {
					value, _ := strconv.ParseInt(item, 10, 64)
					tm := time.Unix(value/1000, 0)
					tmstr := tm.Format("01/02/2006 15:04:05")
					detail := tmstr[strings.IndexByte(tmstr, ' ')+1:]
					if !(detail >= tmStart && detail <= tmEnd) {
						nextLine = true
						break
					}
					buf.WriteString(tmstr + "\t|")
				}

				if i >= headerLen {
					name := sd.metrics[i-headerLen]
					if name == "sar_process_pid" {
						pids := strings.Split(item, ",")
						for idx, curPid := range pids {
							if curPid == *pid {
								target = idx
								break
							}
						}
						mask |= 1
					} else if name == "sar_process_read_bytes" {
						rbs = strings.Split(item, ",")
						mask |= 2
					} else if name == "sar_process_write_bytes" {
						wbs = strings.Split(item, ",")
						mask |= 4
					} else if name == "sar_process_cancelled_write_bytes" {
						cwbs = strings.Split(item, ",")
						mask |= 8
					} else if name == "sar_process_utime" {
						utime = strings.Split(item, ",")
						mask |= 16
					} else if name == "sar_process_stime" {
						stime = strings.Split(item, ",")
						mask |= 32
					} else if name == "sar_process_D_status" {
						dstatus = strings.Split(item, ",")
						mask |= 64
					} else if name == "sar_process_rss" {
						mem = strings.Split(item, ",")
						mask |= 128
					}
					if mask == 255 {
						break
					}
				}
			}
			if nextLine {
				continue
			}
			if target == -1 {
				fmt.Println(buf.String())
				buf.Reset()
				continue
			}
			buf.WriteString(utime[target] + "\t|")
			buf.WriteString(stime[target] + "\t|")
			buf.WriteString(rbs[target] + "\t|")
			buf.WriteString(wbs[target] + "\t|")
			buf.WriteString(cwbs[target] + "\t\t|")
			memMB := mapMem(mem)
			buf.WriteString(memMB[target] + "\t|")
			buf.WriteString(dstatus[target] + "\t|")
			fmt.Println(buf.String())
			buf.Reset()
			outputline++
		}
	}
out:
}
