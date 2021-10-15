/*-------------------------------------------------------------------------
 *
 * data_logger.go
 *    Data Logger
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
 *           common/log/data_logger.go
 *-------------------------------------------------------------------------
 */
package log

import (
	"bytes"
	"fmt"
	"log"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	
	"github.com/ApsaraDB/db-monitor/common/utils"
)

type DataLogger struct {
	logger *log.Logger
	buff   bytes.Buffer
}

// InitDataLogger init the data logger
func NewDataLogger(suffix, conf string) *DataLogger {
	logConf, err := unmarshalConf(conf)
	if err != nil {
		logConf = &Conf{
			Filename:   utils.GetBasePath() + "/data/data",
			Compress:   true,
			MaxSizeMB:  64,
			MaxBackups: 5,
			MaxAgeDays: 1,
		}
		fmt.Printf("[WARNING] initialize data conf failed, use default conf: %v", logConf)
		Warn("initialize data conf failed, use default conf",
			String("detail", fmt.Sprintf("%v", logConf)))
	}
	logger := log.New(&lumberjack.Logger{
		Filename:   logConf.Filename + "_" + suffix,
		MaxSize:    logConf.MaxSizeMB, // megabytes
		MaxBackups: logConf.MaxBackups,
		MaxAge:     logConf.MaxAgeDays, // days
		Compress:   logConf.Compress,
	}, "", 0)
	return &DataLogger{
		logger: logger,
	}
}

// PrintData print the original data to file, with one data per line
// not thread safe
func (d *DataLogger) PrintData(data string) {
	d.logger.Output(2, data)
}

func (d *DataLogger) BufferedPrintData(data string) {
	if d.buff.Len() > 64*1024 { // 经测试64KB效率最高,约 2.3us
		d.FlushData()
	}
	d.buff.WriteString(data)
	d.buff.WriteByte(0x0A)
}

func (d *DataLogger) FlushData() {
	d.logger.Output(2, d.buff.String())
	d.buff.Reset()
}
