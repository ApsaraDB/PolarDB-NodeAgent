/*-------------------------------------------------------------------------
 *
 * logger.go
 *    Logger wrapper
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
 *           common/log/logger.go
 *-------------------------------------------------------------------------
 */
package log

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"

        "github.com/ApsaraDB/db-monitor/common/utils"
)

const (
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
)

type Conf struct {
	Scan       bool   `json:"scan,omitempty"`
	ScanPeriod int    `json:"scan_period,omitempty"`
	Production bool   `json:"production,omitempty"`
	Level      string `json:"level,omitempty"`
	Filename   string `json:"filename"`
	Compress   bool   `json:"compress"` // compress using gzip?
	MaxSizeMB  int    `json:"max_size_mb"`
	MaxBackups int    `json:"max_backups"`
	MaxAgeDays int    `json:"max_age_days"`
}

type LogErrorMessage struct {
	ErrorCode int    `json:"errorCode"`
	ErrorMsg  string `json:"errorMsg"`
}

type Field = zap.Field

var zapLogger *zap.Logger
var errorLogger *zap.Logger

func Init() {
	filename := "conf/log.json"
	logfile := utils.GetBasePath() + "/universe.log"
	logConf, err := unmarshalConf(filename)
	if err != nil {
		logConf = &Conf{
			Scan:       false,
			ScanPeriod: 60,
			Production: false,
			Level:      "INFO",
			Filename:   logfile,
			Compress:   false,
			MaxSizeMB:  50,
			MaxBackups: 20,
			MaxAgeDays: 7,
		}
		fmt.Printf("[WARNING] initialize log conf failed, use default conf: %v", logConf)
	}

	level := level(logConf)
	encoderConfig := newEncoderConfig(logConf.Production)

	if !strings.HasPrefix(logConf.Filename, "/") {
		logfile = utils.GetBasePath() + "/" + logConf.Filename
	} else {
		logfile = logConf.Filename
	}

	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logfile,
		MaxSize:    logConf.MaxSizeMB, // megabytes
		MaxBackups: logConf.MaxBackups,
		MaxAge:     logConf.MaxAgeDays, // days
		Compress:   logConf.Compress,
	})
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		w, // os.Stdout while debug
		level,
	)
	logger := zap.New(core)

	zapLogger = logger
}

func newEncoderConfig(production bool) zapcore.EncoderConfig {
	if production {
		return zap.NewProductionEncoderConfig()
	} else {
		return zap.NewDevelopmentEncoderConfig()
	}
}

func level(logConf *Conf) zapcore.Level {
	level := zap.InfoLevel
	switch strings.ToUpper(logConf.Level) {
	case LevelDebug:
		level = zap.DebugLevel
	case LevelInfo:
		level = zap.InfoLevel
	case LevelWarn:
		level = zap.WarnLevel
	case LevelError:
		level = zap.ErrorLevel
	}
	return level
}

func unmarshalConf(filename string) (*Conf, error) {
	logConf := &Conf{}

	if fi, err := os.Stat(filename); os.IsNotExist(err) || fi.IsDir() {
		return nil, fmt.Errorf("log file not found: %s\n", filename)
	}

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read log file failed: %s, err: %s\n", filename, err.Error())
	}

	err = json.Unmarshal(content, logConf)
	if err != nil {
		return nil, fmt.Errorf("unmarshal log file failed: %s, err: %s\n", filename, err.Error())
	}

	return logConf, nil
}

func Binary(key string, val []byte) Field {
	return zap.Binary(key, val)
}

func Bool(key string, val bool) Field {
	return zap.Bool(key, val)
}

func ByteString(key string, val []byte) Field {
	return zap.ByteString(key, val)
}

func String(key string, val string) Field {
	return zap.String(key, val)
}

func Int(key string, val int) Field {
	return zap.Int(key, val)
}

func Int8(key string, val int8) Field {
	return zap.Int8(key, val)
}

func Int16(key string, val int16) Field {
	return zap.Int16(key, val)
}

func Int32(key string, val int32) Field {
	return zap.Int32(key, val)
}

func Int64(key string, val int64) Field {
	return zap.Int64(key, val)
}

func Uint(key string, val uint) Field {
	return zap.Uint(key, val)
}

func Uint8(key string, val uint8) Field {
	return zap.Uint8(key, val)
}

func Uint16(key string, val uint16) Field {
	return zap.Uint16(key, val)
}

func Uint32(key string, val uint32) Field {
	return zap.Uint32(key, val)
}

func Uint64(key string, val uint64) Field {
	return zap.Uint64(key, val)
}

func Float64(key string, val float64) Field {
	return zap.Float64(key, val)
}

func Sync() {
	zapLogger.Sync()
}

func Debug(msg string, fields ...Field) {
	zapLogger.Debug(msg, fields...)
}

func Info(msg string, fields ...Field) {
	zapLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...Field) {
	zapLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...Field) {
	zapLogger.Error(msg, fields...)
}
