/*-------------------------------------------------------------------------
 *
 * plugin_logger.go
 *    logger for polardb pg plugin
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
 *           common/polardb_pg/log/plugin_logger.go
 *-------------------------------------------------------------------------
 */
package log

import (
	"fmt"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
)

type PluginLogger struct {
	tag        string
	identifier map[string]string
}

func NewPluginLogger(tag string, identifier map[string]string) *PluginLogger {
	return &PluginLogger{tag: tag, identifier: identifier}
}

func (l *PluginLogger) SetTag(tag string) {
	l.tag = tag
}

func (l *PluginLogger) SetIdentifier(identifier map[string]string) {
	l.identifier = identifier
}

func (l *PluginLogger) logmsg(msg string) string {
	return fmt.Sprintf("[%s] %s", l.tag, msg)
}

func (l *PluginLogger) logfieldlist() []log.Field {
	fieldlist := make([]log.Field, 0, len(l.identifier))
	for k, v := range l.identifier {
		fieldlist = append(fieldlist, log.String(k, v))
	}

	return fieldlist
}

func (l *PluginLogger) Debug(msg string, fields ...log.Field) {
	fieldlist := l.logfieldlist()
	fieldlist = append(fieldlist, fields...)
	log.Debug(l.logmsg(msg), fieldlist...)
}

func (l *PluginLogger) Info(msg string, fields ...log.Field) {
	fieldlist := l.logfieldlist()
	fieldlist = append(fieldlist, fields...)
	log.Info(l.logmsg(msg), fieldlist...)
}

func (l *PluginLogger) Warn(msg string, err error, fields ...log.Field) {
	fieldlist := l.logfieldlist()
	if err != nil {
		fieldlist = append(fieldlist, log.String("error", err.Error()))
	}
	fieldlist = append(fieldlist, fields...)
	log.Warn(l.logmsg(msg), fieldlist...)
}

func (l *PluginLogger) Error(msg string, err error, fields ...log.Field) {
	fieldlist := l.logfieldlist()
	fieldlist = append(fieldlist, log.String("error", err.Error()))
	fieldlist = append(fieldlist, fields...)
	log.Error(l.logmsg(msg), fieldlist...)
}

func Binary(key string, val []byte) log.Field {
    return log.Binary(key, val)
}

func Bool(key string, val bool) log.Field {
    return log.Bool(key, val)
}

func ByteString(key string, val []byte) log.Field {
    return log.ByteString(key, val)
}

func String(key string, val string) log.Field {
    return log.String(key, val)
}

func Int(key string, val int) log.Field {
    return log.Int(key, val)
}

func Int8(key string, val int8) log.Field {
    return log.Int8(key, val)
}

func Int16(key string, val int16) log.Field {
    return log.Int16(key, val)
}

func Int32(key string, val int32) log.Field {
    return log.Int32(key, val)
}

func Int64(key string, val int64) log.Field {
	return log.Int64(key, val)
}

func Uint(key string, val uint) log.Field {
	return log.Uint(key, val)
}

func Uint8(key string, val uint8) log.Field {
	return log.Uint8(key, val)
}

func Uint16(key string, val uint16) log.Field {
	return log.Uint16(key, val)
}

func Uint32(key string, val uint32) log.Field {
	return log.Uint32(key, val)
}

func Uint64(key string, val uint64) log.Field {
	return log.Uint64(key, val)
}

func Float64(key string, val float64) log.Field {
	return log.Float64(key, val)
}

func Debug(msg string, fields ...log.Field) {
	log.Debug(msg, fields...)
}

func Info(msg string, fields ...log.Field) {
	log.Info(msg, fields...)
}

func Warn(msg string, fields ...log.Field) {
	log.Warn(msg, fields...)
}

func Error(msg string, fields ...log.Field) {
	log.Error(msg, fields...)
}
