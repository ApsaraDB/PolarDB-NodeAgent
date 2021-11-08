/*-------------------------------------------------------------------------
 *
 * plugin_logger.go
 *    Logger for plugin
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
 *           common/polardb_pg/logger/plugin_logger.go
 *-------------------------------------------------------------------------
 */
package logger

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
