/*-------------------------------------------------------------------------
 *
 * logger_test.go
 *    Logger test case
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
 *           common/log/Logger_test.go
 *-------------------------------------------------------------------------
 */
package log

import (
	"log"
	"os"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func TestLoggerNormal(t *testing.T) {
	Init()
	Info("abcdddfadfafafafafafafafafafafafa")
}

func TestGolangLog(t *testing.T) {
	logger := log.New(&lumberjack.Logger{
		Filename:   "/tmp/logs",
		MaxSize:    50, // megabytes
		MaxBackups: 10,
		MaxAge:     28, // days
	}, "", 0)

	logger.Println("abcdddfadfafafafafafafafafafafafa")
	teardown("/tmp/logs")
}

func TestLoggerDataPrint(t *testing.T) {

	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "/tmp/data",
		MaxSize:    50, // megabytes
		MaxBackups: 10,
		MaxAge:     28, // days
	})
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		w, // os.Stdout while debug
		zap.InfoLevel,
	)
	logger := zap.New(core)
	logger.Info("fff")
}

// BenchmarkZapLog-8   	  200000	      7371 ns/op
func BenchmarkZapLog(b *testing.B) {
	Init()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info("abcdddfadfafafafafafafafafafafafa")
	}
	b.StopTimer()
	teardown("/tmp/logs")
}

// BenchmarkGolangLog-8   	  200000	      5849 ns/op
func BenchmarkGolangLog(b *testing.B) {
	logger := log.New(&lumberjack.Logger{
		Filename:   "/tmp/logs",
		MaxSize:    50, // megabytes
		MaxBackups: 10,
		MaxAge:     28, // days
	}, "", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Println("abcdddfadfafafafafafafafafafafafa")
	}
	b.StopTimer()
	teardown("/tmp/logs")
}

func teardown(filePath string) {
	os.Remove(filePath)
}
