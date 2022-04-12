package collector

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func PrintErrorIfExists(err error) {
	if err != nil {
		fmt.Printf("err: %s", err.Error())
	}
}

func TestProfiler(t *testing.T) {
	var err error
	var result ProfileResultSet

	profiler := NewProfiler()

	// cannot start at first
	err = profiler.Start()
	assert.Errorf(t, err, "start failed when no event added err=%s", err.Error())

	// add one event
	err = profiler.AddEvent(unix.PERF_TYPE_HARDWARE, unix.PERF_COUNT_HW_CPU_CYCLES, 0, 0, -1, 20)
	PrintErrorIfExists(err)
	assert.NoError(t, err)

	err = profiler.Start()
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	err = profiler.Read(&result)
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	assert.Truef(t, len(result.Values) == 1, "profile result: %+v", result)

	// add another event
	err = profiler.AddEvent(unix.PERF_TYPE_HARDWARE, unix.PERF_COUNT_HW_CACHE_MISSES, 0, 0, -1, 20)
	PrintErrorIfExists(err)
	assert.NoError(t, err)

	err = profiler.Read(&result)
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	assert.Truef(t, len(result.Values) == 2, "profile result: %+v", result)

	// stop and close
	err = profiler.Stop()
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	err = profiler.Close()
	assert.NoError(t, err)
}

func TestReadableProfiler(t *testing.T) {
	var err error
	var profiler *ReadableProfiler
	var result ReadableResultSet

	config1 := EventConfig{
		Key:       EventKey{Name: "test1", CpuId: 20},
		TypeStr:   "PERF_TYPE_HARDWARE",
		ConfigStr: "PERF_COUNT_HW_CPU_CYCLES",
	}

	config2 := EventConfig{
		Key:       EventKey{Name: "test2", CpuId: 20},
		TypeStr:   "PERF_TYPE_HARDWARE",
		ConfigStr: "PERF_COUNT_HW_CACHE_MISSES",
	}

	profiler = NewReadableProfiler("test", 20)
	PrintErrorIfExists(err)
	assert.NoError(t, err)

	// cannot start at first
	err = profiler.Start()
	assert.Errorf(t, err, "start failed when no event added err=%s", err.Error())

	// add one event
	err = profiler.AddEvent(config1)
	PrintErrorIfExists(err)
	assert.NoError(t, err)

	err = profiler.Start()
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	err = profiler.Read(&result)
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	assert.Truef(t, len(result.ResultMap) == 1, "profile result: %+v", result)

	// add another event
	err = profiler.AddEvent(config2)
	PrintErrorIfExists(err)
	assert.NoError(t, err)

	err = profiler.Read(&result)
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	assert.Truef(t, len(result.ResultMap) == 2, "profile result: %+v", result)

	// stop and close
	err = profiler.Stop()
	PrintErrorIfExists(err)
	assert.NoError(t, err)
	err = profiler.Close()
	assert.NoError(t, err)
}
