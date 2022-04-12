package collector

import (
	"encoding/binary"
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	// PERF_SAMPLE_IDENTIFIER is not defined in x/sys/unix.
	PERF_SAMPLE_IDENTIFIER = 1 << 16
)

type ProfileValue struct {
	Value uint64
	Id    uint64
}

type ProfileResultSet struct {
	Num         uint64
	TimeEnabled uint64
	TimeRunning uint64
	Values      []ProfileValue
}

type Profiler struct {
	fd  int
	num int
	buf []byte
}

type ReadableProfiler struct {
	Name           string
	CpuId          int
	resMappingList []EventConfig
	profiler       *Profiler
}

type EventKey struct {
	Name  string
	CpuId uint64
}

type EventConfig struct {
	Key       EventKey
	Type      uint64
	TypeStr   string
	Config    uint64
	ConfigStr string
	Config1   uint64
	Config2   uint64
}

type ReadableResultSet struct {
	TimeEnabled uint64
	TimeRunning uint64
	ResultMap   map[EventKey]uint64
}

const (
	DefaultPerfReadFormat = unix.PERF_FORMAT_TOTAL_TIME_RUNNING | unix.PERF_FORMAT_TOTAL_TIME_ENABLED | unix.PERF_FORMAT_GROUP | unix.PERF_FORMAT_ID
	DefaultPerfBits       = unix.PerfBitDisabled
)

func NewReadableProfiler(name string, cpuid int) *ReadableProfiler {
	return &ReadableProfiler{
		Name:           name,
		CpuId:          cpuid,
		resMappingList: make([]EventConfig, 0),
		profiler:       NewProfiler(),
	}
}

func (p *ReadableProfiler) HasEvent() bool {
	return len(p.resMappingList) > 0
}

func (p *ReadableProfiler) AddEvent(config EventConfig) error {
	var err error
	var perftype uint64
	var perfconfig uint64

	perftype = config.Type
	if config.TypeStr != "" {
		if perftype, err = ResolvePerfType(config.TypeStr); err != nil {
			return err
		}
	}

	perfconfig = config.Config
	if config.ConfigStr != "" {
		if perfconfig, err = ResolvePerfConfig(config.ConfigStr); err != nil {
			return err
		}
	}

	err = p.profiler.AddEvent(uint32(perftype), perfconfig, config.Config1, config.Config2, -1, int(config.Key.CpuId))
	if err != nil {
		return err
	}

	p.resMappingList = append(p.resMappingList, config)

	return nil
}

func (p *ReadableProfiler) Read(result *ReadableResultSet) error {
	var baseresult ProfileResultSet
	if err := p.profiler.Read(&baseresult); err != nil {
		return err
	}

	result.TimeEnabled = baseresult.TimeEnabled
	result.TimeRunning = baseresult.TimeRunning
	result.ResultMap = make(map[EventKey]uint64)

	for i, value := range baseresult.Values {
		if i > len(p.resMappingList) {
			return fmt.Errorf("value number[%d] is more than result config list[%d]",
				len(baseresult.Values), len(p.resMappingList))
		}

		result.ResultMap[p.resMappingList[i].Key] = value.Value
	}

	return nil
}

func (p *ReadableProfiler) Reset() error {
	return p.profiler.Reset()
}

func (p *ReadableProfiler) Start() error {
	return p.profiler.Start()
}

func (p *ReadableProfiler) Stop() error {
	return p.profiler.Stop()
}

func (p *ReadableProfiler) Close() error {
	return p.profiler.Close()
}

func NewProfiler() *Profiler {
	return &Profiler{
		fd:  0,
		num: 0,
		buf: make([]byte, 0),
	}
}

func (p *Profiler) checkInit() error {
	if p.num < 1 {
		return fmt.Errorf("may not be initialized, no event found at all")
	}

	if p.fd == 0 {
		return fmt.Errorf("may not be initialized, event fd is still zero")
	}

	return nil
}

func (p *Profiler) AddEvent(profilerType uint32,
	config, config1, config2 uint64,
	pid, cpu int) error {
	var err error

	eventAttr := &unix.PerfEventAttr{
		Type:        profilerType,
		Config:      config,
		Size:        uint32(unsafe.Sizeof(unix.PerfEventAttr{})),
		Bits:        DefaultPerfBits,
		Read_format: DefaultPerfReadFormat,
		Ext1:        config1,
		Ext2:        config2,
		Sample_type: PERF_SAMPLE_IDENTIFIER,
	}

	if p.fd != 0 {
		// group fd
		eventAttr.Bits = 0
		_, err = unix.PerfEventOpen(eventAttr, pid, cpu, p.fd, 0)
	} else {
		var fd int
		fd, err = unix.PerfEventOpen(eventAttr, pid, cpu, -1, 0)
		if err == nil {
			p.fd = fd
		}
	}

	if err != nil {
		return err
	}

	p.num += 1
	p.buf = make([]byte, 24+16*p.num)

	return nil
}

// Reset is used to reset the counters of the profiler.
func (p *Profiler) Reset() error {
	if err := p.checkInit(); err != nil {
		return err
	}
	return unix.IoctlSetInt(p.fd, unix.PERF_EVENT_IOC_RESET, unix.PERF_IOC_FLAG_GROUP)
}

// Start is used to Start the profiler.
func (p *Profiler) Start() error {
	if err := p.checkInit(); err != nil {
		return err
	}
	return unix.IoctlSetInt(p.fd, unix.PERF_EVENT_IOC_ENABLE, unix.PERF_IOC_FLAG_GROUP)
}

// Stop is used to stop the profiler.
func (p *Profiler) Stop() error {
	if err := p.checkInit(); err != nil {
		return err
	}
	return unix.IoctlSetInt(p.fd, unix.PERF_EVENT_IOC_DISABLE, unix.PERF_IOC_FLAG_GROUP)
}

// Profile returns the current Profile.
func (p *Profiler) Read(val *ProfileResultSet) error {
	if err := p.checkInit(); err != nil {
		return err
	}
	// The underlying struct that gets read from the profiler looks like
	// when using PERF_FORMAT_GROUP flag
	/*
			struct read_format {
		       u64 nr;            // The number of events
		       u64 time_enabled;  // if PERF_FORMAT_TOTAL_TIME_ENABLED
		       u64 time_running;  // if PERF_FORMAT_TOTAL_TIME_RUNNING
		       struct {
		           u64 value;     // The value of the event
		           u64 id;        // if PERF_FORMAT_ID
		       } values[nr];
		    };
	*/

	_, err := syscall.Read(p.fd, p.buf)
	if err != nil {
		fmt.Printf("error here: %s", err.Error())
		return err
	}
	val.Num = binary.LittleEndian.Uint64(p.buf[0:8])
	val.TimeEnabled = binary.LittleEndian.Uint64(p.buf[8:16])
	val.TimeRunning = binary.LittleEndian.Uint64(p.buf[16:24])
	val.Values = make([]ProfileValue, val.Num)
	for i := uint64(0); i < val.Num; i++ {
		val.Values[i].Value = binary.LittleEndian.Uint64(p.buf[24+16*i : 24+16*i+8])
		val.Values[i].Id = binary.LittleEndian.Uint64(p.buf[24+16*i+8 : 24+16*i+16])
	}

	return nil
}

// Close is used to close the perf context.
func (p *Profiler) Close() error {
	if err := p.checkInit(); err != nil {
		return err
	}
	return unix.Close(p.fd)
}
