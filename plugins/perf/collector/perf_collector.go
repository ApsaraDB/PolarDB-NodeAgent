package collector

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/ApsaraDB/PolarDB-NodeAgent/common/log"
	"gopkg.in/yaml.v2"
)

type PerfEvent struct {
	Name      string `yaml:"name"`
	Type      uint64 `yaml:"type"`
	TypeStr   string `yaml:"type_str"`
	Config    uint64 `yaml:"config"`
	ConfigStr string `yaml:"config_str"`
	Config1   uint64 `yaml:"config1"`
	Config2   uint64 `yaml:"config2"`
}

type PerfEventGroup struct {
	Name   string      `yaml:"name"`
	Enable bool        `yaml:"enable"`
	CpuIds []int       `yaml:"cpuids"`
	Events []PerfEvent `yaml:"events"`
}

type CpuEvents struct {
	CpuModel    string           `yaml:"cpu_model"`
	EventGroups []PerfEventGroup `yaml:"event_groups"`
}

type PerfEventConf struct {
	CpuEvents []CpuEvents `yaml:"cpu_events"`
}

type PreValue struct {
	LastTimestamp int64
	Value         uint64
}

type PerfCollector struct {
	preValueMap   map[string]PreValue
	timestamp     int64
	eventGroupMap map[string]PerfEventGroup
	profilers     []*ReadableProfiler
}

// New create new PerfCollector
func New() *PerfCollector {
	return &PerfCollector{
		preValueMap: make(map[string]PreValue),

		eventGroupMap: make(map[string]PerfEventGroup),
		profilers:     make([]*ReadableProfiler, 0),
	}
}

func (c *PerfCollector) loadPerfEventConfFile(filepath string) (*PerfEventConf, error) {
	var buf bytes.Buffer
	var perfEventConf PerfEventConf

	fp, err := os.Open(filepath)
	if err != nil {
		log.Error("[perf_collector] open perf event conf file failed",
			log.String("error", err.Error()))
		return nil, err
	}
	defer fp.Close()

	_, err = buf.ReadFrom(fp)
	if err != nil {
		log.Error("[perf_collector] read perf event conf file failed",
			log.String("error", err.Error()))
		return nil, err
	}

	err = yaml.Unmarshal(buf.Bytes(), &perfEventConf)
	if err != nil {
		log.Error("[perf_collector] read perf event conf file failed",
			log.String("error", err.Error()))
		return nil, err
	}

	return &perfEventConf, nil
}

func (c *PerfCollector) initPerfEventConf(perfEventConf *PerfEventConf) error {
	var cpulist []int
	cpus := make([]int, runtime.NumCPU())
	for i := range cpus {
		cpus[i] = i
	}

	log.Info("[perf_collector] init cpu number", log.Int("num", len(cpus)))

	for _, eventgroup := range perfEventConf.CpuEvents[0].EventGroups {
		if !eventgroup.Enable {
			log.Info("[perf_collector] event group is disabled",
				log.String("name", eventgroup.Name))
			continue
		}

		if len(eventgroup.CpuIds) == 0 {
			cpulist = cpus
		} else {
			cpulist = eventgroup.CpuIds
		}

		for i, cpu := range cpulist {
			profiler := NewReadableProfiler(eventgroup.Name, cpu)
			for _, event := range eventgroup.Events {
				config := EventConfig{
					Key:       EventKey{Name: event.Name, CpuId: uint64(cpu)},
					Type:      event.Type,
					TypeStr:   event.TypeStr,
					Config:    event.Config,
					ConfigStr: event.ConfigStr,
					Config1:   event.Config1,
					Config2:   event.Config2,
				}
				log.Info("[perf_collector] add event ", log.String("config", fmt.Sprintf("%+v", config)))
				if err := profiler.AddEvent(config); err != nil {
					if i == 0 {
						log.Warn("[perf_collector] init event failed",
							log.String("event", fmt.Sprintf("%+v", event)),
							log.String("error", err.Error()))
					}
					continue
				}
			}

			if !profiler.HasEvent() {
				if i == 0 {
					log.Warn("[perf_collector] init the whole event group failed",
						log.String("eventgroup", eventgroup.Name))
				}
				continue
			}

			err := profiler.Start()
			if err != nil {
				if i == 0 {
					log.Warn("[perf_collector] start profiler failed",
						log.String("err", err.Error()))
				}
				continue
			}

			c.profilers = append(c.profilers, profiler)
		}
	}

	return nil
}

// Init perf
func (c *PerfCollector) Init(m map[string]interface{}) error {
	perfEventConfFile := m["perf_events_config_file"].(string)

	perfEventConf, err := c.loadPerfEventConfFile(perfEventConfFile)
	if err != nil {
		log.Error("[perf_collector] load perf event conf failed",
			log.String("error", err.Error()))
		return err
	}

	err = c.initPerfEventConf(perfEventConf)
	if err != nil {
		log.Error("[perf_collector] init perf event conf failed",
			log.String("error", err.Error()))
		return err
	}

	log.Info("[perf_collector] init done",
		log.Int("profiler num", len(c.profilers)))

	return nil
}

// Collect collect metric
func (c *PerfCollector) Collect(out map[string]interface{}) error {
	c.timestamp = time.Now().Unix()

	cpuIdResultMap := make(map[int]map[string]interface{})
	for _, profiler := range c.profilers {
		// get value
		var results ReadableResultSet
		if err := profiler.Read(&results); err != nil {
			log.Warn("[perf_collector] profile error", log.String("err", err.Error()))
			continue
		}

		enabled := c.calDelta(strconv.Itoa(int(profiler.CpuId)), profiler.Name+"_enabled", results.TimeEnabled)
		running := c.calDelta(strconv.Itoa(int(profiler.CpuId)), profiler.Name+"_running", results.TimeRunning)
		if running == 0 {
			log.Info("[perf_collector] running is zero", log.String("name", profiler.Name), log.Int("cpu", profiler.CpuId),
				log.String("results", fmt.Sprintf("%+v", results)))
			continue
		}

		if enabled < running {
			log.Info("[perf_collector] multiplex enabled",
				log.String("group", profiler.Name),
				log.Uint64("time_enabled", enabled),
				log.Uint64("time_running", running))
		}

		// deal with value
		for k, v := range results.ResultMap {
			if _, ok := cpuIdResultMap[int(k.CpuId)]; !ok {
				cpuIdResultMap[int(k.CpuId)] = make(map[string]interface{})
			}

			c.calDeltaData(
				cpuIdResultMap[int(k.CpuId)],
				strconv.Itoa(int(k.CpuId)),
				k.Name, 0, 0,
				v)
		}
	}

	c.buildOutDataDict(out, cpuIdResultMap)

	log.Debug("[perf_collector] out data dict", log.String("out", fmt.Sprintf("%+v", out)))

	return nil
}

func (c *PerfCollector) buildOutDataDict(out map[string]interface{},
	cpuIdResultMap map[int]map[string]interface{}) {
	out["send_to_multibackend"] = make(map[string]interface{})
	sout := out["send_to_multibackend"].(map[string]interface{})
	perfoutlist := make([]map[string]interface{}, 0)

	for cpuid, result := range cpuIdResultMap {
		statm := make(map[string]interface{})
		statm["dim_cpuid"] = strconv.Itoa(cpuid)
		for k, v := range result {
			statm[k] = v
		}

		perfoutlist = append(perfoutlist, statm)
	}

	sout["perf_event_counters"] = perfoutlist
}

func (c *PerfCollector) Stop() error {
	for _, prof := range c.profilers {
		if err := prof.Stop(); err != nil {
			log.Warn("[perf_collector] stop profiler stat failed",
				log.String("error", err.Error()))
		}

		if err := prof.Close(); err != nil {
			log.Warn("[perf_collector] close profiler fd failed",
				log.String("error", err.Error()))
		}
	}

	return nil
}

func (c *PerfCollector) calDelta(ns, k string, value uint64) uint64 {
	ret := uint64(0)
	key := ns + "___" + k
	if oldValue, ok := c.preValueMap[key]; ok {
		if value > oldValue.Value {
			ret = value - oldValue.Value
		} else if value < oldValue.Value {
			ret = oldValue.Value - value
			log.Info("[perf_collector] negative!!! old value and new value", log.String("key", key), log.Uint64("oldvalue", uint64(oldValue.Value)), log.Uint64("newvalue", value))
		}
	}
	c.preValueMap[key] = PreValue{LastTimestamp: c.timestamp, Value: value}

	return ret
}

func (c *PerfCollector) calDeltaData(out map[string]interface{}, ns, k string,
	timeEnabled, timeRunning uint64, value uint64) {
	key := ns + "___" + k
	if oldValue, ok := c.preValueMap[key]; ok {
		if value >= oldValue.Value && c.timestamp-oldValue.LastTimestamp > 0 {
			ratio := float64(1.0)
			// if timeEnabled < timeRunning && timeRunning != 0 {
			// 	ratio = float64(timeEnabled) / float64(timeRunning)
			// }
			out[k] = float64(value-oldValue.Value) / (float64(c.timestamp-oldValue.LastTimestamp) * ratio)
		}
	}

	c.preValueMap[key] = PreValue{LastTimestamp: c.timestamp, Value: value}
}

