package collector

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

var PerfReadFormatMap = map[string]uint64{
	"PERF_FORMAT_TOTAL_TIME_ENABLED": unix.PERF_FORMAT_TOTAL_TIME_ENABLED,
	"PERF_FORMAT_TOTAL_TIME_RUNNING": unix.PERF_FORMAT_TOTAL_TIME_RUNNING,
	"PERF_FORMAT_ID":                 unix.PERF_FORMAT_ID,
	"PERF_FORMAT_GROUP":              unix.PERF_FORMAT_GROUP,
}

var PerfBitMap = map[string]uint64{
	"PerfBitDisabled":               unix.PerfBitDisabled,
	"PerfBitInherit":                unix.PerfBitInherit,
	"PerfBitPinned":                 unix.PerfBitPinned,
	"PerfBitExclusive":              unix.PerfBitExclusive,
	"PerfBitExcludeUser":            unix.PerfBitExcludeUser,
	"PerfBitExcludeKernel":          unix.PerfBitExcludeKernel,
	"PerfBitExcludeHv":              unix.PerfBitExcludeHv,
	"PerfBitExcludeIdle":            unix.PerfBitExcludeIdle,
	"PerfBitMmap":                   unix.PerfBitMmap,
	"PerfBitComm":                   unix.PerfBitComm,
	"PerfBitFreq":                   unix.PerfBitFreq,
	"PerfBitInheritStat":            unix.PerfBitInheritStat,
	"PerfBitEnableOnExec":           unix.PerfBitEnableOnExec,
	"PerfBitTask":                   unix.PerfBitTask,
	"PerfBitWatermark":              unix.PerfBitWatermark,
	"PerfBitPreciseIPBit1":          unix.PerfBitPreciseIPBit1,
	"PerfBitPreciseIPBit2":          unix.PerfBitPreciseIPBit2,
	"PerfBitMmapData":               unix.PerfBitMmapData,
	"PerfBitSampleIDAll":            unix.PerfBitSampleIDAll,
	"PerfBitExcludeHost":            unix.PerfBitExcludeHost,
	"PerfBitExcludeGuest":           unix.PerfBitExcludeGuest,
	"PerfBitExcludeCallchainKernel": unix.PerfBitExcludeCallchainKernel,
	"PerfBitExcludeCallchainUser":   unix.PerfBitExcludeCallchainUser,
	"PerfBitMmap2":                  unix.PerfBitMmap2,
	"PerfBitCommExec":               unix.PerfBitCommExec,
	"PerfBitUseClockID":             unix.PerfBitUseClockID,
	"PerfBitContextSwitch":          unix.PerfBitContextSwitch,
}

var PerfFlagMap = map[string]uint64{
	"PERF_FLAG_FD_CLOEXEC":  unix.PERF_FLAG_FD_CLOEXEC,
	"PERF_FLAG_FD_NO_GROUP": unix.PERF_FLAG_FD_NO_GROUP,
	"PERF_FLAG_FD_OUTPUT":   unix.PERF_FLAG_FD_OUTPUT,
	"PERF_FLAG_PID_CGROUP":  unix.PERF_FLAG_PID_CGROUP,
}

var PerfTypeMap = map[string]uint64{
	"PERF_TYPE_HARDWARE":   unix.PERF_TYPE_HARDWARE,
	"PERF_TYPE_SOFTWARE":   unix.PERF_TYPE_SOFTWARE,
	"PERF_TYPE_TRACEPOINT": unix.PERF_TYPE_TRACEPOINT,
	"PERF_TYPE_HW_CACHE":   unix.PERF_TYPE_HW_CACHE,
	"PERF_TYPE_RAW":        unix.PERF_TYPE_RAW,
	"PERF_TYPE_BREAKPOINT": unix.PERF_TYPE_BREAKPOINT,
}

var PerfConfigMap = map[string]uint64{
	"PERF_COUNT_HW_CPU_CYCLES":              unix.PERF_COUNT_HW_CPU_CYCLES,
	"PERF_COUNT_HW_INSTRUCTIONS":            unix.PERF_COUNT_HW_INSTRUCTIONS,
	"PERF_COUNT_HW_CACHE_REFERENCES":        unix.PERF_COUNT_HW_CACHE_REFERENCES,
	"PERF_COUNT_HW_CACHE_MISSES":            unix.PERF_COUNT_HW_CACHE_MISSES,
	"PERF_COUNT_HW_BRANCH_INSTRUCTIONS":     unix.PERF_COUNT_HW_BRANCH_INSTRUCTIONS,
	"PERF_COUNT_HW_BRANCH_MISSES":           unix.PERF_COUNT_HW_BRANCH_MISSES,
	"PERF_COUNT_HW_BUS_CYCLES":              unix.PERF_COUNT_HW_BUS_CYCLES,
	"PERF_COUNT_HW_STALLED_CYCLES_FRONTEND": unix.PERF_COUNT_HW_STALLED_CYCLES_FRONTEND,
	"PERF_COUNT_HW_STALLED_CYCLES_BACKEND":  unix.PERF_COUNT_HW_STALLED_CYCLES_BACKEND,
	"PERF_COUNT_HW_REF_CPU_CYCLES":          unix.PERF_COUNT_HW_REF_CPU_CYCLES,
	"PERF_COUNT_HW_MAX":                     unix.PERF_COUNT_HW_MAX,
	"PERF_COUNT_HW_CACHE_L1D":               unix.PERF_COUNT_HW_CACHE_L1D,
	"PERF_COUNT_HW_CACHE_L1I":               unix.PERF_COUNT_HW_CACHE_L1I,
	"PERF_COUNT_HW_CACHE_LL":                unix.PERF_COUNT_HW_CACHE_LL,
	"PERF_COUNT_HW_CACHE_DTLB":              unix.PERF_COUNT_HW_CACHE_DTLB,
	"PERF_COUNT_HW_CACHE_ITLB":              unix.PERF_COUNT_HW_CACHE_ITLB,
	"PERF_COUNT_HW_CACHE_BPU":               unix.PERF_COUNT_HW_CACHE_BPU,
	"PERF_COUNT_HW_CACHE_NODE":              unix.PERF_COUNT_HW_CACHE_NODE,
	"PERF_COUNT_HW_CACHE_MAX":               unix.PERF_COUNT_HW_CACHE_MAX,
	"PERF_COUNT_HW_CACHE_OP_READ":           unix.PERF_COUNT_HW_CACHE_OP_READ,
	"PERF_COUNT_HW_CACHE_OP_WRITE":          unix.PERF_COUNT_HW_CACHE_OP_WRITE,
	"PERF_COUNT_HW_CACHE_OP_PREFETCH":       unix.PERF_COUNT_HW_CACHE_OP_PREFETCH,
	"PERF_COUNT_HW_CACHE_OP_MAX":            unix.PERF_COUNT_HW_CACHE_OP_MAX,
	"PERF_COUNT_HW_CACHE_RESULT_ACCESS":     unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS,
	"PERF_COUNT_HW_CACHE_RESULT_MISS":       unix.PERF_COUNT_HW_CACHE_RESULT_MISS,
	"PERF_COUNT_HW_CACHE_RESULT_MAX":        unix.PERF_COUNT_HW_CACHE_RESULT_MAX,
	"PERF_COUNT_SW_CPU_CLOCK":               unix.PERF_COUNT_SW_CPU_CLOCK,
	"PERF_COUNT_SW_TASK_CLOCK":              unix.PERF_COUNT_SW_TASK_CLOCK,
	"PERF_COUNT_SW_PAGE_FAULTS":             unix.PERF_COUNT_SW_PAGE_FAULTS,
	"PERF_COUNT_SW_CONTEXT_SWITCHES":        unix.PERF_COUNT_SW_CONTEXT_SWITCHES,
	"PERF_COUNT_SW_CPU_MIGRATIONS":          unix.PERF_COUNT_SW_CPU_MIGRATIONS,
	"PERF_COUNT_SW_PAGE_FAULTS_MIN":         unix.PERF_COUNT_SW_PAGE_FAULTS_MIN,
	"PERF_COUNT_SW_PAGE_FAULTS_MAJ":         unix.PERF_COUNT_SW_PAGE_FAULTS_MAJ,
	"PERF_COUNT_SW_ALIGNMENT_FAULTS":        unix.PERF_COUNT_SW_ALIGNMENT_FAULTS,
	"PERF_COUNT_SW_EMULATION_FAULTS":        unix.PERF_COUNT_SW_EMULATION_FAULTS,
	"PERF_COUNT_SW_DUMMY":                   unix.PERF_COUNT_SW_DUMMY,
	"PERF_COUNT_SW_BPF_OUTPUT":              unix.PERF_COUNT_SW_BPF_OUTPUT,
	"PERF_COUNT_SW_MAX":                     unix.PERF_COUNT_SW_MAX,
}

func ResolvePerfReadFormat(formatstr string) (uint64, error) {
	result, err := resolve(formatstr, PerfReadFormatMap)
	if err != nil {
		return 0, err
	}

	return bitsor(result), nil
}

func ResolvePerfType(typestr string) (uint64, error) {
	result, err := resolve(typestr, PerfTypeMap)
	if err != nil {
		return 0, err
	}

	return bitsor(result), nil

}

func ResolvePerfFlag(flagstr string) (uint64, error) {
	result, err := resolve(flagstr, PerfFlagMap)
	if err != nil {
		return 0, err
	}

	return bitsor(result), nil
}

func ResolvePerfBits(bitstr string) (uint64, error) {
	result, err := resolve(bitstr, PerfBitMap)
	if err != nil {
		return 0, err
	}

	return bitsor(result), nil
}

func ResolvePerfConfig(configstr string) (uint64, error) {
	result, err := resolve(configstr, PerfConfigMap)
	if err != nil {
		return 0, err
	}

	if len(result) == 1 {
		return result[0], nil
	}

	if len(result) != 3 {
		return 0, fmt.Errorf("config result is either 1 or 3")
	}

	return result[0] | (result[1] << 8) | (result[2] << 16), nil
}

func bitsor(codes []uint64) uint64 {
	result := uint64(0)
	for _, code := range codes {
		result |= code
	}
	return result
}

func resolve(description string, mapping map[string]uint64) ([]uint64, error) {
	descriptions := strings.Split(description, "|")
	result := make([]uint64, 0)
	for _, desc := range descriptions {
		cleanDesc := strings.TrimSpace(desc)
		if cleanDesc == "" {
			continue
		}

		if x, ok := mapping[cleanDesc]; ok {
			result = append(result, x)
		} else {
			return nil, fmt.Errorf("type code cannot be found: %s", cleanDesc)
		}
	}

	return result, nil
}
