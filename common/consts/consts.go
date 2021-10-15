/*-------------------------------------------------------------------------
 *
 * consts.go
 *    Consts variables
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
 *           common/consts/consts.go
 *-------------------------------------------------------------------------
 */
package consts

const (
	// type define
	PluginTypeKey    = "type"
	PluginTypeGolang = "golang"
	PluginTypeLua    = "lua"

	PluginModeKey       = "mode"
	PluginModeCtrl      = "ctrl"
	PluginModeCollector = "collector"
	PluginModeBackend   = "backend"
	PluginModeExtern    = "extern"

	// runner define
	PluginRunnerKey    = "runner"
	PluginRunnerRds    = "rds"
	PluginRunnerDocker = "docker"

	// conf other key define
	PluginTargetKey   = "target"
	PluginDBTypeKey   = "db_type"
	PluginWorkDirKey  = "workdir"
	PluginPrefixKey   = "prefix"
	PluginIntervalKey = "interval"
	PluginEnableKey   = "enable"
	PluginExportsKey  = "exports"
	PluginExternKey   = "extern"
	PluginNameKey     = "name"
	PluginPathKey     = "path"
	PluginBackendKey  = "backend"

	// lua conf
	PluginLuaScriptPath = "luafile"

	// schema
	SchemaVersionKey        = "version"
	SchemaItemSeparator     = ';'
	SchemaItemListSeparator = ','

	// schema type
	SchemaTypeInt          = "int"
	SchemaTypeString       = "string"
	SchemaTypeFloat        = "float"
	SchemaTypeMapKey       = "mapkey"
	SchemaTypeList         = "list"
	SchemaTypeTopIO        = "sar_top_cpu"
	SchemaTypeTopCPU       = "sar_top_io"
	SchemaTypeTopMem       = "sar_top_mem"
	SchemaTypeDStatus      = "sar_dstatus"
	SchemeTypeMap          = "map"
	SchemaTypeIntDoubleMap = "map<int,double>"

	// extern conf
	ExternBusinessKey = "business"
	ExternTypeKey     = "type" // ==> extern.type

	ErrorFile = "errorFile"

	// Plugin Context Keys
	PluginContextKeyImportsMap = "ImportsMap"
	PluginContextKeyPort       = "port"
	PluginContextKeyInsName    = "ins_name"
	PluginContextKeyInsId      = "ins_id"
	PluginContextKeyInsPath    = "ins_path"
	PluginContextKeyEnv        = "env"
	PluginContextKeyDbEndpoint = "db_endpoint"
	PluginContextKeyPassword   = "password"
	PluginContextKeyDatabase   = "database"
	PluginContextKeyQueries    = "queries"
	PluginContextKeyUserName   = "username"
	PluginContextKeyPrivate    = "private"

	// message
	MessageSeparator = 0x0A

	// metrics name
	// cpu
	MetricNameSuffixDelta = "_delta"
	MetricNameCPUUser     = "cpu_user"
	MetricNameCPUSys      = "cpu_sys"
	MetricNameClkTck      = "clk_tck"

	// mem
	MetricNameMemSize     = "mem_size"
	MetricNameMemResident = "mem_resident"
	MetricNameMemShared   = "mem_shared"

	// io
	MetricNameDataIo      = "data_io"
	MetricNameLogIo       = "log_io"
	MetricNameDataIoBytes = "data_io_bytes"
	MetricNameLogIoBytes  = "log_io_bytes"

	// schema header
	SchemaHeaderVersion     = "version"
	SchemaHeaderDbType      = "db_type"
	SchemaHeaderBusiness    = "business"
	SchemaHeaderStatusCode  = "status_code"
	SchemaHeaderHostname    = "hostname"
	SchemaHeaderIp          = "ip"
	SchemaHeaderTime        = "time"
	SchemaHeaderInsName     = "ins_name"
	SchemaHeaderPort        = "port"
	SchemaHeaderInterval    = "interval"
	SchemaHeaderCollectTime = "collect_time"
	SchemaHeaderProductCode = "product_code"
	SchemaHeaderLogType     = "log_type"

	// agent status
	AgentStart   = 1
	AgentRunning = 2
	AgentStop    = 3

	// runner
	RunnerCollectorMaxFailCounts = 3

	// notify pipeline keys
	StatusPipeline = "status_pipeline"
	ErrorPipeline  = "error_pipeline"
	StatPipeline   = "stat_pipeline"

	// conf file
	DataJsonConf          = "conf/data.json"
	SingletonDataJsonConf = "conf/singleton_data.json"

	DBVersionMySQL56 = "5.6"
	DBVersionMySQL57 = "5.7"
	DBVersionMySQL80 = "8.0"

	SwitchOn  = "ON"
	SwitchOff = "OFF"

	PlutoPluginIdentifier                             = "golang-collector-pluto"
	PlutoNewInstanceIdentifier                        = PlutoPluginIdentifier + ".NewInstance"
	PlutoRemoveInstanceIdentifier                     = PlutoPluginIdentifier + ".RemoveInstance"
	PlutoDirSizeCollectorIdentifier                   = PlutoPluginIdentifier + ".GetDirSize"
	PlutoFilteredDirSizeCollectorIdentifier           = PlutoPluginIdentifier + ".GetFilteredDirSize"
	PlutoRegisterMySQLDiskDetailCollectorIdentifier   = PlutoPluginIdentifier + ".RegisterMySQLDiskDetailInstance"
	PlutoUnRegisterMySQLDiskDetailCollectorIdentifier = PlutoPluginIdentifier + ".UnRegisterMySQLDiskDetailInstance"
	PlutoMysqlDiskDetailSizeCollectorIdentifier       = PlutoPluginIdentifier + ".GetMySQLDiskDetailSize"

	SaturnPluginIdentifier                = "golang-collector-saturn"
	SaturnNewInstanceIdentifier           = SaturnPluginIdentifier + ".NewInstance"
	SaturnRemoveInstanceIdentifier        = SaturnPluginIdentifier + ".RemoveInstance"
	SaturnDeletedFilesCollectorIdentifier = SaturnPluginIdentifier + ".GetDeletedFilesSize"

	LocalSSD  = "local_ssd"
	CloudESSD = "cloud_essd"
	CloudSSD  = "cloud_ssd"

	ProcMemInfo       = "/proc/meminfo"
	ProcCpuStat       = "/proc/stat"
	ProcLoadAvg       = "/proc/loadavg"
	EtcMTab           = "/etc/mtab"
	ProcNetDev        = "/proc/net/dev"
	ProcMountInfo     = "/proc/self/mountinfo"
	ProcDiskStat      = "/proc/diskstats"
	ProcNetSockStat   = "/proc/net/sockstat"
	ProcNetStat       = "/proc/net/netstat"
	ProcNetSnmp       = "/proc/net/snmp"
	ProcVmStat        = "/proc/vmstat"
	ProcNvmeStatFile  = "/proc/nvmestats/stat"
	ProcNvmeQueueStat = "/proc/nvmestats/queue_stat"
	ProcfsJbd         = "/proc/fs/jbd2"

	CPUStatColNum    = 10
	DiskIostatColNum = 22
	DiskNum          = 512
	NetDevNum        = 64
	NetExtStatColNum = 128
	NetSnmpColNum    = 32
	NetDevColNum     = 24
	PartitionNum     = 32

	AligroupLabel = "/etc/optimize.d/autorun/aligroup"
	Aliyun        = "aliyun"
	Aligroup      = "aligroup"

	CGroupCpuPath     = "cgroup_cpu_path"
	CGroupMemPath     = "cgroup_mem_path"
	CGroupBlkioPath   = "cgroup_blkio_path"
	CGroupHugeMemPath = "cgroup_huge_mem_path"
	CGroupCpuAcctPath = "cgroup_cpuacct_path"

	ApsaraMetricInsID             = "apsara.metric.ins_id"
	ApsaraMetricClusterID         = "apsara.metric.cluster_id"
	ApsaraMetricLogicCustinsID    = "apsara.metric.logic_custins_id"
	ApsaraMetricPhysicalCustinsID = "apsara.metric.physical_custins_id"
	ApsaraMetricInsName           = "apsara.metric.ins_name"
	ApsaraInsPort                 = "apsara.ins.port"
	ApsaraMetricDbType            = "apsara.metric.db_type"
	ApsaraMetricDbVersion         = "apsara.metric.db_version"
	ApsaraMetricCategory          = "apsara.metric.category"
	ApsaraMetricStorageType       = "apsara.metric.storagetype"
	ApsaraMetricArch              = "apsara.metric.arch"
	ApsaraAccountSuper            = "apsara.account.super"
	ApsaraMetricRole              = "apsara.metric.role"
	ApsaraAccountPwd              = "apsara.account.pwd"
	ApsaraAccountSafe             = "apsara.account.safe"
	ApsaraMetricUEPort            = "apsara.metric.ue_port"
	ApsaraMetricReplicaName       = "apsara.metric.replica_name"

	AccountRoot       = "root"
	AccountAliyunRoot = "aliyun_root"

	PerformanceDir = "/home/performance"

	SafeDir = "/opt/pdb_ue/.safe"
)
