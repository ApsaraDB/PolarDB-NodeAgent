GOCC=go
DEBUG=0
GOOS=linux

ARCH=$(shell uname -m)
ifeq ($(ARCH),aarch64)
GC_ENV=GOOS=$(GOOS) GOARCH=arm64
else
GC_ENV=GOOS=$(GOOS) GOARCH=amd64
endif


ifeq ($(DEBUG),1)
	GC_COMMON_FLAGS= -gcflags=all="-N -l"
	GC_LDFLAGS= -ldflags=-compressdwarf=false
	GC_FLAGS=build -mod=vendor $(GC_LDFLAGS) $(GC_COMMON_FLAGS)
	GC_PLUGIN_FLAGS=build -mod=vendor -buildmode=plugin $(GC_LDFLAGS) $(GC_COMMON_FLAGS)
else
	GC_COMMON_FLAGS=
	GC_LDFLAGS= -ldflags -w
	GC_FLAGS=build -mod=vendor $(GC_LDFLAGS) $(GC_COMMON_FLAGS)
	GC_PLUGIN_FLAGS=build -mod=vendor -buildmode=plugin $(GC_LDFLAGS) $(GC_COMMON_FLAGS)
endif
