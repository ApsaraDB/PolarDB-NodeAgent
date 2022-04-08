include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/cgroup/cgroup.so
#PLUGIN_SRC=$(shell find . -name '*.go')
PLUGIN_SRC=cgroup.go collector/cgroup_collector.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/cgroup

$(PLUGIN_TARGET): $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/cgroup
