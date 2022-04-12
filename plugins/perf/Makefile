include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/perf/perf.so
#PLUGIN_SRC=$(shell find . -name '*.go')
PLUGIN_SRC=perf.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/perf

$(PLUGIN_TARGET): $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/perf
