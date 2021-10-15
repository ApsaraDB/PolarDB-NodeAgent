include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/prometheus_backend/prometheus_backend.so
PLUGIN_SRC=$(shell find . -name "*.go")
PLUGIN_MAIN=prometheus_backend.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/prometheus_backend

$(PLUGIN_TARGET): $(PLUGIN_MAIN) $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/prometheus_backend
