include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/perfdata_processor/perfdata_processor.so
PLUGIN_SRC=$(shell find . -name "*.go")
PLUGIN_MAIN=perfdata_processor.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/perfdata_processor

$(PLUGIN_TARGET): $(PLUGIN_MAIN) $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/perfdata_processor
