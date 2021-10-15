include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/sar/sar.so
PLUGIN_SRC=$(shell find . -name "*.go")
#PLUGIN_MAIN=$(shell find . -name '*.go')
PLUGIN_SRC=$(shell find . -name "*.go")
PLUGIN_MAIN=sar.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/sar

$(PLUGIN_TARGET): $(PLUGIN_MAIN) $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/sar
