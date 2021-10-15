include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/pluto/pluto.so
PLUGIN_SRC=$(shell find . -name "*.go")
#PLUGIN_MAIN=$(shell find . -name '*.go')
PLUGIN_SRC=$(shell find . -name "*.go")
#PLUGIN_MAIN=$(wildcard *.go)
PLUGIN_SRC=$(shell find . -name "*.go")
PLUGIN_MAIN=pluto.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/pluto

$(PLUGIN_TARGET): $(PLUGIN_MAIN) $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/pluto
