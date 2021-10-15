include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/db_backend/db_backend.so
PLUGIN_SRC=$(shell find . -name "*.go")
PLUGIN_MAIN=db_backend.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/db_backend

$(PLUGIN_TARGET): $(PLUGIN_MAIN) $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/db_backend
