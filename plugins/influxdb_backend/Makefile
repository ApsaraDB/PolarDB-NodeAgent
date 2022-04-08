include ../../common.mk

PLUGIN_TARGET = ../../bin/plugins/influxdb_backend/influxdb_backend.so
PLUGIN_SRC=influxdb_backend.go

all: prepare $(PLUGIN_TARGET)

prepare:
	@mkdir -p ../../bin/plugins/influxdb_backend

$(PLUGIN_TARGET): $(PLUGIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_PLUGIN_FLAGS) -o $@ $<

clean:
	$(GOCC) clean -mod=vendor && rm -rf ../../bin/plugins/influxdb_backend
