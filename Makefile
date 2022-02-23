include common.mk

DESTDIR ?= /opt/db-monitor
APP_ROOT := ${DESTDIR}/
APP_ROOT_ESCAPE = $(subst /,\/,${APP_ROOT})
APP_BIN := ${APP_ROOT}"/bin"
APP_LIB := ${APP_ROOT}"lib"
APP_CONF := ${APP_ROOT}"/conf"
APP_LOG := ${APP_ROOT}"/log"
SRC_CONF := "conf_envs/conf_opensource_host_env"
SRC_SCRIPTS := "scripts/"

TOPTARGETS := all clean

# build all plugins
$(TOPTARGETS):

ifneq ($(GOOS),windows)
PLUGIN_BUILD=$(MAKE) -f Makefile.plugin
else
PLUGIN_BUILD=
endif


PLUGINS = bin/*
TARGET = bin/universe
DECODER = bin/uetool
ALL_BIN = $(PLUGINS)

MAIN_SRC = cmd/universe/main.go
DECODER_SRC = cmd/uetool/msgdecoder.go

.PHONY: $(TARGET)

all: prepare $(TARGET) $(DECODER)

prepare:
	@mkdir -p bin/

$(TARGET): $(MAIN_SRC)
	$(GC_ENV) $(GOCC) $(GC_FLAGS) -o $@ $^
	$(PLUGIN_BUILD)

$(DECODER): $(DECODER_SRC)
	$(GC_ENV) $(GOCC) $(GC_FLAGS) -o $@ $^


clean:
	rm -rf $(ALL_BIN)
	if [ ! "$(PLUGIN_BUILD)" = "" ]; then \
		$(PLUGIN_BUILD) clean; \
	fi \

install: $(TARGET) $(DECODER)
	@echo "install into "${APP_ROOT}
	@sudo mkdir -p ${APP_BIN} ${APP_LIB} ${APP_CONF} ${APP_LOG}
	@sudo rm -rf ${APP_BIN}/* && sudo cp -rf bin/* ${APP_BIN}
	@sudo rm -rf ${APP_CONF}/* && sudo cp -rf ${SRC_CONF}/* ${APP_CONF}
	@sudo sed 's/{BASEPATH}/${APP_ROOT_ESCAPE}/g' ${SRC_SCRIPTS}/service.sh > ${SRC_SCRIPTS}/service_install.sh
	@sudo cp ${SRC_SCRIPTS}/service_install.sh ${APP_BIN}/service.sh

uninstall:
	sudo rm -fr ${APP_ROOT}	
	
