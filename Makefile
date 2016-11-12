all: carbon-clickhouse

GO ?= go
export GOPATH := $(CURDIR)/_vendor

submodules:
	git submodule init
	git submodule update --recursive

carbon-clickhouse:
	$(GO) build carbon-clickhouse.go
