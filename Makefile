NAME:=carbon-clickhouse
DESCRIPTION:="Graphite metrics receiver with ClickHouse as storage"
MODULE:=github.com/lomik/carbon-clickhouse

GO ?= go
export GOFLAGS +=  -mod=vendor
export GO111MODULE := on
TEMPDIR:=$(shell mktemp -d)

DEVEL ?= 0
ifeq ($(DEVEL), 0)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')
else
VERSION:=$(shell sh -c 'git describe --always --tags | sed -e "s/^v//i"')
endif

SRCS:=$(shell find . -name '*.go')

all: $(NAME)

.PHONY: clean
clean:
	rm -f $(NAME)
	rm -f e2e-test
	rm -rf out
	rm -f *deb *rpm
	rm -f sha256sum md5sum

$(NAME): $(SRCS)
	$(GO) build $(MODULE)

e2e-test: $(NAME)
	$(GO) build $(MODULE)/cmd/e2e-test

test:
	$(GO) test -race ./...

gox-build: out/$(NAME)-linux-amd64 out/$(NAME)-linux-arm64 out/root/etc/$(NAME)/$(NAME).conf

ARCH = amd64 arm64
out/$(NAME)-linux-%: out $(SRCS)
	GOOS=linux GOARCH=$* $(GO) build -o $@ $(MODULE)

out:
	mkdir -p out


out/root/etc/$(NAME)/$(NAME).conf: $(NAME)
	mkdir -p "$(shell dirname $@)"
	./$(NAME) -config-print-default > $@

nfpm-deb: gox-build
	$(MAKE) nfpm-build-deb ARCH=amd64
	$(MAKE) nfpm-build-deb ARCH=arm64
nfpm-rpm: gox-build
	$(MAKE) nfpm-build-rpm ARCH=amd64
	$(MAKE) nfpm-build-rpm ARCH=arm64

nfpm-build-%: nfpm.yaml
	NAME=$(NAME) DESCRIPTION=$(DESCRIPTION) ARCH=$(ARCH) VERSION_STRING=$(VERSION) nfpm package --packager $*

.ONESHELL:
RPM_VERSION:=$(subst -,_,$(VERSION))
packagecloud-push-rpm: $(wildcard $(NAME)-$(RPM_VERSION)-1.*.rpm)
	for pkg in $^; do
		package_cloud push $(REPO)/el/7 $${pkg} || true
		package_cloud push $(REPO)/el/8 $${pkg} || true
		package_cloud push $(REPO)/el/9 $${pkg} || true
	done

.ONESHELL:
packagecloud-push-deb: $(wildcard $(NAME)_$(VERSION)_*.deb)
	for pkg in $^; do
		package_cloud push $(REPO)/ubuntu/xenial   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/bionic   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/focal    $${pkg} || true
		package_cloud push $(REPO)/debian/stretch  $${pkg} || true
		package_cloud push $(REPO)/debian/buster   $${pkg} || true
		package_cloud push $(REPO)/debian/bullseye $${pkg} || true
	done

packagecloud-push:
	@$(MAKE) packagecloud-push-rpm
	@$(MAKE) packagecloud-push-deb

packagecloud-autobuilds:
	$(MAKE) packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	$(MAKE) packagecloud-push REPO=go-graphite/stable

sum-files: | sha256sum md5sum

md5sum:
	md5sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > md5sum

sha256sum:
	sha256sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > sha256sum
