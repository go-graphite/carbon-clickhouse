NAME:=carbon-clickhouse
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Graphite metrics receiver with ClickHouse as storage"

GO ?= go
export GOPATH := $(CURDIR)/_vendor
TEMPDIR:=$(shell mktemp -d)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')

all: build

build:
	$(GO) build github.com/lomik/$(NAME)

gox-build:
	rm -rf out
	mkdir -p out
	gox -os="linux" -arch="amd64" -arch="386" -output="out/$(NAME)-{{.OS}}-{{.Arch}}"  github.com/lomik/$(NAME)
	ls -la out/
	mkdir -p out/root/etc/$(NAME)/
	./out/$(NAME)-linux-amd64 -config-print-default > out/root/etc/$(NAME)/$(NAME).conf

fpm-deb:
	make fpm-build-deb ARCH=amd64
	make fpm-build-deb ARCH=386
fpm-rpm:
	make fpm-build-rpm ARCH=amd64
	make fpm-build-rpm ARCH=386

fpm-build-deb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--force \
		--deb-compression bzip2 \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--config-files /etc/$(NAME)/$(NAME).conf \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/systemd/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		out/root/=/


fpm-build-rpm:
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) \
		--force \
		--rpm-compression bzip2 --rpm-os linux \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--config-files /etc/$(NAME)/$(NAME).conf \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/systemd/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		out/root/=/

packagecloud-push:
	package_cloud push $(REPO)/el/7 $(NAME)-$(VERSION)-1.x86_64.rpm || true
	package_cloud push $(REPO)/ubuntu/xenial $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/bionic $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/disco $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/eoan $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/buster $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/stretch $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/jessie $(NAME)_$(VERSION)_amd64.deb || true

packagecloud-autobuilds:
	make packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	make packagecloud-push REPO=go-graphite/stable

