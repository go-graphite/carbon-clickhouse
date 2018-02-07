#!/bin/sh

cd `dirname $0`
ROOT=`pwd`

PACKAGE="github.com/lomik/carbon-clickhouse"

cd _vendor/src/${PACKAGE}/
GOPATH=$ROOT/_vendor dep $@
