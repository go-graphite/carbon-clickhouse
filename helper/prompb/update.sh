#!/bin/sh

cd `dirname $0`

rm -rf prometheus
git clone https://github.com/prometheus/prometheus.git
cp prometheus/prompb/{types,remote}* .
rm -rf prometheus
