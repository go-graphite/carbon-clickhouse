[![deb](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)
[![rpm](https://img.shields.io/badge/rpm-packagecloud.io-844fec.svg)](https://packagecloud.io/go-graphite/stable)

# carbon-clickhouse
Graphite metrics receiver with ClickHouse as storage

## Production status
Last releases are stable and ready for production use

## TL;DR
[Preconfigured docker-compose](https://github.com/lomik/graphite-clickhouse-tldr)

### Docker
Docker images are available on [packages](https://github.com/lomik/carbon-clickhouse/pkgs/container/carbon-clickhouse) page.

## Build

Required golang 1.18+

```sh
# build binary
git clone https://github.com/lomik/carbon-clickhouse.git
cd carbon-clickhouse
make
```

## ClickHouse configuration

1. Add `graphite_rollup` section to config.xml. Sample [here](https://github.com/lomik/graphite-clickhouse-tldr/blob/master/rollup.xml). You can use [carbon-schema-to-clickhouse](https://github.com/bzed/carbon-schema-to-clickhouse) for generate rollup xml from graphite [storage-schemas.conf](http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf).

2. Create tables.

```sql
CREATE TABLE graphite (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMM(Date)
ORDER BY (Path, Time);

-- optional table for faster metric search
CREATE TABLE graphite_index (
  Date Date,
  Level UInt32,
  Path String,
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(Date)
ORDER BY (Level, Path, Date);

-- optional table for storing Graphite tags
CREATE TABLE graphite_tagged (
  Date Date,
  Tag1 String,
  Path String,
  Tags Array(String),
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(Date)
ORDER BY (Tag1, Path, Date);
```

[GraphiteMergeTree documentation](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/graphitemergetree/)

You can create Replicated tables. See [ClickHouse documentation](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replication/)

## Configuration
```
$ carbon-clickhouse -help
Usage of carbon-clickhouse:
  -check-config=false: Check config and exit
  -config="": Filename of config
  -config-print-default=false: Print default config
  -version=false: Print version
```

Date are broken by default (not always in UTC), but this used from start of project, and can produce some bugs.  
Change to UTC requires points/index/tags tables rebuild (Date recalc to true UTC) or queries with wide Date range.  
Set `data.utc-date = true` for this.  
Without UTC date is required to run carbon-clickhouse and graphite-clickhouse in one timezone.

```toml
[common]
# Prefix for store all internal carbon-clickhouse graphs. Supported macroses: {host}
metric-prefix = "carbon.agents.{host}"
# Endpoint for store internal carbon metrics. Valid values: "" or "local", "tcp://host:port", "udp://host:port"
metric-endpoint = "local"
# Interval of storing internal metrics. Like CARBON_METRIC_INTERVAL
metric-interval = "1m0s"
# GOMAXPROCS
max-cpu = 1

[logging]
# "stderr", "stdout" can be used as file name
file = "/var/log/carbon-clickhouse/carbon-clickhouse.log"
# Logging error level. Valid values: "debug", "info", "warn" "error"
level = "info"

[data]
# Folder for buffering received data
path = "/data/carbon-clickhouse/"
# Rotate (and upload) file iniciated on size and interval
# Rotate (and upload) file size (in bytes, also k, m and g units can be used)
# chunk-max-size = '512m'
chunk-max-size = 0
# Rotate (and upload) file interval
# Minimize chunk-interval for minimize lag between point receive and store
chunk-interval = "1s"
# Auto-increase chunk interval if the number of unprocessed files is grown
# Sample, set chunk interval to 10 if unhandled files count >= 5 and set to 60s if unhandled files count >= 20:
# chunk-auto-interval = "5:10s,20:60s"
chunk-auto-interval = ""

# Compression algorithm to use when storing temporary files.
# Might be useful to reduce space usage when Clickhouse is unavailable for an extended period of time.
# Currently supported: none, lz4
compression = "none"

# Compression level to use.
# For "lz4" 0 means use normal LZ4, >=1 use LZ4HC with this depth (the higher - the better compression, but slower)
compression-level = 0

# Date are broken by default (not always in UTC)
#utc-date = false

[upload.graphite]
type = "points"
table = "graphite"
threads = 1
url = "http://localhost:8123/"
# compress-data enables gzip compression while sending to clickhouse
compress-data = true
timeout = "1m0s"
# save zero value to Timestamp column (for point and posts-reverse tables)
zero-timestamp = false

[upload.graphite_index]
type = "index"
table = "graphite_index"
threads = 1
url = "http://localhost:8123/"
timeout = "1m0s"
cache-ttl = "12h0m0s"
# Store hash of metric in memory instead of full metric name
# Allowed values: "", "city64" (empty value - disabled)
hash = ""
# If daily index should be disabled, default is `false`
disable-daily-index = false

# # You can define additional upload destinations of any supported type:
# # - points
# # - index
# # - tagged (is described below)
# # - points-reverse (same scheme as points, but path 'a1.b2.c3' stored as 'c3.b2.a1')

# # For uploaders with types "points" and "points-reverse" there is a possibility to ignore data using patterns. E.g.
# [upload.graphite]
# type = "graphite"
# table = "graphite.points"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "30s"
# ignored-patterns = [
#     "a1.b2.*.c3",
# ]

# # Extra table which can be used as index for tagged series
# # Also, there is an opportunity to avoid writing tags for some metrics.
# # Example below, ignored-tagged-metrics.
# [upload.graphite_tagged]
# type = "tagged"
# table = "graphite_tagged"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "1m0s"
# cache-ttl = "12h0m0s"
# ignored-tagged-metrics = [
#     "a.b.c.d",  # all tags (but __name__) will be ignored for metrics like a.b.c.d?tagName1=tagValue1&tagName2=tagValue2...
#     "*",  # all tags (but __name__) will be ignored for all metrics; this is the only special case with wildcards
# ]
#
# It is possible to connect to clickhouse with OpenSSL certificates (mTLS) like below:
# [upload.graphite]
# type = "points"
# table = "graphite"
# threads = 1
# compress-data = true
# zero-timestamp = false
# timeout = "1m0s"
# url = "https://localhost:8443/" # url is https
# [upload.graphite.tls]
# ca-cert = [ "<path/to/rootCA.crt>", "<path/to/other/rootCA.crt>" ]
# server-name = "<server-name>"
# insecure-skip-verify = false # if true, server certificates will not be validated
# [[upload.graphite.tls.certificates]]
# key = "<path/to/client.key>"
# cert = "<path/to/client.crt>"

[udp]
listen = ":2003"
enabled = true
# drop received point if timestamp > now + value. 0 - don't drop anything
drop-future = "0s"
# drop received point if timestamp < now - value. 0 - don't drop anything
drop-past = "0s"
# drop metrics with names longer than this value. 0 - don't drop anything
drop-longer-than = 0

[tcp]
listen = ":2003"
enabled = true
drop-future = "0s"
drop-past = "0s"
drop-longer-than = 0

[pickle]
listen = ":2004"
enabled = true
drop-future = "0s"
drop-past = "0s"
drop-longer-than = 0

# https://github.com/lomik/carbon-clickhouse/blob/master/grpc/carbon.proto
[grpc]
listen = ":2005"
enabled = false
drop-future = "0s"
drop-past = "0s"
drop-longer-than = 0

[prometheus]
listen = ":2006"
enabled = false
drop-future = "0s"
drop-past = "0s"
drop-longer-than = 0

[telegraf_http_json]
listen = ":2007"
enabled = false
drop-future = "0s"
drop-past = "0s"
drop-longer-than = 0
# the character to join telegraf metric and field (default is "_" for historical reason and Prometheus compatibility)
concat = "."

# Golang pprof + some extra locations
#
# Last 1000 points dropped by "drop-future", "drop-past" and "drop-longer-than" rules:
# /debug/receive/tcp/dropped/
# /debug/receive/udp/dropped/
# /debug/receive/pickle/dropped/
# /debug/receive/grpc/dropped/
# /debug/receive/prometheus/dropped/
# /debug/receive/telegraf_http_json/dropped/
[pprof]
listen = "localhost:7007"
enabled = false

# You can use tag matching like in InfluxDB. Format is exactly the same.
# It will parse all metrics that don't have tags yet.
# For more information see https://docs.influxdata.com/influxdb/v1.7/supported_protocols/graphite/
# Example:
# [convert_to_tagged]
# enabled = true 
# separator = "_"
# tags = ["region=us-east", "zone=1c"]
# templates = [
#     "generated.* .measurement.cpu  metric=idle",
#     "* host.measurement* template_match=none",
# ] 
```
