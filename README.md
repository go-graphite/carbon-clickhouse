# carbon-clickhouse
Graphite metrics receiver with ClickHouse as storage

## Production status
Last releases are stable and ready for production use

## TL;DR
[Preconfigured docker-compose](https://github.com/lomik/graphite-clickhouse-tldr)

## Build
```sh
# build binary
git clone https://github.com/lomik/carbon-clickhouse.git
cd carbon-clickhouse
make
```

## ClickHouse configuration

1. Add `graphite_rollup` section to config.xml. Sample [here](https://clickhouse.yandex/docs/en/operations/table_engines/graphitemergetree/). You can use [carbon-schema-to-clickhouse](https://github.com/bzed/carbon-schema-to-clickhouse) for generate rollup xml from graphite [storage-schemas.conf](http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf).

2. Create tables.
Be aware that in these examples the tables are partitioned by day.
If you want old Clickhouse behaviour with monthly partitions then change toYYYYMMDD to toYYYYMM everywhere.

```sql
CREATE TABLE graphite ( 
  Path String,  
  Value Float64,  
  Time UInt32,  
  Date Date,  
  Version UInt32
) ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMMDD(Date)
ORDER BY (Path, Time);

-- optional table for faster metric search
CREATE TABLE graphite_tree (
  Level UInt32,
  Path String,
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY (Level)
ORDER BY (Level, Path);

-- optional table for daily series (see config file)
CREATE TABLE graphite_series (
  Date Date,
  Level UInt32,
  Path String,
  Version UInt32
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMMDD(Date)
ORDER BY (Level, Path, Date);

-- optional table for storing Graphite tags
CREATE TABLE graphite_tagged (
  Date Date,
  Tag1 String,
  Path String,
  Tags Array(String),
  Version UInt32,
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMMDD(Date)
ORDER BY (Tag1, Path, Date);
```

[GraphiteMergeTree documentation](https://clickhouse.yandex/docs/en/table_engines/graphitemergetree.html)

You can create Replicated tables. See [ClickHouse documentation](https://clickhouse.yandex/docs/en/table_engines/replication.html)

## Configuration
```
$ carbon-clickhouse -help
Usage of carbon-clickhouse:
  -check-config=false: Check config and exit
  -config="": Filename of config
  -config-print-default=false: Print default config
  -version=false: Print version
```

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
# Rotate (and upload) file interval.
# Minimize chunk-interval for minimize lag between point receive and store
chunk-interval = "1s"
# Auto-increase chunk interval if the number of unprocessed files is grown
# Sample, set chunk interval to 10 if unhandled files count >= 5 and set to 60s if unhandled files count >= 20:
# chunk-auto-interval = "5:10s,20:60s"
chunk-auto-interval = ""

[upload.graphite]
type = "points"
table = "graphite"
threads = 1
url = "http://localhost:8123/"
timeout = "1m0s"
# save zero value to Timestamp column (for point and posts-reverse tables)
zero-timestamp = false 

[upload.graphite_tree]
type = "tree"
table = "graphite_tree"
date = "2016-11-01"
threads = 1
url = "http://localhost:8123/"
timeout = "1m0s"
cache-ttl = "12h0m0s"

# # You can define additional upload destinations of any supported type:
# # - points
# # - tree
# # - series (is described below)
# # - tagged (is described below)
# # - points-reverse (same scheme as points, but path 'a1.b2.c3' stored as 'c3.b2.a1')
# # - series-reverse (same scheme as series, but path 'a1.b2.c3' stored as 'c3.b2.a1')

# # Extra table with daily series list
# [upload.graphite_series]
# type = "series"
# table = "graphite_series"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "1m0s"
# cache-ttl = "12h0m0s"

# # Extra table which can be used as index for tagged series
# [upload.graphite_tagged]
# type = "tagged"
# table = "graphite_tagged"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "1m0s"
# cache-ttl = "12h0m0s"

[udp]
listen = ":2003"
enabled = true
# drop received point if timestamp > now + value. 0 - don't drop anything
drop-future = "0s"
# drop received point if timestamp < now - value. 0 - don't drop anything
drop-past = "0s"

[tcp]
listen = ":2003"
enabled = true
drop-future = "0s"
drop-past = "0s"

[pickle]
listen = ":2004"
enabled = true
drop-future = "0s"
drop-past = "0s"

# https://github.com/lomik/carbon-clickhouse/blob/master/grpc/carbon.proto
[grpc]
listen = ":2005"
enabled = false
drop-future = "0s"
drop-past = "0s"

[prometheus]
listen = ":2006"
enabled = false
drop-future = "0s"
drop-past = "0s"

[telegraf_http_json]
listen = ":2007"
enabled = false
drop-future = "0s"
drop-past = "0s"

# Golang pprof + some extra locations
#
# Last 1000 points dropped by "drop-future" and "drop-past" rules:
# /debug/receive/tcp/dropped/
# /debug/receive/udp/dropped/
# /debug/receive/pickle/dropped/
# /debug/receive/grpc/dropped/
# /debug/receive/prometheus/dropped/
# /debug/receive/telegraf_http_json/dropped/
[pprof] 
listen = "localhost:7007"
enabled = false
```
