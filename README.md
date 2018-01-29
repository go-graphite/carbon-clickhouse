# carbon-clickhouse
Graphite metrics receiver with ClickHouse as storage

## Production status
Last releases are stable and ready for production use

## Build
```sh
# build binary
git clone https://github.com/lomik/carbon-clickhouse.git
cd carbon-clickhouse
make submodules
make
```

## ClickHouse configuration

1. Add `graphite_rollup` section to config.xml. Sample [here](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Server/config.xml#L168). You can use [carbon-schema-to-clickhouse](https://github.com/bzed/carbon-schema-to-clickhouse) for generate rollup xml from graphite [storage-schemas.conf](http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf).

2. Create tables
```sql
CREATE TABLE graphite ( 
  Path String,  
  Value Float64,  
  Time UInt32,  
  Date Date,  
  Timestamp UInt32
) ENGINE = GraphiteMergeTree(Date, (Path, Time), 8192, 'graphite_rollup');
 
-- optional table for faster metric search
CREATE TABLE graphite_tree (
  Date Date,
  Level UInt32,
  Path String,
  Deleted UInt8,
  Version UInt32
) ENGINE = ReplacingMergeTree(Date, (Level, Path), 8192, Version);
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

[upload.graphite]
type = "points"
table = "graphite"
threads = 1
url = "http://localhost:8123/"
timeout = "1m0s"

[upload.graphite_tree]
type = "tree"
table = "graphite_tree"
date = "2016-11-01"
threads = 1
url = "http://localhost:8123/"
timeout = "1m0s"
cache-ttl = "12h0m0s"

# # Extra table with daily series list
#
# # CREATE TABLE graphite_series (
# #   Date Date,
# #   Level UInt32,
# #   Path String,
# #   Deleted UInt8,
# #   Version UInt32
# # ) ENGINE = ReplacingMergeTree(Date, (Level, Path, Date), 8192, Version);
# [upload.graphite_series]
# type = "series"
# table = "graphite_series"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "1m0s"
# cache-ttl = "12h0m0s"

# You can define additional upload destinations of any supported type
# [upload.any_unique_name]
# type = "points"
# table = "graphite3600"
# threads = 1
# url = "http://localhost:8123/"
# timeout = "1m0s"

[udp]
listen = ":2003"
enabled = true

[tcp]
listen = ":2003"
enabled = true

[pickle]
listen = ":2004"
enabled = true

# https://github.com/lomik/carbon-clickhouse/blob/master/grpc/carbon.proto
[grpc]
listen = ":2005"
enabled = false

[pprof]
listen = "localhost:7007"
enabled = false
```
