# carbon-clickhouse
Graphite metrics receiver with ClickHouse as storage

## Production status
Beta users are welcome

## Build
```sh
# build binary
git clone https://github.com/lomik/carbon-clickhouse.git
cd carbon-clickhouse
make submodules
make
```

## ClickHouse configuration

1. Add `graphite_rollup` section to config.xml. Sample [here](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Server/config.xml#L168).

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
  Path String
) ENGINE = ReplacingMergeTree(Date, (Level, Path), 8192);
```

[GraphiteMergeTree documentation](https://github.com/yandex/ClickHouse/blob/master/dbms/include/DB/DataStreams/GraphiteRollupSortedBlockInputStream.h)

You can create Replicated tables. See [ClickHouse documentation](https://clickhouse.yandex/reference_en.html#Data replication)

## Configuration
```
$ carbon-clickhouse -help
Usage of carbon-clickhouse:
  -check-config=false: Check config and exit
  -config="": Filename of config
  -config-print-default=false: Print default config
  -daemon=false: Run in background
  -pidfile="": Pidfile path (only for daemon)
  -version=false: Print version
```

```toml
[common]
# Run as user. Works only in daemon mode
user = ""
# If logfile is empty use stderr
logfile = "/var/log/carbon-clickhouse/carbon-clickhouse.log"
# Logging error level. Valid values: "debug", "info", "warn", "warning", "error"
loglevel = "info"
# Prefix for store all internal carbon-clickhouse graphs. Supported macroses: {host}
metric-prefix = "carbon.agents.{host}"
# Endpoint for store internal carbon metrics. Valid values: "" or "local", "tcp://host:port", "udp://host:port"
metric-endpoint = "local"
# Interval of storing internal metrics. Like CARBON_METRIC_INTERVAL
metric-interval = "1m0s"
# GOMAXPROCS
max-cpu = 1

[clickhouse]
# Url to ClickHouse http port. 
url = "http://localhost:8123/"
data-table = "graphite"
# Set empty value if not need
tree-table = "graphite_tree"
# Date for records in graphite_tree table
tree-date = "2016-11-01"
# Concurent upload jobs
threads = 1
# Upload timeout
data-timeout = "1m0s"
tree-timeout = "1m0s"

[data]
# Folder for buffering received data
path = "/data/carbon-clickhouse/"
# Internal queue size between receiver and writer to files
input-buffer = 1048576
# Rotate (and upload) file every N bytes
chunk-bytes = 134217728
# And every interval seconds.
# Minimize chunk-interval for minimize lag between point receive and store
chunk-interval = "1s"

[udp]
listen = ":2003"
enabled = true
log-incomplete = false

[tcp]
listen = ":2003"
enabled = true

[pickle]
listen = ":2004"
max-message-size = 67108864
enabled = true

[pprof]
listen = "localhost:7007"
enabled = false
```
