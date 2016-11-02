# carbon-clickhouse
Graphite metrics receiver with ClickHouse as storage

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
