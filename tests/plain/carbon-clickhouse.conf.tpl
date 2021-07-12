[common]

[data]
path = "{{ .CCH_STORE_DIR }}"
chunk-interval = "5s"
chunk-auto-interval = ""

[upload.graphite_index]
type = "index"
table = "graphite_index"
url = "http://{{ .CLICKHOUSE_ADDR }}/"
timeout = "2m30s"
cache-ttl = "1h"

[upload.graphite_tags]
type = "tagged"
table = "graphite_tags"
threads = 3
url = "http://{{ .CLICKHOUSE_ADDR }}/"
timeout = "2m30s"
cache-ttl = "1h"

[upload.graphite_reverse]
type = "points-reverse"
table = "graphite_reverse"
url = "http://{{ .CLICKHOUSE_ADDR }}/"
timeout = "2m30s"
zero-timestamp = true

[upload.graphite]
type = "points"
table = "graphite"
url = "http://{{ .CLICKHOUSE_ADDR }}/"
timeout = "2m30s"
zero-timestamp = true

[tcp]
listen = "{{ .CCH_ADDR }}"
enabled = true
drop-future = "0s"
drop-past = "0s"

[logging]
file = "{{ .CCH_STORE_DIR }}/carbon-clickhouse.log"
level = "debug"
