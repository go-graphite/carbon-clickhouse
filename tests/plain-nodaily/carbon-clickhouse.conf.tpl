[common]

[data]
path = "{{ .CCH_STORE_DIR }}"
chunk-max-size = 128
chunk-interval = "5s"
chunk-auto-interval = ""

[upload.graphite_index]
type = "index"
table = "graphite_index"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
cache-ttl = "1h"
disable-daily-index = true

[upload.graphite_tags]
type = "tagged"
table = "graphite_tags"
threads = 3
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
cache-ttl = "1h"

[upload.graphite_reverse]
type = "points-reverse"
table = "graphite_reverse"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
zero-timestamp = true

[upload.graphite]
type = "points"
table = "graphite"
url = "{{ .CLICKHOUSE_URL }}/"
timeout = "2m30s"
zero-timestamp = true

[tcp]
listen = "{{ .CCH_ADDR }}"
enabled = true
drop-future = "0s"
drop-past = "0s"

[udp]
enabled = false

[pickle]
enabled = false

[grpc]
enabled = false

[prometheus]
enabled = false

[telegraf_http_json]
enabled = false

[logging]
file = "{{ .CCH_STORE_DIR }}/carbon-clickhouse.log"
level = "debug"
