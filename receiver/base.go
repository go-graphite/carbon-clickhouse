package receiver

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/stop"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"go.uber.org/zap"
)

const droppedListSize = 1000

type Base struct {
	stop.Struct
	stat struct {
		samplesReceived    uint64 // atomic
		messagesReceived   uint64 // atomic
		metricsReceived    uint64 // atomic
		errors             uint64 // atomic
		active             int64  // atomic
		incompleteReceived uint64 // atomic
		futureDropped      uint64 // atomic
		pastDropped        uint64 // atomic
		tooLongDropped     uint64 // atomic
	}
	droppedList        [droppedListSize]string
	droppedListNext    int
	droppedListMu      sync.Mutex
	parseThreads       int
	dropFutureSeconds  uint32
	dropPastSeconds    uint32
	dropTooLongLimit   uint16
	readTimeoutSeconds uint32
	writeChan          chan *RowBinary.WriteBuffer
	logger             *zap.Logger
	Tags               tags.TagConfig
	concatCharacter    string
}

// func NewBase(logger *zap.Logger, config tags.TagConfig) Base {
// 	return Base{logger: logger, Tags: config}
// }

func sendUint64Counter(send func(metric string, value float64), metric string, value *uint64) {
	v := atomic.LoadUint64(value)
	atomic.AddUint64(value, -v)
	send(metric, float64(v))
}

func sendInt64Gauge(send func(metric string, value float64), metric string, value *int64) {
	send(metric, float64(atomic.LoadInt64(value)))
}

func (base *Base) Init(logger *zap.Logger, config tags.TagConfig, opts ...Option) {
	base.logger = logger
	base.Tags = config

	for _, optApply := range opts {
		optApply(base)
	}
}

func (base *Base) isDrop(nowTime uint32, metricTime uint32) bool {
	if base.dropFutureSeconds != 0 && (metricTime > (nowTime + base.dropFutureSeconds)) {
		atomic.AddUint64(&base.stat.futureDropped, 1)
		return true
	}
	if base.dropPastSeconds != 0 && (nowTime > (metricTime + base.dropPastSeconds)) {
		atomic.AddUint64(&base.stat.pastDropped, 1)
		return true
	}
	return false
}

func (base *Base) isDropMetricNameTooLong(name string) bool {
	if base.dropTooLongLimit != 0 && len(name) > int(base.dropTooLongLimit) {
		atomic.AddUint64(&base.stat.tooLongDropped, 1)
		return true
	}
	return false
}

func (base *Base) DroppedHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	n := make([]string, droppedListSize)
	base.droppedListMu.Lock()
	copy(n, base.droppedList[:])
	base.droppedListMu.Unlock()
	sort.Strings(n)
	for i := 0; i < len(n); i++ {
		if n[i] != "" {
			fmt.Fprintln(w, n[i])
		}
	}
}

func (base *Base) saveDropped(name string, nowTime uint32, metricTime uint32, value float64) {
	s := fmt.Sprintf("rcv:%d\tname:%s\ttimestamp:%d\tvalue:%#v", nowTime, name, metricTime, value)

	base.droppedListMu.Lock()
	base.droppedList[base.droppedListNext%droppedListSize] = s
	base.droppedListNext++
	base.droppedListMu.Unlock()
}

func (base *Base) isDropString(name string, nowTime uint32, metricTime uint32, value float64) bool {
	if !base.isDrop(nowTime, metricTime) && !base.isDropMetricNameTooLong(name) {
		return false
	}

	base.saveDropped(name, nowTime, metricTime, value)
	return true
}

func (base *Base) isDropBytes(name []byte, nowTime uint32, metricTime uint32, value float64) bool {
	if !base.isDrop(nowTime, metricTime) && !base.isDropMetricNameTooLong(unsafeString(name)) {
		return false
	}
	base.saveDropped(unsafeString(name), nowTime, metricTime, value)
	return true
}

func (base *Base) SendStat(send func(metric string, value float64), fields ...string) {
	for _, f := range fields {
		switch f {
		case "samplesReceived":
			sendUint64Counter(send, f, &base.stat.samplesReceived)
		case "messagesReceived":
			sendUint64Counter(send, f, &base.stat.messagesReceived)
		case "metricsReceived":
			sendUint64Counter(send, f, &base.stat.metricsReceived)
		case "incompleteReceived":
			sendUint64Counter(send, f, &base.stat.incompleteReceived)
		case "futureDropped":
			sendUint64Counter(send, f, &base.stat.futureDropped)
		case "pastDropped":
			sendUint64Counter(send, f, &base.stat.pastDropped)
		case "tooLongDropped":
			sendUint64Counter(send, f, &base.stat.tooLongDropped)
		case "errors":
			sendUint64Counter(send, f, &base.stat.errors)
		case "active":
			sendInt64Gauge(send, f, &base.stat.active)
		}
	}

}
