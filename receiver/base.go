package receiver

import (
	"sync/atomic"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/stop"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

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
		oldDropped         uint64 // atomic
	}
	ctx               context.Context
	ctxCancel         context.CancelFunc
	parseThreads      int
	dropFutureSeconds int
	dropOldSeconds    int
	writeChan         chan *RowBinary.WriteBuffer
	logger            *zap.Logger
}

func NewBase(logger *zap.Logger) Base {
	return Base{logger: logger}
}

func sendUint64Counter(send func(metric string, value float64), metric string, value *uint64) {
	v := atomic.LoadUint64(value)
	atomic.AddUint64(value, -v)
	send(metric, float64(v))
}

func sendInt64Gauge(send func(metric string, value float64), metric string, value *int64) {
	send(metric, float64(atomic.LoadInt64(value)))
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
		case "errors":
			sendUint64Counter(send, f, &base.stat.errors)
		case "active":
			sendInt64Gauge(send, f, &base.stat.active)
		}
	}

}
