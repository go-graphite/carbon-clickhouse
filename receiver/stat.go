package receiver

import "sync/atomic"

type stat struct {
	samplesReceived    uint64 // atomic
	messagesReceived   uint64 // atomic
	metricsReceived    uint64 // atomic
	errors             uint64 // atomic
	active             int64  // atomic
	incompleteReceived uint64 // atomic
}

func sendUint64Counter(send func(metric string, value float64), metric string, value *uint64) {
	v := atomic.LoadUint64(value)
	atomic.AddUint64(value, -v)
	send(metric, float64(v))
}

func sendInt64Gauge(send func(metric string, value float64), metric string, value *int64) {
	send(metric, float64(atomic.LoadInt64(value)))
}

func (s *stat) Stat(send func(metric string, value float64), fields ...string) {
	for _, f := range fields {
		switch f {
		case "samplesReceived":
			sendUint64Counter(send, f, &s.samplesReceived)
		case "messagesReceived":
			sendUint64Counter(send, f, &s.messagesReceived)
		case "metricsReceived":
			sendUint64Counter(send, f, &s.metricsReceived)
		case "incompleteReceived":
			sendUint64Counter(send, f, &s.incompleteReceived)
		case "errors":
			sendUint64Counter(send, f, &s.errors)
		case "active":
			sendInt64Gauge(send, f, &s.active)
		}
	}

}
