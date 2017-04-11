package receiver

import (
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
)

func PickleParser(exit chan struct{}, in chan []byte, out chan *RowBinary.WriteBuffer, metricsReceived *uint32, errors *uint32) {
	days := &days1970.Days{}

	for {
		select {
		case <-exit:
			return
		case b := <-in:
			PickeParseBytes(exit, b, out, days, metricsReceived, errors)
		}
	}
}

func PickeParseBytes(exit chan struct{}, b []byte, out chan *RowBinary.WriteBuffer, days *days1970.Days, metricsReceived *uint32, errors *uint32) {
}
