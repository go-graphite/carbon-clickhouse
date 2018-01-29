package receiver

import (
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	pickle "github.com/lomik/graphite-pickle"
)

func PickleParser(exit chan struct{}, in chan []byte, out chan *RowBinary.WriteBuffer, metricsReceived *uint32, errors *uint32) {
	days := &days1970.Days{}

	for {
		select {
		case <-exit:
			return
		case b := <-in:
			PickeParseBytes(exit, b, uint32(time.Now().Unix()), out, days, metricsReceived, errors)
		}
	}
}

func PickeParseBytes(exit chan struct{}, b []byte, now uint32, out chan *RowBinary.WriteBuffer, days *days1970.Days, metricsReceived *uint32, errors *uint32) {
	metricCount := uint32(0)
	wb := RowBinary.GetWriteBuffer()

	flush := func() {
		if wb != nil {
			if wb.Empty() {
				wb.Release()
			} else {
				select {
				case out <- wb:
					// pass
				case <-exit:
					// pass
				}
			}
			wb = nil
		}
	}

	fail := func() {
		// @TODO: log
		flush()
		atomic.AddUint32(errors, 1)
	}

	pickle.ParseMessage(b, func(name string, value float64, timestamp int64) {
		name, err := tags.Graphite(name)
		if err != nil {
			// @TODO: log?
			return
		}

		if !wb.CanWriteGraphitePoint(len(name)) {
			flush()
			if len(name) > RowBinary.WriteBufferSize-50 {
				fail()
				return
			}
			wb = RowBinary.GetWriteBuffer()
		}

		wb.WriteGraphitePoint(
			[]byte(name),
			value,
			uint32(timestamp),
			days.TimestampWithNow(uint32(timestamp), now),
			now,
		)

		metricCount++
	})

	flush()
	if metricCount > 0 {
		atomic.AddUint32(metricsReceived, metricCount)
	}
}
