package receiver

import (
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	pickle "github.com/lomik/graphite-pickle"
	"golang.org/x/net/context"
)

func PickleParser(ctx context.Context, in chan []byte, out chan *RowBinary.WriteBuffer, metricsReceived *uint64, errors *uint64) {

	for {
		select {
		case <-ctx.Done():
			return
		case b := <-in:
			PickeParseBytes(ctx, b, uint32(time.Now().Unix()), out, metricsReceived, errors)
		}
	}
}

func PickeParseBytes(ctx context.Context, b []byte, now uint32, out chan *RowBinary.WriteBuffer, metricsReceived *uint64, errors *uint64) {
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
				case <-ctx.Done():
					// pass
				}
			}
			wb = nil
		}
	}

	fail := func() {
		// @TODO: log
		flush()
		atomic.AddUint64(errors, 1)
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
			now,
		)

		metricCount++
	})

	flush()
	if metricCount > 0 {
		atomic.AddUint64(metricsReceived, uint64(metricCount))
	}
}
