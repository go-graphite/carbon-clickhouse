package receiver

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	pickle "github.com/lomik/graphite-pickle"
)

func (base *Base) PickleParser(ctx context.Context, in chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-in:
			base.PickleParseBytes(ctx, b, uint32(time.Now().Unix()))
		}
	}
}

func (base *Base) PickleParseBytes(ctx context.Context, b []byte, now uint32) {
	metricCount := uint32(0)
	wb := RowBinary.GetWriteBuffer()

	flush := func() {
		if wb != nil {
			if wb.Empty() {
				wb.Release()
			} else {
				select {
				case base.writeChan <- wb:
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
		atomic.AddUint64(&base.stat.errors, 1)
	}

	splitBuf := make([]string, 256)

	pickle.ParseMessage(b, func(name string, value float64, timestamp int64) {
		name, err := tags.Graphite(base.Tags, name, splitBuf)
		if err != nil {
			// @TODO: log?
			return
		}

		if base.isDropString(name, now, uint32(timestamp), value) {
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
		atomic.AddUint64(&base.stat.metricsReceived, uint64(metricCount))
	}
}
