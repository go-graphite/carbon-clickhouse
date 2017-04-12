package receiver

import (
	"bytes"
	"math"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
	"github.com/lomik/og-rek"
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

func asList(value interface{}) ([]interface{}, bool) {
	if l, ok := value.([]interface{}); ok {
		return l, ok
	}

	if l, ok := value.(ogórek.Tuple); ok {
		return []interface{}(l), ok
	}

	// pp.Println("v", v)
	// pp.Println("Fail", value, v)

	return nil, false
}

func asInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case float32:
		return int64(value), true
	case float64:
		return int64(value), true
	case int:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return int64(value), true
	case int8:
		return int64(value), true
	case uint:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	case uint8:
		return int64(value), true
	default:
		return 0, false
	}
	return 0, false
}

func asFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return float64(value), true
	case int:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int8:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uint8:
		return float64(value), true
	default:
		return 0, false
	}
	return 0, false
}

func PickeParseBytes(exit chan struct{}, b []byte, now uint32, out chan *RowBinary.WriteBuffer, days *days1970.Days, metricsReceived *uint32, errors *uint32) {
	d := ogórek.NewDecoder(bytes.NewBuffer(b))

	// metricCount := uint32(0)
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

	v, err := d.Decode()
	if err != nil {
		fail()
		return
	}

	series, ok := asList(v)
	if !ok {
		fail()
		return
	}

	for _, s := range series {
		metric, ok := asList(s)
		if !ok {
			fail()
			return
		}

		if len(metric) < 2 {
			fail()
			return
		}

		name, ok := metric[0].(string)
		if !ok {
			fail()
			return
		}

		for _, p := range metric[1:] {
			point, ok := asList(p)
			if !ok {
				fail()
				return
			}

			if len(point) != 2 {
				fail()
				return
			}

			timestamp, ok := asInt64(point[0])
			if !ok {
				fail()
				return
			}

			if timestamp < 0 || timestamp > math.MaxUint32 {
				fail()
				return
			}

			value, ok := asFloat64(point[1])
			if !ok {
				fail()
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
		}
	}

	flush()
}
