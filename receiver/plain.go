package receiver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"github.com/msaf1980/go-stringutils"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func HasDoubleDot(p []byte) bool {
	for i := 1; i < len(p); i += 2 {
		if p[i] == '.' {
			if p[i-1] == '.' {
				return true
			}
			if i+1 < len(p) && p[i+1] == '.' {
				return true
			}
		}
	}
	return false
}

func RemoveDoubleDot(p []byte) []byte {
	if !HasDoubleDot(p) {
		return p
	}

	shift := 0
	for i := 1; i < len(p); i++ {
		if p[i] == '.' && p[i-1-shift] == '.' {
			shift++
		} else if shift > 0 {
			p[i-shift] = p[i]
		}
	}

	return p[:len(p)-shift]
}

func (base *Base) PlainParseLine(p []byte, now uint32) ([]byte, float64, uint32, error) {
	i1 := bytes.IndexByte(p, ' ')
	if i1 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	i2 := bytes.IndexByte(p[i1+1:], ' ')
	if i2 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}
	i2 += i1 + 1

	i3 := len(p)
	if p[i3-1] == '\n' {
		i3--
	}
	if p[i3-1] == '\r' {
		i3--
	}

	value, err := strconv.ParseFloat(unsafeString(p[i1+1:i2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	var timestamp uint32

	if (i3-i2 == 3) && (p[i2+1] == '-') && (p[i2+2] == '1') {
		timestamp = now
	} else {
		tsf, err := strconv.ParseFloat(unsafeString(p[i2+1:i3]), 64)
		if err != nil || math.IsNaN(tsf) {
			return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
		}
		timestamp = uint32(tsf)
	}

	s := RemoveDoubleDot(p[:i1])

	// parse tagged
	// @TODO: parse as bytes, don't cast to string and back
	name, err := tags.Graphite(base.Tags, unsafeString(s))
	return stringutils.UnsafeStringBytes(&name), value, timestamp, err
}

func (base *Base) PlainParseBuffer(ctx context.Context, b *Buffer) {
	offset := 0
	metricCount := uint32(0)
	errorCount := uint32(0)

	wb := RowBinary.GetWriteBuffer()

MainLoop:
	for offset < b.Used {
		lineEnd := bytes.IndexByte(b.Body[offset:b.Used], '\n')
		if lineEnd < 0 {
			errorCount++
			// @TODO: log unfinished line
			break MainLoop
		} else if lineEnd == 0 {
			// skip empty line
			offset++
			continue MainLoop
		}

		name, value, timestamp, err := base.PlainParseLine(b.Body[offset:offset+lineEnd+1], b.Time)
		offset += lineEnd + 1

		// @TODO: check required buffer size, get new

		if err != nil {
			errorCount++
			// @TODO: log error
			continue MainLoop
		}

		if base.isDropBytes(name, b.Time, timestamp, value) {
			continue MainLoop
		}

		// write result to buffer for clickhouse
		wb.WriteGraphitePoint(name, value, timestamp, b.Time)
		metricCount++
	}

	if metricCount > 0 {
		atomic.AddUint64(&base.stat.metricsReceived, uint64(metricCount))
	}
	if errorCount > 0 {
		atomic.AddUint64(&base.stat.errors, uint64(errorCount))
	}

	if wb.Empty() {
		wb.Release()
		return
	}

	select {
	case base.writeChan <- wb:
		// pass
	case <-ctx.Done():
		return
	}
}

func (base *Base) PlainParser(ctx context.Context, in chan *Buffer) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-in:
			base.PlainParseBuffer(ctx, b)
			b.Release()
		}
	}
}
