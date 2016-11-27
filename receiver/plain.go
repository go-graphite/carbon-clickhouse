package receiver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"unsafe"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func PlainParseLine(p []byte) ([]byte, float64, uint32, error) {
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

	value, err := strconv.ParseFloat(unsafeString(p[i1+1:i2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	tsf, err := strconv.ParseFloat(unsafeString(p[i2+1:i3]), 64)
	if err != nil || math.IsNaN(tsf) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	return p[:i1], value, uint32(tsf), nil
}

func PlainParseBuffer(exit chan struct{}, b *Buffer, out chan *WriteBuffer, days *DaysFrom1970, metricsReceived *uint32, errors *uint32) {
	offset := 0
	metricCount := uint32(0)
	errorCount := uint32(0)

	version := make([]byte, 4)
	binary.LittleEndian.PutUint32(version, b.Time)

	wb := GetWriteBuffer()

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

		name, value, timestamp, err := PlainParseLine(b.Body[offset : offset+lineEnd+1])
		offset += lineEnd + 1

		// @TODO: check required buffer size, get new

		if err != nil {
			errorCount++
			// @TODO: log error
			continue MainLoop
		}

		// write result to buffer for clickhouse
		wb.RowBinaryWriteBytes(name)
		wb.RowBinaryWriteFloat64(value)
		wb.RowBinaryWriteUint32(timestamp)
		wb.RowBinaryWriteUint16(days.TimestampWithNow(timestamp, b.Time))
		wb.Write(version)
		metricCount++
	}

	if metricCount > 0 {
		atomic.AddUint32(metricsReceived, metricCount)
	}
	if errorCount > 0 {
		atomic.AddUint32(errors, errorCount)
	}

	if wb.Empty() {
		wb.Release()
		return
	}

	select {
	case out <- wb:
		// pass
	case <-exit:
		return
	}
}

func PlainParser(exit chan struct{}, in chan *Buffer, out chan *WriteBuffer, metricsReceived *uint32, errors *uint32) {
	days := &DaysFrom1970{}

	for {
		select {
		case <-exit:
			return
		case b := <-in:
			PlainParseBuffer(exit, b, out, days, metricsReceived, errors)
			b.Release()
		}
	}
}
