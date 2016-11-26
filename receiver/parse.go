package receiver

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"unsafe"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ParseLine(p []byte) ([]byte, float64, int32, error) {
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

	return p[:i1], value, int32(tsf), nil
}

func ParseBufferPlain(b *Buffer) {
	offset := 0

MainLoop:
	for offset < b.Used {
		lineEnd := bytes.IndexByte(b.Body[offset:b.Used], '\n')
		if lineEnd < 0 {
			// @TODO: log unfinished line
			break MainLoop
		} else if lineEnd == 0 {
			// skip empty line
			offset++
			continue MainLoop
		}

		ParseLine(b.Body[offset : offset+lineEnd+1])
		offset += lineEnd + 1
		// fmt.Println(offset)
	}

	BufferPool.Put(b)
}
