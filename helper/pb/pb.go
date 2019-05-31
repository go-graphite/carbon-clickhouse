package pb

import (
	"errors"
	"math"
)

var ErrorTruncated = errors.New("Message truncated")
var ErrorUnknownWireType = errors.New("Unknown wire type")

func WireType(p []byte) (byte, []byte, error) {
	for i := 0; i < len(p); i++ {
		if p[i]&0x80 == 0 { // last byte of varint
			return p[0] & 0x07, p[i+1:], nil
		}
	}
	return 0, p, ErrorTruncated
}

func Uint64(p []byte) (uint64, []byte, error) {
	var ret uint64
	for i := 0; i < len(p); i++ {
		ret += uint64(p[i]&0x7f) << (7 * uint(i))
		if p[i]&0x80 == 0 { // last byte of varint
			return ret, p[i+1:], nil
		}
	}
	return 0, p, ErrorTruncated
}

func Int64(p []byte) (int64, []byte, error) {
	var ret int64
	for i := 0; i < len(p); i++ {
		ret += int64(p[i]&0x7f) << (7 * uint(i))
		if p[i]&0x80 == 0 { // last byte of varint
			return ret, p[i+1:], nil
		}
	}
	return 0, p, ErrorTruncated
}

func Double(p []byte) (float64, []byte, error) {
	if len(p) < 8 {
		return 0, p, ErrorTruncated
	}

	u := uint64(p[0]) | (uint64(p[1]) << 8) | (uint64(p[2]) << 16) | (uint64(p[3]) << 24) |
		(uint64(p[4]) << 32) | (uint64(p[5]) << 40) | (uint64(p[6]) << 48) | (uint64(p[7]) << 56)

	return math.Float64frombits(u), p[8:], nil
}

func SkipVarint(p []byte) ([]byte, error) {
	for i := 0; i < len(p); i++ {
		if p[i]&0x80 == 0 { // last byte of varint
			return p[i+1:], nil
		}
	}
	return p, ErrorTruncated
}

func Bytes(p []byte) ([]byte, []byte, error) {
	if len(p) < 1 {
		return nil, p, ErrorTruncated
	}

	if p[0] < 128 { // single byte varint
		l := int(p[0])
		p = p[1:]
		if len(p) < l {
			return nil, p, ErrorTruncated
		}

		return p[:l], p[l:], nil
	}

	l64, p, err := Uint64(p)
	if err != nil {
		return nil, p, err
	}

	l := int(l64)

	if len(p) < l {
		return nil, p, ErrorTruncated
	}

	return p[:l], p[l:], nil
}

func Skip(p []byte) ([]byte, error) {
	var wt byte
	var err error
	wt, p, err = WireType(p)
	if err != nil {
		return p, err
	}

	switch wt {
	case 0: // Varint
		return SkipVarint(p)
	case 1: // 64-bit
		if len(p) < 8 {
			return p, ErrorTruncated
		}
		return p[8:], nil
	case 2: // Length-delimited
		_, p, err = Bytes(p)
		return p, err
	case 5: // 32-bit
		if len(p) < 4 {
			return p, ErrorTruncated
		}
		return p[4:], nil
	default:
		return p, ErrorUnknownWireType
	}
}
