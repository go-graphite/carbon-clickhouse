package uploader

import (
	"encoding/binary"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

// decoder for test RowBinary encoding (without bounds checker)
type decoder struct {
	b   []byte
	pos int
}

func newDecoder(b []byte) *decoder {
	return &decoder{b: b}
}

func (d *decoder) GetUint16() uint16 {
	n := binary.LittleEndian.Uint16(d.b[d.pos:])
	d.pos += RowBinary.SIZE_INT16

	return n
}

func (d *decoder) GetUint32() uint32 {
	n := binary.LittleEndian.Uint32(d.b[d.pos:])
	d.pos += RowBinary.SIZE_INT32

	return n
}

func (d *decoder) GetUvariant() uint64 {
	v, n := binary.Uvarint(d.b[d.pos:])
	d.pos += n

	return v
}

func (d *decoder) GetString() string {
	length := d.GetUvariant()

	if length == 0 {
		return ""
	}
	s := string(d.b[d.pos : d.pos+int(length)])
	d.pos += int(length)

	return s
}

func (d *decoder) GetStrings() []string {
	length := d.GetUvariant()

	var result []string
	if length > 0 {
		for i := 0; i < int(length); i++ {
			result = append(result, d.GetString())
		}
	}

	return result
}
