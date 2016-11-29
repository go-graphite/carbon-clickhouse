package RowBinary

import (
	"encoding/binary"
	"math"
	"sync"
)

var WriteBufferPool = sync.Pool{
	New: func() interface{} {
		return &WriteBuffer{}
	},
}

type WriteBuffer struct {
	Used int
	Body [524288]byte
}

func GetWriteBuffer() *WriteBuffer {
	return WriteBufferPool.Get().(*WriteBuffer).Reset()
}

func (wb *WriteBuffer) Reset() *WriteBuffer {
	wb.Used = 0
	return wb
}

func (wb *WriteBuffer) Empty() bool {
	return wb.Used == 0
}

func (wb *WriteBuffer) Release() {
	wb.Used = 0
	WriteBufferPool.Put(wb)
}

func (wb *WriteBuffer) WriteBytes(p []byte) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(len(p)))
	wb.Used += copy(wb.Body[wb.Used:], p)
}

func (wb *WriteBuffer) WriteFloat64(value float64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], math.Float64bits(value))
	wb.Used += 8
}

func (wb *WriteBuffer) WriteUint16(value uint16) {
	binary.LittleEndian.PutUint16(wb.Body[wb.Used:], value)
	wb.Used += 2
}

func (wb *WriteBuffer) WriteUint32(value uint32) {
	binary.LittleEndian.PutUint32(wb.Body[wb.Used:], value)
	wb.Used += 4
}

func (wb *WriteBuffer) WriteUint64(value uint64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], value)
	wb.Used += 8
}

func (wb *WriteBuffer) Write(p []byte) {
	wb.Used += copy(wb.Body[wb.Used:], p)
}
