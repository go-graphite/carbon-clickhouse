package receiver

import (
	"encoding/binary"
	"math"
	"sync"
)

var BufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{}
	},
}

type Buffer struct {
	Time uint32
	Used int
	Body [262144]byte
}

var WriteBufferPool = sync.Pool{
	New: func() interface{} {
		return &WriteBuffer{}
	},
}

type WriteBuffer struct {
	Used int
	Body [524288]byte
}

func GetBuffer() *Buffer {
	return BufferPool.Get().(*Buffer).Reset()
}

func GetWriteBuffer() *WriteBuffer {
	return WriteBufferPool.Get().(*WriteBuffer).Reset()
}

func (b *Buffer) Reset() *Buffer {
	b.Used = 0
	return b
}

func (b *Buffer) Release() {
	b.Used = 0
	BufferPool.Put(b)
}

func (b *Buffer) Write(p []byte) {
	b.Used += copy(b.Body[b.Used:], p)
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

func (wb *WriteBuffer) RowBinaryWriteBytes(p []byte) {
	wb.Used += binary.PutUvarint(wb.Body[wb.Used:], uint64(len(p)))
	wb.Used += copy(wb.Body[wb.Used:], p)
}

func (wb *WriteBuffer) RowBinaryWriteFloat64(value float64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], math.Float64bits(value))
	wb.Used += 8
}

func (wb *WriteBuffer) RowBinaryWriteUint16(value uint16) {
	binary.LittleEndian.PutUint16(wb.Body[wb.Used:], value)
	wb.Used += 2
}

func (wb *WriteBuffer) RowBinaryWriteUint32(value uint32) {
	binary.LittleEndian.PutUint32(wb.Body[wb.Used:], value)
	wb.Used += 4
}

func (wb *WriteBuffer) RowBinaryWriteUint64(value uint64) {
	binary.LittleEndian.PutUint64(wb.Body[wb.Used:], value)
	wb.Used += 8
}

func (wb *WriteBuffer) Write(p []byte) {
	wb.Used += copy(wb.Body[wb.Used:], p)
}
