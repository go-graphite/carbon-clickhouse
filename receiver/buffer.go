package receiver

import "sync"

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

func (wb *WriteBuffer) Reset() *WriteBuffer {
	wb.Used = 0
	return wb
}

func (wb *WriteBuffer) Release() {
	wb.Used = 0
	WriteBufferPool.Put(wb)
}
