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

func GetBuffer() *Buffer {
	return BufferPool.Get().(*Buffer).Reset()
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
