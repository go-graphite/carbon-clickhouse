package receiver

import (
	"sync"
	"time"
)

var BufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{}
	},
}

type Buffer struct {
	Time time.Time
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
