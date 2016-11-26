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
