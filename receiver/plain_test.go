package receiver

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkPlainParseBuffer(b *testing.B) {
	days := &DaysFrom1970{}
	buf := GetBuffer()
	out := make(chan *WriteBuffer, 1)

	c1 := uint32(0)
	c2 := uint32(0)

	msg := fmt.Sprintf("carbon.agents.localhost.cache.size 1412351 %d\n", time.Now().Unix())

	for i := 0; i < 50; i++ {
		buf.Write([]byte(msg))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PlainParseBuffer(nil, buf, out, days, &c1, &c2)
		wb := <-out
		wb.Release()
	}
}
