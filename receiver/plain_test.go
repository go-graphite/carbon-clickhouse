package receiver

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkPlainParseBuffer(b *testing.B) {
	days := &DaysFrom1970{}
	out := make(chan *WriteBuffer, 1)

	c1 := uint32(0)
	c2 := uint32(0)

	now := time.Now().Unix()

	msg := fmt.Sprintf("carbon.agents.localhost.cache.size 1412351 %d\n", now)
	buf := GetBuffer()
	buf.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf.Write([]byte(msg))
	}

	msg2 := fmt.Sprintf("carbon.agents.server.udp.received 42 %d\n", now)
	buf2 := GetBuffer()
	buf2.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf2.Write([]byte(msg2))
	}

	b.ResetTimer()

	var wb *WriteBuffer
	for i := 0; i < b.N; i++ {
		PlainParseBuffer(nil, buf, out, days, &c1, &c2)
		wb = <-out
		wb.Release()

		PlainParseBuffer(nil, buf2, out, days, &c1, &c2)
		wb = <-out
		wb.Release()
	}
}
