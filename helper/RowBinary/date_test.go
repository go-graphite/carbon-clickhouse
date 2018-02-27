package RowBinary

import (
	"testing"
	"time"
)

func TestTimestampToDays(t *testing.T) {
	ts := uint32(0)
	end := uint32(time.Now().Unix()) + 473040000 // +15 years
	for ts < end {
		d1 := SlowTimestampToDays(ts)
		d2 := TimestampToDays(ts)
		if d1 != d2 {
			t.FailNow()
		}

		ts += 780 // step 13 minutes
	}
}
func BenchmarkTimestampToDays(b *testing.B) {
	timestamp := uint32(time.Now().Unix())
	x := SlowTimestampToDays(timestamp)

	for i := 0; i < b.N; i++ {
		if TimestampToDays(timestamp) != x {
			b.FailNow()
		}
	}
}

func BenchmarkSlowTimestampToDays(b *testing.B) {
	timestamp := uint32(time.Now().Unix())
	x := SlowTimestampToDays(timestamp)

	for i := 0; i < b.N; i++ {
		if SlowTimestampToDays(timestamp) != x {
			b.FailNow()
		}
	}
}
