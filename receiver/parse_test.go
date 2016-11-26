package receiver

import (
	"testing"
	"time"
)

var testZeroDay = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func testDaysFrom1970(ts uint32) int {
	t := time.Unix(int64(ts), 0)
	return int(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Sub(ZeroDay) / (24 * time.Hour))
}

func BenchmarkToDays1970(b *testing.B) {
	now := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		testDaysFrom1970(now)
	}
}
