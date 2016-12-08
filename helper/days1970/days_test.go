package days1970

import (
	"testing"
	"time"
)

func BenchmarkDaysTimestamp(b *testing.B) {
	days := Days{}

	timestamp := uint32(time.Now().Unix() - 24*60*60)

	for i := 0; i < b.N; i++ {
		days.Timestamp(timestamp)
	}
}

func BenchmarkDaysTimestampTodayWithNow(b *testing.B) {
	days := Days{}

	timestamp := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		days.TimestampWithNow(timestamp, timestamp)
	}
}
