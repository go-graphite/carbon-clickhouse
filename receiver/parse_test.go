package receiver

import (
	"sort"
	"testing"
	"time"
)

var testZeroDay = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func testDaysFrom1970v1(ts uint32) int {
	t := time.Unix(int64(ts), 0)
	return int(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Sub(ZeroDay) / (24 * time.Hour))
}

var testDayTimestamp []int

func init() {
	testDayTimestamp = make([]int, 65536)

	zeroDay := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	step := 12 * 60 * 60
	for i := 0; i < 4294967296; i += step {
		d := time.Unix(int64(i), 0)
		dayLocal := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.Local)
		dayUTC := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
		days := int(dayUTC.Sub(zeroDay) / (24 * time.Hour))

		testDayTimestamp[days] = int(dayLocal.Unix())
	}
}

func testDaysFrom1970v2(ts uint32) int {
	return sort.SearchInts(testDayTimestamp, int(ts))
}

func BenchmarkToDays1970v1(b *testing.B) {
	now := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		testDaysFrom1970v1(now)
	}
}

func BenchmarkToDays1970v2(b *testing.B) {
	now := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		testDaysFrom1970v2(now)
	}
}
