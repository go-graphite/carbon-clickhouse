package RowBinary

import (
	"os"
	"strconv"
	"testing"
	"time"
)

var runBroken bool

func init() {
	if os.Getenv("RUN_BRKEN_TESTS") == "1" {
		runBroken = true
	}
}

// SlowTimestampToDays is broken on some cases
//
// TZ=Etc/GMT-5 RUN_BRKEN_TESTS=1 make test
// --- FAIL: TestSlowTimestampToDays (0.00s)
//
//	--- FAIL: TestSlowTimestampToDays/1668106870_2022-11-11T00:01:10+05:00,_UTC_2022-11-10T19:01:10Z_[0] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19307 (2022-11-11), want 19306 (2022-11-10)
//	--- FAIL: TestSlowTimestampToDays/1668193200_2022-11-12T00:00:00+05:00,_UTC_2022-11-11T19:00:00Z_[1] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19308 (2022-11-12), want 19307 (2022-11-11)
//
// FAIL
// FAIL	github.com/lomik/carbon-clickhouse/helper/RowBinary	3.328s
//
// $ TZ=Etc/GMT+5 RUN_BRKEN_TESTS=1 make test
// go test -race ./...
// --- FAIL: TestSlowTimestampToDays (0.00s)
//
//	--- FAIL: TestSlowTimestampToDays/1668106870_2022-11-10T14:01:10-05:00,_UTC_2022-11-10T19:01:10Z_[0] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19306 (2022-11-09), want 19306 (2022-11-10)
//	--- FAIL: TestSlowTimestampToDays/1668193200_2022-11-11T14:00:00-05:00,_UTC_2022-11-11T19:00:00Z_[1] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19307 (2022-11-10), want 19307 (2022-11-11)
//	--- FAIL: TestSlowTimestampToDays/1668124800_2022-11-10T19:00:00-05:00,_UTC_2022-11-11T00:00:00Z_[2] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19306 (2022-11-09), want 19307 (2022-11-11)
//	--- FAIL: TestSlowTimestampToDays/1668142799_2022-11-10T23:59:59-05:00,_UTC_2022-11-11T04:59:59Z_[3] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19306 (2022-11-09), want 19307 (2022-11-11)
//	--- FAIL: TestSlowTimestampToDays/1650776160_2022-04-23T23:56:00-05:00,_UTC_2022-04-24T04:56:00Z_[4] (0.00s)
//	    date_test.go:62: TimestampDaysFormat() = 19105 (2022-04-22), want 19106 (2022-04-24)
//
// --- FAIL: TestTimestampToDays (0.00s)
func TestSlowTimestampToDays(t *testing.T) {
	if !runBroken {
		t.Log("skip broken test, set RUN_BRKEN_TESTS=1 for run")
		return
	}
	tests := []struct {
		ts      uint32
		want    uint16
		wantStr string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want:    19306,
			wantStr: "2022-11-10",
		},
		{
			ts: 1668193200, // 2022-11-12 00:00:00 +05:00 ; 2022-11-11 19:00:00 UTC
			// SELECT Date(19307)
			//         2022-11-11
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts:      1668124800, // 2022-11-11 00:00:00 UTC
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts:      1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want:    19106,
			wantStr: "2022-04-24",
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(int64(tt.ts), 10)+" "+time.Unix(int64(tt.ts), 0).Format(time.RFC3339)+", UTC "+time.Unix(int64(tt.ts), 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			got := SlowTimestampToDays(tt.ts)
			// gotStr := dayStart.Add(time.Duration(int64(got) * 24 * 3600 * 1e9)).Format("2006-01-02")
			gotStr := time.Unix(int64(got)*24*3600, 0).Format("2006-01-02")
			if gotStr != tt.wantStr || got != tt.want {
				t.Errorf("TimestampDaysFormat() = %v (%s), want %v (%s)", got, gotStr, tt.want, tt.wantStr)
			}
			convStr := UTCTimestampToDaysFormat(uint32(tt.want) * 24 * 3600)
			if convStr != tt.wantStr {
				t.Errorf("conversion got %s, want %s", convStr, tt.wantStr)
			}
		})
	}
}

// TimestampToDays is broken on some cases like SlowTimestampToDays
func TestTimestampToDays(t *testing.T) {
	ts := uint32(0)
	end := uint32(time.Now().Unix()) + 473040000 // +15 years
	for ts < end {
		d1 := SlowTimestampToDays(ts)
		d2 := TimestampToDays(ts)
		if d1 != d2 {
			t.Fatalf("SlowTimestampToDays(%d)=%d, TimestampToDays(%d)=%d", ts, d1, ts, d2)
		}
		ts1 := time.Unix(int64(d1)*86400, 0)
		ds1 := time.Date(ts1.Year(), ts1.Month(), ts1.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		ds2 := TimestampToDaysFormat(int64(d1) * 86400)
		if ds1 != ds2 {
			t.Fatalf("SlowTimestampToDays(%d)=%d, format error %s %s", ts, d1, ds1, ds2)
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

func TestUTCTimestampToDays(t *testing.T) {
	tests := []struct {
		ts      uint32
		want    uint16
		wantStr string
	}{
		{
			ts: 1668106870, // 2022-11-11 00:01:10 +05:00 ; 2022-11-10 19:01:10 UTC
			// select toDate(1650776160,'UTC')
			//     2022-11-10
			want:    19306,
			wantStr: "2022-11-10",
		},
		{
			ts: 1668193200, // 2022-11-12 00:00:00 +05:00 ; 2022-11-11 19:00:00 UTC
			// SELECT Date(19307)
			//         2022-11-11
			// SELECT toDate(1668193200, 'UTC')
			//         2022-11-11
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts:      1668124800, // 2022-11-11 00:00:00 UTC
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts:      1668142799, // 2022-11-10 23:59:59 -05:00; 2022-11-11 04:59:59 UTC
			want:    19307,
			wantStr: "2022-11-11",
		},
		{
			ts: 1650776160, // graphite-clickhouse issue #184, graphite-clickhouse in UTC, clickhouse in PDT(UTC-7)
			// 2022-04-24 4:56:00
			// select toDate(1650776160,'UTC')
			//                        2022-04-24
			// select toDate(1650776160,'Etc/GMT+7')
			//                        2022-04-23
			want:    19106,
			wantStr: "2022-04-24",
		},
	}
	for i, tt := range tests {
		t.Run(strconv.FormatInt(int64(tt.ts), 10)+" "+time.Unix(int64(tt.ts), 0).Format(time.RFC3339)+", UTC "+time.Unix(int64(tt.ts), 0).UTC().Format(time.RFC3339)+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			got := UTCTimestampToDays(tt.ts)
			// gotStr := dayStart.Add(time.Duration(int64(got) * 24 * 3600 * 1e9)).Format("2006-01-02")
			gotStr := UTCTimestampToDaysFormat(uint32(got) * 86400)
			if gotStr != tt.wantStr || got != tt.want {
				t.Errorf("TimestampDaysFormat() = %v (%s), want %v (%s)", got, gotStr, tt.want, tt.wantStr)
			}
		})
	}
}
