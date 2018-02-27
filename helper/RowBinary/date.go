package RowBinary

import "time"

var daysTimestampStart []int64

func init() {
	daysTimestampStart = make([]int64, 0)
	end := time.Now().UTC().Add(10 * 365 * 24 * time.Hour)

	t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	for t.Before(end) {
		ts := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local).Unix()
		daysTimestampStart = append(daysTimestampStart, ts)
		t = t.Add(24 * time.Hour)
	}
}

func TimestampToDays(timestamp uint32) uint16 {
	if int64(timestamp) < daysTimestampStart[0] {
		return 0
	}

	i := int(timestamp / 86400)
	ts := int64(timestamp)

	if i < 10 || i > len(daysTimestampStart)-10 {
		// fallback to slow method
		return SlowTimestampToDays(timestamp)
	}

FindLoop:
	for {
		if ts < daysTimestampStart[i] {
			i--
			continue FindLoop
		}
		if ts >= daysTimestampStart[i+1] {
			i++
			continue FindLoop
		}
		return uint16(i)
	}
}

func SlowTimestampToDays(timestamp uint32) uint16 {
	t := time.Unix(int64(timestamp), 0)
	return uint16(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}
