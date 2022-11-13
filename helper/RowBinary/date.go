package RowBinary

import "time"

var TimestampToDays func(timestamp uint32) uint16

// UTCTimestampToDays is always UTC, but mismatch SlowTimestampToDays and need points/index/tags table rebuild (with Date recalc)
func SetUTCDate() {
	TimestampToDays = UTCTimestampToDays
}

// PrecalcTimestampToDays is broken, not always UTC, like SlowTimestampToDays, but used from start of project
func SetDefaultDate() {
	TimestampToDays = PrecalcTimestampToDays
}

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
	SetDefaultDate()
}

// PrecalcTimestampToDays is broken, not always UTC, like SlowTimestampToDays
func PrecalcTimestampToDays(timestamp uint32) uint16 {
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

// SlowTimestampToDays is broken, not always UTC
func SlowTimestampToDays(timestamp uint32) uint16 {
	t := time.Unix(int64(timestamp), 0)
	return uint16(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

// TimestampToDaysFormat is pair for SlowTimestampToDays and broken also for symmetric
func TimestampToDaysFormat(timestamp int64) string {
	t := time.Unix(timestamp, 0)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

// TimeToDaysFormat like TimestampDaysFormat, but for time.Time
func TimeToDaysFormat(t time.Time) string {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

func UTCTimestampToDays(timestamp uint32) uint16 {
	return uint16(timestamp / 86400)
}

func UTCTimestampToDaysFormat(timestamp uint32) string {
	return time.Unix(int64(timestamp), 0).UTC().Format("2006-01-02")
}
