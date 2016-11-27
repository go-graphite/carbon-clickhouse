package receiver

import "time"

// Helper for fast calculate days count from 1970-01-01 in local timezone
// Not thread-safe!
type DaysFrom1970 struct {
	todayStartTimestamp uint32
	todayEndTimestamp   uint32
	todayDays           uint16
}

func (dd *DaysFrom1970) Timestamp(timestamp uint32) uint16 {
	if timestamp >= dd.todayStartTimestamp && timestamp <= dd.todayStartTimestamp {
		return dd.todayDays
	}

	t := time.Unix(int64(timestamp), 0)
	return uint16(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

func (dd *DaysFrom1970) TimestampWithNow(timestamp uint32, now uint32) uint16 {
	if timestamp < dd.todayStartTimestamp {
		return dd.Timestamp(timestamp)
	}

	// timestamp >= pp.todayStartTimestamp
	if timestamp <= dd.todayEndTimestamp {
		return dd.todayDays
	}

	// timestamp > dd.todayEndTimestamp
	// check now date
	if now > dd.todayEndTimestamp {
		// update "today" required
		d := time.Unix(int64(now), 0)
		dd.todayStartTimestamp = uint32(time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.Local).Unix())
		dd.todayEndTimestamp = uint32(time.Date(d.Year(), d.Month(), d.Day(), 23, 59, 59, 0, time.Local).Unix())
		dd.todayDays = dd.Timestamp(now)
	}

	return dd.Timestamp(timestamp)
}
