package uploader

import (
	"time"

	"github.com/lomik/carbon-clickhouse/helper/config"
)

type Config struct {
	Type            string           `toml:"type"`  // points, series, points-reverse, series-reverse
	TableName       string           `toml:"table"` // keep empty for same as key
	Timeout         *config.Duration `toml:"timeout"`
	Date            string           `toml:"date"` // for tree table
	TreeDate        time.Time        `toml:"-"`
	ZeroTimestamp   bool             `toml:"zero-timestamp"` // for points, points-reverse tables
	Threads         int              `toml:"threads"`
	URL             string           `toml:"url"`
	CacheTTL        *config.Duration `toml:"cache-ttl"`
	IgnoredPatterns []string         `toml:"ignored-patterns,omitempty"` // points, points-reverse
}

func (cfg *Config) Parse() error {
	var err error

	if cfg.Date != "" {
		cfg.TreeDate, err = time.ParseInLocation("2006-01-02", cfg.Date, time.Local)
		if err != nil {
			return err
		}
	}

	return nil
}
