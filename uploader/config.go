package uploader

import (
	"github.com/lomik/carbon-clickhouse/helper/config"
)

type Config struct {
	Type          string           `toml:"type"`  // points, series, points-reverse, series-reverse
	TableName     string           `toml:"table"` // keep empty for same as key
	Timeout       *config.Duration `toml:"timeout"`
	ZeroTimestamp bool             `toml:"zero-timestamp"` // for points, points-reverse tables
	Threads       int              `toml:"threads"`
	URL           string           `toml:"url"`
	CacheTTL      *config.Duration `toml:"cache-ttl"`
}

func (cfg *Config) Parse() error {
	// Leave empty for potential future use
	return nil
}
