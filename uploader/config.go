package uploader

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/zapwriter"

	"github.com/lomik/carbon-clickhouse/helper/config"
)

type Config struct {
	Type                 string              `toml:"type"`  // points, series, points-reverse, series-reverse
	TableName            string              `toml:"table"` // keep empty for same as key
	Timeout              *config.Duration    `toml:"timeout"`
	Date                 string              `toml:"date"` // for tree table
	TreeDate             time.Time           `toml:"-"`
	ZeroTimestamp        bool                `toml:"zero-timestamp"` // for points, points-reverse tables
	TLS                  *config.TLS         `toml:"tls"`            // for secure connection to uploader
	Threads              int                 `toml:"threads"`
	URL                  string              `toml:"url"`
	CacheTTL             *config.Duration    `toml:"cache-ttl"`
	IgnoredPatterns      []string            `toml:"ignored-patterns,omitempty"` // points, points-reverse
	CompressData         bool                `toml:"compress-data"`              // compress data while sending to clickhouse
	IgnoredTaggedMetrics []string            `toml:"ignored-tagged-metrics"`     // for tagged table; create only `__name__` tag for these metrics and ignore others
	Hash                 string              `toml:"hash"`                       // in index uploader store hash in memory instead of full metric
	DisableDailyIndex    bool                `toml:"disable-daily-index"`        // do not calculate and upload daily index to ClickHouse
	hashFunc             func(string) string `toml:"-"`
	client               *http.Client        `toml:"-"`
}

func (cfg *Config) Parse() error {
	var err error

	if cfg.Date != "" {
		cfg.TreeDate, err = time.ParseInLocation("2006-01-02", cfg.Date, time.Local)
		if err != nil {
			return err
		}
	}

	if cfg.Timeout == nil {
		cfg.Timeout = &config.Duration{Duration: time.Minute}
	}

	var known bool
	cfg.hashFunc, known = knownHash[cfg.Hash]
	if !known {
		return fmt.Errorf("unknown hash function %#v", cfg.Hash)
	}

	cfg.client = &http.Client{
		Timeout: cfg.Timeout.Value(),
	}
	if cfg.TLS != nil {
		tlsConfig, warns, err := config.ParseClientTLSConfig(cfg.TLS)
		if err != nil {
			return err
		}
		p, err := url.Parse(cfg.URL)
		if err != nil {
			return err
		}
		if p.Scheme == "https" {
			cfg.client.Transport = &http.Transport{
				TLSClientConfig: tlsConfig,
			}
		} else {
			warns = append(warns, "TLS configurations is ignored because scheme is not HTTPS")
		}
		if len(warns) > 0 {
			logger := zapwriter.Logger("config")
			logger.Warn("insecure options detected, while parsing HTTP Client TLS Config for uploader",
				zap.Strings("warnings", warns),
			)
		}
	}

	return nil
}
