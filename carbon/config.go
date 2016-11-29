package carbon

import (
	"bytes"
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
)

const MetricEndpointLocal = "local"

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ toml.TextMarshaler = &Duration{}

// UnmarshalText from TOML
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText encode text with TOML format
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Value return time.Duration value
func (d *Duration) Value() time.Duration {
	return d.Duration
}

type commonConfig struct {
	MetricPrefix   string    `toml:"metric-prefix"`
	MetricInterval *Duration `toml:"metric-interval"`
	MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU         int       `toml:"max-cpu"`
}

type clickhouseConfig struct {
	Url            string    `toml:"url"`
	DataTable      string    `toml:"data-table"`
	DataTimeout    *Duration `toml:"data-timeout"`
	TreeTable      string    `toml:"tree-table"`
	TreeDateString string    `toml:"tree-date"`
	TreeDate       time.Time `toml:"-"`
	TreeTimeout    *Duration `toml:"tree-timeout"`
	Threads        int       `toml:"threads"`
}

type udpConfig struct {
	Listen        string `toml:"listen"`
	Enabled       bool   `toml:"enabled"`
	LogIncomplete bool   `toml:"log-incomplete"`
}

type tcpConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type pickleConfig struct {
	Listen         string `toml:"listen"`
	MaxMessageSize int    `toml:"max-message-size"`
	Enabled        bool   `toml:"enabled"`
}

type pprofConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type dataConfig struct {
	Path         string    `toml:"path"`
	FileInterval *Duration `toml:"chunk-interval"`
}

type loggingConfig struct {
	File  string `toml:"file"`
	Level string `toml:"level"`
}

// Config ...
type Config struct {
	Common     commonConfig     `toml:"common"`
	Logging    loggingConfig    `toml:"logging"`
	ClickHouse clickhouseConfig `toml:"clickhouse"`
	Data       dataConfig       `toml:"data"`
	Udp        udpConfig        `toml:"udp"`
	Tcp        tcpConfig        `toml:"tcp"`
	Pickle     pickleConfig     `toml:"pickle"`
	Pprof      pprofConfig      `toml:"pprof"`
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			MetricPrefix: "carbon.agents.{host}",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
		},
		Logging: loggingConfig{
			File:  "/var/log/carbon-clickhouse/carbon-clickhouse.log",
			Level: "info",
		},
		ClickHouse: clickhouseConfig{
			Url:            "http://localhost:8123/",
			DataTable:      "graphite",
			TreeTable:      "graphite_tree",
			TreeDateString: "2016-11-01",
			DataTimeout: &Duration{
				Duration: time.Minute,
			},
			TreeTimeout: &Duration{
				Duration: time.Minute,
			},
			Threads: 1,
		},
		Data: dataConfig{
			Path: "/data/carbon-clickhouse/",
			FileInterval: &Duration{
				Duration: time.Second,
			},
		},
		Udp: udpConfig{
			Listen:        ":2003",
			Enabled:       true,
			LogIncomplete: false,
		},
		Tcp: tcpConfig{
			Listen:  ":2003",
			Enabled: true,
		},
		Pickle: pickleConfig{
			Listen:         ":2004",
			Enabled:        true,
			MaxMessageSize: 67108864, // 64 Mb
		},
		Pprof: pprofConfig{
			Listen:  "localhost:7007",
			Enabled: false,
		},
	}

	return cfg
}

// PrintConfig ...
func PrintConfig(cfg *Config) error {
	buf := new(bytes.Buffer)

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// ParseConfig ...
func ParseConfig(filename string, cfg *Config) error {
	if filename != "" {
		if _, err := toml.DecodeFile(filename, cfg); err != nil {
			return err
		}
	}

	var err error
	cfg.ClickHouse.TreeDate, err = time.ParseInLocation("2006-01-02", cfg.ClickHouse.TreeDateString, time.Local)
	if err != nil {
		return err
	}

	return nil
}
