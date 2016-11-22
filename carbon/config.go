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
	LogFile        string    `toml:"logfile"`
	LogLevel       string    `toml:"loglevel"`
	MetricPrefix   string    `toml:"metric-prefix"`
	MetricInterval *Duration `toml:"metric-interval"`
	MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU         int       `toml:"max-cpu"`
}

type clickhouseConfig struct {
	Url         string    `toml:"url"`
	DataTable   string    `toml:"data-table"`
	DataTimeout *Duration `toml:"data-timeout"`
	TreeTable   string    `toml:"tree-table"`
	TreeDate    string    `toml:"tree-date"`
	TreeTimeout *Duration `toml:"tree-timeout"`
	Threads     int       `toml:"threads"`
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
	InputBuffer  int       `toml:"input-buffer"`
	FileInterval *Duration `toml:"chunk-interval"`
	FileBytes    int       `toml:"chunk-bytes"`
}

// Config ...
type Config struct {
	Common     commonConfig     `toml:"common"`
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
			LogFile:      "/var/log/carbon-clickhouse/carbon-clickhouse.log",
			LogLevel:     "info",
			MetricPrefix: "carbon.agents.{host}",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
		},
		ClickHouse: clickhouseConfig{
			Url:       "http://localhost:8123/",
			DataTable: "graphite",
			TreeTable: "graphite_tree",
			TreeDate:  "2016-11-01",
			DataTimeout: &Duration{
				Duration: time.Minute,
			},
			TreeTimeout: &Duration{
				Duration: time.Minute,
			},
			Threads: 1,
		},
		Data: dataConfig{
			Path:        "/data/carbon-clickhouse/",
			InputBuffer: 1024 * 1024,
			FileInterval: &Duration{
				Duration: time.Second,
			},
			FileBytes: 128 * 1024 * 1024,
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
func PrintConfig(cfg interface{}) error {
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
func ParseConfig(filename string, cfg interface{}) error {
	if filename != "" {
		if _, err := toml.DecodeFile(filename, cfg); err != nil {
			return err
		}
	}
	return nil
}
