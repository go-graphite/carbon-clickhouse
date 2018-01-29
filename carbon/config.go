package carbon

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/lomik/carbon-clickhouse/uploader"
	"github.com/lomik/zapwriter"
)

const MetricEndpointLocal = "local"

type commonConfig struct {
	MetricPrefix   string           `toml:"metric-prefix"`
	MetricInterval *config.Duration `toml:"metric-interval"`
	MetricEndpoint string           `toml:"metric-endpoint"`
	MaxCPU         int              `toml:"max-cpu"`
}

type clickhouseConfig struct {
	Url string `toml:"url"`
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
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type grpcConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type pprofConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type dataConfig struct {
	Path         string           `toml:"path"`
	FileInterval *config.Duration `toml:"chunk-interval"`
}

// Config ...
type Config struct {
	Common  commonConfig                `toml:"common"`
	Data    dataConfig                  `toml:"data"`
	Upload  map[string]*uploader.Config `toml:"upload"`
	Udp     udpConfig                   `toml:"udp"`
	Tcp     tcpConfig                   `toml:"tcp"`
	Pickle  pickleConfig                `toml:"pickle"`
	Grpc    grpcConfig                  `toml:"grpc"`
	Pprof   pprofConfig                 `toml:"pprof"`
	Logging []zapwriter.Config          `toml:"logging"`
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			MetricPrefix: "carbon.agents.{host}",
			MetricInterval: &config.Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
		},
		Logging: nil,
		Data: dataConfig{
			Path: "/data/carbon-clickhouse/",
			FileInterval: &config.Duration{
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
			Listen:  ":2004",
			Enabled: true,
		},
		Grpc: grpcConfig{
			Listen:  ":2005",
			Enabled: false,
		},
		Pprof: pprofConfig{
			Listen:  "localhost:7007",
			Enabled: false,
		},
	}

	return cfg
}

func NewLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/carbon-clickhouse/carbon-clickhouse.log"
	return cfg
}

// PrintConfig ...
func PrintDefaultConfig() error {
	cfg := NewConfig()
	buf := new(bytes.Buffer)

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	cfg.Upload = map[string]*uploader.Config{
		"graphite": &uploader.Config{
			Type: "points",
			Timeout: &config.Duration{
				Duration: time.Minute,
			},
			Threads:   1,
			TableName: "graphite",
			URL:       "http://localhost:8123/",
		},
		"graphite_tree": &uploader.Config{
			Type: "tree",
			Date: "2016-11-01",
			Timeout: &config.Duration{
				Duration: time.Minute,
			},
			CacheTTL: &config.Duration{
				Duration: 12 * time.Hour,
			},
			Threads:   1,
			TableName: "graphite_tree",
			URL:       "http://localhost:8123/",
		},
	}

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// ReadConfig ...
func ReadConfig(filename string) (*Config, error) {
	// var err error

	cfg := NewConfig()
	if filename != "" {
		b, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		body := string(b)

		// @TODO: fix for config starts with [logging]
		body = strings.Replace(body, "\n[logging]\n", "\n[[logging]]\n", -1)

		if _, err := toml.Decode(body, cfg); err != nil {
			return nil, err
		}
	}

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	if err := zapwriter.CheckConfig(cfg.Logging, nil); err != nil {
		return nil, err
	}

	for _, u := range cfg.Upload {
		if err := u.Parse(); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
