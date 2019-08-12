package carbon

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/receiver"
	"github.com/lomik/carbon-clickhouse/uploader"
	"github.com/lomik/carbon-clickhouse/writer"
	"github.com/lomik/zapwriter"
)

type App struct {
	sync.RWMutex
	Config           *Config
	Writer           *writer.Writer
	Uploaders        map[string]uploader.Uploader
	UDP              receiver.Receiver
	TCP              receiver.Receiver
	Pickle           receiver.Receiver
	Grpc             receiver.Receiver
	Prometheus       receiver.Receiver
	TelegrafHttpJson receiver.Receiver
	Collector        *Collector // (!!!) Should be re-created on every change config/modules
	writeChan        chan *RowBinary.WriteBuffer
	exit             chan bool
	ConfigFilename   string
}

// New App instance
func New(configFilename string) *App {
	app := &App{
		exit:           make(chan bool),
		ConfigFilename: configFilename,
	}

	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure() error {
	cfg, err := ReadConfig(app.ConfigFilename)
	if err != nil {
		return err
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.Replace(hostname, ".", "_", -1)
		cfg.Common.MetricPrefix = strings.Replace(cfg.Common.MetricPrefix, "{host}", hostname, -1)
	} else {
		cfg.Common.MetricPrefix = strings.Replace(cfg.Common.MetricPrefix, "{host}", "localhost", -1)
	}

	if cfg.Common.MetricEndpoint == "" {
		cfg.Common.MetricEndpoint = MetricEndpointLocal
	}

	if cfg.Common.MetricEndpoint != MetricEndpointLocal {
		u, err := url.Parse(cfg.Common.MetricEndpoint)

		if err != nil {
			return fmt.Errorf("common.metric-endpoint parse error: %s", err.Error())
		}

		if u.Scheme != "tcp" && u.Scheme != "udp" {
			return fmt.Errorf("common.metric-endpoint supports only tcp and udp protocols. %#v is unsupported", u.Scheme)
		}
	}

	err = cfg.TagDesc.Configure()
	if err != nil {
		return err
	}

	app.Config = cfg

	return nil
}

// ParseConfig loads config from config file
func (app *App) ParseConfig() error {
	app.Lock()
	defer app.Unlock()

	return app.configure()
}

// Stop all socket listeners
func (app *App) stopListeners() {
	logger := zapwriter.Logger("app")

	if app.TCP != nil {
		app.TCP.Stop()
		app.TCP = nil
		logger.Debug("finished", zap.String("module", "tcp"))
	}

	if app.Pickle != nil {
		app.Pickle.Stop()
		app.Pickle = nil
		logger.Debug("finished", zap.String("module", "pickle"))
	}

	if app.UDP != nil {
		app.UDP.Stop()
		app.UDP = nil
		logger.Debug("finished", zap.String("module", "udp"))
	}

	if app.Grpc != nil {
		app.Grpc.Stop()
		app.Grpc = nil
		logger.Debug("finished", zap.String("module", "grpc"))
	}

	if app.Prometheus != nil {
		app.Prometheus.Stop()
		app.Prometheus = nil
		logger.Debug("finished", zap.String("module", "prometheus"))
	}

	if app.TelegrafHttpJson != nil {
		app.TelegrafHttpJson.Stop()
		app.TelegrafHttpJson = nil
		logger.Debug("finished", zap.String("module", "telegraf_http_json"))
	}
}

func (app *App) stopAll() {
	logger := zapwriter.Logger("app")

	app.stopListeners()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		logger.Debug("finished", zap.String("module", "collector"))
	}

	if app.Writer != nil {
		app.Writer.Stop()
		app.Writer = nil
		logger.Debug("finished", zap.String("module", "writer"))
	}

	if app.Uploaders != nil {
		for n, u := range app.Uploaders {
			u.Stop()
			logger.Debug("finished", zap.String("module", "uploader"), zap.String("name", n))
		}
		app.Uploaders = nil
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		logger.Debug("close(app.exit)", zap.String("module", "app"))
	}
}

// Stop force stop all components
func (app *App) Stop() {
	app.Lock()
	defer app.Unlock()
	app.stopAll()
}

// Start starts
func (app *App) Start() (err error) {
	app.Lock()
	defer app.Unlock()

	defer func() {
		if err != nil {
			app.stopAll()
		}
	}()

	conf := app.Config

	runtime.GOMAXPROCS(conf.Common.MaxCPU)

	app.writeChan = make(chan *RowBinary.WriteBuffer)

	/* WRITER start */
	uploaders := make([]string, 0, len(conf.Upload))
	for t := range conf.Upload {
		uploaders = append(uploaders, t)
	}

	conf.Data.AutoInterval.SetDefault(conf.Data.FileInterval.Value())

	if err := os.MkdirAll(conf.Data.Path, 0755); err != nil {
		return err
	}

	app.Writer = writer.New(
		app.writeChan,
		conf.Data.Path,
		conf.Data.AutoInterval,
		conf.Data.CompAlgo.CompAlgo,
		conf.Data.CompLevel,
		uploaders,
		nil,
	)
	app.Writer.Start()
	/* WRITER end */

	/* UPLOADER start */
	app.Uploaders = make(map[string]uploader.Uploader)
	for uploaderName, uploaderConfig := range conf.Upload {
		uploaderDir := filepath.Join(conf.Data.Path, uploaderName)
		if err := os.MkdirAll(uploaderDir, 0755); err != nil {
			return err
		}
		up, err := uploader.New(uploaderDir, uploaderName, uploaderConfig)
		if err != nil {
			return err
		}
		app.Uploaders[uploaderName] = up

		// debug cache dump
		if dumper, ok := up.(uploader.DebugCacheDumper); ok {
			func(uploaderName string, d uploader.DebugCacheDumper) {
				http.HandleFunc(fmt.Sprintf("/debug/upload/%s/cache/", uploaderName), func(w http.ResponseWriter, r *http.Request) {
					d.CacheDump(w)
				})
			}(uploaderName, dumper)
		}
	}

	for _, uploader := range app.Uploaders {
		uploader.Start()
	}
	/* UPLOADER end */

	/* RECEIVER start */
	if conf.Tcp.Enabled {
		app.TCP, err = receiver.New(
			"tcp://"+conf.Tcp.Listen,
			app.Config.TagDesc,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.Tcp.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.Tcp.DropPast.Value().Seconds())),
			receiver.ReadTimeout(uint32(conf.Tcp.ReadTimeout.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/tcp/dropped/", app.TCP.DroppedHandler)
	}

	if conf.Udp.Enabled {
		app.UDP, err = receiver.New(
			"udp://"+conf.Udp.Listen,
			app.Config.TagDesc,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.Udp.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.Udp.DropPast.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/udp/dropped/", app.UDP.DroppedHandler)
	}

	if conf.Pickle.Enabled {
		app.Pickle, err = receiver.New(
			"pickle://"+conf.Pickle.Listen,
			app.Config.TagDesc,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.Pickle.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.Pickle.DropPast.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/pickle/dropped/", app.Pickle.DroppedHandler)
	}

	if conf.Grpc.Enabled {
		app.Grpc, err = receiver.New(
			"grpc://"+conf.Grpc.Listen,
			app.Config.TagDesc,
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.Grpc.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.Grpc.DropPast.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/grpc/dropped/", app.Grpc.DroppedHandler)
	}

	if conf.Prometheus.Enabled {
		app.Prometheus, err = receiver.New(
			"prometheus://"+conf.Prometheus.Listen,
			app.Config.TagDesc,
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.Prometheus.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.Prometheus.DropPast.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/prometheus/dropped/", app.Prometheus.DroppedHandler)
	}

	if conf.TelegrafHttpJson.Enabled {
		app.TelegrafHttpJson, err = receiver.New(
			"telegraf+http+json://"+conf.TelegrafHttpJson.Listen,
			app.Config.TagDesc,
			receiver.WriteChan(app.writeChan),
			receiver.DropFuture(uint32(conf.TelegrafHttpJson.DropFuture.Value().Seconds())),
			receiver.DropPast(uint32(conf.TelegrafHttpJson.DropPast.Value().Seconds())),
		)

		if err != nil {
			return
		}

		http.HandleFunc("/debug/receive/telegraf_http_json/dropped/", app.TelegrafHttpJson.DroppedHandler)
	}
	/* RECEIVER end */

	/* COLLECTOR start */
	app.Collector = NewCollector(app)
	/* COLLECTOR end */

	return
}

// Reset cache in uploaders
func (app *App) Reset() {
	logger := zapwriter.Logger("app")
	logger.Info("HUP received")

	for n, u := range app.Uploaders {
		if v, ok := u.(uploader.UploaderWithReset); ok {
			logger.Info("reset cache", zap.String("module", "uploader"), zap.String("name", n))
			go v.Reset()
		}
	}
}

// Loop ...
func (app *App) Loop() {
	app.RLock()
	exitChan := app.exit
	app.RUnlock()

	if exitChan != nil {
		<-app.exit
	}
}
