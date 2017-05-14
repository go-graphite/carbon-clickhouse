package carbon

import (
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/uber-go/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/receiver"
	"github.com/lomik/carbon-clickhouse/uploader"
	"github.com/lomik/carbon-clickhouse/writer"
)

type App struct {
	sync.RWMutex
	Config    *Config
	Writer    *writer.Writer
	Uploader  *uploader.Uploader
	UDP       receiver.Receiver
	TCP       receiver.Receiver
	Pickle    receiver.Receiver
	Collector *Collector // (!!!) Should be re-created on every change config/modules
	writeChan chan *RowBinary.WriteBuffer
	exit      chan bool
	logger    zap.Logger
}

// New App instance
func New(cfg *Config, logger zap.Logger) (*App, error) {
	app := &App{
		exit:   make(chan bool),
		logger: logger,
	}

	if err := app.configure(cfg); err != nil {
		return nil, err
	}
	return app, nil
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure(cfg *Config) error {
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

	app.Config = cfg

	return nil
}

// // ReloadConfig reloads some settings from config
// func (app *App) ReloadConfig() error {
// 	app.Lock()
// 	defer app.Unlock()

// 	var err error
// 	if err = app.configure(); err != nil {
// 		return err
// 	}

// 	// TODO: reload something?

// 	if app.Collector != nil {
// 		app.Collector.Stop()
// 		app.Collector = nil
// 	}

// 	app.Collector = NewCollector(app)

// 	return nil
// }

// Stop all socket listeners
func (app *App) stopListeners() {
	if app.TCP != nil {
		app.TCP.Stop()
		app.TCP = nil
		app.logger.Debug("finished", zap.String("module", "tcp"))
	}

	if app.Pickle != nil {
		app.Pickle.Stop()
		app.Pickle = nil
		app.logger.Debug("finished", zap.String("module", "pickle"))
	}

	if app.UDP != nil {
		app.UDP.Stop()
		app.UDP = nil
		app.logger.Debug("finished", zap.String("module", "udp"))
	}
}

func (app *App) stopAll() {
	app.stopListeners()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		app.logger.Debug("finished", zap.String("module", "collector"))
	}

	if app.Writer != nil {
		app.Writer.Stop()
		app.Writer = nil
		app.logger.Debug("finished", zap.String("module", "writer"))
	}

	if app.Uploader != nil {
		app.Uploader.Stop()
		app.Uploader = nil
		app.logger.Debug("finished", zap.String("module", "uploader"))
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		app.logger.Debug("close(app.exit)", zap.String("module", "app"))
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
	app.Writer = writer.New(
		app.writeChan,
		conf.Data.Path,
		conf.Data.FileInterval.Value(),
		app.logger.With(zap.String("module", "writer")),
	)
	app.Writer.Start()
	/* WRITER end */

	/* UPLOADER start */
	dataTables := conf.ClickHouse.DataTables
	if dataTables == nil {
		dataTables = make([]string, 0)
	}

	if conf.ClickHouse.DataTable != "" {
		exists := false
		for i := 0; i < len(dataTables); i++ {
			if dataTables[i] == conf.ClickHouse.DataTable {
				exists = true
			}
		}

		if !exists {
			dataTables = append(dataTables, conf.ClickHouse.DataTable)
		}
	}

	app.Uploader = uploader.New(
		uploader.Path(conf.Data.Path),
		uploader.ClickHouse(conf.ClickHouse.Url),
		uploader.DataTables(dataTables),
		uploader.DataTimeout(conf.ClickHouse.DataTimeout.Value()),
		uploader.TreeTable(conf.ClickHouse.TreeTable),
		uploader.TreeDate(conf.ClickHouse.TreeDate),
		uploader.TreeTimeout(conf.ClickHouse.TreeTimeout.Value()),
		uploader.InProgressCallback(app.Writer.IsInProgress),
		uploader.Threads(app.Config.ClickHouse.Threads),
		uploader.Logger(app.logger.With(zap.String("module", "uploader"))),
	)
	app.Uploader.Start()
	/* UPLOADER end */

	/* RECEIVER start */
	if conf.Tcp.Enabled {
		app.TCP, err = receiver.New(
			"tcp://"+conf.Tcp.Listen,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.Logger(app.logger.With(zap.String("module", "tcp"))),
		)

		if err != nil {
			return
		}
	}

	if conf.Udp.Enabled {
		app.UDP, err = receiver.New(
			"udp://"+conf.Udp.Listen,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.Logger(app.logger.With(zap.String("module", "udp"))),
		)

		if err != nil {
			return
		}
	}

	if conf.Pickle.Enabled {
		app.Pickle, err = receiver.New(
			"pickle://"+conf.Pickle.Listen,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
			receiver.Logger(app.logger.With(zap.String("module", "pickle"))),
		)

		if err != nil {
			return
		}
	}
	/* RECEIVER end */

	/* COLLECTOR start */
	app.Collector = NewCollector(app)
	/* COLLECTOR end */

	return
}

// ClearTreeExistsCache in Uploader
func (app *App) ClearTreeExistsCache() {
	app.Lock()
	up := app.Uploader
	app.Unlock()

	if up != nil {
		go up.ClearTreeExistsCache()
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
