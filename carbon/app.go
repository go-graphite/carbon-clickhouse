package carbon

import (
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/lomik/carbon-clickhouse/receiver"
	"github.com/lomik/carbon-clickhouse/uploader"
	"github.com/lomik/carbon-clickhouse/writer"
	"github.com/uber-go/zap"
)

type App struct {
	sync.RWMutex
	ConfigFilename string
	Config         *Config
	Writer         *writer.Writer
	Uploader       *uploader.Uploader
	// UDP            receiver.Receiver
	TCP receiver.Receiver
	// Pickle         receiver.Receiver
	Collector *Collector // (!!!) Should be re-created on every change config/modules
	writeChan chan *receiver.WriteBuffer
	exit      chan bool
	logger    zap.Logger
}

// New App instance
func New(configFilename string, logger zap.Logger) *App {
	app := &App{
		ConfigFilename: configFilename,
		Config:         NewConfig(),
		exit:           make(chan bool),
		logger:         logger,
	}
	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure() error {
	var err error

	cfg := NewConfig()
	if err = ParseConfig(app.ConfigFilename, cfg); err != nil {
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

	app.Config = cfg

	return nil
}

// ParseConfig loads config from config file, schemas.conf, aggregation.conf
func (app *App) ParseConfig() error {
	app.Lock()
	defer app.Unlock()

	return app.configure()
}

// ReloadConfig reloads some settings from config
func (app *App) ReloadConfig() error {
	app.Lock()
	defer app.Unlock()

	var err error
	if err = app.configure(); err != nil {
		return err
	}

	// TODO: reload something?

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}

	app.Collector = NewCollector(app)

	return nil
}

// Stop all socket listeners
func (app *App) stopListeners() {
	if app.TCP != nil {
		app.TCP.Stop()
		app.TCP = nil
		logrus.Debug("[tcp] finished")
	}

	// if app.Pickle != nil {
	// 	app.Pickle.Stop()
	// 	app.Pickle = nil
	// 	logrus.Debug("[pickle] finished")
	// }

	// if app.UDP != nil {
	// 	app.UDP.Stop()
	// 	app.UDP = nil
	// 	logrus.Debug("[udp] finished")
	// }
}

func (app *App) stopAll() {
	app.stopListeners()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		logrus.Debug("[stat] finished")
	}

	if app.Writer != nil {
		app.Writer.Stop()
		app.Writer = nil
		logrus.Debug("[writer] finished")
	}

	if app.Uploader != nil {
		app.Uploader.Stop()
		app.Uploader = nil
		logrus.Debug("[uploader] finished")
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		logrus.Debug("[app] close(exit)")
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

	app.writeChan = make(chan *receiver.WriteBuffer, 128)

	/* WRITER start */
	app.Writer = writer.New(
		app.writeChan,
		conf.Data.Path,
		conf.Data.FileInterval.Value(),
	)
	app.Writer.Start()
	/* WRITER end */

	/* UPLOADER start */
	app.Uploader = uploader.New(
		uploader.Path(conf.Data.Path),
		uploader.ClickHouse(conf.ClickHouse.Url),
		uploader.DataTable(conf.ClickHouse.DataTable),
		uploader.DataTimeout(conf.ClickHouse.DataTimeout.Value()),
		uploader.TreeTable(conf.ClickHouse.TreeTable),
		uploader.TreeDate(conf.ClickHouse.TreeDate),
		uploader.TreeTimeout(conf.ClickHouse.TreeTimeout.Value()),
		uploader.InProgressCallback(app.Writer.IsInProgress),
		uploader.Threads(app.Config.ClickHouse.Threads),
		uploader.Logger(app.logger.With(zap.String("module", "upload"))),
	)
	app.Uploader.Start()
	/* UPLOADER end */

	/* UDP start */
	// if conf.Udp.Enabled {
	// 	app.UDP, err = receiver.New(
	// 		"udp://"+conf.Udp.Listen,
	// 		receiver.OutChan(app.input),
	// 		receiver.UDPLogIncomplete(conf.Udp.LogIncomplete),
	// 	)

	// 	if err != nil {
	// 		return
	// 	}
	// }
	/* UDP end */

	/* TCP start */
	if conf.Tcp.Enabled {
		app.TCP, err = receiver.New(
			"tcp://"+conf.Tcp.Listen,
			receiver.ParseThreads(runtime.GOMAXPROCS(-1)*2),
			receiver.WriteChan(app.writeChan),
		)

		if err != nil {
			return
		}
	}
	/* TCP end */

	/* PICKLE start */
	// if conf.Pickle.Enabled {
	// 	app.Pickle, err = receiver.New(
	// 		"pickle://"+conf.Pickle.Listen,
	// 		receiver.OutChan(app.input),
	// 		receiver.PickleMaxMessageSize(uint32(conf.Pickle.MaxMessageSize)),
	// 	)

	// 	if err != nil {
	// 		return
	// 	}
	// }
	/* PICKLE end */

	/* COLLECTOR start */
	app.Collector = NewCollector(app)
	/* COLLECTOR end */

	return
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
