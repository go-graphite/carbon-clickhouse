package carbon

import (
	"fmt"
	"time"

	"github.com/lomik/stop"
	"github.com/uber-go/zap"
)

type statFunc func()

type statModule interface {
	Stat(send func(metric string, value float64))
}

type Collector struct {
	stop.Struct
	graphPrefix    string
	metricInterval time.Duration
	endpoint       string
	stats          []statFunc
	logger         zap.Logger
}

func NewCollector(app *App) *Collector {
	// app locked by caller

	c := &Collector{
		graphPrefix:    app.Config.Common.MetricPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		endpoint:       app.Config.Common.MetricEndpoint,
		stats:          make([]statFunc, 0),
		logger:         app.logger.With(zap.String("module", "collector")),
		// data:           make(chan *points.Points, 4096),
	}

	c.Start()

	sendCallback := func(moduleName string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, moduleName, metric)
			c.logger.Info("stat", zap.String("key", key), zap.Float64("value", value))
			// select {
			// case c.data <- points.NowPoint(key, value):
			// 	// pass
			// default:
			// 	logrus.WithField("key", key).WithField("value", value).
			// 		Warn("[stat] send queue is full. Metric dropped")
			// }
		}
	}

	moduleCallback := func(moduleName string, moduleObj statModule) statFunc {
		return func() {
			moduleObj.Stat(sendCallback(moduleName))
		}
	}

	// if app.UDP != nil {
	// 	c.stats = append(c.stats, moduleCallback("udp", app.UDP))
	// }

	if app.TCP != nil {
		c.stats = append(c.stats, moduleCallback("tcp", app.TCP))
	}

	// if app.Pickle != nil {
	// 	c.stats = append(c.stats, moduleCallback("pickle", app.Pickle))
	// }

	// collector worker
	c.Go(func(exit chan struct{}) {
		ticker := time.NewTicker(c.metricInterval)
		defer ticker.Stop()

		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				c.collect()
			}
		}
	})

	return c
}

func (c *Collector) collect() {
	for _, stat := range c.stats {
		stat()
	}
}
