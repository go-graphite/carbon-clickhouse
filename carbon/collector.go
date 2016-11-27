package carbon

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/stop"
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
	data           chan *points.Points
	stats          []statFunc
}

func NewCollector(app *App) *Collector {
	// app locked by caller

	c := &Collector{
		graphPrefix:    app.Config.Common.MetricPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		// data:           make(chan *points.Points, 4096),
		endpoint: app.Config.Common.MetricEndpoint,
		stats:    make([]statFunc, 0),
	}

	c.Start()

	sendCallback := func(moduleName string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, moduleName, metric)
			logrus.Infof("[stat] %s=%#v", key, value)
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
	c.Go(func(exit chan bool) {
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
