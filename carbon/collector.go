package carbon

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/stop"
	"github.com/lomik/zapwriter"
)

type statFunc func()

type statModule interface {
	Stat(send func(metric string, value float64))
}

type Point struct {
	Metric    string
	Value     float64
	Timestamp uint32
}

type Collector struct {
	stop.Struct
	graphPrefix    string
	metricInterval time.Duration
	endpoint       string
	stats          []statFunc
	logger         *zap.Logger
	data           chan *Point
	writeChan      chan *RowBinary.WriteBuffer
}

func NewCollector(app *App) *Collector {
	// app locked by caller

	c := &Collector{
		graphPrefix:    app.Config.Common.MetricPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		endpoint:       app.Config.Common.MetricEndpoint,
		stats:          make([]statFunc, 0),
		logger:         zapwriter.Logger("stat"),
		data:           make(chan *Point, 4096),
		writeChan:      app.writeChan,
	}

	c.Start()

	sendCallback := func(moduleName string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, moduleName, metric)

			c.logger.Info("stat", zap.String("metric", key), zap.Float64("value", value))

			select {
			case c.data <- &Point{Metric: key, Value: value, Timestamp: uint32(time.Now().Unix())}:
				// pass
			default:
				c.logger.Warn(
					"send queue is full. metric dropped",
					zap.String("metric", key),
					zap.Float64("value", value),
				)
			}
		}
	}

	moduleCallback := func(moduleName string, moduleObj statModule) statFunc {
		return func() {
			moduleObj.Stat(sendCallback(moduleName))
		}
	}

	if app.Writer != nil {
		c.stats = append(c.stats, moduleCallback("writer", app.Writer))
	}

	if app.TCP != nil {
		c.stats = append(c.stats, moduleCallback("tcp", app.TCP))
	}

	if app.Pickle != nil {
		c.stats = append(c.stats, moduleCallback("pickle", app.Pickle))
	}

	if app.UDP != nil {
		c.stats = append(c.stats, moduleCallback("udp", app.UDP))
	}

	if app.Prometheus != nil {
		c.stats = append(c.stats, moduleCallback("prometheus", app.Prometheus))
	}

	for n, u := range app.Uploaders {
		c.stats = append(c.stats, moduleCallback(fmt.Sprintf("upload.%s", n), u))
	}

	var u *url.URL
	var err error

	if c.endpoint == "" {
		c.endpoint = MetricEndpointLocal
	}

	if c.endpoint != MetricEndpointLocal {
		u, err = url.Parse(c.endpoint)

		if err != nil || !(u.Scheme == "tcp" || u.Scheme == "udp") {
			c.logger.Error("metric-endpoint parse error, using \"local\"",
				zap.Error(err),
				zap.String("metric-endpoint", c.endpoint),
			)
			c.endpoint = MetricEndpointLocal
		}
	}

	c.logger = c.logger.With(zap.String("endpoint", c.endpoint))

	if c.endpoint == MetricEndpointLocal {
		c.Go(c.local)
	} else {
		c.Go(func(exit chan struct{}) {
			c.remote(exit, u)
		})
	}

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

func (c *Collector) readData(exit chan struct{}) []*Point {
	result := make([]*Point, 0)

	for {
		// wait for first point
		select {
		case <-exit:
			return result
		case p := <-c.data:
			result = append(result, p)

			// read all
			for {
				select {
				case <-exit:
					return result
				case p := <-c.data:
					result = append(result, p)
				default:
					return result
				}
			}
		}
	}

	return result
}

func (c *Collector) local(exit chan struct{}) {
	for {
		points := c.readData(exit)
		if points == nil || len(points) == 0 {
			// exit closed
			return
		}
		now := uint32(time.Now().Unix())

		b := RowBinary.GetWriteBuffer()

		for _, p := range points {
			if !b.CanWriteGraphitePoint(len(p.Metric)) {
				// buffer is full
				select {
				case <-exit:
					return
				case c.writeChan <- b:
					// pass
				}

				b = RowBinary.GetWriteBuffer()
			}

			b.WriteGraphitePoint(
				[]byte(p.Metric),
				p.Value,
				p.Timestamp,
				now,
			)
		}

		select {
		case <-exit:
			return
		case c.writeChan <- b:
			// pass
		}
	}
}

func (c *Collector) chunked(exit chan struct{}, chunkSize int, callback func([]byte)) {
	for {
		points := c.readData(exit)
		if points == nil || len(points) == 0 {
			// exit closed
			return
		}

		buf := bytes.NewBuffer(nil)

		for _, p := range points {
			s := fmt.Sprintf("%s %v %d\n", p.Metric, p.Value, p.Timestamp)

			if buf.Len()+len(s) > chunkSize {
				callback(buf.Bytes())
				buf = bytes.NewBuffer(nil)
			}

			buf.Write([]byte(s))
		}

		callback(buf.Bytes())
	}
}

func (c *Collector) remote(exit chan struct{}, u *url.URL) {

	chunkSize := 32768
	if u.Scheme == "udp" {
		chunkSize = 1000 // nc limitation (1024 for udp) and mtu friendly
	}

	c.chunked(exit, chunkSize, func(chunk []byte) {

		var conn net.Conn
		var err error
		defaultTimeout := 5 * time.Second

		defer func() {
			if conn != nil {
				conn.Close()
				conn = nil
			}
		}()

	SendLoop:
		for {

			// check exit
			select {
			case <-exit:
				break SendLoop
			default:
				// pass
			}

			// close old broken connection
			if conn != nil {
				conn.Close()
				conn = nil
			}

			conn, err = net.DialTimeout(u.Scheme, u.Host, defaultTimeout)
			if err != nil {
				c.logger.Warn("dial failed", zap.Error(err))
				time.Sleep(time.Second)
				continue SendLoop
			}

			err = conn.SetDeadline(time.Now().Add(defaultTimeout))
			if err != nil {
				c.logger.Warn("conn.SetDeadline failed", zap.Error(err))
				time.Sleep(time.Second)
				continue SendLoop
			}

			_, err := conn.Write(chunk)
			if err != nil {
				c.logger.Warn("conn.Write failed", zap.Error(err))
				time.Sleep(time.Second)
				continue SendLoop
			}

			break SendLoop
		}
	})
}

func (c *Collector) collect() {
	for _, stat := range c.stats {
		stat()
	}
}
