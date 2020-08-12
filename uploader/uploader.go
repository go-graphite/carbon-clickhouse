package uploader

import (
	"fmt"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type Uploader interface {
	Start() error
	Stop()
	Stat(send func(metric string, value float64))
}

type UploaderWithReset interface {
	Reset()
}

func New(path string, name string, config *Config) (Uploader, error) {
	c := *config

	if c.Threads < 1 {
		c.Threads = 1
	}

	logger := zapwriter.Logger("upload").With(zap.String("name", name))
	u := &Base{
		path:    path,
		name:    name,
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &c,
	}

	if c.Type != "points" && c.Type != "points-reverse" && len(c.IgnoredPatterns) > 0 {
		logger.Warn(fmt.Sprintf("IgnoredPatterns are supported for points and points-reverse only, not for %s", c.Type))
	}

	if c.Type != "tagged" && len(c.IgnoredTaggedMetrics) > 0 {
		logger.Warn(fmt.Sprintf("IgnoredTaggedMetrics are supported for tagged only, not for %s", c.Type))
	}

	var res Uploader

	switch c.Type {
	case "points":
		res = NewPoints(u, false)
	case "tree":
		res = NewTree(u)
	case "points-reverse":
		res = NewPoints(u, true)
	case "series":
		res = NewSeries(u, false)
	case "series-reverse":
		res = NewSeries(u, true)
	case "tagged":
		res = NewTagged(u)
	case "index":
		res = NewIndex(u)
	default:
		return nil, fmt.Errorf("unknown uploader type %#v", c.Type)
	}

	return res, nil
}
