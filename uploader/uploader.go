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

	u := &Base{
		path:    path,
		name:    name,
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  zapwriter.Logger("upload").With(zap.String("name", name)),
		config:  &c,
	}

	var res Uploader

	switch c.Type {
	case "points":
		res = NewPoints(u)
	case "tree":
		res = NewTree(u)
	case "points-reverse":
		res = NewPointsReverse(u)
	case "series":
		res = NewSeries(u)
	case "series-reverse":
		res = NewSeriesReverse(u)
	default:
		return nil, fmt.Errorf("unknown uploader type %#v", c.Type)
	}

	return res, nil
}
