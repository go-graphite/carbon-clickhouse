package receiver

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"github.com/lomik/zapwriter"
)

type Receiver interface {
	Stat(func(metric string, value float64))
	DroppedHandler(w http.ResponseWriter, r *http.Request)
	Stop()
}

type Option func(interface{}) error

// WriteChan creates option for New contructor
func WriteChan(ch chan *RowBinary.WriteBuffer) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.writeChan = ch
		}
		return nil
	}
}

// ParseThreads creates option for New contructor
func ParseThreads(threads int) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.parseThreads = threads
		}
		return nil
	}
}

// DropFuture creates option for New contructor
func DropFuture(seconds uint32) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.dropFutureSeconds = seconds
		}
		return nil
	}
}

// DropPast creates option for New contructor
func DropPast(seconds uint32) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.dropPastSeconds = seconds
		}
		return nil
	}
}

// DropLongerThan creates option for New constructor
func DropLongerThan(maximumLength uint16) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.dropTooLongLimit = maximumLength
		}
		return nil
	}
}

// ReadTimeout creates option for New constructor
func ReadTimeout(seconds uint32) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.readTimeoutSeconds = seconds
		}
		return nil
	}
}

// ConcatChar creates option for New constructor
func ConcatChar(concat string) Option {
	return func(r interface{}) error {
		if t, ok := r.(*Base); ok {
			t.concatCharacter = concat
		}
		return nil
	}
}

// New creates udp, tcp, pickle receiver
func New(dsn string, config tags.TagConfig, opts ...Option) (Receiver, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	logger := zapwriter.Logger(strings.Replace(u.Scheme, "+", "_", -1))

	if u.Scheme == "tcp" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TCP{
			parseChan: make(chan *Buffer),
		}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err

	} else if u.Scheme == "pickle" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &Pickle{
			parseChan: make(chan []byte),
		}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err

	} else if u.Scheme == "udp" {
		addr, err := net.ResolveUDPAddr("udp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &UDP{
			parseChan: make(chan *Buffer),
		}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err

	} else if u.Scheme == "grpc" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &GRPC{}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err

	} else if u.Scheme == "prometheus" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &PrometheusRemoteWrite{}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err

	} else if u.Scheme == "telegraf+http+json" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TelegrafHttpJson{}
		r.Init(logger, config, opts...)

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	return nil, fmt.Errorf("unknown proto %#v", u.Scheme)
}
