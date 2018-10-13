package receiver

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/zapwriter"
)

type Receiver interface {
	Stat(func(metric string, value float64))
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

// New creates udp, tcp, pickle receiver
func New(dsn string, opts ...Option) (Receiver, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	base := NewBase(zapwriter.Logger(strings.Replace(u.Scheme, "+", "_", -1)))

	for _, optApply := range opts {
		optApply(&base)
	}

	if u.Scheme == "tcp" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TCP{
			Base:      base,
			parseChan: make(chan *Buffer),
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "pickle" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &Pickle{
			Base:      base,
			parseChan: make(chan []byte),
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "udp" {
		addr, err := net.ResolveUDPAddr("udp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &UDP{
			Base:      base,
			parseChan: make(chan *Buffer),
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "grpc" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &GRPC{
			Base: base,
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "prometheus" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &PrometheusRemoteWrite{
			Base: base,
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	if u.Scheme == "telegraf+http+json" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TelegrafHttpJson{
			Base: base,
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	return nil, fmt.Errorf("unknown proto %#v", u.Scheme)
}
