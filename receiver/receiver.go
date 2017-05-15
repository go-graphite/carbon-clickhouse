package receiver

import (
	"fmt"
	"net"
	"net/url"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/zapwriter"
)

type Receiver interface {
	Stat(func(metric string, value float64))
	Stop()
}

type Option func(Receiver) error

// WriteChan creates option for New contructor
func WriteChan(ch chan *RowBinary.WriteBuffer) Option {
	return func(r Receiver) error {
		if t, ok := r.(*TCP); ok {
			t.writeChan = ch
		}
		if t, ok := r.(*Pickle); ok {
			t.writeChan = ch
		}
		if t, ok := r.(*UDP); ok {
			t.writeChan = ch
		}
		return nil
	}
}

// ParseThreads creates option for New contructor
func ParseThreads(threads int) Option {
	return func(r Receiver) error {
		if t, ok := r.(*TCP); ok {
			t.parseThreads = threads
		}
		if t, ok := r.(*Pickle); ok {
			t.parseThreads = threads
		}
		if t, ok := r.(*UDP); ok {
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

	if u.Scheme == "tcp" {
		addr, err := net.ResolveTCPAddr("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		r := &TCP{
			parseChan: make(chan *Buffer),
			logger:    zapwriter.Logger("tcp"),
		}

		for _, optApply := range opts {
			optApply(r)
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
			parseChan: make(chan []byte),
			logger:    zapwriter.Logger("pickle"),
		}

		for _, optApply := range opts {
			optApply(r)
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
			parseChan: make(chan *Buffer),
			logger:    zapwriter.Logger("udp"),
		}

		for _, optApply := range opts {
			optApply(r)
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	return nil, fmt.Errorf("unknown proto %#v", u.Scheme)
}
