package receiver

import (
	"fmt"
	"net"
	"net/url"
)

type Receiver interface {
	Stat(func(metric string, value float64))
	Stop()
}

type Option func(Receiver) error

// OutFunc creates option for New contructor
func HandleBuffer(handler func(*Buffer)) Option {
	return func(r Receiver) error {
		if t, ok := r.(*TCP); ok {
			t.bufferHandler = handler
		}
		// if t, ok := r.(*UDP); ok {
		// 	t.out = out
		// }
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
			bufferHandler: func(b *Buffer) { BufferPool.Put(b) },
			name:          u.Scheme,
		}

		for _, optApply := range opts {
			optApply(r)
		}

		if err = r.Listen(addr); err != nil {
			return nil, err
		}

		return r, err
	}

	// if u.Scheme == "udp" {
	// 	addr, err := net.ResolveUDPAddr("udp", u.Host)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	r := &UDP{
	// 		out:  blackhole,
	// 		name: u.Scheme,
	// 	}

	// 	for _, optApply := range opts {
	// 		optApply(r)
	// 	}

	// 	err = r.Listen(addr)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	return r, err
	// }

	return nil, fmt.Errorf("unknown proto %#v", u.Scheme)
}
