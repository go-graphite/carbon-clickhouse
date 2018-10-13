package receiver

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/graphite-pickle/framing"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const maxPickleMessageSize = 67108864

// Pickle receive metrics from TCP connections
type Pickle struct {
	Base
	listener  *net.TCPListener
	parseChan chan []byte
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *Pickle) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *Pickle) Stat(send func(metric string, value float64)) {
	rcv.SendStat(send, "metricsReceived", "messagesReceived", "errors", "active", "futureDropped", "pastDropped")
}

func (rcv *Pickle) HandleConnection(conn net.Conn) {
	framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
	defer func() {
		if r := recover(); r != nil {
			rcv.logger.Error("panic recovered", zap.String("traceback", fmt.Sprint(r)))
		}
	}()

	atomic.AddInt64(&rcv.stat.active, 1)
	defer atomic.AddInt64(&rcv.stat.active, -1)

	defer conn.Close()

	finished := make(chan bool)
	defer close(finished)

	rcv.Go(func(ctx context.Context) {
		select {
		case <-finished:
			return
		case <-ctx.Done():
			conn.Close()
			return
		}
	})

	framedConn.MaxFrameSize = uint(maxPickleMessageSize)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		data, err := framedConn.ReadFrame()
		if err == framing.ErrPrefixLength {
			atomic.AddUint64(&rcv.stat.errors, 1)
			rcv.logger.Warn("bad message size")
			return
		} else if err != nil {
			if err != io.EOF {
				atomic.AddUint64(&rcv.stat.errors, 1)
				rcv.logger.Warn("can't read message body", zap.Error(err))
			}
			return
		}

		rcv.parseChan <- data
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *Pickle) Listen(addr *net.TCPAddr) error {
	return rcv.StartFunc(func() error {

		tcpListener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		rcv.Go(func(ctx context.Context) {
			<-ctx.Done()
			tcpListener.Close()
		})

		handler := rcv.HandleConnection

		rcv.Go(func(ctx context.Context) {
			defer tcpListener.Close()

			for {

				conn, err := tcpListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					rcv.logger.Warn("failed to accept connection", zap.Error(err))
					continue
				}

				rcv.Go(func(ctx context.Context) {
					handler(conn)
				})
			}

		})

		for i := 0; i < rcv.parseThreads; i++ {
			rcv.Go(func(ctx context.Context) {
				rcv.PickleParser(rcv.parseChan)
			})
		}

		rcv.listener = tcpListener

		return nil
	})
}
