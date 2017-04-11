package receiver

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/framing"
	"github.com/lomik/stop"
	"github.com/uber-go/zap"
)

const maxPickleMessageSize = 67108864

// Pickle receive metrics from TCP connections
type Pickle struct {
	stop.Struct
	stat struct {
		messagesReceived uint32 // atomic
		metricsReceived  uint32 // atomic
		errors           uint32 // atomic
		active           int32  // atomic
	}
	listener     *net.TCPListener
	parseThreads int
	parseChan    chan []byte
	writeChan    chan *RowBinary.WriteBuffer
	logger       zap.Logger
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *Pickle) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *Pickle) Stat(send func(metric string, value float64)) {
	metricsReceived := atomic.LoadUint32(&rcv.stat.metricsReceived)
	atomic.AddUint32(&rcv.stat.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	messagesReceived := atomic.LoadUint32(&rcv.stat.messagesReceived)
	atomic.AddUint32(&rcv.stat.messagesReceived, -messagesReceived)
	send("messagesReceived", float64(messagesReceived))

	errors := atomic.LoadUint32(&rcv.stat.errors)
	atomic.AddUint32(&rcv.stat.errors, -errors)
	send("errors", float64(errors))

	send("active", float64(atomic.LoadInt32(&rcv.stat.active)))
}

func (rcv *Pickle) HandleConnection(conn net.Conn) {
	framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
	defer func() {
		if r := recover(); r != nil {
			rcv.logger.Error("panic recovered", zap.String("traceback", fmt.Sprint(r)))
		}
	}()

	atomic.AddInt32(&rcv.stat.active, 1)
	defer atomic.AddInt32(&rcv.stat.active, -1)

	defer conn.Close()

	finished := make(chan bool)
	defer close(finished)

	rcv.Go(func(exit chan struct{}) {
		select {
		case <-finished:
			return
		case <-exit:
			conn.Close()
			return
		}
	})

	framedConn.MaxFrameSize = uint(maxPickleMessageSize)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		data, err := framedConn.ReadFrame()
		if err == framing.ErrPrefixLength {
			atomic.AddUint32(&rcv.stat.errors, 1)
			rcv.logger.Warn("bad message size")
			return
		} else if err != nil {
			atomic.AddUint32(&rcv.stat.errors, 1)
			rcv.logger.Warn("can't read message body", zap.Error(err))
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

		rcv.Go(func(exit chan struct{}) {
			<-exit
			tcpListener.Close()
		})

		handler := rcv.HandleConnection

		rcv.Go(func(exit chan struct{}) {
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

				rcv.Go(func(exit chan struct{}) {
					handler(conn)
				})
			}

		})

		for i := 0; i < rcv.parseThreads; i++ {
			rcv.Go(func(exit chan struct{}) {
				PickleParser(
					exit,
					rcv.parseChan,
					rcv.writeChan,
					&rcv.stat.metricsReceived,
					&rcv.stat.errors,
				)
			})
		}

		rcv.listener = tcpListener

		return nil
	})
}
