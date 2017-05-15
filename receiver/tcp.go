package receiver

import (
	"bytes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/stop"
	"go.uber.org/zap"
)

// TCP receive metrics from TCP connections
type TCP struct {
	stop.Struct
	stat struct {
		metricsReceived uint32 // atomic
		errors          uint32 // atomic
		active          int32  // atomic
	}
	listener     *net.TCPListener
	parseThreads int
	parseChan    chan *Buffer
	writeChan    chan *RowBinary.WriteBuffer
	logger       *zap.Logger
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TCP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TCP) Stat(send func(metric string, value float64)) {
	metricsReceived := atomic.LoadUint32(&rcv.stat.metricsReceived)
	atomic.AddUint32(&rcv.stat.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.stat.errors)
	atomic.AddUint32(&rcv.stat.errors, -errors)
	send("errors", float64(errors))

	send("active", float64(atomic.LoadInt32(&rcv.stat.active)))
}

func (rcv *TCP) HandleConnection(conn net.Conn) {
	atomic.AddInt32(&rcv.stat.active, 1)
	defer atomic.AddInt32(&rcv.stat.active, -1)

	defer conn.Close()

	logger := rcv.logger.With(zap.String("peer", conn.RemoteAddr().String()))

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

	buffer := GetBuffer()

	var n int
	var err error

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		n, err = conn.Read(buffer.Body[buffer.Used:])
		conn.SetDeadline(time.Time{})
		buffer.Used += n
		buffer.Time = uint32(time.Now().Unix())

		if err != nil {
			if err == io.EOF {
				if buffer.Used > 0 {
					logger.Warn("unfinished line", zap.String("line", string(buffer.Body[:buffer.Used])))
				}
			} else {
				atomic.AddUint32(&rcv.stat.errors, 1)
				logger.Error("read failed", zap.Error(err))
			}
			break
		}

		chunkSize := bytes.LastIndexByte(buffer.Body[:buffer.Used], '\n') + 1

		if chunkSize > 0 {
			newBuffer := GetBuffer()

			if chunkSize < buffer.Used { // has unfinished data
				copy(newBuffer.Body[:], buffer.Body[chunkSize:buffer.Used])
				newBuffer.Used = buffer.Used - chunkSize
				buffer.Used = chunkSize
			}

			rcv.parseChan <- buffer
			buffer = newBuffer
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TCP) Listen(addr *net.TCPAddr) error {
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
				PlainParser(
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
