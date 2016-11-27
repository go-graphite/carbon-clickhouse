package receiver

import (
	"bytes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/stop"
	"github.com/uber-go/zap"
)

// TCP receive metrics from TCP connections
type TCP struct {
	stop.Struct
	name            string // name for store metrics
	metricsReceived uint32
	errors          uint32
	active          int32 // counter
	listener        *net.TCPListener
	parseThreads    int
	parseChan       chan *Buffer
	writeChan       chan *WriteBuffer
	logger          zap.Logger
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TCP) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TCP) Stat(send func(metric string, value float64)) {

}

func (rcv *TCP) HandleConnection(conn net.Conn) {
	atomic.AddInt32(&rcv.active, 1)
	defer atomic.AddInt32(&rcv.active, -1)

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

	buffer := BufferPool.Get().(*Buffer)
	buffer.Used = 0

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
					logrus.Warningf("[tcp] Unfinished line: %#v", string(buffer.Body[:buffer.Used]))
				}
			} else {
				atomic.AddUint32(&rcv.errors, 1)
				logrus.Error(err)
			}
			break
		}

		chunkSize := bytes.LastIndexByte(buffer.Body[:buffer.Used], '\n') + 1

		if chunkSize > 0 {
			newBuffer := BufferPool.Get().(*Buffer)
			newBuffer.Used = 0

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

		rcv.Go(func(exit chan bool) {
			<-exit
			tcpListener.Close()
		})

		handler := rcv.HandleConnection

		rcv.Go(func(exit chan bool) {
			defer tcpListener.Close()

			for {

				conn, err := tcpListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					logrus.Warningf("[tcp] Failed to accept connection: %s", err)
					continue
				}

				rcv.Go(func(exit chan bool) {
					handler(conn)
				})
			}

		})

		for i := 0; i < rcv.parseThreads; i++ {
			rcv.Go((&PlainParser{In: rcv.parseChan, Out: rcv.writeChan}).Worker)
		}

		rcv.listener = tcpListener

		return nil
	})
}
