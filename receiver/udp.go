package receiver

import (
	"bytes"
	"net"
	"strings"
	"sync/atomic"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/stop"
	"go.uber.org/zap"
)

// UDP receive metrics from UDP messages
type UDP struct {
	stop.Struct
	stat struct {
		metricsReceived    uint32 // atomic
		errors             uint32 // atomic
		incompleteReceived uint32 // atomic
	}
	name         string // name for store metrics
	conn         *net.UDPConn
	parseThreads int
	parseChan    chan *Buffer
	writeChan    chan *RowBinary.WriteBuffer
	logger       *zap.Logger
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *UDP) Addr() net.Addr {
	if rcv.conn == nil {
		return nil
	}
	return rcv.conn.LocalAddr()
}

func (rcv *UDP) Stat(send func(metric string, value float64)) {
	metricsReceived := atomic.LoadUint32(&rcv.stat.metricsReceived)
	atomic.AddUint32(&rcv.stat.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.stat.errors)
	atomic.AddUint32(&rcv.stat.errors, -errors)
	send("errors", float64(errors))

	incompleteReceived := atomic.LoadUint32(&rcv.stat.incompleteReceived)
	atomic.AddUint32(&rcv.stat.incompleteReceived, -incompleteReceived)
	send("incompleteReceived", float64(incompleteReceived))
}

func (rcv *UDP) receiveWorker(exit chan struct{}) {
	defer rcv.conn.Close()

	buffer := GetBuffer()

ReceiveLoop:
	for {

		n, peer, err := rcv.conn.ReadFromUDP(buffer.Body[:])
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break ReceiveLoop
			}
			atomic.AddUint32(&rcv.stat.errors, 1)
			rcv.logger.Error("ReadFromUDP failed", zap.Error(err), zap.String("peer", peer.String()))
			continue ReceiveLoop
		}

		if n > 0 {
			chunkSize := bytes.LastIndexByte(buffer.Body[:n], '\n') + 1

			if chunkSize < n {
				// @TODO: log and count incomplete with peer
			}

			if chunkSize > 0 {
				buffer.Used = chunkSize
				rcv.parseChan <- buffer
				buffer = GetBuffer()
			}

		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UDP) Listen(addr *net.UDPAddr) error {
	return rcv.StartFunc(func() error {
		var err error

		rcv.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return err
		}

		rcv.Go(func(exit chan struct{}) {
			<-exit
			rcv.conn.Close()
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

		rcv.Go(rcv.receiveWorker)

		return nil
	})
}
