package receiver

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// UDP receive metrics from UDP messages
type UDP struct {
	Base
	conn      *net.UDPConn
	parseChan chan *Buffer
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *UDP) Addr() net.Addr {
	if rcv.conn == nil {
		return nil
	}
	return rcv.conn.LocalAddr()
}

func (rcv *UDP) Stat(send func(metric string, value float64)) {
	rcv.SendStat(send, "metricsReceived", "errors", "incompleteReceived", "futureDropped", "pastDropped")
}

func (rcv *UDP) receiveWorker(ctx context.Context) {
	defer rcv.conn.Close()

	buffer := GetBuffer()

ReceiveLoop:
	for {

		n, peer, err := rcv.conn.ReadFromUDP(buffer.Body[:])
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break ReceiveLoop
			}
			atomic.AddUint64(&rcv.stat.errors, 1)
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
				buffer.Time = uint32(time.Now().Unix())
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

		rcv.Go(func(ctx context.Context) {
			<-ctx.Done()
			rcv.conn.Close()
		})

		for i := 0; i < rcv.parseThreads; i++ {
			rcv.Go(func(ctx context.Context) {
				rcv.PlainParser(ctx, rcv.parseChan)
			})
		}

		rcv.Go(rcv.receiveWorker)

		return nil
	})
}
