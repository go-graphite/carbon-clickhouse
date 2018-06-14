package receiver

import (
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/stop"
	"go.uber.org/zap"
)

type TelegrafHttpJson struct {
	stop.Struct
	stat struct {
		samplesReceived uint32 // atomic
		errors          uint32 // atomic
		active          int32  // atomic
	}
	listener  *net.TCPListener
	writeChan chan *RowBinary.WriteBuffer
	logger    *zap.Logger
}

func (rcv *TelegrafHttpJson) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rcv.logger.Info("received " + string(body))
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TelegrafHttpJson) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TelegrafHttpJson) Stat(send func(metric string, value float64)) {
	samplesReceived := atomic.LoadUint32(&rcv.stat.samplesReceived)
	atomic.AddUint32(&rcv.stat.samplesReceived, -samplesReceived)
	send("samplesReceived", float64(samplesReceived))

	errors := atomic.LoadUint32(&rcv.stat.errors)
	atomic.AddUint32(&rcv.stat.errors, -errors)
	send("errors", float64(errors))
}

// Listen bind port. Receive messages and send to out channel
func (rcv *TelegrafHttpJson) Listen(addr *net.TCPAddr) error {
	return rcv.StartFunc(func() error {

		tcpListener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		s := &http.Server{
			Handler:        rcv,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}

		rcv.Go(func(exit chan struct{}) {
			<-exit
			tcpListener.Close()
		})

		rcv.Go(func(exit chan struct{}) {
			if err := s.Serve(tcpListener); err != nil {
				rcv.logger.Fatal("failed to serve", zap.Error(err))
			}

		})

		rcv.listener = tcpListener

		return nil
	})
}
