package receiver

import (
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/lomik/stop"
	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/prompb"
	"github.com/lomik/carbon-clickhouse/helper/tags"
)

type PrometheusRemoteWrite struct {
	stop.Struct
	stat struct {
		samplesReceived uint32 // atomic
		errors          uint32 // atomic
		active          int32  // atomic
	}
	listener     *net.TCPListener
	parseThreads int
	writeChan    chan *RowBinary.WriteBuffer
	logger       *zap.Logger
}

func (rcv *PrometheusRemoteWrite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writer := RowBinary.NewWriter(r.Context(), rcv.writeChan)

	series := req.GetTimeseries()
	for i := 0; i < len(series); i++ {
		metric, err := tags.Prometheus(series[i].GetLabels())

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := series[i].GetSamples()

		for j := 0; j < len(samples); j++ {
			if samples[j] == nil {
				continue
			}
			if math.IsNaN(samples[j].Value) {
				continue
			}
			writer.WritePoint(metric, samples[j].Value, samples[j].Timestamp/1000)
		}
	}

	writer.Flush()

	if samplesCount := writer.PointsWritten(); samplesCount > 0 {
		atomic.AddUint32(&rcv.stat.samplesReceived, samplesCount)
	}

	if writeErrors := writer.WriteErrors(); writeErrors > 0 {
		atomic.AddUint32(&rcv.stat.errors, writeErrors)
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *PrometheusRemoteWrite) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *PrometheusRemoteWrite) Stat(send func(metric string, value float64)) {
	samplesReceived := atomic.LoadUint32(&rcv.stat.samplesReceived)
	atomic.AddUint32(&rcv.stat.samplesReceived, -samplesReceived)
	send("samplesReceived", float64(samplesReceived))

	errors := atomic.LoadUint32(&rcv.stat.errors)
	atomic.AddUint32(&rcv.stat.errors, -errors)
	send("errors", float64(errors))
}

// Listen bind port. Receive messages and send to out channel
func (rcv *PrometheusRemoteWrite) Listen(addr *net.TCPAddr) error {
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
