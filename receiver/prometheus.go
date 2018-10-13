package receiver

import (
	"context"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/prompb"
	"github.com/lomik/carbon-clickhouse/helper/tags"
)

type PrometheusRemoteWrite struct {
	Base
	listener *net.TCPListener
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
			if rcv.isDrop(writer.Now(), uint32(samples[j].Timestamp/1000)) {
				continue
			}

			writer.WritePoint(metric, samples[j].Value, samples[j].Timestamp/1000)
		}
	}

	writer.Flush()

	if samplesCount := writer.PointsWritten(); samplesCount > 0 {
		atomic.AddUint64(&rcv.stat.samplesReceived, uint64(samplesCount))
	}

	if writeErrors := writer.WriteErrors(); writeErrors > 0 {
		atomic.AddUint64(&rcv.stat.errors, uint64(writeErrors))
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
	rcv.SendStat(send, "samplesReceived", "errors")
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

		rcv.Go(func(ctx context.Context) {
			<-ctx.Done()
			tcpListener.Close()
		})

		rcv.Go(func(ctx context.Context) {
			if err := s.Serve(tcpListener); err != nil {
				rcv.logger.Fatal("failed to serve", zap.Error(err))
			}

		})

		rcv.listener = tcpListener

		return nil
	})
}
