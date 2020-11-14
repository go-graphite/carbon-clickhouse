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
	"github.com/lomik/carbon-clickhouse/helper/pb"
	"github.com/lomik/carbon-clickhouse/helper/prompb"
	"github.com/lomik/carbon-clickhouse/helper/tags"
)

var nameLabel = []byte("\n\b__name__\x12")

type PrometheusRemoteWrite struct {
	Base
	listener *net.TCPListener
}

func (rcv *PrometheusRemoteWrite) unpackFast(ctx context.Context, bufBody []byte) error {

	b := bufBody
	var err error
	var ts []byte
	var sample []byte

	metricBuffer := newPrometheusMetricBuffer()

	var metric []string
	var samplesOffset int

	var value float64
	var timestamp int64

	writer := RowBinary.NewWriter(ctx, rcv.writeChan)

TimeSeriesLoop:
	for len(b) > 0 {
		if b[0] != 0x0a { // repeated prometheus.TimeSeries timeseries = 1;
			if b, err = pb.Skip(b); err != nil {
				break TimeSeriesLoop
			}
			continue TimeSeriesLoop
		}

		if ts, b, err = pb.Bytes(b[1:]); err != nil {
			break TimeSeriesLoop
		}

		if metric, samplesOffset, err = metricBuffer.timeSeries(ts); err != nil {
			break TimeSeriesLoop
		}

		ts = ts[samplesOffset:]
	SamplesLoop:
		for len(ts) > 0 {
			if ts[0] != 0x12 { // repeated Sample samples = 2;
				if ts, err = pb.Skip(ts); err != nil {
					break TimeSeriesLoop
				}
				continue SamplesLoop
			}

			if sample, ts, err = pb.Bytes(ts[1:]); err != nil {
				break TimeSeriesLoop
			}

			timestamp = 0
			value = 0

			for len(sample) > 0 {
				switch sample[0] {
				case 0x09: // double value    = 1;
					if value, sample, err = pb.Double(sample[1:]); err != nil {
						break TimeSeriesLoop
					}
				case 0x10: // int64 timestamp = 2;
					if timestamp, sample, err = pb.Int64(sample[1:]); err != nil {
						break TimeSeriesLoop
					}
				default:
					if sample, err = pb.Skip(sample); err != nil {
						break TimeSeriesLoop
					}
				}
			}

			if math.IsNaN(value) {
				continue SamplesLoop
			}

			if rcv.isDropString("", writer.Now(), uint32(timestamp/1000), value) {
				continue
			}

			writer.WritePointTagged(metric, value, timestamp/1000)
		}
	}

	if err != nil {
		return err
	}

	writer.Flush()

	if samplesCount := writer.PointsWritten(); samplesCount > 0 {
		atomic.AddUint64(&rcv.stat.samplesReceived, uint64(samplesCount))
	}

	if writeErrors := writer.WriteErrors(); writeErrors > 0 {
		atomic.AddUint64(&rcv.stat.errors, uint64(writeErrors))
	}

	return nil
}

func (rcv *PrometheusRemoteWrite) unpackDefault(ctx context.Context, bufBody []byte) error {
	var req prompb.WriteRequest
	if err := proto.Unmarshal(bufBody, &req); err != nil {
		return err
	}

	writer := RowBinary.NewWriter(ctx, rcv.writeChan)

	series := req.GetTimeseries()
	for i := 0; i < len(series); i++ {
		metric, err := tags.Prometheus(series[i].GetLabels())

		if err != nil {
			return err
		}

		samples := series[i].GetSamples()

		for j := 0; j < len(samples); j++ {
			if samples[j] == nil {
				continue
			}
			if math.IsNaN(samples[j].Value) {
				continue
			}
			if rcv.isDropString(metric, writer.Now(), uint32(samples[j].Timestamp/1000), samples[j].Value) {
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

	return nil
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

	err = rcv.unpackFast(r.Context(), reqBuf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
	rcv.SendStat(send, "samplesReceived", "errors", "futureDropped", "pastDropped", "tooLongDropped")
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
