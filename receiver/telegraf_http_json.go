package receiver

import (
	"bytes"
	"context"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/escape"
	"go.uber.org/zap"
)

type TelegrafHttpMetric struct {
	Name      string                 `json:"name"`
	Timestamp int64                  `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields"`
	Tags      map[string]string      `json:"tags"`
}

type TelegrafHttpPayload struct {
	Metrics []TelegrafHttpMetric `json:"metrics"`
}

type TelegrafHttpJson struct {
	Base
	listener *net.TCPListener
}

func TelegrafEncodeTags(tags map[string]string) string {
	if len(tags) < 1 {
		return ""
	}

	if len(tags) == 1 {
		var res bytes.Buffer
		for k, v := range tags {
			res.WriteString(escape.Query(k))
			res.WriteByte('=')
			res.WriteString(escape.Query(v))
			return res.String()
		}
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var res bytes.Buffer
	for i := 0; i < len(keys); i++ {
		if i > 0 {
			res.WriteByte('&')
		}

		// `name` is reserved for metric, replace it as tag name
		key := keys[i]
		if key == "name" {
			key = "_name"
		}

		res.WriteString(escape.Query(key))
		res.WriteByte('=')
		res.WriteString(escape.Query(tags[keys[i]]))
	}
	return res.String()
}

func (rcv *TelegrafHttpJson) process(ctx context.Context, body []byte) (err error) {
	var data TelegrafHttpPayload
	err = json.Unmarshal(body, &data)
	if err != nil {
		return
	}

	writer := RowBinary.NewWriter(ctx, rcv.writeChan)

	var pathBuf bytes.Buffer

metricsLoop:
	for i := 0; i < len(data.Metrics); i++ {
		m := data.Metrics[i]

		tags := TelegrafEncodeTags(m.Tags)

		for f, vi := range m.Fields {
			v, ok := vi.(float64)
			if !ok {
				vb, ok := vi.(bool)
				if !ok {
					continue
				}
				if vb {
					v = 1
				} else {
					v = 0
				}
			}
			pathBuf.Reset()
			pathBuf.WriteString(escape.Path(m.Name))

			if math.IsNaN(v) {
				continue
			}

			if f != "value" {
				pathBuf.WriteString(rcv.concatCharacter)
				pathBuf.WriteString(escape.Path(f))
			}

			pathBuf.WriteByte('?')
			pathBuf.WriteString(tags)

			name := pathBuf.String()
			if rcv.isDropString(name, writer.Now(), uint32(m.Timestamp), v) {
				continue metricsLoop
			}
			writer.WritePoint(name, v, m.Timestamp)
		}
	}

	writer.Flush()
	if samplesCount := writer.PointsWritten(); samplesCount > 0 {
		atomic.AddUint64(&rcv.stat.samplesReceived, uint64(samplesCount))
	}

	if writeErrors := writer.WriteErrors(); writeErrors > 0 {
		atomic.AddUint64(&rcv.stat.errors, uint64(writeErrors))
	}

	return
}

func (rcv *TelegrafHttpJson) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err = rcv.process(r.Context(), body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *TelegrafHttpJson) Addr() net.Addr {
	if rcv.listener == nil {
		return nil
	}
	return rcv.listener.Addr()
}

func (rcv *TelegrafHttpJson) Stat(send func(metric string, value float64)) {
	rcv.SendStat(send, "samplesReceived", "errors", "futureDropped", "pastDropped", "tooLongDropped")
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
