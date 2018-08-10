package receiver

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/stop"
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

func TelegrafEncodeTags(tags map[string]string) string {
	if len(tags) < 1 {
		return ""
	}

	if len(tags) == 1 {
		var res bytes.Buffer
		for k, v := range tags {
			res.WriteString(url.QueryEscape(k))
			res.WriteByte('=')
			res.WriteString(url.QueryEscape(v))
			return res.String()
		}
	}

	keys := make([]string, 0, len(tags))
	for k, _ := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var res bytes.Buffer
	for i := 0; i < len(keys); i++ {
		if i > 1 {
			res.WriteByte('&')
		}
		res.WriteString(url.QueryEscape(keys[i]))
		res.WriteByte('=')
		res.WriteString(url.QueryEscape(tags[keys[i]]))
	}
	return res.String()
}

func (rcv *TelegrafHttpJson) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var data TelegrafHttpPayload
	err = json.Unmarshal(body, &data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writer := RowBinary.NewWriter(r.Context(), rcv.writeChan)

	var pathBuf bytes.Buffer

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
			pathBuf.WriteString(url.PathEscape(m.Name))

			if math.IsNaN(v) {
				continue
			}

			if f != "value" {
				pathBuf.WriteByte('_')
				pathBuf.WriteString(url.PathEscape(f))
			}

			pathBuf.WriteByte('?')
			pathBuf.WriteString(tags)

			writer.WritePoint(pathBuf.String(), v, m.Timestamp)
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
