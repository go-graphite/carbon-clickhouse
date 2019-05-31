package receiver

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/prompb"
	"github.com/stretchr/testify/assert"
)

func TestProm1Unpack(t *testing.T) {
	assert := assert.New(t)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	reqBuf, err := snappy.Decode(nil, compressed)
	assert.NoError(err)

	var req prompb.WriteRequest
	assert.NoError(proto.Unmarshal(reqBuf, &req))

	assert.Equal(1663, len(req.Timeseries))
}

func TestProm1UnpackFast(t *testing.T) {
	assert := assert.New(t)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	reqBuf, err := snappy.Decode(nil, compressed)
	assert.NoError(err)

	fast := &PrometheusRemoteWrite{}
	fast.writeChan = make(chan *RowBinary.WriteBuffer, 1024)
	assert.NoError(fast.unpackFast(context.Background(), reqBuf))

	slow := &PrometheusRemoteWrite{}
	slow.writeChan = make(chan *RowBinary.WriteBuffer, 1024)
	assert.NoError(slow.unpackDefault(context.Background(), reqBuf))

	var wbf, wbs *RowBinary.WriteBuffer
chanLoop:
	for {
		select {
		case wbf = <-fast.writeChan:
		default:
			break chanLoop
		}

		wbs = <-slow.writeChan
		if !assert.Equal(wbf.Body[:wbf.Used], wbs.Body[:wbs.Used]) {
			return
		}
	}
}

func BenchmarkProm1Snappy(b *testing.B) {
	assert := assert.New(b)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = snappy.Decode(nil, compressed)
		if err != nil {
			b.Fatalf("Unexpected error: %#v", err.Error())
		}
	}
}

func BenchmarkProm1UnpackFast(b *testing.B) {
	assert := assert.New(b)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	reqBuf, err := snappy.Decode(nil, compressed)
	assert.NoError(err)

	h := &PrometheusRemoteWrite{}
	h.writeChan = make(chan *RowBinary.WriteBuffer, 1024)
	var wb *RowBinary.WriteBuffer

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = h.unpackFast(context.Background(), reqBuf)
		if err != nil {
			b.Fatalf("Unexpected error: %#v", err.Error())
		}
	readLoop:
		for {
			select {
			case wb = <-h.writeChan:
				wb.Release()
			default:
				break readLoop
			}
		}
	}
}

func BenchmarkProm1UnpackSlow(b *testing.B) {
	assert := assert.New(b)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	reqBuf, err := snappy.Decode(nil, compressed)
	assert.NoError(err)

	h := &PrometheusRemoteWrite{}
	h.writeChan = make(chan *RowBinary.WriteBuffer, 1024)
	var wb *RowBinary.WriteBuffer

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = h.unpackDefault(context.Background(), reqBuf)
		if err != nil {
			b.Fatalf("Unexpected error: %#v", err.Error())
		}
	readLoop:
		for {
			select {
			case wb = <-h.writeChan:
				wb.Release()
			default:
				break readLoop
			}
		}
	}
}
