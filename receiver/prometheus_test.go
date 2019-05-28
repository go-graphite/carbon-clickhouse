package receiver

import (
	"encoding/base64"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
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

func BenchmarkProm1Unmarshal(b *testing.B) {
	assert := assert.New(b)

	compressed, err := base64.StdEncoding.DecodeString(prom1)
	assert.NoError(err)

	reqBuf, err := snappy.Decode(nil, compressed)
	assert.NoError(err)

	var req prompb.WriteRequest

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = proto.Unmarshal(reqBuf, &req)
		if err != nil {
			b.Fatalf("Unexpected error: %#v", err.Error())
		}
	}
}
