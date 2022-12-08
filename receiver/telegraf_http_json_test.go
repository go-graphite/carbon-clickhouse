package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary/reader"
	"github.com/lomik/carbon-clickhouse/helper/tags"
	"github.com/lomik/carbon-clickhouse/helper/tests"
	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
)

func TestTelegrafEncodeTags(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
		want string
	}{
		{
			name: "tags with spaces",
			tags: map[string]string{
				"name": "name with space",
				"tag":  "value with space",
			},
			want: "_name=name+with+space&tag=value+with+space",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TelegrafEncodeTags(tt.tags))
		})
	}
}

func TestTelegrafHttpJson(t *testing.T) {
	writeChan := make(chan *RowBinary.WriteBuffer)
	address, err := tests.GetFreeTCPPort("")
	if err != nil {
		t.Fatal("get free port", err)
	}

	payload := TelegrafHttpPayload{
		Metrics: []TelegrafHttpMetric{
			{
				Name:      "name with space.",
				Fields:    map[string]interface{}{"counter": 3538944},
				Tags:      map[string]string{"key with space": "value with space", "name": "name_value"},
				Timestamp: 1670348700,
			},
			{
				Name:      "name with space.",
				Fields:    map[string]interface{}{"gauge": 3538945},
				Tags:      map[string]string{"key2": "value2", "key1": "value2"},
				Timestamp: 1670348702,
			},
		},
	}

	// paths are encoded
	wantPoints := []reader.Point{
		{
			Path:      "name%20with%20space.counter?key+with+space=value+with+space&_name=name_value",
			Value:     3538944,
			Timestamp: 1670348700,
			Days:      19332,
		},
		{
			Path:      "name%20with%20space.gauge?key1=value2&key2=value2",
			Value:     3538945,
			Timestamp: 1670348702,
			Days:      19332,
		},
	}

	out, err := json.Marshal(&payload)
	if err != nil {
		t.Fatal("payload marshal", err)
	}

	// simulate writer
	var rawBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case b := <-writeChan:
				rawBuf.Write(b.Bytes())
				b.Release()
			case <-ctx.Done():
				return
			}
		}
	}()

	r, err := New(
		"telegraf+http+json://"+address,
		tags.DisabledTagConfig(),
		ParseThreads(runtime.GOMAXPROCS(-1)*2),
		WriteChan(writeChan),
		DropFuture(uint32(0)),
		DropPast(uint32(0)),
		DropLongerThan(0),
		ReadTimeout(uint32(120)),
	)
	if err != nil {
		t.Fatal("receiver New()", err)
	}

	start := uint32(time.Now().Unix())

	req, err := http.NewRequest("POST", "http://"+address, bytes.NewReader(out))
	if err != nil {
		t.Fatal("http.NewRequest()", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("POST", err)
	}
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
	_ = r

	verifyIndexUploaded(t, &rawBuf, wantPoints, start, uint32(time.Now().Unix()))
}

func BenchmarkTelegrafHttpJson5(b *testing.B) {
	writeChan := make(chan *RowBinary.WriteBuffer)
	// simulate writer
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case b := <-writeChan:
				b.Release()
			case <-ctx.Done():
				return
			}
		}
	}()
	r := &TelegrafHttpJson{}
	r.Init(zapwriter.Logger("telegraf"), tags.DisabledTagConfig())

	r.Base.writeChan = writeChan

	payload := TelegrafHttpPayload{
		Metrics: []TelegrafHttpMetric{
			{
				Name:      "name with space1.",
				Fields:    map[string]interface{}{"counter": 3538944},
				Tags:      map[string]string{"key with space": "value with space", "name": "name_value"},
				Timestamp: 1670348700,
			},
			{
				Name:      "name with space2.",
				Fields:    map[string]interface{}{"gauge": 3538945},
				Tags:      map[string]string{"key2": "value2", "key1": "value2"},
				Timestamp: 1670348702,
			},
			{
				Name:      "name with space3.",
				Fields:    map[string]interface{}{"gauge": 3538945},
				Tags:      map[string]string{"key2": "value2", "key1": "value2"},
				Timestamp: 1670348702,
			},
			{
				Name:      "name with space4.",
				Fields:    map[string]interface{}{"gauge": 3538945},
				Tags:      map[string]string{"key2": "value2", "key1": "value2"},
				Timestamp: 1670348702,
			},
			{
				Name:      "name with space5.",
				Fields:    map[string]interface{}{"gauge": 3538945},
				Tags:      map[string]string{"key2": "value2", "key1": "value2"},
				Timestamp: 1670348702,
			},
		},
	}
	out, err := json.Marshal(&payload)
	if err != nil {
		b.Fatal("payload marshal", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.process(context.Background(), out); err != nil {
			b.Fatal("process", err)
		}
	}
	b.StopTimer()

	cancel()
	wg.Wait()
}
