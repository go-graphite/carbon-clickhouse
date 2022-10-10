package receiver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/tags"
)

func BenchmarkPlainParseBuffer(b *testing.B) {
	out := make(chan *RowBinary.WriteBuffer, 1)

	now := time.Now().Unix()

	msg := fmt.Sprintf("carbon.agents.localhost.cache.size 1412351 %d\n", now)
	buf := GetBuffer()
	buf.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf.Write([]byte(msg))
	}

	msg2 := fmt.Sprintf("carbon.agents.server.udp.received 42 %d\n", now)
	buf2 := GetBuffer()
	buf2.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf2.Write([]byte(msg2))
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case b := <-out:
				b.Release()
			case <-ctx.Done():
				return
			}
		}
	}()

	b.ResetTimer()

	base := &Base{writeChan: out}
	var tagBuf tags.GraphiteBuf
	tagBuf.Resize(128, 4096)

	for i := 0; i < b.N; i += 100 {
		base.PlainParseBuffer(context.Background(), buf, &tagBuf)
		base.PlainParseBuffer(context.Background(), buf2, &tagBuf)
	}

	b.StopTimer()
	cancel()
}

func BenchmarkPlainParseBufferTagged(b *testing.B) {
	out := make(chan *RowBinary.WriteBuffer, 1)

	now := time.Now().Unix()

	msg := fmt.Sprintf("cpu.loadavg;env=test2;host=host1;env=test 21.4 %d\n", now)
	buf := GetBuffer()
	buf.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf.Write([]byte(msg))
	}

	msg2 := fmt.Sprintf("cpu.loadavg;env=test;host=host1 13 %d\n", now)
	buf2 := GetBuffer()
	buf2.Time = uint32(now)
	for i := 0; i < 50; i++ {
		buf2.Write([]byte(msg2))
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case b := <-out:
				b.Release()
			case <-ctx.Done():
				return
			}
		}
	}()

	b.ResetTimer()

	base := &Base{writeChan: out}
	var tagBuf tags.GraphiteBuf
	tagBuf.Resize(128, 4096)

	for i := 0; i < b.N; i += 100 {
		base.PlainParseBuffer(context.Background(), buf, &tagBuf)
		base.PlainParseBuffer(context.Background(), buf2, &tagBuf)
	}

	b.StopTimer()
	cancel()
}

func TestRemoveDoubleDot(t *testing.T) {
	table := [](struct {
		input    string
		expected string
	}){
		{"", ""},
		{".....", "."},
		{"hello.world", "hello.world"},
		{"hello..world", "hello.world"},
		{"..hello..world..", ".hello.world."},
	}

	for _, p := range table {
		t.Run(p.input, func(b *testing.T) {
			v := RemoveDoubleDot([]byte(p.input))
			if string(v) != p.expected {
				t.Fatalf("%#v != %#v", string(v), p.expected)
			}
		})
	}
}

func BenchmarkRemoveDoubleDot(b *testing.B) {
	benchmarks := []string{
		"hello.world",
		"hello..world",
		"..hello..world..",
		"hello.world.lon.metric.with..dots",
		"hello.world.lon.metric.without.dots",
	}

	for _, bm := range benchmarks {
		b.Run(bm, func(b *testing.B) {
			input := []byte(bm)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = RemoveDoubleDot(input)
			}
		})
	}
}

func TestPlainParseLine(t *testing.T) {
	now := uint32(time.Now().Unix())

	table := [](struct {
		b         string
		name      string
		value     float64
		timestamp uint32
	}){
		{b: "42"},
		{b: ""},
		{b: "\n"},
		{b: "metric..name 42 \n"},
		{b: "metric..name 42"},
		{b: "metric.name 42 a1422642189\n"},
		{b: "metric.name 42a 1422642189\n"},
		{b: "metric.name NaN 1422642189\n"},
		{b: "metric.name 42 NaN\n"},
		{"metric.name -42.76 1422642189\n", "metric.name", -42.76, 1422642189},
		{"metric.name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric..name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric...name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric.name 42.15 1422642189\r\n", "metric.name", 42.15, 1422642189},
		{"metric.name;tag=value;k=v 42.15 1422642189\r\n", "metric.name?k=v&tag=value", 42.15, 1422642189},
		{"metric..name 42.15 -1\n", "metric.name", 42.15, now},
		{"cpu.loadavg;env=test2;host=host1;env=test 21.4 1422642189\n", "cpu.loadavg?env=test&host=host1", 21.4, 1422642189},
	}

	base := &Base{}
	var tagBuf tags.GraphiteBuf
	tagBuf.Resize(128, 4096)

	for _, p := range table {
		name, value, timestamp, err := base.PlainParseLine([]byte(p.b), now, &tagBuf)
		if p.name == "" {
			// expected error
			if err == nil {
				t.Fatal("error expected")
			}
		} else {
			if string(name) != p.name {
				t.Fatalf("%#v != %#v", string(name), p.name)
			}
			if value != p.value {
				t.Fatalf("%#v != %#v", value, p.value)
			}
			if timestamp != p.timestamp {
				t.Fatalf("%d != %d", timestamp, p.timestamp)
			}
		}
	}
}
