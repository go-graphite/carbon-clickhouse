package receiver

import (
	"context"
	"fmt"
	"regexp"
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
		{"cpu.loadavg~ 21.4 1422642189\n", "cpu.loadavg~", 21.4, 1422642189},
		{"cpu.loadavg~;env=test2;host=host1;env=test 21.4 1422642189\n", "cpu.loadavg~?env=test&host=host1", 21.4, 1422642189},
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

	tableWithValidation := [](struct {
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

		// Additional test cases for validation
		// Test invalid characters in metric names
		{b: "metric@name 42.15 1422642189\n"},
		{b: "metric#name 42.15 1422642189\n"},
		{b: "metric$name 42.15 1422642189\n"},
		{b: "metric%name 42.15 1422642189\n"},
		{b: "metric&name 42.15 1422642189\n"},
		{b: "metric*name 42.15 1422642189\n"},
		{b: "metric!name 42.15 1422642189\n"},
		{b: "metric name 42.15 1422642189\n"},  // space in metric name
		{b: "metric\tname 42.15 1422642189\n"}, // tab in metric name
		{b: "metric[name] 42.15 1422642189\n"},
		{b: "metric{name} 42.15 1422642189\n"},
		{b: "metric(name) 42.15 1422642189\n"},
		{b: "metric/name 42.15 1422642189\n"},
		{b: "metric\\name 42.15 1422642189\n"},
		{b: "metric|name 42.15 1422642189\n"},
		{b: "metric?name 42.15 1422642189\n"},
		{b: "metric<name> 42.15 1422642189\n"},
		{b: "metric'name' 42.15 1422642189\n"},
		{b: "metric\"name\" 42.15 1422642189\n"},

		// Test valid characters that should pass
		{"metric-name 42.15 1422642189\n", "metric-name", 42.15, 1422642189},
		{"metric_name 42.15 1422642189\n", "metric_name", 42.15, 1422642189},
		{"metric:name 42.15 1422642189\n", "metric:name", 42.15, 1422642189},
		{"metric.sub.name 42.15 1422642189\n", "metric.sub.name", 42.15, 1422642189},
		{"metric-123_test:data 42.15 1422642189\n", "metric-123_test:data", 42.15, 1422642189},

		// Test invalid characters in tags
		{b: "metric.name;tag@=value 42.15 1422642189\n"},
		{b: "metric.name;tag=val@ue 42.15 1422642189\n"},
		{b: "metric.name;t ag=value 42.15 1422642189\n"},
		{b: "metric.name;tag=val ue 42.15 1422642189\n"},
		{b: "metric.name;tag#key=value 42.15 1422642189\n"},
		{b: "metric.name;tag=value! 42.15 1422642189\n"},
		{b: "metric.name;tag=value;key=val*ue 42.15 1422642189\n"},
		{b: "metric.name;tag=value;k ey=value 42.15 1422642189\n"},
		{b: "metric.name;tag=value;key=val\tue 42.15 1422642189\n"},
		{b: "metric.name;tag=value;key=val\nue 42.15 1422642189\n"},

		// Test valid tags that should pass
		{"metric.name;env=prod 42.15 1422642189\n", "metric.name?env=prod", 42.15, 1422642189},
		{"metric.name;env=prod;region=us-east-1 42.15 1422642189\n", "metric.name?env=prod&region=us-east-1", 42.15, 1422642189},
		{"metric.name;tag-name=tag-value 42.15 1422642189\n", "metric.name?tag-name=tag-value", 42.15, 1422642189},
		{"metric.name;tag_name=tag_value 42.15 1422642189\n", "metric.name?tag_name=tag_value", 42.15, 1422642189},
		{"metric.name;tag:name=tag:value 42.15 1422642189\n", "metric.name?tag%3Aname=tag%3Avalue", 42.15, 1422642189},
		{"metric.name;tag.name=tag.value 42.15 1422642189\n", "metric.name?tag.name=tag.value", 42.15, 1422642189},

		// Test edge cases with multiple invalid characters
		{b: "metric@#$%name 42.15 1422642189\n"},
		{b: "metric.name;tag@#=value$% 42.15 1422642189\n"},
		{b: "met!ric.na@me;ta#g=val$ue 42.15 1422642189\n"},

		// Test unicode characters (should fail validation)
		{b: "metric.名前 42.15 1422642189\n"},
		{b: "metric.name;tag=値 42.15 1422642189\n"},
		{b: "metric.name;标签=value 42.15 1422642189\n"},
		{b: "метрика.name 42.15 1422642189\n"},

		// Test empty tag keys/values
		{b: "metric.name;=value 42.15 1422642189\n"},
		{b: "metric.name;= 42.15 1422642189\n"},

		// Test metrics with numbers
		{"metric123 42.15 1422642189\n", "metric123", 42.15, 1422642189},
		{"123metric 42.15 1422642189\n", "123metric", 42.15, 1422642189},
		{"123 42.15 1422642189\n", "123", 42.15, 1422642189},

		// Test metrics with only valid special characters
		{"metric-_.:name 42.15 1422642189\n", "metric-_.:name", 42.15, 1422642189},
		{"metric.name;tag-_.:key=tag-_.:value 42.15 1422642189\n", "metric.name?tag-_.%3Akey=tag-_.%3Avalue", 42.15, 1422642189},

		// Additional tests for colon encoding
		{"host:port:metric 42.15 1422642189\n", "host:port:metric", 42.15, 1422642189},
		{"metric.name;service:port=web:8080 42.15 1422642189\n", "metric.name?service%3Aport=web%3A8080", 42.15, 1422642189},
		{"app:service:metric;env=prod:primary 42.15 1422642189\n", "app:service:metric?env=prod%3Aprimary", 42.15, 1422642189},
	}

	baseWithValidation := &Base{blacklistRegex: regexp.MustCompile(`[^a-zA-Z0-9.;\-_:=]{1}`)}
	for _, p := range tableWithValidation {
		name, value, timestamp, err := baseWithValidation.PlainParseLine([]byte(p.b), now, &tagBuf)
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
