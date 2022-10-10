package tags

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type graphiteTestCase struct {
	in  string
	ex  string
	err bool
}

var graphiteTestTable = []graphiteTestCase{
	{";tag1=value2;tag2=value.2;tag1=value3", "", true},                    // name not set
	{"used;metric_type=gauge;agentdiamond;processed_by=statsd2", "", true}, // tags without value
	{"notag", "notag", false},                                              // no tags
	{"some.metric;tag1=value2;tag2=value.2;tag1=value3", "some.metric?tag1=value3&tag2=value.2", false},
	{"some.metric;tag1=value2;tag2=value.2;tag1=value0", "some.metric?tag1=value0&tag2=value.2", false},
	{"some.metric;c=1;b=2;a=3", "some.metric?a=3&b=2&c=1", false},
	{"some.metric;k=a;k=_;k2=3;k=0;k=42", "some.metric?k=42&k2=3", false}, // strange order but as in python-carbon
	{"some.metric", "some.metric", false},
	{"complex.delete_me.tag2./some/url/fff.series;tag2=value2", "complex.delete_me.tag2./some/url/fff.series?tag2=value2", false},
	{"name.иван", "name.иван", false},
	{"name.иван;tagged=true", "name.%D0%B8%D0%B2%D0%B0%D0%BD?tagged=true", false},
	{"some.metric,1", "some.metric,1", false},
	{"some.metric,1;tagged=true", "some.metric,1?tagged=true", false},
	{"some.metric?name", "some.metric?name", false}, // question mark is disallowed in plain graphite protocol
	{"some.metric?name;tagged=true", "some.metric%3Fname?tagged=true", false},
	{"some.metric;tagged=true?false", "some.metric?tagged=true%3Ffalse", false},
}

var graphiteBenchmarkMetric = "used;host=dfs1;what=diskspace;mountpoint=srv/node/dfs10;unit=B;metric_type=gauge;agent=diamond;processed_by=statsd2"
var graphiteBenchmarkMetricBroken = "used;host=dfs1;what=diskspace;mountpoint=srv/node/dfs10;unit=B;metric_type=gauge;agentdiamond;processed_by=statsd2"
var graphiteBenchmarkMetricEscaped = "used;host=dfs1;what=diskspace;mountpoint=srv/node/dfs10;unit=A?B;metric_type=gauge;agent=diamond;url=http://dfs1.test.int/metrics"

func TestGraphite(t *testing.T) {
	assert := assert.New(t)

	for i := 0; i < len(graphiteTestTable); i++ {
		n, err := Graphite(DisabledTagConfig(), graphiteTestTable[i].in)

		if !graphiteTestTable[i].err {
			assert.NoError(err, graphiteTestTable[i].in)
		} else {
			assert.Error(err, graphiteTestTable[i].in)
		}

		assert.Equal(graphiteTestTable[i].ex, n, graphiteTestTable[i].in)
	}
}

func TestGraphiteBuf(t *testing.T) {
	assert := assert.New(t)

	var buf GraphiteBuf

	for i := 0; i < len(graphiteTestTable); i++ {
		n, err := GraphiteBuffered(DisabledTagConfig(), graphiteTestTable[i].in, &buf)

		if !graphiteTestTable[i].err {
			assert.NoError(err)
		} else {
			assert.Error(err)
		}

		assert.Equal(graphiteTestTable[i].ex, n)
	}
}

func BenchmarkGraphite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Graphite(DisabledTagConfig(), graphiteBenchmarkMetric)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphiteBroken(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Graphite(DisabledTagConfig(), graphiteBenchmarkMetricBroken)
		if err == nil {
			b.Fatal("must error")
		}
	}
}

func BenchmarkGraphiteEscaped(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Graphite(DisabledTagConfig(), graphiteBenchmarkMetricEscaped)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphiteBuffered(b *testing.B) {
	var buf GraphiteBuf
	buf.Resize(128, len(graphiteBenchmarkMetric)+10)
	for i := 0; i < b.N; i++ {
		_, err := GraphiteBuffered(DisabledTagConfig(), graphiteBenchmarkMetric, &buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphiteBrokenBuffered(b *testing.B) {
	var buf GraphiteBuf
	buf.Resize(128, len(graphiteBenchmarkMetric)+10)
	for i := 0; i < b.N; i++ {
		_, err := GraphiteBuffered(DisabledTagConfig(), graphiteBenchmarkMetricBroken, &buf)
		if err == nil {
			b.Fatal("must error")
		}
	}
}

func BenchmarkGraphiteEscapedBuffered(b *testing.B) {
	var buf GraphiteBuf
	buf.Resize(128, len(graphiteBenchmarkMetric)+10)
	for i := 0; i < b.N; i++ {
		_, err := GraphiteBuffered(DisabledTagConfig(), graphiteBenchmarkMetricEscaped, &buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
