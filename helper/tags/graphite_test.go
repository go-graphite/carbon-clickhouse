package tags

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type graphiteTestCase struct {
	in  string
	ex  string
	err bool
}

var graphiteTestTable = []graphiteTestCase{
	{"some.metric;tag1=value2;tag2=value.2;tag1=value3", "some.metric?tag1=value3&tag2=value.2", false},
	{"some.metric;tag1=value2;tag2=value.2;tag1=value0", "some.metric?tag1=value0&tag2=value.2", false},
	{"some.metric;c=1;b=2;a=3", "some.metric?a=3&b=2&c=1", false},
	{"some.metric;k=a;k=_;k2=3;k=0;k=42", "some.metric?k=42&k2=3", false}, // strange order but as in python-carbon
	{"some.metric", "some.metric", false},
}

var graphiteBenchmarkMetric = "used;host=dfs1;what=diskspace;mountpoint=srv/node/dfs10;unit=B;metric_type=gauge;agent=diamond;processed_by=statsd2"

func TestGraphite(t *testing.T) {
	assert := assert.New(t)

	for i := 0; i < len(graphiteTestTable); i++ {
		n, err := Graphite(DisabledTagConfig(), graphiteTestTable[i].in)

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
