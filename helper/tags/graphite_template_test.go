package tags

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var graphiteTemplateTestTable = []graphiteTestCase{
	{"some.metric", "metric?tag0=value0&tag1=value1", false},
	{"aval.bval.cval.app", "app?a=aval&b=bval&c=cval&tag0=value0&tag1=value1", false},
	{"stats.local.a.b.c.d", "a_b_c_d?host=local&region=us-west&tag0=value0&tag1=new-value1", false},
	//{"some.metric;k=a;k=_;k2=3;k=0;k=42", "some.metric?k=42&k2=3", false}, // strange order but as in python-carbon
	//{"some.metric", "some.metric", false},
}

func TestTemplates(t *testing.T) {
	assert := assert.New(t)
	tagCfg := TagConfig{Enabled: true,
		Separator: "_",
		Tags:      []string{"tag0=value0", "tag1=value1"},
		Templates: []string{
			"*.app a.b.c.measurement",
			"stats.* .host.measurement* region=us-west,tag1=new-value1",
			//"stats.* .host.measurement.field",
			".measurement*",
		},
	}
	tagCfg.Configure()

	for i := 0; i < len(graphiteTemplateTestTable); i++ {
		n, err := Graphite(tagCfg, graphiteTemplateTestTable[i].in)

		if !graphiteTemplateTestTable[i].err {
			assert.NoError(err)
		} else {
			assert.Error(err)
		}

		assert.Equal(graphiteTemplateTestTable[i].ex, n)
	}
}
