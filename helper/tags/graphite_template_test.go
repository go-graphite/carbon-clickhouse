package tags

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var graphiteTemplateTestTable = []graphiteTestCase{
	{"some.metric", "metric?tag0=value0&tag1=value1", false},
	{"aval.bval.cval.app", "app?a=aval&b=bval&c=cval&tag0=value0&tag1=value1", false},
	{"stats.local.a.b.c.d", "a_b_c_d?host=local&region=us-west&tag0=value0&tag1=new-value1", false},
	{"multi.tags.aval.m1.m2.m3", "m1_m2_m3?a=aval&tag0=new-value0&tag1=value1", false},
}

func TestTemplates(t *testing.T) {
	assert := assert.New(t)
	tagCfg := TagConfig{Enabled: true,
		Separator: "_",
		Tags:      []string{"tag0=value0", "tag1=value1"},
		Templates: []string{
			"*.app a.b.c.measurement",
			"stats.* .host.measurement* region=us-west,tag1=new-value1",
			"multi.tags.* ..a.measurement*    tag0=new-value0",
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
