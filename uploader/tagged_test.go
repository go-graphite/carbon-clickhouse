package uploader

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/escape"
	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
)

func TestUrlParse(t *testing.T) {
	assert := assert.New(t)

	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	assert.Equal("instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10%3A9100&job=node", metric)

	// original url.Parse
	m, err := url.Parse(metric)
	assert.NotNil(m)
	assert.NoError(err)
	assert.Equal("", m.Path)

	// from tagged uploader
	m, err = urlParse(metric)
	assert.NotNil(m)
	assert.NoError(err)
	assert.Equal("instance:cpu_utilization:ratio_avg", m.Path)
}

func BenchmarkKeySprintf(b *testing.B) {
	path := "test.path"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d:%s", 1285, path)
	}
}

func BenchmarkKeyConcat(b *testing.B) {
	path := "test.path"
	var unum uint16 = 1245

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = strconv.Itoa(int(unum)) + ":" + path
	}
}

func TestTagged_parseName_Overflow(t *testing.T) {
	var tag1 []string
	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	var sb strings.Builder
	sb.WriteString("very_long_name_field1.very_long_name_field2.very_long_name_field3.very_long_name_field4?")
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteString("&")
		}
		sb.WriteString(fmt.Sprintf("very_long_tag%d=very_long_value%d", i, i))
	}
	u := NewTagged(base)
	err := u.parseName(sb.String(), 10, tag1, wb, tagsBuf)
	assert.NoError(t, err)
}
