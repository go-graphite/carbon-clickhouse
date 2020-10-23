package uploader

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"testing"

	"github.com/lomik/carbon-clickhouse/helper/escape"
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

func TestTagsParse(t *testing.T) {
	assert := assert.New(t)

	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	assert.Equal("instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node", metric)

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
	m.Path = "__name__=" + m.Path
	mapTags := m.Query()
	mTags := make([]string, len(mapTags))
	n := 0
	for k, v := range mapTags {
		mTags[n] = k + "=" + v[0]
		n++
	}
	sort.Strings(mTags)

	name, tags, err := tagsParse(metric)
	if err != nil {
		t.Errorf("tagParse: %s", err.Error())
	}
	assert.Equal(m.Path, name)
	assert.Equal(mTags, tags)
}

func BenchmarkNetUrlParse(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		u, _ := url.Parse(metric)
		u.Path = "__name__=" + u.Path
		_ = u.Query()
	}
}

func BenchmarkUrlParse(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		u, _ := urlParse(metric)
		u.Path = "__name__=" + u.Path
		_ = u.Query()
	}
}

func BenchmarkTagParse(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = tagsParse(metric)
	}
}
