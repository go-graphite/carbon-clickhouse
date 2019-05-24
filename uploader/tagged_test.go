package uploader

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUrlParse(t *testing.T) {
	assert := assert.New(t)

	// make metric name as receiver
	metric := url.PathEscape("instance:cpu_utilization:ratio_avg") +
		"?" + url.QueryEscape("dc") + "=" + url.QueryEscape("qwe") +
		"&" + url.QueryEscape("fqdn") + "=" + url.QueryEscape("asd") +
		"&" + url.QueryEscape("instance") + "=" + url.QueryEscape("10.33.10.10:9100") +
		"&" + url.QueryEscape("job") + "=" + url.QueryEscape("node")

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
