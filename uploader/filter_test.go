package uploader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchExact(t *testing.T) {
	blacklist := NewBlacklist([]string{
		"a.b.c.d.e",
		"a.b.c.d.f",
		"xxx.yyy.zz.tt",
		"1234.2345.3456.4567.5678.67890",
	})

	assert.True(t, blacklist.Contains("a.b.c.d.e", false))
	assert.True(t, blacklist.Contains("a.b.c.d.f", false))
	assert.True(t, blacklist.Contains("xxx.yyy.zz.tt", false))
	assert.True(t, blacklist.Contains("1234.2345.3456.4567.5678.67890", false))

	assert.True(t, blacklist.Contains("e.d.c.b.a", true))
	assert.True(t, blacklist.Contains("f.d.c.b.a", true))
	assert.True(t, blacklist.Contains("tt.zz.yyy.xxx", true))
	assert.True(t, blacklist.Contains("67890.5678.4567.3456.2345.1234", true))

	assert.False(t, blacklist.Contains("", false))
	assert.False(t, blacklist.Contains("a.b.c.d", false))
	assert.False(t, blacklist.Contains("a.a.a.a.a", false))
	assert.False(t, blacklist.Contains("a.b.c.a.e", false))
	assert.False(t, blacklist.Contains("a.b.c.d.g", false))

	assert.False(t, blacklist.Contains("", true))
	assert.False(t, blacklist.Contains("d.c.b.a", true))
	assert.False(t, blacklist.Contains("a.a.a.a.a", true))
	assert.False(t, blacklist.Contains("e.d.c.b.b", true))
	assert.False(t, blacklist.Contains("f.d.c.a.a", true))
	assert.False(t, blacklist.Contains("g.d.c.b.a", true))
}

func TestMatchWildcard(t *testing.T) {
	blacklist := NewBlacklist([]string{
		"*",
		"aa.*.bb",
		"aa.bb.*",
		"aa.*.bb.*.cc",
		"*.*.*.*",
	})

	assert.True(t, blacklist.Contains("xyz", false))
	assert.True(t, blacklist.Contains("aa.bb.cc", false))
	assert.True(t, blacklist.Contains("aa.cc.bb", false))
	assert.True(t, blacklist.Contains("aa.xyz.bb.hhh.cc", false))
	assert.True(t, blacklist.Contains("1.2.3.4", false))

	assert.True(t, blacklist.Contains("abc", true))
	assert.True(t, blacklist.Contains("cc.bb.aa", true))
	assert.True(t, blacklist.Contains("bb.cc.aa", true))
	assert.True(t, blacklist.Contains("cc.1234.bb.5678.aa", true))
	assert.True(t, blacklist.Contains("1.2.3.4", true))

	assert.False(t, blacklist.Contains("aa.bb", false))
	assert.False(t, blacklist.Contains("aa.dd.bc", false))
	assert.False(t, blacklist.Contains("aa.bb.bc.dd.cc", false))
}
