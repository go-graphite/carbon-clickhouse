package escape

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnescape(t *testing.T) {
	var tests = []struct {
		in   string
		want string
	}{
		{
			"", ""},
		{"abc", "abc"},
		{"1%41", "1A"},
		{"1%41%42%43", "1ABC"},
		{"%4a", "J"},
		{"%6F", "o"},
		{
			"%", // not enough characters after %
			"%",
		},
		{
			"%a", // not enough characters after %
			"%a",
		},
		{
			"%1", // not enough characters after %
			"%1",
		},
		{
			"123%45%6", // not enough characters after %
			"123E%6",
		},
		{
			"%zzzzz", // invalid hex digits
			"%zzzzz",
		},
		{"a+b", "a b"},
		{"a+%3D+b", "a = b"},
	}

	for i, tt := range tests {
		t.Run("["+strconv.Itoa(i)+"] "+tt.in, func(t *testing.T) {
			assert := assert.New(t)

			got := Unescape(tt.in)
			assert.Equal(tt.want, got)

			var sb strings.Builder

			got = UnescapeTo(tt.in, &sb)
			assert.Equal(tt.want, got)

			gotName, gotNameTag := UnescapeNameTo(tt.in, &sb)
			assert.Equal(tt.want, gotName)
			assert.Equal("__name__="+tt.want, gotNameTag)
		})
	}
}

func Test_Query_Unescape(t *testing.T) {
	var tests = []struct {
		in         string
		wantEscape string
	}{
		{"", ""},
		{"abc", "abc"},
		{"%", "%25"},
		{"%a", "%25a"},
		{"a+b", "a%2Bb"},
		{"a b", "a+b"},
		{"a = b", "a+%3D+b"},
	}

	for i, tt := range tests {
		t.Run("["+strconv.Itoa(i)+"] "+tt.in, func(t *testing.T) {
			gotEscape := Query(tt.in)
			if tt.wantEscape != gotEscape {
				t.Fatalf("Query(%q) = %q, want %q", tt.in, gotEscape, tt.wantEscape)
			}
			gotUnescape := Unescape(gotEscape)
			if tt.in != gotUnescape {
				t.Fatalf("Unescape(%q) = %q, want %q", gotEscape, gotUnescape, tt.in)
			}
		})
	}
}
