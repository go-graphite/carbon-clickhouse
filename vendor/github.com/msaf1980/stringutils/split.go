package stringutils

import "strings"

var empthy = ""

// Split2 return the split string results (without memory allocations)
//   If sep string not found: 's' '' 1
//   If s or sep string is empthy: 's' '' 1
//   In other cases: 's0' 's2' 2
func Split2(s string, sep string) (string, string, int) {
	if sep == "" {
		return s, empthy, 1
	}

	if pos := strings.Index(s, sep); pos == -1 {
		return s, empthy, 1
	} else if pos == len(s)-1 {
		return s[0:pos], empthy, 2
	} else {
		return s[0:pos], s[pos+1:], 2
	}
}
