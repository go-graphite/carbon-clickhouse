package escape

import "github.com/msaf1980/go-stringutils"

// Path escapes the string so it can be safely used as a URL path.
func Path(s string) string {
	return escape(s, encodePath)
}

func PathTo(s string, sb *stringutils.Builder) {
	escapeTo(s, encodePath, sb)
}

// Query escapes the string so it can be safely placed inside a URL query.
func Query(s string) string {
	return escape(s, encodeQueryComponent)
}

func QueryTo(s string, sb *stringutils.Builder) {
	escapeTo(s, encodeQueryComponent, sb)
}
