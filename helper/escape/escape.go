package escape

// Path escapes the string so it can be safely used as a URL path.
func Path(s string) string {
	return escape(s, encodePath)
}

// Query escapes the string so it can be safely placed inside a URL query.
func Query(s string) string {
	return escape(s, encodeQueryComponent)
}
