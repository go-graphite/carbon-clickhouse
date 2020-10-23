# stringutils

Some string utils

`UnsafeString([]byte) string` return unsafe string from bytes slice indirectly (without allocation)
`UnsafeStringFromPtr(*byte, length) string` return unsafe string from bytes slice pointer indirectly (without allocation)
`UnsafeStringBytes(*string) []bytes` return unsafe string bytes indirectly (without allocation)

`Split2(s string, sep string) (string, string, int)`  Split2 return the split string results (without memory allocations)

`Builder` very simular to strings.Builder, but has better perfomance (at golang 1.14).
