package uploader

import (
	"bytes"
	"unsafe"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func pathLevel(name []byte) int {
	p := name
	level := 1
	for index := bytes.IndexByte(p, '.'); index >= 0; index = bytes.IndexByte(p, '.') {
		p = p[index+1:]
		level++
	}
	return level
}
