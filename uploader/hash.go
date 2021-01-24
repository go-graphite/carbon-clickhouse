package uploader

import (
	"encoding/binary"

	"github.com/zentures/cityhash"
)

var knownHash = map[string](func(string) string){
	"":       keepOriginal,
	"city64": cityHash64,
}

func keepOriginal(s string) string {
	return s
}

func cityHash64(s string) string {
	p := []byte(s)
	h := cityhash.CityHash64(p, uint32(len(p)))

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], h)
	return string(b[:])
}
