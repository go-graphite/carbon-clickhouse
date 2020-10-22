package uploader

import (
	"testing"

	"github.com/msaf1980/stringutils"
	"github.com/spaolacci/murmur3"
)

func Benchmark_fnv32(b *testing.B) {
	s := "asdfghjklqwe.rtyuiopzxc.vbnm1234567890"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fnv32(s)
	}
}

func Benchmark_murmur32(b *testing.B) {
	s := "asdfghjklqwe.rtyuiopzxc.vbnm1234567890"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		murmur3.Sum32(stringutils.UnsafeStringBytes(&s))
	}
}
