package uploader

import (
	"fmt"
	"testing"
)

func Benchmark_KeyFmt(b *testing.B) {
	name := []byte("0.thread_idle.100_CACHE.threadpool.storage.tvoara-pez030.1.OLT.AT.PROD.SC")

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d%s", 123, unsafeString(name))
	}
}

func Benchmark_KeyConcat(b *testing.B) {
	days := []byte{0x00, 0xAB}
	name := []byte("0.thread_idle.100_CACHE.threadpool.storage.tvoara-pez030.1.OLT.AT.PROD.SC")

	for i := 0; i < b.N; i++ {
		_ = unsafeString(days) + unsafeString(name)
	}
}
