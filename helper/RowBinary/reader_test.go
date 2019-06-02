package RowBinary

import (
	"io"
	"io/ioutil"
	"testing"
)

func BenchmarkReadFileDirect(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := NewReader("testdata/default.1559465733030407809", false)
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil || n != 20037 {
			b.Fatal()
		}
	}
}

func BenchmarkReadFileReverse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := NewReader("testdata/default.1559465733030407809", true)
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil || n != 20037 {
			b.Fatal()
		}
	}
}
