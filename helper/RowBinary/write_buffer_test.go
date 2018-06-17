package RowBinary

import (
	"bytes"
	"testing"
)

func TestWriteBufferWriteReversePath(t *testing.T) {
	wb := GetWriteBuffer()
	wb.WriteReversePath([]byte("a1.b2.c3"))

	if !bytes.Equal(wb.Bytes(), []byte("\bc3.b2.a1")) {
		t.FailNow()
	}
}
