package RowBinary

import (
	"bytes"
	"testing"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

func TestWriteBufferWriteReversePath(t *testing.T) {
	wb := RowBinary.GetWriteBuffer()
	wb.WriteReversePath([]byte("a1.b2.c3"))

	if bytes.Compare(wb.Bytes(), []byte("\bc3.b2.a1")) != 0 {
		t.FailNow()
	}
}
