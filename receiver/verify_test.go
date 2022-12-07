package receiver

import (
	"io"
	"testing"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary/reader"
	"github.com/msaf1980/go-stringutils"
)

func pointToBuf(sb *stringutils.Builder, p *reader.Point) {
	sb.WriteString(p.Path)
	sb.WriteString(" ")
	sb.WriteFloat(p.Value, 'f', -1, 64)
	sb.WriteString(" ")
	sb.WriteUint(uint64(p.Timestamp), 10)
	sb.WriteString("\n")
}

func verifyIndexUploaded(t *testing.T, b io.Reader, points []reader.Point, start, end uint32) {
	var (
		p   *reader.Point
		err error
	)

	br := reader.NewReader(b)

	for i, point := range points {
		if p, err = br.ReadGraphitePoint(); err != nil {
			t.Fatalf("read [%d]: %v,\nwant\n%+v", i, err, point)
		}
		if p.Version < start || p.Version > end {
			t.Errorf("read [%d] version: got %d, want >= %d, <= %d", i, p.Version, start, end)
		}
		p.Version = point.Version
		if *p != point {
			t.Errorf("read [%d]: got\n%+v,\nwant\n%+v", i, *p, point)
		}
	}

	if p, err = br.ReadGraphitePoint(); err == nil {
		t.Fatalf("read at end: got %+v, want '%v'", *p, io.EOF)
	} else if err != io.EOF {
		t.Fatalf("read at end: got '%v', want '%v'", err, io.EOF)
	}
}
