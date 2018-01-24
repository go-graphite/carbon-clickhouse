package uploader

import (
	"bytes"
	"io"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
)

type Tree struct {
	*cached
}

var _ Uploader = &Tree{}
var _ UploaderWithReset = &Tree{}

func NewTree(base *Base) *Tree {
	u := &Tree{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	return u
}

func (u *Tree) parseFile(filename string, out io.Writer) (map[string]bool, error) {
	reader, err := RowBinary.NewReader(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	days := (&days1970.Days{}).Timestamp(uint32(u.config.TreeDate.Unix()))
	version := uint32(time.Now().Unix())

	newSeries := make(map[string]bool)

	wb := RowBinary.GetWriteBuffer()

	var level, index, l int
	var p []byte

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		if u.existsCache.Exists(unsafeString(name)) {
			continue LineLoop
		}

		if newSeries[unsafeString(name)] {
			continue LineLoop
		}

		level = pathLevel(name)

		wb.Reset()

		newSeries[string(name)] = true
		wb.WriteUint16(days)
		wb.WriteUint32(uint32(level))
		wb.WriteBytes(name)
		wb.WriteUint32(version)

		p = name
		l = level
		for l--; l > 0; l-- {
			index = bytes.LastIndexByte(p, '.')
			if newSeries[unsafeString(p[:index+1])] {
				break
			}

			newSeries[string(p[:index+1])] = true
			wb.WriteUint16(days)
			wb.WriteUint32(uint32(l))
			wb.WriteBytes(p[:index+1])
			wb.WriteUint32(version)

			p = p[:index]
		}

		_, err = out.Write(wb.Bytes())
		if err != nil {
			return nil, err
		}
	}

	wb.Release()

	return newSeries, nil
}
