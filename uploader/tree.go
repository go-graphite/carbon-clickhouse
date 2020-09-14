package uploader

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
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
	if u.config.TreeDate.IsZero() {
		u.query = fmt.Sprintf("%s (Level, Path, Version)", u.config.TableName)
	} else {
		u.query = fmt.Sprintf("%s (Date, Level, Path, Version)", u.config.TableName)
	}
	return u
}

func (u *Tree) parseFile(filename string, out io.Writer) (uint64, map[string]bool, error) {
	var n uint64

	reader, err := RowBinary.NewReader(filename, false)
	if err != nil {
		return n, nil, err
	}
	defer reader.Close()

	version := uint32(time.Now().Unix())

	var days uint16
	if !u.config.TreeDate.IsZero() {
		days = RowBinary.TimestampToDays(uint32(u.config.TreeDate.Unix()))
	}

	newSeries := make(map[string]bool)

	var level, index, l int
	var p []byte

	writePathLevel := func(p []byte, level int) error {
		if days > 0 {
			if err := RowBinary.WriteUint16(out, days); err != nil {
				return err
			}
		}
		if err := RowBinary.WriteUint32(out, uint32(level)); err != nil {
			return err
		}
		if err := RowBinary.WriteBytes(out, p); err != nil {
			return err
		}
		if err := RowBinary.WriteUint32(out, version); err != nil {
			return err
		}
		return nil
	}

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		// skip tagged
		if bytes.IndexByte(name, '?') >= 0 {
			continue
		}

		if u.existsCache.Exists(unsafeString(name)) {
			continue LineLoop
		}

		if newSeries[unsafeString(name)] {
			continue LineLoop
		}
		n++

		level = pathLevel(name)

		newSeries[string(name)] = true

		if err = writePathLevel(name, level); err != nil {
			return n, nil, err
		}

		p = name
		l = level
		for l--; l > 0; l-- {
			index = bytes.LastIndexByte(p, '.')
			if newSeries[unsafeString(p[:index+1])] {
				break
			}

			newSeries[string(p[:index+1])] = true

			if err = writePathLevel(p[:index+1], l); err != nil {
				return n, nil, err
			}

			p = p[:index]
		}
	}

	return n, newSeries, nil
}
