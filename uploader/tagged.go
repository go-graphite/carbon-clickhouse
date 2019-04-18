package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
)

type Tagged struct {
	*cached
}

var _ Uploader = &Tagged{}
var _ UploaderWithReset = &Tagged{}

func NewTagged(base *Base) *Tagged {
	u := &Tagged{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	u.query = fmt.Sprintf("%s (Date, Tag1, Path, Tags, Version)", u.config.TableName)
	return u
}

func (u *Tagged) parseFile(filename string, out io.Writer) (map[string]bool, error) {
	var reader *RowBinary.Reader
	var err error

	reader, err = RowBinary.NewReader(filename, u.config.CompAlgo, false)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	version := uint32(time.Now().Unix())

	newTagged := make(map[string]bool)

	wb := RowBinary.GetWriteBuffer()
	tagsBuf := RowBinary.GetWriteBuffer()
	defer wb.Release()
	defer tagsBuf.Release()

	tag1 := make([]string, 0)

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		// skip not tagged
		if bytes.IndexByte(name, '?') < 0 {
			continue
		}

		key := fmt.Sprintf("%d:%s", reader.Days(), unsafeString(name))

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newTagged[key] {
			continue LineLoop
		}

		m, err := url.Parse(unsafeString(name))
		if err != nil {
			continue
		}

		newTagged[key] = true

		wb.Reset()
		tagsBuf.Reset()
		tag1 = tag1[:0]

		t := fmt.Sprintf("__name__=%s", m.Path)
		tag1 = append(tag1, t)
		tagsBuf.WriteString(t)

		for k, v := range m.Query() {
			t := fmt.Sprintf("%s=%s", k, v[0])
			tag1 = append(tag1, t)
			tagsBuf.WriteString(t)
		}

		for i := 0; i < len(tag1); i++ {
			wb.WriteUint16(reader.Days())
			wb.WriteString(tag1[i])
			wb.WriteBytes(name)
			wb.WriteUVarint(uint64(len(tag1)))
			wb.Write(tagsBuf.Bytes())
			wb.WriteUint32(version)
		}

		_, err = out.Write(wb.Bytes())
		if err != nil {
			return nil, err
		}
	}

	return newTagged, nil
}
