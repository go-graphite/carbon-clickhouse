package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"go.uber.org/zap"
)

type Tagged struct {
	*cached
	ignoredMetrics map[string]bool
}

var _ Uploader = &Tagged{}
var _ UploaderWithReset = &Tagged{}

var errBufOverflow = fmt.Errorf("output buffer overflow")

func NewTagged(base *Base) *Tagged {
	u := &Tagged{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	u.query = fmt.Sprintf("%s (Date, Tag1, Path, Tags, Version)", u.config.TableName)

	u.ignoredMetrics = make(map[string]bool, len(u.config.IgnoredTaggedMetrics))
	for _, metric := range u.config.IgnoredTaggedMetrics {
		u.ignoredMetrics[metric] = true
	}

	return u
}

func urlParse(rawurl string) (*url.URL, error) {
	p := strings.IndexByte(rawurl, '?')
	if p < 0 {
		return url.Parse(rawurl)
	}
	m, err := url.Parse(rawurl[p:])
	if m != nil {
		m.Path, err = url.PathUnescape(rawurl[:p])
		if err != nil {
			return nil, err
		}
	}
	return m, err
}

func (u *Tagged) parseName(name string, days uint16,
	// reusable buffers
	tag1 []string, wb *RowBinary.WriteBuffer, tagsBuf *RowBinary.WriteBuffer) error {

	m, err := urlParse(name)
	if err != nil {
		return err
	}

	version := uint32(time.Now().Unix())

	wb.Reset()
	tagsBuf.Reset()
	tag1 = tag1[:0]

	mName := m.Path
	t := fmt.Sprintf("__name__=%s", mName)
	tag1 = append(tag1, t)
	tagsBuf.WriteString(t)

	// calc size for prevent w
	sizeTags := 2 + 8 + len(mName) + 8 + len(name) + 8 + tagsBuf.Len() + 2

	// don't upload any other tag but __name__
	// if either main metric (m.Path) or each metric (*) is ignored
	ignoreAllButName := u.ignoredMetrics[mName] || u.ignoredMetrics["*"]
	tagsWritten := 1
	for k, v := range m.Query() {
		t := fmt.Sprintf("%s=%s", k, v[0])

		sizeTags += 2 + 8 + len(t) + 8 + len(name) + 8 + tagsBuf.Len() + 4
		if sizeTags >= wb.FreeSize() {
			return errBufOverflow
		}

		tagsBuf.WriteString(t)
		tagsWritten++

		if !ignoreAllButName {
			tag1 = append(tag1, t)
		}
	}

	for i := 0; i < len(tag1); i++ {
		wb.WriteUint16(days)
		wb.WriteString(tag1[i])
		wb.WriteString(name)
		wb.WriteUVarint(uint64(tagsWritten))
		wb.Write(tagsBuf.Bytes())
		wb.WriteUint32(version)
	}

	return nil
}

func (u *Tagged) parseFile(filename string, out io.Writer) (uint64, map[string]bool, error) {
	var reader *RowBinary.Reader
	var err error
	var n uint64

	reader, err = RowBinary.NewReader(filename, false)
	if err != nil {
		return n, nil, err
	}
	defer reader.Close()

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

		nameStr := unsafeString(name)

		days := reader.Days()
		key := strconv.Itoa(int(days)) + ":" + nameStr
		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newTagged[key] {
			// already processed
			continue LineLoop
		}

		n++

		if err = u.parseName(nameStr, days, tag1, wb, tagsBuf); err != nil {
			u.logger.Warn("parse",
				zap.String("metric", nameStr), zap.String("type", "tagged"), zap.String("name", filename), zap.Error(err),
			)
			continue LineLoop
		} else if _, err = out.Write(wb.Bytes()); err != nil {
			return n, nil, err
		}
		newTagged[key] = true
	}

	return n, newTagged, nil
}
