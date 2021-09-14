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
	"github.com/msaf1980/go-stringutils"
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

// don't unescape special symbols
// escape also not needed (all is done in receiver/plain.go, Base.PlainParseLine)
func tagsParse(path string) (string, map[string]string, error) {
	name, args, n := stringutils.Split2(path, "?")
	if n == 1 || args == "" {
		return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	tags := make(map[string]string)
	for {
		if delim := strings.Index(args, "="); delim == -1 {
			// corrupted tag
			break
		} else {
			key := args[0:delim]
			v := args[delim+1:]
			if end := strings.Index(v, "&"); end == -1 {
				tags[key] = args[0:]
				break
			} else {
				end += delim + 1
				tags[key] = args[0:end]
				args = args[end+1:]
			}
		}
	}
	return name, tags, nil
}

func (u *Tagged) parseName(name string, days uint16, version uint32,
	// reusable buffers
	tag1 []string, wb *RowBinary.WriteBuffer, tagsBuf *RowBinary.WriteBuffer) error {

	mPath, tags, err := tagsParse(name)
	if err != nil {
		return err
	}

	wb.Reset()
	tagsBuf.Reset()
	tag1 = tag1[:0]

	t := "__name__=" + mPath
	tag1 = append(tag1, t)
	tagsBuf.WriteString(t)
	tagsWritten := 1

	// calc size for prevent buffer overflow
	sizeTags := RowBinary.SIZE_INT16 /* days */ +
		RowBinary.SIZE_INT64 + len(mPath) +
		RowBinary.SIZE_INT64 + len(name) +
		RowBinary.SIZE_INT64 + //  tagsBuf.Len() not known at this step
		RowBinary.SIZE_INT16 //version

	// don't upload any other tag but __name__
	// if either main metric (m.Path) or each metric (*) is ignored
	ignoreAllButName := u.ignoredMetrics[mPath] || u.ignoredMetrics["*"]
	for _, tag := range tags {
		sizeTags += RowBinary.SIZE_INT16 /* days */ +
			RowBinary.SIZE_INT64 + len(tag) +
			RowBinary.SIZE_INT64 + len(name) +
			RowBinary.SIZE_INT64 + // tagsBuf.Len() not known at this step
			RowBinary.SIZE_INT16 //version

		if sizeTags >= wb.FreeSize() {
			return errBufOverflow
		}

		tagsBuf.WriteString(tag)
		tagsWritten++

		if !ignoreAllButName {
			tag1 = append(tag1, tag)
		}
	}

	sizeTags += len(tag1) * tagsBuf.Len()
	if sizeTags >= wb.FreeSize() {
		return errBufOverflow
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

	tag1 := make([]string, 0, 32)

	hashFunc := u.config.hashFunc
	if hashFunc == nil {
		hashFunc = keepOriginal
	}

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
		key := strconv.Itoa(int(days)) + ":" + hashFunc(nameStr)
		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newTagged[key] {
			// already processed
			continue LineLoop
		}

		n++

		version := uint32(time.Now().Unix())
		if err = u.parseName(nameStr, days, version, tag1, wb, tagsBuf); err != nil {
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
