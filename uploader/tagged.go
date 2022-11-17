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

func ishex(c byte) bool {
	switch {
	case '0' <= c && c <= '9':
		return true
	case 'a' <= c && c <= 'f':
		return true
	case 'A' <= c && c <= 'F':
		return true
	}
	return false
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

func isPercentEscape(s string, i int) bool {
	return i+2 < len(s) && ishex(s[i+1]) && ishex(s[i+2])
}

// unescape unescapes a string; the mode specifies
// which section of the URL string is being unescaped.
func unescape(s string) string {
	first := strings.IndexByte(s, '%')
	if first == -1 {
		return s
	}
	var t strings.Builder
	t.Grow(len(s))
	t.WriteString(s[:first])

LOOP:
	for i := first; i < len(s); i++ {
		switch s[i] {
		case '%':
			if len(s) < i+3 {
				t.WriteString(s[i:])
				break LOOP
			}
			if !isPercentEscape(s, i) {
				t.WriteString(s[i : i+3])
			} else {
				t.WriteByte(unhex(s[i+1])<<4 | unhex(s[i+2]))
			}
			i += 2
		default:
			t.WriteByte(s[i])
		}
	}

	return t.String()
}

// Unescape is needed, tags already sorted in in helper/tags/graphite.go, tags.Graphite
func tagsParse(path string) (string, map[string]string, error) {
	delim := strings.IndexRune(path, '?')
	if delim < 1 {
		return "", nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	name, err := url.PathUnescape(path[:delim])
	if err != nil {
		return "", nil, fmt.Errorf("invalid name tag in '%s'", path)
	}
	args := path[delim+1:]
	tags := make(map[string]string)
	for {
		if delim = strings.IndexRune(args, '='); delim == -1 {
			// corrupted tag
			break
		} else {
			key := unescape(args[0:delim])
			v := args[delim+1:]
			if end := strings.IndexRune(v, '&'); end == -1 {
				tags[key] = unescape(args[0:])
				break
			} else {
				end += delim + 1
				tags[key] = unescape(args[0:end])
				args = args[end+1:]
			}

		}
	}
	return name, tags, nil
}

// Unescape is needed, tags already sorted in in helper/tags/graphite.go, tags.Graphite
func tagsParseToSlice(path string) (string, []string, error) {
	delim := strings.IndexRune(path, '?')
	if delim < 1 {
		return "", nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	name := unescape(path[:delim])
	args := path[delim+1:]
	tags := make([]string, 0, 32)

	for {
		if delim = strings.IndexRune(args, '='); delim == -1 {
			// corrupted tag
			break
		} else {
			v := args[delim+1:]
			if end := strings.IndexRune(v, '&'); end == -1 {
				tags = append(tags, unescape(args[0:]))
				break
			} else {
				tags = append(tags, unescape(args[0:end+delim+1]))
				end += delim + 1
				args = args[end+1:]
			}
		}
	}
	return name, tags, nil
}

func (u *Tagged) parseName(name string, days uint16, version uint32,
	// reusable buffers
	tag1 []string, wb *RowBinary.WriteBuffer, tagsBuf *RowBinary.WriteBuffer) error {

	mPath, tags, err := tagsParseToSlice(name)
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
	for _, t := range tags {
		sizeTags += RowBinary.SIZE_INT16 /* days */ +
			RowBinary.SIZE_INT64 + len(t) +
			RowBinary.SIZE_INT64 + len(name) +
			RowBinary.SIZE_INT64 + // tagsBuf.Len() not known at this step
			RowBinary.SIZE_INT16 //version

		if sizeTags >= wb.FreeSize() {
			return errBufOverflow
		}

		tagsBuf.WriteString(t)
		tagsWritten++

		if !ignoreAllButName {
			tag1 = append(tag1, t)
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

	version := uint32(time.Now().Unix())

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
