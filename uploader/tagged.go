package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/escape"
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
			key := escape.Unescape(args[0:delim])
			v := args[delim+1:]
			if end := strings.IndexRune(v, '&'); end == -1 {
				tags[key] = escape.Unescape(args[0:])
				break
			} else {
				end += delim + 1
				tags[key] = escape.Unescape(args[0:end])
				args = args[end+1:]
			}

		}
	}
	return name, tags, nil
}

// Unescape is needed, tags already sorted in in helper/tags/graphite.go, tags.Graphite
func tagsParseToSlice(path string, tagsBuff *tagsBuffers) (string, string, []string, error) {
	delim := strings.IndexRune(path, '?')
	if delim < 1 {
		return "", "", nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	tagsBuff.nameBuf.Reset()
	tagsBuff.tagsParse = tagsBuff.tagsParse[:0]
	tagsBuff.nameBuf.Grow(len(path))
	name, nameTag := escape.UnescapeNameTo(path[:delim], &tagsBuff.nameBuf)
	args := path[delim+1:]

	for {
		if delim = strings.IndexRune(args, '='); delim == -1 {
			// corrupted tag
			break
		} else {
			v := args[delim+1:]
			if end := strings.IndexRune(v, '&'); end == -1 {
				tagsBuff.tagsParse = append(tagsBuff.tagsParse, escape.UnescapeTo(args[0:], &tagsBuff.nameBuf))
				break
			} else {
				tagsBuff.tagsParse = append(tagsBuff.tagsParse, escape.UnescapeTo(args[0:end+delim+1], &tagsBuff.nameBuf))
				end += delim + 1
				args = args[end+1:]
			}
		}
	}
	return name, nameTag, tagsBuff.tagsParse, nil
}

func (u *Tagged) parseName(name string, days uint16, version uint32,
	// reusable buffers
	tagsBuff *tagsBuffers) error {

	mPath, nameTag, tags, err := tagsParseToSlice(name, tagsBuff)
	if err != nil {
		return err
	}

	tagsBuff.wb.Reset()
	tagsBuff.tagsBuf.Reset()
	tagsBuff.tag1 = tagsBuff.tag1[:0]

	tagsBuff.tag1 = append(tagsBuff.tag1, nameTag)
	tagsBuff.tagsBuf.WriteString(nameTag)
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

		if sizeTags >= tagsBuff.wb.FreeSize() {
			return errBufOverflow
		}

		tagsBuff.tagsBuf.WriteString(t)
		tagsWritten++

		if !ignoreAllButName {
			tagsBuff.tag1 = append(tagsBuff.tag1, t)
		}
	}

	sizeTags += len(tagsBuff.tag1) * tagsBuff.tagsBuf.Len()
	if sizeTags >= tagsBuff.wb.FreeSize() {
		return errBufOverflow
	}

	for i := 0; i < len(tagsBuff.tag1); i++ {
		tagsBuff.wb.WriteUint16(days)
		tagsBuff.wb.WriteString(tagsBuff.tag1[i])
		tagsBuff.wb.WriteString(name)
		tagsBuff.wb.WriteUVarint(uint64(tagsWritten))
		tagsBuff.wb.Write(tagsBuff.tagsBuf.Bytes())
		tagsBuff.wb.WriteUint32(version)
	}

	return nil
}

type tagsBuffers struct {
	wb        RowBinary.WriteBuffer
	tagsBuf   RowBinary.WriteBuffer
	nameBuf   strings.Builder
	tag1      []string
	tagsParse []string
}

var tagsBufferPool = sync.Pool{
	New: func() interface{} {
		b := &tagsBuffers{
			wb:        RowBinary.WriteBuffer{},
			tagsBuf:   RowBinary.WriteBuffer{},
			tag1:      make([]string, 0, 64),
			tagsParse: make([]string, 0, 64),
		}
		b.nameBuf.Grow(512)
		return b
	},
}

func getTagsBuffer() *tagsBuffers {
	b := tagsBufferPool.Get().(*tagsBuffers)
	b.wb.Reset()
	b.tagsBuf.Reset()

	return b
}

func releaseTagsBuffer(b *tagsBuffers) {
	tagsBufferPool.Put(b)
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

	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)

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

		if err = u.parseName(nameStr, days, version, tagsBuf); err != nil {
			u.logger.Warn("parse",
				zap.String("metric", nameStr), zap.String("type", "tagged"), zap.String("name", filename), zap.Error(err),
			)
			continue LineLoop
		} else if _, err = out.Write(tagsBuf.wb.Bytes()); err != nil {
			return n, nil, err
		}
		// tagsBuf.wb.Reset()
		// tagsBuf.tagsBuf.Reset()
		newTagged[key] = true
	}

	return n, newTagged, nil
}
