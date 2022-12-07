package uploader

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/msaf1980/go-stringutils"
	"go.uber.org/zap"
)

type Index struct {
	*cached
}

var _ Uploader = &Index{}
var _ UploaderWithReset = &Index{}

const ReverseLevelOffset = 10000
const TreeLevelOffset = 20000
const ReverseTreeLevelOffset = 30000

const DefaultTreeDate = 42 // 1970-02-12

func NewIndex(base *Base) *Index {
	u := &Index{}
	u.cached = newCached(base)
	u.cached.parser = u.parseFile
	return u
}

func (u *Index) parseName(name, reverseName []byte, treeDate, days uint16, version uint32, disableDailyIndex bool, indexBuf *indexBuffers) error {
	var index, l int
	var p []byte

	sizeIndex := 2 * (RowBinary.SIZE_INT16 /* days */ +
		RowBinary.SIZE_INT32 + len(name) +
		RowBinary.SIZE_INT32) //  version

	if sizeIndex >= indexBuf.wb.FreeSize() {
		return errBufOverflow
	}
	indexBuf.wb.Reset()

	level := pathLevel(name)

	// Tree
	indexBuf.wb.WriteUint16(treeDate)
	indexBuf.wb.WriteUint32(uint32(level + TreeLevelOffset))
	indexBuf.wb.WriteBytes(name)
	indexBuf.wb.WriteUint32(version)

	p = name
	l = level
	for l--; l > 0; l-- {
		index = bytes.LastIndexByte(p, '.')
		segment := p[:index+1]
		if indexBuf.newUniq[unsafeString(segment)] {
			break
		}

		sizeIndex += RowBinary.SIZE_INT16 /* days */ +
			RowBinary.SIZE_INT32 + len(segment) +
			RowBinary.SIZE_INT32 //  version

		if sizeIndex >= indexBuf.wb.FreeSize() {
			return errBufOverflow
		}

		indexBuf.newUniq[string(p[:index+1])] = true

		indexBuf.wb.WriteUint16(treeDate)
		indexBuf.wb.WriteUint32(uint32(l + TreeLevelOffset))
		indexBuf.wb.WriteBytes(p[:index+1])
		indexBuf.wb.WriteUint32(version)

		p = p[:index]
	}

	// Write data with treeDate
	indexBuf.wb.WriteUint16(treeDate)
	indexBuf.wb.WriteUint32(uint32(level + ReverseTreeLevelOffset))
	indexBuf.wb.WriteBytes(reverseName)
	indexBuf.wb.WriteUint32(version)

	// Skip daily index
	if !disableDailyIndex {
		// Direct path with date
		indexBuf.wb.WriteUint16(days)
		indexBuf.wb.WriteUint32(uint32(level))
		indexBuf.wb.WriteBytes(name)
		indexBuf.wb.WriteUint32(version)

		// Reverse path with date
		indexBuf.wb.WriteUint16(days)
		indexBuf.wb.WriteUint32(uint32(level + ReverseLevelOffset))
		indexBuf.wb.WriteBytes(reverseName)
		indexBuf.wb.WriteUint32(version)
	}

	return nil
}

type indexBuffers struct {
	wb          RowBinary.WriteBuffer
	newUniq     map[string]bool
	reverseName []byte
	key         stringutils.Builder
}

var indexBufferPool = sync.Pool{
	New: func() interface{} {
		b := &indexBuffers{
			wb:          RowBinary.WriteBuffer{},
			newUniq:     make(map[string]bool),
			reverseName: make([]byte, 256),
		}
		b.key.Grow(128)
		return b
	},
}

func getIndexBuffer() *indexBuffers {
	b := indexBufferPool.Get().(*indexBuffers)
	b.wb.Reset()
	for k := range b.newUniq {
		delete(b.newUniq, k)
	}

	return b
}

func releaseIndexBuffer(b *indexBuffers) {
	indexBufferPool.Put(b)
}

func (u *Index) parseFile(filename string, out io.Writer) (uint64, map[string]bool, error) {
	var reader *RowBinary.Reader
	var err error
	var n uint64

	reader, err = RowBinary.NewReader(filename, false)
	if err != nil {
		return n, nil, err
	}
	defer reader.Close()

	version := uint32(time.Now().Unix())
	newSeries := make(map[string]bool)
	indexBuf := getIndexBuffer()
	defer releaseIndexBuffer(indexBuf)

	treeDate := uint16(DefaultTreeDate)
	if !u.config.TreeDate.IsZero() {
		treeDate = RowBinary.TimestampToDays(uint32(u.config.TreeDate.Unix()))
	}

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

		// skip tagged
		if bytes.IndexByte(name, '?') >= 0 {
			continue
		}

		nameStr := unsafeString(name)
		indexBuf.key.Reset()
		indexBuf.key.WriteUint(uint64(reader.Days()), 10)
		indexBuf.key.WriteByte(':')
		indexBuf.key.WriteString(hashFunc(nameStr))
		key := indexBuf.key.String()

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newSeries[key] {
			continue LineLoop
		}

		newSeries[stringutils.Clone(key)] = true

		n++
		days := reader.Days()

		l := len(name)
		if l > len(indexBuf.reverseName) {
			indexBuf.reverseName = make([]byte, len(name)*2)
		}
		reverseName := indexBuf.reverseName[:len(name)]
		RowBinary.ReverseBytesTo(reverseName, name)

		err = u.parseName(name, reverseName, treeDate, days, version, u.config.DisableDailyIndex, indexBuf)
		if err != nil {
			u.logger.Warn("parse",
				zap.String("metric", nameStr), zap.String("type", "tagged"), zap.String("name", filename), zap.Error(err),
			)
			continue LineLoop
		}

		// Write data
		_, err = out.Write(indexBuf.wb.Bytes())
		if err != nil {
			return n, nil, err
		}
	}

	return n, newSeries, nil
}
