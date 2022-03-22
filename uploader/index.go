package uploader

import (
	"bytes"
	"io"
	"strconv"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
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
	newUniq := make(map[string]bool)
	wb := RowBinary.GetWriteBuffer()

	var level, index, l int
	var p []byte

	treeDate := uint16(DefaultTreeDate)
	if !u.config.TreeDate.IsZero() {
		treeDate = RowBinary.TimestampToDays(uint32(u.config.TreeDate.Unix()))
	}

	reverseNameBuf := make([]byte, 256)

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

		key := hashFunc(strconv.Itoa(int(reader.Days())) + ":" + unsafeString(name))

		if u.existsCache.Exists(key) {
			continue LineLoop
		}

		if newSeries[key] {
			continue LineLoop
		}
		n++

		level = pathLevel(name)
		days := reader.Days()

		wb.Reset()

		newSeries[key] = true

		// Tree
		wb.WriteUint16(treeDate)
		wb.WriteUint32(uint32(level + TreeLevelOffset))
		wb.WriteBytes(name)
		wb.WriteUint32(version)

		p = name
		l = level
		for l--; l > 0; l-- {
			index = bytes.LastIndexByte(p, '.')
			if newUniq[unsafeString(p[:index+1])] {
				break
			}

			newUniq[string(p[:index+1])] = true

			wb.WriteUint16(treeDate)
			wb.WriteUint32(uint32(l + TreeLevelOffset))
			wb.WriteBytes(p[:index+1])
			wb.WriteUint32(version)

			p = p[:index]
		}

		// Reverse path without date
		l = len(name)
		if l > len(reverseNameBuf) {
			reverseNameBuf = make([]byte, len(name)*2)
		}
		reverseName := reverseNameBuf[0:l]
		RowBinary.ReverseBytesTo(reverseName, name)

		wb.WriteUint16(treeDate)
		wb.WriteUint32(uint32(level + ReverseTreeLevelOffset))
		wb.WriteBytes(reverseName)
		wb.WriteUint32(version)

		// Write data with treeDate
		_, err = out.Write(wb.Bytes())
		if err != nil {
			return n, nil, err
		}

		// Skip daily index
		if u.config.DisableDailyIndex {
			continue LineLoop
		}

		// Direct path with date
		wb.WriteUint16(days)
		wb.WriteUint32(uint32(level))
		wb.WriteBytes(name)
		wb.WriteUint32(version)

		// Reverse path with date
		wb.WriteUint16(days)
		wb.WriteUint32(uint32(level + ReverseLevelOffset))
		wb.WriteBytes(reverseName)
		wb.WriteUint32(version)

		// Write data with daily index
		_, err = out.Write(wb.Bytes())
		if err != nil {
			return n, nil, err
		}
	}

	wb.Release()

	return n, newSeries, nil
}
