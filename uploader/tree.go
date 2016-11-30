package uploader

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (u *Uploader) MakeTree(filename string) (io.ReadWriter, error) {
	reader, err := RowBinary.NewReader(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	days := (&days1970.Days{}).Timestamp(uint32(u.treeDate.Unix()))

	treeData := bytes.NewBuffer(nil)

	localUniq := make(map[string]bool)

	// var key string
	var level, index int
	// var exists bool
	var p []byte

	wb := RowBinary.GetWriteBuffer()

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err != nil { // io.EOF or corrupted file
			break
		}

		if u.treeExists.Exists(unsafeString(name)) {
			continue LineLoop
		}

		if localUniq[unsafeString(name)] {
			continue LineLoop
		}

		p = name
		level = 1
		for index = bytes.IndexByte(p, '.'); index >= 0; index = bytes.IndexByte(p, '.') {
			p = p[index+1:]
			level++
		}

		wb.Reset()

		localUniq[string(name)] = true
		wb.WriteUint16(days)
		wb.WriteUint32(uint32(level))
		wb.WriteBytes(name)

		// fmt.Println(string(name), level)

		p = name
		for level--; level > 0; level-- {
			index = bytes.LastIndexByte(p, '.')
			if localUniq[unsafeString(p[:index+1])] {
				break
			}

			localUniq[string(p[:index+1])] = true
			wb.WriteUint16(days)
			wb.WriteUint32(uint32(level))
			wb.WriteBytes(p[:index+1])

			// fmt.Println(string(p[:index+1]), level)
			p = p[:index]
		}

		treeData.Write(wb.Bytes()) // @TODO: error check?
	}

	// copy data from localUniq to global
	for key, _ := range localUniq {
		u.treeExists.Add(key)
	}

	wb.Release()
	return treeData, nil
}
