package uploader

import (
	"bytes"
	"time"
	"unsafe"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/days1970"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

type Tree struct {
	data     *bytes.Buffer
	uniq     map[string]bool
	uploader *Uploader
}

func (tree *Tree) Success() {
	// copy data from local uniq to global
	for key, _ := range tree.uniq {
		tree.uploader.treeExists.Add(key)
	}
}

func (u *Uploader) MakeTree(filename string) (*Tree, error) {
	reader, err := RowBinary.NewReader(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	days := (&days1970.Days{}).Timestamp(uint32(u.treeDate.Unix()))
	version := uint32(time.Now().Unix())

	tree := &Tree{
		data:     bytes.NewBuffer(nil),
		uniq:     make(map[string]bool),
		uploader: u,
	}

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

		if tree.uniq[unsafeString(name)] {
			continue LineLoop
		}

		p = name
		level = 1
		for index = bytes.IndexByte(p, '.'); index >= 0; index = bytes.IndexByte(p, '.') {
			p = p[index+1:]
			level++
		}

		wb.Reset()

		tree.uniq[string(name)] = true
		wb.WriteUint16(days)
		wb.WriteUint32(uint32(level))
		wb.WriteBytes(name)
		wb.WriteUint32(version)

		// fmt.Println(string(name), level)

		p = name
		for level--; level > 0; level-- {
			index = bytes.LastIndexByte(p, '.')
			if tree.uniq[unsafeString(p[:index+1])] {
				break
			}

			tree.uniq[string(p[:index+1])] = true
			wb.WriteUint16(days)
			wb.WriteUint32(uint32(level))
			wb.WriteBytes(p[:index+1])
			wb.WriteUint32(version)

			// fmt.Println(string(p[:index+1]), level)
			p = p[:index]
		}

		tree.data.Write(wb.Bytes()) // @TODO: error check?
	}

	wb.Release()
	return tree, nil
}
