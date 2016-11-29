package uploader

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func MakeTree(filename string) (io.ReadWriter, error) {
	reader, err := NewReader(filename)

	if err != nil {
		return nil, err
	}
	defer reader.Close()

	treeData := bytes.NewBuffer(nil)

	localUniq := make(map[string]bool)

	// var key string
	var level, index int
	// var exists bool
	var p []byte

LineLoop:
	for {
		name, err := reader.ReadRecord()
		if err == io.EOF {
			break
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

		localUniq[string(name)] = true
		fmt.Println(string(name), level)

		p = name
		for level--; level > 0; level-- {
			index = bytes.LastIndexByte(p, '.')
			if localUniq[unsafeString(p[:index+1])] {
				break
			}

			localUniq[string(p[:index+1])] = true
			fmt.Println(string(p[:index+1]), level)
			p = p[:index]
		}
	}

	return treeData, nil
	// 	row := strings.Split(string(line), "\t")
	// 	metric := row[0]

	// 	if u.treeExists.Exists(metric) {
	// 		continue LineLoop
	// 	}

	// 	if _, exists = localUniq[metric]; exists {
	// 		continue LineLoop
	// 	}

	// 	offset := 0
	// 	for level = 1; ; level++ {
	// 		p := strings.IndexByte(metric[offset:], '.')
	// 		if p < 0 {
	// 			break
	// 		}
	// 		key = metric[:offset+p+1]

	// 		if !u.treeExists.Exists(key) {
	// 			if _, exists := localUniq[key]; !exists {
	// 				localUniq[key] = true
	// 				fmt.Fprintf(treeData, "%s\t%d\t%s\n", u.treeDate, level, key)
	// 			}
	// 		}

	// 		offset += p + 1
	// 	}

	// 	localUniq[metric] = true
	// 	fmt.Fprintf(treeData, "%s\t%d\t%s\n", u.treeDate, level, metric)
	// }

	// // @TODO: insert to tree data metrics
	// err = uploadData(u.clickHouseDSN, u.treeTable, u.treeTimeout, treeData)
	// if err != nil {
	// 	return err
	// }

	// // copy data from localUniq to global
	// for key, _ = range localUniq {
	// 	u.treeExists.Add(key)
	// }
}
