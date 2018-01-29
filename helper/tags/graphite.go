package tags

import (
	"bytes"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

type byKey []string

func (a byKey) Len() int      { return len(a) }
func (a byKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool {
	p1 := strings.Index(a[i], "=") - 1
	if p1 < 0 {
		p1 = len(a[i])
	}
	p2 := strings.Index(a[j], "=") - 1
	if p2 < 0 {
		p2 = len(a[j])
	}

	return strings.Compare(a[i][:p1+1], a[j][:p2+1]) < 0
}

func Graphite(s string) (string, error) {
	if strings.IndexByte(s, ';') < 0 {
		return s, nil
	}

	arr := strings.Split(s, ";")

	if len(arr[0]) == 0 {
		return "", fmt.Errorf("cannot parse path %#v, no metric found", s)
	}

	// check tags
	for i := 1; i < len(arr); i++ {
		if strings.Index(arr[i], "=") < 1 {
			return "", fmt.Errorf("cannot parse path %#v, invalid segment %#v", s, arr[i])
		}
	}

	sort.Stable(byKey(arr[1:]))

	// uniq
	toDel := 0
	prevKey := ""
	for i := 1; i < len(arr); i++ {
		p := strings.Index(arr[i], "=")
		key := arr[i][:p]
		if key == prevKey {
			toDel++
		} else {
			prevKey = key
		}
		if toDel > 0 {
			arr[i-toDel] = arr[i]
		}
	}

	var res bytes.Buffer

	arr = arr[:len(arr)-toDel]

	if len(arr) > 0 {
		res.WriteString(url.PathEscape(arr[0]))
		res.WriteByte('?')
	}

	for i := 1; i < len(arr); i++ {
		if i > 1 {
			res.WriteByte('&')
		}
		p := strings.Index(arr[i], "=")
		res.WriteString(url.QueryEscape(arr[i][:p]))
		res.WriteByte('=')
		res.WriteString(url.QueryEscape(arr[i][p+1:]))
	}

	return res.String(), nil
}
