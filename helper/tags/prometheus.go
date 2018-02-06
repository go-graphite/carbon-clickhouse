package tags

import (
	"bytes"
	"net/url"
	"sort"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/prompb"
)

type labelByName []*prompb.Label

func (a labelByName) Len() int      { return len(a) }
func (a labelByName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a labelByName) Less(i, j int) bool {
	return strings.Compare(a[i].Name, a[j].Name) < 0
}

func Prometheus(labels []*prompb.Label) (string, error) {
	var nameOffset int
	for i := 0; i < len(labels); i++ {
		if labels[i].Name == "__name__" {
			if i != 0 {
				labels[0], labels[i] = labels[i], labels[0]
			}
			nameOffset = 1
			break
		}
	}

	sort.Stable(labelByName(labels[nameOffset:]))

	var res bytes.Buffer

	if nameOffset > 0 {
		res.WriteString(url.PathEscape(labels[0].Value))
	}

	res.WriteByte('?')

	for i := 1; i < len(labels); i++ {
		if i > 1 {
			res.WriteByte('&')
		}
		res.WriteString(url.QueryEscape(labels[i].Name))
		res.WriteByte('=')
		res.WriteString(url.QueryEscape(labels[i].Value))
	}

	return res.String(), nil
}
