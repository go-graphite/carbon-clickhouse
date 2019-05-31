package receiver

import (
	"bytes"
	"errors"
	"net/url"

	"github.com/lomik/carbon-clickhouse/helper/pb"
)

type prometheusLabel struct {
	name  []byte
	value []byte
}

type prometheusMetricBuffer struct {
	labels      []prometheusLabel
	queryEscape map[string]string
	metric      []string // ["name", "key1", "value1", ...]
	metricUsed  int
	// labelsEncoded bytes.Buffer
}

func newPrometheusMetricBuffer() *prometheusMetricBuffer {
	return &prometheusMetricBuffer{
		labels:      make([]prometheusLabel, 16),
		queryEscape: make(map[string]string),
		metric:      make([]string, 128),
	}
}

func shouldEscapeByte(c byte) bool {
	// §2.3 Unreserved characters (alphanum)
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '~': // §2.3 Unreserved characters (mark)
		return false
	}

	// Everything else must be escaped.
	return true
}

func shouldQueryEscape(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if shouldEscapeByte(b[i]) {
			return true
		}
	}

	return false
}

func (mb *prometheusMetricBuffer) urlQueryEscape(b []byte) string {
	bs := unsafeString(b)
	if !shouldQueryEscape(b) {
		return bs
	}

	v, exists := mb.queryEscape[bs]
	if exists {
		return v
	}

	v = url.QueryEscape(bs)
	mb.queryEscape[bs] = v
	return v
}

// timeSeries returns (["metric_name", "key1", "value1", "key2", "value2", ...], firstSamplesOffset, error)
func (mb *prometheusMetricBuffer) timeSeries(tsBody []byte) ([]string, int, error) {
	ts := tsBody
	labelIndex := 0
	var label []byte
	var err error
	var samplesOffset int
	var firstLabelLen int

	for len(ts) > 0 {
		switch ts[0] {
		case 0x0a: // repeated Label labels = 1;
			if len(mb.labels) < labelIndex+1 {
				mb.labels = append(mb.labels, prometheusLabel{})
			}
			mb.labels[labelIndex].name = nil
			mb.labels[labelIndex].value = nil

			if label, ts, err = pb.Bytes(ts[1:]); err != nil {
				return nil, 0, err
			}

			if firstLabelLen == 0 {
				firstLabelLen = len(tsBody) - len(ts)
			}

			for len(label) > 0 {
				switch label[0] {
				case 0x0a: // string name  = 1;
					if mb.labels[labelIndex].name, label, err = pb.Bytes(label[1:]); err != nil {
						return nil, 0, err
					}
				case 0x12: // string value = 2;
					if mb.labels[labelIndex].value, label, err = pb.Bytes(label[1:]); err != nil {
						return nil, 0, err
					}
				default:
					if label, err = pb.Skip(label); err != nil {
						return nil, 0, err
					}
				}
			}
			if mb.labels[labelIndex].name != nil && mb.labels[labelIndex].value != nil {
				labelIndex++
			}

			continue
		case 0x12: // repeated Sample samples = 2;
			if samplesOffset == 0 {
				samplesOffset = len(tsBody) - len(ts)
			}
		default: // unknown field
		}

		if ts, err = pb.Skip(ts); err != nil {
			return nil, 0, err
		}
	}

	var nameFound bool
	for i := 0; i < labelIndex; i++ {
		if bytes.Equal(mb.labels[i].name, []byte("__name__")) {
			if i != 0 {
				mb.labels[0], mb.labels[i] = mb.labels[i], mb.labels[0]
			}
			nameFound = true
			break
		}
	}

	if !nameFound {
		return nil, 0, errors.New("__name__ not found")
	}

	mb.metric[0] = unsafeString(mb.labels[0].value)

	mb.metricUsed = 1

	// sort.Slice(labels, func(i, j int) bool { return bytes.Compare(labels[i].name, labels[j].name) < 0 })
	// alloc free sort
	// check for sort

	sortRequired := false
	for i := 1; i < labelIndex-1; i++ {
		if bytes.Compare(mb.labels[i].name, mb.labels[i+1].name) > 0 {
			sortRequired = true
			break
		}
	}

	if sortRequired {
		for i := 1; i < labelIndex-1; i++ {
			for j := i + 1; j < labelIndex; j++ {
				if bytes.Compare(mb.labels[i].name, mb.labels[j].name) > 0 {
					mb.labels[i], mb.labels[j] = mb.labels[j], mb.labels[i]
				}
			}
		}
	}

	if labelIndex*2 > len(mb.metric) {
		for i := len(mb.metric); i < labelIndex*2; i++ {
			mb.metric = append(mb.metric, "")
		}
	}

	for i := 1; i < labelIndex; i++ {
		mb.metric[mb.metricUsed] = mb.urlQueryEscape(mb.labels[i].name)
		mb.metricUsed++
		mb.metric[mb.metricUsed] = mb.urlQueryEscape(mb.labels[i].value)
		mb.metricUsed++
	}

	return mb.metric[:mb.metricUsed], samplesOffset, nil
}
