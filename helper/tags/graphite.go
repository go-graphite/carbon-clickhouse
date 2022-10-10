package tags

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/escape"
	"github.com/msaf1980/go-stringutils"
)

type GraphiteBuf struct {
	KV KVSlice
	B  stringutils.Builder
}

func (b *GraphiteBuf) Resize(kvLen, builderLen int) {
	b.KV = make(KVSlice, 0, kvLen)
	b.B.Grow(builderLen)
}

func (b *GraphiteBuf) ResizeKV(kvLen int) {
	b.KV = make(KVSlice, 0, kvLen)
}

func (b *GraphiteBuf) ResizeBuilder(builderLen int) {
	b.B.Grow(builderLen)
}

type kv struct {
	key   string
	value string
}

type KVSlice []kv

func (a KVSlice) Len() int      { return len(a) }
func (a KVSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a KVSlice) Less(i, j int) bool {
	return a[i].key < a[j].key
}

func kvParse(s string, buf *GraphiteBuf) (KVSlice, error) {
	buf.KV = buf.KV[:0]
	path := s
	for {
		seg := s
		pos := strings.IndexByte(s, ';')
		if pos == 0 {
			return nil, errors.New("cannot parse path '" + path + "', empty segment")
		} else if pos > 0 {
			seg = s[:pos]
		}

		kpos := strings.IndexByte(seg, '=')
		if kpos < 1 {
			return nil, errors.New("cannot parse path '" + path + "', invalid segment '" + seg + "', no '='")
		}

		buf.KV = append(buf.KV, kv{key: seg[:kpos], value: seg[kpos+1:]})
		if pos < 0 {
			break
		}
		s = s[pos+1:]
	}
	return buf.KV, nil
}

func Graphite(config TagConfig, s string) (string, error) {
	if config.Enabled && strings.IndexByte(s, ';') < 0 {
		tagged, err := config.toGraphiteTagged(s)
		if err != nil {
			return "", err
		}
		s = tagged
	}

	pos := strings.IndexByte(s, ';')
	if pos < 0 {
		return s, nil
	} else if pos == 0 {
		return "", errors.New("cannot parse path '" + s + "', no metric found")
	}
	name := s[:pos]

	var tagBuf GraphiteBuf
	tagBuf.ResizeKV(strings.Count(s, ";") + 1)

	arr, err := kvParse(s[pos+1:], &tagBuf)
	if err != nil {
		return "", err
	}

	tagBuf.ResizeBuilder(len(s) + 10)

	sort.Stable(arr)

	// uniq
	toDel := 0
	prevKey := ""
	for i := 0; i < len(arr); i++ {
		if arr[i].key == prevKey {
			toDel++
		} else {
			prevKey = arr[i].key
		}
		if toDel > 0 {
			arr[i-toDel] = arr[i]
		}
	}

	arr = arr[:len(arr)-toDel]

	escape.PathTo(name, &tagBuf.B)
	tagBuf.B.WriteByte('?')
	for i := 0; i < len(arr); i++ {
		if i > 0 {
			tagBuf.B.WriteByte('&')
		}
		escape.QueryTo(arr[i].key, &tagBuf.B)
		tagBuf.B.WriteByte('=')
		escape.QueryTo(arr[i].value, &tagBuf.B)
	}

	return tagBuf.B.String(), nil
}

func GraphiteBuffered(config TagConfig, s string, tagBuf *GraphiteBuf) (string, error) {
	if config.Enabled && strings.IndexByte(s, ';') < 0 {
		tagged, err := config.toGraphiteTagged(s)
		if err != nil {
			return "", err
		}
		s = tagged
	}

	pos := strings.IndexByte(s, ';')
	if pos < 0 {
		return s, nil
	} else if pos == 0 {
		return "", errors.New("cannot parse path '" + s + "', no metric found")
	}
	name := s[:pos]

	arr, err := kvParse(s[pos+1:], tagBuf)
	if err != nil {
		return "", err
	}

	tagBuf.B.Reset()

	sort.Stable(arr)

	// uniq
	toDel := 0
	prevKey := ""
	for i := 0; i < len(arr); i++ {
		if arr[i].key == prevKey {
			toDel++
		} else {
			prevKey = arr[i].key
		}
		if toDel > 0 {
			arr[i-toDel] = arr[i]
		}
	}

	arr = arr[:len(arr)-toDel]

	escape.PathTo(name, &tagBuf.B)
	tagBuf.B.WriteByte('?')
	for i := 0; i < len(arr); i++ {
		if i > 0 {
			tagBuf.B.WriteByte('&')
		}
		escape.QueryTo(arr[i].key, &tagBuf.B)
		tagBuf.B.WriteByte('=')
		escape.QueryTo(arr[i].value, &tagBuf.B)
	}

	return tagBuf.B.String(), nil
}

type TemplateDesc struct {
	Filter    *regexp.Regexp
	Template  []string
	ExtraTags map[string]string
}

type TagConfig struct {
	Enabled       bool              `toml:"enabled"`
	Separator     string            `toml:"separator"`
	Tags          []string          `toml:"tags"`
	TagMap        map[string]string `toml:"-"`
	Templates     []string          `toml:"templates"`
	TemplateDescs []TemplateDesc    `toml:"-"`
}

func DisabledTagConfig() TagConfig {
	return TagConfig{Enabled: false}
}

func makeRegexp(filter string) *regexp.Regexp {
	if filter == "" {
		return regexp.MustCompile(`[.]^*`)
	}
	begin := "^"
	end := "$"
	if strings.HasPrefix(filter, "*") {
		begin = ""
		filter = filter[1:]
	}
	if strings.HasSuffix(filter, "*") {
		end = ""
		filter = filter[:len(filter)-1]
	}
	pattern := begin + strings.Replace(strings.Replace(filter, ".", "\\.", -1), "*", "[^\\.]*", -1) + end
	return regexp.MustCompile(pattern)
}

func (cfg *TagConfig) Configure() error {
	cfg.TagMap = make(map[string]string)
	makeTagMap(cfg.TagMap, cfg.Tags)

	for _, s := range cfg.Templates {
		dirtyTokens := strings.Split(s, " ")
		tokens := dirtyTokens[:0]
		for _, token := range dirtyTokens {
			trimmed := strings.TrimSpace(token)
			if trimmed != "" {
				tokens = append(tokens, trimmed)
			}
		}
		if len(tokens) > 3 {
			return fmt.Errorf("wrong template format")
		}
		var filter string
		var template string
		var tags string
		if len(tokens) == 2 {
			if strings.Contains(tokens[1], "=") {
				tags = tokens[1]
				template = tokens[0]
			} else {
				template = tokens[1]
				filter = tokens[0]
			}
		} else if len(tokens) == 3 {
			filter = tokens[0]
			template = tokens[1]
			tags = tokens[2]
		} else {
			template = tokens[0]
		}

		newDesc := TemplateDesc{}
		newDesc.Filter = makeRegexp(filter)
		newDesc.Template = strings.Split(template, ".")
		tagPairs := strings.Split(tags, ",")
		newDesc.ExtraTags = make(map[string]string)
		makeTagMap(newDesc.ExtraTags, tagPairs)

		cfg.TemplateDescs = append(cfg.TemplateDescs, newDesc)
	}
	return nil
}

func makeTagMap(tagMap map[string]string, tags []string) {
	if len(tags) == 0 || tags[0] == "" {
		return
	}
	for _, tag := range tags {
		keyValue := strings.Split(tag, "=")
		tagMap[keyValue[0]] = keyValue[1]
	}
}

func (cfg *TagConfig) toGraphiteTagged(s string) (string, error) {
	for _, desc := range cfg.TemplateDescs {
		if !desc.Filter.Match([]byte(s)) {
			continue
		}

		tagMap := make(map[string]string)
		for k, v := range cfg.TagMap {
			tagMap[k] = v
		}
		for k, v := range desc.ExtraTags {
			tagMap[k] = v
		}

		names := strings.Split(s, ".")
		measurement := ""
		tags := ""

		if len(names) != len(desc.Template) && !strings.HasSuffix(desc.Template[len(desc.Template)-1], "*") ||
			len(names) < len(desc.Template) {
			continue
		}

	Metric:
		for i, name := range names {
			switch desc.Template[i] {
			case "":
				continue
			case "measurement":
				measurement += name + cfg.Separator
			case "measurement*":
				measurement += strings.Join(names[i:], cfg.Separator)
				break Metric
			default:
				if prefix, ok := tagMap[desc.Template[i]]; ok {
					tagMap[desc.Template[i]] = prefix + cfg.Separator + name
				} else {
					tagMap[desc.Template[i]] = name
				}
			}
		}

		if strings.HasSuffix(measurement, "_") {
			measurement = measurement[:len(measurement)-1]
		}

		for k, v := range tagMap {
			tags += ";" + k + "=" + v
		}

		return measurement + tags, nil
	}
	return "", nil
}
