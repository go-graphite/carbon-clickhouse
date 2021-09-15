package tags

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/lomik/carbon-clickhouse/helper/escape"
	"github.com/msaf1980/go-stringutils"
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

func Graphite(config TagConfig, s string, splitBuf []string) (string, error) {
	hasSemicolon := strings.IndexByte(s, ';')
	if hasSemicolon < 0 && config.Enabled {
		tagged, err := config.toGraphiteTagged(s)
		if err != nil {
			return "", err
		}
		s = tagged
		hasSemicolon = strings.IndexByte(s, ';')
	}

	if hasSemicolon < 0 {
		return s, nil
	}

	//arr := strings.Split(s, ";")
	arr := stringutils.SplitN(s, ";", splitBuf)
	if len(arr) >= len(splitBuf) {
		return "", fmt.Errorf("cannot parse path %#v, tags overflow", s)
	}
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

	var res stringutils.Builder
	res.Grow(len(s) + 10)

	arr = arr[:len(arr)-toDel]

	if len(arr) > 0 {
		escape.PathTo(arr[0], &res)
		res.WriteByte('?')
	}

	for i := 1; i < len(arr); i++ {
		if i > 1 {
			res.WriteByte('&')
		}
		p := strings.Index(arr[i], "=")
		escape.QueryTo(arr[i][:p], &res)
		res.WriteByte('=')
		escape.QueryTo(arr[i][p+1:], &res)
	}

	return res.String(), nil
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
