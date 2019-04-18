package tags

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
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

func Graphite(config TagConfig, s string) (string, error) {
	if strings.IndexByte(s, ';') < 0 && config.Enabled {
		tagged, err := config.toGraphiteTagged(s)
		if err != nil {
			return "", err
		}
		s = tagged
	}

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
	newF := begin + strings.Replace(strings.Replace(filter, ".", "\\.", -1), "*", "[^\\.]*", -1) + end
	return regexp.MustCompile(newF)
}

func (cfg *TagConfig) Configure() error {
	cfg.TagMap = make(map[string]string)
	makeTagMap(cfg.TagMap, cfg.Tags)

	for _, s := range cfg.Templates {
		descs := strings.Split(s, " ")
		if len(descs) > 3 {
			return fmt.Errorf("wrong template format")
		}
		var filter string
		var template string
		var tags string
		if len(descs) == 2 {
			if strings.Contains(descs[1], "=") {
				tags = descs[1]
				template = descs[0]
			} else {
				template = descs[1]
				filter = descs[0]
			}
		} else if len(descs) == 3 {
			filter = descs[0]
			template = descs[1]
			tags = descs[2]
		} else {
			template = descs[0]
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
				tagMap[desc.Template[i]] = name
			}
		}

		if bytes.HasSuffix([]byte(measurement), []byte("_")) {
			measurement = measurement[:len(measurement)-1]
		}

		for k, v := range tagMap {
			tags += ";" + k + "=" + v
		}

		return measurement + tags, nil
	}
	return "", nil
}
