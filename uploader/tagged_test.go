package uploader

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary/reader"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/lomik/carbon-clickhouse/helper/escape"
	"github.com/lomik/carbon-clickhouse/helper/tests"
	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUrlParse(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization?ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe+1") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd&a") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node&a b")

	assert.Equal("instance:cpu_utilization%3Fratio_avg?dc=qwe%2B1&fqdn=asd%26a&instance=10.33.10.10%3A9100&job=node%26a+b", metric)

	// original url.Parse
	mu, err := url.Parse(metric)
	require.NoError(err)
	require.NotNil(mu)
	assert.Equal("", mu.Path)

	// from tagged uploader
	m, err := urlParse(metric)
	require.NoError(err)
	require.NotNil(m)
	assert.Equal("instance:cpu_utilization?ratio_avg", m.Path)
	assert.Equal(mu.Query(), m.Query())
	assert.Equal(url.Values{
		"dc":       []string{"qwe+1"},
		"fqdn":     []string{"asd&a"},
		"instance": []string{"10.33.10.10:9100"},
		"job":      []string{"node&a b"},
	}, m.Query())
}

func TestTagsParse(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization?ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe+1") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd&a") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node&a b")

	assert.Equal("instance:cpu_utilization%3Fratio_avg?dc=qwe%2B1&fqdn=asd%26a&instance=10.33.10.10%3A9100&job=node%26a+b", metric)

	m, err := urlParse(metric)
	require.NoError(err)
	require.NotNil(m)
	assert.Equal("instance:cpu_utilization?ratio_avg", m.Path)

	mapTags := m.Query()
	mTags := make(map[string]string)
	for k, v := range mapTags {
		mTags[k] = k + "=" + v[0]
	}

	name, tags, err := tagsParse(metric)
	if err != nil {
		t.Errorf("tagParse: %s", err.Error())
	}
	assert.Equal(m.Path, name)
	assert.Equal(mTags, tags)
}

func TestTagsParseToSlice(t *testing.T) {
	tests := []struct {
		metric      string
		wantName    string
		wantNameTag string
		wantTags    []string
	}{
		{
			metric: escape.Path("instance:cpu_utilization?ratio_avg") +
				"?" + escape.Query("dc") + "=" + escape.Query("qwe+1") +
				"&" + escape.Query("fqdn") + "=" + escape.Query("asd&a") +
				"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
				"&" + escape.Query("job") + "=" + escape.Query("node&a b"),
			wantName:    "instance:cpu_utilization?ratio_avg",
			wantNameTag: "__name__=instance:cpu_utilization?ratio_avg",
			wantTags:    []string{"dc=qwe+1", "fqdn=asd&a", "instance=10.33.10.10:9100", "job=node&a b"},
		},
		{
			metric:      "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node",
			wantName:    "instance:cpu_utilization:ratio_avg",
			wantNameTag: "__name__=instance:cpu_utilization:ratio_avg",
			wantTags:    []string{"dc=qwe", "fqdn=asd", "instance=10.33.10.10_9100", "job=node"},
		},
	}

	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)
	for i, tt := range tests {
		t.Run(tt.metric+" ["+strconv.Itoa(i)+"]", func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			m, err := urlParse(tt.metric)
			require.NoError(err)
			require.NotNil(m)
			assert.Equal(tt.wantName, m.Path)

			mapTags := m.Query()
			mTags := make([]string, 0, len(mapTags))
			for k, v := range mapTags {
				mTags = append(mTags, k+"="+v[0])
			}
			sort.Strings(mTags)

			tagsBuf.nameBuf.Reset()
			name, nameTag, tags, err := tagsParseToSlice(tt.metric, tagsBuf)
			if err != nil {
				t.Errorf("tagParseToSlice: %s", err.Error())
			}
			assert.Equal(tt.wantName, name)
			assert.Equal(mTags, tags)
			assert.Equal(tt.wantNameTag, nameTag)
			assert.Equal(tt.wantTags, tags)
		})
	}
}

func BenchmarkUrlParse(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		u, _ := urlParse(metric)
		u.Path = "__name__=" + u.Path
		_ = u.Query()
	}
}

func BenchmarkTagParse(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10_9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = tagsParse(metric)
	}
}

func BenchmarkTagParseToSlice(b *testing.B) {
	// make metric name as receiver
	metric := escape.Path("instance:cpu_utilization:ratio_avg") +
		"?" + escape.Query("dc") + "=" + escape.Query("qwe") +
		"&" + escape.Query("fqdn") + "=" + escape.Query("asd") +
		"&" + escape.Query("instance") + "=" + escape.Query("10.33.10.10:9100") +
		"&" + escape.Query("job") + "=" + escape.Query("node b")

	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tagsBuf.nameBuf.Reset()
		_, _, _, _ = tagsParseToSlice(metric, tagsBuf)
	}
}

func BenchmarkKeySprintf(b *testing.B) {
	path := "test.path"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d:%s", 1285, path)
	}
}

func BenchmarkKeyConcat(b *testing.B) {
	path := "test.path"
	var unum uint16 = 1245

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = strconv.Itoa(int(unum)) + ":" + path
	}
}

func BenchmarkTaggedParseNameShort(b *testing.B) {
	logger := zapwriter.Logger("upload")

	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	u := NewTagged(base)

	name := "instance:cpu_utilization:ratio_avg?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node"
	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version := uint32(time.Now().Unix())
		if err := u.parseName(name, uint16(i), version, tagsBuf); err != nil {
			b.Fatalf("Tagged.parseName() error = %v", err)
		}
	}
}

func BenchmarkTaggedParseNameLong(b *testing.B) {
	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	u := NewTagged(base)

	name := "k8s.production-cl1.nginx_ingress_controller_response_size_bucket?app_kubernetes_io_component=controller&app_kubernetes_io_instance=ingress-nginx&app_kubernetes_io_managed_by=Helm&app_kubernetes_io_name=ingress-nginx&app_kubernetes_io_version=0_32_0&controller_class=nginx&controller_namespace=ingress-nginx&controller_pod=ingress-nginx-controller-d2ppr&helm_sh_chart=ingress-nginx-2_3_0&host=vm1_test_int&ingress=web-ingress&instance=192_168_0.10&job=kubernetes-service-endpoints&kubernetes_name=ingress-nginx-controller-metrics&kubernetes_namespace=ingress-nginx&kubernetes_node=k8s-n03&le=10&method=GET&namespace=web-app&path=_&service=web-app&status=500"
	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version := uint32(time.Now().Unix())
		if err := u.parseName(name, uint16(i), version, tagsBuf); err != nil {
			b.Fatalf("Tagged.parseName() error = %v", err)
		}
	}
}

type taggedRecord struct {
	days    uint16
	tag1    string
	path    string
	tags    []string
	version uint32
}

func (r *taggedRecord) Read(rdr *reader.Reader) error {
	var err error

	r.days, err = rdr.ReadUint16()
	if err != nil {
		return err
	}
	r.tag1, err = rdr.ReadString()
	if err != nil {
		return reader.CheckError(err)
	}
	r.path, err = rdr.ReadString()
	if err != nil {
		return reader.CheckError(err)
	}
	r.tags, err = rdr.ReadStringList()
	if err != nil {
		return reader.CheckError(err)
	}
	r.version, err = rdr.ReadUint32()

	return reader.CheckError(err)
}

func TestTaggedParseFileDedup(t *testing.T) {
	points := []point{
		{
			path:    "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			value:   1.0,
			time:    1559465760,
			date:    18049,
			version: 1559465760,
		},
		{
			// duplicate for tagged
			path:    "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			value:   2.0,
			time:    1559465800,
			date:    18049,
			version: 1559465800,
		},
		{
			// next day
			path:    "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			value:   1.0,
			time:    1662098177,
			date:    19237,
			version: 1662098177,
		},
		{
			path:    "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			value:   1.0,
			time:    1559465760,
			date:    18049,
			version: 1559465760,
		},
		{
			// plain metric
			path:    "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			value:   1.0,
			time:    1559465760,
			date:    18049,
			version: 1559465760,
		},
	}

	wantTaggedRecords := []taggedRecord{
		// "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer"
		{
			days: 18049,
			tag1: "__name__=writtenBytes",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 18049,
			tag1: "app=carbon-clickhouse",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 18049,
			tag1: "project=carbon",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 18049,
			tag1: "subsystem=writer",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		// next day
		{
			days: 19237,
			tag1: "__name__=writtenBytes",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 19237,
			tag1: "app=carbon-clickhouse",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 19237,
			tag1: "project=carbon",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		{
			days: 19237,
			tag1: "subsystem=writer",
			path: "writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer",
			tags: []string{"__name__=writtenBytes", "app=carbon-clickhouse", "project=carbon", "subsystem=writer"},
		},
		// "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp"
		{
			days: 18049,
			tag1: "__name__=errors",
			path: "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			tags: []string{"__name__=errors", "app=carbon-clickhouse", "project=carbon", "subsystem=receiver", "type=tcp"},
		},
		{
			days: 18049,
			tag1: "app=carbon-clickhouse",
			path: "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			tags: []string{"__name__=errors", "app=carbon-clickhouse", "project=carbon", "subsystem=receiver", "type=tcp"},
		},
		{
			days: 18049,
			tag1: "project=carbon",
			path: "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			tags: []string{"__name__=errors", "app=carbon-clickhouse", "project=carbon", "subsystem=receiver", "type=tcp"},
		},
		{
			days: 18049,
			tag1: "subsystem=receiver",
			path: "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			tags: []string{"__name__=errors", "app=carbon-clickhouse", "project=carbon", "subsystem=receiver", "type=tcp"},
		},
		{
			days: 18049,
			tag1: "type=tcp",
			path: "errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp",
			tags: []string{"__name__=errors", "app=carbon-clickhouse", "project=carbon", "subsystem=receiver", "type=tcp"},
		},
	}
	cacheMap := map[string]bool{
		"18049:writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer":      true,
		"19237:writtenBytes?app=carbon-clickhouse&project=carbon&subsystem=writer":      true,
		"18049:errors?app=carbon-clickhouse&project=carbon&subsystem=receiver&type=tcp": true,
	}

	filename, err := writeFile(points, config.CompAlgoNone, 0)
	if err != nil {
		t.Fatalf("writeFile() got error: %v", err)
	}
	defer os.Remove(filename)

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test", DisableDailyIndex: true},
	}
	u := NewTagged(base)

	for i := 0; i < 3; i++ {
		t.Run("#"+strconv.Itoa(i), func(t *testing.T) {
			var out bytes.Buffer
			start := uint32(time.Now().Unix())
			n, m, err := u.parseFile(filename, &out)
			end := uint32(time.Now().Unix())
			if err != nil {
				t.Fatalf("Tagged.parseFile() got error: %v", err)
			}
			if n != 3 {
				t.Errorf("Tagged.parseFile() got %d, want %d", n, 3)
			}

			records := make([]taggedRecord, 0, len(wantTaggedRecords))
			var rec taggedRecord
			br := reader.NewReader(&out)
			for err = rec.Read(br); err == nil; err = rec.Read(br) {
				records = append(records, rec)
			}
			if err != io.EOF {
				t.Fatalf("taggedRecord.Read() got error: %v", err)
			}
			maxLen := tests.Max(len(wantTaggedRecords), len(records))
			for i := 0; i < maxLen; i++ {
				if i >= len(records) {
					t.Errorf("[%d]\n- %+v", i, wantTaggedRecords[i])
				} else if i >= len(wantTaggedRecords) {
					t.Errorf("[%d]\n+ %+v", i, records[i])
				} else if wantTaggedRecords[i].path != records[i].path || wantTaggedRecords[i].days != records[i].days {
					t.Errorf("[%d]\n- %+v\n+ %+v", i, wantTaggedRecords[i], records[i])
				} else {
					if wantTaggedRecords[i].tag1 != records[i].tag1 {
						t.Errorf("[%d].tag1 want '%s', got '%+v'", i, wantTaggedRecords[i].tag1, records[i])
					}
					if !reflect.DeepEqual(wantTaggedRecords[i].tags, records[i].tags) {
						t.Errorf("[%d].tags want %+v, got %+v", i, wantTaggedRecords[i].tags, records[i])
					}
					if records[i].version < start || records[i].version > end {
						t.Errorf("[%d].verion want beetween %d and %d, got '%+v'", i, start, end, records[i])
					}
				}
			}

			for v := range cacheMap {
				if _, exist := m[v]; !exist {
					t.Errorf("Tagged.parseFile() got\n- map[%s]", v)
				}
			}
			for v := range m {
				if _, exist := cacheMap[v]; !exist {
					t.Errorf("Tagged.parseFile() got\n+ map[%s]", v)
				}
			}
		})
	}
}

func verifyTaggedUploaded(t *testing.T, b io.Reader, points []point, start, end uint32) {
	var (
		rec taggedRecord
		err error
	)

	br := reader.NewReader(b)
	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)
	for i, point := range points {
		if strings.IndexByte(point.path, '?') == -1 {
			continue
		}
		tagsBuf.nameBuf.Reset()
		name, nameTag, tags, err := tagsParseToSlice(point.path, tagsBuf)
		if err != nil {
			t.Fatalf("urlParse [%d]: %v,\nwant\n%+v", i, err, point)
		}
		wantNameTag := "__name__=" + name
		if wantNameTag != nameTag {
			t.Fatalf("nameTag [%d]: %q, want %q", i, nameTag, wantNameTag)
		}
		tags = append([]string{nameTag}, tags...)
		for i := 0; i < len(tags); i++ {
			if err = rec.Read(br); err != nil {
				t.Fatalf("read [%d]: %v,\nwant\n%+v", i, err, point)
			}
			if point.path != rec.path || point.date != rec.days {
				t.Errorf("[%d]\n- %+v\n+ %+v", i, point, rec)
			}

			want := taggedRecord{
				days: point.date,
				tag1: tags[i],
				path: point.path,
				tags: tags,
			}

			if tags[i] != want.tag1 {
				t.Errorf("[%d].tag1 want '%s', got %+v", i, want.tag1, rec)
			}
			if !reflect.DeepEqual(want.tags, rec.tags) {
				t.Errorf("[%d].tags want %+v, got %+v", i, want.tags, rec)
			}
			if rec.version < start || rec.version > end {
				t.Errorf("[%d].verion want beetween %d and %d, got '%+v'", i, start, end, rec)
			}
		}
	}

	if err = rec.Read(br); err == nil {
		t.Fatalf("read at end: got '%+v', want '%v'", rec, io.EOF)
	} else if err != io.EOF {
		t.Fatalf("read at end: got '%v', want '%v'", err, io.EOF)
	}
}

func TestTaggedParseFile(t *testing.T) {
	points := generateMetrics()
	wantPoints := uint64(len(points) / 2)
	cacheMap := buildCacheMap(points, true)

	tests := []struct {
		name          string
		compress      config.CompAlgo
		compressLevel int
	}{
		{
			name:          "Uncompressed",
			compress:      config.CompAlgoNone,
			compressLevel: 0,
		},
		{
			name:          "Compressed",
			compress:      config.CompAlgoNone,
			compressLevel: 3,
		},
	}
	logger := zapwriter.Logger("upload")
	for _, tt := range tests {
		filename, err := writeFile(points, tt.compress, tt.compressLevel)
		if err != nil {
			t.Fatalf("writeFile() got error: %v", err)
		}
		defer os.Remove(filename)

		base := &Base{
			queue:   make(chan string, 1024),
			inQueue: make(map[string]bool),
			logger:  logger,
			config:  &Config{TableName: "test"},
		}
		u := NewTagged(base)

		for i := 0; i < 3; i++ {
			t.Run(tt.name+"#"+strconv.Itoa(i), func(t *testing.T) {
				var out bytes.Buffer
				start := uint32(time.Now().Unix())
				n, m, err := u.parseFile(filename, &out)
				end := uint32(time.Now().Unix())
				if err != nil {
					t.Fatalf("Tagged.parseFile() got error: %v", err)
				}
				if n != wantPoints {
					t.Errorf("Tagged.parseFile() got %d, want %d", n, wantPoints)
				}
				for v := range cacheMap {
					if _, exist := m[v]; !exist {
						t.Errorf("Index.parseFile() got\n- map[%s]", v)
					}
				}
				for v := range m {
					if _, exist := cacheMap[v]; !exist {
						t.Errorf("Index.parseFile() got\n+ map[%s]", v)
					}
				}

				verifyTaggedUploaded(t, &out, points, start, end)
			})
		}
	}
}

type taggedBench struct {
	name          string
	compress      config.CompAlgo
	compressLevel int
}

func benchmarkTaggedParseFile(b *testing.B, bm *taggedBench, points []point, wantPoints uint64) {
	logger := zapwriter.Logger("upload")
	var out bytes.Buffer
	out.Grow(524288)

	filename, err := writeFile(points, bm.compress, bm.compressLevel)
	if err != nil {
		b.Fatalf("writeFile() got error: %v", err)
	}
	defer os.Remove(filename)

	b.Run(bm.name, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			base := &Base{
				queue:   make(chan string, 1024),
				inQueue: make(map[string]bool),
				logger:  logger,
				config:  &Config{TableName: "test"},
			}
			u := NewTagged(base)

			out.Reset()

			n, _, err := u.parseFile(filename, &out)
			if err != nil {
				b.Fatalf("Tagged.parseFile() got error: %v", err)
			}
			if n != wantPoints {
				b.Fatalf("Tagged.parseFile() got %d, want %d", n, wantPoints)
			}
		}
	})
}

func BenchmarkTaggedParseFileUncompressed(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := taggedBench{
		name:          fmt.Sprintf("%40s", "Uncompressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 0,
	}

	benchmarkTaggedParseFile(b, &bm, points, wantPoints)
}

func BenchmarkTaggedParseFileCompressed(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := taggedBench{
		name:          fmt.Sprintf("%40s", "Compressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 1,
	}

	benchmarkTaggedParseFile(b, &bm, points, wantPoints)
}

func TestTagged_parseName_Overflow(t *testing.T) {
	tagsBuf := getTagsBuffer()
	defer releaseTagsBuffer(tagsBuf)

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	var (
		sb      strings.Builder
		nameBuf strings.Builder
	)

	sb.WriteString("very_long_name_field1.very_long_name_field2.very_long_name_field3.very_long_name_field4?")
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteString("&")
		}
		sb.WriteString(fmt.Sprintf("very_long_tag%d=very_long_value%d", i, i))
	}
	u := NewTagged(base)
	nameBuf.Reset()
	err := u.parseName(sb.String(), 10, 1, tagsBuf)
	assert.Equal(t, errBufOverflow, err)
}

func benchmarkTaggedParseFileParallel(b *testing.B, bm *taggedBench, points []point, wantPoints uint64) {
	b.Run(bm.name, func(b *testing.B) {
		logger := zapwriter.Logger("upload")
		var out bytes.Buffer
		out.Grow(524288)

		filename, err := writeFile(points, bm.compress, bm.compressLevel)
		if err != nil {
			b.Fatalf("writeFile() got error: %v", err)
		}
		defer os.Remove(filename)

		base := &Base{
			queue:   make(chan string, 1024),
			inQueue: make(map[string]bool),
			logger:  logger,
			config:  &Config{TableName: "test"},
		}
		u := NewTagged(base)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				out.Reset()

				n, _, err := u.parseFile(filename, &out)
				if err != nil {
					b.Fatalf("Tagged.parseFile() got error: %v", err)
				}
				if n != wantPoints {
					b.Fatalf("Tagged.parseFile() got %d, want %d", n, wantPoints)
				}
			}
		})
	})
}

func BenchmarkTaggedParseFileUncompressedParallel(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := taggedBench{
		name:          fmt.Sprintf("%40s", "Uncompressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 0,
	}

	benchmarkTaggedParseFileParallel(b, &bm, points, wantPoints)
}
