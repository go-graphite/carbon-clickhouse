package uploader

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lomik/carbon-clickhouse/helper/RowBinary"
	"github.com/lomik/carbon-clickhouse/helper/RowBinary/reader"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/lomik/carbon-clickhouse/helper/tests"
	"github.com/lomik/zapwriter"
)

type indexRecord struct {
	date    uint16
	level   uint32
	name    string
	version uint32
}

func (r *indexRecord) Read(rdr *reader.Reader) error {
	var err error
	r.date, err = rdr.ReadUint16()
	if err != nil {
		return err
	}
	r.level, err = rdr.ReadUint32()
	if err != nil {
		return err
	}
	r.name, err = rdr.ReadString()
	if err != nil {
		return err
	}
	r.version, err = rdr.ReadUint32()

	return err
}

func TestIndexParseFileDedup(t *testing.T) {
	now32 := uint32(time.Now().Unix())
	points := []point{
		{
			path:    "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			value:   1.0,
			time:    1559465760,
			date:    18049,
			version: 1559465760,
		},
		{
			// duplicate for index
			path:    "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			value:   2.0,
			time:    1559465800,
			date:    18049,
			version: 1559465800,
		},
		{
			// duplicate parts for index
			path:    "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			value:   2.0,
			time:    1559465800,
			date:    18049,
			version: 1559465800,
		},
		{
			// next day
			path:    "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			value:   1.0,
			time:    1662098177,
			date:    19237,
			version: 1662098177,
		},
		{
			// tagged metric
			path:    "errors?app=carbon-clickhouse&scope=tcp",
			value:   1.0,
			time:    1662098177,
			date:    19237,
			version: 1662098177,
		},
	}

	wantIndexRecords := []indexRecord{
		// carbon.agents.carbon-clickhouse.writer.writtenBytes
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes", version: now32},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.writer.", version: now32},
		{date: 42, level: TreeLevelOffset + 3, name: "carbon.agents.carbon-clickhouse.", version: now32},
		{date: 42, level: TreeLevelOffset + 2, name: "carbon.agents.", version: now32},
		{date: 42, level: TreeLevelOffset + 1, name: "carbon.", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon", version: now32},
		{date: 18049, level: 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes", version: now32},
		{date: 18049, level: ReverseLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon", version: now32},
		// carbon.agents.carbon-clickhouse.tcp.errors
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.tcp.receiver.", version: now32},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.tcp.", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
		{date: 18049, level: 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 18049, level: ReverseLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
		// next day
		// TODO (msaf1980): maybe can deduplicated, but not at now
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
		// next day - continue
		{date: 19237, level: 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 19237, level: ReverseLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
	}
	cacheMap := map[string]bool{
		"18049:carbon.agents.carbon-clickhouse.tcp.receiver.errors": true,
		"18049:carbon.agents.carbon-clickhouse.writer.writtenBytes": true,
		"19237:carbon.agents.carbon-clickhouse.tcp.receiver.errors": true,
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
		config:  &Config{TableName: "test", DisableDailyIndex: false},
	}
	u := NewIndex(base)

	var out bytes.Buffer
	n, m, err := u.parseFile(filename, &out)
	if err != nil {
		t.Fatalf("Index.parseFile() got error: %v", err)
	}
	if n != 3 {
		t.Errorf("Index.parseFile() got %d, want %d", n, 3)
	}

	records := make([]indexRecord, 0, len(wantIndexRecords))
	var rec indexRecord
	br := reader.NewReader(&out)
	for err = rec.Read(br); err != io.EOF; err = rec.Read(br) {
		records = append(records, rec)
	}
	maxLen := tests.Max(len(wantIndexRecords), len(records))
	for i := 0; i < maxLen; i++ {
		if i >= len(records) {
			t.Errorf("[%d]\n- %+v", i, wantIndexRecords[i])
		} else if i >= len(wantIndexRecords) {
			t.Errorf("[%d]\n+ %+v", i, records[i])
		} else if wantIndexRecords[i] != records[i] {
			t.Errorf("[%d]\n- %+v\n+ %+v", i, wantIndexRecords[i], records[i])
		}
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
}

func TestIndexParseFileDedupNoDaily(t *testing.T) {
	now32 := uint32(time.Now().Unix())
	points := []point{
		{
			path:    "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			value:   1.0,
			time:    1559465760,
			date:    18049,
			version: 1559465760,
		},
		{
			// duplicate for index
			path:    "carbon.agents.carbon-clickhouse.writer.writtenBytes",
			value:   2.0,
			time:    1559465800,
			date:    18049,
			version: 1559465800,
		},
		{
			// duplicate parts for index
			path:    "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			value:   2.0,
			time:    1559465800,
			date:    18049,
			version: 1559465800,
		},
		{
			// next day
			path:    "carbon.agents.carbon-clickhouse.tcp.receiver.errors",
			value:   1.0,
			time:    1662098177,
			date:    19237,
			version: 1662098177,
		},
		{
			// tagged metric
			path:    "errors?app=carbon-clickhouse&scope=tcp",
			value:   1.0,
			time:    1662098177,
			date:    19237,
			version: 1662098177,
		},
	}

	wantIndexRecords := []indexRecord{
		// carbon.agents.carbon-clickhouse.writer.writtenBytes
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes", version: now32},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.writer.", version: now32},
		{date: 42, level: TreeLevelOffset + 3, name: "carbon.agents.carbon-clickhouse.", version: now32},
		{date: 42, level: TreeLevelOffset + 2, name: "carbon.agents.", version: now32},
		{date: 42, level: TreeLevelOffset + 1, name: "carbon.", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon", version: now32},
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.tcp.receiver.", version: now32},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.tcp.", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
		// next day
		// TODO (msaf1980): maybe can deduplicated, but not at now
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors", version: now32},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon", version: now32},
	}
	// @TODO(msaf1980): may be 42 as date prefix ?
	cacheMap := map[string]bool{
		"18049:carbon.agents.carbon-clickhouse.tcp.receiver.errors": true,
		"18049:carbon.agents.carbon-clickhouse.writer.writtenBytes": true,
		"19237:carbon.agents.carbon-clickhouse.tcp.receiver.errors": true,
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
	u := NewIndex(base)

	var out bytes.Buffer
	n, m, err := u.parseFile(filename, &out)
	if err != nil {
		t.Fatalf("Index.parseFile() got error: %v", err)
	}
	if n != 3 {
		t.Errorf("Index.parseFile() got %d, want %d", n, 3)
	}

	records := make([]indexRecord, 0, len(wantIndexRecords))
	var rec indexRecord
	br := reader.NewReader(&out)
	for err = rec.Read(br); err != io.EOF; err = rec.Read(br) {
		records = append(records, rec)
	}
	maxLen := tests.Max(len(wantIndexRecords), len(records))
	for i := 0; i < maxLen; i++ {
		if i >= len(records) {
			t.Errorf("[%d]\n- %+v", i, wantIndexRecords[i])
		} else if i >= len(wantIndexRecords) {
			t.Errorf("[%d]\n+ %+v", i, records[i])
		} else if wantIndexRecords[i] != records[i] {
			t.Errorf("[%d]\n- %+v\n+ %+v", i, wantIndexRecords[i], records[i])
		}
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
}

func verifyIndexUploaded(t *testing.T, b io.Reader, points []point, version uint32, disableDailyIndex bool) {
	var (
		rec indexRecord
		err error
	)

	br := reader.NewReader(b)
	newUniq := make(map[string]bool)

	for i, point := range points {
		if strings.IndexByte(point.path, '?') >= 0 {
			continue
		}

		bpath := []byte(point.path)
		level := pathLevel(bpath)

		// Tree
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] tree[%d]: %v,\nwant\n%+v", i, level+TreeLevelOffset, err, point)
		}

		wantRecord := indexRecord{
			date:    DefaultTreeDate,
			level:   uint32(TreeLevelOffset + level),
			name:    point.path,
			version: version,
		}
		if wantRecord != rec {
			t.Fatalf("verify [%d] tree[%d]: got\n%+v\nwant\n%+v", i, level+TreeLevelOffset, rec, wantRecord)
		}
		p := bpath
		for l := level - 1; l > 0; l-- {
			end := bytes.LastIndexByte(p, '.')
			s := unsafeString(p[:end+1])
			if newUniq[s] {
				break
			}
			newUniq[string(s)] = true

			if err = rec.Read(br); err != nil {
				t.Fatalf("read [%d] tree[%d]: %v,\nwant\n%+v", i, l+TreeLevelOffset, err, point)
			}
			wantRecord := indexRecord{
				date:    DefaultTreeDate,
				level:   uint32(l + TreeLevelOffset),
				name:    s,
				version: version,
			}
			if wantRecord != rec {
				t.Fatalf("verify [%d] tree[%d]: got\n%+v\nwant\n%+v", i, l+TreeLevelOffset, rec, wantRecord)
			}
			p = p[:end]
		}

		// Reverse path without date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] tree_reverse[%d]: %v,\nwant\n%+v", i, level+ReverseTreeLevelOffset, err, point)
		}

		rpath := RowBinary.ReverseBytes(bpath)

		wantRecord = indexRecord{
			date:    DefaultTreeDate,
			level:   uint32(level + ReverseTreeLevelOffset),
			name:    string(rpath),
			version: version,
		}
		if wantRecord != rec {
			t.Fatalf("verify [%d] tree_reverse[%d]: got\n%+v\nwant\n%+v", i, level+ReverseTreeLevelOffset, rec, wantRecord)
		}

		if disableDailyIndex {
			continue
		}

		// Direct path with date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] daily[%d]: %v,\nwant\n%+v", i, level, err, point)
		}
		wantRecord = indexRecord{
			date:    point.date,
			level:   uint32(level),
			name:    point.path,
			version: version,
		}
		if wantRecord != rec {
			t.Fatalf("verify [%d] daily[%d]: got\n%+v\nwant\n%+v", i, level, rec, wantRecord)
		}

		// Reverse path with date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] daily_reverse[%d]: %v,\nwant\n%+v", i, level+ReverseLevelOffset, err, point)
		}
		wantRecord = indexRecord{
			date:    point.date,
			level:   uint32(level + ReverseLevelOffset),
			name:    unsafeString(rpath),
			version: version,
		}
		if wantRecord != rec {
			t.Fatalf("verify [%d] daily_reverse[%d]: got\n%+v\nwant\n%+v", i, level+ReverseLevelOffset, rec, wantRecord)
		}
	}

	if err = rec.Read(br); err == nil {
		t.Fatalf("read at end: got '%+v', want '%v'", rec, io.EOF)
	} else if err != io.EOF {
		t.Fatalf("read at end: got '%v', want '%v'", err, io.EOF)
	}
}

func TestIndexParseFile(t *testing.T) {
	points := generateMetrics()
	wantPoints := uint64(len(points) / 2)
	cacheMap := buildCacheMap(points, false)

	tests := []struct {
		name              string
		compress          config.CompAlgo
		compressLevel     int
		disableDailyIndex bool
	}{
		{
			name:          "Uncompressed",
			compress:      config.CompAlgoNone,
			compressLevel: 0,
		},
		{
			name:              "Uncompressed without daily index",
			compress:          config.CompAlgoNone,
			compressLevel:     0,
			disableDailyIndex: true,
		},
		{
			name:          "Compressed",
			compress:      config.CompAlgoNone,
			compressLevel: 3,
		},
		{
			name:              "Compressed without daily index",
			compress:          config.CompAlgoNone,
			compressLevel:     3,
			disableDailyIndex: true,
		},
	}
	logger := zapwriter.Logger("upload")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename, err := writeFile(points, tt.compress, tt.compressLevel)
			if err != nil {
				t.Fatalf("writeFile() got error: %v", err)
			}
			defer os.Remove(filename)

			base := &Base{
				queue:   make(chan string, 1024),
				inQueue: make(map[string]bool),
				logger:  logger,
				config:  &Config{TableName: "test", DisableDailyIndex: tt.disableDailyIndex},
			}
			u := NewIndex(base)

			var out bytes.Buffer
			now32 := uint32(time.Now().Unix())
			n, m, err := u.parseFile(filename, &out)
			if err != nil {
				t.Fatalf("Index.parseFile() got error: %v", err)
			}
			if n != wantPoints {
				t.Errorf("Index.parseFile() got %d, want %d", n, wantPoints)
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

			verifyIndexUploaded(t, &out, points, now32, tt.disableDailyIndex)
		})
	}
}

type indexBench struct {
	name              string
	compress          config.CompAlgo
	compressLevel     int
	disableDailyIndex bool
}

func benchmarkIndexParseFile(b *testing.B, bm *indexBench, points []point, wantPoints uint64) {
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
				config:  &Config{TableName: "test", DisableDailyIndex: bm.disableDailyIndex},
			}
			u := NewIndex(base)

			out.Reset()

			n, _, err := u.parseFile(filename, &out)
			if err != nil {
				b.Fatalf("Index.parseFile() got error: %v", err)
			}
			if n != wantPoints {
				b.Fatalf("Index.parseFile() got %d, want %d", n, wantPoints)
			}
		}
	})
}

func BenchmarkIndexParseFileUncompressed(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := indexBench{
		name:          fmt.Sprintf("%40s", "Uncompressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 0,
	}

	benchmarkIndexParseFile(b, &bm, points, wantPoints)
}

func BenchmarkIndexParseFileCompressed(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := indexBench{
		name:          fmt.Sprintf("%40s", "Compressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 1,
	}

	benchmarkIndexParseFile(b, &bm, points, wantPoints)
}

func BenchmarkIndexParseFileUncompressedNoDaily(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := indexBench{
		name:              fmt.Sprintf("%40s", "Uncompressed without daily index "+wantPointsStr),
		compress:          config.CompAlgoNone,
		compressLevel:     0,
		disableDailyIndex: true,
	}

	benchmarkIndexParseFile(b, &bm, points, wantPoints)
}

func BenchmarkIndexParseFileCompressedNoDaily(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := indexBench{
		name:              fmt.Sprintf("%40s", "Compressed without daily index "+wantPointsStr),
		compress:          config.CompAlgoNone,
		compressLevel:     1,
		disableDailyIndex: true,
	}

	benchmarkIndexParseFile(b, &bm, points, wantPoints)
}
