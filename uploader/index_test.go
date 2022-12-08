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
	"github.com/stretchr/testify/assert"
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
		return reader.CheckError(err)
	}
	r.name, err = rdr.ReadString()
	if err != nil {
		return reader.CheckError(err)
	}
	r.version, err = rdr.ReadUint32()

	return reader.CheckError(err)
}

func verifyIndexRecord(want, got *indexRecord, start, end uint32) (errs []string) {
	if want.date != got.date {
		errs = append(errs, fmt.Sprintf("date want %d, got %d", want.date, got.date))
	}
	if want.level != got.level {
		errs = append(errs, fmt.Sprintf("level want %d, got %d", want.level, got.level))
	}
	if want.name != got.name {
		errs = append(errs, fmt.Sprintf("name want %q, got %q", want.name, got.name))
	}
	if got.version < start || got.version > end {
		errs = append(errs, fmt.Sprintf("version want beetween %d and %d, got %d", start, end, got.version))
	}
	return
}

func TestIndexParseFileDedup(t *testing.T) {
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
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes"},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.writer."},
		{date: 42, level: TreeLevelOffset + 3, name: "carbon.agents.carbon-clickhouse."},
		{date: 42, level: TreeLevelOffset + 2, name: "carbon.agents."},
		{date: 42, level: TreeLevelOffset + 1, name: "carbon."},
		{date: 42, level: ReverseTreeLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon"},
		{date: 18049, level: 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes"},
		{date: 18049, level: ReverseLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon"},
		// carbon.agents.carbon-clickhouse.tcp.errors
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.tcp.receiver."},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.tcp."},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
		{date: 18049, level: 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 18049, level: ReverseLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
		// next day
		// TODO (msaf1980): maybe can deduplicated, but not at now
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
		// next day - continue
		{date: 19237, level: 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 19237, level: ReverseLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
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

	for i := 0; i < 3; i++ {
		t.Run("#"+strconv.Itoa(i), func(t *testing.T) {
			var out bytes.Buffer
			start := uint32(time.Now().Unix())
			n, m, err := u.parseFile(filename, &out)
			end := uint32(time.Now().Unix())
			if err != nil {
				t.Fatalf("Index.parseFile() got error: %v", err)
			}
			if n != 3 {
				t.Errorf("Index.parseFile() got %d, want %d", n, 3)
			}

			records := make([]indexRecord, 0, len(wantIndexRecords))
			var rec indexRecord
			br := reader.NewReader(&out)
			for err = rec.Read(br); err == nil; err = rec.Read(br) {
				records = append(records, rec)
			}
			if err != io.EOF {
				t.Fatalf("indexRecord.Read() got error: %v", err)
			}
			maxLen := tests.Max(len(wantIndexRecords), len(records))
			for i := 0; i < maxLen; i++ {
				if i >= len(records) {
					t.Errorf("[%d]\n- %+v", i, wantIndexRecords[i])
				} else if i >= len(wantIndexRecords) {
					t.Errorf("[%d]\n+ %+v", i, records[i])
				} else if errs := verifyIndexRecord(&wantIndexRecords[i], &records[i], start, end); len(errs) > 0 {
					t.Errorf("[%d] %+v\n%s", i, records[i], strings.Join(errs, "\n"))
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
		})
	}
}

func TestIndexParseFileDedupNoDaily(t *testing.T) {
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
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.writer.writtenBytes"},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.writer."},
		{date: 42, level: TreeLevelOffset + 3, name: "carbon.agents.carbon-clickhouse."},
		{date: 42, level: TreeLevelOffset + 2, name: "carbon.agents."},
		{date: 42, level: TreeLevelOffset + 1, name: "carbon."},
		{date: 42, level: ReverseTreeLevelOffset + 5, name: "writtenBytes.writer.carbon-clickhouse.agents.carbon"},
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 42, level: TreeLevelOffset + 5, name: "carbon.agents.carbon-clickhouse.tcp.receiver."},
		{date: 42, level: TreeLevelOffset + 4, name: "carbon.agents.carbon-clickhouse.tcp."},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
		// next day
		// TODO (msaf1980): maybe can deduplicated, but not at now
		{date: 42, level: TreeLevelOffset + 6, name: "carbon.agents.carbon-clickhouse.tcp.receiver.errors"},
		{date: 42, level: ReverseTreeLevelOffset + 6, name: "errors.receiver.tcp.carbon-clickhouse.agents.carbon"},
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

	for i := 0; i < 3; i++ {
		t.Run("#"+strconv.Itoa(i), func(t *testing.T) {
			var out bytes.Buffer
			start := uint32(time.Now().Unix())
			n, m, err := u.parseFile(filename, &out)
			end := uint32(time.Now().Unix())
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
				} else if errs := verifyIndexRecord(&wantIndexRecords[i], &records[i], start, end); len(errs) > 0 {
					t.Errorf("[%d] %+v\n%s", i, records[i], strings.Join(errs, "\n"))
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
		})
	}
}

func verifyIndexUploaded(t *testing.T, b io.Reader, points []point, start, end uint32, disableDailyIndex bool) {
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
			date:  DefaultTreeDate,
			level: uint32(TreeLevelOffset + level),
			name:  point.path,
		}

		if errs := verifyIndexRecord(&wantRecord, &rec, start, end); len(errs) > 0 {
			t.Fatalf("verify [%d] tree[%d]: +%v\n%s", i, level+TreeLevelOffset, rec, strings.Join(errs, "\n"))
		}
		p := bpath
		for l := level - 1; l > 0; l-- {
			dot := bytes.LastIndexByte(p, '.')
			s := unsafeString(p[:dot+1])
			if newUniq[s] {
				break
			}
			newUniq[string(s)] = true

			if err = rec.Read(br); err != nil {
				t.Fatalf("read [%d] tree[%d]: %v,\nwant\n%+v", i, l+TreeLevelOffset, err, point)
			}
			wantRecord := indexRecord{
				date:  DefaultTreeDate,
				level: uint32(l + TreeLevelOffset),
				name:  s,
			}
			if errs := verifyIndexRecord(&wantRecord, &rec, start, end); len(errs) > 0 {
				t.Fatalf("verify [%d] tree[%d]: +%v\n%s", i, i+TreeLevelOffset, rec, strings.Join(errs, "\n"))
			}
			p = p[:dot]
		}

		// Reverse path without date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] tree_reverse[%d]: %v,\nwant\n%+v", i, level+ReverseTreeLevelOffset, err, point)
		}

		rpath := RowBinary.ReverseBytes(bpath)

		wantRecord = indexRecord{
			date:  DefaultTreeDate,
			level: uint32(level + ReverseTreeLevelOffset),
			name:  string(rpath),
		}
		if errs := verifyIndexRecord(&wantRecord, &rec, start, end); len(errs) > 0 {
			t.Fatalf("verify [%d] tree_reverse[%d]: +%v\n%s", i, level+ReverseTreeLevelOffset, rec, strings.Join(errs, "\n"))
		}

		if disableDailyIndex {
			continue
		}

		// Direct path with date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] daily[%d]: %v,\nwant\n%+v", i, level, err, point)
		}
		wantRecord = indexRecord{
			date:  point.date,
			level: uint32(level),
			name:  point.path,
		}
		if errs := verifyIndexRecord(&wantRecord, &rec, start, end); len(errs) > 0 {
			t.Fatalf("verify [%d] daily[%d]: +%v\n%s", i, level, rec, strings.Join(errs, "\n"))
		}

		// Reverse path with date
		if err = rec.Read(br); err != nil {
			t.Fatalf("read [%d] daily_reverse[%d]: %v,\nwant\n%+v", i, level+ReverseLevelOffset, err, point)
		}
		wantRecord = indexRecord{
			date:  point.date,
			level: uint32(level + ReverseLevelOffset),
			name:  unsafeString(rpath),
		}
		if errs := verifyIndexRecord(&wantRecord, &rec, start, end); len(errs) > 0 {
			t.Fatalf("verify [%d] daily_reverse[%d]: +%v\n%s", i, level+ReverseLevelOffset, rec, strings.Join(errs, "\n"))
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

		for i := 0; i < 3; i++ {
			t.Run("#"+strconv.Itoa(i), func(t *testing.T) {
				var out bytes.Buffer
				start := uint32(time.Now().Unix())
				n, m, err := u.parseFile(filename, &out)
				end := uint32(time.Now().Unix())
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

				verifyIndexUploaded(t, &out, points, start, end, tt.disableDailyIndex)
			})
		}
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
		base := &Base{
			queue:   make(chan string, 1024),
			inQueue: make(map[string]bool),
			logger:  logger,
			config:  &Config{TableName: "test", DisableDailyIndex: bm.disableDailyIndex},
		}
		u := NewIndex(base)
		for i := 0; i < b.N; i++ {
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

func benchmarkIndexParseFileParallel(b *testing.B, bm *indexBench, points []point, wantPoints uint64) {
	b.Run(bm.name, func(b *testing.B) {
		logger := zapwriter.Logger("upload")
		var out bytes.Buffer
		out.Grow(524288)

		filename, err := writeFile(points, bm.compress, bm.compressLevel)
		if err != nil {
			b.Fatalf("writeFile() got error: %v", err)
		}
		defer os.Remove(filename)

		b.Run(bm.name, func(b *testing.B) {
			base := &Base{
				queue:   make(chan string, 1024),
				inQueue: make(map[string]bool),
				logger:  logger,
				config:  &Config{TableName: "test", DisableDailyIndex: bm.disableDailyIndex},
			}
			u := NewIndex(base)

			for i := 0; i < b.N; i++ {
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
	})
}

func BenchmarkIndexParseFileUncompressedParallel(b *testing.B) {
	points := generateMetricsLarge()
	wantPoints := uint64(len(points) / 2)
	wantPointsStr := strconv.FormatUint(wantPoints, 10)
	bm := indexBench{
		name:          fmt.Sprintf("%40s", "Uncompressed "+wantPointsStr),
		compress:      config.CompAlgoNone,
		compressLevel: 0,
	}

	benchmarkIndexParseFileParallel(b, &bm, points, wantPoints)
}

func TestIndexParseName_Overflow(t *testing.T) {
	indexBuf := getIndexBuffer()
	defer releaseIndexBuffer(indexBuf)

	logger := zapwriter.Logger("upload")
	base := &Base{
		queue:   make(chan string, 1024),
		inQueue: make(map[string]bool),
		logger:  logger,
		config:  &Config{TableName: "test"},
	}
	var sb bytes.Buffer
	for i := 0; i < 400; i++ {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(fmt.Sprintf("very_long%d", i))
	}
	u := NewIndex(base)

	name := sb.Bytes()

	l := len(name)
	if l > len(indexBuf.reverseName) {
		indexBuf.reverseName = make([]byte, len(name)*2)
	}
	RowBinary.ReverseBytesTo(indexBuf.reverseName, name)

	treeDate := uint16(DefaultTreeDate)
	days := uint16(10)
	version := uint32(time.Now().Unix())

	err := u.parseName(name, indexBuf.reverseName, treeDate, days, version, u.config.DisableDailyIndex, indexBuf)
	assert.Equal(t, errBufOverflow, err)
}
