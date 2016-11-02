package writer

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

// Writer dumps all received data in prepared for clickhouse format
type Writer struct {
	helper.Stoppable
	sync.RWMutex
	inputChan     chan *points.Points
	path          string
	fileInterval  time.Duration
	fileSize      int
	writtenBytes  uint64          // stat "bytes"
	writtenPoints uint64          // stat "points"
	openedFiles   uint64          // stat "files"
	inProgress    map[string]bool // current writing files
}

func New(in chan *points.Points, path string, fileInterval time.Duration, fileSize int) *Writer {
	return &Writer{
		inputChan:    in,
		path:         path,
		fileInterval: fileInterval,
		fileSize:     fileSize,
		inProgress:   make(map[string]bool),
	}
}

func (w *Writer) Start() error {
	return w.StartFunc(func() error {
		w.Go(func(exit chan bool) {
			w.worker(exit)
		})

		return nil
	})
}

func (w *Writer) IsInProgress(filename string) bool {
	w.RLock()
	v := w.inProgress[filename]
	w.RUnlock()
	return v
}

func removeExtraDots(s string) string {
	v := s
	for {
		v2 := strings.Replace(v, "..", ".", -1)
		if v2 == v {
			break
		} else {
			v = v2
		}
	}
	return v
}

func (w *Writer) glue(exit chan bool, in chan *points.Points, chunkSize int, chunkTimeout time.Duration, callback func([]byte)) {
	var p *points.Points
	var ok bool

	buf := bytes.NewBuffer(nil)

	flush := func() {
		if buf.Len() == 0 {
			return
		}
		callback(buf.Bytes())
		buf = bytes.NewBuffer(nil)
	}

	ticker := time.NewTicker(chunkTimeout)
	defer ticker.Stop()

	for {
		p = nil
		select {
		case p, ok = <-in:
			if !ok { // in chan closed
				flush()
				return
			}
			// pass
		case <-ticker.C:
			flush()
		case <-exit:
			flush()
			return
		}

		if p == nil {
			continue
		}

		for _, d := range p.Data {
			if math.IsInf(d.Value, 0) {
				continue
			}
			s := fmt.Sprintf("%s\t%v\t%d\t%s\t%d\n",
				removeExtraDots(p.Metric),
				d.Value,
				d.Timestamp,
				time.Unix(d.Timestamp, 0).Format("2006-01-02"),
				time.Now().Unix(), // @TODO: may be cache?
			)

			if buf.Len()+len(s) > chunkSize {
				flush()
			}
			buf.Write([]byte(s))
		}
	}

}

func (w *Writer) worker(exit chan bool) {
	var nextRotate time.Time
	var writtenBytes int
	var out *os.File
	var fn string // current filename

	defer func() {
		if out != nil {
			out.Close()
		}
	}()

	// close old file, open new
	rotate := func() {
		if out != nil {
			out.Close()
			out = nil
		}

		var err error

	OpenLoop:
		for {
			// replace fn in inProgress
			w.Lock()
			delete(w.inProgress, fn)
			fn = path.Join(w.path, fmt.Sprintf("default.%d", time.Now().UnixNano()))
			w.inProgress[fn] = true
			w.Unlock()

			out, err = os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

			if err != nil {
				logrus.Errorf("open %s: %s", fn, err.Error())

				// check exit channel
				select {
				case <-exit:
					break OpenLoop
				default:
				}

				// try and spam to error log every second
				time.Sleep(time.Second)

				continue OpenLoop
			}

			writtenBytes = 0
			nextRotate = time.Now().Add(w.fileInterval)
			break OpenLoop
		}
	}

	// open first file
	rotate()

	w.glue(exit, w.inputChan, 64*1024, 100*time.Millisecond, func(chunk []byte) {
		if (writtenBytes > 0 && len(chunk)+writtenBytes > w.fileSize) || time.Now().After(nextRotate) {
			rotate()
			if out == nil { // exited
				return
			}
		}

		out.Write(chunk)
	})
}
